"""Select committed registry inputs and submit exactly one Rust MergeJob."""

from __future__ import annotations

import asyncio
import json
import os
import shlex
import shutil
import time
import tomllib
from datetime import UTC
from pathlib import Path
from typing import Any, Self, cast
from uuid import uuid4

from pydantic import Field, model_validator

from forge.historical.engine.managed import checksum, resolve, supervised
from forge.historical.engine.process import (
    Lifecycle,
    PreparationObserver,
    check,
    ignore_preparation,
    joined_io,
    supervise,
)
from forge.historical.engine.work import READY_MESSAGE, WorkIndex, merge_key, output_present
from forge.historical.merge.config import (
    DatabaseID,
    DerivedRecipe,
    StreamRank,
    TimelineOrdering,
    Timestamp,
    normalization_source,
)
from forge.historical.merge.discovery import discover
from forge.historical.normalization.datasets import (
    primary_reference,
    register_verified,
    verify,
    verify_inventory,
)
from forge.historical.normalization.execution import run_process, sync_submission
from forge.historical.normalization.service import write_job
from forge.orchestration.registry import retain_recipe
from forge.shared.config.loader import configuration_errors
from forge.shared.config.models import SettingsModel
from forge.shared.database.connection import (
    database_connection,
    database_dsn,
    database_flag,
    database_purpose,
    is_testing,
    pinned_registry,
    require_registry,
    selected_registry_id,
)
from forge.shared.errors import MarketForgeError
from forge.shared.identity import RunOperation
from forge.shared.paths import ProjectPaths

MERGE_NAMESPACE = "marketforge:historical:merge:v1"

TEMPLATE = """# Example registry primary-file IDs in precedence order. Replace before preparing.
dataset_ids = [1, 2]
start_ns = 1727683200000000000
end_ns = 1727769600000000000
"""


class Recipe(SettingsModel, frozen=True):
    dataset_ids: list[DatabaseID] = Field(min_length=1)
    start_ns: Timestamp
    end_ns: Timestamp
    stream_ranks: dict[str, StreamRank] = Field(default_factory=dict)
    timeline_ordering: TimelineOrdering | None = None

    @model_validator(mode="after")
    def validate_selection(self) -> Self:
        if len(set(self.dataset_ids)) != len(self.dataset_ids):
            raise ValueError("Select each registered dataset once.")
        if self.start_ns >= self.end_ns:
            raise ValueError("Merge range must satisfy start_ns < end_ns.")
        return self


def read_json(path: Path) -> dict[str, Any]:
    try:
        result = json.loads(path.read_bytes())
    except ValueError as error:
        raise MarketForgeError(f"Invalid JSON artifact {path}: {error}") from error
    if not isinstance(result, dict):
        raise MarketForgeError(f"Expected a JSON object in {path}.")
    return cast(dict[str, Any], result)


def file_pins(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "dataset_id": item["database_id"],
        "path": item["registered_path"],
        "sha256": item["file_sha256"],
        "publication": {
            "engine_dataset_id": str(item["engine_id"]),
            "manifest_path": item["manifest_path"],
            "manifest_sha256": item["manifest_sha256"],
        },
    }


def file_input(item: dict[str, Any]) -> dict[str, Any]:
    return {
        **file_pins(item),
        "stream_id": item["stream_id"],
        "stream_rank": item["stream_rank"],
        "content_type": item["content_type"],
    }


async def select_inputs(
    paths: ProjectPaths,
    *,
    ids: list[int] | None = None,
    job: dict[str, Any] | None = None,
    recovery: bool = False,
) -> list[dict[str, Any]]:
    references: list[tuple[int, str, dict[str, Any]]] = []
    submitted_files: list[dict[str, Any]] | None = None
    if ids is None:
        if job and job.get("primary_files"):
            # Keep registration recovery for publications produced with the previous contract.
            references = [
                (engine, job["input_paths"][str(engine)], item)
                for engine in job["inputs"]
                for item in job["primary_files"][str(engine)]
            ]
        elif (
            job
            and isinstance(job.get("inputs"), list)
            and all(isinstance(item, dict) for item in job["inputs"])
        ):
            submitted_files = job["inputs"]
            assert submitted_files is not None
            candidates = [item.get("dataset_id") for item in submitted_files]
            if any(type(identity) is not int for identity in candidates):
                raise MarketForgeError("Merge input requires a registered file dataset ID.")
            ids = [identity for identity in candidates if isinstance(identity, int)]
        else:
            raise MarketForgeError("Prepare a new merge from registered file dataset IDs.")
    if ids is not None and (
        not ids
        or any(type(i) is not int or not 0 < i < 2**63 for i in ids)
        or len(set(ids)) != len(ids)
    ):
        raise MarketForgeError("Select valid registered file dataset IDs without duplicates.")

    async with database_connection(database_dsn(paths.root)) as c:
        query = """SELECT d.id,d.storage_path,d.primary_file,d.file_sha256,d.operation_details,
                   d.start_timestamp_ns,d.end_timestamp_ns,p.storage_root,p.engine_dataset_id,
                   p.instrument_namespace,p.evidence->>'manifest_sha256',p.manifest_path
                   FROM historical.datasets d JOIN historical.dataset_publications p ON p.id=d.publication_id
                   WHERE d.operation='normalize' AND p.operation='normalize' AND d.status='complete'
                   AND d.storage_format='parquet' AND d.primary_file IS NOT NULL"""
        rows: list[tuple[Any, ...]]
        if ids is not None:
            cur = await c.execute(query + " AND d.id=ANY(%s)", (ids,))
            records = {r[0]: r for r in await cur.fetchall()}
            if set(records) != set(ids):
                raise MarketForgeError(
                    f"Select active normalized file dataset IDs. Unavailable: {sorted(set(ids) - records.keys())}."
                )
            rows = [records[i] for i in ids]
        else:
            rows = []
            for engine, root, item in references:
                cur = await c.execute(
                    query + " AND p.storage_root=%s AND d.primary_file=%s AND d.file_sha256=%s",
                    (root, item["path"], item["sha256"]),
                )
                found = await cur.fetchall()
                if len(found) != 1 or found[0][8] != str(engine):
                    raise MarketForgeError(
                        f"No unique registered file matches {root}/{item['path']}."
                    )
                rows.append(found[0])

    result: list[dict[str, Any]] = []
    publications: dict[str, dict[str, Any]] = {}
    by_engine: dict[str, str] = {}
    file_ids: dict[tuple[int, int], int] = {}
    selections: dict[str, set[str]] = {}
    for row in rows:
        selections.setdefault(row[7], set()).add(row[2])
    declarations = {item["dataset_id"]: item for item in submitted_files or []}
    for (
        db_id,
        file,
        relative,
        sha,
        summary,
        start,
        end,
        root,
        engine,
        namespace,
        manifest_sha,
        manifest_path,
    ) in rows:
        if not all((file, relative, root, engine, namespace, sha, manifest_sha, manifest_path)):
            raise MarketForgeError(
                f"Dataset {db_id} lacks a registered file path, publication identity or fingerprint."
            )
        if not Path(file).is_absolute() or not Path(manifest_path).is_absolute():
            raise MarketForgeError(
                f"Dataset {db_id} requires absolute registered file and manifest paths."
            )
        if engine in by_engine and by_engine[engine] != root:
            raise MarketForgeError(f"Ambiguous engine publication ID {engine}.")
        by_engine[engine] = root
        if root not in publications:
            publications[root] = await asyncio.to_thread(
                verify, paths, Path(root), selected_files=selections[root]
            )
        evidence = publications[root]
        manifest = evidence["manifest"]
        if (
            evidence["manifest_sha256"] != manifest_sha
            or evidence["instrument_namespace"] != namespace
            or str(manifest["dataset_id"]) != engine
            or evidence["manifest_path"] != manifest_path
            or file != str(Path(root) / relative)
            or evidence["inventory_sha256"].get(relative) != sha
        ):
            raise MarketForgeError(
                f"Selected dataset {db_id} disagrees with its publication or primary checksum."
            )
        entry = next((f for f in manifest["files"] if f["path"] == relative), None)
        if entry is None or (start, end) != (
            entry["start_timestamp_ns"],
            entry["end_timestamp_ns"],
        ):
            raise MarketForgeError(f"Dataset {db_id} is not an exact primary manifest member.")
        metadata = await asyncio.to_thread(Path(file).stat)
        physical_file = (metadata.st_dev, metadata.st_ino)
        if physical_file in file_ids:
            raise MarketForgeError(
                f"Datasets {file_ids[physical_file]} and {db_id} reference the same physical file: {file}"
            )
        file_ids[physical_file] = db_id
        streams = manifest.get("streams")
        owners = (
            [stream for stream in streams if Path(relative).parent == Path(stream["directory"])]
            if streams
            else evidence["resolved_job"]["streams"]
        )
        if len(owners) != 1:
            raise MarketForgeError(f"Dataset {db_id} has no unique committed stream owner.")
        result.append(
            {
                "stream_id": owners[0]["stream_id"],
                "content_type": manifest["content_type"],
                "database_id": db_id,
                "engine_id": int(engine),
                "namespace": namespace,
                "path": root,
                "primary_file": relative,
                "registered_path": file,
                "manifest_path": manifest_path,
                "file_sha256": sha,
                "manifest_sha256": manifest_sha,
                "start_ns": start,
                "end_ns": end,
                "integrity_status": manifest["integrity_status"],
                "requires_causal_timeline": (
                    manifest["content_type"] == "depth" and manifest["protocol_version"] >= 4
                ),
                "memberships": summary["memberships"],
            }
        )
    if references and any(
        item["manifest_sha256"] != expected["manifest_sha256"]
        for item, (_, _, expected) in zip(result, references, strict=True)
    ):
        raise MarketForgeError("Submitted file selection has stale publication fingerprints.")
    if submitted_files is not None:
        fields = {"stream_id", "stream_rank", "content_type"}
        legacy = recovery and all(not fields.intersection(item) for item in submitted_files)
        expected_files: list[dict[str, Any]] = []
        for item in result:
            if legacy:
                expected_files.append(file_pins(item))
            else:
                declaration = declarations[item["database_id"]]
                rank = declaration.get("stream_rank")
                if type(rank) is not int or not 0 <= rank < 2**32:
                    raise MarketForgeError(
                        "Invalid or missing stream rank. Regenerate the merge job."
                    )
                item["stream_rank"] = rank
                expected_files.append(file_input(item))
        if expected_files != submitted_files:
            raise MarketForgeError(
                "Submitted file inputs disagree with registered paths or fingerprints."
            )
    if len({item["namespace"] for item in result}) != 1:
        raise MarketForgeError("Select files from a single instrument namespace.")
    return result


def publication_selection(selected: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_engine: dict[int, dict[str, Any]] = {}
    for item in selected:
        identity = item["engine_id"]
        by_engine.setdefault(identity, item)
    return list(by_engine.values())


def coverage(inputs: list[dict[str, Any]], start_ns: int, end_ns: int) -> dict[str, Any]:
    start, end = start_ns, end_ns
    observations: dict[str, list[tuple[int, int]]] = {}
    envelopes: dict[str, set[tuple[int, int]]] = {}
    for item in inputs:
        lo, hi = max(start, item["start_ns"]), min(end, item["end_ns"] + 1)
        if lo >= hi:
            raise MarketForgeError(
                f"Registry input {item['database_id']} does not overlap the merge interval."
            )
        for member in item["memberships"]:
            first = member.get("start_timestamp_ns", item["start_ns"])
            last = member.get("end_timestamp_ns", item["end_ns"])
            left, right = max(start, first), min(end, last + 1)
            if left < right:
                stream = member["stream_id"]
                observations.setdefault(stream, []).append((left, right))
                envelopes.setdefault(stream, set()).add((first, last))
    gaps: dict[str, list[tuple[int, int]]] = {}
    for stream, intervals in sorted(observations.items()):
        cursor = start
        missing: list[tuple[int, int]] = []
        for left, right in sorted(intervals):
            if left > cursor:
                missing.append((cursor, left))
            cursor = max(cursor, right)
        if cursor < end:
            missing.append((cursor, end))
        if missing:
            gaps[stream] = missing
    return {
        "requested": {"start_ns": start, "end_ns": end},
        "observed_extent_gaps": gaps,
        "observed_intervals_inclusive": {
            stream: sorted(extents) for stream, extents in sorted(envelopes.items())
        },
        "files_extending_outside": sorted(
            {
                item["database_id"]
                for item in inputs
                if item["start_ns"] < start or item["end_ns"] >= end
            }
        ),
        "continuity": "unresolved",
        "note": "Observed timestamp bounds cannot prove continuous source coverage.",
    }


def validation_command(
    paths: ProjectPaths, binary: Path, source: Path, config: Path | None
) -> list[str]:
    return [
        str(binary),
        "--validate-job",
        str(source),
        "--project-root",
        str(paths.root),
        "--config",
        str(config or runtime_path(paths)),
    ]


def validate_job(
    paths: ProjectPaths, binary: Path, source: Path, config: Path | None = None
) -> None:
    response = supervised(validation_command(paths, binary, source, config), cwd=paths.root)
    if response.returncode:
        raise MarketForgeError(f"Production merge validation failed: {response.stdout}")


async def validate_async(
    paths: ProjectPaths, binary: Path, source: Path, config: Path, directory: Path
) -> None:
    response = await check(
        validation_command(paths, binary, source, config), paths.root, directory, "validation"
    )
    if response.returncode:
        raise MarketForgeError(f"Production merge validation failed: {response.stdout}")


def source_selection(paths: ProjectPaths, recipe: DerivedRecipe) -> dict[str, Any]:
    return asyncio.run(source_selection_async(paths, recipe))


async def source_selection_async(paths: ProjectPaths, recipe: DerivedRecipe) -> dict[str, Any]:
    path, config, fingerprint = await joined_io(
        normalization_source, paths, recipe.selection.source
    )
    result = await discover(paths, recipe, path, config)
    selected = set(result["selected_dataset_ids"])
    result["coverage"] = coverage(
        [
            {
                "database_id": item["database_id"],
                "start_ns": item["start_ns"],
                "end_ns": item["end_ns"],
                "memberships": [
                    member for member in item["memberships"] if member["request_indexes"]
                ],
            }
            for item in result["candidates"]
            if item["database_id"] in selected
        ],
        recipe.start_ns,
        recipe.end_ns,
    )
    result["coverage"]["missing_request_indexes"] = [
        item["request_index"] for item in result["missing"]
    ]
    result["interval_display"] = (
        f"{recipe.interval.start.astimezone(UTC).isoformat()} → {recipe.interval.end.astimezone(UTC).isoformat()}"
    )
    result["source"] = {
        "reference": recipe.selection.source,
        "path": str(path),
        "sha256": fingerprint,
    }
    return result


def prepare(
    paths: ProjectPaths, recipe: Recipe | DerivedRecipe, name: str, *, dry_run: bool = False
) -> dict[str, Any]:
    identity = uuid4().int & (2**63 - 1)
    directory = paths.runs_dir / f"merge-prepare-{identity}"
    result: dict[str, Any] = {}
    if isinstance(recipe, DerivedRecipe):
        result = source_selection(paths, recipe)
        result.update(
            recipe=name,
            environment=database_purpose(),
            report_path=str(directory / "prepare-report.json"),
        )
        if result["blockers"]:
            return blocked(directory, result, dry_run=dry_run)
        ids = result["selected_dataset_ids"]
    else:
        ids = recipe.dataset_ids
    try:
        return prepare_job(paths, recipe, name, identity, directory, ids, result, dry_run=dry_run)
    except (MarketForgeError, OSError, ValueError) as error:
        if not isinstance(recipe, DerivedRecipe) and not dry_run:
            raise
        result.update(recipe=name, environment=database_purpose())
        result.setdefault("blockers", []).append(
            {
                "dataset_ids": ids,
                "reason": str(error),
                "suggestion": "Check the selected publications and narrow or correct the recipe before preparing again.",
            }
        )
        return blocked(directory, result, dry_run=dry_run)


def blocked(directory: Path, report: dict[str, Any], *, dry_run: bool = False) -> dict[str, Any]:
    report.update(status="blocked", compatibility="blocked")
    if dry_run:
        report["dry_run"] = True
        return report
    directory.mkdir(parents=True, exist_ok=True)
    write_job(directory / "prepare-report.json", report)
    return report


def load_recipe(source: Path, *, retain: bool = True) -> Recipe | DerivedRecipe:
    with configuration_errors(source):
        data = source.read_bytes()
        document = tomllib.loads(data.decode())
        recipe = (
            DerivedRecipe.model_validate(document)
            if {"version", "interval", "selection"} & document.keys()
            else Recipe.model_validate(document)
        )
    if retain:
        retain_recipe(RunOperation.MERGE, source, data)
    return recipe


def validate_recipe(paths: ProjectPaths, recipe: Recipe | DerivedRecipe) -> None:
    if isinstance(recipe, DerivedRecipe):
        normalization_source(paths, recipe.selection.source)


def runtime_path(paths: ProjectPaths) -> Path:
    return paths.runtime_config("merge")


def assign_ranks(
    selected: list[dict[str, Any]],
    overrides: dict[str, int],
    candidates: dict[int, dict[str, Any]],
) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = {}
    for item in selected:
        groups.setdefault(item["stream_id"], []).append(item)
    if overrides.keys() - groups.keys() or len(set(overrides.values())) != len(overrides):
        raise MarketForgeError(
            "Rank overrides must refer to selected streams and use unique ranks."
        )

    def requested(stream: str) -> tuple[int, str]:
        return (
            min(
                (
                    index
                    for item in groups[stream]
                    for member in candidates[item["database_id"]]["memberships"]
                    if member["stream_id"] == stream
                    for index in member["request_indexes"]
                ),
                default=2**63,
            ),
            stream,
        )

    order = sorted(groups, key=requested) if candidates else groups
    reserved = set(overrides.values())
    available = (rank for rank in range(2**32) if rank not in reserved)
    summary: list[dict[str, Any]] = []
    for stream in order:
        files = groups[stream]
        if len({item["content_type"] for item in files}) != 1:
            raise MarketForgeError(f"Conflicting content types for stream {stream}.")
        rank = overrides[stream] if stream in overrides else next(available)
        for item in files:
            item["stream_rank"] = rank
        summary.append(
            {
                "stream_id": stream,
                "stream_rank": rank,
                "content_type": files[0]["content_type"],
                "file_count": len(files),
            }
        )
    return sorted(summary, key=lambda stream: stream["stream_rank"])


def effective_runtime(paths: ProjectPaths, binary: Path, config: Path) -> dict[str, Any]:
    result = supervised([str(binary), "--runtime-settings", str(config)], cwd=paths.root)
    if result.returncode:
        raise MarketForgeError(f"Could not resolve merge runtime: {result.stdout}")
    settings: object = json.loads(result.stdout)
    if not isinstance(settings, dict):
        raise MarketForgeError("Expected merge runtime settings from the engine.")
    return cast(dict[str, Any], settings)


def prepare_job(
    paths: ProjectPaths,
    recipe: Recipe | DerivedRecipe,
    name: str,
    identity: int,
    directory: Path,
    ids: list[int],
    discovery: dict[str, Any],
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    async def select() -> tuple[str, list[dict[str, Any]]]:
        registry = await selected_registry_id(paths.root)
        with pinned_registry(registry):
            return registry, await select_inputs(paths, ids=ids)

    registry, selected = asyncio.run(select())
    return assemble_job(
        paths,
        recipe,
        name,
        identity,
        directory,
        selected,
        discovery,
        registry=registry,
        dry_run=dry_run,
    )


async def prepare_direct(
    paths: ProjectPaths,
    recipe: Recipe,
    name: str,
    *,
    historical: Path,
    executable: Path,
    lifecycle: Lifecycle = lambda _: None,
    on_candidate: PreparationObserver = ignore_preparation,
) -> dict[str, Any]:
    return await prepare_async(
        paths,
        recipe,
        name,
        historical=historical,
        executable=executable,
        lifecycle=lifecycle,
        on_candidate=on_candidate,
    )


async def prepare_async(
    paths: ProjectPaths,
    recipe: Recipe | DerivedRecipe,
    name: str,
    *,
    historical: Path | None = None,
    executable: Path | None = None,
    lifecycle: Lifecycle = lambda _: None,
    on_candidate: PreparationObserver = ignore_preparation,
) -> dict[str, Any]:
    identity = uuid4().int & (2**63 - 1)
    directory = paths.runs_dir / f"merge-prepare-{identity}"
    discovery: dict[str, Any] = {}
    if isinstance(recipe, DerivedRecipe):
        discovery = await source_selection_async(paths, recipe)
        discovery.update(recipe=name, report_path=str(directory / "prepare-report.json"))
        if discovery["blockers"]:
            return await joined_io(blocked, directory, discovery)
        ids = discovery["selected_dataset_ids"]
    else:
        ids = recipe.dataset_ids
    registry = await selected_registry_id(paths.root)
    with pinned_registry(registry):
        selected = await select_inputs(paths, ids=ids)
    result = await joined_io(
        lambda: assemble_job(
            paths,
            recipe,
            name,
            identity,
            directory,
            selected,
            discovery,
            registry=registry,
            validate=False,
            preview_work=historical is None and executable is None,
        )
    )
    if result["status"] == "complete":
        return result
    source = Path(result["job_path"])
    binary = executable or await joined_io(resolve, paths, "merge")
    runtime = paths.runtime_config("merge", historical)
    config = directory / runtime.name
    result["candidate_executable_sha256"] = await joined_io(
        copy_runtime, result, binary, runtime, config
    )
    candidate = directory / "prepare-candidate.json"
    write_job(candidate, result)
    on_candidate(source, candidate)
    return await validate_candidate(paths, source, result, binary, lifecycle=lifecycle)


async def validate_candidate(
    paths: ProjectPaths,
    source: Path,
    result: dict[str, Any],
    executable: Path,
    *,
    lifecycle: Lifecycle,
) -> dict[str, Any]:
    root = source.parent
    config = root / str(result.get("configuration_filename", "general.toml"))
    digest = result["candidate_executable_sha256"]
    if (
        checksum(executable) != digest
        or checksum(config) != result["configuration_sha256"]
        or checksum(source) != result["submitted_sha256"]
    ):
        raise MarketForgeError("Retained merge preparation changed before validation.")
    logs = root / f"validation-{uuid4().hex}"
    logs.mkdir()
    try:
        lifecycle("validate")
        await validate_async(paths, executable, source, config, logs)
        finish_preparation(result, executable, source, digest)
    except (MarketForgeError, OSError) as error:
        result["validation_error"] = str(error)
        result["report_path"] = str(logs / "prepare-report.json")
    else:
        result["report_path"] = str(root / "prepare-report.json")
    result["validation_logs"] = str(logs)
    write_job(Path(result["report_path"]), result)
    return result


def copy_runtime(report: dict[str, Any], binary: Path, source: Path, config: Path) -> str:
    digest = checksum(binary)
    shutil.copyfile(source, config)
    report["configuration_origin"] = str(source)
    report["configuration_filename"] = config.name
    report["configuration_sha256"] = checksum(config)
    return digest


def finish_preparation(report: dict[str, Any], binary: Path, source: Path, digest: str) -> None:
    if checksum(binary) != digest or checksum(source) != report["submitted_sha256"]:
        raise MarketForgeError(
            "Managed merge executable or generated job changed during validation."
        )
    report.update(status="prepared", compatibility="validated", executable_sha256=digest)


def assemble_job(
    paths: ProjectPaths,
    recipe: Recipe | DerivedRecipe,
    name: str,
    identity: int,
    directory: Path,
    selected: list[dict[str, Any]],
    discovery: dict[str, Any],
    *,
    registry: str,
    validate: bool = True,
    dry_run: bool = False,
    preview_work: bool = False,
) -> dict[str, Any]:
    candidates: dict[int, dict[str, Any]] = {}
    if discovery:
        candidates = {item["database_id"]: item for item in discovery["candidates"]}
        for item in selected:
            candidate = candidates[item["database_id"]]
            if (
                any(
                    item[key] != candidate[key]
                    for key in (
                        "namespace",
                        "path",
                        "primary_file",
                        "start_ns",
                        "end_ns",
                        "file_sha256",
                        "manifest_sha256",
                    )
                )
                or str(item["engine_id"]) != candidate["engine_id"]
                or item["registered_path"] != candidate["registered_path"]
                or item["memberships"]
                != [
                    {
                        key: value
                        for key, value in member.items()
                        if key not in {"request_indexes", "overlaps"}
                    }
                    for member in candidate["memberships"]
                ]
            ):
                raise MarketForgeError(
                    f"Registry dataset {item['database_id']} changed during preparation."
                )

    streams = assign_ranks(selected, recipe.stream_ranks, candidates)
    selected_coverage = (
        discovery["coverage"] if discovery else coverage(selected, recipe.start_ns, recipe.end_ns)
    )
    output = (paths.datasets_dir / "merged" / str(identity)).absolute()
    if output.exists() or output.is_symlink():
        raise MarketForgeError(f"Merge output already exists: {output}")
    source = directory / "merge-job.json"
    job: dict[str, Any] = {
        "job_id": identity,
        "operation": "merge",
        "inputs": [file_input(item) for item in selected],
        "timeline_ordering": recipe.timeline_ordering
        or (
            "causal"
            if any(item.get("requires_causal_timeline") for item in selected)
            else "chronological"
        ),
        "time_range": {"start_ns": recipe.start_ns, "end_ns": recipe.end_ns},
        "output": {"dataset_path": str(output)},
    }
    report: dict[str, Any] = {
        **discovery,
        "environment": database_purpose(),
        "registry_id": registry,
        "status": "generated",
        "compatibility": "not_validated",
        "timeline_ordering": job["timeline_ordering"],
        "recipe": name,
        "job_path": str(source),
        "inputs": selected,
        **({"requested_file_ids": recipe.dataset_ids} if isinstance(recipe, Recipe) else {}),
        "streams": streams,
        "stream_count": len(streams),
        "selected_file_count": len(selected),
        "publication_count": len(publication_selection(selected)),
        "report_path": str(directory / "prepare-report.json"),
        "coverage": selected_coverage,
    }
    if validate or preview_work:
        try:
            binary = resolve(paths, "merge")
            settings = effective_runtime(paths, binary, runtime_path(paths))
            index = WorkIndex(paths.root, registry, "merge", directory.name)
            key = merge_key(registry, checksum(binary), job, settings, selected)
            prior_work = asyncio.run(index.peek([key]))
            report["reuse_plan"] = {"work_key": key, "status": "would_execute"}
            if key in prior_work:
                owner, claim, original = prior_work[key]
                if original is None:
                    report["reuse_plan"].update(
                        status="needs_registration_recovery"
                        if output_present(Path(claim["path"]))
                        else "in_progress",
                        attempt=owner,
                    )
                else:
                    asyncio.run(
                        verify_reused_merge(paths, index, binary, selected, settings, original)
                    )
                    report["reuse_plan"].update(status="would_reuse", path=original["path"])
                    if not dry_run:
                        result: dict[str, Any] = {
                            **original,
                            "disposition": "reused",
                            "reuse_message": READY_MESSAGE,
                            "engine_execution_skipped": True,
                            "canonical_execution_id": original["execution_id"],
                            "execution_id": directory.name,
                            "recipe": name,
                            "report_path": str(directory / "reuse-result.json"),
                            "requested_output_path": str(output),
                            "reuse_plan": report["reuse_plan"],
                        }
                        write_job(Path(result["report_path"]), result)
                        return result
        except (MarketForgeError, OSError, ValueError, KeyError) as error:
            report["reuse_plan"] = {"status": "blocked", "reason": str(error)}
    if dry_run:
        report.update(
            status="planned",
            dry_run=True,
            compatibility="not_validated",
            planned_output_path=str(output),
            configuration_origin=str(paths.engine_config_dir / "merge.toml"),
        )
        return report
    directory.mkdir(parents=True, exist_ok=False)
    write_job(source, job)
    report["submitted_sha256"] = checksum(source)
    if validate:
        try:
            binary = resolve(paths, "merge")
            runtime = runtime_path(paths)
            config = directory / runtime.name
            digest = copy_runtime(report, binary, runtime, config)
            validate_job(paths, binary, source, config)
            finish_preparation(report, binary, source, digest)
        except (MarketForgeError, OSError) as error:
            report["validation_error"] = str(error)
        write_job(directory / "prepare-report.json", report)
    return report


def verify_output(
    paths: ProjectPaths,
    source: Path,
    binary: Path,
    selected: list[dict[str, Any]],
    expected: dict[str, Any],
) -> dict[str, Any]:
    root = source.parent if source.name == "manifest.json" else source
    if root.is_symlink():
        raise MarketForgeError("Merge dataset must not be a symlink.")
    root = root.resolve()
    manifest_path = root / "manifest.json"
    manifest = read_json(manifest_path)
    job = read_json(root / "merge-job.json")
    publications = publication_selection(selected)
    if (
        job != expected
        or job["operation"] != "merge"
        or Path(job["output"]["dataset_path"]) != root
        or manifest["job_id"] != job["job_id"]
        or manifest["time_range"] != job["time_range"]
        or manifest.get("timeline_ordering", "chronological")
        != job.get("timeline_ordering", "chronological")
        or manifest["start_timestamp_ns"] is None
        or manifest["end_timestamp_ns"] is None
        or [(entry["dataset_id"], entry["dataset_path"]) for entry in manifest["input_datasets"]]
        != [(item["engine_id"], item["path"]) for item in publications]
        or json.loads((root / "merge-input-checksums.json").read_bytes())
        != [item["manifest_sha256"] for item in publications]
    ):
        raise MarketForgeError(
            "Committed merge output disagrees with expected job, bounds or inputs."
        )
    response = supervised(
        [str(binary), "--validate-dataset", str(root)], cwd=paths.root, timeout=None
    )
    if response.returncode:
        raise MarketForgeError(f"Rust merged-output validation failed: {response.stdout}")
    inventory = verify_inventory(
        root,
        manifest["files"],
        (
            "manifest.json",
            "merge-job.json",
            "merge-input-checksums.json",
            "merge-partitions.json",
            *(("merge-runtime.json",) if (root / "merge-runtime.json").is_file() else ()),
        ),
        require_commit_checksums=True,
    )
    from forge.datasets.registration import merged_file_memberships

    try:
        memberships = merged_file_memberships(root, manifest, selected)
    except ImportError as error:
        raise MarketForgeError(str(error)) from error
    parent_keys = (
        "database_id",
        "namespace",
        "engine_id",
        "path",
        "primary_file",
        "file_sha256",
        "manifest_sha256",
    )
    parent_keys += tuple(
        key
        for key in ("stream_id", "stream_rank", "content_type")
        if all(key in item for item in selected)
    )
    return {
        "operation": "merge",
        "file_memberships": memberships,
        "content_type": manifest["mode"],
        "path": str(root),
        "manifest_path": str(manifest_path),
        "manifest_sha256": inventory["manifest.json"],
        "engine_dataset_id": str(manifest["job_id"]),
        "instrument_namespace": MERGE_NAMESPACE,
        "manifest": manifest,
        "resolved_job": job,
        "inventory_sha256": inventory,
        "processing_job_sha256": inventory["merge-job.json"],
        "bytes_written": sum(item["size_bytes"] for item in manifest["files"]),
        "merge_inputs": [{key: item[key] for key in parent_keys} for item in selected],
    }


def same_inputs(saved: list[dict[str, Any]], selected: list[dict[str, Any]]) -> bool:
    # Older receipts carried an inferred stream and predate explicit registered path fields.
    if len(saved) != len(selected):
        return False
    for previous, current in zip(saved, selected, strict=True):
        if "requires_causal_timeline" not in previous:
            current = {
                key: value for key, value in current.items() if key != "requires_causal_timeline"
            }
        if "stream_rank" not in previous:
            previous = {key: value for key, value in previous.items() if key != "stream_id"}
            current = {
                key: value
                for key, value in current.items()
                if key not in {"stream_id", "stream_rank", "content_type"}
            }
        if "registered_path" not in previous and "manifest_path" not in previous:
            previous["registered_path"] = str(Path(previous["path"]) / previous["primary_file"])
            previous["manifest_path"] = str(Path(previous["path"]) / "manifest.json")
        if previous != current:
            return False
    return True


def execute(paths: ProjectPaths, source: Path, *, tui: bool = False) -> dict[str, Any]:
    return asyncio.run(execute_async(paths, source, tui=tui, standalone=True))


async def execute_async(
    paths: ProjectPaths,
    source: Path,
    *,
    tui: bool = False,
    standalone: bool = False,
    execution_directory: Path | None = None,
    historical: Path | None = None,
    executable: Path | None = None,
    lifecycle: Lifecycle = lambda _: None,
) -> dict[str, Any]:
    managed_started = time.monotonic()
    if tui and not all(os.isatty(fd) for fd in (0, 1, 2)):
        raise MarketForgeError("--tui requires an interactive terminal.")
    binary = executable or resolve(paths, "merge")
    directory = execution_directory or paths.runs_dir / f"merge-execute-{uuid4().hex}"
    directory.mkdir(parents=True, exist_ok=False)
    pinned = directory / "marketforge-merge"
    digest = checksum(binary)
    shutil.copyfile(binary, pinned)
    pinned.chmod(0o755)
    if checksum(pinned) != digest:
        raise MarketForgeError("Managed merge executable changed during execution preparation.")
    submission = directory / "submitted-merge.json"
    await joined_io(lambda: submission.write_bytes(source.read_bytes()))
    submitted_sha = checksum(submission)
    report = source.parent / "prepare-report.json"
    prepared = read_json(report) if report.is_file() else {}
    runtime = (
        source.parent / str(prepared.get("configuration_filename", "general.toml"))
        if prepared.get("configuration_sha256") is not None
        else paths.runtime_config("merge", historical)
    )
    config = directory / runtime.name
    origin = prepared.get("configuration_origin", str(runtime))
    shutil.copyfile(runtime, config)
    configuration_sha256 = checksum(config)
    settings = await joined_io(effective_runtime, paths, pinned, config)
    job = read_json(submission)
    registry = await selected_registry_id(paths.root)
    if prepared.get("registry_id") is not None:
        require_registry(prepared, registry)
    elif not is_testing():
        raise MarketForgeError("Prepare this merge job again in production.")
    with pinned_registry(registry):
        selected = await select_inputs(paths, job=job)
    if prepared and (
        prepared.get("submitted_sha256") != submitted_sha
        or (
            prepared.get("executable_sha256") is not None
            and prepared["executable_sha256"] != digest
        )
        or not same_inputs(prepared.get("inputs", []), selected)
        or (
            prepared.get("configuration_sha256") is not None
            and prepared["configuration_sha256"] != configuration_sha256
        )
    ):
        raise MarketForgeError(
            "Prepared merge job, selected inputs, executable or runtime changed."
        )
    intent = directory / "execution-intent.json"
    result_path = directory / "execution-result.json"
    output = Path(job["output"]["dataset_path"])
    engine_result_path = directory / "engine-result.json"
    receipt: dict[str, Any] = {
        "schema_version": 1,
        "operation": "merge",
        "environment": database_purpose(),
        "registry_id": registry,
        "execution_id": directory.name,
        "job_id": job["job_id"],
        "recipe": prepared.get("recipe"),
        "submission_path": str(submission),
        "submitted_sha256": submitted_sha,
        "executable_sha256": digest,
        "configuration_sha256": configuration_sha256,
        "configuration_path": str(config),
        "configuration_origin": origin,
        "effective_runtime": settings,
        "inputs": selected,
        "coverage": coverage(selected, **job["time_range"]),
        "path": str(output),
        "report_path": str(result_path),
        "execution_receipt": str(intent),
        "engine_result_path": str(engine_result_path),
        "recovery": f"mf merge register {shlex.quote(str(intent))} {database_flag()}",
        "execution": "running",
        "verification": "not_started",
        "registration": "not_started",
        "counts": {"requested": 1, "successful": 0, "failed": 0, "registered": 0},
    }
    key = merge_key(registry, digest, job, settings, selected)
    index = WorkIndex(paths.root, registry, "merge", directory.name)
    candidate = {"report_path": str(result_path), "path": str(output), "intent": str(intent)}

    async def recover_candidate(_key: str, _owner: str, source: Path) -> None:
        await register(paths, source, require_pinned=True, lifecycle=lifecycle)

    await index.recover_committed([key], recover_candidate)
    existing = await index.acquire({key: candidate})
    if key in existing:
        _, _, original = existing[key]
        assert original is not None
        await verify_reused_merge(paths, index, pinned, selected, settings, original)
        reused: dict[str, Any] = {
            **original,
            "disposition": "reused",
            "reuse_message": READY_MESSAGE,
            "engine_execution_skipped": True,
            "canonical_execution_id": original["execution_id"],
            "execution_id": directory.name,
            "report_path": str(result_path),
            "requested_submission_path": str(submission),
            "requested_output_path": str(output),
            "execution_intent_reference": original["execution_receipt"],
        }
        write_job(result_path, reused)
        return reused
    try:
        if standalone:
            validate_job(paths, pinned, submission, config)
        else:
            await validate_async(paths, pinned, submission, config, directory)
        receipt["work_key"] = key
        sync_submission(submission)
        if (
            checksum(submission) != submitted_sha
            or checksum(pinned) != digest
            or checksum(config) != configuration_sha256
        ):
            raise MarketForgeError("Validated merge job or executable changed before launch.")
        write_job(intent, receipt)
    except BaseException:
        await index.release([key])
        raise
    try:
        lifecycle("execute")
        started = time.monotonic()
        argv = [
            str(pinned),
            "--job",
            str(submission),
            "--project-root",
            str(paths.root),
            "--config",
            str(config),
            "--result-file",
            str(engine_result_path),
            *(["--config-origin", origin] if tui else []),
        ]
        code, interrupted = (
            run_process(argv, paths.root, directory, tui=tui)
            if standalone
            else await supervise(argv, paths.root, directory)
        )
    except OSError as error:
        receipt.update(
            status="failed",
            execution="failed",
            error=str(error),
            managed_elapsed_ms=int((time.monotonic() - managed_started) * 1000),
        )
        write_job(result_path, receipt)
        if not await joined_io(output_present, output):
            await index.release([key])
        return receipt
    receipt.update(
        engine_elapsed_ms=int((time.monotonic() - started) * 1000),
        exit_code=code,
        execution="interrupted" if interrupted else "failed" if code else "complete",
        status="interrupted" if interrupted else "failed",
    )
    diagnostics = directory / ("engine-tui.log" if tui else "engine-stderr.log")
    if diagnostics.is_file():
        receipt["engine_diagnostics_path"] = str(diagnostics)
    if engine_result_path.is_file():
        try:
            receipt["engine_result"] = read_json(engine_result_path)
        except MarketForgeError as error:
            receipt["error"] = str(error)
    if not interrupted and code != 0:
        receipt["error"] = receipt.get("engine_result", {}).get(
            "error",
            "Engine failed. Inspect the execution directory and the retained terminal diagnostics.",
        )
    write_job(directory / "engine-outcome.json", receipt)
    try:
        if code == 0 and not interrupted:
            lifecycle("verify")
            try:
                engine_result = receipt.get("engine_result")
                if engine_result != {
                    "schema_version": 1,
                    "kind": "merge_result",
                    "status": "complete",
                    "job_id": job["job_id"],
                    "dataset_path": str(output),
                    "manifest_path": str(output / "manifest.json"),
                }:
                    raise MarketForgeError(
                        "Merge engine returned inconsistent publication evidence."
                    )
                evidence = await joined_io(
                    lambda: verify_output(paths, output, pinned, selected, job)
                )
                if read_json(output / "merge-runtime.json") != settings:
                    raise MarketForgeError("Committed merge runtime differs from execution intent.")
            except (MarketForgeError, OSError, ValueError, KeyError, TypeError) as error:
                receipt.update(verification="failed", error=str(error))
            else:
                receipt.update(
                    verification="verified",
                    dataset={
                        **evidence["manifest"],
                        "bytes_written": evidence["bytes_written"],
                        "engine_dataset_id": evidence["engine_dataset_id"],
                        "manifest_sha256": evidence["manifest_sha256"],
                    },
                )
                receipt["counts"].update(
                    successful=1,
                    verified_publications=1,
                    verified_primary_files=evidence["manifest"]["files_written"],
                )
                lifecycle("register")
                result = await register_verified(
                    paths,
                    [evidence],
                    receipt_directory=directory,
                    execution={
                        "execution_id": directory.name,
                        "environment": receipt["environment"],
                        "registry_id": receipt["registry_id"],
                        "submitted_sha256": receipt["submitted_sha256"],
                    },
                )
                receipt.update(
                    registration=result["registration"],
                    registration_receipt=result["registration_receipt"],
                )
                if result["registration"] == "registered":
                    receipt.update(
                        status="complete",
                        catalogue_dataset_ids=result["catalogue_dataset_ids"],
                        publications=result["publications"],
                    )
                    receipt["counts"].update(
                        registered=len(result["catalogue_dataset_ids"]), **result["counts"]
                    )
                    receipt.pop("recovery")
                else:
                    receipt["registration_error"] = result["error"]
    except (KeyboardInterrupt, asyncio.CancelledError):
        interrupted = True
        receipt.update(status="interrupted")
    if not interrupted and not receipt["counts"]["successful"]:
        receipt["counts"]["failed"] = 1
    receipt["managed_elapsed_ms"] = int((time.monotonic() - managed_started) * 1000)
    write_job(result_path, receipt)
    if receipt["status"] == "complete":
        await index.complete(key, receipt, recovery=True)
    elif not await joined_io(output_present, output):
        await index.release([key])
    return receipt


async def verify_reused_merge(
    paths: ProjectPaths,
    index: WorkIndex,
    binary: Path,
    selected: list[dict[str, Any]],
    settings: dict[str, Any],
    original: dict[str, Any],
) -> None:
    original_submission = Path(original["submission_path"])
    if checksum(original_submission) != original["submitted_sha256"]:
        raise MarketForgeError("Canonical merge submission changed.")
    previous_job = read_json(original_submission)
    committed = await joined_io(
        verify_output, paths, Path(original["path"]), binary, selected, previous_job
    )
    if (
        committed["manifest_sha256"] != original["dataset"]["manifest_sha256"]
        or read_json(Path(original["path"]) / "merge-runtime.json") != settings
    ):
        raise MarketForgeError("Canonical merge evidence changed.")
    await index.verify_registered(original, original["path"], committed["inventory_sha256"])


async def register(
    paths: ProjectPaths,
    source: Path,
    *,
    lifecycle: Lifecycle = lambda _: None,
    require_pinned: bool = False,
) -> dict[str, Any]:
    binary = None
    execution = None
    intent = None
    selected_primary = None
    if source.name == "execution-intent.json":
        intent = read_json(source)
        if intent.get("operation") != "merge":
            raise MarketForgeError("Expected a managed merge execution receipt.")
        require_registry(intent, await selected_registry_id(paths.root))
        directory = (await asyncio.to_thread(source.resolve)).parent
        pinned = directory / "marketforge-merge"
        if pinned.is_file() and checksum(pinned) == intent["executable_sha256"]:
            binary = pinned
        elif require_pinned:
            raise MarketForgeError("Pinned merge executable changed or is unavailable.")
        submission = directory / "submitted-merge.json"
        if checksum(submission) != intent["submitted_sha256"]:
            raise MarketForgeError("Recovery submission differs from merge execution intent.")
        job = read_json(submission)
        source = Path(job["output"]["dataset_path"])
        execution = {
            "execution_id": intent["execution_id"],
            "environment": intent["environment"],
            "registry_id": intent["registry_id"],
            "submitted_sha256": intent["submitted_sha256"],
        }
    else:
        source, selected_primary = primary_reference(source)
        root = source.parent if source.name == "manifest.json" else source
        job = read_json(root / "merge-job.json")
    binary = binary or resolve(paths, "merge")
    lifecycle("verify")
    if intent is not None:
        with pinned_registry(intent["registry_id"]):
            selected = await select_inputs(paths, job=job, recovery=True)
    else:
        selected = await select_inputs(paths, job=job, recovery=True)
    if intent is not None and not same_inputs(intent["inputs"], selected):
        raise MarketForgeError("Recovery inputs differ from merge execution intent.")
    try:
        evidence = await joined_io(lambda: verify_output(paths, source, binary, selected, job))
    except (ValueError, KeyError, TypeError) as error:
        raise MarketForgeError(f"Invalid merge output evidence: {error}") from error
    if intent and "effective_runtime" in intent:
        if read_json(Path(evidence["path"]) / "merge-runtime.json") != intent["effective_runtime"]:
            raise MarketForgeError("Committed merge runtime differs from execution intent.")
    if selected_primary is not None:
        evidence["selected_primary"] = selected_primary
    lifecycle("register")
    result = await register_verified(
        paths,
        [evidence],
        receipt_directory=paths.runs_dir / f"merge-register-{uuid4().hex}",
        execution=execution,
    )
    if (
        intent
        and intent.get("work_key")
        and result["registration"] == "registered"
        and execution is not None
    ):
        index = WorkIndex(paths.root, intent["registry_id"], "merge", intent["execution_id"])
        recovered = {
            **intent,
            **result,
            "status": "complete",
            "verification": "verified",
            "execution": "complete",
            "dataset": {
                **evidence["manifest"],
                "bytes_written": evidence["bytes_written"],
                "engine_dataset_id": evidence["engine_dataset_id"],
                "manifest_sha256": evidence["manifest_sha256"],
            },
        }
        await index.complete(intent["work_key"], recovered, recovery=True)
    return result
