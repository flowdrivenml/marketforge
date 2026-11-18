"""Select committed registry inputs and submit exactly one Rust MergeJob."""

from __future__ import annotations

import asyncio
import json
import os
import shutil
from datetime import UTC
from pathlib import Path
from typing import Any, Self, cast
from uuid import uuid4

from pydantic import Field, model_validator

from forge.historical.engine.managed import checksum, resolve, supervised
from forge.historical.merge.config import DatabaseID, DerivedRecipe, Timestamp, normalization_source
from forge.historical.merge.discovery import discover, layout_reason
from forge.historical.normalization.datasets import (
    primary_reference,
    register_verified,
    verify,
    verify_inventory,
)
from forge.historical.normalization.execution import run_process, sync_submission
from forge.historical.normalization.service import write_job
from forge.shared.config.models import SettingsModel
from forge.shared.database.testing import testing_connection, testing_dsn
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths

MERGE_NAMESPACE = "marketforge:historical:merge:v1"

TEMPLATE = """# Registry dataset IDs in precedence order.
dataset_ids = [1, 2]
start_ns = 1727683200000000000
end_ns = 1727769600000000000
"""


class Recipe(SettingsModel, frozen=True):
    dataset_ids: list[DatabaseID] = Field(min_length=1)
    start_ns: Timestamp
    end_ns: Timestamp

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


def runtime_settings(paths: ProjectPaths, binary: Path, config: Path) -> dict[str, Any]:
    process = supervised([str(binary), "--runtime-settings", str(config)], cwd=paths.root)
    if process.returncode:
        raise MarketForgeError(f"Invalid historical runtime configuration: {process.stdout}")
    return json.loads(process.stdout)


async def select_inputs(
    paths: ProjectPaths,
    *,
    ids: list[int] | None = None,
    job: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    references: list[tuple[str, dict[str, Any]]]
    if ids is None:
        if job is None or not job.get("primary_files"):
            raise MarketForgeError(
                "Prepare a new merge from file dataset IDs; legacy whole-publication requests are ambiguous."
            )
        references = [
            (job["input_paths"][str(engine)], item)
            for engine in job["inputs"]
            for item in job["primary_files"][str(engine)]
        ]
    else:
        references = []
    async with testing_connection(testing_dsn(paths.root)) as c:
        query = """SELECT d.id,d.storage_path,d.primary_file,d.file_sha256,d.operation_details,
                   d.start_timestamp_ns,d.end_timestamp_ns,p.storage_root,p.engine_dataset_id,
                   p.instrument_namespace,p.evidence->>'manifest_sha256'
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
            for root, item in references:
                cur = await c.execute(
                    query + " AND p.storage_root=%s AND d.primary_file=%s AND d.file_sha256=%s",
                    (root, item["path"], item["sha256"]),
                )
                found = await cur.fetchall()
                if len(found) != 1:
                    raise MarketForgeError(
                        f"No unique registered file matches {root}/{item['path']}."
                    )
                rows.append(found[0])

    result: list[dict[str, Any]] = []
    publications: dict[str, dict[str, Any]] = {}
    by_engine: dict[str, str] = {}
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
    ) in rows:
        if engine in by_engine and by_engine[engine] != root:
            raise MarketForgeError(f"Ambiguous engine publication ID {engine}.")
        by_engine[engine] = root
        if root not in publications:
            publications[root] = await asyncio.to_thread(verify, paths, Path(root))
        evidence = publications[root]
        manifest = evidence["manifest"]
        if (
            evidence["manifest_sha256"] != manifest_sha
            or evidence["instrument_namespace"] != namespace
            or str(manifest["dataset_id"]) != engine
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
        selected_streams: set[str] = (
            {
                stream["stream_id"]
                for stream in manifest["streams"]
                if any(r[7] == root and str(Path(r[2]).parent) == stream["directory"] for r in rows)
            }
            if manifest["streams"] and "directory" in manifest["streams"][0]
            else set()
        )
        if reason := layout_reason(manifest):
            raise MarketForgeError(f"Dataset {db_id}: {reason}")
        streams = evidence["resolved_job"]["streams"]
        if len(selected_streams) == 1 and manifest["content_type"] == "trades":
            stream_id = next(iter(selected_streams))
        elif len(streams) == 1 and len(manifest["streams"]) <= 1:
            stream_id = streams[0]["stream_id"]
        else:
            raise MarketForgeError(
                f"Dataset {db_id} belongs to a multi-stream selection unsupported by the current Rust merger. Select one concrete trade stream per publication."
            )
        if manifest["content_type"] == "depth":
            siblings = {r[2] for r in rows if r[7] == root}
            missing = [f["path"] for f in manifest["files"] if f["path"] not in siblings]
            if missing:
                raise MarketForgeError(
                    f"Depth reconstruction in publication {engine} requires explicit selection of all primary files. Missing {missing}."
                )
        result.append(
            {
                "database_id": db_id,
                "engine_id": int(engine),
                "namespace": namespace,
                "path": root,
                "primary_file": relative,
                "file_sha256": sha,
                "manifest_sha256": manifest_sha,
                "start_ns": start,
                "end_ns": end,
                "stream_id": stream_id,
                "integrity_status": manifest["integrity_status"],
                "memberships": summary["memberships"],
            }
        )
    if job is not None and any(
        item["manifest_sha256"] != expected["manifest_sha256"]
        for item, (_, expected) in zip(result, references, strict=True)
    ):
        raise MarketForgeError("Submitted file selection has stale publication fingerprints.")
    if len({item["namespace"] for item in result}) != 1:
        raise MarketForgeError("Select files from a single instrument namespace.")
    return result


def publication_selection(selected: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_engine: dict[int, dict[str, Any]] = {}
    current = None
    for item in selected:
        identity = item["engine_id"]
        if identity in by_engine and current != identity:
            raise MarketForgeError(
                "Arrange files from each publication consecutively to preserve input precedence."
            )
        by_engine.setdefault(identity, item)
        current = identity
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
        observations.setdefault(item["stream_id"], []).append((lo, hi))
        envelopes.setdefault(item["stream_id"], set()).add((item["start_ns"], item["end_ns"]))
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


def validate_job(paths: ProjectPaths, binary: Path, source: Path) -> None:
    response = supervised(
        [str(binary), "--validate-job", str(source), "--project-root", str(paths.root)],
        cwd=paths.root,
    )
    if response.returncode:
        raise MarketForgeError(f"Production merge validation failed: {response.stdout}")


def source_selection(paths: ProjectPaths, recipe: DerivedRecipe) -> dict[str, Any]:
    path, config, fingerprint = normalization_source(paths, recipe.selection.source)
    result = asyncio.run(discover(paths, recipe, path, config))
    selected = set(result["selected_dataset_ids"])
    result["coverage"] = coverage(
        [
            {
                "database_id": item["database_id"],
                "stream_id": member["stream_id"],
                "start_ns": member.get("start_timestamp_ns", item["start_ns"]),
                "end_ns": member.get("end_timestamp_ns", item["end_ns"]),
            }
            for item in result["candidates"]
            if item["database_id"] in selected
            for member in item["memberships"]
            if member["request_indexes"]
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


def prepare(paths: ProjectPaths, recipe: Recipe | DerivedRecipe, name: str) -> dict[str, Any]:
    identity = uuid4().int & (2**63 - 1)
    directory = paths.runs_dir / f"merge-prepare-{identity}"
    result: dict[str, Any] = {}
    if isinstance(recipe, DerivedRecipe):
        result = source_selection(paths, recipe)
        result.update(recipe=name, report_path=str(directory / "prepare-report.json"))
        if result["blockers"]:
            return blocked(directory, result)
        ids = result["selected_dataset_ids"]
    else:
        ids = recipe.dataset_ids
    try:
        return prepare_job(paths, recipe, name, identity, directory, ids, result)
    except (MarketForgeError, OSError, ValueError) as error:
        if not isinstance(recipe, DerivedRecipe):
            raise
        result["blockers"].append(
            {
                "dataset_ids": ids,
                "reason": str(error),
                "suggestion": "Check the selected publications and narrow or correct the recipe before preparing again.",
            }
        )
        return blocked(directory, result)


def blocked(directory: Path, report: dict[str, Any]) -> dict[str, Any]:
    report.update(status="blocked", compatibility="blocked")
    directory.mkdir(parents=True, exist_ok=True)
    write_job(directory / "prepare-report.json", report)
    return report


def prepare_job(
    paths: ProjectPaths,
    recipe: Recipe | DerivedRecipe,
    name: str,
    identity: int,
    directory: Path,
    ids: list[int],
    discovery: dict[str, Any],
) -> dict[str, Any]:
    binary = resolve(paths, "merge")
    selected = asyncio.run(select_inputs(paths, ids=ids))
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
                or str(Path(item["path"]) / item["primary_file"]) != candidate["registered_path"]
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

    selected_coverage = (
        discovery["coverage"] if discovery else coverage(selected, recipe.start_ns, recipe.end_ns)
    )
    runtime = paths.config_dir / "historical/general.toml"
    config_sha = checksum(runtime)
    settings = runtime_settings(paths, binary, runtime)
    if checksum(runtime) != config_sha:
        raise MarketForgeError("Historical runtime configuration changed during preparation.")
    directory.mkdir(parents=True, exist_ok=False)
    source = directory / "merge-job.json"
    publications = publication_selection(selected)
    grouped: dict[str, list[dict[str, str]]] = {}
    for item in selected:
        grouped.setdefault(str(item["engine_id"]), []).append(
            {
                "path": item["primary_file"],
                "sha256": item["file_sha256"],
                "manifest_sha256": item["manifest_sha256"],
            }
        )
    job: dict[str, Any] = {
        "operation": "merge",
        "job_id": identity,
        "inputs": [item["engine_id"] for item in publications],
        "primary_files": grouped,
        "input_paths": {str(item["engine_id"]): item["path"] for item in selected},
        "time_range": {"start_ns": recipe.start_ns, "end_ns": recipe.end_ns},
        **settings,
        "output": {"dataset_path": str(paths.datasets_dir / "merged" / str(identity))},
    }
    candidate = directory / "candidate-job.json"
    write_job(candidate, job)
    try:
        validate_job(paths, binary, candidate)
        candidate.rename(source)
    finally:
        candidate.unlink(missing_ok=True)
    report: dict[str, Any] = {
        **discovery,
        "status": "prepared",
        "compatibility": "validated",
        "recipe": name,
        "job_path": str(source),
        "executable_sha256": checksum(binary),
        "configuration_sha256": config_sha,
        "submitted_sha256": checksum(source),
        "inputs": selected,
        "report_path": str(directory / "prepare-report.json"),
        "coverage": selected_coverage,
    }
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
        ("manifest.json", "merge-job.json", "merge-input-checksums.json", "merge-partitions.json"),
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


def execute(paths: ProjectPaths, source: Path, *, tui: bool = False) -> dict[str, Any]:
    if tui and not all(os.isatty(fd) for fd in (0, 1, 2)):
        raise MarketForgeError("--tui requires an interactive terminal.")
    binary = resolve(paths, "merge")
    directory = paths.runs_dir / f"merge-execute-{uuid4().hex}"
    directory.mkdir(parents=True, exist_ok=False)
    pinned = directory / "marketforge-merge"
    digest = checksum(binary)
    shutil.copyfile(binary, pinned)
    pinned.chmod(0o755)
    if checksum(pinned) != digest:
        raise MarketForgeError("Managed merge executable changed during execution preparation.")
    submission = directory / "submitted-merge.json"
    submission.write_bytes(source.read_bytes())
    validate_job(paths, pinned, submission)
    job = read_json(submission)
    selected = asyncio.run(select_inputs(paths, job=job))
    report = source.parent / "prepare-report.json"
    prepared = read_json(report) if report.is_file() else {}
    if prepared and (
        prepared.get("submitted_sha256") != checksum(submission)
        or prepared.get("executable_sha256") != digest
        or prepared.get("inputs") != selected
        or prepared.get("configuration_sha256")
        != checksum(paths.config_dir / "historical/general.toml")
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
        "environment": "testing",
        "execution_id": directory.name,
        "job_id": job["job_id"],
        "recipe": prepared.get("recipe"),
        "submission_path": str(submission),
        "submitted_sha256": checksum(submission),
        "executable_sha256": digest,
        "configuration_sha256": prepared.get("configuration_sha256"),
        "inputs": selected,
        "coverage": coverage(selected, **job["time_range"]),
        "path": str(output),
        "report_path": str(result_path),
        "execution_receipt": str(intent),
        "engine_result_path": str(engine_result_path),
        "recovery": f"mf merge register {intent} --testing",
        "execution": "running",
        "verification": "not_started",
        "registration": "not_started",
        "counts": {"requested": 1, "successful": 0, "failed": 0, "registered": 0},
    }
    sync_submission(submission)
    write_job(intent, receipt)
    try:
        code, interrupted = run_process(
            [
                str(pinned),
                "--job",
                str(submission),
                "--project-root",
                str(paths.root),
                "--result-file",
                str(engine_result_path),
            ],
            paths.root,
            directory,
            tui=tui,
        )
    except OSError as error:
        receipt.update(status="failed", execution="failed", error=str(error))
        write_job(result_path, receipt)
        return receipt
    receipt.update(
        exit_code=code,
        execution="interrupted" if interrupted else "failed" if code else "complete",
        status="interrupted" if interrupted else "failed",
    )
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
                evidence = verify_output(paths, output, pinned, selected, job)
            except (MarketForgeError, OSError, ValueError, KeyError, TypeError) as error:
                receipt.update(verification="failed", error=str(error))
            else:
                receipt.update(
                    verification="verified",
                    dataset={
                        **evidence["manifest"],
                        "bytes_written": evidence["bytes_written"],
                        "engine_dataset_id": evidence["engine_dataset_id"],
                    },
                )
                receipt["counts"].update(
                    successful=1,
                    verified_publications=1,
                    verified_primary_files=evidence["manifest"]["files_written"],
                )
                result = asyncio.run(
                    register_verified(
                        paths,
                        [evidence],
                        receipt_directory=directory,
                        execution={
                            "execution_id": directory.name,
                            "submitted_sha256": receipt["submitted_sha256"],
                        },
                    )
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
    except KeyboardInterrupt:
        interrupted = True
        receipt.update(status="interrupted")
    if not interrupted and not receipt["counts"]["successful"]:
        receipt["counts"]["failed"] = 1
    write_job(result_path, receipt)
    return receipt


async def register(paths: ProjectPaths, source: Path) -> dict[str, Any]:
    binary = resolve(paths, "merge")
    execution = None
    intent = None
    selected_primary = None
    if source.name == "execution-intent.json":
        intent = read_json(source)
        if intent.get("operation") != "merge" or intent.get("environment") != "testing":
            raise MarketForgeError("Expected a managed merge execution receipt.")
        directory = (await asyncio.to_thread(source.resolve)).parent
        pinned = directory / "marketforge-merge"
        if pinned.is_file() and checksum(pinned) == intent["executable_sha256"]:
            binary = pinned
        submission = directory / "submitted-merge.json"
        if checksum(submission) != intent["submitted_sha256"]:
            raise MarketForgeError("Recovery submission differs from merge execution intent.")
        job = read_json(submission)
        source = Path(job["output"]["dataset_path"])
        execution = {
            "execution_id": intent["execution_id"],
            "submitted_sha256": intent["submitted_sha256"],
        }
    else:
        source, selected_primary = primary_reference(source)
        root = source.parent if source.name == "manifest.json" else source
        job = read_json(root / "merge-job.json")
    selected = await select_inputs(paths, job=job)
    if intent is not None and selected != intent["inputs"]:
        raise MarketForgeError("Recovery inputs differ from merge execution intent.")
    try:
        evidence = await asyncio.to_thread(verify_output, paths, source, binary, selected, job)
    except (ValueError, KeyError, TypeError) as error:
        raise MarketForgeError(f"Invalid merge output evidence: {error}") from error
    if selected_primary is not None:
        evidence["selected_primary"] = selected_primary
    return await register_verified(
        paths,
        [evidence],
        receipt_directory=paths.runs_dir / f"merge-register-{uuid4().hex}",
        execution=execution,
    )
