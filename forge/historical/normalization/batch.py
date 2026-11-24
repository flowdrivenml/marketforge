"""Read-only recipe planning and atomic publication of a Forge batch wrapper."""

import hashlib
import json
import os
import re
import shutil
import tempfile
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, LiteralString, cast
from uuid import uuid4

from psycopg import AsyncConnection
from psycopg.rows import TupleRow

from forge.historical.archives.config import AcquisitionConfig
from forge.historical.archives.selection import (
    ArchiveSelection,
    ArchiveSelectionError,
    request_key,
    revision_time,
)
from forge.historical.engine.managed import checksum
from forge.historical.engine.process import (
    Lifecycle,
    PreparationObserver,
    check,
    ignore_preparation,
    joined_io,
)
from forge.historical.metadata.resolution import (
    FamilyResolutionError,
    compiled_profiles,
    profile_fingerprints,
)
from forge.historical.normalization.config import (
    ExactArchive,
    ExactRecipe,
    NormalizationRecipe,
    NormalizationSource,
    PreparationRequest,
)
from forge.historical.normalization.contract import preparation_document
from forge.historical.normalization.diagnostics import summarize
from forge.historical.normalization.mapping import ResolvedArchive, resolve_archive
from forge.historical.normalization.ranking import RankingSelection, positions_for
from forge.historical.normalization.resolution import require
from forge.historical.normalization.service import default_output_paths, output_paths, write_job
from forge.markets.models import obj
from forge.markets.ranking.service import find_latest_snapshot
from forge.shared.config.models import HttpSettings
from forge.shared.database.connection import (
    database_connection,
    database_dsn,
    database_purpose,
    registry_id,
    require_registry,
)
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths
from forge.storage.coverage import CoverageGroup
from forge.storage.models import StorageFilters, interval
from forge.storage.registry import parameters, scan


def utc_timestamp(value: object) -> str:
    parsed = (
        value
        if isinstance(value, datetime)
        else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    )
    require(parsed.tzinfo is not None, "Coverage timestamp requires an explicit timezone.")
    return parsed.astimezone(UTC).isoformat().replace("+00:00", "Z")


SourceOrder = tuple[datetime, datetime, str, int, str, str]
SourceIdentity = tuple[str, str, str]
SourceGroups = dict[str, dict[tuple[SourceIdentity, str], tuple[SourceOrder, ResolvedArchive]]]


def source_order(row: dict[str, object], path: object) -> SourceOrder:
    bounds, issue = interval(row.get("start_at"), row.get("end_at"))
    if bounds is None:
        raise MarketForgeError(f"Archive {row['id']} has {issue}.")
    provider = str(row.get("raw_source_key") or row.get("external_source_key") or "")
    key = str(row.get("source_object_key") or "")
    # The Bitget adapter retains the exchange's numbered trade part in the
    # registered object key. Parts share the same declared daily coverage.
    part = (
        re.fullmatch(r"history/trades/.+_\d{8}_(\d+)\.zip", key)
        if provider == "marketforge.exchange.bitget"
        else None
    )
    return (*bounds, provider, int(part[1]) if part else 0, key, str(path))


def source_identity(row: dict[str, object]) -> SourceIdentity:
    provider = row.get("raw_source_key") or row.get("external_source_key")
    if provider and row.get("source_object_key"):
        return str(row["origin"]), str(provider), str(row["source_object_key"])
    return str(row["origin"]), "local", str(row["id"])


def dense_ranks(
    streams: Mapping[str, int | None], positions: dict[int, int]
) -> list[dict[str, object]]:
    """Stored sparse positions are ordering evidence, never generated rank values."""
    ordered = sorted(
        streams, key=lambda s: (streams[s] not in positions, positions.get(streams[s] or -1, 0), s)
    )
    return [{"stream_id": stream, "stream_rank": rank} for rank, stream in enumerate(ordered, 1)]


async def ranking_positions(
    c: AsyncConnection[TupleRow], policy: str = "turnover"
) -> tuple[str | None, dict[int, int]]:
    snapshot = await find_latest_snapshot(c, policy)
    return str(snapshot) if snapshot is not None else None, await positions_for(c, snapshot)


async def used_ids(c: AsyncConnection[TupleRow], paths: ProjectPaths) -> set[int]:
    used: set[int] = set()
    checks: tuple[tuple[LiteralString, str], ...] = (
        (
            "SELECT operation_details->>'engine_dataset_id' FROM historical.datasets "
            "WHERE operation_details->>'engine_dataset_id' ~ '^[0-9]+$'",
            "historical.datasets",
        ),
        (
            "SELECT engine_dataset_id FROM historical.dataset_publications WHERE engine_dataset_id ~ '^[0-9]+$'",
            "historical.dataset_publications",
        ),
        ("SELECT id FROM historical.jobs", "historical.jobs"),
    )
    for query, table in checks:
        cur = await c.execute("SELECT to_regclass(%s)", (table,))
        row = await cur.fetchone()
        if row and row[0] is not None:
            cur = await c.execute(query)
            used.update(int(row[0]) for row in await cur.fetchall())
    for directory in (
        paths.normalization_jobs_dir,
        paths.data_root / ".jobs/process",
        paths.runs_dir,
    ):
        for source in directory.rglob("*.json"):
            if source.stem.isdecimal():
                used.add(int(source.stem))
            try:
                loaded: object = json.loads(source.read_text())
                if not isinstance(loaded, dict):
                    continue
                value = cast(dict[str, object], loaded)
                documents = [value]
                if value.get("kind") == "normalization_batch" and isinstance(
                    value.get("jobs"), list
                ):
                    documents.extend(
                        cast(dict[str, object], job)
                        for job in cast(list[object], value["jobs"])
                        if isinstance(job, dict)
                    )
                for document in documents:
                    for key in ("job_id", "dataset_id"):
                        if isinstance(document.get(key), int):
                            used.add(int(str(document[key])))
            except (OSError, ValueError, UnicodeError):
                continue
    return used


def allocate(seed: str, used: set[int], paths: ProjectPaths) -> int:
    attempt = 0
    while True:
        identity = int.from_bytes(
            hashlib.sha256(f"{seed}:{attempt}".encode()).digest()[:8], "big"
        ) & (2**63 - 1)
        output = paths.data_root / "datasets" / "normalized"
        if (
            identity
            and identity not in used
            and not (output / str(identity)).exists()
            and not (output / str(identity)).is_symlink()
            and not (paths.data_root / "test" / "normalized" / str(identity)).exists()
            and not any(output.glob(f".{identity}.staging.*"))
        ):
            used.add(identity)
            return identity
        attempt += 1


@dataclass
class BatchPlan:
    report: dict[str, object]
    batch: dict[str, object]
    jobs: dict[int, dict[str, object]]
    run: Path


async def plan_batch(
    c: AsyncConnection[TupleRow],
    paths: ProjectPaths,
    recipe: AcquisitionConfig | NormalizationRecipe,
    ranking: RankingSelection | None = None,
    *,
    pinned: ArchiveSelection | None = None,
    allocation_seed: str = "",
    profile_root: Path | None = None,
    executable: Path | None = None,
) -> BatchPlan:
    policy = recipe.ranking_policy if isinstance(recipe, NormalizationRecipe) else "turnover"
    snapshot, positions = (
        (
            str(ranking.snapshot) if ranking.snapshot is not None else None,
            await positions_for(c, ranking.snapshot),
        )
        if ranking is not None
        else await ranking_positions(c)
        if policy == "turnover"
        else await ranking_positions(c, policy)
    )
    used = await used_ids(c, paths)
    seed = recipe.model_dump_json() + allocation_seed
    groups: SourceGroups = {}
    requests: list[dict[str, object]] = []
    blocked = False
    for index, request in enumerate(recipe.requests, 1):
        source_key = request.source_key if isinstance(request, NormalizationSource) else None
        filters = StorageFilters(
            exchange=request.exchange,
            market=request.market_category,
            instrument_type=request.instrument_type,
            instrument=request.scope.identifier,
            scope_kind=request.scope.kind,
            data_type=request.data_type,
            status="complete",
            origin="external" if source_key else None,
            raw_source_key=source_key,
            feed=(request.source_options.feed if request.source_options else "incremental")
            if request.exchange == "gateio" and request.data_type == "order_book_l2"
            else None,
            start=recipe.interval.start,
            end=recipe.interval.end,
        )
        rows: list[dict[str, object]] = []
        if pinned is None:
            after = 0
            while page := await scan(
                c, parameters(paths.raw_dir, filters, include_unusable=True), after
            ):
                rows.extend(page)
                after = max(int(str(row["id"])) for row in page)
        else:
            wanted = {
                pin.local_archive_id: pin
                for pin in pinned.archives
                if request_key(pin.request) == request_key(request)
            }
            if not wanted:
                raise ArchiveSelectionError(f"No acquired archives for request {index}.")
            # A discovery request can select native child scopes, such as OKX
            # option families under a base coin. The acquisition receipt binds
            # each child to its requested scope. The registry row owns actual
            # content identity for normalization resolution.
            pinned_filters = filters.model_copy(
                update={"instrument": None, "scope_kind": None, "origin": "remote"}
            )
            for local_id, pin in sorted(wanted.items()):
                found = await scan(
                    c,
                    parameters(
                        paths.raw_dir, pinned_filters, archive_id=local_id, include_unusable=True
                    ),
                    0,
                )
                if len(found) != 1:
                    raise ArchiveSelectionError(
                        f"Pinned archive {local_id} is unavailable for request {index}."
                    )
                row = found[0]
                modified = row.get("source_last_modified")
                if (
                    row["relative_path"] != pin.relative_path
                    or row["sha256"] != pin.sha256
                    or row.get("remote_archive_id") != pin.remote_archive_id
                    or row.get("source_object_key") != pin.source_object_key
                    or row.get("source_etag") != pin.source_etag
                    or revision_time(modified if isinstance(modified, datetime) else None)
                    != pin.source_last_modified
                ):
                    raise ArchiveSelectionError(
                        f"Pinned archive {pin.local_archive_id} changed after acquisition."
                    )
                rows.append(row)
        coverage = CoverageGroup({})
        diagnostics: list[dict[str, object]] = []
        selected: list[dict[str, object]] = []
        rows.sort(key=lambda row: int(str(row["id"])))
        for row in rows:
            coverage_row = row
            content: Literal["trades", "depth"] = (
                "trades" if request.data_type == "trades" else "depth"
            )
            try:
                order = source_order(row, row.get("relative_path"))
                if profile_root is None and executable is None:
                    resolved = await resolve_archive(c, paths, row, content)
                else:
                    resolved = await resolve_archive(
                        c, paths, row, content, profile_root=profile_root, executable=executable
                    )
                task = resolved.task
                stream = str(task["stream_id"])
                bounds = order[:2]
                source = source_identity(row)
                group = groups.setdefault(content, {})
                previous_order, previous = group.setdefault((source, stream), (order, resolved))
                require(
                    previous.task == task and previous_order == order,
                    f"Conflicting metadata for source {source} in stream {stream}.",
                )
                coverage_row = {
                    **row,
                    **{
                        k: task[k]
                        for k in ("base_asset", "quote_asset")
                        if row.get(k) is None and k in task
                    },
                }
                selected.append(
                    {
                        "local_id": row["id"],
                        "remote_archive_id": row.get("remote_archive_id"),
                        "external_archive_id": row.get("external_archive_id"),
                        "instrument_id": resolved.instrument_id,
                        "stream_id": stream,
                        "metadata_warnings": resolved.warnings,
                        "identity_resolution": resolved.identity_resolution,
                        "registered_input": asdict(resolved.selection)
                        if resolved.selection
                        else None,
                        "start": str(row["start_at"]),
                        "end_exclusive": str(row["end_at"]),
                        "extends_requested_interval": bounds[0] < recipe.interval.start
                        or bounds[1] > recipe.interval.end,
                    }
                )
            except (
                MarketForgeError,
                OSError,
                ValueError,
            ) as exc:
                blocked = True
                message = str(exc)
                diagnostic: dict[str, object] = {"local_id": row["id"], "reason": message}
                if isinstance(exc, FamilyResolutionError):
                    diagnostic["family_population"] = exc.population
                diagnostics.append(diagnostic)
            coverage.add(coverage_row)
        coverage_doc = coverage.document(filters, include_gaps=True)
        if coverage_doc["gap_count"]:
            blocked = True
        requests.append(
            {
                "request_index": index,
                "request": request.model_dump(mode="json"),
                "matched_archives": [
                    {
                        "local_id": row["id"],
                        "start": str(row["start_at"]),
                        "end_exclusive": str(row["end_at"]),
                        "feed": row["feed"],
                        "feed_evidence": row.get("feed_source"),
                    }
                    for row in rows
                ],
                "selected_archives": selected,
                "coverage": coverage_doc,
                "blockers": diagnostics,
            }
        )
    return build_plan(
        paths,
        seed,
        {"start": utc_timestamp(recipe.interval.start), "end": utc_timestamp(recipe.interval.end)},
        groups,
        requests,
        blocked,
        snapshot,
        positions,
        used,
        ranking=ranking,
        policy=policy,
    )


def build_plan(
    paths: ProjectPaths,
    seed: str,
    requested_interval: object,
    groups: SourceGroups,
    requests: list[dict[str, object]],
    blocked: bool,
    snapshot: str | None,
    positions: dict[int, int],
    used: set[int],
    manual: PreparationRequest | None = None,
    ranking: RankingSelection | None = None,
    policy: str = "turnover",
) -> BatchPlan:
    jobs: dict[int, dict[str, object]] = {}
    summaries: list[dict[str, object]] = []
    matched_stream_count = 0
    unmatched_stream_count = 0
    for content in ("trades", "depth"):
        resolved_inputs = groups.get(content, {})
        if not resolved_inputs:
            continue
        # Rust derives source_priority from the serialized position in each job.
        ordered = sorted(
            resolved_inputs.values(), key=lambda item: (str(item[1].task["stream_id"]), item[0])
        )
        job_id = manual.job_id if manual else allocate(f"{seed}:{content}:job", used, paths)
        dataset_id = (
            manual.dataset_id if manual else allocate(f"{seed}:{content}:dataset", used, paths)
        )
        tasks = [{**resolved.task, "task_id": i} for i, (_, resolved) in enumerate(ordered, 1)]
        staging, dataset = (
            output_paths(paths, manual)
            if manual
            else default_output_paths(paths, job_id, dataset_id)
        )
        streams = {
            str(resolved.task["stream_id"]): resolved.instrument_id for _, resolved in ordered
        }
        matched_stream_count += sum(identity in positions for identity in streams.values())
        unmatched_stream_count += sum(identity not in positions for identity in streams.values())
        ranks = dense_ranks(streams, positions)
        rank_by_stream = {str(item["stream_id"]): int(str(item["stream_rank"])) for item in ranks}
        tasks.sort(
            key=lambda task: (rank_by_stream[str(task["stream_id"])], int(str(task["task_id"])))
        )
        for task_id, task in enumerate(tasks, 1):
            task["task_id"] = task_id
            task["stream_rank"] = rank_by_stream[str(task["stream_id"])]
        document = preparation_document(job_id, dataset_id, content, tasks, staging, dataset)
        jobs[job_id] = document
        summaries.append(
            {
                "job_id": job_id,
                "dataset_id": dataset_id,
                "content_type": content,
                "task_count": len(tasks),
                "stream_count": len(ranks),
                "staging_path": str(staging),
                "dataset_path": str(dataset),
            }
        )
    run_id = (
        "normalize-" + hashlib.sha256(json.dumps(jobs, sort_keys=True).encode()).hexdigest()[:20]
    )
    run = paths.runs_dir / run_id
    suffix = 0
    while run.exists() or run.is_symlink():
        suffix += 1
        run = paths.runs_dir / f"{run_id}-{suffix}"
    run_id = run.name
    executable = paths.bin_dir / "marketforge-normalize"
    instrument_definitions: dict[str, dict[str, object]] = {}
    for content_inputs in groups.values():
        for _, resolved in content_inputs.values():
            for definition in resolved.offline_instruments:
                previous = instrument_definitions.setdefault(
                    str(definition["instrument_id"]), definition
                )
                require(
                    previous == definition,
                    "Conflicting family instrument specification evidence; split the requested intervals.",
                )
    for job in jobs.values():
        for task in cast(list[dict[str, object]], job["tasks"]):
            if task.get("instrument_id") is None:
                continue
            definition = {
                key: task[key]
                for key in (
                    "instrument_id",
                    "exchange",
                    "instrument_type",
                    "market_category",
                    "symbol",
                    "base_asset",
                    "quote_asset",
                    "instrument",
                )
            }
            previous = instrument_definitions.setdefault(str(task["instrument_id"]), definition)
            require(
                previous == definition,
                "A selected instrument crosses conflicting specification/identity evidence. Split normalization into historically applicable intervals.",
            )
    batch: dict[str, object] = {
        "schema_version": 4,
        "kind": "normalization_batch",
        "status": "partial" if blocked else "prepared",
        "run_id": run_id,
        "run_directory": str(run),
        "batch_path": str(run / "normalize-batch.json"),
        "ranking_snapshot": snapshot,
        "ranking_policy": ranking.details["policy"] if ranking else policy,
        "requested_interval": requested_interval,
        "instrument_namespace": "marketforge:testing:historical:v1",
        "instruments": [
            instrument_definitions[key] for key in sorted(instrument_definitions, key=int)
        ],
        "families": [],
        "jobs": list(jobs.values()),
    }
    report: dict[str, object] = {
        **{
            key: value
            for key, value in batch.items()
            if key not in {"jobs", "schema_version", "kind"}
        },
        "schema_version": 1,
        "kind": "normalization_prepare_report",
        "batch_schema_version": batch["schema_version"],
        "diagnostic_summary": summarize(requests),
        "report_path": str(run / "prepare-report.json"),
        "job_summaries": summaries,
        "prepared_job_count": len(jobs),
        "prepared_task_count": sum(int(str(item["task_count"])) for item in summaries),
        "prepared_stream_count": sum(int(str(item["stream_count"])) for item in summaries),
        "requests": requests,
        "ranking_resolution": {
            "snapshot": snapshot,
            "policy": policy,
            "fallback": snapshot is None,
            "reason": "No published turnover snapshot; deterministic stream identity ordering."
            if snapshot is None
            else "Published turnover positions; identity ordering for ties and unranked streams.",
        },
        "archive_content_resolution": "Family selections use persisted archive identity evidence. Rust resolves concrete archive bindings and decodes the sources.",
        "family_rank_semantics": "Ranks identify requested source streams; engine expansion must define concrete stream ranks.",
        "identifiers": "Collision-checked, unreserved testing identifiers; no database registration.",
        "engine_binary_available": executable.is_file() and os.access(executable, os.X_OK),
        "engine_ready": False,
        "engine_compatibility": {
            "direct_execution_supported": True,
            "current_rust_protocol_version": 4,
            "validation": "pending production parser validation",
        },
        "execution": (
            "Engine execution did not start. Resolve preparation blockers and prepare again."
            if blocked
            else "Not executed; use mf normalize run on the saved JSON."
        ),
    }
    if blocked:
        report["failed_stage"] = "Preparation"
    if ranking is not None:
        report["ranking_resolution"] = {
            **ranking.details,
            "matched_stream_count": matched_stream_count,
            "unmatched_stream_count": unmatched_stream_count,
        }
        if ranking.snapshot is None:
            report["ranking_pending"] = True
    return BatchPlan(report, batch, jobs, run)


def exact_archive_evidence(row: dict[str, object], resolved: ResolvedArchive) -> dict[str, object]:
    return {
        "archive": asdict(resolved.selection) if resolved.selection else None,
        "revision": {
            key: row.get(key)
            for key in (
                "remote_archive_id",
                "external_archive_id",
                "source_object_key",
                "source_etag",
                "source_last_modified",
                "data_type",
            )
        },
        "task": resolved.task,
        "instrument_definitions": resolved.offline_instruments,
        "identity_resolution": resolved.identity_resolution,
    }


def exact_archive_sha256(evidence: dict[str, object]) -> str:
    return hashlib.sha256(json.dumps(evidence, sort_keys=True, default=str).encode()).hexdigest()


async def resolve_exact_archive(
    c: AsyncConnection[TupleRow],
    paths: ProjectPaths,
    archive_id: int,
    *,
    profile_root: Path | None = None,
    executable: Path | None = None,
) -> tuple[str, str, SourceOrder, ResolvedArchive, dict[str, object]]:
    rows = await scan(c, parameters(paths.raw_dir, StorageFilters(), archive_id), 0)
    if len(rows) != 1:
        raise MarketForgeError(f"Local archive {archive_id} is not registered in this raw root.")
    row = rows[0]
    kind = {"trades": "trades", "order_book_l2": "depth"}.get(str(row.get("data_type")))
    if kind is None:
        raise MarketForgeError(f"Unsupported normalization data kind for archive {archive_id}.")
    resolved = await resolve_archive(
        c, paths, row, kind, profile_root=profile_root, executable=executable
    )
    profile = (
        ".".join(str(row[key]) for key in ("exchange", "instrument_type", "market_category"))
        + "."
        + kind
    )
    return kind, profile, source_order(row, row["relative_path"]), resolved, row


async def inspect_exact_archives(
    paths: ProjectPaths, archive_ids: tuple[int, ...]
) -> tuple[str, list[dict[str, object]]]:
    async with database_connection(database_dsn(paths.root)) as c, c.transaction():
        await c.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        registry = await registry_id(c)
        fingerprints = await profile_fingerprints(paths, None, None)
        results: list[dict[str, object]] = []
        for archive_id in archive_ids:
            _, profile, _, resolved, row = await resolve_exact_archive(c, paths, archive_id)
            if profile not in fingerprints:
                raise MarketForgeError(
                    f"Missing compatible profile {profile} for archive {archive_id}."
                )
            evidence = exact_archive_evidence(row, resolved)
            results.append(
                {
                    "archive_id": archive_id,
                    "selection_sha256": exact_archive_sha256(evidence),
                    "profile_id": profile,
                    "profile_sha256": fingerprints[profile],
                    "source": evidence["archive"],
                }
            )
        return registry, results


async def plan_manual_batch(
    c: AsyncConnection[TupleRow],
    paths: ProjectPaths,
    request: PreparationRequest | ExactRecipe,
    ranking: RankingSelection | None = None,
    *,
    profile_root: Path | None = None,
    executable: Path | None = None,
) -> BatchPlan:
    """Read legacy selections, but publish only the current Forge representation."""
    policy = request.ranking_policy
    snapshot, positions = (
        (
            str(ranking.snapshot) if ranking.snapshot is not None else None,
            await positions_for(c, ranking.snapshot),
        )
        if ranking is not None
        else await ranking_positions(c)
        if policy == "turnover"
        else await ranking_positions(c, policy)
    )
    used = await used_ids(c, paths)
    exact = isinstance(request, ExactRecipe)
    if exact:
        require_registry({"registry_id": str(request.registry_id)}, await registry_id(c))
        fingerprints = await profile_fingerprints(paths, profile_root, executable)
    else:
        require(
            request.job_id not in used and request.dataset_id not in used,
            "Job or dataset ID already exists in registered or prepared artifacts.",
        )
        fingerprints = {}
    groups: SourceGroups = {}
    outcomes: list[dict[str, object]] = []
    blocked = False
    bounds: list[tuple[str, str]] = []
    expected_raw: dict[str, tuple[int, str]] = {}
    exact_profiles: dict[str, str] = {}
    for index, selection in enumerate(request.archive_pins if exact else request.inputs, 1):
        diagnostics: list[dict[str, object]] = []
        selected: list[dict[str, object]] = []
        profile = ""
        try:
            if isinstance(selection, ExactArchive):
                content, profile, order, resolved, row = await resolve_exact_archive(
                    c, paths, selection.archive_id, profile_root=profile_root, executable=executable
                )
                evidence = exact_archive_evidence(row, resolved)
                require(
                    exact_archive_sha256(evidence) == selection.selection_sha256,
                    "The exact archive revision or its resolved instrument evidence has changed.",
                )
                require(profile in fingerprints, f"Missing compatible profile {profile}.")
                exact_profiles[profile] = fingerprints[profile]
                if selection.profile_id is not None:
                    require(
                        selection.profile_id == profile,
                        f"Incompatible profile {selection.profile_id}.",
                    )
                    require(
                        selection.profile_sha256 == fingerprints[profile],
                        f"Profile {profile} has changed.",
                    )
                path = str(resolved.task["input_path"])
                pin = (selection.archive_id, str(row["sha256"]))
                require(
                    expected_raw.setdefault(path, pin) == pin,
                    "Conflicting archive IDs for one managed path.",
                )
            else:
                assert isinstance(request, PreparationRequest)
                content = request.content_type
                rows = await scan(
                    c, parameters(paths.raw_dir, StorageFilters(), selection.archive_id), 0
                )
                require(
                    len(rows) == 1,
                    f"Local archive {selection.archive_id} is not registered in this raw root.",
                )
                row = rows[0]
                order = source_order(row, row.get("relative_path"))
                resolved = await resolve_archive(c, paths, row, content)
                require(
                    resolved.instrument_id == selection.instrument_id,
                    "Selected instrument differs from archive identity.",
                )
            bounds.append((utc_timestamp(order[0]), utc_timestamp(order[1])))
            stream = str(resolved.task["stream_id"])
            source = source_identity(row)
            previous_order, previous = groups.setdefault(content, {}).setdefault(
                (source, stream), (order, resolved)
            )
            require(
                previous.task == resolved.task and previous_order == order,
                f"Conflicting metadata for source {source}.",
            )
            selected.append(
                {
                    "local_id": selection.archive_id,
                    "instrument_id": resolved.instrument_id,
                    "stream_id": stream,
                    "metadata_warnings": resolved.warnings,
                    "identity_resolution": resolved.identity_resolution,
                    **(
                        {
                            "registered_input": asdict(resolved.selection),
                            "profile": profile,
                            "profile_selection": "explicit"
                            if selection.profile_id
                            else "automatic",
                        }
                        if isinstance(selection, ExactArchive) and resolved.selection
                        else {}
                    ),
                }
            )
        except (MarketForgeError, OSError, ValueError) as exc:
            blocked = True
            diagnostic: dict[str, object] = {"local_id": selection.archive_id, "reason": str(exc)}
            if isinstance(exc, FamilyResolutionError):
                diagnostic["family_population"] = exc.population
            diagnostics.append(diagnostic)
        outcomes.append(
            {
                "request_index": index,
                "selected_archives": selected,
                "blockers": diagnostics,
                (
                    "archive_pin" if isinstance(selection, ExactArchive) else "legacy_selection"
                ): selection.model_dump(mode="json"),
            }
        )
    plan = build_plan(
        paths,
        request.model_dump_json(),
        {
            "start": min(item[0] for item in bounds),
            "end": max(item[1] for item in bounds),
        }
        if bounds
        else None,
        groups,
        outcomes,
        blocked,
        snapshot,
        positions,
        used,
        None if exact else request,
        ranking,
        policy,
    )
    if exact:
        plan.report["exact_raw_inputs"] = expected_raw
        plan.report["exact_profile_fingerprints"] = exact_profiles
    return plan


def blocked_execution(report: Mapping[str, object], *, recipe: str) -> dict[str, object]:
    """Project an unexecuted preparation result into a recipe run outcome."""
    state = report["status"]
    return {
        **report,
        "recipe": recipe,
        "preparation_status": state,
        "status": "failed" if state in {"failed", "invalid"} else "blocked",
        "failed_stage": report.get("failed_stage", "Preparation"),
        "execution": "Engine execution did not start.",
    }


async def prepare_batch(
    paths: ProjectPaths,
    recipe: AcquisitionConfig | NormalizationRecipe | PreparationRequest | ExactRecipe,
    *,
    dry_run: bool = False,
    recipe_source: Path | None = None,
    http: HttpSettings | None = None,
    pinned: ArchiveSelection | None = None,
    allocation_seed: str = "",
    historical: Path | None = None,
    executable: Path | None = None,
    lifecycle: Lifecycle = lambda _: None,
    on_candidate: PreparationObserver = ignore_preparation,
    supervised_validation: bool = False,
) -> dict[str, object]:
    dsn = database_dsn(paths.root)
    async with database_connection(dsn) as c:
        async with c.transaction():
            await c.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            if isinstance(recipe, ExactRecipe):
                plan = await plan_manual_batch(
                    c, paths, recipe, profile_root=historical, executable=executable
                )
            elif isinstance(recipe, PreparationRequest):
                plan = await plan_manual_batch(c, paths, recipe)
            elif pinned is None and not allocation_seed:
                plan = await plan_batch(c, paths, recipe)
            else:
                plan = await plan_batch(
                    c,
                    paths,
                    recipe,
                    pinned=pinned,
                    allocation_seed=allocation_seed,
                    profile_root=historical,
                    executable=executable,
                )
        report = cast(dict[str, object], json.loads(json.dumps(plan.report, default=str)))
        if not dry_run:
            from forge.orchestration.registry import recipe_evidence

            if recipe_snapshot := recipe_evidence():
                report["recipe_evidence"] = recipe_snapshot
        report["dry_run"] = dry_run
        if report.get("status") == "prepared" and pinned is None:
            from forge.historical.engine.managed import resolve
            from forge.historical.normalization.execution import add_reused, preview_reuse

            try:
                engine = executable or resolve(paths)
            except MarketForgeError:
                engine = None
            if engine is not None:
                try:
                    registry = await registry_id(c)
                    states, ready = await preview_reuse(
                        paths,
                        cast(dict[str, Any], plan.batch),
                        registry,
                        binary=engine,
                        historical=historical,
                        expected_raw=cast(
                            dict[str, tuple[int, str]] | None, plan.report.get("exact_raw_inputs")
                        ),
                    )
                except (MarketForgeError, OSError, ValueError) as error:
                    report["reuse_plan_error"] = str(error)
                else:
                    report["reuse_plan"] = states
                    if ready and len(ready) == len(plan.jobs) and not dry_run:
                        result: dict[str, Any] = {
                            "status": "complete",
                            "operation": "normalize",
                            "environment": database_purpose(),
                            "registry_id": registry,
                            "execution_id": plan.run.name,
                            "requested_jobs": plan.batch["jobs"],
                            "requested_interval": plan.batch["requested_interval"],
                            "recipe_source": str(await joined_io(recipe_source.resolve))
                            if recipe_source
                            else None,
                            "requests": report["requests"],
                            "ranking_policy": report["ranking_policy"],
                            "engine_execution_skipped": True,
                            "normalization": "not_started",
                            "verification": "verified",
                            "registration": "registered",
                            "jobs": [],
                            "counts": {"requested": 0, "successful": 0, "failed": 0},
                            "reused_outputs": ready,
                            "reuse_plan": states,
                        }
                        add_reused(result, ready)
                        result["report_path"] = str(plan.run / "reuse-result.json")
                        write_job(Path(result["report_path"]), result)
                        return result
        if not dry_run:
            report.update(environment=database_purpose(), registry_id=await registry_id(c))
            if historical is None and executable is None and not supervised_validation:
                publish_batch(paths, plan, report)
            else:
                await publish_batch_async(
                    paths,
                    plan,
                    report,
                    historical,
                    executable,
                    lifecycle=lifecycle,
                    on_candidate=on_candidate,
                )
        return report


def stage_batch(
    paths: ProjectPaths,
    plan: BatchPlan,
    report: dict[str, object],
    directory: Path,
    historical: Path | None,
    executable: Path | None,
) -> list[str] | None:
    from forge.historical.engine.managed import resolve
    from forge.historical.normalization.execution import profile_inventory

    for document in plan.jobs.values():
        output = obj(document["output"])
        for key in ("staging_path", "dataset_path"):
            target = Path(str(output[key]))
            require(
                not target.exists() and not target.is_symlink(), f"Output already exists: {target}"
            )
    source = directory / "normalize-batch.json"
    write_job(source, plan.batch)
    profile_root = directory / "historical"
    profile_root.mkdir()
    (profile_root / "profiles").mkdir()
    profiles = profile_inventory(paths, historical)
    for item in profiles:
        original = Path(item["location"])
        target = profile_root / "profiles" / item["id"].rsplit(".", 1)[1] / original.name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(original, target)
        require(checksum(target) == item["sha256"], f"Profile changed while pinning: {original}")
    report["profile_evidence"] = [{"id": item["id"], "sha256": item["sha256"]} for item in profiles]
    origin = paths.runtime_config("normalize", historical)
    config = directory / origin.name
    shutil.copyfile(origin, config)
    report["configuration_origin"] = str(origin)
    report["configuration_filename"] = config.name
    report["configuration_sha256"] = checksum(config)
    report["submitted_sha256"] = checksum(source)
    if plan.batch["status"] != "prepared":
        return None
    try:
        binary = executable or resolve(paths)
    except MarketForgeError as error:
        report.update(
            status="blocked",
            failed_stage="Engine binary resolution",
            engine_binary_available=False,
            error=str(error),
        )
        return None
    report["executable_sha256"] = checksum(binary)
    if expected := report.get("exact_profile_fingerprints"):
        pinned = compiled_profiles(
            paths,
            tuple((item["id"], item["sha256"]) for item in profiles),
            profile_root,
            binary,
        )
        require(
            all(pinned.get(name) == digest for name, digest in obj(expected).items()),
            "An exact archive profile changed during preparation. Review the selection again.",
        )
    return batch_validation_command(paths, binary, source, config, profile_root)


def batch_validation_command(
    paths: ProjectPaths, binary: Path, source: Path, config: Path, profile_root: Path
) -> list[str]:
    return [
        str(binary),
        "--validate-batch",
        str(source),
        "--project-root",
        str(paths.root),
        "--config",
        str(config),
        "--profiles-root",
        str(profile_root),
    ]


def record_batch_validation(report: dict[str, object], output: str) -> None:
    report["engine_validation"] = json.loads(output)
    report["engine_ready"] = True
    report["engine_compatibility"] = {
        **obj(report.get("engine_compatibility", {})),
        "validation": "passed production batch parser",
    }


def publish_batch(paths: ProjectPaths, plan: BatchPlan, report: dict[str, object]) -> None:
    from forge.historical.engine.managed import supervised

    paths.runs_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".prepare-normalize-", dir=paths.runs_dir) as temporary:
        directory = Path(temporary)
        argv = stage_batch(paths, plan, report, directory, None, None)
        if argv is not None:
            response = supervised(argv, cwd=paths.root)
            if response.returncode:
                report.update(
                    status="invalid",
                    failed_stage="Engine batch validation",
                    validation_error=response.stdout,
                )
            else:
                record_batch_validation(report, response.stdout)
        write_job(directory / "prepare-report.json", report)
        require(
            not plan.run.exists() and not plan.run.is_symlink(), f"Batch already exists: {plan.run}"
        )
        directory.rename(plan.run)


async def validate_candidate(
    paths: ProjectPaths,
    source: Path,
    report: dict[str, object],
    historical: Path | None,
    executable: Path,
    *,
    lifecycle: Lifecycle,
) -> dict[str, object]:
    from forge.historical.normalization.execution import profile_inventory

    profiles = source.parent / "historical"
    if "configuration_origin" in report:
        config = source.parent / str(report.get("configuration_filename", "general.toml"))
    else:
        config = paths.runtime_config("normalize", historical)
        profiles = historical or paths.engine_config_dir / "normalize"
    require(
        report["profile_evidence"]
        == [
            {"id": item["id"], "sha256": item["sha256"]}
            for item in profile_inventory(paths, profiles)
        ],
        "Retained normalization profiles changed before validation.",
    )
    require(
        checksum(executable) == report["executable_sha256"]
        and checksum(config) == report["configuration_sha256"]
        and checksum(source) == report["submitted_sha256"],
        "Retained normalization preparation changed before validation.",
    )
    directory = source.parent / f"validation-{uuid4().hex}"
    directory.mkdir()
    lifecycle("validate")
    response = await check(
        batch_validation_command(paths, executable, source, config, profiles),
        paths.root,
        directory,
        "validation",
    )
    if response.returncode:
        report.update(
            status="invalid",
            failed_stage="Engine batch validation",
            validation_error=response.stdout,
            report_path=str(directory / "prepare-report.json"),
        )
    else:
        record_batch_validation(report, response.stdout)
        report["report_path"] = str(source.parent / "prepare-report.json")
    report["validation_logs"] = str(directory)
    write_job(Path(str(report["report_path"])), report)
    return report


async def publish_batch_async(
    paths: ProjectPaths,
    plan: BatchPlan,
    report: dict[str, object],
    historical: Path | None,
    executable: Path | None,
    *,
    lifecycle: Lifecycle,
    on_candidate: PreparationObserver,
) -> None:
    plan.run.mkdir(parents=True, exist_ok=False)
    report["engine_ready"] = False
    report.pop("engine_validation", None)
    argv = await joined_io(
        lambda: stage_batch(paths, plan, report, plan.run, historical, executable)
    )
    if argv is None:
        write_job(plan.run / "prepare-report.json", report)
        return
    candidate = plan.run / "prepare-candidate.json"
    write_job(candidate, report)
    source = plan.run / "normalize-batch.json"
    on_candidate(source, candidate)
    await validate_candidate(paths, source, report, historical, Path(argv[0]), lifecycle=lifecycle)
