"""Read-only recipe planning and atomic publication of a Forge batch wrapper."""

import hashlib
import json
import os
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, LiteralString, cast

from psycopg import AsyncConnection
from psycopg.rows import TupleRow

from forge.historical.archives.config import AcquisitionConfig
from forge.historical.normalization.config import NormalizationRecipe, PreparationRequest
from forge.historical.normalization.contract import preparation_document
from forge.historical.normalization.mapping import ResolvedArchive, resolve_archive
from forge.historical.normalization.ranking import RankingSelection, ensure_ranking, positions_for
from forge.historical.normalization.resolution import require
from forge.historical.normalization.service import default_output_paths, output_paths, write_job
from forge.markets.ranking.coordination import ranking_lock
from forge.markets.ranking.service import find_latest_snapshot
from forge.shared.config.models import HttpSettings
from forge.shared.database.testing import testing_connection, testing_dsn
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths
from forge.storage.coverage import CoverageGroup
from forge.storage.models import StorageFilters, interval
from forge.storage.registry import parameters, scan


def dense_ranks(
    streams: Mapping[str, int | None], positions: dict[int, int]
) -> list[dict[str, object]]:
    """Stored sparse positions are ordering evidence, never generated rank values."""
    ordered = sorted(
        streams, key=lambda s: (streams[s] not in positions, positions.get(streams[s] or -1, 0), s)
    )
    return [{"stream_id": stream, "stream_rank": rank} for rank, stream in enumerate(ordered, 1)]


async def ranking_positions(c: AsyncConnection[TupleRow]) -> tuple[str | None, dict[int, int]]:
    snapshot = await find_latest_snapshot(c, "turnover")
    return str(snapshot) if snapshot is not None else None, await positions_for(c, snapshot)


async def used_ids(c: AsyncConnection[TupleRow], paths: ProjectPaths) -> set[int]:
    used: set[int] = set()
    checks: tuple[tuple[LiteralString, str], ...] = (
        ("SELECT id FROM historical.datasets", "historical.datasets"),
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
        output = paths.data_root / "test/normalized"
        if (
            identity
            and identity not in used
            and not (output / str(identity)).exists()
            and not (output / str(identity)).is_symlink()
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
    recipe: AcquisitionConfig,
    ranking: RankingSelection | None = None,
) -> BatchPlan:
    snapshot, positions = (
        (
            str(ranking.snapshot) if ranking.snapshot is not None else None,
            await positions_for(c, ranking.snapshot),
        )
        if ranking is not None
        else await ranking_positions(c)
    )
    used = await used_ids(c, paths)
    seed = recipe.model_dump_json()
    groups: dict[str, dict[tuple[str, str], ResolvedArchive]] = {}
    requests: list[dict[str, object]] = []
    blocked = False
    for index, request in enumerate(recipe.requests, 1):
        filters = StorageFilters(
            exchange=request.exchange,
            market=request.market_category,
            instrument_type=request.instrument_type,
            instrument=request.scope.identifier,
            scope_kind=request.scope.kind,
            data_type=request.data_type,
            status="complete",
            feed=(request.source_options.feed if request.source_options else "incremental")
            if request.exchange == "gateio" and request.data_type == "order_book_l2"
            else None,
            start=recipe.interval.start,
            end=recipe.interval.end,
        )
        rows: list[dict[str, object]] = []
        after = 0
        while page := await scan(c, parameters(paths.raw_dir, filters), after):
            rows.extend(page)
            after = int(str(page[-1]["id"]))
        coverage = CoverageGroup({})
        for row in rows:
            coverage.add(row)
        coverage_doc = coverage.document(filters, include_gaps=True)
        diagnostics: list[dict[str, object]] = []
        selected: list[dict[str, object]] = []
        if coverage_doc["gap_count"]:
            blocked = True
        for row in sorted(
            rows, key=lambda r: (str(r["start_at"]), str(r["end_at"]), int(str(r["id"])))
        ):
            content: Literal["trades", "depth"] = (
                "trades" if request.data_type == "trades" else "depth"
            )
            try:
                resolved = await resolve_archive(c, paths, row, content)
                task = resolved.task
                stream = str(task["stream_id"])
                bounds, _ = interval(row["start_at"], row["end_at"])
                assert bounds is not None
                source_identity = f"{row['origin']}:{row['id']}"
                group = groups.setdefault(content, {})
                previous = group.setdefault((source_identity, stream), resolved)
                require(
                    previous.task == task,
                    f"Conflicting metadata for source {source_identity} in stream {stream}.",
                )
                selected.append(
                    {
                        "local_id": row["id"],
                        "remote_archive_id": row.get("remote_archive_id"),
                        "external_archive_id": row.get("external_archive_id"),
                        "instrument_id": resolved.instrument_id,
                        "stream_id": stream,
                        "metadata_warnings": resolved.warnings,
                        "identity_resolution": resolved.identity_resolution,
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
                diagnostics.append({"local_id": row["id"], "reason": message})
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
        recipe.interval.model_dump(mode="json"),
        groups,
        requests,
        blocked,
        snapshot,
        positions,
        used,
        ranking=ranking,
    )


def build_plan(
    paths: ProjectPaths,
    seed: str,
    requested_interval: object,
    groups: dict[str, dict[tuple[str, str], ResolvedArchive]],
    requests: list[dict[str, object]],
    blocked: bool,
    snapshot: str | None,
    positions: dict[int, int],
    used: set[int],
    manual: PreparationRequest | None = None,
    ranking: RankingSelection | None = None,
) -> BatchPlan:
    jobs: dict[int, dict[str, object]] = {}
    summaries: list[dict[str, object]] = []
    matched_stream_count = 0
    unmatched_stream_count = 0
    for content in ("trades", "depth"):
        resolved_inputs = groups.get(content, {})
        if not resolved_inputs:
            continue
        ordered_inputs = sorted(resolved_inputs.items(), key=lambda item: (item[0][1], item[0][0]))
        job_id = manual.job_id if manual else allocate(f"{seed}:{content}:job", used, paths)
        dataset_id = (
            manual.dataset_id if manual else allocate(f"{seed}:{content}:dataset", used, paths)
        )
        tasks = [
            {**resolved.task, "task_id": i} for i, (_, resolved) in enumerate(ordered_inputs, 1)
        ]
        staging, dataset = (
            output_paths(paths, manual)
            if manual
            else default_output_paths(paths, job_id, dataset_id)
        )
        streams = {
            str(resolved.task["stream_id"]): resolved.instrument_id
            for resolved in resolved_inputs.values()
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
    batch: dict[str, object] = {
        "schema_version": 3,
        "kind": "normalization_batch",
        "status": "partial" if blocked else "prepared",
        "run_id": run_id,
        "run_directory": str(run),
        "batch_path": str(run / "normalize-batch.json"),
        "ranking_snapshot": snapshot,
        "ranking_policy": ranking.details["policy"] if ranking else "turnover",
        "requested_interval": requested_interval,
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
        "report_path": str(run / "prepare-report.json"),
        "job_summaries": summaries,
        "prepared_job_count": len(jobs),
        "prepared_task_count": sum(int(str(item["task_count"])) for item in summaries),
        "prepared_stream_count": sum(int(str(item["stream_count"])) for item in summaries),
        "requests": requests,
        "ranking_resolution": {
            "snapshot": snapshot,
            "policy": "turnover",
            "fallback": snapshot is None,
            "reason": "No published turnover snapshot; deterministic stream identity ordering."
            if snapshot is None
            else "Published turnover positions; identity ordering for ties and unranked streams.",
        },
        "archive_content_resolution": "Delegated to engine: format profiles, compression, members and content-to-instrument mappings are not validated by Forge.",
        "family_rank_semantics": "Ranks identify requested source streams; engine expansion must define concrete stream ranks.",
        "identifiers": "Collision-checked, unreserved testing identifiers; no database registration.",
        "engine_binary_available": executable.is_file() and os.access(executable, os.X_OK),
        "engine_ready": False,
        "engine_compatibility": {
            "direct_execution_supported": False,
            "current_rust_protocol_version": 3,
            "reasons": [
                "Rust accepts a single protocol-v3 job, not a Forge batch.",
                "Rust v3 requires job-level streams and rejects task-level stream_rank.",
                "Rust v3 requires format_profile, instrument_id and the older instrument shape; Forge v3 supplies explicit identity or unresolved scopes.",
            ],
        },
        "execution": "Not executed. Forge wrapper is not a Rust --job document; dispatch is deferred.",
    }
    if ranking is not None:
        report["ranking_resolution"] = {
            **ranking.details,
            "matched_stream_count": matched_stream_count,
            "unmatched_stream_count": unmatched_stream_count,
        }
        if ranking.snapshot is None:
            report["status"] = batch["status"] = "partial"
            report["ranking_pending"] = True
    return BatchPlan(report, batch, jobs, run)


async def plan_manual_batch(
    c: AsyncConnection[TupleRow],
    paths: ProjectPaths,
    request: PreparationRequest,
    ranking: RankingSelection | None = None,
) -> BatchPlan:
    """Read legacy selections, but publish only the current Forge representation."""
    snapshot, positions = (
        (
            str(ranking.snapshot) if ranking.snapshot is not None else None,
            await positions_for(c, ranking.snapshot),
        )
        if ranking is not None
        else await ranking_positions(c)
    )
    used = await used_ids(c, paths)
    require(
        request.job_id not in used and request.dataset_id not in used,
        "Job or dataset ID already exists in registered or prepared artifacts.",
    )
    groups: dict[str, dict[tuple[str, str], ResolvedArchive]] = {request.content_type: {}}
    outcomes: list[dict[str, object]] = []
    blocked = False
    for index, selection in enumerate(request.inputs, 1):
        diagnostics: list[dict[str, object]] = []
        selected: list[dict[str, object]] = []
        try:
            rows = await scan(
                c, parameters(paths.raw_dir, StorageFilters(), selection.archive_id), 0
            )
            require(
                len(rows) == 1,
                f"Local archive {selection.archive_id} is not registered in this raw root.",
            )
            row = rows[0]
            resolved = await resolve_archive(c, paths, row, request.content_type)
            require(
                resolved.instrument_id == selection.instrument_id,
                "Selected instrument differs from archive identity.",
            )
            stream = str(resolved.task["stream_id"])
            source = f"{row['origin']}:{row['id']}"
            previous = groups[request.content_type].setdefault((source, stream), resolved)
            require(previous.task == resolved.task, f"Conflicting metadata for source {source}.")
            selected.append(
                {
                    "local_id": selection.archive_id,
                    "instrument_id": resolved.instrument_id,
                    "stream_id": stream,
                    "metadata_warnings": resolved.warnings,
                    "identity_resolution": resolved.identity_resolution,
                }
            )
        except (MarketForgeError, OSError, ValueError) as exc:
            blocked = True
            diagnostics.append({"local_id": selection.archive_id, "reason": str(exc)})
        outcomes.append(
            {
                "request_index": index,
                "selected_archives": selected,
                "blockers": diagnostics,
                "legacy_selection": selection.model_dump(mode="json"),
            }
        )
    return build_plan(
        paths,
        request.model_dump_json(),
        None,
        groups,
        outcomes,
        blocked,
        snapshot,
        positions,
        used,
        request,
        ranking,
    )


async def prepare_batch(
    paths: ProjectPaths,
    recipe: AcquisitionConfig | PreparationRequest,
    *,
    dry_run: bool = False,
    http: HttpSettings | None = None,
) -> dict[str, object]:
    policy = (
        recipe.ranking_policy
        if isinstance(recipe, (NormalizationRecipe, PreparationRequest))
        else "turnover"
    )
    dsn = testing_dsn(paths.root)
    async with testing_connection(dsn) as c:
        async with ranking_lock(c, shared=dry_run):
            ranking = await ensure_ranking(
                c,
                dsn,
                paths.config_dir / "markets/rankings",
                policy,
                http or HttpSettings(),
                dry_run=dry_run,
            )
            async with c.transaction():
                await c.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
                plan = (
                    await plan_batch(c, paths, recipe, ranking)
                    if isinstance(recipe, AcquisitionConfig)
                    else await plan_manual_batch(c, paths, recipe, ranking)
                )
            report = cast(dict[str, object], json.loads(json.dumps(plan.report, default=str)))
            report["dry_run"] = dry_run
            if not dry_run:
                try:
                    publish_batch(paths, plan, report)
                except OSError as exc:
                    raise MarketForgeError(
                        f"Could not publish normalization batch/report at {plan.run}: {exc}"
                    ) from exc
            return report


def publish_batch(paths: ProjectPaths, plan: BatchPlan, report: dict[str, object]) -> None:
    paths.runs_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".prepare-normalize-", dir=paths.runs_dir) as temporary:
        directory = Path(temporary)
        for document in plan.jobs.values():
            output = document["output"]
            assert isinstance(output, dict)
            output = cast(dict[str, object], output)
            for key in ("staging_path", "dataset_path"):
                target = Path(str(output[key]))
                require(
                    not target.exists() and not target.is_symlink(),
                    f"Output already exists: {target}",
                )
        write_job(directory / "normalize-batch.json", plan.batch)
        write_job(directory / "prepare-report.json", report)
        require(
            not plan.run.exists() and not plan.run.is_symlink(), f"Batch already exists: {plan.run}"
        )
        directory.rename(plan.run)
