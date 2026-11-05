"""Read-only recipe planning and atomic publication of a Forge batch wrapper."""

import hashlib
import json
import os
import tarfile
import tempfile
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal, LiteralString, cast

from psycopg import AsyncConnection
from psycopg.rows import TupleRow

from forge.historical.archives.config import AcquisitionConfig
from forge.historical.normalization.config import InputSelection, PreparationRequest
from forge.historical.normalization.contract import job_document
from forge.historical.normalization.mapping import selection_for
from forge.historical.normalization.resolution import require, resolve_input
from forge.historical.normalization.service import output_paths, write_job
from forge.shared.database.testing import testing_connection, testing_dsn
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths
from forge.storage.coverage import CoverageGroup
from forge.storage.models import StorageFilters, interval
from forge.storage.registry import parameters, scan


def dense_ranks(streams: dict[str, int], positions: dict[int, int]) -> list[dict[str, object]]:
    """Stored sparse positions are ordering evidence, never generated rank values."""
    ordered = sorted(
        streams, key=lambda s: (streams[s] not in positions, positions.get(streams[s], 0), s)
    )
    return [{"stream_id": stream, "stream_rank": rank} for rank, stream in enumerate(ordered, 1)]


async def ranking_positions(c: AsyncConnection[TupleRow]) -> tuple[str | None, dict[int, int]]:
    cursor = await c.execute(
        "SELECT id FROM ranking.snapshots WHERE policy_name='turnover' AND status='published' "
        "AND COALESCE(policy_json->>'metric','turnover')='turnover' ORDER BY created_at DESC,id DESC LIMIT 1"
    )
    row = await cursor.fetchone()
    if row is None:
        return None, {}
    cursor = await c.execute(
        "SELECT instrument_id,position FROM ranking.entries WHERE snapshot_id=%s AND position IS NOT NULL",
        (row[0],),
    )
    return str(row[0]), {int(i): int(p) for i, p in await cursor.fetchall()}


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
class PlannedGroup:
    content: Literal["trades", "depth"]
    feed: str | None
    instrument_id: int
    inputs: dict[tuple[str, str | None], tuple[InputSelection, dict[str, object]]] = field(
        default_factory=lambda: {}
    )


@dataclass
class BatchPlan:
    report: dict[str, object]
    jobs: dict[int, dict[str, object]]
    run: Path


async def plan_batch(
    c: AsyncConnection[TupleRow], paths: ProjectPaths, recipe: AcquisitionConfig
) -> BatchPlan:
    snapshot, positions = await ranking_positions(c)
    used = await used_ids(c, paths)
    seed = recipe.model_dump_json()
    groups: dict[tuple[int, str, str | None], PlannedGroup] = {}
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
            group: PlannedGroup | None = None
            try:
                selection = await selection_for(c, paths, row, content)
                key = (
                    selection.instrument_id,
                    content,
                    str(row["feed"]) if row["feed"] is not None else None,
                )
                group = groups.setdefault(key, PlannedGroup(content, key[2], key[0]))
                bounds, _ = interval(row["start_at"], row["end_at"])
                assert bounds is not None
                task, stream = await resolve_input(c, paths, selection, content)
                if content == "depth" and group.feed is not None:
                    task["stream_id"] = f"{stream}.{group.feed}"
                source_identity = f"{row['origin']}:{row.get('remote_archive_id') or row.get('external_archive_id') or row['id']}"
                group.inputs.setdefault(
                    (source_identity, selection.archive_member), (selection, task)
                )
                selected.append(
                    {
                        "local_id": selection.archive_id,
                        "archive_member": selection.archive_member,
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
                EOFError,
                tarfile.TarError,
                zipfile.BadZipFile,
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
    jobs: dict[int, dict[str, object]] = {}
    summaries: list[dict[str, object]] = []
    for content in ("trades", "depth"):
        resolved = [
            (key, selection, task)
            for group in groups.values()
            if group.content == content
            for key, (selection, task) in group.inputs.items()
        ]
        if not resolved:
            continue
        resolved.sort(key=lambda item: (str(item[2]["stream_id"]), item[0][0], item[0][1] or ""))
        job_id = allocate(f"{seed}:{content}:job", used, paths)
        dataset_id = allocate(f"{seed}:{content}:dataset", used, paths)
        selections = [
            selection.model_copy(update={"task_id": i})
            for i, (_, selection, _) in enumerate(resolved, 1)
        ]
        tasks = [{**task, "task_id": i} for i, (_, _, task) in enumerate(resolved, 1)]
        request = PreparationRequest(
            job_id=job_id,
            dataset_id=dataset_id,
            content_type=content,
            inputs=selections,
        )
        staging, dataset = output_paths(paths, request)
        streams = {str(task["stream_id"]): int(str(task["instrument_id"])) for task in tasks}
        ranks = dense_ranks(streams, positions)
        jobs[job_id] = job_document(request, tasks, ranks, staging, dataset)
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
    report: dict[str, object] = {
        "schema_version": 1,
        "kind": "normalization_batch",
        "status": "partial" if blocked else "prepared",
        "run_id": run_id,
        "run_directory": str(run),
        "batch_path": str(run / "normalize-batch.json"),
        "ranking_snapshot": snapshot,
        "ranking_policy": "turnover",
        "requested_interval": recipe.interval.model_dump(mode="json"),
        "jobs": list(jobs.values()),
        "job_summaries": summaries,
        "prepared_job_count": len(jobs),
        "requests": requests,
        "identifiers": "Collision-checked, unreserved testing identifiers; no database registration.",
        "engine_ready": executable.is_file() and os.access(executable, os.X_OK),
        "execution": "Not executed. Forge wrapper is not a Rust --job document; dispatch is deferred.",
    }
    return BatchPlan(report, jobs, run)


async def prepare_batch(
    paths: ProjectPaths, recipe: AcquisitionConfig, *, dry_run: bool = False
) -> dict[str, object]:
    async with testing_connection(testing_dsn(paths.root)) as c:
        async with c.transaction():
            await c.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            plan = await plan_batch(c, paths, recipe)
    # JSON-safe UTC interval diagnostics are shared by dry-run and publication.
    report = cast(dict[str, object], json.loads(json.dumps(plan.report, default=str)))
    report["dry_run"] = dry_run
    if dry_run:
        return report
    publish_batch(paths, plan, report)
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
        write_job(directory / "normalize-batch.json", report)
        require(
            not plan.run.exists() and not plan.run.is_symlink(), f"Batch already exists: {plan.run}"
        )
        directory.rename(plan.run)
