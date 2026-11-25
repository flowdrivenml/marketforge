"""Searchable run metadata. Complete execution evidence stays on disk."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import AsyncGenerator, Awaitable, Callable, Generator, Mapping
from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, LiteralString, cast
from uuid import UUID

from psycopg import AsyncConnection
from psycopg.rows import TupleRow, dict_row
from psycopg.types.json import Jsonb

from forge.historical.normalization.service import write_job
from forge.orchestration.run_summary import mapping, when
from forge.orchestration.run_visibility import marker
from forge.orchestration.sorting import RUNS
from forge.shared.database.connection import (
    DatabaseError,
    database_connection,
    database_dsn,
    database_purpose,
    pinned_registry,
    registry_id,
    require_registry,
)
from forge.shared.errors import MarketForgeError
from forge.shared.identity import (
    PIPELINE_STAGES,
    RunId,
    RunOperation,
    new_attempt_id,
    new_run_id,
    parse_run_id,
)
from forge.shared.paths import ProjectPaths

MAX_RESOURCES = 2048
TERMINAL = frozenset(
    {
        "completed",
        "prepared",
        "partial",
        "failed",
        "blocked",
        "interrupted",
        "cancelled",
        "stopped",
        "unknown",
    }
)
RESOURCE_TYPES = frozenset(
    {
        "discovery_scan",
        "remote_archive",
        "local_archive",
        "engine_execution",
        "engine_publication",
        "primary_file_dataset",
        "prepared_batch",
        "prepared_job",
        "conversion_output",
        "conversion_output_ref",
        "report",
        "child_run",
        "source_publication",
        "registry_publication",
    }
)
NUMERIC_RESOURCES = frozenset(
    {
        "discovery_scan",
        "remote_archive",
        "local_archive",
        "primary_file_dataset",
        "engine_publication",
        "source_publication",
        "registry_publication",
    }
)


def valid_resource(kind: str, reference: str) -> bool:
    if kind in NUMERIC_RESOURCES:
        return reference.isascii() and reference.isdecimal() and not reference.startswith("0")
    if kind == "conversion_output_ref":
        return re.fullmatch(r"[a-f0-9]{64}", reference) is not None
    if kind == "child_run":
        try:
            return parse_run_id(reference).operation is not None
        except ValueError:
            return False
    return True


SUMMARY_FIELDS = (
    "registration",
    "verification",
    "execution",
    "counts",
    "request_counts",
    "file_counts",
    "interval",
    "files_known",
    "bytes_written",
    "submitted_sha256",
    "recipe_sha256",
    "configuration_sha256",
    "target_quote",
    "settings_files",
)
execution_started: ContextVar[Callable[[Execution], None] | None] = ContextVar(
    "execution_started", default=None
)
_current: ContextVar[Execution | None] = ContextVar("execution_registry", default=None)


def current_execution() -> Execution | None:
    return _current.get()


def utcnow() -> datetime:
    return datetime.now(UTC)


def status_of(result: Mapping[str, object], action: str) -> str:
    state = str(result.get("state") or result.get("status") or "")
    if state == "failed" and result.get("operation") == "acquisition":
        counts = mapping(result.get("request_counts"))
        files = mapping(result.get("file_counts"))
        if any(
            type(value) is int and value > 0
            for value in (
                counts.get("successful") if action == "map" else None,
                files.get("completed"),
                files.get("reused"),
            )
        ):
            return "partial"
    if (
        state in {"failed", "blocked"}
        and result.get("collection_path")
        and any(
            job.get("status") == "complete" and isinstance(job.get("publication"), Mapping)
            for job in rows(result.get("jobs"))
        )
    ):
        return "partial"
    if result.get("registration") in {"failed", "pending"} and state in {
        "complete",
        "completed",
        "registered",
    }:
        return "partial"
    if state == "generated":
        return "prepared" if result.get("compatibility") == "validated" else "blocked"
    state = {
        "complete": "completed",
        "registered": "completed",
        "invalid": "failed",
        "incomplete": "blocked",
    }.get(state, state)
    if (
        action == "register"
        and state not in TERMINAL
        and result.get("registration") == "registered"
    ):
        return "completed"
    return state if state in TERMINAL else "unknown"


def rows(value: object) -> list[Mapping[str, object]]:
    return [mapping(row) for row in cast(list[object], value)] if isinstance(value, list) else []


def resource_refs(result: Mapping[str, object]) -> list[tuple[str, str, str]]:
    found: set[tuple[str, str, str]] = set()

    def collect(document: Mapping[str, object], stage: str = "") -> None:
        def add(kind: str, value: object) -> None:
            if (
                type(value) is int
                and value > 0
                or isinstance(value, str)
                and value
                and len(value) <= 4096
            ):
                if len(found) < MAX_RESOURCES and valid_resource(kind, str(value)):
                    found.add((stage, kind, str(value)))

        for key, kind in {
            "execution_id": "engine_execution",
            "batch_path": "prepared_batch",
            "job_path": "prepared_job",
            "report_path": "report",
            "report": "report",
            "execution_receipt": "report",
            "child_report": "report",
        }.items():
            add(kind, document.get(key))
        for value in mapping(document.get("catalogue_dataset_ids")).values():
            add("primary_file_dataset", value)
        for key, kind in (("primary_file_ids", "primary_file_dataset"), ("jobs", None)):
            values = document.get(key)
            if isinstance(values, list):
                for item in cast(list[object], values):
                    if kind:
                        add(kind, item)
                    elif stage == "":
                        add("prepared_job", mapping(item).get("path"))
        for request in rows(document.get("requests")):
            add("discovery_scan", request.get("scan_id"))
        for archive in rows(mapping(document.get("selection")).get("archives")):
            add("local_archive", archive.get("local_archive_id"))
            add("remote_archive", archive.get("remote_archive_id"))
        for selected in rows(document.get("inputs")) + rows(document.get("sources")):
            add(
                "source_publication",
                selected.get("engine_id") or selected.get("engine_publication_id"),
            )
            add("registry_publication", selected.get("registry_publication_id"))
            add("local_archive", selected.get("local_archive_id"))
        publications = document.get("publications")
        for publication in [
            *rows(publications),
            *(mapping(item) for item in mapping(publications).values()),
        ]:
            add("engine_publication", publication.get("engine_dataset_id"))
            add("registry_publication", publication.get("publication_id"))
        dataset = mapping(document.get("dataset"))
        add("engine_publication", dataset.get("engine_dataset_id"))
        for job in rows(document.get("jobs")):
            result_path = job.get("result_path")
            if isinstance(result_path, str):
                add("engine_execution", Path(result_path).parent.name)
                add("report", result_path)
            publication = mapping(job.get("publication"))
            if publication:
                if job.get("status") == "complete" and job.get("indexing") == "complete":
                    add("conversion_output_ref", job.get("work_key"))
                manifest = publication.get("manifest")
                if isinstance(manifest, str):
                    add("conversion_output", str(Path(manifest).parent))
            source = mapping(job.get("input"))
            add("source_publication", source.get("engine_publication_id"))
            add("registry_publication", source.get("registry_publication_id"))
        for copy in rows(document.get("copies")):
            add("local_archive", copy.get("local_archive_id"))
        add("child_run", document.get("child_run_id"))
        add("report", document.get("terminal_outcome"))
        add("report", mapping(document.get("prepared")).get("report"))
        add(
            "prepared_batch" if stage == "normalize" else "prepared_job",
            mapping(document.get("prepared")).get("job"),
        )

    collect(result)
    for stage, evidence in mapping(result.get("stages")).items():
        if stage in PIPELINE_STAGES:
            item = mapping(evidence)
            collect(item, stage)
            collect(mapping(item.get("result")), stage)
    return sorted(found)


def bounded_refs(refs: list[tuple[str, str, str]]) -> list[tuple[str, str, str]]:
    result: list[tuple[str, str, str]] = []
    remaining = 256 * 1024
    for ref in refs[:MAX_RESOURCES]:
        remaining -= len(json.dumps(ref)) + 2
        if remaining < 0:
            break
        result.append(ref)
    return result


def bounded_summary(result: Mapping[str, object]) -> dict[str, object]:
    result_summary: dict[str, object] = {}
    for field in SUMMARY_FIELDS:
        if field in result:
            encoded = json.dumps(result[field], default=str)
            if len(encoded) <= 1024:
                result_summary[field] = json.loads(encoded)
    result_summary["domain_status"] = str(result.get("state") or result.get("status") or "")[:64]
    quote = mapping(result.get("settings")).get("target_quote")
    if isinstance(quote, str):
        result_summary["target_quote"] = quote[:64]
    stages = mapping(result.get("stages"))
    if stages:
        result_summary["stage_order"] = [stage for stage in PIPELINE_STAGES if stage in stages]
        result_summary["stages"] = {
            name: {key: str(mapping(stage).get(key, ""))[:64] for key in ("state", "phase")}
            for name, stage in stages.items()
            if name in PIPELINE_STAGES
        }
    return result_summary


@dataclass(frozen=True, slots=True)
class Execution:
    paths: ProjectPaths
    id: str
    operation: RunOperation
    action: str
    attempt_id: UUID
    number: int
    started_at: datetime
    registry: str
    purpose: str
    root: Path
    dsn: str

    @property
    def acquisition_root(self) -> Path:
        return self.root if self.number == 1 else self.outcome.parent / "acquisition"

    @property
    def outcome(self) -> Path:
        return self.root / "attempts" / str(self.attempt_id) / "outcome.json"

    def retain_outcome(self, outcome: dict[str, Any], report_path: str | None) -> None:
        if not self.outcome.resolve().is_relative_to(self.root.resolve()):
            raise OSError("Unsafe attempt evidence path.")
        if report_path:
            path = Path(report_path)
            if (
                not path.is_symlink()
                and path.resolve().is_relative_to(self.paths.runs_dir.resolve())
                and path.is_file()
            ):
                with path.open("rb") as stream:
                    data = stream.read(4 * 1024 * 1024 + 1)
                if len(data) <= 4 * 1024 * 1024:
                    outcome["report_sha256"] = hashlib.sha256(data).hexdigest()
                    if self.operation == RunOperation.PIPELINE:
                        snapshot = self.outcome.with_name("pipeline-report.json")
                        write_job(snapshot, json.loads(data))
                        outcome["report_path"] = str(snapshot)
                        outcome["report_sha256"] = hashlib.sha256(snapshot.read_bytes()).hexdigest()
        write_job(self.outcome, outcome)

    async def finish(self, result: Mapping[str, object]) -> dict[str, Any]:
        """Publish a retryable outcome before updating the index."""
        report = result.get("report_path") or result.get("report")
        report_path = str(report) if isinstance(report, (str, Path)) else None
        status = status_of(result, self.action)
        refs = set(resource_refs(result))
        prepared = self.outcome.with_name("preparation.json")
        summary = bounded_summary(result)
        if prepared.is_file():
            saved = json.loads(prepared.read_bytes())
            refs.update(tuple(item) for item in saved["resources"])
            preparation = saved["summary"]
            if len(json.dumps(preparation)) <= 1024:
                summary["preparation"] = preparation
        recipe_metadata = self.outcome.with_name("recipe-source.json")
        if recipe_metadata.is_file():
            metadata = json.loads(recipe_metadata.read_bytes())
            summary["recipe_source"] = metadata
            summary["recipe_snapshot"] = metadata.get(
                "snapshot", str(self.outcome.with_name("recipe.toml"))
            )
        # Reserve room for PostgreSQL's JSONB representation and retain
        # essential status and provenance before optional result details.
        priority = ("domain_status", "recipe_source", "recipe_snapshot", "stages", "stage_order")
        indexed_summary: dict[str, object] = {}
        budget = 14 * 1024
        for field in dict.fromkeys((*priority, *summary)):
            if field in summary:
                size = len(json.dumps({field: summary[field]}).encode()) + 8
                if size <= budget:
                    indexed_summary[field] = summary[field]
                    budget -= size
        outcome: dict[str, Any] = {
            "run_id": self.id,
            "attempt_id": str(self.attempt_id),
            "number": self.number,
            "environment": self.purpose,
            "registry_id": self.registry,
            "operation": self.operation,
            "status": status,
            "finished_at": utcnow().isoformat(),
            "report_path": report_path,
            "summary": indexed_summary,
            "resources": bounded_refs(sorted(refs)),
            "error_summary": "Inspect retained diagnostics."
            if status in {"failed", "blocked", "interrupted", "cancelled"}
            else None,
        }
        public = dict(result)
        public.setdefault("run_id", self.id)
        if public["run_id"] != self.id:
            public["logical_run_id"] = self.id
        try:
            if self.outcome.is_file():
                from forge.orchestration.registry_maintenance import bounded_json, validate_report

                saved = bounded_json(self.outcome, self.root)
                if any(
                    saved.get(key) != outcome[key]
                    for key in (
                        "run_id",
                        "attempt_id",
                        "number",
                        "operation",
                        "environment",
                        "registry_id",
                        "status",
                    )
                ):
                    raise MarketForgeError(
                        "Retained outcome conflicts with this execution attempt."
                    )
                if json.dumps(saved.get("summary"), sort_keys=True) != json.dumps(
                    outcome["summary"], sort_keys=True
                ) or saved.get("resources") != [list(ref) for ref in outcome["resources"]]:
                    raise MarketForgeError("Retained execution resources or summary conflict.")
                validate_report(saved, self.paths.runs_dir)
                outcome = saved
            else:
                self.retain_outcome(outcome, report_path)
        except MarketForgeError as error:
            public["execution_registry_warning"] = f"Could not finalize {self.id}. {error}"
            return public
        except (OSError, ValueError):
            public["execution_registry_warning"] = (
                f"Could not retain the registry outcome for {self.id}."
            )
            return public
        try:
            with pinned_registry(self.registry):
                async with database_connection(self.dsn) as connection:
                    await finalize(connection, outcome, self.outcome)
            if self.operation == RunOperation.PIPELINE:
                public["execution_registry_status"] = "finalized"
        except (DatabaseError, MarketForgeError):
            public["execution_registry_warning"] = (
                f"Outcome retained for {self.id}. Reconcile the execution index without replaying processing."
            )
        return public


@asynccontextmanager
async def connection_for(
    paths: ProjectPaths,
) -> AsyncGenerator[tuple[AsyncConnection[TupleRow], str]]:
    async with database_connection(database_dsn(paths.root)) as connection:
        yield connection, await registry_id(connection)


async def begin(
    paths: ProjectPaths,
    operation: RunOperation,
    action: str,
    *,
    recipe: str | None = None,
    source: Path | None = None,
    identity: RunId | None = None,
    resume: bool = False,
    evidence: Mapping[str, object] | None = None,
    dsn: str | None = None,
) -> Execution:
    run_id = identity or new_run_id(operation)
    if run_id.operation != operation and not (resume and run_id.operation is None):
        raise MarketForgeError("Run ID operation does not match its execution.")
    root = paths.runs_dir / str(run_id)
    if root.is_symlink() or root.resolve().parent != paths.runs_dir.resolve():
        raise MarketForgeError("Unsafe execution artifact root.")
    started = utcnow()
    attempt = new_attempt_id()
    purpose = database_purpose()
    parent = current_execution()
    parent_id = parent.id if parent is not None else None
    dsn = dsn or database_dsn(paths.root)
    async with database_connection(dsn) as connection, connection.transaction():
        registry = await registry_id(connection)
        if evidence is not None:
            require_registry(evidence, registry)
        if parent is not None and (parent.registry != registry or parent.purpose != purpose):
            raise DatabaseError("The child execution requires its parent's registry identity.")
        if resume and run_id.operation is None and evidence is not None:
            # Legacy pipelines may be adopted only with an explicit verified retained identity.
            await connection.execute(
                """INSERT INTO historical.execution_runs
                   (run_id,operation,action,recipe,recipe_source,database_purpose,database_id,started_at,status,artifact_root)
                   VALUES (%s,%s,'run',%s,%s,%s,%s,%s,'pending',%s) ON CONFLICT DO NOTHING""",
                (
                    str(run_id),
                    operation,
                    recipe,
                    str(source) if source else None,
                    purpose,
                    registry,
                    when(evidence.get("started_at")) or started,
                    str(root),
                ),
            )
        if not resume:
            await connection.execute(
                """INSERT INTO historical.execution_runs
                   (run_id,operation,action,recipe,recipe_source,database_purpose,database_id,parent_run_id,started_at,status,artifact_root)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'pending',%s)""",
                (
                    str(run_id),
                    operation,
                    action,
                    recipe,
                    str(source) if source else None,
                    purpose,
                    registry,
                    parent_id,
                    started,
                    str(root),
                ),
            )
        cursor = await connection.execute(
            """SELECT operation, database_purpose, database_id::text, artifact_root, parent_run_id
               FROM historical.execution_runs WHERE run_id=%s FOR UPDATE""",
            (str(run_id),),
        )
        indexed = await cursor.fetchone()
        if (
            indexed is None
            or indexed[:4] != (operation, purpose, registry, str(root))
            or (operation == RunOperation.ACQUISITION and indexed[4] != parent_id)
        ):
            raise MarketForgeError(
                "Run is absent or belongs to a different operation, workspace or registry."
            )
        removal = await marker(connection, str(run_id))
        if removal and removal["mode"] == "full":
            raise MarketForgeError(
                "This run has an explicit deletion marker; inspect its cleanup receipt."
            )
        cursor = await connection.execute(
            """INSERT INTO historical.execution_attempts
               (attempt_id,run_id,number,action,started_at,status,outcome_path)
               SELECT %s,%s,COALESCE(max(number),0)+1,%s,%s,'running',%s
               FROM historical.execution_attempts WHERE run_id=%s RETURNING number""",
            (
                attempt,
                str(run_id),
                action,
                started,
                str(root / "attempts" / str(attempt) / "outcome.json"),
                str(run_id),
            ),
        )
        row = await cursor.fetchone()
        assert row is not None
        number = int(row[0])
        intent = root / "attempts" / str(attempt) / "intent.json"
        if not intent.resolve().is_relative_to(root.resolve()):
            raise MarketForgeError("Unsafe execution intent location.")
        try:
            write_job(
                intent,
                {
                    "run_id": str(run_id),
                    "operation": operation,
                    "action": action,
                    "attempt_id": str(attempt),
                    "number": number,
                    "started_at": started.isoformat(),
                    "environment": purpose,
                    "registry_id": registry,
                    "parent_run_id": parent_id,
                    "recipe": recipe,
                    "recipe_source": str(source) if source else None,
                },
            )
        except OSError as error:
            raise MarketForgeError(f"Could not retain execution intent for {run_id}.") from error
        await connection.execute(
            """UPDATE historical.execution_runs SET status='running', finished_at=NULL,
               report_path=NULL, summary='{}'::jsonb, error_summary=NULL,
               updated_at=clock_timestamp() WHERE run_id=%s""",
            (str(run_id),),
        )
    execution = Execution(
        paths,
        str(run_id),
        operation,
        action,
        attempt,
        number,
        started,
        registry,
        purpose,
        root,
        dsn,
    )
    if parent is None and (started_observer := execution_started.get()) is not None:
        started_observer(execution)
    return execution


async def finalize(
    connection: AsyncConnection[TupleRow],
    outcome: Mapping[str, Any],
    path: Path,
    *,
    apply: bool = True,
) -> bool:
    """Validate the retained outcome and, when requested, finalize its original attempt."""

    identity = await registry_id(connection)
    require_registry(outcome, identity)
    if outcome.get("environment") != database_purpose():
        raise MarketForgeError("Outcome belongs to a different database environment.")
    status = outcome["status"]
    finished = when(outcome["finished_at"])
    if status not in TERMINAL or finished is None:
        raise MarketForgeError("Invalid retained execution outcome.")
    resource_data: object = outcome["resources"]
    if (
        not isinstance(resource_data, list)
        or len(cast(list[object], resource_data)) > MAX_RESOURCES
    ):
        raise MarketForgeError("Invalid retained resource associations.")
    refs: list[tuple[str, str, str]] = []
    for item in cast(list[object], resource_data):
        if not isinstance(item, (list, tuple)):
            raise MarketForgeError("Invalid retained resource association.")
        values = cast("list[object] | tuple[object, ...]", item)
        if len(values) != 3:
            raise MarketForgeError("Invalid retained resource association.")
        stage, kind, reference = values
        if (
            not isinstance(stage, str)
            or stage not in ("", *PIPELINE_STAGES)
            or not isinstance(kind, str)
            or kind not in RESOURCE_TYPES
            or kind == "conversion_output_ref"
            and stage not in ("", "convert")
            or not isinstance(reference, str)
            or not 1 <= len(reference) <= 4096
            or not valid_resource(kind, reference)
        ):
            raise MarketForgeError("Invalid retained resource association.")
        refs.append((stage, kind, reference))
    async with connection.transaction():
        cursor = await connection.execute(
            """SELECT a.status,a.outcome_path,r.operation,r.database_id::text,r.database_purpose,
                      a.finished_at,a.report_path,a.error_summary
               FROM historical.execution_attempts a JOIN historical.execution_runs r USING(run_id)
               WHERE a.run_id=%s AND a.attempt_id=%s AND a.number=%s
                 AND NOT EXISTS (
                     SELECT 1 FROM historical.execution_run_deletions d
                     WHERE d.database_id=r.database_id AND d.run_id=r.run_id AND d.mode='full'
                 )
               """
            + (" FOR UPDATE OF a,r" if apply else ""),
            (outcome["run_id"], outcome["attempt_id"], outcome["number"]),
        )
        row = await cursor.fetchone()
        if row is None or row[1:5] != (
            str(path),
            outcome["operation"],
            identity,
            database_purpose(),
        ):
            raise MarketForgeError("Outcome does not match the indexed execution attempt.")
        evidence = (finished, outcome["report_path"], outcome["error_summary"])
        if row[0] != "running":
            if row[0] != status or row[5:] != evidence:
                raise MarketForgeError("Conflicting terminal outcome for an execution attempt.")
            cursor = await connection.execute(
                "SELECT stage,kind,reference FROM historical.execution_resources WHERE attempt_id=%s",
                (outcome["attempt_id"],),
            )
            if not set(refs).issubset(await cursor.fetchall()):
                raise MarketForgeError("Conflicting resources for a finalized execution attempt.")
            return False
        if not apply:
            return True
        await connection.execute(
            """UPDATE historical.execution_attempts SET status=%s, finished_at=%s,
               report_path=%s,error_summary=%s WHERE attempt_id=%s""",
            (status, *evidence, outcome["attempt_id"]),
        )
        if refs:
            async with connection.cursor() as cursor:
                await cursor.executemany(
                    """INSERT INTO historical.execution_resources (attempt_id,stage,kind,reference)
                       VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
                    [(outcome["attempt_id"], *ref) for ref in refs],
                )
        await connection.execute(
            """UPDATE historical.execution_runs r SET status=%s,finished_at=%s,report_path=%s,
                 error_summary=%s,summary=%s,updated_at=clock_timestamp()
               WHERE r.run_id=%s AND %s=(SELECT max(number) FROM historical.execution_attempts WHERE run_id=r.run_id)""",
            (status, *evidence, Jsonb(outcome["summary"]), outcome["run_id"], outcome["number"]),
        )

    return True


@contextmanager
def active(execution: Execution) -> Generator[None]:
    token = _current.set(execution)
    try:
        with pinned_registry(execution.registry):
            yield
    finally:
        _current.reset(token)


async def list_index(
    paths: ProjectPaths,
    *,
    limit: int,
    offset: int = 0,
    operation: str | None = None,
    action: str | None = None,
    status: str | None = None,
    recipe: str | None = None,
    start: str | None = None,
    end: str | None = None,
    sort: str | None = None,
    order: str | None = None,
    search: str | None = None,
) -> dict[str, Any]:
    if not 1 <= limit <= 1000 or not 0 <= offset <= 100000:
        raise MarketForgeError("Run pages require limit 1..1000 and offset 0..100000.")
    ordering = RUNS.resolve(sort, order)
    clauses: list[LiteralString] = [
        "database_id=%s",
        "database_purpose=%s",
        "NOT EXISTS (SELECT 1 FROM historical.execution_run_deletions d "
        "WHERE d.database_id=historical.execution_runs.database_id "
        "AND d.run_id=historical.execution_runs.run_id)",
    ]
    values: list[object] = []
    for clause, value in (
        ("operation=%s", operation),
        ("action=%s", action),
        ("status=%s", status),
        ("recipe=%s", recipe),
        ("started_at >= %s", start),
        ("started_at < %s", end),
    ):
        if value is not None:
            parsed = when(value) if clause.startswith("started_at") else value
            if parsed is None:
                raise MarketForgeError(
                    "Run time filters require an ISO-8601 timestamp with a timezone."
                )
            clauses.append(cast(LiteralString, clause))
            values.append(parsed)
    if search and (term := search.strip()):
        clauses.append(
            "position(lower(%s) in lower(concat_ws(' ', run_id, operation, action, recipe, status))) > 0"
        )
        values.append(term)
    async with (
        connection_for(paths) as (connection, registry),
        connection.cursor(row_factory=dict_row) as cursor,
    ):
        await cursor.execute(
            "SELECT * FROM historical.execution_runs WHERE "
            + " AND ".join(clauses)
            + " ORDER BY "
            + ordering.sql()
            + " LIMIT %s OFFSET %s",
            (registry, database_purpose(), *values, limit + 1, offset),
        )
        records = await cursor.fetchall()
    return {
        "schema_version": 2,
        "runs": [display_record(row) for row in records[:limit]],
        "offset": offset,
        "more": len(records) > limit,
    }


async def workspace_runs(paths: ProjectPaths) -> list[dict[str, Any]]:
    """Recent indexed runs plus the latest recorded activity per operation."""
    async with (
        connection_for(paths) as (connection, registry),
        connection.cursor(row_factory=dict_row) as cursor,
    ):
        await connection.execute("SET statement_timeout = '5s'")
        await cursor.execute(
            """
            WITH eligible AS NOT MATERIALIZED (
                SELECT r.* FROM historical.execution_runs r
                WHERE r.database_id=%s AND r.database_purpose=%s AND NOT EXISTS (
                    SELECT 1 FROM historical.execution_run_deletions d
                    WHERE d.database_id=r.database_id AND d.run_id=r.run_id
                )
            ), recent AS (
                SELECT * FROM eligible ORDER BY started_at DESC,run_id LIMIT 8
            ), latest AS (
                SELECT DISTINCT ON (operation) * FROM eligible
                ORDER BY operation,coalesce(finished_at,started_at) DESC,run_id
            )
            SELECT * FROM recent UNION ALL
                SELECT * FROM latest WHERE NOT EXISTS (
                    SELECT 1 FROM recent WHERE recent.run_id=latest.run_id
                )
            """,
            (registry, database_purpose()),
        )
        return [display_record(row) for row in await cursor.fetchall()]


def display_record(record: dict[str, Any]) -> dict[str, Any]:
    start, finish = record["started_at"], record["finished_at"]
    return {
        **record,
        "duration_seconds": (finish - start).total_seconds() if finish else None,
        "liveness": "unknown" if record["status"] == "running" else None,
    }


async def show_index(paths: ProjectPaths, reference: str) -> dict[str, Any] | None:
    async with (
        connection_for(paths) as (connection, registry),
        connection.cursor(row_factory=dict_row) as cursor,
    ):
        await cursor.execute(
            """SELECT * FROM historical.execution_runs WHERE run_id=%s AND database_id=%s
               AND database_purpose=%s""",
            (reference, registry, database_purpose()),
        )
        record = await cursor.fetchone()
        if record is None:
            return None
        await cursor.execute(
            """SELECT * FROM historical.execution_attempts WHERE run_id=%s ORDER BY number LIMIT 1001""",
            (reference,),
        )
        attempts = await cursor.fetchall()
        await cursor.execute(
            """SELECT a.number,x.stage,x.kind,x.reference FROM historical.execution_resources x
               JOIN historical.execution_attempts a USING(attempt_id) WHERE a.run_id=%s
               ORDER BY a.number,x.stage,x.kind,x.reference LIMIT 4097""",
            (reference,),
        )
        resources = await cursor.fetchall()
        await cursor.execute(
            """SELECT run_id,operation,action,status FROM historical.execution_runs
               WHERE parent_run_id=%s AND database_id=%s AND database_purpose=%s
               ORDER BY started_at,run_id LIMIT 1000""",
            (reference, registry, database_purpose()),
        )
        return {
            "schema_version": 2,
            **display_record(record),
            "attempts": attempts[:1000],
            "attempts_truncated": len(attempts) > 1000,
            "resources": resources[:4096],
            "resources_truncated": len(resources) > 4096,
            "children": await cursor.fetchall(),
        }


def checkpoint(result: Mapping[str, object]) -> None:
    """Retain preparation references for a combined prepare-and-execute invocation."""
    execution = current_execution()
    if execution is None:
        return
    path = execution.outcome.with_name("preparation.json")
    if not path.exists():
        write_job(path, {"summary": bounded_summary(result), "resources": resource_refs(result)})


def finish_cli(result: Mapping[str, object]) -> dict[str, Any]:
    import asyncio

    execution = current_execution()
    return asyncio.run(execution.finish(result)) if execution else dict(result)


async def begin_operation(
    paths: ProjectPaths,
    operation: RunOperation,
    action: str,
    reference: str,
) -> Execution:
    import asyncio

    source = await asyncio.to_thread(Path(reference).expanduser)
    if action == "register" and source.is_file():
        async with connection_for(paths) as (connection, registry):
            cursor = await connection.execute(
                """SELECT DISTINCT r.run_id FROM historical.execution_runs r
                   JOIN historical.execution_attempts a USING(run_id)
                   JOIN historical.execution_resources x USING(attempt_id)
                   WHERE r.operation=%s AND r.database_id=%s AND r.database_purpose=%s
                   AND x.kind='report' AND x.reference=%s LIMIT 2""",
                (operation, registry, database_purpose(), str(source.resolve())),
            )
            matches = await cursor.fetchall()
        if len(matches) > 1:
            raise MarketForgeError(
                "Several logical runs refer to the supplied registration evidence."
            )
        if matches:
            return await begin(
                paths, operation, action, identity=parse_run_id(str(matches[0][0])), resume=True
            )
    return await begin(paths, operation, action, recipe=reference, source=source)


@contextmanager
def cli_execution(
    paths: ProjectPaths,
    operation: RunOperation,
    action: str,
    reference: str,
) -> Generator[Execution]:
    import asyncio

    execution = asyncio.run(begin_operation(paths, operation, action, reference))
    with active(execution):
        try:
            yield execution
        except BaseException as error:
            outcome = {
                "status": "interrupted" if isinstance(error, KeyboardInterrupt) else "failed",
                "error": type(error).__name__,
            }
            saved = asyncio.run(execution.finish(outcome))
            if "execution_registry_warning" in saved:
                error.add_note(saved["execution_registry_warning"])
            raise


async def complete_call(
    execution: Execution,
    call: Callable[[], Awaitable[Mapping[str, object]]],
) -> dict[str, Any]:
    import asyncio

    with active(execution):
        try:
            result = await call()
        except BaseException as error:
            outcome = {
                "status": "interrupted"
                if isinstance(error, (KeyboardInterrupt, asyncio.CancelledError))
                else "failed"
            }
            if (
                execution.operation == RunOperation.ACQUISITION
                and (execution.acquisition_root / "report.json").is_file()
            ):
                from forge.orchestration.runs import safe_report

                outcome = safe_report(execution.acquisition_root / "report.json", execution.root)
                outcome.setdefault("report_path", str(execution.acquisition_root / "report.json"))
            saved = await execution.finish(outcome)
            if "execution_registry_warning" in saved:
                error.add_note(saved["execution_registry_warning"])
            raise
        return await execution.finish(result)


def retain_recipe(
    operation: RunOperation,
    source: Path,
    submitted: bytes,
    *,
    effective: Mapping[str, object] | None = None,
    destination: Path | None = None,
    family: str | None = None,
) -> dict[str, Any]:
    """Commit the bytes and metadata used by this attempt before dispatch."""
    import tomllib

    from forge.orchestration.artifacts import retain_bytes

    execution = current_execution()
    if execution is None or execution.operation != operation:
        return {}
    destination = destination or execution.outcome.with_name("recipe.toml")
    if not destination.resolve().is_relative_to(execution.root.resolve()):
        raise MarketForgeError("Unsafe retained recipe location.")
    document = tomllib.loads(submitted.decode())
    version = document.get("version") or (effective.get("version") if effective else None)
    if version is None and family == "registration":
        version = document["registration"]["version"]
    elif version is None and operation == RunOperation.ACQUISITION and family is None:
        version = 1
    digest = retain_bytes(destination, submitted)
    metadata: dict[str, Any] = {
        "path": str(source.resolve()),
        "name": source.stem,
        "family": family or str(operation),
        "version": version,
        "snapshot": str(destination),
        "sha256": digest,
        "submitted_at": execution.started_at.isoformat(),
        "environment": execution.purpose,
        "registry_id": execution.registry,
    }
    if effective is not None:
        metadata.update(retain_effective(effective))
    retain_bytes(
        execution.outcome.with_name("recipe-source.json"),
        json.dumps(metadata, sort_keys=True).encode(),
    )
    return metadata


def retain_effective(effective: Mapping[str, object]) -> dict[str, str]:
    """Retain resolved settings from the same model used by execution."""
    from forge.orchestration.artifacts import retain_bytes

    execution = current_execution()
    if execution is None:
        return {}
    path = execution.outcome.with_name("effective-configuration.json")
    digest = retain_bytes(
        path, json.dumps(effective, sort_keys=True, indent=2, allow_nan=False).encode()
    )
    return {"effective_configuration": str(path), "effective_sha256": digest}


def recipe_evidence() -> dict[str, Any]:
    """Return the recipe declaration from the active attempt, when present."""
    execution = current_execution()
    if execution is None:
        return {}
    metadata = execution.outcome.with_name("recipe-source.json")
    return json.loads(metadata.read_bytes()) if metadata.is_file() else {}


def inherit_recipe(operation: RunOperation, evidence: Mapping[str, Any]) -> None:
    """Pin the original recipe from a verified prepared submission."""
    execution = current_execution()
    if not evidence or execution is None or execution.operation != operation:
        return
    require_registry(evidence, execution.registry)
    previous = recipe_evidence()
    if previous:
        if previous["sha256"] != evidence["sha256"]:
            raise MarketForgeError("Prepared job belongs to a different recipe snapshot.")
        evidence = previous

    root = execution.paths.runs_dir.resolve()
    snapshot = Path(evidence["snapshot"])
    if snapshot.is_symlink() or not snapshot.resolve().is_relative_to(root):
        raise MarketForgeError("Prepared recipe snapshot is outside run storage.")
    data = snapshot.read_bytes()
    if hashlib.sha256(data).hexdigest() != evidence["sha256"]:
        raise MarketForgeError("Prepared recipe snapshot fingerprint differs.")
    effective = None
    if effective_path := evidence.get("effective_configuration"):
        source = Path(effective_path)
        if source.is_symlink() or not source.resolve().is_relative_to(root):
            raise MarketForgeError("Prepared effective configuration is outside run storage.")
        payload = source.read_bytes()
        if hashlib.sha256(payload).hexdigest() != evidence.get("effective_sha256"):
            raise MarketForgeError("Prepared effective configuration fingerprint differs.")
        effective = json.loads(payload)
    if not previous:
        retain_recipe(
            operation, Path(evidence["path"]), data, effective=effective, family=evidence["family"]
        )
