"""Explicit maintenance from retained evidence without replaying any work."""

import hashlib
import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from forge.orchestration.inspection import MANAGED_RUN, read_managed_report
from forge.orchestration.registry import (
    bounded_summary,
    connection_for,
    finalize,
    resource_refs,
    status_of,
)
from forge.orchestration.run_summary import when
from forge.orchestration.run_visibility import hidden_names, marker
from forge.shared.database.connection import database_purpose
from forge.shared.errors import MarketForgeError
from forge.shared.identity import RunOperation, parse_run_id
from forge.shared.paths import ProjectPaths

LEGACY_KINDS = {
    "historical_pipeline": RunOperation.PIPELINE,
}


@dataclass(slots=True)
class MaintenanceEvidence:
    """Details available during maintenance without changing its machine payload."""

    indexing: dict[str, str] = field(default_factory=dict[str, str])
    attempts: dict[str, tuple[int, str]] = field(default_factory=dict[str, tuple[int, str]])


def bounded_json(path: Path, root: Path) -> dict[str, Any]:
    if path.is_symlink() or not path.resolve().is_relative_to(root.resolve()):
        raise MarketForgeError("Unsafe retained outcome path.")
    with path.open("rb") as source:
        data = source.read(512 * 1024 + 1)
    if len(data) > 512 * 1024:
        raise MarketForgeError("Retained outcome exceeds inspection bounds.")
    document: object = json.loads(data)
    if not isinstance(document, dict):
        raise MarketForgeError("Retained outcome must be an object.")
    return json.loads(data)


def candidates(root: Path, limit: int, offset: int) -> list[Path]:
    if not 1 <= limit <= 1000 or not 0 <= offset <= 100000:
        raise MarketForgeError("Maintenance pages require limit 1..1000 and offset 0..100000.")
    if not root.is_dir():
        return []
    names: list[str] = []
    with os.scandir(root) as entries:
        for index, entry in enumerate(entries):
            if index >= 100000:
                raise MarketForgeError("Run inventory exceeds bounded scan limit.")
            if entry.is_dir(follow_symlinks=False):
                names.append(entry.name)
    return [root / name for name in sorted(names)[offset : offset + limit]]


def legacy_operation(report: dict[str, Any]) -> RunOperation | None:
    known = LEGACY_KINDS.get(str(report.get("kind", "")))
    if known is None and {"archive_outcomes", "request_counts", "selection"} <= report.keys():
        known = RunOperation.ACQUISITION
    try:
        explicit = RunOperation(report["operation"]) if "operation" in report else None
    except ValueError:
        return None
    return None if known and explicit and known != explicit else known or explicit


async def index_legacy(
    paths: ProjectPaths,
    *,
    limit: int,
    offset: int,
    apply: bool,
    evidence: MaintenanceEvidence | None = None,
) -> dict[str, Any]:
    outcomes: list[dict[str, object]] = []
    async with connection_for(paths) as (connection, registry):
        hidden = hidden_names(paths.runs_dir)
        for path in candidates(paths.runs_dir, limit, offset):
            name = path.name
            item: dict[str, object] = {"run_id": name}
            outcomes.append(item)
            if name in hidden or await marker(
                connection, name, include_alias=True, runs_root=paths.runs_dir
            ):
                item["status"] = "history_removed"
                continue
            managed = bool(MANAGED_RUN.fullmatch(name))
            if not managed:
                try:
                    identity = parse_run_id(name)
                except ValueError:
                    item["status"] = "unrecognized"
                    continue
                if identity.operation:
                    item["status"] = "canonical_run"
                    continue
            try:
                report = read_managed_report(paths.runs_dir, name)
            except (OSError, ValueError, MarketForgeError):
                item["status"] = "invalid_evidence"
                continue
            operation = legacy_operation(report)
            if (
                operation is None
                or report.get("run_id", name) != name
                or (managed and not name.startswith(f"{operation}-"))
            ):
                item["status"] = "ambiguous_operation_or_identity"
                continue
            if report.get("environment") not in {"production", "testing"}:
                item["status"] = "unknown_environment"
                continue
            if report["environment"] != database_purpose() or report.get("registry_id") != registry:
                item["status"] = "different_or_unverified_registry"
                continue
            started = when(report.get("started_at"))
            finished = when(report.get("finished_at"))
            if started is None or finished and finished < started:
                item["status"] = "missing_or_invalid_timing"
                continue
            if (
                report.get("kind") != "historical_pipeline"
                and managed
                and report.get("status") == "running"
            ):
                # Liveness cannot be reconstructed from a retained intent.
                item["status"] = "running_liveness_unknown"
                continue
            report_path = report.get("resolved_report") if managed else str(path / "report.json")
            cursor = await connection.execute(
                "SELECT operation,database_id::text,database_purpose,artifact_root FROM historical.execution_runs WHERE run_id=%s",
                (name,),
            )
            existing = await cursor.fetchone()
            if existing is not None:
                item["status"] = (
                    "indexed"
                    if existing == (operation, registry, database_purpose(), str(path))
                    else "conflict"
                )
                if evidence is not None and item["status"] == "indexed":
                    evidence.indexing[name] = "already_indexed"
                continue
            item["status"] = "eligible"
            if apply:
                inserted = await connection.execute(
                    """INSERT INTO historical.execution_runs
                       (run_id,operation,action,recipe,database_purpose,database_id,started_at,finished_at,status,summary,artifact_root,report_path)
                       VALUES (%s,%s,'legacy',%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
                    (
                        name,
                        operation,
                        str(report.get("recipe") or report.get("name") or "")[:1024] or None,
                        database_purpose(),
                        registry,
                        started,
                        finished,
                        status_of(report, "legacy"),
                        Jsonb(bounded_summary(report)),
                        str(path),
                        report_path,
                    ),
                )
                item["status"] = "indexed"
                if evidence is not None:
                    evidence.indexing[name] = (
                        "newly_indexed" if inserted.rowcount == 1 else "concurrent_insert"
                    )
    return {"dry_run": not apply, "offset": offset, "outcomes": outcomes}


def valid_report(reference: object, root: Path, digest: object) -> bool:
    if not isinstance(reference, str):
        return False
    path = Path(reference)
    if path.is_symlink() or not path.resolve().is_relative_to(root.resolve()) or not path.is_file():
        return False
    if digest is not None:
        with path.open("rb") as source:
            data = source.read(4 * 1024 * 1024 + 1)
        return len(data) <= 4 * 1024 * 1024 and hashlib.sha256(data).hexdigest() == digest
    return True


def validate_report(outcome: dict[str, Any], root: Path) -> None:
    report = outcome.get("report_path")
    if report is not None and not valid_report(report, root, outcome.get("report_sha256")):
        raise MarketForgeError("Missing or unsafe retained report.")


def pipeline_report_resources(outcome: dict[str, Any]) -> list[tuple[str, str, str]]:
    """Recover newer resource identities from an already fingerprinted pipeline snapshot."""
    if outcome.get("operation") != RunOperation.PIPELINE or not outcome.get("report_sha256"):
        return []
    reference = outcome.get("report_path")
    if not isinstance(reference, str):
        return []
    with Path(reference).open("rb") as stream:
        data = stream.read(4 * 1024 * 1024 + 1)
    if len(data) > 4 * 1024 * 1024 or hashlib.sha256(data).hexdigest() != outcome["report_sha256"]:
        raise MarketForgeError("Retained pipeline report changed during reconciliation.")
    report: dict[str, Any] = json.loads(data)
    if any(report.get(key) != outcome.get(key) for key in ("run_id", "registry_id", "environment")):
        raise MarketForgeError("Retained pipeline report conflicts with the outcome identity.")
    if status_of(report, "run") != outcome["status"]:
        raise MarketForgeError("Retained pipeline report conflicts with the outcome status.")
    return [
        ref
        for ref in resource_refs(report)
        if ref[1] in {"conversion_output_ref", "prepared_batch"}
    ]


async def reconcile(
    paths: ProjectPaths,
    *,
    limit: int,
    offset: int,
    apply: bool,
    evidence: MaintenanceEvidence | None = None,
    run_id: str | None = None,
) -> dict[str, Any]:
    if not 1 <= limit <= 1000 or not 0 <= offset <= 100000:
        raise MarketForgeError("Invalid reconciliation page.")
    if run_id is not None:
        try:
            run_id = str(parse_run_id(run_id))
        except ValueError as error:
            raise MarketForgeError("Expected an exact run ID.") from error
        if offset:
            raise MarketForgeError("Exact-run reconciliation does not accept an offset.")
    items: list[dict[str, object]] = []
    prefix = str(paths.runs_dir) + "/"
    async with connection_for(paths) as (connection, registry):
        async with connection.cursor(row_factory=dict_row) as cursor:
            await cursor.execute(
                """SELECT a.attempt_id,a.number,a.status,a.outcome_path,a.report_path,
                          r.run_id,r.artifact_root,r.operation,r.report_path AS run_report
                   FROM historical.execution_attempts a JOIN historical.execution_runs r USING(run_id)
                   WHERE r.database_id=%s AND r.database_purpose=%s
                   AND left(r.artifact_root,length(%s))=%s
                   AND NOT EXISTS (
                      SELECT 1 FROM historical.execution_run_deletions d
                      WHERE d.database_id=r.database_id AND d.run_id=r.run_id AND d.mode='full'
                   )
                   AND (%s::text IS NULL OR r.run_id=%s)
                   AND ((%s::text IS NOT NULL AND
                       a.number=(SELECT max(last.number) FROM historical.execution_attempts last
                                 WHERE last.run_id=r.run_id)) OR
                       (%s::text IS NULL AND (a.status='running' OR (
                       r.report_path IS NULL AND a.report_path IS NOT NULL AND
                       a.number=(SELECT max(last.number) FROM historical.execution_attempts last
                                 WHERE last.run_id=r.run_id)))))
                   ORDER BY a.started_at,a.attempt_id LIMIT %s OFFSET %s""",
                (
                    registry,
                    database_purpose(),
                    prefix,
                    prefix,
                    run_id,
                    run_id,
                    run_id,
                    run_id,
                    limit,
                    offset,
                ),
            )
            attempts = await cursor.fetchall()
        if run_id is not None and not attempts:
            raise MarketForgeError(
                "No eligible attempt for this run in the selected registry and workspace."
            )
        for attempt in attempts:
            identity = attempt["run_id"]
            attempt_id = str(attempt["attempt_id"])
            item: dict[str, object] = {"run_id": identity, "attempt_id": attempt_id}
            items.append(item)
            if evidence is not None:
                evidence.attempts[attempt_id] = (attempt["number"], attempt["status"])
            root = paths.runs_dir / identity
            source = root / "attempts" / str(attempt["attempt_id"]) / "outcome.json"
            if (
                root.is_symlink()
                or root.resolve().parent != paths.runs_dir.resolve()
                or (str(root) != attempt["artifact_root"] or str(source) != attempt["outcome_path"])
            ):
                item["status"] = "conflicting_artifact_root"
                continue
            if not source.is_file():
                item["status"] = (
                    "running_liveness_unknown"
                    if attempt["status"] == "running"
                    else "missing_outcome"
                )
                continue
            try:
                outcome = bounded_json(source, root)
                expected = {
                    "run_id": identity,
                    "attempt_id": str(attempt["attempt_id"]),
                    "number": attempt["number"],
                    "operation": attempt["operation"],
                    "registry_id": registry,
                    "environment": database_purpose(),
                }
                if any(outcome.get(key) != value for key, value in expected.items()):
                    raise MarketForgeError("Retained identity conflicts with the registry.")
                report = outcome.get("report_path")
                if report is not None and not valid_report(
                    report, paths.runs_dir, outcome.get("report_sha256")
                ):
                    item["status"] = "missing_or_unsafe_report"
                    continue
                pending = await finalize(connection, outcome, source, apply=False)
                restore_pointer = bool(
                    report and not attempt["run_report"] and attempt["report_path"]
                )
                if pending:
                    extra = [
                        list(ref)
                        for ref in pipeline_report_resources(outcome)
                        if list(ref) not in outcome["resources"]
                    ]
                    outcome = {**outcome, "resources": [*outcome["resources"], *extra]}
                    await finalize(connection, outcome, source, apply=apply)
                if apply and restore_pointer:
                    await connection.execute(
                        """UPDATE historical.execution_runs r SET report_path=%s,updated_at=clock_timestamp()
                               WHERE run_id=%s AND report_path IS NULL AND
                               %s=(SELECT max(number) FROM historical.execution_attempts WHERE run_id=r.run_id)""",
                        (report, identity, attempt["number"]),
                    )
                item["status"] = (
                    ("reconciled" if apply else "retained_outcome")
                    if pending or restore_pointer
                    else "already_reconciled"
                )
            except (OSError, ValueError, KeyError, MarketForgeError) as error:
                item["status"] = "invalid_or_conflicting_outcome"
                if run_id is not None:
                    item["reason"] = str(error)
        if run_id is not None:
            return {"dry_run": not apply, "offset": offset, "outcomes": items}
        hidden = hidden_names(paths.runs_dir)
        for path in candidates(paths.runs_dir, limit, offset):
            if path.name in hidden or await marker(
                connection, path.name, include_alias=True, runs_root=paths.runs_dir
            ):
                continue
            try:
                identity = parse_run_id(path.name)
            except ValueError:
                continue
            if identity.operation is None:
                continue
            cursor = await connection.execute(
                "SELECT 1 FROM historical.execution_runs WHERE run_id=%s AND database_id=%s",
                (path.name, registry),
            )
            if await cursor.fetchone() is None:
                items.append(
                    {"run_id": path.name, "status": "unindexed_local_evidence_requires_review"}
                )
    return {"dry_run": not apply, "offset": offset, "outcomes": items}
