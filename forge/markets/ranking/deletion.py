"""Testing-only snapshot selection, concrete dependency inspection and maintenance deletion."""

import json
from typing import cast
from uuid import UUID

from psycopg import AsyncConnection
from psycopg.pq import TransactionStatus
from psycopg.rows import TupleRow, dict_row

from forge.markets.ranking.coordination import ranking_lock
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths

REFERENCE_KEYS = {"ranking_snapshot", "ranking_snapshot_id", "snapshot_id"}


def snapshot_references(value: object) -> set[str]:
    found: set[str] = set()
    if isinstance(value, dict):
        for key, item in cast(dict[str, object], value).items():
            if key in REFERENCE_KEYS and isinstance(item, str):
                try:
                    found.add(str(UUID(item)))
                except ValueError:
                    pass
            found.update(snapshot_references(item))
    elif isinstance(value, list):
        for item in cast(list[object], value):
            found.update(snapshot_references(item))
    return found


def managed_references(paths: ProjectPaths) -> dict[str, list[str]]:
    sources = set((paths.data_root / ".jobs").rglob("*.json"))
    sources.update(paths.runs_dir.rglob("*.json"))
    found: dict[str, list[str]] = {}
    for source in sorted(sources):
        # Preparation diagnostics are not execution dependencies; the batch is.
        if "report" in source.name:
            continue
        try:
            value: object = json.loads(source.read_text())
        except (OSError, ValueError, UnicodeError):
            continue
        for snapshot in snapshot_references(value):
            found.setdefault(snapshot, []).append(f"Managed artifact {source}")
    return found


async def dependencies(c: AsyncConnection[TupleRow], paths: ProjectPaths) -> dict[str, list[str]]:
    found = managed_references(paths)
    async with c.cursor(row_factory=dict_row) as cursor:
        await cursor.execute(
            "SELECT id,operation_details,manifest_path,result_report_path FROM historical.datasets"
        )
        for row in await cursor.fetchall():
            references = snapshot_references(row["operation_details"])
            for key in ("manifest_path", "result_report_path"):
                if isinstance(row[key], str):
                    path = paths.root / str(row[key])
                    try:
                        references.update(snapshot_references(json.loads(path.read_text())))
                    except (OSError, ValueError, UnicodeError):
                        pass
            for snapshot in references:
                found.setdefault(snapshot, []).append(f"Dataset {row['id']}")
        await cursor.execute("SELECT to_regclass('historical.jobs') AS relation")
        row = await cursor.fetchone()
        if row and row["relation"] is not None:
            await cursor.execute("SELECT to_jsonb(j) AS document FROM historical.jobs j")
            for row in await cursor.fetchall():
                for snapshot in snapshot_references(row["document"]):
                    found.setdefault(snapshot, []).append(
                        "Registered historical job: " + str(row["document"])
                    )
    return found


def selection(selector: str | None, all_snapshots: bool) -> tuple[UUID | None, str | None]:
    if (selector is None) == (not all_snapshots):
        raise MarketForgeError("Select exactly one policy name, snapshot UUID, or --all.")
    if selector is None:
        return None, None
    try:
        return UUID(selector), None
    except ValueError:
        return None, selector


async def delete_snapshots(
    c: AsyncConnection[TupleRow],
    paths: ProjectPaths,
    selector: str | None = None,
    *,
    all_snapshots: bool = False,
    apply: bool = False,
) -> dict[str, object]:
    identity, policy = selection(selector, all_snapshots)
    owned = c.info.transaction_status == TransactionStatus.IDLE
    async with ranking_lock(c, shared=not apply):
        async with c.transaction():
            if not apply and owned:
                await c.execute("SET TRANSACTION READ ONLY")
            async with c.cursor(row_factory=dict_row) as cursor:
                await cursor.execute(
                    """SELECT id,policy_name,created_at,status,included_count,
                    universe_count-included_count AS excluded_count FROM ranking.snapshots
                    WHERE (%s OR (%s::uuid IS NOT NULL AND id=%s)
                       OR (%s::text IS NOT NULL AND policy_name=%s AND status='published'))
                    ORDER BY created_at DESC,id DESC"""
                    + (" FOR UPDATE" if apply else ""),
                    (all_snapshots, identity, identity, policy, policy),
                )
                rows = list(await cursor.fetchall())
            references = await dependencies(c, paths)
            results: list[dict[str, object]] = []
            for row in rows:
                snapshot = str(row["id"])
                blockers = references.get(snapshot, [])
                removed = False
                if apply and not blockers:
                    cursor = await c.execute("SELECT ranking.delete_snapshot(%s)", (row["id"],))
                    result = await cursor.fetchone()
                    removed = bool(result and result[0])
                results.append(
                    {
                        **{
                            key: str(value) if key in {"id", "created_at"} else value
                            for key, value in row.items()
                        },
                        "eligible": not blockers,
                        "blockers": blockers,
                        "result": "blocked" if blockers else "deleted" if removed else "eligible",
                    }
                )
    blocked = sum(bool(row["blockers"]) for row in results)
    deleted = sum(row["result"] == "deleted" for row in results)
    return {
        "schema_version": 1,
        "action": "apply" if apply else "preview",
        "selector": selector,
        "all": all_snapshots,
        "status": "partial"
        if blocked
        else "already_absent"
        if identity and not results
        else "complete",
        "matched_count": len(results),
        "deleted_count": deleted,
        "blocked_count": blocked,
        "eligible_count": sum(bool(row["eligible"]) for row in results),
        "snapshots": results,
        "deleted_ids": [row["id"] for row in results if row["result"] == "deleted"],
        "blocked_ids": [row["id"] for row in results if row["blockers"]],
        "message": "Already absent."
        if identity and not results
        else "Zero matching snapshots."
        if not results
        else "Apply re-resolves its selection transactionally; preview does not lock a later target set.",
    }
