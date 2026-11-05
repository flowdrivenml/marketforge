"""Preview-first managed archive deletion and failed-attempt retirement."""

import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal, cast

from psycopg import AsyncConnection
from psycopg.rows import TupleRow

from forge.historical.archives.operation import failure_message
from forge.historical.archives.storage.acquire import download_filename, root_key
from forge.shared.errors import MarketForgeError
from forge.shared.ownership import WorkspaceOwnership
from forge.shared.paths import ProjectPaths
from forge.storage.cleanup_plans import (
    CleanupPlan,
    Target,
    canonical,
    digest,
    load_plan,
    new_plan_id,
    write_document,
)
from forge.storage.dependencies import (
    PreparedReferences,
    blockers,
    is_external_original,
    prepared_references,
)
from forge.storage.managed_files import FileChanged, snapshot, unlink_reviewed
from forge.storage.models import StorageFilters
from forge.storage.registry import parameters, scan

SELECTORS = (
    "exchange",
    "market",
    "instrument_type",
    "instrument",
    "scope_kind",
    "match",
    "base_asset",
    "quote_asset",
    "data_type",
    "feed",
    "origin",
    "start",
    "end",
)


def validate_selection(
    operation: str, filters: StorageFilters, archive_id: int | None, overlap: bool
) -> None:
    if overlap and (operation != "delete" or (filters.start is None and filters.end is None)):
        raise MarketForgeError("--overlap requires a date bound on storage delete.")
    if (
        operation == "delete"
        and archive_id is None
        and not any(getattr(filters, field) is not None for field in SELECTORS)
    ):
        raise MarketForgeError(
            "Delete requires local:ID or an explicit instrument/identity/date selector; status-only selection is not allowed."
        )


async def selected_records(
    c: AsyncConnection[TupleRow],
    paths: ProjectPaths,
    filters: StorageFilters,
    operation: str,
    archive_id: int | None,
    overlap: bool,
) -> list[dict[str, object]]:
    validate_selection(operation, filters, archive_id, overlap)
    params = parameters(paths.raw_dir, filters, archive_id)
    params["containment"] = operation == "delete" and not overlap
    result: list[dict[str, object]] = []
    after = 0
    while rows := await scan(c, params, after):
        result.extend(rows)
        after = int(str(rows[-1]["id"]))
    return result


async def inspect_target(
    c: AsyncConnection[TupleRow],
    paths: ProjectPaths,
    row: dict[str, object],
    operation: Literal["prune", "delete"],
    *,
    inactive: bool,
    references: PreparedReferences | None = None,
    resume_final: bool = False,
) -> Target:
    record = cast(dict[str, object], json.loads(canonical(row)))
    relative = str(row["relative_path"])
    target = Target(
        local_id=int(str(row["id"])),
        record=record,
        record_digest=digest(record),
        relative_path=relative,
        state="blocked",
        action="none",
    )
    partial = relative + ".part"
    target.partial_relative_path = partial
    target.path = str(paths.raw_dir / (partial if operation == "prune" else relative))
    try:
        target.final_file = snapshot(paths.raw_dir, relative)
        target.partial_file = snapshot(paths.raw_dir, partial)
    except FileChanged as error:
        target.blockers.append(str(error))
        return target
    target.actual_bytes = (
        target.partial_file.size if operation == "prune" else target.final_file.size
    )
    status = row["status"]
    if (
        operation == "delete"
        and status == "deleted"
        and not target.final_file.exists
        and not target.partial_file.exists
    ):
        target.state = "absent"
        return target
    if operation == "delete" and status in {"complete", "corrupt", "missing"}:
        if not inactive:
            target.blockers.append("An active managed operation holds the workspace/storage lock.")
        if not target.final_file.exists and not resume_final:
            target.blockers.append(
                "Managed archive is absent on disk; inspect or reconcile the record before deletion."
            )
        if target.partial_file.exists:
            target.blockers.append(
                "An unpublished partial also exists; reconcile this reservation before deleting its final archive."
            )
        if is_external_original(paths, row):
            target.blockers.append(
                "The registered managed path is the original external source; originals are never removed."
            )
        target.blockers.extend(
            await blockers(c, paths, row, references or prepared_references(paths))
        )
        if target.blockers:
            return target
        target.state = "eligible"
        target.path = str(paths.raw_dir / relative)
        target.actual_bytes = target.final_file.size
        target.reclaimable_bytes = target.final_file.size
        target.action = "delete_managed_archive"
        return target
    if status != "failed":
        target.blockers.append(
            "Only failed attempts can be pruned; active attempts are retained."
            if operation == "prune"
            else "Pending/downloading or unexpectedly populated deleted records require inspection before deletion."
        )
        return target
    if target.final_file.exists:
        target.blockers.append(
            "A final file exists for this failed reservation; reconcile its state before cleanup."
        )
        return target
    if not inactive:
        target.blockers.append(
            "Application operation lock is held; failed-attempt inactivity cannot be established."
        )
        return target
    if row.get("remote_archive_id") is None or row.get("external_archive_id") is not None:
        target.blockers.append(
            "Partial ownership is not the remote downloader's; external originals and import reservations are retained."
        )
        return target
    if row.get("bytes_written") is not None or row.get("sha256") is not None:
        target.blockers.append(
            "Reservation contains durable byte/checksum metadata; recovery may still publish it."
        )
        return target
    parts = relative.split("/")
    filename = row.get("filename")
    if (
        len(parts) != 3
        or parts[0] != str(row["remote_archive_id"])
        or re.fullmatch(r"[a-f0-9]{32}", parts[1]) is None
        or not isinstance(filename, str)
        or parts[2] != download_filename(filename)
    ):
        target.blockers.append(
            "Registry path does not prove ownership by the downloader's remote-ID/attempt-UUID layout."
        )
        return target
    prefix = "/".join(parts[:2]) + "/"
    cursor = await c.execute(
        """SELECT id FROM historical.local_archives WHERE storage_root_key=%s AND id<>%s
                             AND left(relative_path,length(%s))=%s LIMIT 1""",
        (root_key(paths.raw_dir), target.local_id, prefix, prefix),
    )
    if await cursor.fetchone() is not None:
        target.blockers.append(
            "Another registry copy shares the attempt directory; ownership is ambiguous."
        )
        return target
    target.blockers.extend(await blockers(c, paths, row, references or prepared_references(paths)))
    if target.blockers:
        return target
    target.state = "eligible"
    target.path = str(paths.raw_dir / partial)
    target.actual_bytes = target.partial_file.size
    target.reclaimable_bytes = target.partial_file.size
    target.action = (
        "remove_partial_and_retire_failed_reservation"
        if target.partial_file.exists
        else "retire_absent_failed_reservation"
    )
    return target


async def preview(
    c: AsyncConnection[TupleRow],
    paths: ProjectPaths,
    filters: StorageFilters,
    operation: Literal["prune", "delete"],
    *,
    archive_id: int | None = None,
    overlap: bool = False,
    inactive: bool = True,
    review_all: bool = False,
) -> CleanupPlan:
    rows = await selected_records(c, paths, filters, operation, archive_id, overlap)
    references = prepared_references(paths)
    targets = [
        await inspect_target(c, paths, row, operation, inactive=inactive, references=references)
        for row in rows
    ]
    return CleanupPlan(
        plan_id=new_plan_id(),
        operation=operation,
        project_root=str(paths.root),
        data_root=str(paths.data_root),
        raw_root=str(paths.raw_dir),
        created_at=datetime.now(UTC).isoformat(),
        filters={**filters.model_dump(mode="json"), "archive_id": archive_id},
        overlap=overlap,
        selected_count=len(targets),
        review_complete=review_all or (filters.offset == 0 and len(targets) <= filters.limit),
        targets=targets,
    )


async def current_record(
    c: AsyncConnection[TupleRow], paths: ProjectPaths, identity: int
) -> dict[str, object] | None:
    params = parameters(paths.raw_dir, StorageFilters(), identity)
    rows = await scan(c, params, 0)
    return rows[0] if rows else None


def totals(targets: list[Target]) -> dict[str, int]:
    return {
        "selected": len(targets),
        "eligible": sum(t.state == "eligible" for t in targets),
        "records_eligible_for_retirement": sum(t.state == "eligible" for t in targets),
        "partial_files_found": sum(
            t.partial_file is not None and t.partial_file.exists for t in targets
        ),
        "eligible_archive_files": sum(
            t.state == "eligible"
            and t.action == "delete_managed_archive"
            and t.final_file is not None
            and t.final_file.exists
            for t in targets
        ),
        "archive_files_found": sum(
            t.final_file is not None and t.final_file.exists for t in targets
        ),
        "eligible_partial_files": sum(
            t.state == "eligible" and t.partial_file is not None and t.partial_file.exists
            for t in targets
        ),
        "blocked": sum(t.state == "blocked" for t in targets),
        "absent": sum(t.state == "absent" for t in targets),
        "reclaimable_bytes": sum(t.reclaimable_bytes for t in targets if t.state == "eligible"),
    }


async def retire_failed(c: AsyncConnection[TupleRow], local_id: int) -> None:
    await c.execute(
        "UPDATE historical.local_archives SET status='deleted',updated_at=statement_timestamp() WHERE id=%s AND status='failed'",
        (local_id,),
    )


async def retire_archive(c: AsyncConnection[TupleRow], local_id: int, status: str) -> None:
    cursor = await c.execute(
        "UPDATE historical.local_archives SET status='deleted',updated_at=statement_timestamp() WHERE id=%s AND status=%s RETURNING id",
        (local_id, status),
    )
    if await cursor.fetchone() is None:
        raise FileChanged(
            "Archive status changed during deletion; registry finalization must be retried."
        )


def retired_identity(row: dict[str, object], original: dict[str, object]) -> bool:
    ignored = {"status", "updated_at"}
    return digest({k: v for k, v in row.items() if k not in ignored}) == digest(
        {k: v for k, v in original.items() if k not in ignored}
    )


async def apply(
    c: AsyncConnection[TupleRow], paths: ProjectPaths, source: Path, plan: CleanupPlan
) -> dict[str, object]:
    if not plan.review_complete:
        raise MarketForgeError(
            "Every selected target must be visible in the reviewed preview before apply."
        )
    # One acquisition/import/prepare-compatible lock for the complete operation.
    with WorkspaceOwnership(paths):
        source, reviewed = load_plan(paths, source, plan.operation)
        if reviewed.digest != plan.digest:
            raise MarketForgeError("Supplied plan differs from the saved reviewed preview.")
        plan = reviewed
        receipt_path = source.with_suffix(".apply.json")
        receipts: dict[str, object] = {}
        if receipt_path.is_symlink():
            raise MarketForgeError("Apply receipt must not be a symlink.")
        if receipt_path.exists():
            receipts = cast(dict[str, object], json.loads(receipt_path.read_text()))
        if receipts and receipts.get("plan_digest") != plan.digest:
            raise MarketForgeError("Apply receipt does not belong to this reviewed preview.")
        saved = receipts.get("targets", {})
        entries = cast(dict[str, str], saved)
        receipts = {"plan_digest": plan.digest, "targets": entries}
        references = prepared_references(paths)
        outcomes: list[dict[str, object]] = []
        for target in plan.targets:
            result: dict[str, object] = {
                "local_id": f"local:{target.local_id}",
                "reclaimed_bytes": 0,
                "partial_removed": False,
                "record_retired": False,
                "archive_removed": False,
                "source_status": target.record.get("status"),
            }
            key = str(target.local_id)
            file_removed = False
            try:
                async with c.transaction():
                    await c.execute(
                        "SELECT id FROM historical.local_archives WHERE id=%s AND storage_root_key=%s FOR UPDATE",
                        (target.local_id, root_key(paths.raw_dir)),
                    )
                    row = await current_record(c, paths, target.local_id)
                    if row is None:
                        raise FileChanged("Registry target is no longer in this configured root.")
                    origin_id = row.get("remote_archive_id")
                    if origin_id is not None:
                        await c.execute(
                            "SELECT id FROM historical.remote_archives WHERE id=%s FOR SHARE",
                            (origin_id,),
                        )
                    elif row.get("external_archive_id") is not None:
                        await c.execute(
                            "SELECT id FROM historical.external_archives WHERE id=%s FOR SHARE",
                            (row["external_archive_id"],),
                        )
                    row = await current_record(c, paths, target.local_id)
                    if row is None:
                        raise FileChanged("Registry target disappeared while acquiring ownership.")
                    if target.state == "blocked":
                        if digest(row) != target.record_digest:
                            raise FileChanged(
                                "Blocked target's registry identity, path or status changed since preview."
                            )
                        current = await inspect_target(
                            c, paths, row, plan.operation, inactive=True, references=references
                        )
                        if (
                            current.state != target.state
                            or current.final_file != target.final_file
                            or current.partial_file != target.partial_file
                        ):
                            raise FileChanged(
                                "Blocked target's ownership or eligibility changed; create a new preview."
                            )
                        result.update(status="blocked", reason="; ".join(target.blockers))
                    elif (
                        entries.get(key) in {"intent", "deleted"}
                        and row["status"] == "deleted"
                        and retired_identity(row, target.record)
                        and row["relative_path"] == target.relative_path
                        and not snapshot(paths.raw_dir, target.relative_path).exists
                        and not snapshot(paths.raw_dir, target.relative_path + ".part").exists
                    ):
                        result["status"] = "already_deleted"
                    else:
                        if digest(row) != target.record_digest:
                            raise FileChanged(
                                "Registry identity, path, status or metadata changed since preview."
                            )
                        final_action = target.action == "delete_managed_archive"
                        fresh = await inspect_target(
                            c,
                            paths,
                            row,
                            plan.operation,
                            inactive=True,
                            references=references,
                            resume_final=final_action and entries.get(key) == "intent",
                        )
                        if fresh.state == "blocked":
                            raise FileChanged("Eligibility changed: " + "; ".join(fresh.blockers))
                        if fresh.state != target.state:
                            raise FileChanged(
                                "Eligibility changed since preview; create a fresh preview."
                            )
                        if fresh.state == "absent":
                            result["status"] = "absent"
                        else:
                            if final_action:
                                if (
                                    fresh.action != target.action
                                    or fresh.partial_file != target.partial_file
                                ):
                                    raise FileChanged(
                                        "Archive eligibility or neighboring partial changed since preview."
                                    )
                                resumed = (
                                    entries.get(key) == "intent"
                                    and fresh.final_file is not None
                                    and not fresh.final_file.exists
                                )
                                if not resumed and fresh.final_file != target.final_file:
                                    raise FileChanged(
                                        "Managed archive file identity or size changed since preview."
                                    )
                            else:
                                resumed = (
                                    (entries.get(key) == "intent" or plan.operation == "prune")
                                    and target.state == "eligible"
                                    and fresh.partial_file is not None
                                    and not fresh.partial_file.exists
                                )
                                if fresh.final_file != target.final_file or (
                                    not resumed and fresh.partial_file != target.partial_file
                                ):
                                    raise FileChanged(
                                        "File identity, path or size changed since preview."
                                    )
                            entries[key] = "intent"
                            write_document(receipt_path, receipts, replace=True)
                            file_state = fresh.final_file if final_action else fresh.partial_file
                            relative = (
                                target.relative_path
                                if final_action
                                else target.partial_relative_path
                            )
                            if file_state is not None and file_state.exists:
                                assert relative is not None
                                unlink_reviewed(paths.raw_dir, relative, file_state)
                                file_removed = True
                                result["archive_removed" if final_action else "partial_removed"] = (
                                    True
                                )
                                result["reclaimed_bytes"] = file_state.size
                            if final_action:
                                await retire_archive(c, target.local_id, str(row["status"]))
                            else:
                                await retire_failed(c, target.local_id)
                            result["status"] = "deleted"
                if result.get("status") == "deleted":
                    result["record_retired"] = True
                if result.get("status") in {"deleted", "removed_partial", "already_deleted"}:
                    entries[key] = (
                        "deleted"
                        if result["status"] == "already_deleted"
                        else str(result["status"])
                    )
                    write_document(receipt_path, receipts, replace=True)
            except FileChanged as error:
                result.update(
                    status="removed_registry_pending" if file_removed else "skipped_changed",
                    reason=str(error),
                )
            except Exception as error:
                result.update(
                    status="removed_registry_pending" if file_removed else "failed",
                    reason=str(error)
                    if isinstance(error, MarketForgeError)
                    else failure_message(error),
                    recovery="Retain the preview and receipt. Reapply the same preview; an absent reviewed file with a durable intent can finalize the registry without removing another file.",
                )
            outcomes.append(result)
        failed = any(
            row.get("status")
            in {"blocked", "skipped_changed", "failed", "removed_registry_pending"}
            for row in outcomes
        )
        return {
            "schema_version": 1,
            "operation": plan.operation,
            "mode": "apply",
            "status": "partial" if failed else "complete",
            "preview_path": str(source),
            "receipt_path": str(receipt_path),
            "totals": totals(plan.targets),
            "outcomes": outcomes,
            "result_totals": {
                "removed_partials": sum(row.get("partial_removed") is True for row in outcomes),
                "partial_files_removed": sum(
                    row.get("partial_removed") is True for row in outcomes
                ),
                "retired_failed_reservations": sum(
                    row.get("record_retired") is True and row.get("source_status") == "failed"
                    for row in outcomes
                ),
                "failed_records_retired": sum(
                    row.get("record_retired") is True and row.get("source_status") == "failed"
                    for row in outcomes
                ),
                "records_deleted": sum(row.get("record_retired") is True for row in outcomes),
                "archive_files_removed": sum(
                    row.get("archive_removed") is True for row in outcomes
                ),
                "reclaimed_bytes": sum(int(str(row["reclaimed_bytes"])) for row in outcomes),
                "blocked_or_changed": sum(
                    row.get("status") in {"blocked", "skipped_changed"} for row in outcomes
                ),
            },
        }
