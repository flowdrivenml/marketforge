"""Exact testing publication deletion with no replacement or equivalence claim."""

import asyncio
from contextlib import nullcontext
from typing import Any
from uuid import uuid4

from psycopg import AsyncConnection
from psycopg.rows import TupleRow

from forge.datasets import converted, deduplication, force_cleanup, legacy_storage
from forge.shared.database.connection import (
    database_connection,
    database_dsn,
    database_environment,
    pinned_registry,
    registry_id,
    selected_registry_id,
)
from forge.shared.errors import MarketForgeError
from forge.shared.ownership import acquire_file_lock
from forge.shared.paths import ProjectPaths
from forge.storage.cleanup_plans import write_document

OPERATION = "explicit_delete"


async def registered(connection: AsyncConnection[TupleRow], identity: int) -> dict[str, Any]:
    cursor = await connection.execute(
        """SELECT p.operation,p.engine_dataset_id,p.instrument_namespace,p.storage_root,
                  p.evidence->'execution',p.evidence->'inventory_sha256',p.evidence->'raw_sha256',
                  jsonb_agg(jsonb_build_object('id',d.id,'primary',d.primary_file,'path',d.storage_path,
                             'sha256',d.file_sha256,'status',d.status) ORDER BY d.id)
           FROM historical.dataset_publications p JOIN historical.datasets d ON d.publication_id=p.id
           WHERE p.id=%s GROUP BY p.id""",
        (identity,),
    )
    row = await cursor.fetchone()
    if row is None:
        raise MarketForgeError(f"Registry publication {identity} is not registered in testing.")
    operation, engine, namespace, path, execution, inventory, raw, files = row
    if any(f["status"] != "complete" or not f["primary"] or not f["sha256"] for f in files):
        raise MarketForgeError(f"Registry publication {identity} is not a complete active output.")
    return {
        "publication_id": identity,
        "operation": operation,
        "engine_publication_id": engine,
        "namespace": namespace,
        "path": path,
        "execution": execution,
        "inventory_sha256": inventory,
        "registered_raw_sha256": raw,
        "files": files,
    }


async def indexed_conversion(
    connection: AsyncConnection[TupleRow], registry: str, key: str
) -> dict[str, Any]:
    cursor = await connection.execute(
        """SELECT owner,evidence->>'path',result FROM historical.resolved_work
           WHERE registry_id=%s AND operation='convert' AND work_key=%s
             AND result->>'status'='complete'""",
        (registry, key),
    )
    row = await cursor.fetchone()
    if not row:
        raise MarketForgeError(f"Conversion {key} is not a complete indexed output in testing.")
    owner, stored_path, result = row
    if result.get("registry_id") != registry or result.get("work_key") != key:
        raise MarketForgeError("Indexed conversion lacks registry-bound execution evidence.")
    entry = await asyncio.to_thread(converted.output_row, key, result, [])
    await asyncio.to_thread(converted.verify_output, entry, None)
    if stored_path != entry["storage_root"]:
        raise MarketForgeError("Conversion path disagrees with its active index.")
    return {
        "publication_id": None,
        "output_ref": key,
        "operation": "convert",
        "engine_publication_id": str(entry["engine_job_id"]),
        "path": stored_path,
        "execution": {"execution_id": owner},
        "indexed_result": result,
        "inventory_sha256": result["publication"]["fingerprints"],
        "manifest_sha256": entry["manifest_sha256"],
        "integrity_status": entry["integrity"],
        "entry": {
            "content_type": entry["data_type"],
            "start_timestamp_ns": entry["start_timestamp_ns"],
            "end_timestamp_ns": entry["end_timestamp_ns"],
        },
        "files": [
            {"path": item["storage_path"], "primary": item["path"], "sha256": item["file_sha256"]}
            for item in entry["files"]
        ],
        "verification": "verified",
    }


def empty_graph() -> dict[str, Any]:
    # Conversion outputs are terminal in the supported registry lineage model.
    # Local artifact and foreign reference checks still run.
    return {"source": None, "edges": [], "dependents": [], "conversion_references": []}


async def verify_registered(
    connection: AsyncConnection[TupleRow], paths: ProjectPaths, registry: str, identity: int
) -> dict[str, Any]:
    candidate = await registered(connection, identity)
    evidence = await asyncio.to_thread(deduplication.verify_candidate, paths, registry, candidate)
    return {**candidate, **evidence}


async def preview(
    paths: ProjectPaths,
    ids: list[int],
    output_refs: list[str],
    retained_ids: list[int],
    *,
    cascade: bool,
) -> dict[str, Any]:
    force_cleanup.require_testing()
    if not ids and not output_refs:
        raise MarketForgeError(
            "Select an exact registry publication ID or indexed conversion reference."
        )
    if len(set(ids)) != len(ids) or len(set(output_refs)) != len(output_refs):
        raise MarketForgeError("Select each deletion target once.")
    if set(ids) & set(retained_ids) or len(set(retained_ids)) != len(retained_ids):
        raise MarketForgeError("Retained publications must be distinct from the deletion targets.")
    other = None
    other_error = None
    with database_environment(testing=False), pinned_registry(None):
        try:
            other = await selected_registry_id(paths.root)
        except MarketForgeError as error:
            other_error = str(error)
    report: dict[str, Any] = {
        "schema_version": 1,
        "environment": "testing",
        "operation": OPERATION,
        "mode": "preview",
        "cascade": cascade,
        "other_registry_id": other,
        "other_registry_error": other_error,
        "legacy_risk_targets": [],
        "ownership_blocked_targets": [],
        "targets": [],
    }
    if other is None:
        report["foreign_ownership_evidence"] = await legacy_storage.production_catalogue(paths)
    protected = set(ids) | set(retained_ids)
    async with database_connection(database_dsn(paths.root)) as connection:
        registry = report["registry_id"] = await registry_id(connection)
        report["retained"] = [
            await verify_registered(connection, paths, registry, identity)
            for identity in retained_ids
        ]
        candidates = [
            await verify_registered(connection, paths, registry, identity) for identity in ids
        ] + [await indexed_conversion(connection, registry, key) for key in output_refs]
        active = await connection.execute(
            "SELECT 1 FROM historical.execution_runs WHERE status IN ('pending','running') LIMIT 1"
        )
        busy = bool(await active.fetchone())
        for item in candidates:
            graph = (
                empty_graph()
                if item.get("output_ref")
                else await force_cleanup.dependency_graph(connection, item["publication_id"])
            )
            plan = {"target": item, "dependency_graph": graph}
            blockers = (
                ("Registered dependents require an explicitly reviewed --cascade.",)
                if graph["edges"] and not cascade
                else ()
            )
            await force_cleanup.review_target(
                paths,
                report,
                item,
                plan,
                protected,
                busy,
                extra_blockers=blockers,
            )
            report["targets"].append(plan)
    approved = sum(bool(plan["review"]) for plan in report["targets"])
    report["status"] = "ready" if approved == len(report["targets"]) else "blocked"
    return report


async def journal(
    connection: AsyncConnection[TupleRow], registry: str, target: dict[str, Any]
) -> tuple[Any, ...] | None:
    if target.get("output_ref"):
        cursor = await connection.execute(
            """SELECT cleanup_id,state,evidence FROM historical.converted_output_deletions
               WHERE registry_id=%s AND work_key=%s""",
            (registry, target["output_ref"]),
        )
    else:
        cursor = await connection.execute(
            """SELECT cleanup_id,state,evidence FROM historical.publication_supersessions
               WHERE publication_id=%s""",
            (target["publication_id"],),
        )
    return await cursor.fetchone()


async def apply(
    paths: ProjectPaths, reviewed: dict[str, Any], *, accept_legacy_risk: bool = False
) -> dict[str, Any]:
    force_cleanup.require_testing()
    if reviewed.get("operation") != OPERATION or reviewed.get(
        "plan_digest"
    ) != deduplication.report_digest(reviewed):
        raise MarketForgeError("Use an exact reviewed testing publication deletion plan.")
    registry = await selected_registry_id(paths.root)
    if registry != reviewed["registry_id"]:
        raise MarketForgeError("Deletion plan belongs to another testing registry.")
    cleanup = uuid4()
    result: dict[str, Any] = {
        **reviewed,
        "mode": "apply",
        "cleanup_id": str(cleanup),
        "status": "running",
        "reviewed_plan": reviewed["report_path"],
        "removed_publications": [],
        "removed_conversion_refs": [],
        "invalidated_downstream": [],
        "blocked_actions": [],
        "physical_files_removed": 0,
        "primary_files_removed": 0,
        "publication_directories_removed": 0,
        "removed_file_allocation_bytes": 0,
        "storage_reclaimed_bytes": 0,
        "risk_acknowledged_publications": [],
    }
    output = paths.runs_dir / f"publication-delete-{cleanup.hex}" / "cleanup-report.json"
    result["report_path"] = str(output)
    write_document(output, result)
    retained = reviewed["retained"]
    for survivor in retained:
        verified = await asyncio.to_thread(
            deduplication.verify_candidate, paths, registry, survivor
        )
        if verified["manifest_sha256"] != survivor["manifest_sha256"]:
            raise MarketForgeError("Retained generation changed since review.")
    for plan in reviewed["targets"]:
        target = plan["target"]
        key = target.get("output_ref") or target["publication_id"]
        physical = plan["review"]
        risk = bool(not physical and plan.get("legacy_risk_review"))
        if risk and accept_legacy_risk:
            physical = plan["legacy_risk_review"]
        if not physical:
            result["blocked_actions"].append({"target": key, "reasons": plan["blockers"]})
            continue
        try:
            async with database_connection(database_dsn(paths.root)) as connection:
                if await registry_id(connection) != registry:
                    raise MarketForgeError("Testing registry changed after review.")
                previous = await journal(connection, registry, target)
                if previous and target.get("output_ref") and previous[1] == "removed":
                    old = previous[2]
                    if old.get("force_plan", {}).get("digest") != reviewed["plan_digest"]:
                        # A later materialization can reuse the same work key;
                        # require its exact active result before opening a new journal entry.
                        previous = None
                if previous is None:
                    current = (
                        await indexed_conversion(connection, registry, key)
                        if target.get("output_ref")
                        else await verify_registered(connection, paths, registry, key)
                    )
                    if current != target:
                        raise MarketForgeError("Publication evidence changed after review.")
            intent = {
                "explicit_delete": True,
                "digest": reviewed["plan_digest"],
                "retained": retained,
                "graph": plan["dependency_graph"],
                "managed_storage": plan["managed_storage"],
                "protected_ids": [
                    item["target"]["publication_id"]
                    for item in reviewed["targets"]
                    if item["target"]["publication_id"] is not None
                ]
                + [item["publication_id"] for item in retained],
            }
            if reviewed["other_registry_id"] is None:
                intent["legacy_ownership"] = {
                    "catalogue_digest": deduplication.report_digest(
                        reviewed["foreign_ownership_evidence"]
                    ),
                    "classification": plan["ownership"],
                    "risk_acknowledged": risk,
                }
            owner = cleanup
            if previous:
                owner, state, evidence = previous
                if evidence.get("force_plan") != intent or evidence.get("review") != physical:
                    raise MarketForgeError(
                        "Existing deletion requires the original reviewed plan and ownership decision."
                    )
                if state == "removed":
                    continue
            storage = await asyncio.to_thread(
                legacy_storage.managed_storage, paths, target, registry
            )
            if str(storage) != intent["managed_storage"]:
                raise MarketForgeError("Managed deletion root changed after review.")
            lock = (
                acquire_file_lock(storage / ".marketforge.lock")
                if storage != paths.data_root
                else nullcontext()
            )
            with lock:
                await deduplication.guarded_removal(
                    paths,
                    result,
                    target,
                    None,
                    owner,
                    physical,
                    resume=bool(previous),
                    force_plan=intent,
                )
            removed = (
                "removed_conversion_refs" if target.get("output_ref") else "removed_publications"
            )
            result[removed].append(key)
            result["publication_directories_removed"] += 1
            result["physical_files_removed"] += len(physical["files"])
            result["primary_files_removed"] += len(target["files"])
            result["removed_file_allocation_bytes"] += physical["physical_bytes"]
            result["storage_reclaimed_bytes"] = None
        except (MarketForgeError, OSError, KeyError, ValueError, TypeError) as error:
            result["blocked_actions"].append({"target": key, "reasons": [str(error)]})
    async with database_connection(database_dsn(paths.root)) as connection:
        if await registry_id(connection) != registry:
            raise MarketForgeError("Testing registry changed during cleanup.")
        cursor = await connection.execute(
            """SELECT i.publication_id,s.publication_id
               FROM historical.publication_invalidations i
               JOIN historical.publication_supersessions s ON s.publication_id=i.source_publication_id
               WHERE s.evidence#>>'{force_plan,digest}'=%s ORDER BY i.publication_id""",
            (reviewed["plan_digest"],),
        )
        result["invalidated_downstream"] = [
            {"publication_id": identity, "source_publication_id": source}
            for identity, source in await cursor.fetchall()
        ]
        retired_publications: list[int] = []
        retired_conversion_refs: list[str] = []
        result["retired_publications"] = retired_publications
        result["retired_conversion_refs"] = retired_conversion_refs
        for plan in reviewed["targets"]:
            target = plan["target"]
            previous = await journal(connection, registry, target)
            if (
                previous
                and previous[2].get("force_plan", {}).get("digest") == reviewed["plan_digest"]
            ):
                field = (
                    "retired_conversion_refs"
                    if target.get("output_ref")
                    else "retired_publications"
                )
                key = target.get("output_ref") or target["publication_id"]
                result[field].append(key)
                if previous[2]["force_plan"].get("legacy_ownership", {}).get("risk_acknowledged"):
                    result["risk_acknowledged_publications"].append(key)
    changed = bool(retired_publications or retired_conversion_refs)
    result["status"] = (
        ("partial" if changed else "blocked") if result["blocked_actions"] else "complete"
    )
    write_document(output, result, replace=True)
    return result


async def inspect_conversion(paths: ProjectPaths, key: str) -> dict[str, Any]:
    force_cleanup.require_testing()
    async with database_connection(database_dsn(paths.root)) as connection:
        row = await journal(connection, await registry_id(connection), {"output_ref": key})
    if row is None:
        raise MarketForgeError("No recorded deletion for this conversion reference.")
    cleanup, state, evidence = row
    return {"output_ref": key, "cleanup_id": str(cleanup), "state": state, "evidence": evidence}
