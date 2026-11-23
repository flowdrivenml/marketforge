"""Reviewed testing-only cleanup for duplicate publications with downstream consumers."""

import asyncio
from contextlib import nullcontext
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

from psycopg import AsyncConnection
from psycopg.rows import TupleRow

from forge.datasets import canonical, deduplication, legacy_storage, publication_cleanup
from forge.shared.database.connection import (
    database_connection,
    database_dsn,
    database_environment,
    is_testing,
    pinned_registry,
    registry_id,
)
from forge.shared.errors import MarketForgeError
from forge.shared.ownership import acquire_file_lock
from forge.shared.paths import ProjectPaths
from forge.storage.cleanup_plans import write_document


def require_testing() -> None:
    if not is_testing():
        raise MarketForgeError("Force deduplication requires the verified testing registry.")


async def dependency_graph(connection: AsyncConnection[TupleRow], source: int) -> dict[str, Any]:
    """Follow exact contributed and selected registry file references transitively."""
    cursor = await connection.execute(
        """WITH RECURSIVE refs AS (
             SELECT input_dataset_id AS input, d.publication_id AS child, d.id AS consumer
             FROM historical.dataset_inputs i JOIN historical.datasets d ON d.id=i.dataset_id
             WHERE d.status<>'superseded'
             UNION
             SELECT d.id,p.id,NULL::bigint FROM historical.dataset_publications p
             CROSS JOIN LATERAL jsonb_array_elements_text(
               COALESCE(NULLIF(p.evidence->'selected_dataset_inputs','null'::jsonb),'[]'::jsonb)) v
             JOIN historical.datasets d ON d.id::text=v.value
             WHERE EXISTS (SELECT 1 FROM historical.datasets f
                           WHERE f.publication_id=p.id AND f.status<>'superseded')
           ), edges AS (
             SELECT d.publication_id AS parent, r.child, r.consumer, d.id AS input
             FROM refs r JOIN historical.datasets d ON d.id=r.input
           ), closure(id) AS (
             SELECT %s::bigint UNION SELECT e.child FROM edges e JOIN closure c ON c.id=e.parent
             WHERE e.child IS NOT NULL
           )
           SELECT e.parent,e.child,e.consumer,e.input,d.storage_path,p.evidence->>'manifest_sha256'
           FROM edges e JOIN closure c ON c.id=e.parent
           JOIN historical.datasets d ON d.id=e.input
           LEFT JOIN historical.dataset_publications p ON p.id=e.parent
           ORDER BY e.parent,e.child,e.consumer,e.input""",
        (source,),
    )
    edges = [
        dict(
            zip(
                (
                    "parent",
                    "child",
                    "consumer_file_id",
                    "input_file_id",
                    "input_path",
                    "manifest_sha256",
                ),
                row,
                strict=True,
            )
        )
        for row in await cursor.fetchall()
    ]
    ids = sorted({edge["child"] for edge in edges if edge["child"] is not None})
    cursor = await connection.execute(
        """SELECT p.id,p.operation,p.engine_dataset_id,p.storage_root,
                  p.evidence->>'manifest_sha256',p.evidence->'execution',p.evidence->'resolved_job',
                  COALESCE((SELECT jsonb_agg(jsonb_build_object('id', d.id, 'path',d.storage_path,
                           'sha256',d.file_sha256,'status',d.status) ORDER BY d.id)
                            FROM historical.datasets d WHERE d.publication_id=p.id),'[]'::jsonb),
                  EXISTS (SELECT 1 FROM historical.publication_supersessions s
                          WHERE s.canonical_id=p.id OR s.publication_id=p.id)
           FROM historical.dataset_publications p WHERE p.id=ANY(%s) ORDER BY p.id""",
        (ids,),
    )
    nodes: list[dict[str, Any]] = [
        dict(
            zip(
                (
                    "publication_id",
                    "operation",
                    "engine_publication_id",
                    "path",
                    "manifest_sha256",
                    "execution",
                    "job",
                    "files",
                    "protected_canonical",
                ),
                row,
                strict=True,
            )
        )
        for row in await cursor.fetchall()
    ]
    for node in nodes:
        node["decision"] = "invalidate_registry_keep_files"
    upstream_paths = {node["path"] for node in nodes}
    upstream_paths.update(
        str(Path(edge["input_path"]).parent) for edge in edges if edge["input_path"]
    )
    cursor = await connection.execute(
        "SELECT storage_root FROM historical.dataset_publications WHERE id=%s", (source,)
    )
    root = await cursor.fetchone()
    if root:
        upstream_paths.add(root[0])
    local_ids = {source, *ids}
    file_ids = {edge["input_file_id"] for edge in edges}
    file_ids.update(file["id"] for node in nodes for file in node["files"])
    cursor = await connection.execute(
        """SELECT work_key,evidence->>'path',result->'input',result->'external_reference',
                  result->>'result_path',result IS NULL
           FROM historical.resolved_work WHERE operation='convert' ORDER BY work_key"""
    )
    conversions: list[dict[str, Any]] = []
    for key, path, source_binding, reference, report, pending in await cursor.fetchall():
        matches: list[dict[str, Any]] = []
        bindings: tuple[object, object] = (source_binding, reference)
        malformed = not pending and not isinstance(source_binding, dict)
        for binding in bindings:
            if binding is None:
                continue
            if not isinstance(binding, dict):
                malformed = True
                continue
            item = cast(dict[str, Any], binding)
            normalized: object = item.get("normalized_inputs")
            if normalized is None:
                normalized = []
            if not isinstance(normalized, list) or any(
                not isinstance(entry, dict) for entry in cast(list[object], normalized)
            ):
                malformed = True
                continue
            upstream = [item, *cast(list[dict[str, Any]], normalized)]
            matches.extend(
                item
                for item in upstream
                if item.get("registry_publication_id") in local_ids
                or item.get("root") in upstream_paths
                or any(
                    publication_cleanup.contains_reference(item, Path(parent), file_ids, identity)
                    for parent in upstream_paths
                    for identity in local_ids
                )
            )
        if pending or matches or malformed:
            conversions.append(
                {
                    "work_key": key,
                    "path": path,
                    "report": report,
                    "bindings": matches,
                    "pending": pending,
                    "malformed": malformed,
                    "decision": "block_branch",
                }
            )
    return {
        "source": source,
        "edges": edges,
        "dependents": nodes,
        "conversion_references": conversions,
    }


def graph_blockers(graph: dict[str, Any], protected: set[int]) -> list[str]:
    blockers: list[str] = []
    if any(edge["child"] is None for edge in graph["edges"]):
        blockers.append("A downstream file has no attributable registry publication.")
    remaining = {
        (edge["parent"], edge["child"]) for edge in graph["edges"] if edge["child"] is not None
    }
    while remaining:
        start = {parent for parent, _ in remaining} - {child for _, child in remaining}
        if not start:
            blockers.append("Downstream registry lineage contains a cycle.")
            break
        remaining = {(parent, child) for parent, child in remaining if parent not in start}
    for reference in graph["conversion_references"]:
        blockers.append(
            f"Conversion {reference['work_key']} has an unresolved active claim."
            if reference["pending"]
            else f"Indexed conversion {reference['work_key']} depends on this branch and has no registry retirement record."
        )
    for node in graph["dependents"]:
        identity = node["publication_id"]
        if identity in protected or node["protected_canonical"]:
            blockers.append(f"Publication {identity} is a protected candidate or canonical output.")
        if (
            node["operation"] not in {"normalize", "merge"}
            or not node["files"]
            or any(file["status"] != "complete" for file in node["files"])
        ):
            blockers.append(f"Publication {identity} has an unresolved downstream state.")
    return blockers


async def revalidate_graph(
    connection: AsyncConnection[TupleRow], current: dict[str, Any], force_plan: dict[str, Any]
) -> bool:
    """Allow only dependencies already invalidated under the same reviewed plan."""
    reviewed = force_plan["graph"]
    if current == reviewed:
        return True
    cursor = await connection.execute(
        """SELECT i.publication_id FROM historical.publication_invalidations i
           JOIN historical.publication_supersessions s ON s.publication_id=i.source_publication_id
           WHERE s.evidence->'force_plan'->>'digest'=%s""",
        (force_plan["digest"],),
    )
    invalidated = {row[0] for row in await cursor.fetchall()}
    edges = [
        edge
        for edge in reviewed["edges"]
        if edge["child"] not in invalidated and edge["parent"] not in invalidated
    ]
    reachable = {reviewed["source"]}
    while True:
        expanded = reachable | {
            edge["child"]
            for edge in edges
            if edge["parent"] in reachable and edge["child"] is not None
        }
        if expanded == reachable:
            break
        reachable = expanded
    remaining = {
        **reviewed,
        "edges": [edge for edge in edges if edge["parent"] in reachable],
        "dependents": [
            node for node in reviewed["dependents"] if node["publication_id"] in reachable
        ],
    }
    return current == remaining


async def optimize_groups(connection: AsyncConnection[TupleRow], report: dict[str, Any]) -> None:
    groups = [group for group in report["groups"] if group["canonical_publication_id"] is not None]
    baseline = tuple(group["canonical_publication_id"] for group in groups)
    candidates: list[tuple[int, ...]] = []
    affected: dict[int, set[str]] = {}
    graphs: dict[int, dict[str, Any]] = {}
    for group in groups:
        ids = tuple(item["publication_id"] for item in group["candidates"])
        candidates.append(ids)
        for identity in ids:
            graph = await dependency_graph(connection, identity)
            graphs[identity] = graph
            affected[identity] = {
                *(f"publication:{node['publication_id']}" for node in graph["dependents"]),
                *(
                    f"conversion:{ref['work_key']}"
                    for ref in graph["conversion_references"]
                    if not ref["pending"] and ref["bindings"]
                ),
            }
    choices, metrics = canonical.optimize(candidates, affected, baseline)
    referenced = [sum(bool(affected[item]) for item in ids) for ids in candidates]
    report["canonical_optimization"] = {
        **metrics,
        "groups_with_no_referenced_candidate": referenced.count(0),
        "groups_with_one_referenced_candidate": referenced.count(1),
        "groups_with_multiple_referenced_candidates": sum(number > 1 for number in referenced),
        "groups_with_unavoidable_invalidation": sum(
            sum(not affected[item] for item in ids) < len(ids) - 1 for ids in candidates
        ),
    }
    all_lost = set[str]().union(
        *(
            affected[item]
            for ids, keep in zip(candidates, choices, strict=True)
            for item in ids
            if item != keep
        )
    )
    for group, ids, prior, keep in zip(groups, candidates, baseline, choices, strict=True):
        universe = set[str]().union(*(affected[item] for item in ids))
        group["canonical_publication_id"] = keep
        group["canonical_dependency_graph"] = graphs[keep]
        group["canonical_decision"] = {
            "previous": prior,
            "selected": keep,
            "downstream_preserved": len(universe - all_lost),
            "downstream_affected": len(universe & all_lost),
            "rationale": "Minimize distinct downstream invalidations across interacting groups, then retain prior choices.",
            "alternatives": [
                {
                    "publication_id": item,
                    "direct_downstream": len(
                        {edge["child"] for edge in graphs[item]["edges"] if edge["parent"] == item}
                    ),
                    "downstream_closure": len(affected[item]),
                }
                for item in ids
            ],
        }
        group["targets"] = [
            {"publication_id": item, "dependency_graph": graphs[item]}
            for item in ids
            if item != keep
        ]


async def review_target(
    paths: ProjectPaths,
    report: dict[str, Any],
    target: dict[str, Any],
    plan: dict[str, Any],
    protected: set[int],
    busy: bool,
    *,
    extra_blockers: tuple[str, ...] = (),
) -> None:
    graph = plan["dependency_graph"]
    blockers = [*extra_blockers, *graph_blockers(graph, protected)]
    plan.update(dependency_graph=graph, blockers=blockers, review=None)
    if busy:
        blockers.append("Testing registry has an active historical run.")
    other = report["other_registry_id"]
    risk_review = False
    if other is None:
        evidence = report["foreign_ownership_evidence"]
        ownership = await asyncio.to_thread(
            legacy_storage.ownership,
            evidence,
            legacy_storage.cleanup_paths(target, graph),
        )
        state = ownership["state"]
        plan["ownership"] = ownership
        if state == "unresolved_legacy_references":
            risk_review = True
        elif state != "verified_disjoint":
            blockers.append(f"Production ownership check: {state.replace('_', ' ')}.")
    else:
        with database_environment(testing=False), pinned_registry(other):
            try:
                async with database_connection(database_dsn(paths.root)) as foreign:
                    overlaps: list[str] = []
                    for item in [target, *graph["dependents"]]:
                        overlaps.extend(
                            await publication_cleanup.referenced_paths(
                                foreign, Path(item["path"]), include_lineage=True
                            )
                        )
                plan["ownership"] = {
                    "state": "known_foreign_reference" if overlaps else "verified_disjoint",
                    "paths": sorted(set(overlaps)),
                }
                if overlaps:
                    blockers.append(f"Other registry references {overlaps[0]}.")
            except MarketForgeError as error:
                plan["ownership"] = {"state": "unavailable"}
                blockers.append(str(error))
    if plan["ownership"]["state"] != "verified_disjoint":
        report["ownership_blocked_targets"].append(
            target.get("output_ref") or target["publication_id"]
        )
    try:
        storage = await asyncio.to_thread(
            legacy_storage.managed_storage, paths, target, report["registry_id"]
        )
        plan["managed_storage"] = str(storage)
    except MarketForgeError as error:
        blockers.append(str(error))
        storage = None
    artifacts = await asyncio.to_thread(
        publication_cleanup.cascade_artifact_dependencies,
        paths,
        target,
        graph["dependents"],
    )
    blockers.extend(f"Prepared, pending, or unindexed reference: {path}" for path in artifacts)
    plan["artifact_blockers"] = artifacts
    if not blockers and storage is not None:
        try:
            physical = await asyncio.to_thread(
                publication_cleanup.review_tree,
                ProjectPaths(paths.root, storage),
                target,
            )
            if risk_review:
                plan["legacy_risk_review"] = physical
                report["legacy_risk_targets"].append(
                    target.get("output_ref") or target["publication_id"]
                )
            else:
                plan["review"] = physical
        except (MarketForgeError, OSError, ValueError) as error:
            blockers.append(str(error))
    if risk_review:
        blockers.append("Unanchored legacy production paths require explicit risk acknowledgement.")


async def preview(paths: ProjectPaths, operation: str) -> dict[str, Any]:
    require_testing()
    report = await deduplication.inventory(paths, operation, apply=False)
    report.update(
        force=True,
        invalidated_downstream=[],
        ownership_blocked_targets=[],
        primary_files_removed=0,
        publication_directories_removed=0,
        legacy_risk_targets=[],
    )
    protected = {
        candidate["publication_id"]
        for group in report["groups"]
        for candidate in group["candidates"]
    }
    if report["other_registry_id"] is None:
        report["foreign_ownership_evidence"] = await legacy_storage.production_catalogue(paths)
    async with database_connection(database_dsn(paths.root)) as connection:
        if await registry_id(connection) != report["registry_id"]:
            raise MarketForgeError("Testing registry changed during planning.")
        active = await connection.execute(
            "SELECT 1 FROM historical.execution_runs WHERE status IN ('pending','running') LIMIT 1"
        )
        busy = bool(await active.fetchone())
        await optimize_groups(connection, report)
        for group in report["groups"]:
            for plan in group["targets"]:
                identity = plan["publication_id"]
                target = next(
                    item for item in group["candidates"] if item["publication_id"] == identity
                )
                await review_target(paths, report, target, plan, protected, busy)
            group["physical_removal_eligible"] = any(plan["review"] for plan in group["targets"])
    return report


async def apply(
    paths: ProjectPaths,
    operation: str,
    reviewed: dict[str, Any],
    *,
    accept_unverified_legacy_references: bool = False,
) -> dict[str, Any]:
    require_testing()
    if (
        not reviewed.get("force")
        or reviewed.get("operation") != operation
        or reviewed.get("mode") != "preview"
        or reviewed.get("plan_digest") != deduplication.report_digest(reviewed)
    ):
        raise MarketForgeError("Apply requires a force preview for the same operation.")
    report = await preview(paths, operation)
    if (report["registry_id"], report["other_registry_id"]) != (
        reviewed["registry_id"],
        reviewed["other_registry_id"],
    ):
        raise MarketForgeError("Registry identity changed since the reviewed force preview.")
    cleanup_id = uuid4()
    output = paths.runs_dir / f"deduplicate-{cleanup_id.hex}" / "cleanup-report.json"
    plan_digest = reviewed["plan_digest"]
    report.update(
        mode="apply",
        status="running",
        cleanup_id=str(cleanup_id),
        reviewed_plan=reviewed["report_path"],
        report_path=str(output),
        legacy_reference_risk_requested=accept_unverified_legacy_references,
        risk_acknowledged_publications=[],
    )
    write_document(output, report)
    fresh = {
        item["publication_id"]: (group, item)
        for group in report["groups"]
        for item in group["candidates"]
    }
    async with database_connection(database_dsn(paths.root)) as connection:
        pending = await connection.execute(
            """SELECT cleanup_id,evidence FROM historical.publication_supersessions
               WHERE state='retiring' AND evidence->'force_plan'->>'digest'=%s""",
            (plan_digest,),
        )
        resume = {
            evidence["target"]["publication_id"]: (owner, evidence)
            for owner, evidence in await pending.fetchall()
        }
    for group in reviewed["groups"]:
        for plan in group["targets"]:
            identity = plan["publication_id"]
            risk = bool(not plan["review"] and plan.get("legacy_risk_review"))
            if risk and not accept_unverified_legacy_references:
                continue
            physical = plan.get("legacy_risk_review") if risk else plan["review"]
            if not physical:
                continue
            record = resume.get(identity)
            try:
                if record:
                    owner, evidence = record
                    target, canonical = evidence["target"], evidence["canonical"]
                    force_plan = evidence["force_plan"]
                    if (
                        evidence["review"] != physical
                        or bool(force_plan.get("legacy_ownership", {}).get("risk_acknowledged"))
                        != risk
                    ):
                        raise MarketForgeError(
                            "Resume requires the original reviewed storage and risk decision."
                        )
                else:
                    owner = cleanup_id
                    current = fresh.get(identity)
                    if current is None:
                        prior = await deduplication.inspect_supersession(paths, identity)
                        if (
                            prior["state"] == "removed"
                            and prior["evidence"].get("force_plan", {}).get("digest") == plan_digest
                        ):
                            continue
                        raise MarketForgeError("Reviewed target is no longer available.")
                    current_group, current_target = current
                    target = next(
                        item for item in group["candidates"] if item["publication_id"] == identity
                    )
                    canonical = next(
                        item
                        for item in group["candidates"]
                        if item["publication_id"] == group["canonical_publication_id"]
                    )
                    current_canonical = fresh.get(canonical["publication_id"])
                    pins = (
                        "path",
                        "work_key",
                        "manifest_sha256",
                        "contract_sha256",
                        "inventory_sha256",
                    )
                    if (
                        current_group["classification"] != "proven_equivalent"
                        or current_canonical is None
                        or current_canonical[0] is not current_group
                        or any(
                            target[key] != current_target[key]
                            or canonical[key] != current_canonical[1][key]
                            for key in pins
                        )
                    ):
                        raise MarketForgeError(
                            "Publication equivalence or fingerprints changed since preview."
                        )
                    force_plan = {
                        "digest": plan_digest,
                        "graph": plan["dependency_graph"],
                        "managed_storage": plan["managed_storage"],
                        **(
                            {
                                "legacy_ownership": {
                                    "catalogue_digest": deduplication.report_digest(
                                        reviewed["foreign_ownership_evidence"]
                                    ),
                                    "classification": plan["ownership"],
                                    "risk_acknowledged": risk,
                                }
                            }
                            if reviewed["other_registry_id"] is None
                            else {}
                        ),
                        "equivalent_ids": sorted(
                            item["publication_id"] for item in group["candidates"]
                        ),
                    }
                verified = await asyncio.to_thread(
                    deduplication.verify_candidate, paths, report["registry_id"], canonical
                )
                if verified["manifest_sha256"] != canonical["manifest_sha256"]:
                    raise MarketForgeError("Canonical publication changed after review.")
                storage = await asyncio.to_thread(
                    legacy_storage.managed_storage, paths, target, report["registry_id"]
                )
                if str(storage) != force_plan["managed_storage"]:
                    raise MarketForgeError("Managed storage root changed after review.")
                lock = (
                    acquire_file_lock(storage / ".marketforge.lock")
                    if storage != paths.data_root
                    else nullcontext()
                )
                with lock:
                    await deduplication.guarded_removal(
                        paths,
                        report,
                        target,
                        canonical,
                        owner,
                        physical,
                        resume=bool(record),
                        force_plan=force_plan,
                    )
                deduplication.record_removal(
                    report, target, canonical, physical, resumed=bool(record)
                )
                report["primary_files_removed"] += len(target["files"])
                report["publication_directories_removed"] += 1
            except (MarketForgeError, OSError, ValueError, KeyError, TypeError) as error:
                report["blocked_actions"].append({"publication_id": identity, "reason": str(error)})
    async with database_connection(database_dsn(paths.root)) as connection:
        cursor = await connection.execute(
            """SELECT i.publication_id,i.source_publication_id FROM historical.publication_invalidations i
               JOIN historical.publication_supersessions s ON s.publication_id=i.source_publication_id
               WHERE s.evidence->'force_plan'->>'digest'=%s ORDER BY i.publication_id""",
            (plan_digest,),
        )
        report["invalidated_downstream"] = [
            {"publication_id": publication, "source_publication_id": source}
            for publication, source in await cursor.fetchall()
        ]
        cursor = await connection.execute(
            """SELECT publication_id FROM historical.publication_supersessions
               WHERE state='retiring' AND evidence->'force_plan'->>'digest'=%s""",
            (plan_digest,),
        )
        report["retired_publications"].extend(row[0] for row in await cursor.fetchall())
        cursor = await connection.execute(
            """SELECT publication_id FROM historical.publication_supersessions
               WHERE evidence->'force_plan'->>'digest'=%s
                 AND evidence#>'{force_plan,legacy_ownership,risk_acknowledged}'='true'::jsonb
               ORDER BY publication_id""",
            (plan_digest,),
        )
        report["risk_acknowledged_publications"] = [row[0] for row in await cursor.fetchall()]
    unresolved = (
        report["blocked_actions"]
        or report["unproven_conversion_paths"]
        or any(
            group["classification"] != "proven_equivalent"
            or any(
                not plan["review"]
                and not (accept_unverified_legacy_references and plan.get("legacy_risk_review"))
                for plan in group["targets"]
            )
            for group in reviewed["groups"]
        )
    )
    report["status"] = (
        ("partial" if report["retired_publications"] else "blocked") if unresolved else "complete"
    )
    write_document(output, report, replace=True)
    return report
