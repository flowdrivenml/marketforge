"""Inventory possible duplicate work without treating equal file hashes as proof."""

import asyncio
import json
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from psycopg.types.json import Jsonb

from forge.datasets import legacy_storage, publication_cleanup
from forge.historical.engine.managed import checksum
from forge.historical.engine.work import VERSION, identity, normalize_key
from forge.historical.normalization.execution import committed_job
from forge.historical.normalization.service import write_job
from forge.historical.profiles.service import discover_at
from forge.shared.database.connection import (
    database_connection,
    database_dsn,
    database_environment,
    database_purpose,
    is_testing,
    pinned_registry,
    registry_id,
    require_registry,
    selected_registry_id,
)
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths


def read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_bytes())


def execution_directory(paths: ProjectPaths, root: Path, owner: str, stage: str) -> Path:
    """Look only in the registered publication's own storage and known run layouts."""
    storage = root.parent.parent.parent
    roots = dict.fromkeys((paths.runs_dir, storage / "runs"))
    candidates = {
        match.parent
        for runs in roots
        for match in (
            runs / owner / "execution-intent.json",
            *runs.glob(f"*/steps/{stage}/{owner}/execution-intent.json"),
        )
        if match.is_file() and match.resolve() == match
    }
    if len(candidates) != 1:
        raise MarketForgeError(
            f"Expected one retained {stage} intent for {owner}; found {len(candidates)}."
        )
    return next(iter(candidates))


def verify_registration_receipt(
    directory: Path, intent: dict[str, Any], candidate: dict[str, Any]
) -> None:
    # Older testing intents predate registry UUID binding. The original complete
    # registration must bind the exact file identities now stored in this registry.
    storage = Path(candidate["path"]).parent.parent.parent
    receipts = [directory / "registration-result.json"]
    receipts.extend((storage / "runs").glob("normalize-recovery-*/registration-result.json"))
    if is_testing() and intent["environment"] == "testing":
        for source in receipts:
            if not source.is_file() or source.resolve() != source:
                continue
            receipt = read(source)
            registration = receipt.get("publications", {}).get(candidate["path"])
            if (
                registration
                and receipt["environment"] == "testing"
                and receipt["registration"] == "registered"
                and receipt["execution"] == candidate["execution"]
                and registration["publication_id"] == candidate["publication_id"]
                and registration["files"] == {f["path"]: f["id"] for f in candidate["files"]}
            ):
                return
    raise MarketForgeError(
        "No retained registration receipt binds the legacy intent to the exact testing file identities."
    )


def normalize_evidence(
    paths: ProjectPaths, registry: str, candidate: dict[str, Any]
) -> dict[str, Any]:
    execution: dict[str, Any] = candidate["execution"] or {}
    owner = execution.get("execution_id", "")
    if not owner or Path(owner).name != owner:
        raise MarketForgeError("No attributable managed execution.")
    root = Path(candidate["path"])
    directory = execution_directory(paths, root, owner, "normalize")
    intent = read(directory / "execution-intent.json")
    if intent.get("execution_id") != owner or intent.get("operation") != "normalize":
        raise MarketForgeError("Retained execution identity disagrees with registration.")
    if intent.get("registry_id") is not None:
        require_registry(intent, registry)
    else:
        verify_registration_receipt(directory, intent, candidate)
    submission = directory / "submitted-batch.json"
    if checksum(submission) != intent["submitted_sha256"] or (
        execution.get("submitted_sha256") != intent["submitted_sha256"]
    ):
        raise MarketForgeError("Submitted normalization hashes disagree.")
    binary = directory / "marketforge-normalize"
    if checksum(binary) != intent["executable_sha256"]:
        raise MarketForgeError("Pinned executable fingerprint disagrees.")
    available = [
        item
        for item in (directory / "normalize.toml", directory / "general.toml")
        if item.is_file() and checksum(item) == intent["configuration_sha256"]
    ]
    if len(available) != 1:
        raise MarketForgeError("Pinned runtime fingerprint disagrees.")
    batch = read(submission)
    job = next(
        entry
        for entry in batch["jobs"]
        if str(entry["dataset_id"]) == candidate["engine_publication_id"]
        and str(paths.root / entry["output"]["dataset_path"]) == candidate["path"]
    )
    if batch["instrument_namespace"] != candidate["namespace"]:
        raise MarketForgeError("Instrument namespace disagrees.")
    evidence = committed_job(paths, binary, batch, job, intent["submitted_sha256"])
    settings = read(root / "processing-runtime.json")
    if evidence["inventory_sha256"] != candidate["inventory_sha256"]:
        raise MarketForgeError("Registered publication and physical inventory disagree.")
    if any(
        evidence["inventory_sha256"].get(file["primary"]) != file["sha256"]
        for file in candidate["files"]
    ):
        raise MarketForgeError("Registered primary file fingerprints disagree.")
    raw = intent["source_sha256_by_job"][str(job["job_id"])]
    if raw != candidate["registered_raw_sha256"]:
        raise MarketForgeError(
            "Registered raw input fingerprints disagree with the retained intent."
        )
    profiles = [
        {"id": item["id"], "sha256": item["sha256"]}
        for item in discover_at(directory / "historical" / "profiles")
    ]
    recorded = intent["profiles"]
    if sorted(profiles, key=lambda item: item["id"]) != sorted(
        recorded, key=lambda item: item["id"]
    ):
        raise MarketForgeError("Retained profile bytes disagree with the intent.")
    key = normalize_key(
        registry, intent["executable_sha256"], batch, job, raw, settings, intent["profiles"]
    )
    manifest = evidence["manifest"]
    # Manifest identities and measured processing metrics are execution metadata.
    # Ordering guarantees, integrity, streams, membership and auxiliary contracts remain.
    semantic_manifest = {
        field: value
        for field, value in manifest.items()
        if field not in {"job_id", "dataset_id", "processing_metrics"}
    }
    return {
        "verification": "verified",
        "work_key": key,
        "engine_sha256": intent["executable_sha256"],
        "runtime_sha256": intent["configuration_sha256"],
        "current_profile_order_key": normalize_key(
            registry, intent["executable_sha256"], batch, job, raw, settings, profiles
        ),
        "work_without_engine_sha256": normalize_key(
            registry, "compare-only", batch, job, raw, settings, intent["profiles"]
        ),
        "manifest_sha256": evidence["manifest_sha256"],
        "logical_bytes": evidence["bytes_written"],
        "submitted_sha256": intent["submitted_sha256"],
        "started_at": intent.get("started_at"),
        "evidence_source": str(directory / "execution-intent.json"),
        "registry_binding": "intent" if intent.get("registry_id") else "legacy_registration",
        "contract_sha256": identity(
            "normalize-output",
            registry,
            "manifest-contract-v1",
            {
                "manifest": semantic_manifest,
                "data": {
                    item["path"]: evidence["inventory_sha256"][item["path"]]
                    for item in manifest["files"] + manifest.get("auxiliary_files", [])
                },
            },
        ),
        "checksum_index_sha256": checksum(root / "publication-checksums.json"),
        "integrity_status": manifest["integrity_status"],
        "reuse_result": {
            "path": candidate["path"],
            "entry": {
                **manifest,
                "path": candidate["path"],
                "status": "complete",
                "verification": "verified",
                "registration": "registered",
                "manifest_sha256": evidence["manifest_sha256"],
                "bytes_written": evidence["bytes_written"],
            },
            "execution_id": owner,
            "submission_path": str(submission),
            "submitted_sha256": intent["submitted_sha256"],
            "publications": {
                candidate["path"]: {
                    "publication_id": candidate["publication_id"],
                    "files": {item["path"]: item["id"] for item in candidate["files"]},
                }
            },
        },
    }


def merge_evidence(paths: ProjectPaths, registry: str, candidate: dict[str, Any]) -> dict[str, Any]:
    from forge.historical.engine.work import merge_key
    from forge.historical.merge.service import verify_output

    execution: dict[str, Any] = candidate["execution"] or {}
    owner = execution.get("execution_id")
    if not isinstance(owner, str) or not owner or Path(owner).name != owner:
        raise MarketForgeError("No attributable managed merge execution.")
    root = Path(candidate["path"])
    directory = execution_directory(paths, root, owner, "merge")
    source = directory / "execution-intent.json"
    intent = read(source)
    require_registry(intent, registry)
    submission = directory / "submitted-merge.json"
    executable = directory / "marketforge-merge"
    if (
        intent.get("execution_id") != owner
        or intent.get("operation") != "merge"
        or intent["submitted_sha256"] != execution["submitted_sha256"]
        or checksum(submission) != intent["submitted_sha256"]
        or checksum(executable) != intent["executable_sha256"]
    ):
        raise MarketForgeError("Submitted merge evidence or executable disagrees with the intent.")
    job = read(submission)
    runtime = read(root / "merge-runtime.json")
    if runtime != intent["effective_runtime"]:
        raise MarketForgeError("Merged runtime sidecar disagrees with the pinned settings.")
    verified = verify_output(paths, root, executable, intent["inputs"], job)
    if (
        verified["engine_dataset_id"] != candidate["engine_publication_id"]
        or verified["instrument_namespace"] != candidate["namespace"]
        or verified["inventory_sha256"] != candidate["inventory_sha256"]
        or any(
            verified["inventory_sha256"].get(f["primary"]) != f["sha256"]
            for f in candidate["files"]
        )
    ):
        raise MarketForgeError("Registered merge identities or inventory disagree.")
    manifest = verified["manifest"]
    receipt = read(directory / "execution-result.json")
    if receipt.get("submitted_sha256") != intent["submitted_sha256"]:
        raise MarketForgeError("Merge execution result is not bound to the verified submission.")
    return {
        "verification": "verified",
        "work_key": merge_key(
            registry, intent["executable_sha256"], job, runtime, intent["inputs"]
        ),
        "engine_sha256": intent["executable_sha256"],
        "runtime_sha256": intent["configuration_sha256"],
        "manifest_sha256": verified["manifest_sha256"],
        "logical_bytes": verified["bytes_written"],
        "submitted_sha256": intent["submitted_sha256"],
        "started_at": receipt.get("started_at"),
        "evidence_source": str(source),
        "registry_binding": "intent",
        "contract_sha256": identity(
            "merge-output",
            registry,
            "manifest-contract-v1",
            {
                "manifest": {
                    k: v for k, v in manifest.items() if k not in {"job_id", "processing_metrics"}
                },
                "data": {
                    item["path"]: verified["inventory_sha256"][item["path"]]
                    for item in manifest["files"]
                },
            },
        ),
        "checksum_index_sha256": checksum(root / "publication-checksums.json"),
        "reuse_result": {
            **receipt,
            "path": str(root),
            "status": "complete",
            "verification": "verified",
            "registration": "registered",
            "dataset": {
                **manifest,
                "manifest_sha256": verified["manifest_sha256"],
                "engine_dataset_id": verified["engine_dataset_id"],
                "bytes_written": verified["bytes_written"],
            },
            "publications": {
                str(root): {
                    "publication_id": candidate["publication_id"],
                    "files": {item["path"]: item["id"] for item in candidate["files"]},
                }
            },
        },
    }


def verify_candidate(paths: ProjectPaths, registry: str, item: dict[str, Any]) -> dict[str, Any]:
    if item["operation"] == "normalize":
        return normalize_evidence(paths, registry, item)
    if item["operation"] == "merge":
        return merge_evidence(paths, registry, item)
    raise MarketForgeError("No verified historical work adapter for this publication.")


def inspect_unindexed_conversion(paths: ProjectPaths, root: Path) -> dict[str, Any]:
    evidence: dict[str, Any] = {
        "path": str(root),
        "engine_publication_id": root.name,
        "classification": "insufficient_historical_evidence",
        "reason": "No registry-bound submitted conversion and pinned processing executable were found.",
    }
    try:
        manifest = read(root / "manifest.json")
        effective = read(root / "conversion-runtime.json")
        evidence["manifest_sha256"] = checksum(root / "manifest.json")
        evidence["engine_publication_id"] = str(manifest["job_id"])
        evidence["integrity"] = manifest["integrity"]["status"]
        evidence["input"] = manifest["input"]
        evidence["conversion"] = manifest["conversion"]
        jobs: list[dict[str, Any]] = []
        for file in paths.runs_dir.glob(f"convert-prepare-*/job-{root.name}.json"):
            if file.resolve() == file and read(file).get("job_id") == manifest["job_id"]:
                jobs.append(
                    {
                        "path": str(file),
                        "sha256": checksum(file),
                        "same_effective_job": read(file) == effective["job"],
                    }
                )
        evidence["prepared_jobs"] = jobs
    except (OSError, ValueError, KeyError, TypeError) as error:
        evidence["inspection_error"] = str(error)
    return evidence


async def inventory(paths: ProjectPaths, operation: str, *, apply: bool) -> dict[str, Any]:
    environment = database_purpose()
    selected_testing = is_testing()
    other_registry_error = None
    other_registry = None
    with database_environment(testing=not selected_testing), pinned_registry(None):
        try:
            other_registry = await selected_registry_id(paths.root)
        except MarketForgeError as error:
            other_registry_error = str(error)

    async with database_connection(database_dsn(paths.root)) as connection:
        registry = await registry_id(connection)
        saved = await connection.execute(
            "SELECT publication_id,canonical_id,state FROM historical.publication_supersessions"
        )
        aliases = {row[0]: (row[1], row[2]) for row in await saved.fetchall()}
        indexed_work = await connection.execute(
            """SELECT work_key,result->>'path' FROM historical.resolved_work
               WHERE registry_id=%s AND result IS NOT NULL""",
            (registry,),
        )
        index_paths = dict(await indexed_work.fetchall())
        cursor = await connection.execute(
            """SELECT p.id,p.operation,p.engine_dataset_id,p.instrument_namespace,p.storage_root,
                      p.evidence->'execution',p.evidence->'inventory_sha256',p.evidence->'raw_sha256',
                      jsonb_agg(jsonb_build_object('id',d.id,'primary',d.primary_file,
                                'path',d.storage_path,'sha256',d.file_sha256,
                                'dependants', ARRAY(
                                    SELECT parent.id FROM historical.dataset_inputs i
                                    JOIN historical.datasets parent ON parent.id=i.dataset_id
                                    WHERE i.input_dataset_id=d.id AND parent.status<>'superseded'
                                    ORDER BY parent.id
                                )) ORDER BY d.id)
               FROM historical.dataset_publications p
               JOIN historical.datasets d ON d.publication_id=p.id
               WHERE p.operation = ANY(%s) AND d.status='complete' AND d.primary_file IS NOT NULL
               GROUP BY p.id ORDER BY p.id""",
            (["normalize", "merge", "convert"] if operation == "all" else [operation],),
        )
        grouped: dict[tuple[str, str, tuple[str, ...]], list[dict[str, Any]]] = {}
        counts: dict[str, int] = {}
        for (
            local,
            op,
            engine,
            namespace,
            path,
            execution,
            fingerprints,
            raw_fingerprints,
            files,
        ) in await cursor.fetchall():
            counts[op] = counts.get(op, 0) + 1
            if not all(item["sha256"] for item in files):
                continue
            group = (op, namespace, tuple(sorted(item["sha256"] for item in files)))
            grouped.setdefault(group, []).append(
                {
                    "publication_id": local,
                    "operation": op,
                    "engine_publication_id": engine,
                    "namespace": namespace,
                    "path": path,
                    "execution": execution,
                    "inventory_sha256": fingerprints,
                    "registered_raw_sha256": raw_fingerprints,
                    "files": files,
                }
            )
        indexed = await connection.execute(
            """SELECT work_key,owner,evidence,result IS NOT NULL FROM historical.resolved_work
               WHERE registry_id=%s AND operation='convert' ORDER BY created_at""",
            (registry,),
        )
        converted_index = [
            {"work_key": key, "owner": owner, "evidence": evidence, "completed": ready}
            for key, owner, evidence, ready in await indexed.fetchall()
        ]

    groups: list[dict[str, Any]] = []
    for (op, _, _), candidates in grouped.items():
        if len(candidates) < 2:
            continue
        for candidate in candidates:
            try:
                candidate.update(
                    await asyncio.to_thread(verify_candidate, paths, registry, candidate)
                )
            except (
                MarketForgeError,
                OSError,
                ValueError,
                KeyError,
                TypeError,
                StopIteration,
            ) as error:
                candidate.update(
                    verification="unproven", reason=str(error) or "Missing job binding."
                )
        keys = {item.get("work_key") for item in candidates}
        downstream = sorted(
            {
                dependant
                for item in candidates
                for file in item["files"]
                for dependant in file["dependants"]
            }
        )
        canonical = None
        targets: list[dict[str, Any]] = []
        contracts = {item.get("contract_sha256") for item in candidates}
        if None in keys:
            classification = "insufficient_historical_evidence"
            reason = "Required execution or verification evidence could not be established."
        elif len(keys) == 1 and len(contracts) != 1:
            classification = "semantically_different"
            reason = "The verified output contracts differ despite the matching work key."
        elif len(keys) > 1:
            if len({item["engine_sha256"] for item in candidates}) > 1:
                classification = "physically_identical_but_semantics_unproven"
                reason = "Primary bytes match, but no scoped processing compatibility proof joins the pinned Engine builds."
            else:
                classification = "semantically_different"
                reason = "The resolved processing inputs or settings differ."
        else:
            classification = "proven_equivalent"
            reason = "Pinned processing inputs, runtime, Engine and output contract agree."
            established = {
                aliases[item["publication_id"]][0]
                for item in candidates
                if item["publication_id"] in aliases
            }
            established.update(
                item["publication_id"]
                for item in candidates
                if item["path"]
                in (
                    index_paths.get(item["work_key"]),
                    index_paths.get(item.get("current_profile_order_key", item["work_key"])),
                )
            )
            if established and (
                len(established) != 1
                or not established <= {item["publication_id"] for item in candidates}
            ):
                reason = "Previously recorded canonical choices disagree with the current group."
                classification = "invalid_or_corrupt"
            else:
                canonical = min(
                    candidates,
                    key=lambda item: (
                        item["publication_id"] not in established,
                        not any(file["dependants"] for file in item["files"]),
                        item.get("started_at") or "9999",
                        item["manifest_sha256"],
                        item["publication_id"],
                    ),
                )
                for item in candidates:
                    if item is canonical:
                        continue
                    blocker_reasons: list[str] = []
                    target: dict[str, Any] = {
                        "publication_id": item["publication_id"],
                        "blockers": blocker_reasons,
                        "review": None,
                    }
                    if any(file["dependants"] for file in item["files"]):
                        blocker_reasons.append(
                            "Registered downstream files require the original publication."
                        )
                    if other_registry is None:
                        blocker_reasons.append(
                            "Other registry identity and references could not be verified."
                        )
                    if not blocker_reasons:
                        try:
                            target["review"] = await asyncio.to_thread(
                                publication_cleanup.review_tree, paths, item
                            )
                            dependencies = await asyncio.to_thread(
                                publication_cleanup.local_artifact_dependencies, paths, item
                            )
                            blocker_reasons.extend(dependencies)
                            root = Path(item["path"])
                            assert isinstance(other_registry, str)
                            with (
                                database_environment(testing=not selected_testing),
                                pinned_registry(other_registry),
                            ):
                                async with database_connection(database_dsn(paths.root)) as foreign:
                                    overlaps = await publication_cleanup.referenced_paths(
                                        foreign, root
                                    )
                            if overlaps:
                                blocker_reasons.append(
                                    f"Other registry references this directory: {overlaps[0]}"
                                )
                            if blocker_reasons:
                                target["review"] = None
                        except (MarketForgeError, OSError, ValueError) as error:
                            target["review"] = None
                            blocker_reasons.append(str(error))
                    targets.append(target)
        groups.append(
            {
                "operation": op,
                "candidates": candidates,
                "disposition": "retained",
                "classification": classification,
                "canonical_publication_id": canonical["publication_id"] if canonical else None,
                "targets": targets,
                "reason": reason,
                "registered_downstream_file_ids": downstream,
                "physical_removal_eligible": any(target["review"] for target in targets),
            }
        )
    converted_root = paths.datasets_dir / "converted"
    indexed_paths = {row["evidence"].get("path") for row in converted_index}
    unindexed = (
        [
            str(entry)
            for entry in sorted(converted_root.iterdir())
            if entry.is_dir() and str(entry) not in indexed_paths
        ]
        if operation in {"all", "convert"} and converted_root.is_dir()
        else []
    )
    return {
        "schema_version": 1,
        "work_identity_version": VERSION,
        "environment": environment,
        "registry_id": registry,
        "operation": operation,
        "other_registry_error": other_registry_error,
        "other_registry_id": other_registry,
        "mode": "apply" if apply else "preview",
        "applied_actions": [],
        "omitted_from_selection": [],
        "blocked_actions": [],
        "status": "preview",
        "registry_publications": counts,
        "groups": groups,
        "conversion_index": converted_index if operation in {"all", "convert"} else [],
        "unproven_conversion_paths": unindexed,
        "unproven_conversion_evidence": [
            await asyncio.to_thread(inspect_unindexed_conversion, paths, Path(item))
            for item in unindexed
        ],
        "requested_apply": apply,
        "removed_publications": [],
        "retired_publications": [],
        "physical_files_removed": 0,
        "storage_reclaimed_bytes": 0,
        "removed_file_allocation_bytes": 0,
    }


def retain_report(paths: ProjectPaths, report: dict[str, Any]) -> None:
    prefix = "publication-delete" if report.get("operation") == "explicit_delete" else "deduplicate"
    destination = paths.runs_dir / f"{prefix}-{uuid4().hex}" / "cleanup-report.json"
    report["report_path"] = str(destination)
    write_job(destination, report)


def report_digest(report: dict[str, Any]) -> str:
    from forge.storage.cleanup_plans import digest

    return digest({k: v for k, v in report.items() if k not in {"report_path", "plan_digest"}})


def save_preview(paths: ProjectPaths, report: dict[str, Any]) -> None:
    report["plan_digest"] = report_digest(report)
    retain_report(paths, report)


def load_preview(paths: ProjectPaths, source: Path) -> dict[str, Any]:
    if (
        source.resolve() != source.absolute()
        or source.parent.parent.absolute() != paths.runs_dir
        or not source.parent.name.startswith(("deduplicate-", "publication-delete-"))
        or source.name != "cleanup-report.json"
    ):
        raise MarketForgeError("Use an exact managed publication cleanup preview report.")
    report = read(source)
    if (
        report.get("mode") != "preview"
        or report.get("environment") != database_purpose()
        or report.get("plan_digest") != report_digest(report)
    ):
        raise MarketForgeError("The preview changed or belongs to another environment.")
    return report


async def backfill(
    paths: ProjectPaths,
    registry: str,
    op: str,
    canonical: dict[str, Any],
    candidates: list[dict[str, Any]],
) -> None:
    """Record reconstructed evidence separately and seed the ordinary verified work resolver."""
    async with database_connection(database_dsn(paths.root)) as connection:
        async with connection.transaction():
            for item in candidates:
                await connection.execute(
                    """INSERT INTO historical.publication_work_evidence(publication_id,work_key,evidence)
                       VALUES (%s,%s,%s)
                       ON CONFLICT (publication_id) DO NOTHING""",
                    (
                        item["publication_id"],
                        item["work_key"],
                        Jsonb(
                            {
                                "version": VERSION,
                                "verification": item["verification"],
                                "source": item["evidence_source"],
                                "registry_binding": item["registry_binding"],
                                "engine_sha256": item["engine_sha256"],
                                "runtime_sha256": item["runtime_sha256"],
                                "contract_sha256": item["contract_sha256"],
                                "manifest_sha256": item["manifest_sha256"],
                                "canonical_candidate": canonical["publication_id"],
                            }
                        ),
                    ),
                )
            result = canonical["reuse_result"]
            for equivalent_key in dict.fromkeys(
                (
                    canonical["work_key"],
                    canonical.get("current_profile_order_key", canonical["work_key"]),
                )
            ):
                await connection.execute(
                    """INSERT INTO historical.resolved_work(registry_id,operation,work_key,owner,evidence,result)
                   VALUES (%s,%s,%s,%s,%s,%s)
                   ON CONFLICT DO NOTHING""",
                    (
                        registry,
                        op,
                        equivalent_key,
                        result["execution_id"],
                        Jsonb(
                            {
                                "path": result["path"],
                                "report_path": canonical["evidence_source"],
                                "source": "historical_reconstruction",
                            }
                        ),
                        Jsonb(result),
                    ),
                )


@asynccontextmanager
async def foreign_clearance(
    paths: ProjectPaths,
    report: dict[str, Any],
    root: Path,
    target: dict[str, Any],
    force_plan: dict[str, Any] | None,
) -> AsyncGenerator[None]:
    managed = report.get("other_registry_id")
    if not managed:
        if not is_testing() or not force_plan or not force_plan.get("legacy_ownership"):
            raise MarketForgeError("Other registry identity and references could not be verified.")
        from psycopg import Error

        reviewed = force_plan["legacy_ownership"]
        try:
            async with legacy_storage.catalogue(paths, lock=True) as evidence:
                if report_digest(evidence) != reviewed["catalogue_digest"]:
                    raise MarketForgeError(
                        "Legacy production storage references changed after review."
                    )
                ownership = await asyncio.to_thread(
                    legacy_storage.ownership,
                    evidence,
                    legacy_storage.cleanup_paths(target, force_plan["graph"]),
                )
                if ownership != reviewed["classification"] or (
                    ownership["state"] != "verified_disjoint"
                    and not (
                        ownership["state"] == "unresolved_legacy_references"
                        and reviewed["risk_acknowledged"]
                    )
                ):
                    raise MarketForgeError("Legacy production references prevent physical cleanup.")
                yield
        except Error as error:
            raise MarketForgeError(
                f"Could not lock or verify the legacy production catalogue (SQLSTATE {error.sqlstate or 'unavailable'})."
            ) from None
        return

    other_testing = not is_testing()
    with database_environment(testing=other_testing), pinned_registry(managed):
        async with database_connection(database_dsn(paths.root)) as other:
            async with other.transaction():
                await other.execute(
                    """LOCK TABLE historical.dataset_publications, historical.datasets,
                       historical.resolved_work, historical.execution_runs IN SHARE MODE"""
                )
                foreign = await publication_cleanup.referenced_paths(
                    other, root, include_lineage=force_plan is not None
                )
                if force_plan:
                    for node in force_plan["graph"]["dependents"]:
                        foreign.extend(
                            await publication_cleanup.referenced_paths(
                                other, Path(node["path"]), include_lineage=True
                            )
                        )
                if foreign:
                    raise MarketForgeError(
                        f"Other registry references the publication: {foreign[0]}"
                    )
                active = await other.execute(
                    "SELECT 1 FROM historical.execution_runs WHERE status IN ('pending','running') LIMIT 1"
                )
                if await active.fetchone():
                    raise MarketForgeError("Other registry has an active historical run.")
                with (
                    database_environment(testing=not other_testing),
                    pinned_registry(report["registry_id"]),
                ):
                    yield


async def guarded_removal(
    paths: ProjectPaths,
    report: dict[str, Any],
    target: dict[str, Any],
    canonical: dict[str, Any] | None,
    cleanup_id: UUID,
    review: dict[str, Any],
    *,
    resume: bool = False,
    force_plan: dict[str, Any] | None = None,
) -> None:
    """Hold the foreign registry's reference tables stable through removal."""
    local_dsn = database_dsn(paths.root)
    root = Path(review["root"])
    async with (
        database_connection(local_dsn) as local,
        foreign_clearance(paths, report, root, target, force_plan),
    ):
        if report["registry_id"] != await registry_id(local):
            raise MarketForgeError("Selected registry changed after review.")
        aliases = await publication_cleanup.referenced_paths(
            local, root, target["publication_id"], excluded_output_ref=target.get("output_ref")
        )
        if aliases:
            raise MarketForgeError(
                f"Selected registry also references this directory: {aliases[0]}"
            )
        active = await local.execute(
            "SELECT 1 FROM historical.execution_runs WHERE status IN ('pending','running') LIMIT 1"
        )
        if await active.fetchone():
            raise MarketForgeError("Selected registry has an active historical run.")
        if force_plan is not None:
            dependencies = await asyncio.to_thread(
                publication_cleanup.cascade_artifact_dependencies,
                paths,
                target,
                force_plan["graph"]["dependents"],
            )
            if resume and not target.get("output_ref"):
                from forge.datasets.force_cleanup import dependency_graph

                current = await dependency_graph(local, target["publication_id"])
                if current["edges"] or current["conversion_references"]:
                    raise MarketForgeError("New dependencies prevent resuming force cleanup.")
        else:
            dependencies = await asyncio.to_thread(
                publication_cleanup.local_artifact_dependencies, paths, target
            )
        if dependencies:
            raise MarketForgeError(f"Local artifact refers to the publication: {dependencies[0]}")
        if not resume:
            storage = Path(force_plan["managed_storage"]) if force_plan else paths.data_root
            fresh = await asyncio.to_thread(
                publication_cleanup.review_tree,
                ProjectPaths(paths.root, storage),
                target,
            )
            if fresh != review:
                raise MarketForgeError("Physical inventory changed after review.")
            await publication_cleanup.claim(
                local,
                target,
                canonical,
                cleanup_id,
                review,
                [],
                force_plan=force_plan,
            )
        await asyncio.to_thread(publication_cleanup.remove_reviewed_tree, review)
        if await asyncio.to_thread(root.exists):
            raise MarketForgeError("Directory still exists after removal.")
        survivors = [canonical] if canonical else (force_plan or {}).get("retained", [])
        for survivor in survivors:
            check = await asyncio.to_thread(
                verify_candidate, paths, report["registry_id"], survivor
            )
            if check["manifest_sha256"] != survivor["manifest_sha256"]:
                raise MarketForgeError("Retained publication manifest changed during removal.")
        if output_ref := target.get("output_ref"):
            await local.execute(
                """UPDATE historical.converted_output_deletions
                   SET state='removed',removed_at=clock_timestamp()
                   WHERE registry_id=%s AND work_key=%s AND cleanup_id=%s AND state='retiring'""",
                (report["registry_id"], output_ref, cleanup_id),
            )
        else:
            await publication_cleanup.finish(local, target["publication_id"])


async def apply_inventory(
    paths: ProjectPaths, operation: str, *, preview: dict[str, Any] | None = None
) -> dict[str, Any]:
    if preview and preview.get("operation") == "explicit_delete":
        raise MarketForgeError("Use datasets delete to apply an explicit deletion plan.")
    if preview and preview.get("force"):
        raise MarketForgeError("A force preview cannot be applied by ordinary deduplication.")
    report = await inventory(paths, operation, apply=True)
    cleanup_id = uuid4()
    report["cleanup_id"] = str(cleanup_id)
    report["status"] = "running"
    report_path = paths.runs_dir / f"deduplicate-{cleanup_id.hex}" / "cleanup-report.json"
    report["report_path"] = str(report_path)
    from forge.storage.cleanup_plans import write_document

    write_document(report_path, report)
    registry = report["registry_id"]
    if preview and (preview["registry_id"], preview["operation"]) != (registry, operation):
        raise MarketForgeError("The preview belongs to another registry or operation.")
    reviewed = {
        item["publication_id"]: (
            group["canonical_publication_id"],
            item["review"],
            [
                (candidate["publication_id"], candidate["work_key"], candidate["manifest_sha256"])
                for candidate in group["candidates"]
            ],
        )
        for group in (preview or report)["groups"]
        for item in group["targets"]
    }
    errors = (MarketForgeError, OSError, ValueError, KeyError, TypeError)
    async with database_connection(database_dsn(paths.root)) as connection:
        pending = await connection.execute(
            """SELECT s.cleanup_id,s.evidence FROM historical.publication_supersessions s
               JOIN historical.dataset_publications p ON p.id=s.publication_id
               WHERE s.state='retiring' AND NOT (s.evidence ? 'force_plan') AND (%s='all' OR p.operation=%s)""",
            (operation, operation),
        )
        for previous_id, evidence in await pending.fetchall():
            target, canonical, review = (evidence[key] for key in ("target", "canonical", "review"))
            try:
                await asyncio.to_thread(verify_candidate, paths, registry, canonical)
                await guarded_removal(
                    paths, report, target, canonical, previous_id, review, resume=True
                )
            except errors as error:
                report["blocked_actions"].append(
                    {"publication_id": target["publication_id"], "reason": str(error)}
                )
            else:
                record_removal(report, target, canonical, review, resumed=True)
    for group in report["groups"]:
        candidates = group["candidates"]
        canonical = next(
            (
                item
                for item in candidates
                if item["publication_id"] == group["canonical_publication_id"]
            ),
            None,
        )
        if canonical is None:
            # Verified singletons in unresolved groups can also seed their exact work key.
            for item in candidates:
                if (
                    item["verification"] == "verified"
                    and sum(other.get("work_key") == item["work_key"] for other in candidates) == 1
                ):
                    await backfill(paths, registry, group["operation"], item, [item])
            continue
        await backfill(paths, registry, group["operation"], canonical, candidates)
        for plan in group["targets"]:
            target_id = plan["publication_id"]
            target = next(item for item in candidates if item["publication_id"] == target_id)
            try:
                if reviewed.get(target_id) != (
                    canonical["publication_id"],
                    plan["review"],
                    [
                        (item["publication_id"], item["work_key"], item["manifest_sha256"])
                        for item in candidates
                    ],
                ):
                    raise MarketForgeError("Candidate differs from the reviewed preview.")
                verify = [target, canonical] if plan["review"] else []
                for item in verify:
                    fresh = await asyncio.to_thread(verify_candidate, paths, registry, item)
                    if any(
                        fresh[field] != item[field]
                        for field in (
                            "work_key",
                            "manifest_sha256",
                            "contract_sha256",
                        )
                    ):
                        raise MarketForgeError("Verified publication changed after review.")
                if plan["review"]:
                    await guarded_removal(
                        paths, report, target, canonical, cleanup_id, plan["review"]
                    )
                    record_removal(report, target, canonical, plan["review"])
                else:
                    async with database_connection(database_dsn(paths.root)) as connection:
                        created = await publication_cleanup.claim(
                            connection,
                            target,
                            canonical,
                            cleanup_id,
                            None,
                            plan["blockers"],
                        )
                    if not created:
                        continue
                    report["omitted_from_selection"].append(target_id)
                    report["applied_actions"].append(
                        {
                            "publication_id": target_id,
                            "canonical_id": canonical["publication_id"],
                            "state": "retained",
                        }
                    )
            except errors as error:
                report["blocked_actions"].append(
                    {"publication_id": target_id, "reason": str(error)}
                )
                async with database_connection(database_dsn(paths.root)) as connection:
                    row = await connection.execute(
                        """SELECT 1 FROM historical.publication_supersessions
                           WHERE publication_id=%s AND cleanup_id=%s AND state='retiring'""",
                        (target_id, cleanup_id),
                    )
                    if await row.fetchone():
                        report["retired_publications"].append(target_id)
                        report["omitted_from_selection"].append(target_id)
                        report["applied_actions"].append(
                            {
                                "publication_id": target_id,
                                "canonical_id": canonical["publication_id"],
                                "state": "retiring",
                            }
                        )
    unresolved = bool(report["unproven_conversion_paths"]) or any(
        group["classification"] != "proven_equivalent"
        or any(target["blockers"] for target in group["targets"])
        for group in report["groups"]
    )
    if unresolved or report["blocked_actions"]:
        report["status"] = "partial" if report["applied_actions"] else "blocked"
    else:
        report["status"] = "complete"
    write_document(report_path, report, replace=True)
    return report


def record_removal(
    report: dict[str, Any],
    target: dict[str, Any],
    canonical: dict[str, Any],
    review: dict[str, Any],
    *,
    resumed: bool = False,
) -> None:
    identity = target["publication_id"]
    report["removed_publications"].append(identity)
    report["retired_publications"].append(identity)
    report["omitted_from_selection"].append(identity)
    report["physical_files_removed"] += len(review["files"])
    # Filesystems can share extents through reflinks or retain snapshots.
    report["storage_reclaimed_bytes"] = None
    report["removed_file_allocation_bytes"] += review["physical_bytes"]
    report["applied_actions"].append(
        {
            "publication_id": identity,
            "canonical_id": canonical["publication_id"],
            "state": "removed",
            "resumed": resumed,
            "path": review["root"],
        }
    )


async def inspect_supersession(paths: ProjectPaths, publication: int) -> dict[str, Any]:
    async with database_connection(database_dsn(paths.root)) as connection:
        cursor = await connection.execute(
            """SELECT canonical_id,state,cleanup_id,evidence,created_at,removed_at
               FROM historical.publication_supersessions WHERE publication_id=%s""",
            (publication,),
        )
        record = await cursor.fetchone()
        if record is None:
            cursor = await connection.execute(
                """SELECT source_publication_id,cleanup_id,evidence,invalidated_at
                   FROM historical.publication_invalidations WHERE publication_id=%s""",
                (publication,),
            )
            invalidation = await cursor.fetchone()
            if invalidation:
                source, cleanup, evidence, recorded = invalidation
                return {
                    "schema_version": 1,
                    "environment": database_purpose(),
                    "publication_id": publication,
                    "canonical_id": None,
                    "source_publication_id": source,
                    "state": "invalidated",
                    "cleanup_id": str(cleanup),
                    "recorded_at": recorded.isoformat(),
                    "removed_at": None,
                    "evidence": evidence,
                }
    if record is None:
        raise MarketForgeError(f"Registry publication {publication} has no recorded supersession.")
    canonical, state, cleanup, evidence, recorded, removed = record
    return {
        "schema_version": 1,
        "environment": database_purpose(),
        "publication_id": publication,
        "canonical_id": canonical,
        "state": state,
        "cleanup_id": str(cleanup),
        "recorded_at": recorded.isoformat(),
        "removed_at": removed.isoformat() if removed else None,
        "evidence": evidence,
    }
