"""Resolve source-derived merge candidates from the guarded file registry."""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from pathlib import Path
from typing import Any

from forge.historical.archives.storage.acquire import root_key
from forge.historical.merge.config import DerivedRecipe, normalization_path
from forge.historical.normalization.batch import utc_timestamp
from forge.historical.normalization.config import NormalizationRecipe, NormalizationSource
from forge.historical.normalization.mapping import bitget_spot_pair
from forge.shared.database.testing import testing_connection, testing_dsn
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths


def linked_submissions(
    paths: ProjectPaths, source: Path, config: NormalizationRecipe
) -> dict[tuple[str, str, str, str], str]:
    """Find retained intents whose submission and prepared request evidence agree."""
    links: dict[tuple[str, str, str, str], str] = {}
    expected = [request.model_dump(mode="json") for request in config.requests]
    interval = {key: utc_timestamp(value) for key, value in config.interval.model_dump().items()}
    for intent_path in sorted(paths.runs_dir.glob("normalize-execute-*/execution-intent.json")):
        try:
            intent = json.loads(intent_path.read_bytes())
            if (
                intent.get("operation") != "normalize"
                or intent.get("environment") != "testing"
                or intent.get("execution_id") != intent_path.parent.name
                or not intent.get("recipe")
                or normalization_path(paths, intent["recipe"]) != source
            ):
                continue
            batch_bytes = (intent_path.parent / "submitted-batch.json").read_bytes()
            digest = hashlib.sha256(batch_bytes).hexdigest()
            if digest != intent["submitted_sha256"]:
                continue
            report = json.loads(
                Path(intent["submitted_source"]).with_name("prepare-report.json").read_bytes()
            )
            batch = json.loads(batch_bytes)
            if (
                report.get("submitted_sha256") != digest
                or report.get("requested_interval") != interval
                or report.get("ranking_policy") != config.ranking_policy
                or [item["request"] for item in report["requests"]] != expected
                or batch["run_id"] != intent["run_id"]
            ):
                continue
            for job in batch["jobs"]:
                root = (paths.root / job["output"]["dataset_path"]).resolve()
                links[
                    (batch["instrument_namespace"], str(job["dataset_id"]), str(root), digest)
                ] = intent["execution_id"]
        except (OSError, ValueError, KeyError, TypeError, AttributeError, MarketForgeError):
            # Old or incomplete receipts cannot establish ownership. Their files
            # can still be discovered and explicitly approved by publication ID.
            continue
    return links


def identity(member: dict[str, Any]) -> tuple[str, str, str, str]:
    return (
        member.get("exchange") or "",
        member.get("instrument_type") or "",
        member.get("market_category") or "",
        member.get("symbol") or "",
    )


def request_may_contain(request: NormalizationSource, instrument: str) -> bool:
    exchange, kind, category, symbol = instrument.split(".", 3)
    if (exchange, kind, category) != (
        request.exchange,
        request.instrument_type,
        request.market_category,
    ):
        return False
    return (
        request.scope.kind != "instrument"
        or request.scope.identifier == symbol
        or (exchange == "bitget" and kind == "spot" and "/" in request.scope.identifier)
    )


def filter_requests(
    recipe: DerivedRecipe, config: NormalizationRecipe
) -> dict[int, NormalizationSource]:
    selection = recipe.selection
    return {
        index: request
        for index, request in enumerate(config.requests, 1)
        if (selection.data_types is None or request.data_type in selection.data_types)
        and (selection.exchanges is None or request.exchange in selection.exchanges)
        and (
            selection.instrument_types is None
            or request.instrument_type in selection.instrument_types
        )
        and (
            selection.market_categories is None
            or request.market_category in selection.market_categories
        )
        and (
            selection.instruments is None
            or any(request_may_contain(request, instrument) for instrument in selection.instruments)
        )
    }


def matches(
    request: NormalizationSource,
    member: dict[str, Any],
    tasks: list[dict[str, Any]],
    source_keys: dict[str, str],
) -> bool:
    exchange, instrument_type, category, symbol = identity(member)
    if (exchange, instrument_type, category) != (
        request.exchange,
        request.instrument_type,
        request.market_category,
    ):
        return False
    sources: list[dict[str, Any]] = [
        t.get("raw_schema", {}).get("marketforge_source", {}) for t in tasks
    ]
    identifier = request.scope.identifier
    assets: list[dict[str, Any]] = [source.get("instrument") or {} for source in sources]
    scopes: list[dict[str, Any]] = [source.get("scope") or {} for source in sources]
    if request.scope.kind == "family":
        # A resolved concrete task in this file with a stored family assertion
        # is historical membership evidence for that exact instrument.
        if not sources or not all(
            scope.get("kind") == "family" and scope.get("identifier") == identifier
            for scope in scopes
        ):
            return False
    elif request.scope.kind == "base_coin":
        if not assets or not all(a.get("base_asset") == identifier for a in assets):
            return False
    elif symbol != identifier:
        if (exchange, instrument_type, category) != (
            "bitget",
            "spot",
            "spot",
        ) or "/" not in identifier:
            return False
        base, quote = bitget_spot_pair(identifier)
        if not assets or not all(
            (str(a.get("base_asset", "")).lower(), str(a.get("quote_asset", "")).lower())
            == (base.lower(), quote.lower())
            for a in assets
        ):
            return False
    if request.source_key and (
        not tasks or not all(source_keys.get(t["input_path"]) == request.source_key for t in tasks)
    ):
        return False
    if exchange == "gateio" and request.data_type == "order_book_l2":
        feed = request.source_options.feed if request.source_options else "incremental"
        options: list[dict[str, Any]] = [source.get("source_options") or {} for source in sources]
        return bool(options) and all(option.get("feed") == feed for option in options)
    return True


def contributing_tasks(evidence: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    from forge.datasets.registration import file_task_ids

    tasks = {task["task_id"]: task for task in evidence["resolved_job"]["tasks"]}
    return {
        name: [tasks[task_id] for task_id in ids]
        for name, ids in file_task_ids(evidence["manifest"], normalize=True)
    }


async def registry(
    paths: ProjectPaths, start: int, end: int
) -> tuple[list[tuple[Any, ...]], dict[int, tuple[Any, ...]], dict[int, dict[str, str]]]:
    async with testing_connection(testing_dsn(paths.root)) as connection:
        async with connection.transaction():
            await connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            cursor = await connection.execute(
                """SELECT d.id,d.publication_id,d.primary_file,d.operation_details,
                          d.start_timestamp_ns,d.end_timestamp_ns,d.status,d.storage_path,d.file_sha256
                   FROM historical.datasets d
                   JOIN historical.dataset_publications p ON p.id=d.publication_id
                   WHERE d.operation='normalize' AND p.operation='normalize'
                     AND d.storage_format='parquet' AND d.primary_file IS NOT NULL
                     AND d.status <> 'deleted'
                     AND d.start_timestamp_ns < %s AND d.end_timestamp_ns >= %s
                   ORDER BY d.id""",
                (end, start),
            )
            files = await cursor.fetchall()
            ids = sorted({row[1] for row in files})
            cursor = await connection.execute(
                """SELECT id,engine_dataset_id,instrument_namespace,storage_root,evidence
                   FROM historical.dataset_publications WHERE id=ANY(%s)""",
                (ids,),
            )
            publications = {row[0]: row[1:] for row in await cursor.fetchall()}
            cursor = await connection.execute(
                """SELECT DISTINCT d.publication_id,l.relative_path,s.source_key
                   FROM historical.dataset_raw_inputs i
                   JOIN historical.datasets d ON d.id=i.dataset_id
                   JOIN historical.local_archives l ON l.id=i.local_archive_id
                   JOIN historical.raw_objects o ON o.id=l.raw_object_id
                   JOIN historical.raw_sources s ON s.id=o.raw_source_id
                   WHERE d.publication_id=ANY(%s) AND l.storage_root_key=%s
                     AND l.sha256=i.input_sha256""",
                (ids, root_key(paths.raw_dir)),
            )
            source_keys: dict[int, dict[str, str]] = defaultdict(dict)
            raw = paths.raw_dir.resolve()
            for pub, name, key in await cursor.fetchall():
                source_keys[pub][str(raw / name)] = key
    return files, publications, source_keys


def layout_reason(manifest: dict[str, Any]) -> str | None:
    version = manifest["protocol_version"]
    if version == 3:
        return None
    if version not in {4, 5} or manifest["content_type"] != "trades":
        return (
            f"Manifest v{version} {manifest['content_type']} requires v3 or timestamp-first trades."
        )
    quality: dict[str, Any] = manifest.get("processing_quality") or {}
    if quality.get("timestamp_ordering") != "event_timestamp_ns":
        return f"Manifest v{version} with native or non-timestamp-first trade ordering requires v3 or a timestamp-first trade layout."
    if any(
        any(source.get("ranges") for source in stream["sources"])
        or len({source["task_id"] for source in stream["sources"]}) != len(stream["sources"])
        for stream in manifest["streams"]
    ):
        return "Trade publication has interleaved or repeated source spans."
    return None


async def discover(
    paths: ProjectPaths, recipe: DerivedRecipe, source: Path, config: NormalizationRecipe
) -> dict[str, Any]:
    requests = filter_requests(recipe, config)
    files, publications, source_keys = await registry(paths, recipe.start_ns, recipe.end_ns)
    links = linked_submissions(paths, source, config)
    tasks_by_publication: dict[int, dict[str, list[dict[str, Any]]]] = {}
    provenance: dict[int, tuple[str, str]] = {}
    for pub, (engine, namespace, root, evidence) in publications.items():
        tasks_by_publication[pub] = contributing_tasks(evidence)
        digests = {
            task.get("raw_schema", {}).get("marketforge_submission_sha256")
            for task in evidence["resolved_job"]["tasks"]
        }
        if len(digests) == 1:
            digest = next(iter(digests))
            intent = links.get((namespace, engine, root, digest))
            execution: dict[str, Any] = evidence.get("execution") or {}
            if intent and (
                not execution
                or (execution.get("submitted_sha256"), execution.get("execution_id"))
                == (digest, intent)
            ):
                provenance[pub] = (intent, digest)

    candidates: list[dict[str, Any]] = []
    for db_id, pub, name, summary, start, end, status, registered_path, file_sha in files:
        engine, namespace, root, evidence = publications[pub]
        manifest = evidence["manifest"]
        content = {"trades": "trades", "depth": "order_book_l2"}.get(manifest["content_type"])
        quality: dict[str, Any] = manifest.get("processing_quality") or {}
        matched: set[int] = set()
        memberships: list[dict[str, Any]] = []
        members: list[dict[str, Any]] = summary.get("memberships", [])
        for member in members:
            stream = member["stream_id"]
            task_matches = [
                t
                for t in tasks_by_publication[pub].get(name, ())
                if (t["stream_id"], t["instrument_id"]) == (stream, member["instrument_id"])
            ]
            lo = member.get("start_timestamp_ns", start)
            hi = member.get("end_timestamp_ns", end)
            in_interval = lo < recipe.end_ns and hi >= recipe.start_ns
            approved = recipe.selection.instruments
            hits = [
                index
                for index, request in requests.items()
                if in_interval
                and request.data_type == content
                and (approved is None or ".".join(identity(member)) in approved)
                and matches(request, member, task_matches, source_keys.get(pub, {}))
            ]
            matched.update(hits)
            memberships.append({**member, "request_indexes": hits, "overlaps": in_interval})
        if matched:
            candidates.append(
                {
                    "database_id": db_id,
                    "publication_id": pub,
                    "engine_id": engine,
                    "namespace": namespace,
                    "path": root,
                    "primary_file": name,
                    "registered_path": registered_path,
                    "file_sha256": file_sha,
                    "manifest_sha256": evidence["manifest_sha256"],
                    "start_ns": start,
                    "end_ns": end,
                    "content_type": content,
                    "manifest_version": manifest["protocol_version"],
                    "status": status,
                    "integrity": manifest.get("integrity_status"),
                    "layout": quality.get("timestamp_ordering"),
                    "layout_issue": layout_reason(manifest),
                    "memberships": memberships,
                    "request_indexes": sorted(matched),
                    "provenance": provenance.get(pub),
                }
            )

    explicit = recipe.selection.publication_ids
    proven = [item for item in candidates if item["provenance"]]
    run_digests = {item["provenance"][1] for item in proven}
    blockers: list[dict[str, Any]] = []
    if explicit:
        selected = [item for item in candidates if item["publication_id"] in explicit]
        for pub in sorted(set(explicit) - {item["publication_id"] for item in selected}):
            blockers.append(
                {"publication_id": pub, "reason": "Approved publication has no matching files."}
            )
    elif len(run_digests) == 1:
        selected = proven
    else:
        selected = []
        if candidates:
            blockers.append(
                {
                    "reason": "Multiple verified submissions match."
                    if proven
                    else "Matching metadata does not establish normalization run ownership.",
                    "suggestion": "Set selection.publication_ids to approve registry publications explicitly.",
                    "publication_ids": sorted({item["publication_id"] for item in candidates}),
                }
            )

    def key(item: dict[str, Any]) -> tuple[Any, ...]:
        return (
            min(identity(m) for m in item["memberships"]),
            item["content_type"],
            item["start_ns"],
            item["publication_id"],
            item["primary_file"],
        )

    groups: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for item in selected:
        groups[item["publication_id"]].append(item)
    ordered = [
        item
        for group in sorted(groups.values(), key=lambda g: min(map(key, g)))
        for item in sorted(group, key=key)
    ]
    if len({item["namespace"] for item in ordered}) > 1:
        blockers.append(
            {
                "publication_ids": sorted(groups),
                "dataset_ids": [item["database_id"] for item in ordered],
                "reason": "Selected publications use multiple instrument namespaces.",
                "suggestion": "Approve publications from one instrument namespace.",
            }
        )
    for pub, group in groups.items():
        manifest = publications[pub][-1]["manifest"]
        streams = {m["stream_id"] for f in group for m in f["memberships"]}
        reason = layout_reason(manifest)
        if len(streams) != 1:
            reason = (
                f"Selected files contain {len(streams)} streams; current Rust merger requires one per publication."
                + (f" {reason}" if reason else "")
            )
        elif manifest["content_type"] == "depth" and {item["primary_file"] for item in group} != {
            entry["path"] for entry in manifest["files"]
        }:
            reason = (
                reason or "Depth reconstruction requires all primary files from the publication."
            )
        if reason:
            blockers.append(
                {
                    "publication_id": pub,
                    "engine_id": group[0]["engine_id"],
                    "manifest_version": manifest["protocol_version"],
                    "stream_ids": sorted(streams),
                    "dataset_ids": [f["database_id"] for f in group],
                    "reason": reason,
                    "suggestion": "Select one supported timestamp-first trade stream per publication, or a complete compatible v3 depth publication.",
                }
            )
    file_issues: dict[tuple[int, str], list[int]] = defaultdict(list)
    for item in ordered:
        if item["status"] != "complete" or item["integrity"] != "clean":
            reason = "Incomplete or degraded file/publication requires a clean complete input."
        elif any(m["overlaps"] and not m["request_indexes"] for m in item["memberships"]):
            reason = "File contains an overlapping stream outside the approved selection."
        else:
            continue
        file_issues[(item["publication_id"], reason)].append(item["database_id"])
    for (pub, reason), dataset_ids in file_issues.items():
        blockers.append(
            {
                "dataset_ids": dataset_ids,
                "publication_id": pub,
                "reason": reason,
                "suggestion": "Choose a supported publication with registered files for the requested stream.",
            }
        )
    matched_indexes = {index for item in candidates for index in item["request_indexes"]}
    selected_indexes = {index for item in ordered for index in item["request_indexes"]}
    missing = [
        {
            "request_index": index,
            "request": request.model_dump(mode="json"),
            "uncovered_interval": {"start_ns": recipe.start_ns, "end_ns": recipe.end_ns},
            "reason": (
                "Matching candidates were not approved."
                if index in matched_indexes
                else "No overlapping file establishes historical family membership."
                if request.scope.kind == "family"
                else "No matching registered files overlap the requested interval."
            ),
        }
        for index, request in requests.items()
        if index not in selected_indexes
    ]
    if not requests or not ordered or missing:
        blockers.append(
            {
                "reason": "No requests pass the filters."
                if not requests
                else "Some requested selections have no approved files."
                if missing
                else "No approved files."
            }
        )
    return {
        "requested_count": len(requests),
        "matched_count": len(candidates),
        "selected_count": len(ordered),
        "publication_count": len(groups),
        "candidates": sorted(candidates, key=key),
        "selected_dataset_ids": [item["database_id"] for item in ordered],
        "missing": missing,
        "blockers": blockers,
        "interval": {"start_ns": recipe.start_ns, "end_ns": recipe.end_ns},
    }
