"""Resolve conversion inputs through committed merge and normalization evidence."""

from __future__ import annotations

from dataclasses import dataclass
from itertools import combinations
from pathlib import Path
from typing import Any

from forge.datasets.discovery import read_object
from forge.historical.archives.config import AcquisitionConfig, ArchiveRequest, ArchiveScope
from forge.historical.archives.models import archive_scope_document, archive_scope_key
from forge.historical.conversion.evidence import verify_publication
from forge.historical.conversion.recipes import LoadedSource, Recipe, source_path
from forge.historical.conversion.registry import (
    Connection,
    Publication,
    parent_tasks,
    parents,
    publications,
    raw_for_files,
)
from forge.historical.engine.managed import checksum
from forge.historical.merge.config import DerivedRecipe, normalization_source
from forge.historical.merge.discovery import (
    filter_requests,
    linked_submissions,
    matches,
)
from forge.historical.merge.service import Recipe as DirectMerge
from forge.historical.normalization.config import NormalizationRecipe
from forge.shared.database.testing import testing_connection, testing_dsn
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths


def verify_raw_task(parent: dict[str, Any], task: dict[str, Any], raw: dict[str, Any]) -> None:
    path = task["input_path"]
    evidence = parent["evidence"]
    pin = [raw["local_archive_id"], raw["input_sha256"]]
    if (
        evidence["raw_links_by_path"].get(path) != pin
        or pin not in evidence["selected_raw_inputs"]
        or evidence["raw_sha256"].get(path) != pin[1]
    ):
        raise MarketForgeError(f"Normalized file {parent['id']} has stale raw provenance.")


@dataclass(frozen=True)
class Lineage:
    upstream: LoadedSource
    requests: dict[int, Any]
    normalization_links: dict[tuple[str, str, str, str], str]
    normalization_sha256: str | None = None


def lineage(paths: ProjectPaths, source: LoadedSource) -> Lineage:
    config = source.config
    if isinstance(config, DerivedRecipe):
        path, normalize, digest = normalization_source(paths, config.selection.source)
        return Lineage(
            source,
            filter_requests(config, normalize),
            linked_submissions(paths, path, normalize),
            digest,
        )
    if isinstance(config, NormalizationRecipe):
        return Lineage(
            source,
            dict(enumerate(config.requests, 1)),
            linked_submissions(paths, Path(source.declaration.path), config),
        )
    if isinstance(config, AcquisitionConfig):
        return Lineage(source, dict(enumerate(config.requests, 1)), {})
    return Lineage(source, {}, {})


def merge_submission(paths: ProjectPaths, pub: Publication) -> tuple[dict[str, Any], Path, str]:
    execution = pub.evidence["execution"]
    name = execution["execution_id"]
    if not name.startswith("merge-execute-") or Path(name).name != name:
        raise MarketForgeError("Invalid retained merge execution ID.")
    directory = paths.runs_dir / name
    intent = read_object(directory / "execution-intent.json")
    submission = directory / "submitted-merge.json"
    if (
        intent.get("execution_id") != name
        or intent.get("operation") != "merge"
        or intent.get("environment") != "testing"
        or str(intent.get("job_id")) != pub.engine_id
        or intent.get("path") != str(pub.root)
        or intent.get("submission_path") != str(submission)
        or intent.get("submitted_sha256") != execution["submitted_sha256"]
        or checksum(submission) != execution["submitted_sha256"]
        or read_object(submission) != pub.evidence["resolved_job"]
    ):
        raise MarketForgeError(
            "Retained merge submission disagrees with registered execution evidence."
        )
    return intent, submission, execution["submitted_sha256"]


def match_normalization(
    parent: dict[str, Any],
    context: Lineage,
    tasks: list[dict[str, Any]],
    raw: dict[str, dict[str, Any]],
) -> set[int]:
    evidence = parent["evidence"]
    run: dict[str, Any] = evidence.get("execution") or {}
    digest = run.get("submitted_sha256", "")
    link = (
        parent["instrument_namespace"],
        parent["engine_dataset_id"],
        parent["storage_root"],
        digest,
    )
    if (
        not tasks
        or not digest
        or context.normalization_links.get(link) != run.get("execution_id")
        or link not in context.normalization_links
        or any(task["raw_schema"].get("marketforge_submission_sha256") != digest for task in tasks)
    ):
        raise MarketForgeError(
            f"Normalized file {parent['id']} has no matching retained normalization submission."
        )
    keys = {path: item["source_key"] for path, item in raw.items()}
    content = {"trades": "trades", "depth": "order_book_l2"}[evidence["content_type"]]
    members = parent["summary"]["memberships"]
    hits = [
        {
            index
            for index, request in context.requests.items()
            if request.data_type == content and matches(request, member, tasks, keys)
        }
        for member in members
    ]
    if not hits or any(not matched for matched in hits):
        raise MarketForgeError(
            f"Normalized file {parent['id']} contains streams outside the upstream request."
        )
    return {index for matched in hits for index in matched}


def acquisition_scope(
    request: ArchiveRequest,
    task: dict[str, Any],
    archive: dict[str, Any],
) -> bool:
    scope = archive["scope_json"]
    if scope != archive_scope_document(request):
        # OKX expands an option base-coin request to concrete archive families.
        # Check the persisted family and base asset from the exact normalized task.
        stored: dict[str, Any] = task["raw_schema"].get("marketforge_source", {})
        instrument: dict[str, Any] = stored.get("instrument") or {}
        stored_scope: dict[str, Any] = stored.get("scope") or {}
        if not (
            request.exchange == "okx"
            and request.instrument_type == "option"
            and request.scope.kind == "base_coin"
            and scope.get("scope_kind") == "family"
            and instrument.get("base_asset") == request.scope.identifier
            and stored_scope.get("identifier") == scope.get("scope_identifier")
            and stored_scope.get("kind") == "family"
        ):
            return False
        request = request.model_copy(
            update={
                "scope": ArchiveScope(kind="family", identifier=scope["scope_identifier"]),
            }
        )
    return (archive["venue_code"], archive["data_type"], archive["scope_key"], scope) == (
        request.exchange,
        request.data_type,
        archive_scope_key(request),
        archive_scope_document(request),
    )


def match_acquisition(
    parent: dict[str, Any],
    config: AcquisitionConfig,
    tasks: list[dict[str, Any]],
    raw: dict[str, dict[str, Any]],
) -> set[int]:
    found: set[int] = set()
    if not tasks:
        raise MarketForgeError(
            f"Normalized file {parent['id']} has no contributing acquisition tasks."
        )
    for task in tasks:
        archive = raw.get(task["input_path"])
        if not archive:
            raise MarketForgeError(
                f"Normalized file {parent['id']} lacks a verified raw archive contribution."
            )
        verify_raw_task(parent, task, archive)
        hits = {
            index
            for index, request in enumerate(config.requests, 1)
            if acquisition_scope(request, task, archive)
            and archive["start_at"] < config.interval.end
            and archive["end_at"] > config.interval.start
        }
        if not hits:
            raise MarketForgeError(
                f"Raw archive {archive['local_archive_id']} is outside the acquisition scope."
            )
        found.update(hits)
    return found


async def prove_source(
    connection: Connection,
    paths: ProjectPaths,
    pub: Publication,
    context: Lineage,
    parent_rows: list[dict[str, Any]],
) -> tuple[set[int], dict[str, Any]]:
    config = context.upstream.config
    pins: dict[str, Any] = {}
    if isinstance(config, (DirectMerge, DerivedRecipe)):
        intent, submission, digest = merge_submission(paths, pub)
        pins.update(merge_submission=str(submission), merge_submission_sha256=digest)
        source = context.upstream.declaration
        if pub.manifest["time_range"] != context.upstream.interval.model_dump():
            raise MarketForgeError("Merge interval differs from the upstream merge recipe.")
        job = pub.evidence["resolved_job"]
        if (
            config.timeline_ordering is not None
            and job.get("timeline_ordering", "chronological") != config.timeline_ordering
        ):
            raise MarketForgeError("Upstream merge ordering differs from the committed job.")
        ranks = {item["stream_id"]: item["stream_rank"] for item in pub.manifest["streams"]}
        if any(ranks.get(stream) != rank for stream, rank in config.stream_ranks.items()):
            raise MarketForgeError("Upstream stream ranks differ from the committed job.")
        if isinstance(config, DirectMerge):
            if config.dataset_ids != pub.parent_ids:
                raise MarketForgeError(
                    "Selected merge parents differ from the direct recipe's normalized file selection."
                )
            if intent.get("recipe") and source_path(paths, "merge", intent["recipe"]) != Path(
                source.path
            ):
                raise MarketForgeError("Retained merge execution names a different recipe.")
            return set(), pins
        if not intent.get("recipe") or source_path(paths, "merge", intent["recipe"]) != Path(
            source.path
        ):
            raise MarketForgeError(
                "Retained merge execution does not identify the upstream merge recipe."
            )
        report_path = paths.runs_dir / f"merge-prepare-{pub.engine_id}" / "prepare-report.json"
        report = read_object(report_path)
        if (
            report.get("submitted_sha256") != digest
            or report.get("selected_dataset_ids") != pub.parent_ids
            or report.get("source", {}).get("sha256") != context.normalization_sha256
            or report.get("interval") != context.upstream.interval.model_dump()
            or config.selection.publication_ids is not None
            and set(config.selection.publication_ids) != {p["publication_id"] for p in parent_rows}
        ):
            raise MarketForgeError(
                "Retained merge preparation does not match the upstream normalization selection."
            )
        pins.update(
            merge_prepare_report=str(report_path), merge_prepare_report_sha256=checksum(report_path)
        )
    raw = await raw_for_files(connection, paths, pub.parent_ids)
    proof: list[dict[str, Any]] = []
    hits: set[int] = set()
    for parent in parent_rows:
        tasks = parent_tasks(parent)
        archives = raw.get(parent["id"], {})
        if isinstance(config, AcquisitionConfig):
            indexes = match_acquisition(parent, config, tasks, archives)
        else:
            indexes = match_normalization(parent, context, tasks, archives)
            if isinstance(config, DerivedRecipe) and config.selection.instruments is not None:
                allowed = set(config.selection.instruments)
                if any(
                    ".".join(
                        member[k]
                        for k in ("exchange", "instrument_type", "market_category", "symbol")
                    )
                    not in allowed
                    for member in parent["summary"]["memberships"]
                ):
                    raise MarketForgeError(
                        f"Normalized file {parent['id']} is outside the upstream instrument selection."
                    )
        hits.update(indexes)
        proof.append(
            {
                "file_dataset_id": parent["id"],
                "request_indexes": sorted(indexes),
                "normalization_execution": parent["evidence"].get("execution"),
                "raw_archives": [
                    {
                        "path": path,
                        **{
                            key: record[key]
                            for key in (
                                "local_archive_id",
                                "raw_object_id",
                                "input_sha256",
                                "scope_key",
                                "scope_json",
                                "venue_code",
                                "data_type",
                                "source_key",
                            )
                        },
                        "start_at": record["start_at"].isoformat(),
                        "end_at": record["end_at"].isoformat(),
                    }
                    for path in sorted({task["input_path"] for task in tasks})
                    if (record := archives.get(path)) is not None
                ],
            }
        )
    return hits, {**pins, "matched_parents": proof}


def check_scope(recipe: Recipe, pub: Publication) -> None:
    scope = pub.manifest["time_range"]
    lo, hi = recipe.interval.start_ns, recipe.interval.end_ns
    if lo >= scope["end_ns"] or hi <= scope["start_ns"]:
        raise MarketForgeError("Publication and recipe intervals are disjoint.")
    if not recipe.selection.whole_publication and (
        scope["start_ns"] < lo
        or scope["end_ns"] > hi
        or pub.manifest["start_timestamp_ns"] < lo
        or pub.manifest["end_timestamp_ns"] >= hi
    ):
        raise MarketForgeError(
            "Publication exceeds the recipe interval. Set selection.whole_publication=true to approve its complete scope."
        )


def competing(left: Publication, right: Publication) -> bool:
    a, b = left.manifest, right.manifest

    def keys(manifest: dict[str, Any]) -> set[tuple[str, str, int, str]]:
        return {
            (s["exchange"], s["symbol"], s["instrument_id"], s["kind"]) for s in manifest["streams"]
        }

    return bool(
        keys(a) & keys(b)
        and a["time_range"]["start_ns"] < b["time_range"]["end_ns"]
        and b["time_range"]["start_ns"] < a["time_range"]["end_ns"]
    )


async def discover(paths: ProjectPaths, recipe: Recipe, source: LoadedSource) -> dict[str, Any]:
    scope = recipe.selection
    explicit = bool(scope.file_dataset_ids or scope.publication_ids)
    context = lineage(paths, source)
    candidates: list[dict[str, Any]] = []
    allowed: list[tuple[Publication, list[dict[str, Any]], dict[str, Any], set[int]]] = []
    blockers: list[str] = []
    async with testing_connection(testing_dsn(paths.root)) as connection, connection.transaction():
        await connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        found = await publications(
            connection,
            ids=scope.publication_ids,
            file_ids=scope.file_dataset_ids,
            interval=None if explicit else (recipe.interval.start_ns, recipe.interval.end_ns),
        )
        for pub in found:
            candidate = {"registry_publication_id": pub.id, "engine_publication_id": pub.engine_id}
            candidates.append(candidate)
            try:
                check_scope(recipe, pub)
                if (
                    isinstance(source.config, DirectMerge)
                    and source.config.dataset_ids != pub.parent_ids
                ):
                    raise MarketForgeError(
                        "Selected normalized input file IDs differ from the upstream merge recipe."
                    )
                parent_rows = await parents(connection, pub)
                hits, proof = await prove_source(connection, paths, pub, context, parent_rows)
            except (MarketForgeError, ValueError, OSError, KeyError, TypeError) as error:
                reason = f"Merge publication {pub.id} ({pub.engine_id}): {error}"
                candidate.update(reason=reason, eligible=False)
                if explicit:
                    blockers.append(reason)
            else:
                candidate["eligible"] = True
                allowed.append((pub, parent_rows, proof, hits))
        ids = {pub.engine_id for pub, *_ in allowed}
        if scope.expected_engine_publication_ids and ids != set(
            scope.expected_engine_publication_ids
        ):
            blockers.append(
                f"Expected engine publication IDs {scope.expected_engine_publication_ids}, resolved {sorted(ids)}."
            )
        if not allowed:
            blockers.append(
                "No complete merge publication has verified upstream lineage for this selection."
            )
        if context.requests and (
            missing := sorted(set(context.requests) - set().union(*(item[3] for item in allowed)))
        ):
            blockers.append(f"No verified selection covers upstream request indexes {missing}.")
        if any(competing(a[0], b[0]) for a, b in combinations(allowed, 2)):
            blockers.append(
                "Competing merge publications overlap. Pin one revision with selection.publication_ids or selection.file_dataset_ids."
            )
        if len({parents[0]["instrument_namespace"] for _, parents, _, _ in allowed}) > 1:
            blockers.append(
                "Selected merge publications use different source instrument namespaces."
            )
        verified: list[dict[str, Any]] = []
        external = None
        if not blockers:
            for pub, parent_rows, proof, _ in allowed:
                verified.append(
                    await verify_publication(connection, paths, pub, parent_rows)
                    | {"lineage": proof}
                )
            if recipe.reference_publication_id is not None:
                ref = (await publications(connection, ids=[recipe.reference_publication_id]))[0]
                if ref.id in {pub.id for pub, *_ in allowed} or ref.manifest["mode"] != "trades":
                    raise MarketForgeError(
                        "External references require a distinct, registered trade-only merge publication."
                    )
                external = await verify_publication(
                    connection, paths, ref, await parents(connection, ref)
                )
                if (
                    external["source_instrument_namespace"]
                    != verified[0]["source_instrument_namespace"]
                ):
                    raise MarketForgeError("External and source instrument namespaces differ.")
    return {
        "candidates": candidates,
        "blockers": blockers,
        "sources": verified,
        "external_reference": external,
    }
