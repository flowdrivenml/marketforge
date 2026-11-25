"""Verify physical merge inventories and historical instrument evidence."""

from __future__ import annotations

import asyncio
import hashlib
import json
from pathlib import Path
from typing import Any, Literal, cast

from pydantic import TypeAdapter

from forge.datasets.discovery import inventory, is_staging, read_object
from forge.historical.conversion.contract import Currency, Identity
from forge.historical.conversion.recipes import PublicationSource
from forge.historical.conversion.registry import (
    Connection,
    Publication,
    rows,
    selectable_publication,
)
from forge.historical.engine.managed import checksum
from forge.historical.merge.service import select_inputs
from forge.historical.normalization.datasets import verify_inventory
from forge.shared.database.connection import (
    database_connection,
    database_dsn,
    database_purpose,
    registry_id,
)
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths


def stream_metadata(
    stream: dict[str, Any],
    parent: dict[str, Any],
) -> dict[str, Any]:
    evidence = parent["evidence"]
    manifest = evidence["manifest"]
    fields = ("exchange", "symbol", "instrument_id")
    metadata = [
        entry
        for entry in manifest["resolved_instruments"]
        if tuple(entry["instrument"][key] for key in fields) == tuple(stream[key] for key in fields)
    ]
    tasks = [
        task
        for task in evidence["resolved_job"]["tasks"]
        if all(task[key] == stream[key] for key in (*fields, "stream_id"))
    ]
    if manifest.get("instrument_namespace") != parent["instrument_namespace"]:
        raise MarketForgeError(f"Conflicting instrument namespace for {stream['stream_id']}.")
    if len(metadata) != 1 or not tasks:
        raise MarketForgeError(
            f"Missing historical instrument specification for {stream['stream_id']}."
        )
    instrument = metadata[0]["instrument"]
    specification = tasks[0]["instrument"]
    if (
        any(task["instrument"] != specification for task in tasks)
        or specification["instrument_kind"] != instrument["instrument_type"]
        or any(specification.get(key) != value for key, value in instrument["instrument"].items())
        or any(
            stream.get(key) is not None and stream[key] != instrument[other]
            for key, other in (("base_currency", "base_asset"), ("quote_currency", "quote_asset"))
        )
    ):
        raise MarketForgeError(
            f"Conflicting historical instrument specifications for {stream['stream_id']}."
        )
    identity = Identity(
        exchange=stream["exchange"],
        symbol=stream["symbol"],
        instrument_kind=specification["instrument_kind"],
        contract_kind=specification.get("contract_kind"),
    )
    currency = TypeAdapter(Currency)
    base, quote = (
        currency.validate_python(instrument[key]) for key in ("base_asset", "quote_asset")
    )
    if base == quote:
        raise MarketForgeError(f"Historical base and quote are equal for {stream['stream_id']}.")
    return {
        **stream,
        "dataset_id": str(stream["dataset_id"]),
        "identity": identity.model_dump(),
        "base_currency": base,
        "quote_currency": quote,
        "instrument_namespace": parent["instrument_namespace"],
        "normalized_registry_publication_id": parent["publication_id"],
        "market_category": instrument["market_category"],
        "instrument_type": instrument["instrument_type"],
        "specification": specification,
        "metadata_origin": metadata[0]["metadata_origin"],
    }


def check_merge_metadata(pub: Publication) -> None:
    root, manifest, evidence = pub.root, pub.manifest, pub.evidence
    if (
        not root.is_absolute()
        or root.resolve() != root
        or is_staging(root)
        or pub.manifest_path != root / "manifest.json"
        or str(manifest["job_id"]) != pub.engine_id
        or manifest["mode"] != evidence["content_type"]
        or evidence["path"] != str(root)
        or evidence["instrument_namespace"] != pub.namespace
        or evidence["engine_dataset_id"] != pub.engine_id
        or evidence["manifest_path"] != str(pub.manifest_path)
    ):
        raise MarketForgeError("Registered merge publication identity is inconsistent.")
    if (
        manifest["protocol_version"] != 1
        or (
            manifest.get("physical_schema_version"),
            manifest.get("timeline_ordering", "chronological"),
        )
        not in {(1, "chronological"), (2, "causal")}
        or manifest["mode"] not in {"trades", "depth", "combined"}
    ):
        raise MarketForgeError("Unsupported committed physical merge manifest contract.")


def bind_publication(pub: Publication, registry: str) -> PublicationSource:
    """Check the commit metadata without scanning or hashing primary files."""
    try:
        check_merge_metadata(pub)
        evidence = pub.evidence
        metadata = {
            "manifest.json": (evidence["manifest_sha256"], pub.manifest),
            "merge-job.json": (evidence["processing_job_sha256"], evidence["resolved_job"]),
        }
        index = pub.root / "publication-checksums.json"
        files = {entry["path"] for entry in pub.manifest["files"]}
        if not files or index.resolve() != index or not index.is_file():
            raise MarketForgeError("Missing or unsafe committed publication metadata.")
        index_data = index.read_bytes()
        inventory_pins = evidence["inventory_sha256"]
        if json.loads(index_data) != inventory_pins or not files <= inventory_pins.keys():
            raise MarketForgeError("Committed checksum index differs from registry evidence.")
        for name, (digest, declared) in metadata.items():
            path = pub.root / name
            if path.resolve() != path or not path.is_file() or inventory_pins.get(name) != digest:
                raise MarketForgeError(f"Missing or unsafe committed metadata: {name}.")
            data = path.read_bytes()
            if hashlib.sha256(data).hexdigest() != digest or json.loads(data) != declared:
                raise MarketForgeError(
                    f"Committed metadata differs from registry evidence: {name}."
                )
        job = evidence["resolved_job"]
        if (
            str(job["job_id"]) != pub.engine_id
            or job["operation"] != "merge"
            or job["time_range"] != pub.manifest["time_range"]
            or job["output"]["dataset_path"] != str(pub.root)
            or job.get("timeline_ordering", "chronological")
            != pub.manifest.get("timeline_ordering", "chronological")
        ):
            raise MarketForgeError("Committed merge job disagrees with the publication scope.")
        return PublicationSource.model_validate(
            {
                "registry_publication_id": pub.id,
                "registry_id": registry,
                "environment": cast(Literal["testing", "production"], database_purpose()),
                "engine_publication_id": pub.engine_id,
                "manifest_sha256": evidence["manifest_sha256"],
                "processing_job_sha256": evidence["processing_job_sha256"],
                "publication_checksums_sha256": hashlib.sha256(index_data).hexdigest(),
                "interval": pub.manifest["time_range"],
            }
        )
    except (MarketForgeError, KeyError, OSError, TypeError, ValueError) as error:
        raise MarketForgeError(
            f"Cannot bind registry merge publication {pub.id}: {error}"
        ) from error


def check_source_environment(source: PublicationSource) -> None:
    if source.environment != database_purpose():
        raise MarketForgeError(
            f"Publication source is bound to {source.environment}. Select --{source.environment}."
        )


async def resolve_binding(
    connection: Connection,
    source: PublicationSource,
    *,
    check_files: bool = False,
) -> Publication:
    check_source_environment(source)
    registry = await registry_id(connection)
    if str(source.registry_id) != registry:
        raise MarketForgeError(
            f"Publication source belongs to a different {source.environment} registry. "
            "Select the original registry or create a new recipe for this registry."
        )
    pub = await selectable_publication(connection, source.registry_publication_id)
    current = (
        await checked_binding(connection, pub, registry)
        if check_files
        else await asyncio.to_thread(bind_publication, pub, registry)
    )
    if current != source:
        raise MarketForgeError(
            f"Committed source binding changed for registry publication {pub.id}. "
            "Inspect that publication before creating a new binding."
        )
    return pub


async def inspect_publication_source(paths: ProjectPaths, publication_id: int) -> PublicationSource:
    async with (
        database_connection(database_dsn(paths.root)) as connection,
        connection.transaction(),
    ):
        await connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        registry = await registry_id(connection)
        pub = await selectable_publication(connection, publication_id)
        return await checked_binding(connection, pub, registry)


async def checked_binding(
    connection: Connection, pub: Publication, registry: str
) -> PublicationSource:
    source = await asyncio.to_thread(bind_publication, pub, registry)
    try:
        await registered_files(connection, pub)
    except (MarketForgeError, KeyError, TypeError, ValueError) as error:
        raise MarketForgeError(
            f"Cannot bind registry merge publication {pub.id}: {error}"
        ) from error
    return source


def verify_merge_inventory(pub: Publication) -> dict[str, str]:
    check_merge_metadata(pub)
    root, manifest, evidence = pub.root, pub.manifest, pub.evidence
    artifacts = (
        "manifest.json",
        "merge-job.json",
        "merge-input-checksums.json",
        "merge-partitions.json",
        *(("merge-runtime.json",) if "merge-runtime.json" in evidence["inventory_sha256"] else ()),
    )
    fingerprints = verify_inventory(
        root, manifest["files"], artifacts, require_commit_checksums=True
    )
    if (
        fingerprints != evidence["inventory_sha256"]
        or fingerprints["manifest.json"] != evidence["manifest_sha256"]
        or fingerprints["merge-job.json"] != evidence["processing_job_sha256"]
        or read_object(pub.manifest_path) != manifest
        or read_object(root / "merge-job.json") != evidence["resolved_job"]
    ):
        raise MarketForgeError(
            "Committed merge manifest, job, or file fingerprints differ from the registry."
        )
    return fingerprints


async def registered_files(
    connection: Connection, pub: Publication
) -> tuple[list[dict[str, Any]], set[tuple[str, int]]]:
    """Check the registry's complete primary-file declarations and membership."""
    manifest, evidence = pub.manifest, pub.evidence
    files = await rows(
        connection,
        """
        SELECT id,primary_file,storage_path,file_sha256,status,operation,storage_format,
               operation_details AS summary,start_timestamp_ns,end_timestamp_ns
        FROM historical.datasets WHERE publication_id=%s ORDER BY primary_file,id
    """,
        (pub.id,),
    )
    entries = {entry["path"]: entry for entry in manifest["files"]}
    if (
        not files
        or len(files) != len(entries)
        or {f["primary_file"] for f in files} != entries.keys()
        or len(entries) != manifest["files_written"]
        or sum(f["rows"] for f in entries.values()) != manifest["events_written"]
        or manifest["trade_events"] + manifest["depth_events"] != manifest["events_written"]
        or min(f["start_timestamp_ns"] for f in entries.values()) != manifest["start_timestamp_ns"]
        or max(f["end_timestamp_ns"] for f in entries.values()) != manifest["end_timestamp_ns"]
    ):
        raise MarketForgeError(
            "Registry files do not cover the complete merged manifest inventory."
        )
    observed_members: set[tuple[str, int]] = set()
    for file in files:
        name = file["primary_file"]
        entry, summary = entries[name], file["summary"]
        if (
            (file["status"], file["operation"], file["storage_format"])
            != ("complete", "merge", "parquet")
            or file["storage_path"] != str(pub.root / name)
            or file["file_sha256"] != evidence["inventory_sha256"].get(name)
            or any(file[key] != entry[key] for key in ("start_timestamp_ns", "end_timestamp_ns"))
            or any(summary.get(key) != value for key, value in entry.items())
            or summary.get("integrity") != "physical_validation"
            or summary.get("memberships") != evidence["file_memberships"].get(name)
        ):
            raise MarketForgeError(
                f"Registry merge file {file['id']} has inconsistent committed evidence."
            )
        for member in summary["memberships"]:
            observed_members.add((member["source_publication_id"], member["stream_rank"]))
    return files, observed_members


async def verify_publication(
    connection: Connection,
    paths: ProjectPaths,
    pub: Publication,
    parent_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    manifest, evidence = pub.manifest, pub.evidence
    job = evidence["resolved_job"]
    if (
        str(job["job_id"]) != pub.engine_id
        or job["operation"] != "merge"
        or job["output"]["dataset_path"] != str(pub.root)
        or job["time_range"] != manifest["time_range"]
        or job.get("timeline_ordering", "chronological")
        != manifest.get("timeline_ordering", "chronological")
    ):
        raise MarketForgeError("Merge job disagrees with committed publication scope.")
    files, observed_members = await registered_files(connection, pub)
    fingerprints = await asyncio.to_thread(verify_merge_inventory, pub)
    selected = await select_inputs(paths, job=job, recovery=True)
    if [item["database_id"] for item in selected] != pub.parent_ids or [
        item["database_id"] for item in evidence["merge_inputs"]
    ] != pub.parent_ids:
        raise MarketForgeError("Merge job, registry and pinned parent file selections disagree.")
    for item, pin, parent in zip(selected, evidence["merge_inputs"], parent_rows, strict=True):
        if (
            any(item.get(key) != value for key, value in pin.items())
            or parent["file_sha256"] != item["file_sha256"]
            or parent["summary"]["memberships"] != item["memberships"]
            or parent["evidence"]["manifest_sha256"] != item["manifest_sha256"]
        ):
            raise MarketForgeError(
                f"Merge normalized input {item['database_id']} fingerprint changed."
            )
    links = await rows(
        connection,
        """
        SELECT dataset_id,input_dataset_id FROM historical.dataset_inputs WHERE dataset_id=ANY(%s)
    """,
        ([file["id"] for file in files],),
    )
    expected_links = {
        (file["id"], parent_id)
        for file in files
        for parent_id in file["summary"].get("contributing_dataset_ids", [])
    }
    if {(i["dataset_id"], i["input_dataset_id"]) for i in links} != expected_links or any(
        parent not in pub.parent_ids for _, parent in expected_links
    ):
        raise MarketForgeError("Registered merge contribution links disagree with file evidence.")
    parent_by_engine = {p["engine_dataset_id"]: p for p in parent_rows}
    inputs = manifest["input_datasets"]
    pinned: list[dict[str, Any]] = []
    if {str(item["dataset_id"]) for item in inputs} != parent_by_engine.keys():
        raise MarketForgeError(
            "Manifest input publications disagree with registered normalized parents."
        )
    checksums = json.loads((pub.root / "merge-input-checksums.json").read_bytes())
    expected_checksums: list[str] = []
    for item in inputs:
        engine = str(item["dataset_id"])
        parent = parent_by_engine[engine]
        ev = parent["evidence"]
        root = Path(parent["storage_root"])
        if (
            item["dataset_path"] != str(root)
            or item["manifest_version"] != ev["manifest"]["protocol_version"]
            or read_object(root / "manifest.json") != ev["manifest"]
            or checksum(root / "processing-job.json") != ev["processing_job_sha256"]
            or read_object(root / "processing-job.json") != ev["resolved_job"]
        ):
            raise MarketForgeError(
                f"Committed normalized parent {engine} disagrees with registry evidence."
            )
        expected_checksums.append(ev["manifest_sha256"])
        pinned.append(
            {
                "registry_publication_id": parent["publication_id"],
                "engine_publication_id": engine,
                "root": str(root),
                "instrument_namespace": parent["instrument_namespace"],
                "manifest_sha256": ev["manifest_sha256"],
                "processing_job_sha256": ev["processing_job_sha256"],
                "files": [
                    {
                        "file_dataset_id": row["database_id"],
                        "path": row["registered_path"],
                        "sha256": row["file_sha256"],
                    }
                    for row in selected
                    if str(row["engine_id"]) == engine
                ],
            }
        )
    if checksums != expected_checksums:
        raise MarketForgeError(
            "Committed merge input manifest fingerprints disagree with normalized parents."
        )

    declarations: dict[tuple[str, int], dict[str, Any]] = {}
    for stream in manifest["streams"]:
        engine = str(stream["dataset_id"])
        key = (engine, stream["stream_rank"])
        parent = parent_by_engine[engine]
        if key in declarations or not any(
            str(item["engine_id"]) == engine
            and item["stream_id"] == stream["stream_id"]
            and item.get("stream_rank", stream["stream_rank"]) == stream["stream_rank"]
            and any(
                member["instrument_id"] == stream["instrument_id"] for member in item["memberships"]
            )
            and item["content_type"] == {"trade": "trades", "depth": "depth"}[stream["kind"]]
            for item in selected
        ):
            raise MarketForgeError(
                "Merge stream is missing a unique registered normalized source identity."
            )
        declarations[key] = stream_metadata(stream, parent)
    observed_bounds: dict[tuple[str, int], tuple[int, int]] = {}
    for file in files:
        for member in file["summary"]["memberships"]:
            key = (member["source_publication_id"], member["stream_rank"])
            stream = declarations.get(key)
            if (
                not stream
                or any(
                    stream[key] != member[key]
                    for key in ("stream_id", "exchange", "symbol", "instrument_id", "kind")
                )
                or any(
                    member.get(key) is not None and stream[key] != member[key]
                    for key in ("instrument_type", "market_category")
                )
            ):
                raise MarketForgeError(
                    "Registry file membership disagrees with committed instrument provenance."
                )

            # Older memberships may omit stream bounds. File bounds remain a safe envelope.
            lo = member.get("start_timestamp_ns", file["start_timestamp_ns"])
            hi = member.get("end_timestamp_ns", file["end_timestamp_ns"])
            if (
                type(lo) is not int
                or type(hi) is not int
                or not file["start_timestamp_ns"] <= lo <= hi <= file["end_timestamp_ns"]
            ):
                lo, hi = file["start_timestamp_ns"], file["end_timestamp_ns"]
            old_lo, old_hi = observed_bounds.get(key, (lo, hi))
            observed_bounds[key] = min(lo, old_lo), max(hi, old_hi)

    registry = {
        pub.root / file["primary_file"]: {
            "primary_file": file["primary_file"],
            "engine_dataset_id": pub.engine_id,
            "summary": file["summary"],
            "status": file["status"],
        }
        for file in files
    }
    datasets, issues = await asyncio.to_thread(inventory, pub.root, registry=registry)
    if len(datasets) != len(files) or issues:
        raise MarketForgeError(
            "Physical merge inventory disagrees with registered evidence. "
            + "; ".join(issue.message for issue in issues)
        )
    if manifest["physical_schema_version"] == 2:
        by_path = {pub.root / file["primary_file"]: file for file in files}
        for dataset in datasets:
            if by_path[dataset.path]["summary"]["membership_scope"] != "file" and any(
                stream.exact_rows != stream.reconstruction_rows for stream in dataset.streams_by_key
            ):
                raise MarketForgeError(
                    "Registry omits market membership for a physical merge file."
                )
    if manifest.get("reconstruction_events", 0) != sum(
        stream.reconstruction_rows or 0 for dataset in datasets for stream in dataset.streams_by_key
    ):
        raise MarketForgeError(
            "Physical reconstruction count disagrees with committed merge manifest."
        )
    ranks = {
        stream.identity["stream_rank"]
        for dataset in datasets
        for stream in dataset.streams_by_key
        if stream.identity
    } | {rank for _, rank in observed_members}
    return {
        "registry_publication_id": pub.id,
        "engine_publication_id": pub.engine_id,
        "registered_file_dataset_ids": [file["id"] for file in files],
        "root": str(pub.root),
        "manifest_path": str(pub.manifest_path),
        "fingerprints": fingerprints,
        "instrument_namespace": pub.namespace,
        "source_instrument_namespace": parent_rows[0]["instrument_namespace"],
        "requested_interval": manifest["time_range"],
        "observed_interval_inclusive": {
            "start_ns": manifest["start_timestamp_ns"],
            "end_ns": manifest["end_timestamp_ns"],
        },
        "events": manifest["events_written"],
        "trade_events": manifest["trade_events"],
        "depth_events": manifest["depth_events"],
        "reconstruction_events": manifest.get("reconstruction_events", 0),
        "mode": manifest["mode"],
        "protocol_version": manifest["protocol_version"],
        "physical_schema_version": manifest["physical_schema_version"],
        "timeline_ordering": manifest.get("timeline_ordering", "chronological"),
        "files": files,
        "normalized_inputs": pinned,
        "streams": [
            {
                **stream,
                "observed": key[1] in ranks,
                "market_observed": key in observed_members,
                **(
                    {
                        "observed_interval_inclusive": {
                            "start_ns": observed_bounds[key][0],
                            "end_ns": observed_bounds[key][1],
                        }
                    }
                    if key in observed_bounds
                    else {}
                ),
            }
            for key, stream in sorted(
                declarations.items(), key=lambda item: (item[0][1], item[0][0])
            )
        ],
    }
