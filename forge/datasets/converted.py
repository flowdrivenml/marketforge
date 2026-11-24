"""Read indexed conversion evidence without scanning output directories or Parquet."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, cast

from psycopg import AsyncConnection
from psycopg.rows import TupleRow

from forge.historical.archives.storage.files import disk
from forge.historical.engine.managed import checksum
from forge.shared.database.connection import registry_id
from forge.shared.errors import MarketForgeError

MAX_MANIFEST_BYTES = 16 * 1024 * 1024


def manifest_evidence(
    result: dict[str, Any], *, verify_fingerprint: bool = True
) -> tuple[Path, dict[str, Any]]:
    publication = result["publication"]
    path = Path(publication["manifest"])
    if (
        not path.is_absolute()
        or path.name != "manifest.json"
        or path.resolve() != path
        or not path.is_file()
    ):
        raise ValueError("Converted manifest path is missing or unsafe.")
    with path.open("rb") as stream:
        raw = stream.read(MAX_MANIFEST_BYTES + 1)
    if len(raw) > MAX_MANIFEST_BYTES:
        raise ValueError("Converted manifest exceeds the metadata read limit.")
    if (
        verify_fingerprint
        and hashlib.sha256(raw).hexdigest() != publication["fingerprints"]["manifest.json"]
    ):
        raise ValueError("Converted manifest differs from its indexed fingerprint.")
    document = json.loads(raw)
    if not isinstance(document, dict):
        raise ValueError("Converted manifest must be an object.")
    return path, cast(dict[str, Any], document)


def output_row(
    key: str,
    result: dict[str, Any],
    members: list[dict[str, Any]],
) -> dict[str, Any]:
    path, manifest = manifest_evidence(result)
    output = manifest["output"]
    conversion = manifest["conversion"]
    files = output["files"]
    if len(files) != output["files_written"]:
        raise ValueError("Converted manifest file count disagrees with its inventory.")
    pinned_files: list[dict[str, Any]] = result["input"].get("files") or []
    if pinned_files:
        members = [
            member
            for saved in pinned_files
            for member in cast(
                list[dict[str, Any]], saved.get("summary", {}).get("memberships", [])
            )
        ]
    return {
        "operation": "convert",
        "output_ref": key,
        "engine_job_id": manifest["job_id"],
        "instrument_namespace": result["input"].get("source_instrument_namespace"),
        "status": result["status"],
        "registration": result.get("registration", "not_available"),
        "verification": result.get("verification", "recorded"),
        "integrity": "clean" if manifest["integrity"]["status"] == "pass" else "failed",
        "data_type": {"trades": "trade", "depth": "l2", "combined": "trade_l2"}[
            output["content_type"]
        ],
        "target_quote": conversion["target_quote"],
        "variant": key,
        "representation": [
            manifest.get("protocol_version"),
            manifest.get("conversion_schema_version"),
            output.get("timeline_ordering"),
        ],
        "start_timestamp_ns": output["first_timestamp_ns"],
        "end_timestamp_ns": output["last_timestamp_ns"],
        "summary": {"memberships": members, "membership_scope": "source publication"},
        "files": [
            {
                **entry,
                "part": index,
                "storage_path": str(path.parent / entry["path"]),
                "file_sha256": result["publication"]["fingerprints"].get(entry["path"]),
            }
            for index, entry in enumerate(files, 1)
        ],
        "manifest_path": str(path),
        "storage_root": str(path.parent),
        "manifest_sha256": result["publication"]["fingerprints"]["manifest.json"],
        "indexed_result": result,
        "conversion": conversion,
        "statistics": manifest.get("statistics"),
        "integrity_evidence": manifest["integrity"],
    }


async def indexed_outputs(
    connection: AsyncConnection[TupleRow],
    output_ref: str | None = None,
) -> tuple[list[dict[str, Any]], str, list[str]]:
    """Read canonical work records and source membership from one registry snapshot."""
    capability = await (
        await connection.execute("SELECT to_regclass('historical.resolved_work')")
    ).fetchone()
    if capability is None or capability[0] is None:
        return [], "unavailable", []
    identity = await registry_id(connection)
    records = await (
        await connection.execute(
            """
            WITH indexed AS (
                SELECT work_key,result,owner FROM historical.resolved_work
                WHERE registry_id=%s AND operation='convert' AND result->>'status'='complete'
                      AND (%s::text IS NULL OR work_key=%s)
            ), parents AS (
                SELECT DISTINCT result#>>'{input,registry_publication_id}' AS id FROM indexed
            ), members AS (
                SELECT p.id::text AS id, jsonb_agg(DISTINCT member) AS items
                FROM parents parent
                JOIN historical.dataset_publications p ON p.id=CASE
                    WHEN parent.id ~ '^[0-9]{1,19}$'
                      AND parent.id::numeric <= 9223372036854775807 THEN parent.id::bigint END
                JOIN historical.datasets d ON d.publication_id=p.id
                CROSS JOIN LATERAL jsonb_array_elements(
                    CASE WHEN jsonb_typeof(d.operation_details->'memberships')='array'
                         THEN d.operation_details->'memberships' ELSE '[]'::jsonb END
                ) member
                WHERE d.primary_file IS NOT NULL
                GROUP BY p.id
            )
            SELECT i.work_key,i.result,COALESCE(m.items,'[]'::jsonb),i.owner
            FROM indexed i LEFT JOIN members m ON m.id=i.result#>>'{input,registry_publication_id}'
            ORDER BY i.work_key
            """,
            (identity, output_ref, output_ref),
        )
    ).fetchall()

    def read() -> tuple[list[dict[str, Any]], str, list[str]]:
        outputs: list[dict[str, Any]] = []
        unavailable: list[str] = []
        for key, result, members, owner in records:
            try:
                output = output_row(key, result, members)
                outputs.append(
                    {
                        **output,
                        "producing_execution": owner,
                        "execution": result.get("execution", "complete"),
                        "indexing": "complete",
                    }
                )
            except (OSError, KeyError, TypeError, ValueError):
                unavailable.append(key)
        return outputs, "available", unavailable

    return await disk(read)


def checked_path(root: Path, name: str) -> Path:
    relative = Path(name)
    path = root / relative
    if (
        relative.is_absolute()
        or ".." in relative.parts
        or path.resolve() != path
        or not path.is_file()
    ):
        raise MarketForgeError("Indexed converted artifact is missing or its path is unsafe.")
    return path


def verify_output(row: dict[str, Any], part: int | None) -> dict[str, Any]:
    pins = row["indexed_result"]["publication"]["fingerprints"]
    root = Path(row["storage_root"])
    files = row["files"]
    names = [files[part - 1]["path"], "manifest.json"] if part is not None else list(pins)
    required = {"manifest.json", "execution.json", "conversion-runtime.json"}
    if part is None and set(pins) != required | {file["path"] for file in files}:
        raise MarketForgeError("Indexed conversion fingerprint inventory is incomplete.")
    for name in names:
        if checksum(checked_path(root, name)) != pins.get(name):
            raise MarketForgeError("Converted artifact differs from its indexed fingerprint.")
    return {
        "status": "verified",
        "scope": "selected converted file and manifest checksums"
        if part is not None
        else "complete indexed conversion fingerprint inventory",
        "artifacts_checked": len(names),
        "output_ref": row["output_ref"],
        "part": part,
    }
