"""Read conversion publication, normalized-parent and raw contribution records."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, LiteralString

from psycopg import AsyncConnection
from psycopg.rows import TupleRow, dict_row

from forge.historical.archives.storage.acquire import root_key
from forge.historical.merge.discovery import (
    contributing_tasks,
)
from forge.shared.database.connection import database_flag
from forge.shared.errors import MarketForgeError
from forge.shared.paths import ProjectPaths

Connection = AsyncConnection[TupleRow]


@dataclass(frozen=True)
class Publication:
    id: int
    engine_id: str
    namespace: str
    root: Path
    manifest_path: Path
    evidence: dict[str, Any]

    @property
    def manifest(self) -> dict[str, Any]:
        return self.evidence["manifest"]

    @property
    def parent_ids(self) -> list[int]:
        return self.evidence["selected_dataset_inputs"]


async def rows(
    connection: Connection, query: LiteralString, parameters: tuple[Any, ...] = ()
) -> list[dict[str, Any]]:
    async with connection.cursor(row_factory=dict_row) as cursor:
        await cursor.execute(query, parameters)
        return await cursor.fetchall()


async def publications(
    connection: Connection,
    *,
    ids: list[int] | None = None,
    file_ids: list[int] | None = None,
    interval: tuple[int, int] | None = None,
) -> list[Publication]:
    if file_ids:
        found = await rows(
            connection,
            """
            SELECT id,publication_id FROM historical.datasets
            WHERE id=ANY(%s) AND operation='merge' AND status='complete'
              AND primary_file IS NOT NULL AND storage_format='parquet'
        """,
            (file_ids,),
        )
        if {row["id"] for row in found} != set(file_ids) or any(
            row["publication_id"] is None for row in found
        ):
            raise MarketForgeError("Select complete registered merge file_dataset_ids.")
        ids = sorted({row["publication_id"] for row in found})

    records = await rows(
        connection,
        """
        SELECT p.id,p.engine_dataset_id,p.instrument_namespace,p.storage_root,p.manifest_path,p.evidence
        FROM historical.dataset_publications p
        WHERE p.operation='merge' AND (%s::bigint[] IS NULL OR p.id=ANY(%s))
          AND EXISTS (SELECT 1 FROM historical.datasets f
              WHERE f.publication_id=p.id AND f.status='complete')
          AND (%s::bigint[] IS NOT NULL OR NOT EXISTS (
              SELECT 1 FROM historical.publication_supersessions s WHERE s.publication_id=p.id))
          AND (%s::bigint IS NULL OR EXISTS (
            SELECT 1 FROM historical.datasets d
            WHERE d.publication_id=p.id AND d.status='complete'
              AND d.start_timestamp_ns < %s AND d.end_timestamp_ns >= %s
          ))
        ORDER BY p.id
    """,
        (
            ids,
            ids,
            ids,
            interval[0] if interval else None,
            interval[1] if interval else None,
            interval[0] if interval else None,
        ),
    )
    if ids and {row["id"] for row in records} != set(ids):
        raise MarketForgeError(
            f"Unknown merge registry publication IDs: {sorted(set(ids) - {row['id'] for row in records})}."
        )
    return [
        Publication(
            row["id"],
            row["engine_dataset_id"],
            row["instrument_namespace"],
            Path(row["storage_root"]),
            Path(row["manifest_path"]),
            row["evidence"],
        )
        for row in records
    ]


def require_external_reference_mode(content_type: str) -> None:
    if content_type in {"trade", "trades"}:
        raise MarketForgeError(
            "Trade-only conversion uses internal reference trades. "
            "Use a depth or combined source publication for external references."
        )


async def selectable_publication(connection: Connection, publication_id: int) -> Publication:
    records = await rows(
        connection,
        """
        SELECT p.operation, s.state AS supersession, i.publication_id AS invalidation
        FROM historical.dataset_publications p
        LEFT JOIN historical.publication_supersessions s ON s.publication_id=p.id
        LEFT JOIN historical.publication_invalidations i ON i.publication_id=p.id
        WHERE p.id=%s
        """,
        (publication_id,),
    )
    if not records or records[0]["operation"] != "merge":
        raise MarketForgeError(
            f"Registry publication {publication_id} is not a registered merge in this environment. "
            f"Find its registry publication ID with mf datasets list --view publications --operation merge {database_flag()}."
        )
    record = records[0]
    if record["supersession"] is not None or record["invalidation"] is not None:
        raise MarketForgeError(
            f"Registry merge publication {publication_id} is superseded, retired or invalidated. "
            "Inspect its recorded state with mf datasets show --publication-id "
            f"{publication_id} {database_flag()} and select an eligible publication explicitly."
        )
    try:
        return (await publications(connection, ids=[publication_id]))[0]
    except MarketForgeError as error:
        raise MarketForgeError(
            f"Registry merge publication {publication_id} has no complete registered files."
        ) from error


async def parents(connection: Connection, pub: Publication) -> list[dict[str, Any]]:
    ids = pub.parent_ids
    if not ids or len(set(ids)) != len(ids):
        raise MarketForgeError("Merge lacks distinct registry-selected input file IDs.")
    records = await rows(
        connection,
        """
        SELECT d.id,d.primary_file,d.operation_details AS summary,p.id AS publication_id,
               p.engine_dataset_id,p.instrument_namespace,p.evidence,p.storage_root,d.file_sha256
        FROM historical.datasets d JOIN historical.dataset_publications p ON p.id=d.publication_id
        WHERE d.id=ANY(%s) AND d.operation='normalize' AND p.operation='normalize'
          AND d.status='complete' AND d.storage_format='parquet'
    """,
        (ids,),
    )
    if {row["id"] for row in records} != set(ids) or len(
        {row["instrument_namespace"] for row in records}
    ) != 1:
        raise MarketForgeError(
            "Selected normalized input files are unavailable or cross instrument namespaces."
        )
    by_id = {row["id"]: row for row in records}
    return [by_id[index] for index in ids]


def parent_tasks(parent: dict[str, Any]) -> list[dict[str, Any]]:
    return contributing_tasks(parent["evidence"])[parent["primary_file"]]


async def raw_for_files(
    connection: Connection, paths: ProjectPaths, parent_ids: list[int]
) -> dict[int, dict[str, dict[str, Any]]]:
    records = await rows(
        connection,
        """
        SELECT i.dataset_id,i.local_archive_id,i.input_sha256,l.relative_path,
               o.id AS raw_object_id,o.venue_code,o.scope_key,o.scope_json,o.data_type,
               o.start_at,o.end_at,s.source_key
        FROM historical.dataset_raw_inputs i
        JOIN historical.local_archives l ON l.id=i.local_archive_id
        JOIN historical.raw_objects o ON o.id=l.raw_object_id
        LEFT JOIN historical.raw_sources s ON s.id=o.raw_source_id
        WHERE i.dataset_id=ANY(%s) AND l.storage_root_key=%s
          AND l.status='complete' AND l.sha256=i.input_sha256
        ORDER BY i.dataset_id,i.local_archive_id
    """,
        (parent_ids, root_key(paths.raw_dir)),
    )
    result: dict[int, dict[str, dict[str, Any]]] = defaultdict(dict)
    for row in records:
        path = paths.raw_dir / row.pop("relative_path")
        if path.resolve() != path or not path.is_relative_to(paths.raw_dir):
            raise MarketForgeError("Unsafe registered raw archive path.")
        if str(path) in result[row["dataset_id"]]:
            raise MarketForgeError("Ambiguous registered raw archive path.")
        result[row["dataset_id"]][str(path)] = row
    return result
