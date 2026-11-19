"""Bounded, display-only lineage queries for the existing dataset registry."""

from typing import Any

from psycopg import AsyncCursor
from psycopg.rows import DictRow

RAW_FIELDS = """links.id,links.sha256,o.venue_code AS exchange,o.filename,
o.scope_json->>'scope_kind' AS scope_kind,o.scope_json->>'scope_identifier' AS scope,
o.start_at,o.end_at"""
RAW_JOINS = """
LEFT JOIN historical.local_archives a ON a.id=links.id
LEFT JOIN historical.raw_objects o ON o.id=a.raw_object_id"""
DATASET_FIELDS = (
    """links.id,d.primary_file,d.data_type,d.operation_details->'memberships' AS memberships"""
)


async def contribution_counts(cur: AsyncCursor[DictRow], dataset_id: int) -> dict[str, int]:
    await cur.execute(
        """SELECT
        (SELECT count(*) FROM historical.dataset_raw_inputs WHERE dataset_id=%s) AS raw,
        (SELECT count(*) FROM historical.dataset_inputs WHERE dataset_id=%s) AS datasets""",
        (dataset_id, dataset_id),
    )
    row = await cur.fetchone()
    assert row is not None
    return row


async def terminal_lineage(
    cur: AsyncCursor[DictRow],
    dataset: dict[str, Any],
    *,
    publication: bool,
    checksums: bool,
    limit: int,
) -> dict[str, Any]:
    dataset_id = dataset["id"]
    publication_id = dataset["publication_id"]
    counts = await contribution_counts(cur, dataset_id)
    await cur.execute(
        """SELECT """
        + RAW_FIELDS
        + """ FROM (
            SELECT local_archive_id AS id,input_sha256 AS sha256 FROM historical.dataset_raw_inputs
            WHERE dataset_id=%s ORDER BY local_archive_id LIMIT %s
        ) links """
        + RAW_JOINS
        + " ORDER BY links.id",
        (dataset_id, limit),
    )
    raw = await cur.fetchall()
    await cur.execute(
        """SELECT """
        + DATASET_FIELDS
        + """ FROM (
            SELECT input_dataset_id AS id FROM historical.dataset_inputs
            WHERE dataset_id=%s ORDER BY input_dataset_id LIMIT %s
        ) links LEFT JOIN historical.datasets d ON d.id=links.id ORDER BY links.id""",
        (dataset_id, limit),
    )
    parents = await cur.fetchall()
    await cur.execute(
        """SELECT jsonb_array_length(evidence->'selected_raw_inputs') AS raw,
                  jsonb_array_length(evidence->'selected_dataset_inputs') AS datasets
           FROM historical.dataset_publications WHERE id=%s""",
        (publication_id,),
    )
    selected = await cur.fetchone()
    result: dict[str, Any] = {
        "file": dataset,
        "counts": counts,
        "raw": raw,
        "parents": parents,
        "selected": selected or {},
        "checksums": checksums,
        "limit": limit,
    }
    if publication:
        await cur.execute(
            """SELECT """
            + RAW_FIELDS
            + """ FROM (
                SELECT (entry->>0)::bigint AS id,entry->>1 AS sha256,ord
                FROM historical.dataset_publications p
                CROSS JOIN LATERAL jsonb_array_elements(p.evidence->'selected_raw_inputs')
                    WITH ORDINALITY AS entries(entry,ord)
                WHERE p.id=%s ORDER BY ord LIMIT %s
            ) links """
            + RAW_JOINS
            + " ORDER BY links.ord",
            (publication_id, limit),
        )
        result["publication_raw"] = await cur.fetchall()
        await cur.execute(
            """SELECT """
            + DATASET_FIELDS
            + """ FROM (
                SELECT (entry#>>'{}')::bigint AS id,ord
                FROM historical.dataset_publications p
                CROSS JOIN LATERAL jsonb_array_elements(p.evidence->'selected_dataset_inputs')
                    WITH ORDINALITY AS entries(entry,ord)
                WHERE p.id=%s ORDER BY ord LIMIT %s
            ) links LEFT JOIN historical.datasets d ON d.id=links.id ORDER BY links.ord""",
            (publication_id, limit),
        )
        result["publication_datasets"] = await cur.fetchall()
    return result
