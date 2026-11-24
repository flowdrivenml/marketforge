"""Bounded, read-only metadata projections for the Forge workspace."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, LiteralString

from psycopg import AsyncConnection
from psycopg.rows import TupleRow, dict_row
from psycopg.sql import SQL

from forge.datasets.converted import manifest_evidence
from forge.datasets.inventory import BASE
from forge.historical.archives.storage.files import disk
from forge.shared.database.connection import database_connection, database_dsn
from forge.shared.paths import ProjectPaths
from forge.storage.browsing import archive_document
from forge.storage.models import StorageFilters
from forge.storage.registry import REGISTRY, parameters

PAGE_SIZE = 20

REGISTERED = """
    SELECT i.id,e.code AS exchange,i.symbol,i.instrument_type,i.market_category
    FROM catalog.instruments i JOIN catalog.exchanges e ON e.id=i.exchange_id
"""
ARCHIVES: LiteralString = (
    REGISTRY
    + ", canonical_raw AS ("
    + REGISTERED
    + """)
    , scopes AS (
        SELECT r.id,r.exchange,r.instrument_type,r.market_category,r.scope_identifier,
               r.data_type,r.start_at,r.end_at,r.bytes_written,0::bigint AS binding
        FROM matching r WHERE r.scope_kind='instrument'
        UNION ALL
        SELECT r.id,a.identity_json->>'exchange',a.identity_json->>'instrument_type',
               a.identity_json->>'market_category',
               coalesce(a.identity_json->>'scope_identifier',a.identity_json->>'symbol'),
               a.data_type,a.declared_start_at,a.declared_end_at,r.bytes_written,b.id
        FROM matching r JOIN historical.raw_bindings b ON b.local_archive_id=r.id
        JOIN historical.external_archives a ON a.id=b.interpretation_id
        WHERE coalesce(a.identity_json->>'scope_kind','instrument')='instrument'
          AND a.import_provenance#>>'{binding,readiness,normalization_handoff}'='ready'
    ), raw_members AS (
        SELECT DISTINCT ON (r.id,i.id,r.data_type)
               r.id,i.exchange,i.symbol,i.id AS instrument,i.instrument_type,i.market_category,
               r.data_type,r.start_at,r.end_at,r.bytes_written
        FROM scopes r JOIN canonical_raw i ON
           (r.exchange,r.instrument_type,r.market_category,r.scope_identifier) =
           (i.exchange,i.instrument_type,i.market_category,i.symbol)
        ORDER BY r.id,i.id,r.data_type,r.binding DESC
    )
    """
)

ARCHIVE_WINDOWS: LiteralString = (
    ARCHIVES
    + """
, member_groups AS (
    SELECT id, CASE WHEN count(*)=1 THEN min(instrument) END AS instrument,
           CASE WHEN count(*)=1 THEN min(symbol) END AS symbol,
           jsonb_agg(jsonb_build_object('catalogue_instrument_id',instrument,'symbol',symbol,
               'exchange',exchange,'instrument_type',instrument_type,
               'market_category',market_category,'data_kind',data_type)
               ORDER BY instrument,data_type) AS instruments
    FROM raw_members GROUP BY id
), entries AS (
    SELECT r.id,r.exchange,r.instrument_type,r.market_category,r.scope_kind,r.scope_identifier,
           r.data_type,r.raw_source_key,r.external_source_key,r.provider,r.feed,
           r.start_at,r.end_at,r.bytes_written,members.instrument,members.symbol,members.instruments,
           jsonb_build_array(r.exchange,r.instrument_type,r.market_category,r.scope_kind,
               r.scope_identifier,members.instrument,r.data_type,r.raw_source_key,r.feed,
               r.external_source_key,r.acquisition_method,
               CASE WHEN members.instrument IS NULL OR r.external_archive_id IS NOT NULL
                    OR count(*) OVER (PARTITION BY r.raw_object_id)>1 OR r.start_at IS NULL
                    OR r.end_at IS NULL OR r.start_at>=r.end_at THEN r.id END) AS source_identity
    FROM matching r LEFT JOIN member_groups members USING (id)
), ordered AS (
    SELECT *, max(end_at) OVER (
        PARTITION BY source_identity ORDER BY start_at,end_at,id
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ) AS prior_end FROM entries
), numbered AS (
    SELECT *, sum(CASE WHEN prior_end IS NULL OR start_at>prior_end THEN 1 ELSE 0 END)
        OVER (PARTITION BY source_identity ORDER BY start_at,end_at,id) AS island
    FROM ordered
)
SELECT exchange,coalesce(symbol, CASE WHEN jsonb_array_length(instruments)>1
       THEN 'Multiple (' || jsonb_array_length(instruments) || ')'
       WHEN scope_kind='instrument' THEN scope_identifier
       WHEN scope_identifier IS NOT NULL THEN 'Scope: ' || scope_identifier END) AS symbol,
       instrument,instruments,scope_kind,instrument_type,market_category,data_type,
       coalesce(raw_source_key,external_source_key) AS source,feed,provider,
       min(start_at) AS start,max(end_at) AS finish,count(*) AS file_count,
       sum(bytes_written) AS recorded_bytes,'complete' AS state,
       array_agg('local:' || id::text ORDER BY start_at,end_at,id) AS archives
FROM numbered GROUP BY source_identity,island,exchange,symbol,instrument,instruments,scope_kind,
      scope_identifier,instrument_type,market_category,data_type,raw_source_key,
      external_source_key,feed,provider
ORDER BY min(start_at) DESC NULLS LAST,min(id)
"""
)

NORMALIZED: LiteralString = (
    BASE.replace(
        "item->>'exchange' AS exchange",
        "NULLIF(item->>'exchange','') AS exchange, item AS membership",
    ).replace(
        "WHERE f.namespace IS NOT NULL AND NULLIF(item->>'instrument_id','') IS NOT NULL"
        " AND NULLIF(item->>'exchange','') IS NOT NULL",
        "WHERE f.operation='normalize'",
    )
    + """
, membership AS (
    SELECT f.id,f.publication,f.namespace,
        m.instrument,m.exchange,max(m.symbol) AS symbol,max(m.instrument_type) AS instrument_type,
        max(m.category) AS category,m.stream,
        coalesce(m.kind,f.data_type) AS kind,
        min(coalesce(
            CASE WHEN coalesce(m.membership->>'observed_start_ns',
                               m.membership->>'start_timestamp_ns') ~ '^-?[0-9]{1,19}$'
                 THEN coalesce(m.membership->>'observed_start_ns',
                               m.membership->>'start_timestamp_ns')::numeric END,
            CASE WHEN jsonb_array_length(CASE WHEN jsonb_typeof(f.details->'memberships')='array'
                 THEN f.details->'memberships' ELSE '[]'::jsonb END)<=1 THEN d.start_timestamp_ns END
        )) AS lo,
        max(coalesce(m.latest, CASE WHEN jsonb_array_length(
            CASE WHEN jsonb_typeof(f.details->'memberships')='array'
                 THEN f.details->'memberships' ELSE '[]'::jsonb END)<=1 THEN d.end_timestamp_ns END
        )) AS hi,
        f.bytes,d.primary_file
    FROM files f JOIN historical.datasets d ON d.id=f.id
    LEFT JOIN members m ON m.id=f.id
    WHERE f.operation='normalize'
    GROUP BY f.id,f.publication,f.namespace,m.instrument,m.exchange,m.stream,coalesce(m.kind,f.data_type),
             f.bytes,d.primary_file
), bounds AS (
    SELECT publication,namespace,instrument,exchange,max(symbol) AS symbol,
        max(instrument_type) AS instrument_type,max(category) AS market_category,stream,
        kind AS data_type,min(lo) AS start,max(hi) AS finish,count(DISTINCT id) AS file_count,
        CASE WHEN count(*)=count(bytes) THEN sum(bytes) END AS recorded_bytes,
        array_agg(id ORDER BY id) AS primary_files,array_agg(primary_file) AS paths
    FROM membership
    GROUP BY publication,namespace,instrument,exchange,stream,kind
)
SELECT b.*,p.engine_dataset_id AS engine_publication,
       coalesce(p.evidence->>'generation',p.evidence->'execution'->>'generation') AS generation,
       p.evidence->'manifest'->>'integrity_status' AS verification,
       'Registered' AS state, partitions.count AS partitions
FROM bounds b JOIN historical.dataset_publications p ON p.id=b.publication
LEFT JOIN LATERAL (
    SELECT nullif(count(DISTINCT item->>'partition_id'),0) AS count
    FROM jsonb_array_elements(CASE WHEN jsonb_typeof(p.evidence->'manifest'->'files')='array'
          THEN p.evidence->'manifest'->'files' ELSE '[]'::jsonb END) item
    WHERE item->>'path' = ANY(b.paths)
) partitions ON true
ORDER BY start DESC NULLS LAST,b.publication DESC,b.instrument,b.stream,b.data_type,b.exchange
"""
)

MERGED: LiteralString = (
    BASE.replace("WHERE f.namespace IS NOT NULL AND", "WHERE")
    + """
, bounds AS (
    SELECT f.publication,min(d.start_timestamp_ns) AS start,max(d.end_timestamp_ns) AS finish,
           count(*) AS file_count, CASE WHEN count(bytes)=count(*) THEN sum(bytes) END AS recorded_bytes
    FROM files f JOIN historical.datasets d ON d.id=f.id
    WHERE f.operation='merge' GROUP BY f.publication
), page AS (
    SELECT * FROM bounds ORDER BY start DESC NULLS LAST,publication DESC
)
SELECT p.publication, reg.engine_dataset_id AS engine_publication,p.start,p.finish,p.file_count,
       p.recorded_bytes, partitions.count AS partitions, array_agg(DISTINCT m.exchange) FILTER (WHERE m.exchange IS NOT NULL) AS exchanges,
       count(DISTINCT (m.exchange,m.instrument)) FILTER (WHERE m.instrument IS NOT NULL) AS instruments,
       array_agg(DISTINCT concat_ws(' · ',initcap(m.instrument_type),
           CASE WHEN m.category<>m.instrument_type THEN initcap(m.category) END))
           FILTER (WHERE m.instrument_type IS NOT NULL) AS markets,
       'Registered' AS state,
       array_agg(DISTINCT m.kind) FILTER (WHERE m.kind IS NOT NULL) AS kinds
FROM page p JOIN historical.dataset_publications reg ON reg.id=p.publication
LEFT JOIN members m ON m.publication=p.publication
LEFT JOIN LATERAL (
    SELECT nullif(count(DISTINCT item->>'partition_id'),0) AS count
    FROM jsonb_array_elements(CASE WHEN jsonb_typeof(reg.evidence->'manifest'->'files')='array'
          THEN reg.evidence->'manifest'->'files' ELSE '[]'::jsonb END) item
) partitions ON true
GROUP BY p.publication,reg.engine_dataset_id,p.start,p.finish,p.file_count,p.recorded_bytes,partitions.count
ORDER BY p.start DESC NULLS LAST,p.publication DESC
"""
)

CONVERTED: LiteralString = """
    SELECT work_key,result,owner
    FROM historical.resolved_work
    WHERE operation='convert' AND result->>'status'='complete'
      AND NULLIF(result#>>'{publication,manifest}','') IS NOT NULL
      AND registry_id=(SELECT database_id FROM forge_meta.database_identity WHERE singleton)
"""

CONVERTED_PAGE: LiteralString = (
    """
WITH outputs AS ("""
    + CONVERTED
    + """), distinct_outputs AS (
    SELECT DISTINCT ON (result#>>'{publication,manifest}') * FROM outputs
    ORDER BY result#>>'{publication,manifest}',work_key
), batch AS (
    SELECT * FROM distinct_outputs ORDER BY work_key
)
SELECT work_key AS output_ref, result->'publication' AS publication_evidence, result#>>'{publication,output,content_type}' AS data_type,
       result#>>'{publication,output,first_timestamp_ns}' AS start,
       result#>>'{publication,output,last_timestamp_ns}' AS finish,
       result#>>'{publication,output,files_written}' AS file_count,
       result#>>'{publication,size_bytes}' AS recorded_bytes,
       result#>>'{publication,materialized_partitions}' AS partitions,
       coalesce(result#>>'{conversion,target_quote}',result#>>'{policy,target_quote}',
                result->>'target_quote') AS target_quote,
       members.exchanges,members.instruments,members.markets,
       'Indexed' AS state
FROM batch
LEFT JOIN LATERAL (
    SELECT array_agg(DISTINCT concat_ws(' · ',initcap(item->>'instrument_type'),
             CASE WHEN item->>'market_category'<>item->>'instrument_type'
                  THEN initcap(item->>'market_category') END))
             FILTER (WHERE item ? 'instrument_type') AS markets,
           array_agg(DISTINCT item->>'exchange') FILTER (WHERE item ? 'exchange') AS exchanges,
           count(DISTINCT (item->>'exchange',item->>'instrument_id'))
               FILTER (WHERE item ? 'instrument_id') AS instruments
    FROM (
        SELECT item FROM jsonb_array_elements(
            CASE WHEN jsonb_typeof(batch.result#>'{input,files}')='array'
                 THEN batch.result#>'{input,files}' ELSE '[]'::jsonb END
        ) saved CROSS JOIN LATERAL jsonb_array_elements(
            CASE WHEN jsonb_typeof(saved#>'{summary,memberships}')='array'
                 THEN saved#>'{summary,memberships}' ELSE '[]'::jsonb END
        ) item
        UNION ALL
        SELECT item FROM historical.datasets d
        CROSS JOIN LATERAL jsonb_array_elements(
            CASE WHEN jsonb_typeof(d.operation_details->'memberships')='array'
                 THEN d.operation_details->'memberships' ELSE '[]'::jsonb END
        ) item
        WHERE d.publication_id::text=batch.result#>>'{input,registry_publication_id}'
          AND d.primary_file IS NOT NULL
          AND coalesce(batch.result#>'{input,files}','[]'::jsonb)='[]'::jsonb
    ) membership
) members ON true
ORDER BY work_key
"""
)


@dataclass(frozen=True)
class WorkspaceInventory:
    registered: int
    with_history: int
    exchanges: int
    universe: tuple[TupleRow, ...]
    availability: tuple[TupleRow, ...]
    storage: dict[str, tuple[int | None, int]]


@asynccontextmanager
async def readonly(paths: ProjectPaths) -> AsyncGenerator[AsyncConnection[TupleRow]]:
    async with (
        database_connection(database_dsn(paths.root)) as connection,
        connection.transaction(),
    ):
        await connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        await connection.execute("SET LOCAL statement_timeout='5s'")
        yield connection


async def stage_page(
    paths: ProjectPaths, stage: str, offset: int, search: str = ""
) -> tuple[list[dict[str, Any]], bool]:
    queries: dict[str, LiteralString] = {
        "Raw": ARCHIVE_WINDOWS,
        "Normalized": NORMALIZED,
        "Merged": MERGED,
        "Converted": CONVERTED_PAGE,
    }
    params = parameters(paths.raw_dir, StorageFilters(status="complete"))
    search = search.strip().casefold()
    exact = (
        "publication"
        if stage in {"Normalized", "Merged"} and search.isdecimal()
        else "output_ref"
        if stage == "Converted" and len(search) == 64
        else None
    )
    params.update(offset=max(offset, 0), limit=PAGE_SIZE + 1, search=search)
    predicate = (
        SQL("publication::text = %(search)s OR engine_publication = %(search)s")
        if exact == "publication"
        else SQL("output_ref = %(search)s")
        if exact == "output_ref"
        else SQL("strpos(lower(inventory::text), %(search)s)>0 OR %(search)s=''")
    )
    async with readonly(paths) as connection, connection.cursor(row_factory=dict_row) as cursor:
        await cursor.execute(
            SQL("SELECT * FROM ({}) inventory WHERE {} LIMIT %(limit)s OFFSET %(offset)s").format(
                SQL(queries[stage]), predicate
            ),
            params,
        )
        result = await cursor.fetchall()
    page = result[:PAGE_SIZE]
    if stage == "Converted":
        await disk(fill_converted_metadata, page)
    return page, len(result) > PAGE_SIZE


async def archive_group(paths: ProjectPaths, ids: list[int]) -> list[dict[str, object]]:
    params = parameters(paths.raw_dir, StorageFilters(status="complete"))
    params["archive_ids"] = ids
    async with readonly(paths) as connection, connection.cursor(row_factory=dict_row) as cursor:
        await cursor.execute(
            REGISTRY + "SELECT * FROM matching WHERE id=ANY(%(archive_ids)s) ORDER BY start_at,id",
            params,
        )
        rows = await cursor.fetchall()
    return [archive_document(row, paths.raw_dir) for row in rows]


def fill_converted_metadata(rows: list[dict[str, Any]]) -> None:
    for row in rows:
        publication = row.pop("publication_evidence")
        if all(
            row.get(key) is not None
            for key in ("target_quote", "data_type", "start", "finish", "file_count")
        ):
            continue
        try:
            _, manifest = manifest_evidence({"publication": publication}, verify_fingerprint=False)
            output = manifest["output"]
            fallback = {
                "target_quote": manifest["conversion"]["target_quote"],
                "data_type": output.get("content_type"),
                "start": output.get("first_timestamp_ns"),
                "finish": output.get("last_timestamp_ns"),
                "file_count": output.get("files_written"),
            }
        except (OSError, ValueError, KeyError, TypeError):
            continue
        for key, value in fallback.items():
            if row[key] is None:
                row[key] = value


async def summary(paths: ProjectPaths) -> WorkspaceInventory:
    params = parameters(paths.raw_dir, StorageFilters(status="complete"))
    async with readonly(paths) as connection:
        universe = await (
            await connection.execute(
                "WITH instruments AS ("
                + REGISTERED
                + """)
            SELECT exchange,count(*) FILTER (WHERE instrument_type='spot'),
                   count(*) FILTER (WHERE instrument_type='perpetual'),
                   count(*) FILTER (WHERE instrument_type='future'),
                   count(*) FILTER (WHERE instrument_type='equity'),count(*) FROM instruments
            GROUP BY exchange ORDER BY exchange
            """
            )
        ).fetchall()
        raw = await (
            await connection.execute(
                ARCHIVES
                + """
            SELECT sum(bytes_written), count(*)-count(bytes_written), count(*)
            FROM matching
        """,
                params,
            )
        ).fetchone()
        availability = await (
            await connection.execute(
                ARCHIVES
                + ", "
                + BASE.removeprefix("\nWITH ")
                + """
            , availability AS (
                SELECT exchange,instrument_type,category,kind FROM members
                UNION ALL
                SELECT exchange,instrument_type,market_category,
                       CASE data_type WHEN 'trades' THEN 'trade'
                                      WHEN 'order_book_l2' THEN 'depth' END
                FROM matching
            )
            SELECT exchange,instrument_type,category,bool_or(kind='trade'),bool_or(kind='depth')
            FROM availability WHERE kind IN ('trade','depth') AND exchange IS NOT NULL
                AND instrument_type IS NOT NULL AND category IS NOT NULL
            GROUP BY 1,2,3 ORDER BY 1,2,3
            """,
                params,
            )
        ).fetchall()
        historical = await (
            await connection.execute(
                ARCHIVES
                + ", "
                + BASE.removeprefix("\nWITH ")
                + ", canonical AS ("
                + REGISTERED
                + """)
            SELECT count(*) FROM (
                SELECT c.id FROM canonical c JOIN members m
                ON (c.exchange,c.symbol,c.instrument_type,c.market_category) =
                   (m.exchange,m.symbol,m.instrument_type,m.category)
                UNION SELECT instrument FROM raw_members
            ) covered
            """,
                params,
            )
        ).fetchone()
        primary = await (
            await connection.execute(
                BASE
                + """
            , identities AS (
                SELECT DISTINCT ON (p.storage_root, d.primary_file)
                    f.operation,f.bytes
                FROM files f JOIN historical.datasets d ON d.id=f.id
                JOIN historical.dataset_publications p ON p.id=f.publication
                ORDER BY p.storage_root,d.primary_file,f.bytes DESC NULLS LAST,f.operation
            )
            SELECT operation,sum(bytes),count(*)-count(bytes),count(*) FROM identities GROUP BY operation
        """
            )
        ).fetchall()
        converted = await (
            await connection.execute(
                """
            WITH physical AS (SELECT DISTINCT ON (result#>>'{publication,manifest}')
                CASE WHEN result#>>'{publication,size_bytes}' ~ '^[0-9]+$'
                THEN (result#>>'{publication,size_bytes}')::numeric END AS bytes
                FROM ("""
                + CONVERTED
                + """) work
                ORDER BY result#>>'{publication,manifest}',bytes DESC NULLS LAST)
            SELECT sum(bytes),count(*)-count(bytes),count(*) FROM physical
        """
            )
        ).fetchone()
    assert raw is not None and converted is not None and historical is not None
    storage: dict[str, tuple[int | None, int]] = {"Normalized": (0, 0), "Merged": (0, 0)}
    for stage, size, missing, count in (
        ("Raw", *raw),
        ("Converted", *converted),
        *(("Normalized" if row[0] == "normalize" else "Merged", *row[1:]) for row in primary),
    ):
        storage[stage] = (int(size) if size is not None else (None if count else 0), int(missing))
    return WorkspaceInventory(
        sum(row[-1] for row in universe),
        historical[0],
        len(universe),
        tuple(universe),
        tuple(availability),
        storage,
    )
