"""Cardinality-preserving registry joins and parameterized browsing filters."""

from pathlib import Path
from typing import LiteralString

from psycopg import AsyncConnection
from psycopg.rows import TupleRow, dict_row

from forge.historical.archives.storage.acquire import root_key
from forge.markets.filters import symbol_pattern
from forge.storage.models import StorageFilters

# Gate's canonical registry intentionally omits source_feed for incremental depth;
# archive_scope_document adds it only for snapshot depth. External/other unknown
# feeds remain NULL and never join a known feed identity.
REGISTRY: LiteralString = """
WITH identities AS (
 SELECT l.*, CASE WHEN l.external_archive_id IS NULL THEN 'remote' ELSE 'external' END AS origin,
   r.filename, a.source_object_key AS external_object, a.normalization_readiness,a.identity_json,
   r.scope_json,r.source_metadata,a.specifications_json,a.import_provenance,a.format_profile,
   a.external_source_id,s.source_key AS external_source_key,s.source_kind,s.root_uri AS external_source_root,
   a.bytes_expected,a.sha256 AS registered_source_sha256,
   COALESCE(e.code,a.identity_json->>'exchange') AS exchange,
   COALESCE(r.scope_json->>'instrument_type',a.identity_json->>'instrument_type') AS instrument_type,
   COALESCE(r.scope_json->>'market_category',a.identity_json->>'market_category') AS market_category,
   CASE WHEN r.id IS NOT NULL THEN r.scope_json->>'scope_kind' ELSE COALESCE(a.identity_json->>'scope_kind','instrument') END AS scope_kind,
   COALESCE(r.scope_json->>'scope_identifier',a.identity_json->>'scope_identifier',a.identity_json->>'symbol') AS scope_identifier,
   COALESCE(r.data_type,a.data_type) AS data_type,
   CASE WHEN r.id IS NOT NULL THEN r.start_at ELSE a.declared_start_at END AS start_at,
   CASE WHEN r.id IS NOT NULL THEN r.end_at ELSE a.declared_end_at END AS end_at,
   CASE WHEN r.scope_json ? 'source_feed' THEN r.scope_json->>'source_feed'
        WHEN r.id IS NOT NULL AND e.code='gateio' AND r.data_type='order_book_l2' THEN 'incremental'
        ELSE a.identity_json->>'source_feed' END AS feed,
   CASE WHEN r.scope_json ? 'source_feed' THEN 'registered_source_feed'
        WHEN r.id IS NOT NULL AND e.code='gateio' AND r.data_type='order_book_l2' THEN 'canonical_gate_incremental_default'
        WHEN a.identity_json ? 'source_feed' THEN 'external_identity' ELSE 'unknown_or_not_applicable' END AS feed_source,
   COALESCE(r.source_object_key,a.source_object_key) AS source_object_key,
   r.source_etag AS registered_source_etag,r.source_last_modified AS registered_source_last_modified
 FROM historical.local_archives l
 LEFT JOIN historical.remote_archives r ON r.id=l.remote_archive_id
 LEFT JOIN catalog.exchanges e ON e.id=r.exchange_id
 LEFT JOIN historical.external_archives a ON a.id=l.external_archive_id
 LEFT JOIN historical.external_sources s ON s.id=a.external_source_id
 WHERE l.storage_root_key=%(root)s
), enriched AS (
 SELECT d.*,
   COALESCE(NULLIF(d.specifications_json->>'base_asset',''),assets.base_asset) AS base_asset,
   COALESCE(NULLIF(d.specifications_json->>'quote_asset',''),assets.quote_asset) AS quote_asset,
   assets.candidate_count,
   CASE WHEN d.specifications_json->>'base_asset' IS NOT NULL THEN 'external_historical_specification'
        WHEN assets.base_asset IS NOT NULL THEN 'catalogue' ELSE 'unknown_or_ambiguous' END AS base_asset_source,
   CASE WHEN d.specifications_json->>'quote_asset' IS NOT NULL THEN 'external_historical_specification'
        WHEN assets.quote_asset IS NOT NULL THEN 'catalogue' ELSE 'unknown_or_ambiguous' END AS quote_asset_source
 FROM identities d
 LEFT JOIN LATERAL (
   SELECT count(*) AS candidate_count,
      CASE WHEN count(DISTINCT lower(i.base_asset))=1 THEN min(i.base_asset) END AS base_asset,
      CASE WHEN count(DISTINCT lower(i.quote_asset))=1 THEN min(i.quote_asset) END AS quote_asset
   FROM catalog.instruments i JOIN catalog.exchanges ex ON ex.id=i.exchange_id
   WHERE ex.code=d.exchange AND i.instrument_type=d.instrument_type AND i.market_category=d.market_category
     AND ((d.scope_kind='instrument' AND i.symbol=d.scope_identifier)
       OR (d.scope_kind='base_coin' AND i.base_asset=d.scope_identifier)
       OR (d.scope_kind='family' AND
          (SELECT raw.raw_json->>'instFamily' FROM catalog.instrument_api_raw raw
           WHERE raw.instrument_id=i.id ORDER BY raw.fetched_at DESC,raw.id DESC LIMIT 1)=d.scope_identifier))
 ) assets ON true
), selected AS (
 SELECT * FROM enriched
 WHERE (%(exchange)s::text IS NULL OR exchange=%(exchange)s)
   AND (%(market)s::text IS NULL OR market_category=%(market)s)
   AND (%(instrument_type)s::text IS NULL OR instrument_type=%(instrument_type)s)
   AND (%(instrument)s::text IS NULL OR scope_identifier=%(instrument)s)
   AND (%(scope_kind)s::text IS NULL OR scope_kind=%(scope_kind)s)
   AND (%(pattern)s::text IS NULL OR scope_identifier ILIKE %(pattern)s ESCAPE '!')
   AND (%(base_asset)s::text IS NULL OR lower(base_asset)=lower(%(base_asset)s))
   AND (%(quote_asset)s::text IS NULL OR lower(quote_asset)=lower(%(quote_asset)s))
   AND (%(data_type)s::text IS NULL OR data_type=%(data_type)s)
   AND (%(feed)s::text IS NULL OR feed=%(feed)s OR (%(feed)s='unknown' AND feed IS NULL))
   AND (%(origin)s::text IS NULL OR origin=%(origin)s)
   AND (%(status)s::text IS NULL OR status=%(status)s)
   AND (%(archive_id)s::bigint IS NULL OR id=%(archive_id)s)
), matching AS (
 SELECT * FROM selected WHERE
   (%(start)s::timestamptz IS NULL AND %(end)s::timestamptz IS NULL)
   OR (start_at IS NOT NULL AND end_at IS NOT NULL AND start_at<end_at
       AND (%(start)s::timestamptz IS NULL OR CASE WHEN %(containment)s THEN start_at>=%(start)s ELSE end_at>%(start)s END)
       AND (%(end)s::timestamptz IS NULL OR CASE WHEN %(containment)s THEN end_at<=%(end)s ELSE start_at<%(end)s END))
)
"""


def parameters(
    root: Path, filters: StorageFilters, archive_id: int | None = None
) -> dict[str, object]:
    return {
        **filters.model_dump(),
        "root": root_key(root),
        "pattern": symbol_pattern(filters.match),
        "archive_id": archive_id,
        "containment": False,
    }


async def counts(c: AsyncConnection[TupleRow], params: dict[str, object]) -> dict[str, object]:
    async with c.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            REGISTRY
            + """SELECT count(*) AS total,COALESCE(sum(bytes_written),0) AS recorded_bytes,
           count(*) FILTER(WHERE bytes_written IS NULL) AS unknown_size_count,
           (SELECT count(*) FROM selected WHERE start_at IS NULL OR end_at IS NULL OR start_at>=end_at) AS unusable_intervals_before_date_filter
           FROM matching""",
            params,
        )
        row = await cur.fetchone()
        assert row is not None
        return {key: int(str(value)) for key, value in row.items()}


async def page(c: AsyncConnection[TupleRow], params: dict[str, object]) -> list[dict[str, object]]:
    async with c.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            REGISTRY + "SELECT * FROM matching ORDER BY id DESC LIMIT %(limit)s OFFSET %(offset)s",
            params,
        )
        return list(await cur.fetchall())


async def scan(
    c: AsyncConnection[TupleRow], params: dict[str, object], after: int
) -> list[dict[str, object]]:
    async with c.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            REGISTRY + "SELECT * FROM matching WHERE id>%(after)s ORDER BY id LIMIT 1000",
            {**params, "after": after},
        )
        return list(await cur.fetchall())
