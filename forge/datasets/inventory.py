"""Read-only counts over registered primary files and retained membership."""

from dataclasses import dataclass
from datetime import UTC, datetime

from psycopg import AsyncConnection
from psycopg.rows import TupleRow

from forge.historical.archives.storage.acquire import root_key
from forge.shared.database.connection import database_connection, database_dsn
from forge.shared.paths import ProjectPaths


@dataclass(frozen=True)
class Market:
    namespace: str
    exchange: str
    identity: str
    symbol: str
    instrument_type: str
    category: str
    trades: bool
    depth: bool
    latest: int | None


@dataclass(frozen=True)
class Stage:
    operation: str
    count: int
    files: int
    size: int | None
    missing_sizes: int
    rows: int | None
    streams: int


@dataclass(frozen=True)
class Inventory:
    stages: tuple[Stage, ...]
    instruments: int
    exchanges: int
    streams: int
    trades: int
    depth: int
    size: int | None
    missing_sizes: int
    markets: tuple[Market, ...]
    checked: datetime
    unresolved_files: int = 0


# A primary-file registry row is unique on (publication_id, primary_file).
# Normalize and merge preserve the canonical stream_id, including distinct feed suffixes.
BASE = """
WITH files AS (
    SELECT d.id, p.id AS publication, p.operation,
        CASE WHEN p.operation='normalize' THEN p.instrument_namespace ELSE (
            SELECT CASE WHEN count(DISTINCT parent->>'namespace')=1 THEN max(parent->>'namespace') END
            FROM jsonb_array_elements(CASE WHEN jsonb_typeof(p.evidence->'merge_inputs')='array'
                THEN p.evidence->'merge_inputs' ELSE '[]'::jsonb END) parent
        ) END AS namespace,
        d.data_type, d.operation_details AS details,
        CASE WHEN d.operation_details->>'size_bytes' ~ '^[0-9]+$'
             THEN (d.operation_details->>'size_bytes')::numeric END AS bytes,
        CASE WHEN d.operation_details->>'rows' ~ '^[0-9]+$'
             THEN (d.operation_details->>'rows')::numeric END AS rows
    FROM historical.dataset_publications p
    JOIN historical.datasets d ON d.publication_id=p.id AND d.primary_file IS NOT NULL
    WHERE p.operation IN ('normalize','merge') AND d.status='complete'
), members AS (
    SELECT f.id, f.publication, f.operation, f.namespace, item->>'instrument_id' AS instrument,
        item->>'exchange' AS exchange, item->>'symbol' AS symbol, item->>'stream_id' AS stream,
        item->>'instrument_type' AS instrument_type, item->>'market_category' AS category,
        CASE COALESCE(item->>'kind', item->>'data_type', f.data_type)
             WHEN 'trade' THEN 'trade' WHEN 'trades' THEN 'trade'
             WHEN 'l2' THEN 'depth' WHEN 'depth' THEN 'depth'
             WHEN 'order_book_l2' THEN 'depth' ELSE 'other' END AS kind,
        CASE WHEN COALESCE(item->>'observed_end_ns',item->>'end_timestamp_ns') ~ '^-?[0-9]{1,18}$'
             THEN COALESCE(item->>'observed_end_ns',item->>'end_timestamp_ns')::bigint
             WHEN COALESCE(item->>'observed_end_ns',item->>'end_timestamp_ns') ~ '^-?[0-9]{19}$'
             THEN COALESCE(item->>'observed_end_ns',item->>'end_timestamp_ns')::numeric END AS latest
    FROM files f
    CROSS JOIN LATERAL jsonb_array_elements(
        CASE WHEN jsonb_typeof(f.details->'memberships')='array'
        THEN f.details->'memberships' ELSE '[]'::jsonb END) item
    WHERE f.namespace IS NOT NULL AND NULLIF(item->>'instrument_id','') IS NOT NULL AND NULLIF(item->>'exchange','') IS NOT NULL
)
"""

MARKETS = (
    BASE
    + """
SELECT namespace, exchange, instrument, COALESCE(MAX(symbol),instrument),
    COALESCE(MAX(instrument_type),''), COALESCE(MAX(category),''),
    BOOL_OR(kind='trade'), BOOL_OR(kind='depth'), MAX(latest)
FROM members GROUP BY namespace, exchange, instrument
ORDER BY exchange, MAX(symbol), namespace, instrument LIMIT %s OFFSET %s
"""
)

TOTALS = (
    BASE
    + """
, streams AS (
    SELECT namespace, exchange, instrument, stream,
        CASE WHEN count(DISTINCT kind) FILTER (WHERE kind<>'other')=1
             THEN max(kind) FILTER (WHERE kind<>'other') ELSE 'other' END AS kind
    FROM members WHERE NULLIF(stream,'') IS NOT NULL
    GROUP BY namespace, exchange, instrument, stream
)
SELECT (SELECT count(DISTINCT (namespace, exchange, instrument)) FROM members),
       (SELECT count(DISTINCT exchange) FROM members),
       (SELECT count(*) FROM streams),
       (SELECT count(*) FROM streams WHERE kind='trade'),
       (SELECT count(*) FROM streams WHERE kind='depth'),
       sum(bytes), count(*)-count(bytes),
       count(*) FILTER (WHERE NOT EXISTS (SELECT 1 FROM members m WHERE m.id=files.id AND NULLIF(m.stream,'') IS NOT NULL))
FROM files
"""
)

STAGES = (
    BASE
    + """
SELECT operation, count(DISTINCT publication), count(*), sum(bytes),
       count(*)-count(bytes), CASE WHEN count(*)=count(rows) THEN sum(rows) END,
       (SELECT count(DISTINCT (namespace,exchange,instrument,stream)) FROM members m
        WHERE NULLIF(stream,'') IS NOT NULL AND m.operation=f.operation)
FROM files f GROUP BY operation
"""
)


async def inventory(paths: ProjectPaths) -> Inventory:
    async with database_connection(database_dsn(paths.root)) as conn, conn.transaction():
        await conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        await conn.execute("SET LOCAL statement_timeout = '5s'")
        return await summarize(conn)


async def summarize(conn: AsyncConnection[TupleRow]) -> Inventory:
    totals = await (await conn.execute(TOTALS)).fetchone()
    assert totals is not None
    stages = await (await conn.execute(STAGES)).fetchall()
    markets = await (await conn.execute(MARKETS, (5, 0))).fetchall()
    return Inventory(
        tuple(
            Stage(
                op,
                count,
                files,
                int(size) if size is not None else None,
                missing,
                int(rows) if rows is not None else None,
                streams,
            )
            for op, count, files, size, missing, rows, streams in stages
        ),
        int(totals[0]),
        int(totals[1]),
        int(totals[2]),
        int(totals[3]),
        int(totals[4]),
        int(totals[5]) if totals[5] is not None else (None if totals[6] else 0),
        totals[6],
        tuple(market_from_row(row) for row in markets),
        datetime.now(UTC),
        totals[7],
    )


def market_from_row(row: tuple[object, ...]) -> Market:
    return Market(
        str(row[0]),
        str(row[1]),
        str(row[2]),
        str(row[3]),
        str(row[4]),
        str(row[5]),
        bool(row[6]),
        bool(row[7]),
        int(str(row[8])) if row[8] is not None else None,
    )


async def market_page(paths: ProjectPaths, offset: int) -> tuple[tuple[Market, ...], bool]:
    async with database_connection(database_dsn(paths.root)) as conn, conn.transaction():
        await conn.execute("SET TRANSACTION READ ONLY")
        await conn.execute("SET LOCAL statement_timeout = '5s'")
        items = await (await conn.execute(MARKETS, (26, offset))).fetchall()
    return tuple(market_from_row(row) for row in items[:25]), len(items) > 25


async def raw_inventory(paths: ProjectPaths) -> tuple[int, int | None]:
    async with database_connection(database_dsn(paths.root)) as conn, conn.transaction():
        await conn.execute("SET TRANSACTION READ ONLY")
        await conn.execute("SET LOCAL statement_timeout = '5s'")
        row = await (
            await conn.execute(
                "SELECT count(*), sum(bytes_written) FROM historical.local_archives "
                "WHERE status='complete' AND storage_root_key=%s",
                (root_key(paths.raw_dir),),
            )
        ).fetchone()
    assert row is not None
    return row[0], int(row[1]) if row[1] is not None else 0


async def publication_page(
    paths: ProjectPaths, offset: int, operation: str | None, market: Market | None
) -> tuple[list[dict[str, object]], bool]:
    async with database_connection(database_dsn(paths.root)) as conn, conn.transaction():
        await conn.execute("SET TRANSACTION READ ONLY")
        await conn.execute("SET LOCAL statement_timeout = '5s'")
        rows = await (
            await conn.execute(
                BASE
                + """
            SELECT p.id, p.engine_dataset_id, p.operation, count(DISTINCT f.id)
            FROM files f JOIN historical.dataset_publications p ON f.publication=p.id
            WHERE (%s::text IS NULL OR p.operation=%s)
              AND (%s::text IS NULL OR EXISTS (
                  SELECT 1 FROM members m WHERE m.publication=p.id
                  AND m.namespace=%s AND m.exchange=%s AND m.instrument=%s))
            GROUP BY p.id ORDER BY p.id DESC LIMIT 26 OFFSET %s
        """,
                (
                    operation,
                    operation,
                    market.identity if market else None,
                    market.namespace if market else None,
                    market.exchange if market else None,
                    market.identity if market else None,
                    offset,
                ),
            )
        ).fetchall()
    return [
        dict(
            zip(
                ("Registry publication", "Engine publication", "Operation", "Primary files"),
                row,
                strict=True,
            )
        )
        for row in rows[:25]
    ], len(rows) > 25
