"""Short catalogue transactions and append-only metric provenance."""

import json
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import LiteralString

from psycopg import AsyncConnection
from psycopg.rows import TupleRow, class_row, dict_row
from psycopg.types.json import Jsonb

from forge.markets.filters import symbol_pattern
from forge.markets.models import Exchange, Instrument, Market, MarketDataError, Observation


def json_value(value: object) -> Jsonb:
    return Jsonb(json.loads(json.dumps(value, default=str)))


async def save_instrument(
    c: AsyncConnection[TupleRow],
    instrument: Instrument,
    fetched: datetime,
    *,
    sync_id: int | None = None,
) -> int:
    i = instrument
    async with c.transaction():
        cur = await c.execute(
            """INSERT INTO catalog.instruments AS existing
            (exchange_id,symbol,instrument_type,market_category,base_asset,quote_asset,settlement_asset,status,updated_at,launch_time_ns,expiry_ns)
            SELECT id,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s FROM catalog.exchanges WHERE code=%s
            ON CONFLICT(exchange_id,instrument_type,market_category,symbol) DO UPDATE SET
              base_asset=EXCLUDED.base_asset,quote_asset=EXCLUDED.quote_asset,settlement_asset=EXCLUDED.settlement_asset,
              status=EXCLUDED.status,updated_at=EXCLUDED.updated_at,launch_time_ns=COALESCE(EXCLUDED.launch_time_ns,existing.launch_time_ns),expiry_ns=CASE WHEN EXCLUDED.instrument_type='future' THEN COALESCE(EXCLUDED.expiry_ns,existing.expiry_ns) ELSE NULL END
            WHERE existing.updated_at<=EXCLUDED.updated_at RETURNING id""",
            (
                i.symbol,
                i.instrument_type,
                i.market,
                i.base,
                i.quote,
                i.settlement,
                i.source_status or ("active" if i.active else "inactive"),
                fetched,
                i.launch_time_ns,
                i.expiry_ns,
                i.exchange,
            ),
        )
        row = await cur.fetchone()
        if row is None:
            cur = await c.execute(
                """SELECT i.id FROM catalog.instruments i JOIN catalog.exchanges e ON e.id=i.exchange_id
                WHERE e.code=%s AND i.symbol=%s AND i.market_category=%s AND i.instrument_type=%s""",
                (i.exchange, i.symbol, i.market, i.instrument_type),
            )
            row = await cur.fetchone()
        if row is None:
            raise MarketDataError("Exchange is not registered.")
        instrument_id: int = row[0]
        await c.execute(
            """INSERT INTO catalog.instrument_specs AS old
            (instrument_id,quantity_type,contract_value,contract_value_asset,tick_size,qty_step,fetched_at,min_qty,max_qty,min_notional)
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(instrument_id) DO UPDATE SET
            quantity_type=EXCLUDED.quantity_type,contract_value=EXCLUDED.contract_value,contract_value_asset=EXCLUDED.contract_value_asset,
            tick_size=EXCLUDED.tick_size,qty_step=EXCLUDED.qty_step,min_qty=EXCLUDED.min_qty,max_qty=EXCLUDED.max_qty,min_notional=EXCLUDED.min_notional,fetched_at=EXCLUDED.fetched_at WHERE old.fetched_at<=EXCLUDED.fetched_at""",
            (
                instrument_id,
                i.quantity_type,
                i.contract_value,
                i.contract_unit,
                i.tick_size,
                i.qty_step,
                fetched,
                i.min_qty,
                i.max_qty,
                i.min_notional,
            ),
        )
        await c.execute(
            "INSERT INTO catalog.instrument_api_raw(instrument_id,fetched_at,raw_json) VALUES(%s,%s,%s)",
            (instrument_id, fetched, json_value(i.raw)),
        )
        if sync_id is not None:
            await c.execute(
                "INSERT INTO catalog.instrument_sync_members(sync_id,instrument_id) VALUES(%s,%s) ON CONFLICT DO NOTHING",
                (sync_id, instrument_id),
            )
        return instrument_id


@dataclass(frozen=True)
class StoredInstrument:
    id: int
    exchange: Exchange
    market: Market
    symbol: str
    base: str
    quote: str
    settlement: str | None
    quantity_type: str
    contract_value: Decimal | None
    contract_unit: str | None
    tick_size: Decimal | None
    qty_step: Decimal | None
    status: str | None
    instrument_type: str = "perpetual"
    qualified_reference: bool = False
    launch_time_ns: int | None = None
    expiry_ns: int | None = None
    min_qty: Decimal | None = None
    max_qty: Decimal | None = None
    min_notional: Decimal | None = None
    raw: dict[str, object] = field(default_factory=dict[str, object])

    @property
    def reference(self) -> str:
        kind = f"{self.instrument_type}." if self.qualified_reference else ""
        return f"{self.exchange}.{self.market}.{kind}{self.symbol}"

    def model(self) -> Instrument:
        return Instrument.model_validate(
            {
                **{k: v for k, v in self.__dict__.items() if k not in {"id", "status"}},
                "source_status": self.status,
                "active": self.status
                in {"active", "TRADING", "Trading", "live", "tradable", "online"},
            }
        )


async def instruments(
    c: AsyncConnection[TupleRow],
    exchange: str | None = None,
    market: str | None = None,
    *,
    limit: int = 10000,
    symbol: str | None = None,
    after_id: int = 0,
    sync_id: int | None = None,
    exchanges: list[str] | None = None,
    markets: list[str] | None = None,
    references: list[str] | None = None,
    active_only: bool = False,
) -> list[StoredInstrument]:
    if not 1 <= limit <= 10000:
        raise MarketDataError("Instrument limit must be 1..10000.")
    async with c.cursor(row_factory=class_row(StoredInstrument)) as cur:
        await cur.execute(
            """SELECT i.id,e.code AS exchange,i.market_category AS market,i.symbol,i.base_asset AS base,i.quote_asset AS quote,
            i.settlement_asset AS settlement,s.quantity_type,s.contract_value,s.contract_value_asset AS contract_unit,s.tick_size,s.qty_step,i.status,i.instrument_type,i.launch_time_ns,i.expiry_ns,s.min_qty,s.max_qty,s.min_notional,
            COALESCE(metadata.raw_json,'{}'::jsonb) AS raw,
            EXISTS(SELECT 1 FROM catalog.instruments other WHERE other.exchange_id=i.exchange_id AND other.market_category=i.market_category AND other.symbol=i.symbol AND other.id<>i.id) AS qualified_reference
            FROM catalog.instruments i JOIN catalog.exchanges e ON e.id=i.exchange_id JOIN catalog.instrument_specs s ON s.instrument_id=i.id
            LEFT JOIN LATERAL (SELECT raw_json FROM catalog.instrument_api_raw
                WHERE instrument_id=i.id AND e.code='bitget' ORDER BY fetched_at DESC,id DESC LIMIT 1) metadata ON true
            WHERE (%s::text IS NULL OR e.code=%s) AND (%s::text IS NULL OR i.market_category=%s)
              AND (%s::text IS NULL OR i.symbol=%s) AND i.instrument_type IN ('spot','perpetual','future')
              AND i.id>%s AND (%s::text[] IS NULL OR e.code=ANY(%s::text[])) AND (%s::text[] IS NULL OR i.market_category=ANY(%s::text[]))
              AND (%s::text[] IS NULL OR e.code||'.'||i.market_category||'.'||i.symbol=ANY(%s::text[]) OR e.code||'.'||i.market_category||'.'||i.instrument_type||'.'||i.symbol=ANY(%s::text[]))
              AND (NOT %s OR (i.status IN ('active','TRADING','Trading','live','tradable','online') AND (i.expiry_ns IS NULL OR i.expiry_ns>extract(epoch FROM now())*1000000000)))
              AND (%s::bigint IS NULL OR EXISTS(SELECT 1 FROM catalog.instrument_sync_members member WHERE member.instrument_id=i.id AND member.sync_id=%s))
            ORDER BY i.id LIMIT %s""",
            (
                exchange,
                exchange,
                market,
                market,
                symbol,
                symbol,
                after_id,
                exchanges,
                exchanges,
                markets,
                markets,
                references,
                references,
                references,
                active_only,
                sync_id,
                sync_id,
                limit,
            ),
        )
        result = list(await cur.fetchall())
        if references and any(
            i.qualified_reference and f"{i.exchange}.{i.market}.{i.symbol}" in references
            for i in result
        ):
            raise MarketDataError(
                "Ambiguous instrument reference; use exchange.market.instrument_type.symbol."
            )
        return result


async def iter_instruments(
    c: AsyncConnection[TupleRow],
    *,
    exchange: str | None = None,
    market: str | None = None,
    symbol: str | None = None,
    exchanges: list[str] | None = None,
    markets: list[str] | None = None,
    references: list[str] | None = None,
    active_only: bool = True,
    sync_id: int | None = None,
) -> AsyncIterator[StoredInstrument]:
    after = 0
    while page := await instruments(
        c,
        exchange,
        market,
        symbol=symbol,
        exchanges=exchanges,
        markets=markets,
        references=references,
        active_only=active_only,
        sync_id=sync_id,
        after_id=after,
        limit=1000,
    ):
        for item in page:
            yield item
        after = page[-1].id


async def save_observation(
    c: AsyncConnection[TupleRow],
    refresh_id: int,
    instrument_id: int,
    o: Observation,
    *,
    update_latest: bool = True,
) -> int:
    async with c.transaction():
        cur = await c.execute(
            """INSERT INTO catalog.metric_observations
            (refresh_id,instrument_id,fetched_at,source_observed_at,window_start,window_end,window_basis,
             base_unit,quote_unit,last_price,base_volume,quote_volume,contract_volume,turnover,turnover_unit,
             turnover_estimated,open_interest,open_interest_unit,funding_rate,mapping_version,raw_json)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
            (
                refresh_id,
                instrument_id,
                o.fetched_at,
                o.source_observed_at,
                o.window_start,
                o.window_end,
                o.window_basis,
                o.instrument.base,
                o.instrument.quote,
                o.last_price,
                o.base_volume,
                o.quote_volume,
                o.contract_volume,
                o.turnover,
                o.turnover_unit,
                o.turnover_estimated,
                o.open_interest,
                o.open_interest_unit,
                o.funding_rate,
                o.mapping_version,
                json_value(
                    {
                        "ticker": o.raw,
                        "instrument_specification": o.instrument.model_dump(mode="json"),
                        "interpretation": o.interpretation,
                    }
                ),
            ),
        )
        row = await cur.fetchone()
        if row is None:
            raise MarketDataError("Observation was not persisted.")
        observation_id: int = row[0]
        if not update_latest:
            return observation_id
        await c.execute(
            """INSERT INTO catalog.instrument_metrics AS old
            (instrument_id,last_price,volume_24h_base,volume_24h_quote,volume_24h_contracts,turnover_24h,turnover_denomination,
             open_interest,funding_rate,measured_at,observation_id,turnover_estimated)
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(instrument_id) DO UPDATE SET
             last_price=EXCLUDED.last_price,volume_24h_base=EXCLUDED.volume_24h_base,volume_24h_quote=EXCLUDED.volume_24h_quote,
             volume_24h_contracts=EXCLUDED.volume_24h_contracts,turnover_24h=EXCLUDED.turnover_24h,turnover_denomination=EXCLUDED.turnover_denomination,
             open_interest=EXCLUDED.open_interest,funding_rate=EXCLUDED.funding_rate,measured_at=EXCLUDED.measured_at,
             observation_id=EXCLUDED.observation_id,turnover_estimated=EXCLUDED.turnover_estimated,updated_at=statement_timestamp()
            WHERE (old.measured_at,COALESCE((SELECT fetched_at FROM catalog.metric_observations WHERE id=old.observation_id),'-infinity'::timestamptz)) <= (EXCLUDED.measured_at,%s)""",
            (
                instrument_id,
                o.last_price,
                o.base_volume,
                o.quote_volume,
                o.contract_volume,
                o.turnover,
                o.turnover_unit,
                o.open_interest,
                o.funding_rate,
                o.window_end,
                observation_id,
                o.turnover_estimated,
                o.fetched_at,
            ),
        )
        return observation_id


async def browse(
    c: AsyncConnection[TupleRow],
    *,
    exchange: str | None = None,
    market: str | None = None,
    exchanges: list[str] | None = None,
    markets: list[str] | None = None,
    symbol: str | None = None,
    base_asset: str | None = None,
    quote_asset: str | None = None,
    match: str | None = None,
    limit: int = 50,
    sort: str = "identity",
    reference: str | None = None,
) -> list[dict[str, object]]:
    if not 1 <= limit <= 10001:
        raise MarketDataError("Result limit must be 1..10001.")
    orders: dict[str, LiteralString] = {
        "identity": "e.code,i.market_category,i.symbol",
        "turnover": "m.turnover_denomination,m.turnover_24h DESC NULLS LAST,e.code,i.market_category,i.symbol",
        "freshness": "m.measured_at DESC NULLS LAST,e.code,i.market_category,i.symbol",
    }
    order = orders.get(sort)
    if order is None:
        raise MarketDataError("Unknown market sort.")
    async with c.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """SELECT i.id,e.code AS exchange,i.instrument_type,i.market_category,i.symbol,
            CASE WHEN EXISTS(SELECT 1 FROM catalog.instruments other WHERE other.exchange_id=i.exchange_id AND other.market_category=i.market_category AND other.symbol=i.symbol AND other.id<>i.id)
            THEN e.code||'.'||i.market_category||'.'||i.instrument_type||'.'||i.symbol ELSE e.code||'.'||i.market_category||'.'||i.symbol END AS reference,i.status,
            i.launch_time_ns,i.expiry_ns,i.settlement_asset,s.tick_size,s.qty_step,s.min_qty,s.max_qty,s.min_notional,
            i.base_asset,i.quote_asset,s.quantity_type,s.contract_value,s.contract_value_asset,m.last_price,
            m.turnover_24h,m.turnover_denomination,m.turnover_estimated,m.volume_24h_base,m.volume_24h_quote,m.volume_24h_contracts,
            m.observation_id,m.open_interest,m.funding_rate,o.open_interest_unit,m.measured_at,o.source_observed_at,o.fetched_at,o.window_start,o.window_end,o.window_basis,o.mapping_version,
            o.raw_json->'interpretation' AS interpretation,
            (o.fetched_at IS NULL OR o.fetched_at < NOW()-INTERVAL '1 hour') AS stale
            FROM catalog.instruments i JOIN catalog.exchanges e ON e.id=i.exchange_id
            LEFT JOIN catalog.instrument_specs s ON s.instrument_id=i.id LEFT JOIN catalog.instrument_metrics m ON m.instrument_id=i.id
            LEFT JOIN catalog.metric_observations o ON o.id=m.observation_id
            WHERE i.instrument_type IN ('spot','perpetual','future') AND (%s::text IS NULL OR e.code=%s) AND (%s::text IS NULL OR i.market_category=%s)
              AND (%s::text[] IS NULL OR e.code=ANY(%s::text[]))
              AND (%s::text[] IS NULL OR i.market_category=ANY(%s::text[]))
              AND (%s::text IS NULL OR i.symbol=%s) AND (%s::text IS NULL OR e.code||'.'||i.market_category||'.'||i.symbol=%s OR e.code||'.'||i.market_category||'.'||i.instrument_type||'.'||i.symbol=%s)
              AND (%s::text IS NULL OR lower(i.base_asset)=lower(%s))
              AND (%s::text IS NULL OR lower(i.quote_asset)=lower(%s))
              AND (%s::text IS NULL OR i.symbol ILIKE %s ESCAPE '!')
            ORDER BY """
            + order
            + " LIMIT %s",
            (
                exchange,
                exchange,
                market,
                market,
                exchanges,
                exchanges,
                markets,
                markets,
                symbol,
                symbol,
                reference,
                reference,
                reference,
                base_asset,
                base_asset,
                quote_asset,
                quote_asset,
                match,
                symbol_pattern(match),
                limit,
            ),
        )
        result = list(await cur.fetchall())
        if reference and len(result) > 1:
            raise MarketDataError(
                "Ambiguous instrument reference; use exchange.market.instrument_type.symbol."
            )
        return result


async def publish_observations(
    c: AsyncConnection[TupleRow], refresh: int, exchange: str, market: str
) -> None:
    """Publish a scope's retained values only after all its endpoints succeed."""
    await c.execute(
        """INSERT INTO catalog.instrument_metrics AS old
        (instrument_id,last_price,volume_24h_base,volume_24h_quote,volume_24h_contracts,turnover_24h,turnover_denomination,open_interest,funding_rate,measured_at,observation_id,turnover_estimated)
        SELECT o.instrument_id,o.last_price,o.base_volume,o.quote_volume,o.contract_volume,o.turnover,o.turnover_unit,o.open_interest,o.funding_rate,o.window_end,o.id,o.turnover_estimated
        FROM catalog.metric_observations o JOIN catalog.instruments i ON i.id=o.instrument_id JOIN catalog.exchanges e ON e.id=i.exchange_id
        WHERE o.refresh_id=%s AND e.code=%s AND i.market_category=%s
        ON CONFLICT(instrument_id) DO UPDATE SET last_price=EXCLUDED.last_price,volume_24h_base=EXCLUDED.volume_24h_base,
        volume_24h_quote=EXCLUDED.volume_24h_quote,volume_24h_contracts=EXCLUDED.volume_24h_contracts,turnover_24h=EXCLUDED.turnover_24h,
        turnover_denomination=EXCLUDED.turnover_denomination,open_interest=EXCLUDED.open_interest,funding_rate=EXCLUDED.funding_rate,
        measured_at=EXCLUDED.measured_at,observation_id=EXCLUDED.observation_id,turnover_estimated=EXCLUDED.turnover_estimated,updated_at=statement_timestamp()
        WHERE (old.measured_at,COALESCE((SELECT fetched_at FROM catalog.metric_observations WHERE id=old.observation_id),'-infinity'::timestamptz)) <=
        (EXCLUDED.measured_at,(SELECT fetched_at FROM catalog.metric_observations WHERE id=EXCLUDED.observation_id))""",
        (refresh, exchange, market),
    )
