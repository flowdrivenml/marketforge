"""Contract identity, specification persistence and paginated ranking universes."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from psycopg import AsyncConnection
from psycopg.rows import TupleRow

from forge.markets.models import Instrument, MarketDataError, Observation
from forge.markets.ranking.policy import RankingPolicy
from forge.markets.ranking.service import build, show
from forge.markets.repository import browse, iter_instruments, save_instrument, save_observation

pytestmark = [pytest.mark.integration, pytest.mark.database]


@pytest.mark.asyncio
async def test_futures_specs_and_ambiguous_short_references(
    database: AsyncConnection[TupleRow],
) -> None:
    now = datetime.now(UTC)
    symbol = "COLLISION_" + uuid4().hex
    future = Instrument(
        exchange="bybit",
        market="linear",
        instrument_type="future",
        symbol=symbol,
        base="BTC",
        quote="USDT",
        expiry_ns=1798185600000000000,
        launch_time_ns=1700000000000000000,
        min_qty=Decimal(".01"),
        max_qty=Decimal(10),
    )
    for item in (
        future,
        future.model_copy(update={"instrument_type": "perpetual", "expiry_ns": None}),
    ):
        await save_instrument(database, item, now)
    with pytest.raises(MarketDataError, match="Ambiguous"):
        await browse(database, reference=f"bybit.linear.{symbol}")
    rows = await browse(database, reference=f"bybit.linear.future.{symbol}")
    assert len(rows) == 1 and rows[0]["instrument_type"] == "future"
    assert rows[0]["expiry_ns"] == future.expiry_ns and rows[0]["min_qty"] == Decimal(".01")
    assert rows[0]["contract_value"] is None
    with pytest.raises(MarketDataError, match="Ambiguous"):
        _ = [i async for i in iter_instruments(database, references=[f"bybit.linear.{symbol}"])]


@pytest.mark.asyncio
async def test_ranking_filters_before_paging_over_10000_records(
    database: AsyncConnection[TupleRow],
) -> None:
    prefix = "UNIVERSE_" + uuid4().hex
    cur = await database.execute("SELECT id FROM catalog.exchanges WHERE code='bybit'")
    row = await cur.fetchone()
    assert row is not None
    await database.execute(
        """INSERT INTO catalog.instruments(exchange_id,symbol,instrument_type,market_category,base_asset,quote_asset,status)
        SELECT %s,%s||n::text,'perpetual','linear','BTC','USDT','active' FROM generate_series(1,10005) n""",
        (row[0], prefix),
    )
    await database.execute(
        """INSERT INTO catalog.instrument_specs(instrument_id,quantity_type,fetched_at)
        SELECT id,'base',now() FROM catalog.instruments WHERE symbol LIKE %s""",
        (prefix + "%",),
    )
    refs = [f"bybit.linear.{prefix}{i}" for i in range(1, 10006)]
    identities = [i async for i in iter_instruments(database, references=refs)]
    assert len(identities) == 10005 and len({i.id for i in identities}) == 10005
    target = Instrument(
        exchange="bybit",
        market="linear",
        instrument_type="future",
        symbol=prefix + "LATE",
        base="BTC",
        quote="USDT",
    )
    now = datetime.now(UTC)
    identity = await save_instrument(database, target, now)
    cur = await database.execute(
        "INSERT INTO catalog.metric_refreshes(scope_json,status,completed_at) VALUES('{}','complete',now()) RETURNING id"
    )
    row = await cur.fetchone()
    assert row is not None
    await save_observation(
        database,
        row[0],
        identity,
        Observation(
            instrument=target,
            fetched_at=now,
            window_start=now - timedelta(hours=24),
            window_end=now,
            window_basis="fetch_time",
            turnover=Decimal(123),
            turnover_unit="USDT",
            raw={},
        ),
    )
    snapshot = await build(database, "late", RankingPolicy(instruments=[target.reference]), now=now)
    detail = await show(database, snapshot)
    from forge.markets.models import array, obj

    entry = obj(array(detail["entries"])[0])
    assert (
        entry["instrument_reference"] == target.reference and entry["instrument_type"] == "future"
    )
    full = await build(
        database,
        "full-large-universe",
        RankingPolicy(instruments=[*refs, target.reference], allow_partial_universe=True),
        now=now,
    )
    full_detail = await show(database, full)
    assert obj(full_detail["snapshot"])["universe_count"] == 10006
    assert len(array(full_detail["entries"])) == 10006
    assert obj(array(full_detail["entries"])[0])["instrument_reference"] == target.reference


@pytest.mark.asyncio
async def test_successful_scope_of_partial_global_refresh_can_rank(
    database: AsyncConnection[TupleRow],
) -> None:
    from forge.markets.models import obj
    from forge.markets.repository import json_value

    now = datetime.now(UTC)
    item = Instrument(
        exchange="binance",
        market="linear",
        instrument_type="future",
        symbol="PARTIAL_" + uuid4().hex,
        base="BTC",
        quote="USDT",
    )
    identity = await save_instrument(database, item, now)
    cur = await database.execute(
        "INSERT INTO catalog.metric_refreshes(scope_json,status,completed_at,source_outcomes) VALUES('{}','partial',now(),%s) RETURNING id",
        (
            json_value(
                [
                    {"exchange": "binance", "market": "linear", "status": "complete"},
                    {"exchange": "okx", "market": "linear", "status": "failed"},
                ]
            ),
        ),
    )
    row = await cur.fetchone()
    assert row is not None
    await save_observation(
        database,
        row[0],
        identity,
        Observation(
            instrument=item,
            fetched_at=now,
            window_start=now - timedelta(hours=24),
            window_end=now,
            window_basis="fetch_time",
            turnover=Decimal("123.456789"),
            turnover_unit="USDT",
            raw={},
        ),
    )
    snapshot = await build(
        database, "successful-scope", RankingPolicy(instruments=[item.reference]), now=now
    )
    result = await show(database, snapshot)
    assert obj(result["snapshot"])["partial_universe"] is False
