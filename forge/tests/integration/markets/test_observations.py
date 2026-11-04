"""Retained observations, current caches, and stable catalogue identities."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from psycopg import AsyncConnection
from psycopg.rows import TupleRow

from forge.markets.models import Instrument, Observation
from forge.markets.repository import browse, save_instrument, save_observation

pytestmark = [pytest.mark.integration, pytest.mark.database]


@pytest.mark.asyncio
async def test_retains_observations_and_never_regresses_latest_cache(
    database: AsyncConnection[TupleRow],
) -> None:
    now = datetime.now(UTC)
    instrument = Instrument(
        exchange="bybit", market="spot", symbol="TEST_" + uuid4().hex, base="BTC", quote="USDT"
    )
    identity = await save_instrument(database, instrument, now)
    assert await save_instrument(database, instrument, now) == identity
    cur = await database.execute(
        "INSERT INTO catalog.metric_refreshes(scope_json,status) VALUES ('{}','running') RETURNING id"
    )
    row = await cur.fetchone()
    assert row is not None
    refresh = row[0]
    current = Observation(
        instrument=instrument,
        fetched_at=now,
        window_start=now - timedelta(hours=24),
        window_end=now,
        window_basis="fetch_time",
        turnover=Decimal(123),
        turnover_unit="USDT",
        raw={},
    )
    await save_observation(database, refresh, identity, current)
    cur = await database.execute(
        "INSERT INTO catalog.metric_refreshes(scope_json,status) VALUES ('{}','running') RETURNING id"
    )
    row = await cur.fetchone()
    assert row is not None
    await save_observation(
        database,
        row[0],
        identity,
        current.model_copy(
            update={"window_end": now - timedelta(minutes=10), "turnover": Decimal(999)}
        ),
    )
    rows = await browse(database, reference=instrument.reference)
    assert rows[0]["turnover_24h"] == Decimal(123)
    assert rows[0]["turnover_denomination"] == "USDT"
    assert rows[0]["open_interest"] is None
    assert rows[0]["stale"] is False
    cur = await database.execute(
        "SELECT count(*) FROM catalog.metric_observations WHERE instrument_id=%s", (identity,)
    )
    assert await cur.fetchone() == (2,)


@pytest.mark.asyncio
async def test_same_window_older_fetch_does_not_replace_newer_cache(
    database: AsyncConnection[TupleRow],
) -> None:
    now = datetime.now(UTC)
    instrument = Instrument(
        exchange="bybit", market="spot", symbol="ORDER_" + uuid4().hex, base="BTC", quote="USDT"
    )
    identity = await save_instrument(database, instrument, now)
    o = Observation(
        instrument=instrument,
        fetched_at=now,
        window_start=now - timedelta(hours=24),
        window_end=now,
        window_basis="fetch_time",
        turnover=Decimal(123),
        turnover_unit="USDT",
        raw={},
    )
    for value in (
        o,
        o.model_copy(update={"fetched_at": now - timedelta(minutes=1), "turnover": Decimal(999)}),
    ):
        cur = await database.execute(
            "INSERT INTO catalog.metric_refreshes(scope_json,status,completed_at) VALUES('{}','complete',now()) RETURNING id"
        )
        row = await cur.fetchone()
        assert row is not None
        await save_observation(database, row[0], identity, value)
    assert (await browse(database, reference=instrument.reference))[0]["turnover_24h"] == Decimal(
        123
    )


@pytest.mark.asyncio
async def test_browsing_multiple_filters_keeps_all_selected_identities(
    database: AsyncConnection[TupleRow],
) -> None:
    symbol = "FILTER_" + uuid4().hex
    for exchange in ("bybit", "binance", "gateio"):
        for market in ("spot", "linear"):
            await save_instrument(
                database,
                Instrument(
                    exchange=exchange, market=market, symbol=symbol, base="BTC", quote="USDT"
                ),
                datetime.now(UTC),
            )
    rows = await browse(
        database, exchanges=["bybit", "binance"], markets=["spot", "linear"], symbol=symbol
    )
    assert {row["reference"] for row in rows} == {
        f"{exchange}.{market}.{symbol}"
        for exchange in ("bybit", "binance")
        for market in ("spot", "linear")
    }
