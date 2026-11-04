"""Published snapshots are atomic, retained, and protected from later mutation."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import psycopg
import pytest
from psycopg import AsyncConnection
from psycopg.rows import TupleRow

from forge.markets.models import Instrument, MarketDataError, Observation
from forge.markets.ranking.policy import RankingPolicy
from forge.markets.ranking.service import build, show
from forge.markets.repository import save_instrument, save_observation

pytestmark = [pytest.mark.database, pytest.mark.integration]


@pytest.mark.asyncio
async def test_snapshot_publication_is_immutable_and_inputs_retained(
    database: AsyncConnection[TupleRow],
) -> None:
    now = datetime.now(UTC)
    symbol = "RANK_" + uuid4().hex
    instrument = Instrument(
        exchange="bybit", market="spot", symbol=symbol, base="BTC", quote="USDT"
    )
    identity = await save_instrument(database, instrument, now)
    cur = await database.execute(
        "INSERT INTO catalog.metric_refreshes(scope_json,status,completed_at) VALUES('{}','complete',now()) RETURNING id"
    )
    row = await cur.fetchone()
    assert row is not None
    observation = await save_observation(
        database,
        row[0],
        identity,
        Observation(
            instrument=instrument,
            fetched_at=now,
            window_start=now - timedelta(hours=24),
            window_end=now,
            window_basis="fetch_time",
            turnover=Decimal(10),
            turnover_unit="USDT",
            raw={},
        ),
    )
    snapshot = await build(
        database, "test", RankingPolicy(instruments=[instrument.reference]), now=now
    )
    original = await show(database, snapshot)
    with pytest.raises(psycopg.errors.RaiseException):
        async with database.transaction():
            await database.execute(
                "UPDATE ranking.snapshots SET policy_name='changed' WHERE id=%s", (snapshot,)
            )
    assert (await show(database, snapshot)) == original
    assert original["snapshot"] is not None

    with pytest.raises(psycopg.errors.RaiseException):
        async with database.transaction():
            await database.execute(
                "UPDATE catalog.metric_observations SET turnover=99 WHERE id=%s", (observation,)
            )
    assert await show(database, snapshot) == original


@pytest.mark.asyncio
async def test_rejected_ranking_never_leaves_a_snapshot(
    database: AsyncConnection[TupleRow],
) -> None:
    name = "reject_" + uuid4().hex
    with pytest.raises(MarketDataError, match="missing from the catalogue"):
        await build(
            database, name, RankingPolicy(instruments=["bybit.spot.MISSING_" + uuid4().hex])
        )
    cur = await database.execute(
        "SELECT count(*) FROM ranking.snapshots WHERE policy_name=%s", (name,)
    )
    assert await cur.fetchone() == (0,)
