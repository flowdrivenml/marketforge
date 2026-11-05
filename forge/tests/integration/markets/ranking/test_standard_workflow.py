"""Pinned refresh isolation, latest publication and type filters in guarded PostgreSQL."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from psycopg import AsyncConnection
from psycopg.rows import TupleRow

from forge.markets.models import Instrument, MarketDataError, Observation, array, obj
from forge.markets.ranking.policy import RankingPolicy
from forge.markets.ranking.service import build, latest_snapshot, show
from forge.markets.repository import json_value, save_instrument, save_observation

pytestmark = [pytest.mark.integration, pytest.mark.database]


@pytest.mark.asyncio
@pytest.mark.parametrize("partial_observation", [False, True])
async def test_failed_scope_does_not_reuse_retained_turnover_and_previous_snapshot_survives(
    database: AsyncConnection[TupleRow],
    partial_observation: bool,
) -> None:
    now = datetime.now(UTC)
    tag = uuid4().hex[:12].upper()
    name = "preset-" + tag
    instruments = [
        Instrument(exchange="bitget", market="spot", symbol="OLD" + tag, base="BTC", quote="USDT"),
        Instrument(
            exchange="bybit", market="linear", symbol="LINEAR" + tag, base="BTC", quote="USDT"
        ),
        Instrument(
            exchange="bybit", market="inverse", symbol="INVERSE" + tag, base="BTC", quote="USDT"
        ),
    ]
    identities = [await save_instrument(database, i, now) for i in instruments]

    async def refresh(status: str) -> int:
        sources: list[dict[str, object]] = [
            {
                "exchange": "bitget",
                "market": "spot",
                "status": "failed" if status == "partial" else "complete",
            },
            {"exchange": "bybit", "market": "linear", "status": "complete"},
            {"exchange": "bybit", "market": "inverse", "status": "complete"},
        ]
        cur = await database.execute(
            "INSERT INTO catalog.metric_refreshes(scope_json,status,completed_at,source_outcomes) "
            "VALUES('{}',%s,now(),%s) RETURNING id",
            (status, json_value(sources)),
        )
        row = await cur.fetchone()
        assert row is not None
        return row[0]

    async def observe(index: int, refresh_id: int, amount: int) -> None:
        await save_observation(
            database,
            refresh_id,
            identities[index],
            Observation(
                instrument=instruments[index],
                fetched_at=now,
                window_start=now - timedelta(hours=24),
                window_end=now,
                window_basis="fetch_time",
                turnover=Decimal(amount),
                turnover_unit="USDT",
                raw={},
            ),
        )

    old_refresh = await refresh("complete")
    for index, amount in enumerate((900, 300, 200)):
        await observe(index, old_refresh, amount)
    policy = RankingPolicy(
        instruments=[i.reference for i in instruments], allow_partial_universe=True
    )
    before = await build(database, name, policy, now=now, refresh_id=old_refresh)
    original = await show(database, before)
    filtered = await show(database, before, instrument_type="perpetual")
    assert [obj(e)["position"] for e in array(filtered["entries"])] == [2, 3]
    assert {obj(e)["market_category"] for e in array(filtered["entries"])} == {"linear", "inverse"}

    current = await refresh("partial")
    # Even an observation written before a scope failure cannot certify that scope.
    for index, amount in enumerate((1000, 400, 250)):
        if index == 0 and not partial_observation:
            continue
        await observe(index, current, amount)
    with pytest.raises(MarketDataError, match="Incomplete/comparability"):
        await build(
            database,
            name,
            policy.model_copy(update={"allow_partial_universe": False}),
            now=now + timedelta(seconds=1),
            refresh_id=current,
        )
    assert await latest_snapshot(database, name) == before
    assert await show(database, before) == original

    after = await build(database, name, policy, now=now + timedelta(seconds=1), refresh_id=current)
    result = await show(database, after)
    assert obj(result["snapshot"])["partial_universe"] is True
    rows = [obj(e) for e in array(result["entries"])]
    assert [r["score"] for r in rows[:2]] == [Decimal(400), Decimal(250)]
    assert rows[-1]["position"] is None
    assert "requested statistics refresh" in str(rows[-1]["exclusion_reason"])
    assert await latest_snapshot(database, name) == after
    assert await show(database, before) == original

    # Identical publication times resolve deterministically by UUID.
    tied = await build(database, name, policy, now=now + timedelta(seconds=1), refresh_id=current)
    assert await latest_snapshot(database, name) == max(after, tied)
    with pytest.raises(MarketDataError, match="rank update missing-"):
        await latest_snapshot(database, "missing-" + tag)
