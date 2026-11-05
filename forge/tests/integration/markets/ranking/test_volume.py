"""Grouped quantity publication against the guarded database, rolled back per test."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from psycopg import AsyncConnection
from psycopg.rows import TupleRow

from forge.markets.models import Instrument, MarketDataError, Observation, array, obj
from forge.markets.ranking.policy import RankingPolicy
from forge.markets.ranking.service import build, latest_snapshot, resolve_snapshot, show
from forge.markets.repository import json_value, save_instrument, save_observation

pytestmark = [pytest.mark.integration, pytest.mark.database]


@pytest.mark.asyncio
async def test_grouped_volume_freezes_evidence_and_prior_publication(
    database: AsyncConnection[TupleRow],
) -> None:
    now = datetime.now(UTC)
    tag = uuid4().hex[:10].upper()
    name = "volume-" + tag
    instruments = [
        Instrument(exchange="bybit", market="spot", symbol=tag + "ETHBTC", base="ETH", quote="BTC"),
        Instrument(
            exchange="bybit", market="spot", symbol=tag + "ETHUSDT", base="ETH", quote="USDT"
        ),
        Instrument(
            exchange="okx",
            market="linear",
            instrument_type="future",
            symbol=tag + "ETHFUTURE",
            base="ETH",
            quote="USDT",
            quantity_type="contracts",
            contract_value=Decimal("0.1"),
            contract_unit="ETH",
        ),
        Instrument(
            exchange="binance",
            market="inverse",
            symbol=tag + "ETHINVERSE",
            base="ETH",
            quote="USD",
            quantity_type="contracts",
            contract_value=Decimal(10),
            contract_unit="USD",
        ),
        Instrument(
            exchange="bybit", market="spot", symbol=tag + "BTCUSDT", base="BTC", quote="USDT"
        ),
        Instrument(
            exchange="bitget", market="spot", symbol=tag + "MISSING", base="ETH", quote="USDT"
        ),
    ]
    identities = [await save_instrument(database, i, now) for i in instruments]
    cur = await database.execute(
        "INSERT INTO catalog.metric_refreshes(scope_json,status,completed_at) VALUES('{}','complete',now()) RETURNING id"
    )
    record = await cur.fetchone()
    assert record is not None
    refresh_id = record[0]
    observations: list[int] = []
    for index, (instrument, identity) in enumerate(zip(instruments, identities, strict=True)):
        base = {0: Decimal(2), 1: Decimal(3), 4: Decimal(0)}.get(index)
        contracts = {2: Decimal(40), 3: Decimal(20)}.get(index)
        observations.append(
            await save_observation(
                database,
                refresh_id,
                identity,
                Observation(
                    instrument=instrument,
                    fetched_at=now,
                    window_start=now - timedelta(hours=24),
                    window_end=now,
                    window_basis="fetch_time",
                    base_volume=base,
                    contract_volume=contracts,
                    last_price=Decimal(100),
                    raw={},
                    interpretation={
                        "activity_scope": "exchange_local",
                        "base_volume_scope": "exchange_local",
                        "price_timestamp_basis": "fetch-time proxy; price timestamp not supplied",
                    },
                ),
            )
        )
    policy = RankingPolicy(
        version=2,
        metric="volume",
        volume_unit="base_asset",
        group_by="base_asset",
        instruments=[i.reference for i in instruments],
        allow_partial_universe=True,
        estimates="allow",
    )
    snapshot = await build(database, name, policy, now=now, refresh_id=refresh_id)
    original = await show(database, snapshot)
    rows = [obj(row) for row in array(original["entries"])]
    assert [(r["base_asset"], r["group_rank"]) for r in rows[:2]] == [("BTC", 1), ("ETH", 1)]
    assert [r["score"] for r in rows[:5]] == [
        Decimal(0),
        Decimal(4),
        Decimal(3),
        Decimal(2),
        Decimal(2),
    ]
    assert [r["position"] for r in rows[:5]] == [1, 2, 3, 4, 5]
    derived = next(r for r in rows if r["instrument_id"] == identities[2])
    assert obj(derived["volume_evidence"])["contract_value"] == "0.1"
    assert derived["volume_estimated"] is False
    estimated = next(r for r in rows if r["instrument_id"] == identities[3])
    assert estimated["volume_estimated"] is True
    assert "reference_price" in str(obj(estimated["volume_evidence"])["formula"])
    assert rows[-1]["position"] is None and "missing" in str(rows[-1]["exclusion_reason"])
    assert obj(original["snapshot"])["comparison_currency"] is None
    assert obj(original["snapshot"])["partial_universe"] is True
    filtered = await show(database, snapshot, base_asset="ETH", instrument_type="future")
    assert [obj(r)["group_rank"] for r in array(filtered["entries"])] == [1]
    with pytest.raises(MarketDataError, match="cannot supply one comparable ordering"):
        await resolve_snapshot(database, snapshot, [])
    # Changing current specs does not change retained group, quantity or derivation.
    await database.execute(
        "UPDATE catalog.instruments SET base_asset='CHANGED' WHERE id=%s", (identities[2],)
    )
    await database.execute(
        "UPDATE catalog.instrument_specs SET contract_value=999 WHERE instrument_id=%s",
        (identities[2],),
    )
    assert await show(database, snapshot) == original
    with pytest.raises(MarketDataError, match="Incomplete/comparability-filtered volume"):
        await build(
            database,
            name,
            policy.model_copy(update={"allow_partial_universe": False}),
            now=now,
            refresh_id=refresh_id,
        )
    assert await latest_snapshot(database, name) == snapshot


@pytest.mark.asyncio
async def test_failed_scope_and_wrong_unit_are_excluded(
    database: AsyncConnection[TupleRow],
) -> None:
    now = datetime.now(UTC)
    tag = uuid4().hex[:10].upper()
    instruments = [
        Instrument(exchange=e, market="spot", symbol=tag + e, base="ETH", quote="USDT")
        for e in ("bybit", "binance", "bitget")
    ]
    identities = [await save_instrument(database, i, now) for i in instruments]

    async def refresh(status: str) -> int:
        cur = await database.execute(
            "INSERT INTO catalog.metric_refreshes(scope_json,status,completed_at,source_outcomes) VALUES('{}',%s,now(),%s) RETURNING id",
            (
                status,
                json_value(
                    [
                        {"exchange": "bybit", "market": "spot", "status": "complete"},
                        {"exchange": "binance", "market": "spot", "status": "complete"},
                        {"exchange": "bitget", "market": "spot", "status": "failed"},
                    ]
                ),
            ),
        )
        row = await cur.fetchone()
        assert row is not None
        return row[0]

    old = await refresh("complete")
    current = await refresh("partial")
    for index, (instrument, identity) in enumerate(zip(instruments, identities, strict=True)):
        for refresh_id in (old, current) if index != 2 else (old,):
            observation = await save_observation(
                database,
                refresh_id,
                identity,
                Observation(
                    instrument=instrument,
                    fetched_at=now,
                    window_start=now - timedelta(hours=24),
                    window_end=now,
                    window_basis="fetch_time",
                    base_volume=Decimal(100),
                    raw={},
                    interpretation={
                        "activity_scope": "exchange_local",
                        "base_volume_scope": "exchange_local",
                    },
                ),
            )
            if index == 1 and refresh_id == current:
                await database.execute(
                    "UPDATE catalog.metric_observations SET base_unit='BTC' WHERE id=%s",
                    (observation,),
                )
    policy = RankingPolicy(
        metric="volume",
        volume_unit="base_asset",
        group_by="base_asset",
        instruments=[i.reference for i in instruments],
        allow_partial_universe=True,
    )
    result = await show(database, await build(database, tag, policy, now=now, refresh_id=current))
    rows = [obj(r) for r in array(result["entries"])]
    assert rows[0]["instrument_id"] == identities[0]
    assert "unit differs" in str(
        next(r for r in rows if r["instrument_id"] == identities[1])["exclusion_reason"]
    )
    assert "requested statistics refresh" in str(
        next(r for r in rows if r["instrument_id"] == identities[2])["exclusion_reason"]
    )
