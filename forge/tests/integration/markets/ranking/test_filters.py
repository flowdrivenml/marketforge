"""Ranking filters preserve frozen metadata, full result sets and stored ranks."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import cast
from uuid import UUID, uuid4

import pytest
from psycopg import AsyncConnection
from psycopg.rows import TupleRow

from forge.markets.models import Instrument, Market, Observation, array, obj
from forge.markets.ranking.policy import RankingPolicy
from forge.markets.ranking.service import build, latest_snapshot, show
from forge.markets.repository import json_value, save_instrument, save_observation

pytestmark = [pytest.mark.integration, pytest.mark.database]


@pytest.mark.asyncio
@pytest.mark.parametrize("metric", ["turnover_24h", "volume"])
async def test_frozen_metadata_filters_preserve_original_ranks(
    database: AsyncConnection[TupleRow],
    metric: str,
) -> None:
    now = datetime.now(UTC)
    tag = "RF" + uuid4().hex[:10].upper()
    instruments = [
        Instrument(
            exchange="bybit",
            market=cast(Market, market),
            symbol=tag + symbol,
            base=base,
            quote=quote,
        )
        for symbol, market, base, quote in (
            ("ETH-USDT-SWAP", "linear", "ETH", "USDT"),
            ("ETHUSDT", "spot", "ETH", "USDT"),
            ("ETH_BTC", "spot", "ETH", "BTC"),
            ("BTCUSDT", "spot", "BTC", "USDT"),
            ("ETH%USDT", "spot", "ETH", "USDT"),
            ("ETHXUSDT", "spot", "ETH", "USDT"),
        )
    ]
    cur = await database.execute(
        "INSERT INTO catalog.metric_refreshes(scope_json,status,completed_at) VALUES('{}','complete',now()) RETURNING id"
    )
    refresh = await cur.fetchone()
    assert refresh is not None
    identities: list[int] = []
    for index, instrument in enumerate(instruments):
        identity = await save_instrument(database, instrument, now)
        identities.append(identity)
        await save_observation(
            database,
            refresh[0],
            identity,
            Observation(
                instrument=instrument,
                fetched_at=now,
                window_start=now - timedelta(hours=24),
                window_end=now,
                window_basis="fetch_time",
                base_volume=Decimal(10 - index),
                turnover=Decimal(100 - index),
                turnover_unit=instrument.quote,
                raw={},
                interpretation={
                    "activity_scope": "exchange_local",
                    "base_volume_scope": "exchange_local",
                },
            ),
        )
    references = [i.reference for i in instruments]
    policy = (
        RankingPolicy(
            metric="volume", volume_unit="base_asset", group_by="base_asset", instruments=references
        )
        if metric == "volume"
        else RankingPolicy.model_validate(
            {
                "instruments": references,
                "conversion_rates": {"BTC": "1"},
                "conversion_as_of": now,
                "conversion_source": "test",
            }
        )
    )
    snapshot = await build(database, tag, policy, now=now)
    assert await latest_snapshot(database, tag) == snapshot
    original = await show(database, snapshot)
    original_rows = {obj(r)["symbol"]: obj(r) for r in array(original["entries"])}
    result = await show(
        database, snapshot, base_asset="eTh", quote_asset="uSdT", match=tag.lower() + "eth*"
    )
    rows = [obj(r) for r in array(result["entries"])]
    assert len(rows) == 4
    for row in rows:
        assert row["position"] == original_rows[row["symbol"]]["position"]
        assert row["group_rank"] == original_rows[row["symbol"]]["group_rank"]
        assert obj(row["metadata_sources"])["symbol"] == "publication"
    btc_quote = await show(database, snapshot, quote_asset="bTc")
    assert [obj(row)["symbol"] for row in array(btc_quote["entries"])] == [tag + "ETH_BTC"]
    quote = await show(database, snapshot, instrument_type="perpetual", quote_asset="usdt")
    assert [obj(r)["symbol"] for r in array(quote["entries"])] == [tag + "ETH-USDT-SWAP"]
    suffix = await show(database, snapshot, base_asset="eth", quote_asset="usdt", match="*UsDt")
    assert {obj(r)["symbol"] for r in array(suffix["entries"])} == {
        tag + "ETHUSDT",
        tag + "ETH%USDT",
        tag + "ETHXUSDT",
    }
    for literal in ("ETH_BTC", "ETH%USDT"):
        assert [
            obj(r)["symbol"]
            for r in array((await show(database, snapshot, match=tag + literal))["entries"])
        ] == [tag + literal]
    question = await show(database, snapshot, match=tag + "ETH?USDT")
    assert {obj(r)["symbol"] for r in array(question["entries"])} == {
        tag + "ETH%USDT",
        tag + "ETHXUSDT",
    }
    # All symbol/asset filters and displayed metadata stay pinned after catalog edits.
    await database.execute(
        "UPDATE catalog.instruments SET symbol=%s,base_asset='RENAMED',quote_asset='USDC' WHERE id=%s",
        (tag + "RENAMED", identities[0]),
    )
    assert await show(database, snapshot) == original
    assert (
        len(array((await show(database, snapshot, quote_asset="usdt", match="*sWaP"))["entries"]))
        == 1
    )
    assert array((await show(database, snapshot, quote_asset="usdc"))["entries"]) == []


@pytest.mark.asyncio
async def test_older_snapshot_metadata_uses_retained_specs_then_explicit_current_fallback(
    database: AsyncConnection[TupleRow],
) -> None:
    now = datetime.now(UTC)
    tag = "LEGACY" + uuid4().hex[:10].upper()
    instrument = Instrument(
        exchange="bybit", market="linear", symbol=tag + "ETH-USDT-SWAP", base="ETH", quote="USDT"
    )
    identity = await save_instrument(database, instrument, now)
    cur = await database.execute(
        "INSERT INTO catalog.metric_refreshes(scope_json,status,completed_at) VALUES('{}','complete',now()) RETURNING id"
    )
    refresh = await cur.fetchone()
    assert refresh is not None

    async def legacy(*, retained: bool, partial: bool = False) -> UUID:
        cursor = await database.execute(
            "INSERT INTO catalog.metric_refreshes(scope_json,status,completed_at) VALUES('{}','complete',now()) RETURNING id"
        )
        legacy_refresh = await cursor.fetchone()
        assert legacy_refresh is not None
        observation = await save_observation(
            database,
            legacy_refresh[0],
            identity,
            Observation(
                instrument=instrument,
                fetched_at=now,
                window_start=now - timedelta(hours=24),
                window_end=now,
                window_basis="fetch_time",
                turnover=Decimal(100),
                turnover_unit="USDT",
                raw={},
            ),
        )
        if not retained:
            await database.execute(
                "UPDATE catalog.metric_observations SET raw_json='{}' WHERE id=%s", (observation,)
            )
        if partial:
            await database.execute(
                "UPDATE catalog.metric_observations SET raw_json=raw_json #- '{instrument_specification,quote}' WHERE id=%s",
                (observation,),
            )
        snapshot = uuid4()
        await database.execute(
            """INSERT INTO ranking.snapshots
            (id,policy_name,policy_json,policy_sha256,algorithm_version,comparison_currency,comparison_basis,
             created_at,status,partial_universe,universe_count,included_count)
            VALUES(%s,%s,%s,%s,'legacy','USDT','{}',%s,'draft',false,1,1)""",
            (snapshot, tag, json_value({"metric": "turnover_24h"}), "a" * 64, now),
        )
        await database.execute(
            """INSERT INTO ranking.entries
            (snapshot_id,instrument_id,instrument_reference,observation_id,position,score)
            VALUES(%s,%s,%s,%s,1,100)""",
            (snapshot, identity, instrument.reference, observation),
        )
        await database.execute(
            "UPDATE ranking.snapshots SET status='published' WHERE id=%s", (snapshot,)
        )
        return snapshot

    retained = await legacy(retained=True)
    fallback = await legacy(retained=False)
    partial = await legacy(retained=True, partial=True)
    await database.execute(
        "UPDATE catalog.instruments SET symbol=%s,base_asset='OTHER',quote_asset='USDC' WHERE id=%s",
        (tag + "CHANGED", identity),
    )
    pinned = await show(database, retained, base_asset="eth", quote_asset="usdt", match="*sWaP")
    row = obj(array(pinned["entries"])[0])
    assert row["symbol"] == instrument.symbol
    assert set(obj(row["metadata_sources"]).values()) == {"retained_observation"}
    assert array((await show(database, fallback, quote_asset="usdt"))["entries"]) == []
    live = await show(database, fallback, base_asset="other", quote_asset="usdc", match=tag + "CH*")
    row = obj(array(live["entries"])[0])
    assert row["symbol"] == tag + "CHANGED"
    assert set(obj(row["metadata_sources"]).values()) == {"current_catalogue"}
    assert "current_catalogue" in str(live["metadata_semantics"])

    partial_result = await show(
        database, partial, base_asset="eth", quote_asset="usdc", match="*swap"
    )
    partial_row = obj(array(partial_result["entries"])[0])
    assert obj(partial_row["metadata_sources"]) == {
        "symbol": "retained_observation",
        "base_asset": "retained_observation",
        "quote_asset": "current_catalogue",
    }
