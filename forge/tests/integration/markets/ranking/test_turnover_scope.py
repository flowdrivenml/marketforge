"""Retained scope evidence and legacy compatibility across immutable snapshots."""

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from psycopg import AsyncConnection
from psycopg.rows import TupleRow

from forge.markets.models import Instrument, Observation, array, obj
from forge.markets.providers.public import PublicProvider
from forge.markets.ranking.policy import RankingPolicy
from forge.markets.ranking.service import build, show
from forge.markets.repository import browse, instruments, save_instrument, save_observation
from forge.shared.http.client import open_http_client
from forge.shared.http.rate_limit import RequestRateLimiter

pytestmark = [pytest.mark.integration, pytest.mark.database]


@pytest.mark.asyncio
async def test_reality_mapping_refresh_preserves_published_snapshot_and_raw_evidence(
    database: AsyncConnection[TupleRow],
) -> None:
    now = datetime.now(UTC)
    tag = uuid4().hex[:16].upper()
    old_instrument = Instrument(
        exchange="bitget",
        market="spot",
        symbol="SCOPE" + tag,
        base="SPY",
        quote="USDT",
        raw={"isReality": "no"},
    )
    other = Instrument(
        exchange="bitget",
        market="spot",
        symbol="OTHER" + tag,
        base="OTHER",
        quote="USDT",
        raw={"isReality": "no"},
    )
    ids = [await save_instrument(database, i, now) for i in (old_instrument, other)]
    cur = await database.execute(
        "INSERT INTO catalog.metric_refreshes(scope_json,status,completed_at) VALUES('{}','complete',now()) RETURNING id"
    )
    row = await cur.fetchone()
    assert row is not None
    refresh = row[0]
    for i, identity in zip((old_instrument, other), ids, strict=True):
        await save_observation(
            database,
            refresh,
            identity,
            Observation(
                instrument=i,
                fetched_at=now,
                window_start=now - timedelta(hours=24),
                window_end=now,
                window_basis="fetch_time",
                turnover=Decimal(100),
                turnover_unit="USDT",
                raw={"turnover24h": "100"},
                mapping_version="current-metrics-v2-native-money",
            ),
        )
    p = RankingPolicy(
        version=2,
        instruments=[old_instrument.reference, other.reference],
        allow_partial_universe=True,
    )
    legacy = await build(database, "before-scope", p, now=now)
    before = await show(database, legacy)
    reality = old_instrument.model_copy(update={"raw": {"isReality": "yes"}})
    later = now + timedelta(seconds=1)
    await save_instrument(database, reality, later)
    stored = await instruments(database, "bitget", "spot", symbol=reality.symbol)
    assert stored[0].model().raw == {"isReality": "yes"}
    rejected = await show(database, await build(database, "requires-refresh", p, now=later))
    rows = [obj(r) for r in array(rejected["entries"])]
    assert "legacy Bitget" in str(rows[-1]["exclusion_reason"])
    assert await show(database, legacy) == before
    cur = await database.execute(
        "INSERT INTO catalog.metric_refreshes(scope_json,status,completed_at) VALUES('{}','complete',now()) RETURNING id"
    )
    row = await cur.fetchone()
    assert row is not None
    ticker: dict[str, object] = {
        "lastPrice": "765.76",
        "volume24h": "44668118.5557",
        "turnover24h": "34271765226.4751",
        "platformTurnover24h": "55046.2049",
    }
    async with open_http_client(limiter=RequestRateLimiter(1)) as client:
        observation = PublicProvider("bitget", client).observation(
            stored[0].model(), ticker, fetched=later
        )
    await save_observation(database, row[0], ids[0], observation)
    corrected = await show(database, await build(database, "corrected-scope", p, now=later))
    entry = obj(array(corrected["entries"])[0])
    assert entry["score"] == Decimal("55046.2049")
    assert obj(entry["turnover_evidence"])["turnover_field"] == "platformTurnover24h"
    assert entry["mapping_version"] == "current-metrics-v3-activity-scope"
    assert await show(database, legacy) == before
    details = (await browse(database, reference=reality.reference))[0]
    assert details["volume_24h_base"] is None
    assert obj(details["interpretation"])["activity_scope"] == "exchange_local"
    document = json.loads(json.dumps(corrected, default=str))
    assert document["entries"][0]["native_turnover"] == "55046.2049"
    assert document["entries"][0]["turnover_evidence"]["turnover_field"] == "platformTurnover24h"
    cur = await database.execute(
        "SELECT raw_json FROM catalog.metric_observations WHERE id=%s", (entry["observation_id"],)
    )
    retained = await cur.fetchone()
    assert retained is not None and retained[0]["ticker"] == ticker
