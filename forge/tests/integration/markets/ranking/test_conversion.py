"""Real retained inputs, mixed instrument types, immutable FX provenance; no network."""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, localcontext
from fractions import Fraction
from typing import LiteralString
from uuid import UUID, uuid4

import psycopg
import pytest
import pytest_asyncio
from psycopg import AsyncConnection
from psycopg.rows import TupleRow

import forge.markets.ranking.service as ranking
from forge.markets.models import Instrument, MarketDataError, Observation, array, obj
from forge.markets.providers.public import PublicProvider
from forge.markets.ranking.conversion import Conversion
from forge.markets.ranking.policy import RankingPolicy
from forge.markets.repository import save_instrument, save_observation
from forge.shared.http.client import open_http_client
from forge.shared.http.rate_limit import RequestRateLimiter

pytestmark = [pytest.mark.integration, pytest.mark.database]


@dataclass(frozen=True)
class FxUniverse:
    now: datetime
    policy: RankingPolicy
    references: list[str]
    source_ids: list[int]
    btc_source: Instrument
    btc_source_id: int
    invalid_reference: str


@pytest_asyncio.fixture
async def fx_universe(database: AsyncConnection[TupleRow]) -> FxUniverse:
    now = datetime.now(UTC)
    tag = uuid4().hex[:12].upper()
    btc, eur, usd, missing = (name + tag for name in ("BTC", "EUR", "USD", "ZZZ"))
    prefix = "FX_" + tag
    cur = await database.execute(
        "INSERT INTO catalog.metric_refreshes(scope_json,status,completed_at) VALUES('{}','complete',now()) RETURNING id"
    )
    row = await cur.fetchone()
    assert row is not None
    refresh: int = row[0]
    sources = [
        Instrument(
            exchange="binance",
            market="spot",
            symbol=prefix + name,
            base=base,
            quote=quote,
        )
        for name, base, quote in [
            ("BTCUSDT", btc, "USDT"),
            ("BTCEUR", btc, eur),
            ("USDUSDT", usd, "USDT"),
        ]
    ]
    source_ids: list[int] = []
    btc_source_id = 0
    for i, price in zip(sources, ("100000", "92000", "1.1"), strict=True):
        identity = await save_instrument(database, i, now)
        if i == sources[0]:
            btc_source_id = identity
        source_ids.append(
            await save_observation(
                database,
                refresh,
                identity,
                Observation(
                    instrument=i,
                    fetched_at=now,
                    source_observed_at=now,
                    window_start=now - timedelta(hours=24),
                    window_end=now,
                    window_basis="source",
                    last_price=Decimal(price),
                    raw={},
                ),
            )
        )
    # A tempting one-leg derivative rate must never become a conversion edge.
    decoy = Instrument(
        exchange="binance",
        market="linear",
        instrument_type="future",
        symbol=prefix + "DERIVATIVE_RATE",
        base=eur,
        quote="USDT",
    )
    decoy_id = await save_instrument(database, decoy, now)
    await save_observation(
        database,
        refresh,
        decoy_id,
        Observation(
            instrument=decoy,
            fetched_at=now,
            source_observed_at=now,
            window_start=now - timedelta(hours=24),
            window_end=now,
            window_basis="source",
            last_price=Decimal(2),
            raw={},
        ),
    )
    targets = [
        Instrument.model_validate(
            {
                "exchange": "binance",
                "market": market,
                "instrument_type": kind,
                "symbol": prefix + name,
                "base": "BASE" + tag,
                "quote": unit,
                "settlement": "BTC",
                "quantity_type": "contracts" if market == "inverse" else "base",
                "contract_value": Decimal(100) if market == "inverse" else None,
                "contract_unit": unit if market == "inverse" else None,
            }
        )
        for name, kind, market, unit in [
            ("SPOT", "spot", "spot", eur),
            ("LINEARPERP", "perpetual", "linear", "USDT"),
            ("INVERSEPERP", "perpetual", "inverse", usd),
            ("LINEARFUTURE", "future", "linear", eur),
            ("INVERSEFUTURE", "future", "inverse", usd),
            ("UNCONVERTIBLE", "spot", "spot", missing),
        ]
    ]
    tickers: list[dict[str, object]] = [
        {"quoteVolume": "92000"},
        {"quoteVolume": "200000"},
        {"volume": "3000"},
        {"quoteVolume": "368000"},
        {"volume": "9999", "quoteVolume": "500000"},
        {"quoteVolume": "10"},
    ]
    async with open_http_client(limiter=RequestRateLimiter(1)) as client:
        for i, ticker in zip(targets, tickers, strict=True):
            identity = await save_instrument(database, i, now)
            observation = PublicProvider("binance", client).observation(
                i, ticker, fetched=now
            )
            await save_observation(database, refresh, identity, observation)
    p = RankingPolicy.model_validate(
        {
            "version": 2,
            "instruments": [i.reference for i in targets],
            "allow_partial_universe": True,
            "conversion": {"bridge_assets": [btc]},
        }
    )
    return FxUniverse(
        now,
        p,
        [i.reference for i in targets],
        source_ids,
        sources[0],
        btc_source_id,
        targets[-1].reference,
    )


@pytest.mark.asyncio
async def test_mixed_market_ranking_uses_external_spot_sources_and_exact_units(
    database: AsyncConnection[TupleRow], fx_universe: FxUniverse
) -> None:
    f = fx_universe
    snapshot = await ranking.build(database, "fx-mixed", f.policy, now=f.now)
    result = await ranking.show(database, snapshot)
    rows = [obj(r) for r in array(result["entries"])]
    assert [r["score"] for r in rows[:5]] == [
        Decimal(550000),
        Decimal(400000),
        Decimal(330000),
        Decimal(200000),
        Decimal(100000),
    ]
    assert [r["instrument_type"] for r in rows[:5]] == [
        "future",
        "future",
        "perpetual",
        "perpetual",
        "spot",
    ]
    assert [r["market_category"] for r in rows[:5]] == [
        "inverse",
        "linear",
        "inverse",
        "linear",
        "spot",
    ]
    assert rows[0]["native_turnover"] == Decimal(
        500000
    )  # native quote volume, not 9999*100
    assert rows[2]["native_turnover"] == Decimal(
        300000
    )  # 3000 contracts*100 native quote units
    assert rows[5][
        "instrument_reference"
    ] == f.invalid_reference and "no eligible conversion pair/path" in str(
        rows[5]["exclusion_reason"]
    )
    assert result["conversion_summary"] == {
        "direct": 2,
        "bridged": 2,
        "identity": 1,
        "excluded": 1,
    }
    assert all(not r["turnover_estimated"] for r in rows[:5])
    assert {r["observation_id"] for r in rows}.isdisjoint(f.source_ids)
    assert (
        len(array(result["conversions"])) == 3
    )  # shared evidence reused per method/native currency
    for r in rows[:5]:
        evidence = obj(r["conversion_evidence"])
        ratio = Fraction(1)
        for leg in array(evidence["legs"]):
            data = obj(leg)
            cur = await database.execute(
                "SELECT last_price FROM catalog.metric_observations WHERE id=%s",
                (data["observation_id"],),
            )
            row = await cur.fetchone()
            assert row is not None and row[0] == Decimal(str(data["original_price"]))
            price = Fraction(row[0])
            ratio *= 1 / price if data["inverted"] else price
        exact = Fraction(Decimal(str(r["native_turnover"]))) * ratio
        with localcontext() as context:
            context.prec = 80
            assert r["score"] == Decimal(exact.numerator) / Decimal(exact.denominator)
    streams = [
        ranking.ConcreteStream(instrument_reference=ref, data_type=kind)
        for ref in f.references[:5]
        for kind in ("trades", "order_book_l2")
    ]
    resolved = await ranking.resolve_snapshot(database, snapshot, streams)
    assert [s.stream_rank for s in resolved] == list(range(1, 11))
    assert resolved[0].name.startswith(f.references[4])


@pytest.mark.asyncio
async def test_strict_rejection_and_persistence_failure_leave_no_snapshot(
    database: AsyncConnection[TupleRow],
    fx_universe: FxUniverse,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    f = fx_universe
    name = "fx-failure-" + uuid4().hex
    with pytest.raises(MarketDataError, match="no eligible conversion"):
        await ranking.build(
            database,
            name,
            f.policy.model_copy(update={"allow_partial_universe": False}),
            now=f.now,
        )

    def fail(self: Conversion) -> dict[str, object]:
        raise RuntimeError("Controlled evidence persistence failure")

    with monkeypatch.context() as patch:
        patch.setattr(Conversion, "document", fail)
        with pytest.raises(RuntimeError, match="Controlled evidence"):
            await ranking.build(database, name, f.policy, now=f.now)
    cur = await database.execute(
        "SELECT count(*) FROM ranking.snapshots WHERE policy_name=%s", (name,)
    )
    assert await cur.fetchone() == (0,)
    cur = await database.execute(
        "SELECT count(*) FROM ranking.conversions c LEFT JOIN ranking.snapshots s ON s.id=c.snapshot_id WHERE s.id IS NULL"
    )
    assert await cur.fetchone() == (0,)


@pytest.mark.asyncio
async def test_published_fx_and_observations_are_retained_and_reproducible(
    database: AsyncConnection[TupleRow],
    fx_universe: FxUniverse,
    monkeypatch: pytest.MonkeyPatch,
) -> None:

    f = fx_universe
    snapshot = await ranking.build(database, "fx-retained", f.policy, now=f.now)
    original = await ranking.show(database, snapshot)
    conversion = obj(array(original["conversions"])[0])
    checks: list[tuple[LiteralString, tuple[object, ...]]] = [
        (
            "UPDATE catalog.metric_observations SET last_price=1 WHERE id=%s",
            (f.source_ids[0],),
        ),
        ("DELETE FROM catalog.metric_observations WHERE id=%s", (f.source_ids[0],)),
        ("UPDATE ranking.conversions SET rate=1 WHERE id=%s", (conversion["id"],)),
        (
            "DELETE FROM ranking.conversion_inputs WHERE conversion_id=%s",
            (conversion["id"],),
        ),
    ]
    for sql, parameters in checks:
        with pytest.raises(psycopg.errors.RaiseException):
            async with database.transaction():
                await database.execute(sql, parameters)
    cur = await database.execute(
        "INSERT INTO catalog.metric_refreshes(scope_json,status,completed_at) VALUES('{}','complete',now()) RETURNING id"
    )
    row = await cur.fetchone()
    assert row is not None
    now = f.now + timedelta(seconds=60)
    await save_observation(
        database,
        row[0],
        f.btc_source_id,
        Observation(
            instrument=f.btc_source,
            fetched_at=now,
            source_observed_at=now,
            window_start=now - timedelta(hours=24),
            window_end=now,
            window_basis="source",
            last_price=Decimal(110000),
            raw={},
        ),
    )
    assert await ranking.show(database, snapshot) == original
    streams = [
        ranking.ConcreteStream(instrument_reference=ref, data_type="trades")
        for ref in f.references[:5]
    ]
    before = await ranking.resolve_snapshot(database, snapshot, streams)

    async def forbidden(
        c: AsyncConnection[TupleRow],
        name: str,
        policy: RankingPolicy,
        *,
        now: datetime | None = None,
    ) -> UUID:
        raise AssertionError("Pinned stream resolution must not rebuild rankings")

    with monkeypatch.context() as patch:
        patch.setattr(ranking, "build", forbidden)
        assert await ranking.resolve_snapshot(database, snapshot, streams) == before
    updated = await ranking.build(database, "fx-new-prices", f.policy, now=now)
    rows = [obj(r) for r in array((await ranking.show(database, updated))["entries"])]
    assert (
        next(r["score"] for r in rows if r["instrument_reference"] == f.references[0])
        == 110000
    )


@pytest.mark.asyncio
async def test_legacy_snapshot_has_no_inferred_conversion_evidence(
    database: AsyncConnection[TupleRow], fx_universe: FxUniverse
) -> None:
    f = fx_universe
    cur = await database.execute(
        """SELECT o.instrument_id,o.id FROM catalog.metric_observations o JOIN catalog.instruments i ON i.id=o.instrument_id
                                  WHERE i.symbol=%s ORDER BY o.id DESC LIMIT 1""",
        (f.references[0].split(".", 2)[2],),
    )
    row = await cur.fetchone()
    assert row is not None
    snapshot = uuid4()
    await database.execute(
        """INSERT INTO ranking.snapshots(id,policy_name,policy_json,policy_sha256,algorithm_version,comparison_currency,comparison_basis,created_at,status,partial_universe,universe_count,included_count)
       VALUES(%s,'legacy','{}',%s,'turnover24h-decimal50-v1','USDT','{}',%s,'draft',false,1,1)""",
        (snapshot, "a" * 64, f.now),
    )
    await database.execute(
        """INSERT INTO ranking.entries(snapshot_id,instrument_id,instrument_reference,observation_id,position,score)
                              VALUES(%s,%s,%s,%s,1,123)""",
        (snapshot, row[0], f.references[0], row[1]),
    )
    await database.execute(
        "UPDATE ranking.snapshots SET status='published' WHERE id=%s", (snapshot,)
    )
    result = await ranking.show(database, snapshot)
    entry = obj(array(result["entries"])[0])
    assert entry["score"] == 123 and entry["conversion_evidence"] is None
    assert result["conversions"] == [] and result["conversion_summary"] == {"legacy": 1}


@pytest.mark.asyncio
async def test_empty_bridge_allowlist_loads_unlisted_stored_intermediate(
    database: AsyncConnection[TupleRow], fx_universe: FxUniverse
) -> None:
    f = fx_universe
    # The fixture's UUID-qualified asset is neither BTC nor ETH and is outside
    # the ranked subset. This exercises SQL source loading as well as resolution.
    data = f.policy.model_dump(mode="python")
    data["conversion"]["bridge_assets"] = []
    p = RankingPolicy.model_validate(data)
    snapshot = await ranking.build(database, "fx-any-bridge", p, now=f.now)
    result = await ranking.show(database, snapshot)
    rows = [obj(r) for r in array(result["entries"])]
    assert [r["score"] for r in rows[:5]] == [
        Decimal(550000),
        Decimal(400000),
        Decimal(330000),
        Decimal(200000),
        Decimal(100000),
    ]
    assert rows[-1]["exclusion_reason"] is not None
