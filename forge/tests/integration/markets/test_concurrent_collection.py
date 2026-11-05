"""Real scoped registry writes, controlled async endpoints, no exchange network."""

import asyncio
from collections.abc import AsyncIterator

import pytest

from forge.markets.capabilities import resolve_scopes
from forge.markets.models import Market, MarketDataError, array, obj
from forge.markets.progress import CollectionProgress
from forge.markets.providers.public import PublicProvider
from forge.markets.service import collect
from forge.shared.config.models import HttpSettings
from forge.shared.database.testing import testing_connection as connect_test
from forge.tests.integration.markets.conftest import CollectionScope

pytestmark = [pytest.mark.integration, pytest.mark.database]


@pytest.mark.asyncio
@pytest.mark.parametrize("workers", [2, 3])
async def test_global_overlap_bounds_and_shared_upstream_budget(
    collection_scope: CollectionScope, monkeypatch: pytest.MonkeyPatch, workers: int
) -> None:
    entered = asyncio.Event()
    release = asyncio.Event()
    active = 0
    peak = 0
    clients: dict[str, set[int]] = {}
    snapshots: list[CollectionProgress] = []

    async def pages(
        self: PublicProvider, market: Market, *, metadata: bool
    ) -> AsyncIterator[list[dict[str, object]]]:
        nonlocal active, peak
        clients.setdefault(self.exchange, set()).add(id(self.http))
        active += 1
        peak = max(peak, active)
        if active == workers:
            entered.set()
        try:
            await release.wait()
            yield []  # Successfully interpreted empty scopes remain successful.
        finally:
            active -= 1

    monkeypatch.setattr(PublicProvider, "pages", pages)
    task = asyncio.create_task(
        collect(
            collection_scope.dsn,
            HttpSettings(max_concurrent=workers),
            metadata=True,
            symbol=collection_scope.symbol,
            progress=snapshots.append,
        )
    )
    await asyncio.wait_for(entered.wait(), 5)
    assert peak == workers and not task.done()
    assert len(clients) == workers  # Independent exchanges really overlap.
    release.set()
    result = await task
    assert result["status"] == "complete" and active == 0
    sources = [obj(v) for v in array(result["sources"])]
    assert {(s["exchange"], s["market"]) for s in sources} == set(resolve_scopes())
    assert all(s["status"] == "complete" for s in sources)
    assert all(len(ids) == 1 for ids in clients.values()) and len(clients) == 5
    assert peak == workers
    assert snapshots[-1].finished == 14 and len(snapshots[-1].scopes) == 14
    async with connect_test(collection_scope.dsn) as c:
        cur = await c.execute(
            "SELECT source_outcomes FROM catalog.instrument_syncs WHERE id=%s",
            (result["refresh_id"],),
        )
        stored = await cur.fetchone()
        assert stored is not None and all(obj(v)["status"] == "complete" for v in array(stored[0]))


@pytest.mark.asyncio
async def test_failed_scope_continues_others_and_progress_failures_are_isolated(
    collection_scope: CollectionScope,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def pages(
        self: PublicProvider, market: Market, *, metadata: bool
    ) -> AsyncIterator[list[dict[str, object]]]:
        if market == "linear":
            raise MarketDataError("Controlled FUTURES endpoint failure")
        yield []

    def broken_display(snapshot: CollectionProgress) -> None:
        raise RuntimeError("terminal disconnected")

    monkeypatch.setattr(PublicProvider, "pages", pages)
    result = await collect(
        collection_scope.dsn,
        HttpSettings(),
        metadata=True,
        exchanges=["bybit"],
        symbol=collection_scope.symbol,
        progress=broken_display,
    )
    assert result["status"] == "partial"
    sources = [obj(v) for v in array(result["sources"])]
    assert [s["status"] for s in sources] == ["complete", "failed", "complete"]


@pytest.mark.asyncio
async def test_cancellation_records_active_and_not_started_scopes(
    collection_scope: CollectionScope,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entered = asyncio.Event()
    cleaned = asyncio.Event()
    active = 0

    async def pages(
        self: PublicProvider, market: Market, *, metadata: bool
    ) -> AsyncIterator[list[dict[str, object]]]:
        nonlocal active
        active += 1
        if active == 2:
            entered.set()
        try:
            await asyncio.Event().wait()
            yield []
        finally:
            active -= 1
            if active == 0:
                cleaned.set()

    monkeypatch.setattr(PublicProvider, "pages", pages)
    task = asyncio.create_task(
        collect(
            collection_scope.dsn,
            HttpSettings(max_concurrent=2),
            metadata=True,
            symbol=collection_scope.symbol,
        )
    )
    await asyncio.wait_for(entered.wait(), 5)
    task.cancel()
    result = await task
    assert result["status"] == "cancelled" and cleaned.is_set()
    sources = [obj(v) for v in array(result["sources"])]
    assert len(sources) == 14 and all(s["status"] == "cancelled" for s in sources)
    assert sum(bool(s["started"]) for s in sources) == 2
    async with connect_test(collection_scope.dsn) as c:
        cur = await c.execute(
            "SELECT status,jsonb_array_length(source_outcomes) FROM catalog.instrument_syncs WHERE id=%s",
            (result["refresh_id"],),
        )
        assert await cur.fetchone() == ("cancelled", 14)


@pytest.mark.asyncio
async def test_exact_ticker_types_and_failed_scope_do_not_freshen_cache(
    collection_scope: CollectionScope,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from datetime import UTC, datetime, timedelta
    from decimal import Decimal

    from forge.markets.capabilities import COLLECTION_VERSION
    from forge.markets.models import Instrument, Observation
    from forge.markets.repository import browse, json_value, save_instrument, save_observation

    now = datetime.now(UTC)
    items = [
        Instrument(
            exchange="okx",
            market="linear",
            instrument_type=kind,
            symbol=collection_scope.symbol,
            base="BTC",
            quote="USDT",
        )
        for kind in ("perpetual", "future")
    ]
    async with connect_test(collection_scope.dsn) as c:
        identities = [await save_instrument(c, i, now) for i in items]
        sync_cur = await c.execute(
            "INSERT INTO catalog.instrument_syncs(scope_json,status,completed_at,source_outcomes) VALUES(%s,'complete',now(),%s) RETURNING id",
            (
                json_value(
                    {"symbol": collection_scope.symbol, "collection_version": COLLECTION_VERSION}
                ),
                json_value([{"exchange": "okx", "market": "linear", "status": "complete"}]),
            ),
        )
        sync = await sync_cur.fetchone()
        assert sync is not None
        for identity in identities:
            await c.execute(
                "INSERT INTO catalog.instrument_sync_members(sync_id,instrument_id) VALUES(%s,%s)",
                (sync[0], identity),
            )
        cur = await c.execute(
            "INSERT INTO catalog.metric_refreshes(scope_json,status,completed_at) VALUES(%s,'complete',now()) RETURNING id",
            (json_value({"symbol": collection_scope.symbol}),),
        )
        row = await cur.fetchone()
        assert row is not None
        for item, identity in zip(items, identities, strict=True):
            await save_observation(
                c,
                row[0],
                identity,
                Observation(
                    instrument=item,
                    fetched_at=now - timedelta(hours=2),
                    window_start=now - timedelta(hours=26),
                    window_end=now - timedelta(hours=2),
                    window_basis="fetch_time",
                    turnover=Decimal(1),
                    turnover_unit="USDT",
                    raw={},
                ),
            )
    failing = True

    async def pages(
        self: PublicProvider, market: Market, *, metadata: bool
    ) -> AsyncIterator[list[dict[str, object]]]:
        assert not metadata
        yield [
            {"instId": collection_scope.symbol, "instType": "SWAP", "last": "100", "volCcy24h": "2"}
        ]
        if failing:
            raise MarketDataError("Controlled FUTURES failure")
        yield [
            {
                "instId": collection_scope.symbol,
                "instType": "FUTURES",
                "last": "50",
                "volCcy24h": "5",
            }
        ]

    monkeypatch.setattr(PublicProvider, "pages", pages)
    failed = await collect(
        collection_scope.dsn,
        HttpSettings(),
        metadata=False,
        exchanges=["okx"],
        markets=["linear"],
        symbol=collection_scope.symbol,
    )
    assert failed["status"] == "partial"
    async with connect_test(collection_scope.dsn) as c:
        old = await browse(c, symbol=collection_scope.symbol)
        assert all(r["turnover_24h"] == Decimal(1) and r["stale"] for r in old)
    failing = False
    successful = await collect(
        collection_scope.dsn,
        HttpSettings(),
        metadata=False,
        exchanges=["okx"],
        markets=["linear"],
        symbol=collection_scope.symbol,
    )
    assert successful["status"] == "complete"
    async with connect_test(collection_scope.dsn) as c:
        rows = await browse(c, symbol=collection_scope.symbol)
        values = {r["instrument_type"]: r["turnover_24h"] for r in rows}
        assert values == {"perpetual": Decimal(200), "future": Decimal(250)}
        assert all(not r["stale"] for r in rows)
        prior_ids = {r["instrument_type"]: r["observation_id"] for r in rows}
    from psycopg import AsyncConnection
    from psycopg.rows import TupleRow

    import forge.markets.service as service
    from forge.markets.repository import publish_observations

    async def fail_publication(
        c: AsyncConnection[TupleRow], refresh: int, exchange: str, market: str
    ) -> None:
        await publish_observations(c, refresh, exchange, market)
        raise MarketDataError("Controlled scope completion persistence failure")

    monkeypatch.setattr(service, "publish_observations", fail_publication)
    rejected = await collect(
        collection_scope.dsn,
        HttpSettings(),
        metadata=False,
        exchanges=["okx"],
        markets=["linear"],
        symbol=collection_scope.symbol,
    )
    assert rejected["status"] == "partial"
    async with connect_test(collection_scope.dsn) as c:
        rows = await browse(c, symbol=collection_scope.symbol)
        assert {r["instrument_type"]: r["observation_id"] for r in rows} == prior_ids


@pytest.mark.asyncio
async def test_excessive_cooldown_is_shared_across_scopes_not_evaded(
    collection_scope: CollectionScope,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import httpx

    from forge.shared.http.rate_limit import RequestRateLimiter

    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(429, headers={"Retry-After": "3600"})

    async def immediate(self: RequestRateLimiter) -> None:
        pass

    async def pages(
        self: PublicProvider, market: Market, *, metadata: bool
    ) -> AsyncIterator[list[dict[str, object]]]:
        await self.http.get("https://controlled.test/metadata", params={"market": market})
        yield []

    monkeypatch.setattr(RequestRateLimiter, "wait", immediate)
    monkeypatch.setattr(PublicProvider, "pages", pages)
    result = await collect(
        collection_scope.dsn,
        HttpSettings(max_concurrent=1),
        metadata=True,
        exchanges=["binance"],
        symbol=collection_scope.symbol,
        transport=httpx.MockTransport(handler),
    )
    assert result["status"] == "partial" and calls == 1
    assert all(obj(s)["status"] == "failed" for s in array(result["sources"]))


@pytest.mark.asyncio
async def test_successful_empty_listing_retains_old_contract_without_requiring_its_ticker(
    collection_scope: CollectionScope,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from datetime import UTC, datetime

    from forge.markets.models import Instrument
    from forge.markets.repository import save_instrument

    async with connect_test(collection_scope.dsn) as c:
        identity = await save_instrument(
            c,
            Instrument(
                exchange="bybit",
                market="linear",
                instrument_type="future",
                symbol=collection_scope.symbol,
                base="BTC",
                quote="USDT",
            ),
            datetime.now(UTC),
        )

    async def empty(
        self: PublicProvider, market: Market, *, metadata: bool
    ) -> AsyncIterator[list[dict[str, object]]]:
        yield []

    monkeypatch.setattr(PublicProvider, "pages", empty)
    metadata = await collect(
        collection_scope.dsn,
        HttpSettings(),
        metadata=True,
        exchanges=["bybit"],
        markets=["linear"],
        symbol=collection_scope.symbol,
    )
    assert metadata["status"] == "complete"
    refreshed = await collect(
        collection_scope.dsn,
        HttpSettings(),
        metadata=False,
        exchanges=["bybit"],
        markets=["linear"],
        symbol=collection_scope.symbol,
    )
    assert (
        refreshed["status"] == "complete" and obj(array(refreshed["sources"])[0])["collected"] == 0
    )
    async with connect_test(collection_scope.dsn) as c:
        cur = await c.execute(
            "SELECT status,instrument_type FROM catalog.instruments WHERE id=%s", (identity,)
        )
        assert await cur.fetchone() == ("active", "future")


@pytest.mark.asyncio
async def test_concurrent_writes_own_connections_and_register_scope_membership(
    collection_scope: CollectionScope,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from datetime import datetime

    from psycopg import AsyncConnection
    from psycopg.rows import TupleRow

    import forge.markets.service as service
    from forge.markets.models import Instrument
    from forge.markets.repository import save_instrument

    entered = asyncio.Event()
    release = asyncio.Event()
    reached = 0
    connections: set[int] = set()

    async def persist(
        c: AsyncConnection[TupleRow],
        instrument: Instrument,
        fetched: datetime,
        *,
        sync_id: int | None = None,
    ) -> int:
        connections.add(id(c))
        return await save_instrument(c, instrument, fetched, sync_id=sync_id)

    async def pages(
        self: PublicProvider, market: Market, *, metadata: bool
    ) -> AsyncIterator[list[dict[str, object]]]:
        nonlocal reached
        if metadata:
            reached += 1
            if reached == 3:
                entered.set()
            await release.wait()
            yield [
                {
                    "symbol": collection_scope.symbol,
                    "baseCoin": "BTC",
                    "quoteCoin": "USD" if market == "inverse" else "USDT",
                    "settleCoin": "BTC" if market == "inverse" else "USDT",
                    "contractType": "InverseFutures" if market == "inverse" else "LinearFutures",
                    "deliveryTime": "1893456000000",
                    "status": "Trading",
                    "priceFilter": {"tickSize": ".1"},
                    "lotSizeFilter": {"basePrecision": ".01", "qtyStep": ".01"},
                }
            ]
        else:
            yield [
                {
                    "symbol": collection_scope.symbol,
                    "lastPrice": "50",
                    "volume24h": "2",
                    "turnover24h": "100",
                }
            ]

    monkeypatch.setattr(service, "save_instrument", persist)
    monkeypatch.setattr(PublicProvider, "pages", pages)
    task = asyncio.create_task(
        collect(
            collection_scope.dsn,
            HttpSettings(),
            metadata=True,
            exchanges=["bybit"],
            symbol=collection_scope.symbol,
        )
    )
    await asyncio.wait_for(entered.wait(), 5)
    release.set()
    result = await task
    assert result["status"] == "complete" and len(connections) == 3
    async with connect_test(collection_scope.dsn) as c:
        cur = await c.execute(
            "SELECT count(*) FROM catalog.instrument_sync_members WHERE sync_id=%s",
            (result["refresh_id"],),
        )
        assert await cur.fetchone() == (3,)
    refreshed = await collect(
        collection_scope.dsn,
        HttpSettings(),
        metadata=False,
        exchanges=["bybit"],
        symbol=collection_scope.symbol,
    )
    assert refreshed["status"] == "complete"
    assert all(obj(s)["collected"] == 1 for s in array(refreshed["sources"]))
