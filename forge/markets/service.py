"""Bounded market collection with per-source outcomes and no implicit browsing sync."""

from collections.abc import AsyncIterator
from contextlib import AsyncExitStack
from datetime import UTC, datetime

import httpx

from forge.historical.archives.operation import failure_message
from forge.markets.models import Exchange, Market, MarketDataError, string
from forge.markets.providers.public import PublicProvider
from forge.markets.repository import instruments, json_value, save_instrument, save_observation
from forge.shared.async_work import bounded_map
from forge.shared.config.models import HttpSettings
from forge.shared.database.testing import testing_connection
from forge.shared.http.configured import open_configured_http_client

EXCHANGES: tuple[Exchange, ...] = ("binance", "bybit", "okx", "gateio", "bitget")
MARKETS: tuple[Market, ...] = ("spot", "linear", "inverse")


async def collect(
    dsn: str,
    settings: HttpSettings,
    *,
    metadata: bool,
    exchanges: list[Exchange],
    markets: list[Market],
    symbol: str | None = None,
    transport: httpx.AsyncBaseTransport | None = None,
) -> dict[str, object]:
    exchanges = list(dict.fromkeys(exchanges))
    markets = list(dict.fromkeys(markets))
    if not exchanges or not markets:
        raise MarketDataError("Select a nonempty exchange and market scope.")
    outcomes: list[dict[str, object]] = []
    scope = {"exchanges": exchanges, "markets": markets, "symbol": symbol}
    refresh_id: int | None = None
    async with testing_connection(dsn) as c:
        if metadata:
            cur = await c.execute(
                "INSERT INTO catalog.instrument_syncs(scope_json,status) VALUES(%s,'running') RETURNING id",
                (json_value(scope),),
            )
            row = await cur.fetchone()
            if row is None:
                raise MarketDataError("Could not reserve instrument sync.")
            refresh_id = row[0]
        else:
            cur = await c.execute(
                "INSERT INTO catalog.metric_refreshes(scope_json,status) VALUES(%s,'running') RETURNING id",
                (json_value(scope),),
            )
            row = await cur.fetchone()
            if row is None:
                raise MarketDataError("Could not reserve metric refresh.")
            refresh_id = row[0]
    async with AsyncExitStack() as stack:
        clients = {
            e: await stack.enter_async_context(
                open_configured_http_client(settings, requests_per_second=0.2, transport=transport)
            )
            for e in exchanges
        }

        async def groups() -> AsyncIterator[tuple[Exchange, Market]]:
            for e in exchanges:
                for m in markets:
                    if e == "bitget" and m == "inverse":
                        continue
                    yield e, m

        async def work(group: tuple[Exchange, Market]) -> bool:
            e, m = group
            outcome: dict[str, object] = {
                "exchange": e,
                "market": m,
                "status": "failed",
                "observations": 0,
            }
            outcomes.append(outcome)
            provider = PublicProvider(e, clients[e])
            count = 0
            try:
                async with testing_connection(dsn) as c:
                    if metadata:
                        async for page in provider.pages(m, metadata=True):
                            fetched = datetime.now(UTC)
                            for row in page:
                                item = provider.instrument(m, row)
                                if item is not None and (symbol is None or item.symbol == symbol):
                                    await save_instrument(c, item, fetched)
                                    count += 1
                    else:
                        stored = await instruments(c, e, m, symbol=symbol)
                        if len(stored) >= 10000:
                            raise MarketDataError(
                                "Market universe exceeds bounded collection limit."
                            )
                        selected = {i.symbol: i for i in stored if i.model().active}
                        if not selected:
                            raise MarketDataError(
                                "No active selected instruments; run markets sync first."
                            )
                        cur = await c.execute(
                            """SELECT id,source_outcomes FROM catalog.instrument_syncs
                            WHERE status IN ('complete','partial') AND scope_json->'exchanges' ? %s AND scope_json->'markets' ? %s
                              AND (scope_json->>'symbol' IS NULL OR scope_json->>'symbol'=%s)
                            ORDER BY completed_at DESC,id DESC LIMIT 1""",
                            (e, m, symbol),
                        )
                        sync = await cur.fetchone()
                        from forge.markets.models import array, obj

                        if sync is None or not any(
                            obj(value).get("exchange") == e
                            and obj(value).get("market") == m
                            and obj(value).get("status") == "complete"
                            for value in array(sync[1])
                        ):
                            raise MarketDataError(
                                "No successfully completed metadata scope; synchronize this market before refreshing statistics."
                            )
                        outcome["instrument_sync_id"] = sync[0]
                        seen: set[str] = set()
                        async for page in provider.pages(m, metadata=False):
                            fetched = datetime.now(UTC)
                            for row in page:
                                key = string(
                                    row.get("symbol")
                                    or row.get("instId")
                                    or row.get("currency_pair")
                                    or row.get("contract")
                                )
                                if key not in selected:
                                    continue
                                if key in seen:
                                    raise MarketDataError(
                                        "Duplicate ticker identity in source response."
                                    )
                                item = selected[key]
                                observation = provider.observation(
                                    item.model(), row, fetched=fetched
                                )
                                if refresh_id is None:
                                    raise MarketDataError("Refresh reservation is missing.")
                                await save_observation(c, refresh_id, item.id, observation)
                                seen.add(key)
                                count += 1
                        missing = sorted(set(selected) - seen)
                        outcome["missing_instruments"] = missing[:50]
                        outcome["missing_count"] = len(missing)
                        if missing:
                            raise MarketDataError(
                                "Refresh omitted selected catalogue instruments; sync metadata explicitly if needed."
                            )
                outcome["status"] = "complete"
            except Exception as error:
                outcome["error"] = (
                    str(error) if isinstance(error, MarketDataError) else failure_message(error)
                )
            outcome["observations"] = count
            return True

        try:
            async for _ in bounded_map(groups(), work, workers=3):
                pass
        except BaseException:
            if refresh_id is not None:
                async with testing_connection(dsn) as c:
                    await c.execute(
                        "UPDATE catalog.instrument_syncs SET status='cancelled',completed_at=now(),source_outcomes=%s WHERE id=%s"
                        if metadata
                        else "UPDATE catalog.metric_refreshes SET status='cancelled',completed_at=now(),source_outcomes=%s WHERE id=%s",
                        (json_value(outcomes), refresh_id),
                    )
            raise
    status = "complete" if all(o["status"] == "complete" for o in outcomes) else "partial"
    if refresh_id is not None:
        async with testing_connection(dsn) as c:
            await c.execute(
                "UPDATE catalog.instrument_syncs SET status=%s,completed_at=now(),source_outcomes=%s WHERE id=%s"
                if metadata
                else "UPDATE catalog.metric_refreshes SET status=%s,completed_at=now(),source_outcomes=%s WHERE id=%s",
                (status, json_value(outcomes), refresh_id),
            )
    return {
        "status": status,
        "refresh_id": refresh_id,
        "sources": sorted(outcomes, key=lambda o: (str(o["exchange"]), str(o["market"]))),
        "semantics": "Current observations only; missing is not zero. Native quote denominations retained.",
    }
