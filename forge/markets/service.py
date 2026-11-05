"""Bounded collection with shared upstream clients and owned scope transactions."""

import asyncio
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack
from datetime import UTC, datetime

import httpx
from psycopg import AsyncConnection
from psycopg.rows import TupleRow

from forge.historical.archives.operation import failure_message
from forge.markets.capabilities import COLLECTION_VERSION, EXCHANGES, MARKETS, resolve_scopes
from forge.markets.models import Exchange, Market, MarketDataError, string
from forge.markets.progress import CollectionProgress, ProgressCallback, ScopeProgress
from forge.markets.providers.public import PublicProvider
from forge.markets.repository import (
    StoredInstrument,
    iter_instruments,
    json_value,
    publish_observations,
    save_instrument,
    save_observation,
)
from forge.shared.async_work import bounded_map
from forge.shared.config.models import HttpSettings
from forge.shared.database.testing import testing_connection
from forge.shared.http.configured import open_configured_http_client

__all__ = ["EXCHANGES", "MARKETS", "collect"]


async def collect(
    dsn: str,
    settings: HttpSettings,
    *,
    metadata: bool,
    exchanges: list[Exchange] | None = None,
    markets: list[Market] | None = None,
    symbol: str | None = None,
    transport: httpx.AsyncBaseTransport | None = None,
    progress: ProgressCallback | None = None,
) -> dict[str, object]:
    scopes = resolve_scopes(exchanges, markets)
    outcomes: list[dict[str, object]] = [
        {
            "exchange": e,
            "market": m,
            "status": "queued",
            "observations": 0,
            "collected": 0,
            "started": False,
            "count_kind": "metadata records" if metadata else "observations",
            "instrument_counts": {"spot": 0, "perpetual": 0, "future": 0},
        }
        for e, m in scopes
    ]
    scope = {
        "exchanges": list(dict.fromkeys(e for e, _ in scopes)),
        "markets": list(dict.fromkeys(m for _, m in scopes)),
        "symbol": symbol,
        "collection_version": COLLECTION_VERSION,
    }
    async with testing_connection(dsn) as c:
        cur = await c.execute(
            "INSERT INTO catalog.instrument_syncs(scope_json,status,source_outcomes) VALUES(%s,'running',%s) RETURNING id"
            if metadata
            else "INSERT INTO catalog.metric_refreshes(scope_json,status,source_outcomes) VALUES(%s,'running',%s) RETURNING id",
            (json_value(scope), json_value(outcomes)),
        )
        row = await cur.fetchone()
        if row is None:
            raise MarketDataError("Could not reserve market collection.")
        refresh: int = row[0]

    def notify() -> None:
        if progress is not None:
            snapshot = CollectionProgress(
                metadata,
                tuple(
                    ScopeProgress(
                        str(o["exchange"]),
                        str(o["market"]),
                        str(o["status"]),
                        int(str(o["collected"])),
                    )
                    for o in outcomes
                ),
            )
            try:
                progress(snapshot)
            except Exception:
                pass  # Presentation cannot change collection/registry correctness.

    async def write_outcome(
        c: AsyncConnection[TupleRow], index: int, value: dict[str, object] | None = None
    ) -> None:
        await c.execute(
            "UPDATE catalog.instrument_syncs SET source_outcomes=jsonb_set(source_outcomes,ARRAY[%s::text],%s) WHERE id=%s"
            if metadata
            else "UPDATE catalog.metric_refreshes SET source_outcomes=jsonb_set(source_outcomes,ARRAY[%s::text],%s) WHERE id=%s",
            (str(index), json_value(value if value is not None else outcomes[index]), refresh),
        )

    async def persist(index: int) -> None:
        async with testing_connection(dsn) as c:
            await write_outcome(c, index)

    cancelled = False
    notify()
    try:
        async with AsyncExitStack() as stack:
            # Initialized serially, once per exchange; all scope workers reuse budgets.
            clients = {
                e: await stack.enter_async_context(
                    open_configured_http_client(
                        settings, requests_per_second=0.2, transport=transport
                    )
                )
                for e in dict.fromkeys(e for e, _ in scopes)
            }

            async def groups() -> AsyncIterator[int]:
                # Round-robin exchanges before revisiting a quota-limited upstream.
                order = {market: index for index, market in enumerate(MARKETS)}
                for index in sorted(range(len(scopes)), key=lambda i: order[scopes[i][1]]):
                    yield index

            async def work(index: int) -> bool:
                e, m = scopes[index]
                outcome = outcomes[index]
                outcome.update(status="running", started=True)
                count = 0
                scope_persisted = False
                counts = {"spot": 0, "perpetual": 0, "future": 0}
                notify()
                try:
                    await persist(index)
                    provider = PublicProvider(e, clients[e])
                    async with testing_connection(dsn) as c:
                        if metadata:
                            seen_metadata: set[tuple[str, str]] = set()
                            async for page in provider.pages(m, metadata=True):
                                fetched = datetime.now(UTC)
                                for row in page:
                                    item = provider.instrument(m, row)
                                    if item is None or (
                                        symbol is not None and item.symbol != symbol
                                    ):
                                        continue
                                    identity = (item.instrument_type, item.symbol)
                                    if identity in seen_metadata:
                                        raise MarketDataError(
                                            "Duplicate metadata identity in source pages."
                                        )
                                    seen_metadata.add(identity)
                                    await save_instrument(c, item, fetched, sync_id=refresh)
                                    count += 1
                                    counts[item.instrument_type] += 1
                                    if count % 100 == 0:
                                        outcome["collected"] = count
                                        notify()
                                outcome["collected"] = count
                                notify()
                        else:
                            cur = await c.execute(
                                """SELECT id FROM catalog.instrument_syncs WHERE status IN ('complete','partial','cancelled')
                                AND scope_json->>'collection_version'=%s AND (scope_json->>'symbol' IS NULL OR scope_json->>'symbol'=%s)
                                AND EXISTS(SELECT 1 FROM jsonb_array_elements(source_outcomes) s WHERE s->>'exchange'=%s AND s->>'market'=%s AND s->>'status'='complete')
                                ORDER BY completed_at DESC,id DESC LIMIT 1""",
                                (COLLECTION_VERSION, symbol, e, m),
                            )
                            sync = await cur.fetchone()
                            if sync is None:
                                raise MarketDataError(
                                    "No completed futures-aware metadata scope; run markets sync for this scope first."
                                )
                            outcome["instrument_sync_id"] = sync[0]
                            outcome["selection"] = (
                                "current certified metadata membership; absent historical records retained"
                            )
                            selected: dict[tuple[str, str], StoredInstrument] = {}
                            async for item in iter_instruments(
                                c, exchange=e, market=m, symbol=symbol, sync_id=sync[0]
                            ):
                                if (
                                    e == "gateio"
                                    and m == "inverse"
                                    and item.instrument_type == "future"
                                ):
                                    continue
                                selected[(item.instrument_type, item.symbol)] = item
                            outcome["empty_current_selection"] = not selected
                            by_symbol: dict[
                                str, list[tuple[tuple[str, str], StoredInstrument]]
                            ] = {}
                            for key, item in selected.items():
                                by_symbol.setdefault(key[1], []).append((key, item))
                            seen: set[tuple[str, str]] = set()
                            async for page in provider.pages(m, metadata=False):
                                fetched = datetime.now(UTC)
                                for row in page:
                                    native = string(
                                        row.get("symbol")
                                        or row.get("instId")
                                        or row.get("currency_pair")
                                        or row.get("contract")
                                    )
                                    hint = row.get("_collection_type")
                                    if row.get("instType") in {"SWAP", "FUTURES", "SPOT"}:
                                        reported_type = {
                                            "SWAP": "perpetual",
                                            "FUTURES": "future",
                                            "SPOT": "spot",
                                        }[str(row["instType"])]
                                        if hint is not None and reported_type != hint:
                                            raise MarketDataError(
                                                "Ticker instrument type disagrees with its endpoint."
                                            )
                                        hint = {
                                            "SWAP": "perpetual",
                                            "FUTURES": "future",
                                            "SPOT": "spot",
                                        }[str(row["instType"])]
                                    candidates = [
                                        (key, item)
                                        for key, item in by_symbol.get(native, [])
                                        if hint is None or key[0] == hint
                                    ]
                                    if not candidates:
                                        continue
                                    if len(candidates) != 1:
                                        raise MarketDataError(
                                            "Ambiguous ticker identity; response must identify the contract type."
                                        )
                                    key, item = candidates[0]
                                    if key in seen:
                                        raise MarketDataError(
                                            "Duplicate ticker identity in source response."
                                        )
                                    observation = provider.observation(
                                        item.model(), row, fetched=fetched
                                    )
                                    await save_observation(
                                        c, refresh, item.id, observation, update_latest=False
                                    )
                                    seen.add(key)
                                    count += 1
                                    counts[item.instrument_type] += 1
                                    if count % 100 == 0:
                                        outcome["collected"] = count
                                        notify()
                                outcome["collected"] = count
                                notify()
                            missing = sorted(
                                f"{kind}.{native}" for kind, native in set(selected) - seen
                            )
                            outcome.update(
                                missing_instruments=missing[:50], missing_count=len(missing)
                            )
                            if missing:
                                raise MarketDataError(
                                    "Refresh omitted selected catalogue instruments; inspect status/expiry and synchronize metadata explicitly."
                                )
                        complete = {
                            **outcome,
                            "status": "complete",
                            "observations": count,
                            "collected": count,
                            "instrument_counts": counts,
                            "completed_at": datetime.now(UTC).isoformat(),
                        }
                        # Scope certification and latest values commit together, after network I/O.
                        async with c.transaction():
                            if not metadata:
                                await publish_observations(c, refresh, e, m)
                            await write_outcome(c, index, complete)
                        outcome.update(complete)
                        scope_persisted = True
                except asyncio.CancelledError:
                    outcome["status"] = "cancelled"
                    raise
                except Exception as error:
                    outcome.update(
                        status="failed",
                        error=str(error)
                        if isinstance(error, MarketDataError)
                        else failure_message(error),
                    )
                finally:
                    if not scope_persisted:
                        outcome.update(
                            observations=count,
                            collected=count,
                            instrument_counts=counts,
                            completed_at=datetime.now(UTC).isoformat(),
                        )
                        await persist(index)
                    notify()
                return True

            async for _ in bounded_map(groups(), work, workers=min(3, settings.max_concurrent)):
                pass
    except asyncio.CancelledError:
        cancelled = True
    except BaseException:
        for o in outcomes:
            if o["status"] in {"queued", "running"}:
                o["status"] = "cancelled"
        async with testing_connection(dsn) as c:
            await c.execute(
                "UPDATE catalog.instrument_syncs SET status='failed',completed_at=now(),source_outcomes=%s WHERE id=%s"
                if metadata
                else "UPDATE catalog.metric_refreshes SET status='failed',completed_at=now(),source_outcomes=%s WHERE id=%s",
                (json_value(outcomes), refresh),
            )
        raise
    if cancelled:
        for o in outcomes:
            if o["status"] in {"queued", "running"}:
                o["status"] = "cancelled"
    status = (
        "cancelled"
        if cancelled
        else "complete"
        if all(o["status"] == "complete" for o in outcomes)
        else "partial"
    )
    async with testing_connection(dsn) as c:
        await c.execute(
            "UPDATE catalog.instrument_syncs SET status=%s,completed_at=now(),source_outcomes=%s WHERE id=%s"
            if metadata
            else "UPDATE catalog.metric_refreshes SET status=%s,completed_at=now(),source_outcomes=%s WHERE id=%s",
            (status, json_value(outcomes), refresh),
        )
    notify()
    return {
        "status": status,
        "refresh_id": refresh,
        "sources": outcomes,
        "collection_version": COLLECTION_VERSION,
        "metadata": metadata,
        "semantics": "Current observations only; missing is not zero. Native quote denominations retained.",
    }
