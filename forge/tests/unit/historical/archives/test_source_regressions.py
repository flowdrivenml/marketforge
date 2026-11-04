"""Sanitized observed listings and strict exchange identity/interval regressions."""

import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest

from forge.historical.archives.config import AcquisitionInterval, ArchiveRequest
from forge.historical.archives.sources.base import DiscoveryError
from forge.historical.archives.sources.exchanges import ExchangeSource
from forge.shared.http.client import open_http_client
from forge.shared.http.rate_limit import RequestRateLimiter

FIXTURES = Path(__file__).parent / "fixtures"


class ImmediateLimiter(RequestRateLimiter):
    async def wait(self) -> None:
        pass


@pytest.fixture
def interval() -> AcquisitionInterval:
    return AcquisitionInterval(
        start=datetime(2026, 11, 1, tzinfo=UTC), end=datetime(2026, 11, 4, tzinfo=UTC)
    )


def selection(exchange: str, market: str = "spot", data_type: str = "trades") -> ArchiveRequest:
    symbol = {
        "bybit": "BTCUSD" if market == "inverse" else "BTCUSDT",
        "gateio": "BTC_USD" if market == "inverse" else "BTC_USDT",
        "okx": "BTC-USD" if market == "inverse" else "BTC-USDT",
        "bitget": "BTC/USDT" if market == "spot" else "BTCUSDT",
        "binance": "BTCUSD_PERP" if market == "inverse" else "BTCUSDT",
    }[exchange]
    return ArchiveRequest.model_validate(
        {
            "exchange": exchange,
            "instrument_type": "spot" if market == "spot" else "perpetual",
            "market_category": market,
            "data_type": data_type,
            "scope": {
                "kind": "family" if exchange == "okx" and market != "spot" else "instrument",
                "identifier": symbol,
            },
        }
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("market", ["spot", "linear", "inverse"])
async def test_observed_bybit_directory_variants(
    interval: AcquisitionInterval, market: str
) -> None:
    page = (FIXTURES / f"bybit_{market}.html").read_text()
    async with open_http_client(
        limiter=ImmediateLimiter(1),
        transport=httpx.MockTransport(lambda _: httpx.Response(200, text=page)),
    ) as http:
        source = ExchangeSource("bybit", http)
        results = [a async for a in source.discover(selection("bybit", market), interval)]
    assert len(results) == 3
    assert len({a.identity for a in results}) == 3
    assert all(
        a.interval.start >= interval.start and a.interval.end <= interval.end for a in results
    )
    assert source.version != "archive-v1"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "invalid", ["denied", "wrong_heading", "unrelated_link", "traversal", "unrelated_instrument"]
)
async def test_bybit_negative_pages_and_links_fail_closed(
    interval: AcquisitionInterval, invalid: str
) -> None:
    page = (FIXTURES / "bybit_linear.html").read_text()
    if invalid == "denied":
        page = (FIXTURES / "bybit_denied.html").read_text()
    elif invalid == "wrong_heading":
        page = page.replace("/trading/BTCUSDT", "/trading/ETHUSDT")
    elif invalid == "unrelated_link":
        page = page.replace('href="BTCUSDT2026-11-01', 'href="https://evil.test/BTCUSDT2026-11-01')
    elif invalid == "traversal":
        page = page.replace('href="BTCUSDT2026-11-01', 'href="../BTCUSDT2026-11-01')
    else:
        page = page.replace("BTCUSDT2026", "ETHUSDT2026")
    async with open_http_client(
        limiter=ImmediateLimiter(1),
        transport=httpx.MockTransport(lambda _: httpx.Response(200, text=page)),
    ) as http:
        with pytest.raises(DiscoveryError):
            _ = [
                a
                async for a in ExchangeSource("bybit", http).discover(
                    selection("bybit", "linear"), interval
                )
            ]


@pytest.mark.asyncio
async def test_bybit_safe_redirect_uses_final_directory_url(interval: AcquisitionInterval) -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        if request.url.path.endswith("/"):
            return httpx.Response(
                302, headers={"Location": "https://public.bybit.com/trading/BTCUSDT"}
            )
        return httpx.Response(200, text=(FIXTURES / "bybit_linear.html").read_text())

    async with open_http_client(
        limiter=ImmediateLimiter(1), transport=httpx.MockTransport(handler)
    ) as http:
        result = [
            a
            async for a in ExchangeSource("bybit", http).discover(
                selection("bybit", "linear"), interval
            )
        ]
    assert len(calls) == 2
    assert len(result) == 3
    assert all(
        a.download_url is not None and "/trading/BTCUSDT/BTCUSDT" in a.download_url for a in result
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("market", ["spot", "linear", "inverse"])
@pytest.mark.parametrize("data_type", ["trades", "order_book_l2"])
async def test_gate_candidate_routes_and_intervals(
    interval: AcquisitionInterval, market: str, data_type: str
) -> None:
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        assert request.method == "HEAD"  # No unfiltered/paginated bucket listing is trusted.
        assert request.url.path.startswith(
            {"spot": "/spot/", "linear": "/futures_usdt/", "inverse": "/futures_btc/"}[market]
        )
        assert ("BTC_USD-" if market == "inverse" else "BTC_USDT-") in request.url.path
        return httpx.Response(200, headers={"Content-Type": "application/gzip"})

    async with open_http_client(
        limiter=ImmediateLimiter(1), transport=httpx.MockTransport(handler)
    ) as http:
        result = [
            a
            async for a in ExchangeSource("gateio", http).discover(
                selection("gateio", market, data_type), interval
            )
        ]
    assert len(calls) == len(result) == (1 if data_type == "trades" else 72)
    assert len({a.identity for a in result}) == len(result)
    assert all(a.request.market_category == market for a in result)
    if data_type == "order_book_l2":
        assert result[0].interval.start == interval.start
        assert result[-1].interval.end == interval.end


@pytest.mark.asyncio
async def test_gate_seventy_distinct_hourly_partitions(interval: AcquisitionInterval) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            404 if any(stamp in request.url.path for stamp in ["2026110114", "2026110217"]) else 200
        )

    async with open_http_client(
        limiter=ImmediateLimiter(1), transport=httpx.MockTransport(handler)
    ) as http:
        result = [
            a
            async for a in ExchangeSource("gateio", http).discover(
                selection("gateio", data_type="order_book_l2"), interval
            )
        ]
    assert len(result) == len({a.identity for a in result}) == 70


@pytest.mark.asyncio
async def test_gate_error_probe_is_not_empty_success(interval: AcquisitionInterval) -> None:
    async with open_http_client(
        limiter=ImmediateLimiter(1),
        transport=httpx.MockTransport(
            lambda _: httpx.Response(200, headers={"Content-Type": "text/html"})
        ),
    ) as http:
        with pytest.raises(DiscoveryError):
            _ = [
                a
                async for a in ExchangeSource("gateio", http).discover(
                    selection("gateio"), interval
                )
            ]


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", ["family", "instrument", "dated_future", "filename"])
async def test_okx_returned_instid_does_not_bypass_scope_validation(
    interval: AcquisitionInterval, invalid: str
) -> None:
    detail: dict[str, object] = {
        "instType": "SWAP",
        "instFamily": "BTC-USDT",
        "instId": "BTC-USDT-SWAP",
        "groupDetails": [
            {
                "filename": "BTC-USDT-SWAP-trades-2026-11-01.zip",
                "dateTs": str(int(datetime(2026, 10, 31, 16, tzinfo=UTC).timestamp() * 1000)),
                "url": "https://archive.test/file.zip",
            }
        ],
    }
    if invalid == "family":
        detail["instFamily"] = "ETH-USDT"
    elif invalid == "instrument":
        detail["instId"] = "ETH-USDT-SWAP"
    elif invalid == "dated_future":
        detail["instId"] = "BTC-USDT-261225"
    else:
        detail["groupDetails"] = [
            {
                "filename": "ETH-USDT-SWAP-trades-2026-11-01.zip",
                "dateTs": "1793462400000",
                "url": "https://archive.test/file.zip",
            }
        ]
    async with open_http_client(
        limiter=ImmediateLimiter(1),
        transport=httpx.MockTransport(
            lambda _: httpx.Response(200, json={"code": "0", "data": [{"details": [detail]}]})
        ),
    ) as http:
        with pytest.raises(DiscoveryError):
            _ = [
                a
                async for a in ExchangeSource("okx", http).discover(
                    selection("okx", "linear"), interval
                )
            ]


@pytest.mark.asyncio
@pytest.mark.parametrize("market", ["spot", "linear", "inverse"])
async def test_okx_two_depth_files_have_exact_day_intervals(
    interval: AcquisitionInterval, market: str
) -> None:
    req = selection("okx", market, "order_book_l2")
    inst = req.scope.identifier + ("" if market == "spot" else "-SWAP")
    files = [
        {
            "filename": f"{inst}-L2orderbook-5000lv-2026-11-0{day}.tar.gz",
            "dateTs": str(int(datetime(2026, 11, day, tzinfo=UTC).timestamp() * 1000)),
            "url": f"https://archive.test/{day}.tar.gz",
        }
        for day in (1, 2)
    ]
    body = {
        "code": "0",
        "data": [
            {
                "details": [
                    {
                        "instFamily": "" if market == "spot" else req.scope.identifier,
                        "instId": inst,
                        "groupDetails": files + files,
                    }
                ]
            }
        ],
    }
    async with open_http_client(
        limiter=ImmediateLimiter(1),
        transport=httpx.MockTransport(lambda _: httpx.Response(200, json=body)),
    ) as http:
        result = [a async for a in ExchangeSource("okx", http).discover(req, interval)]
    assert len(result) == 2
    assert result[0].interval.start == interval.start
    assert result[-1].interval.end == datetime(2026, 11, 3, tzinfo=UTC)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "market,kind,expected",
    [("linear", "trades", 7), ("linear", "depth", 2), ("spot", "trades", 3), ("spot", "depth", 2)],
)
async def test_bitget_preserves_real_partitions_and_deduplicates(
    interval: AcquisitionInterval, market: str, kind: str, expected: int
) -> None:
    body = json.loads((FIXTURES / f"bitget_{market}_{kind}.json").read_text())
    body["data"].append(dict(body["data"][0]))
    async with open_http_client(
        limiter=ImmediateLimiter(1),
        transport=httpx.MockTransport(lambda _: httpx.Response(200, json=body)),
    ) as http:
        result = [
            a
            async for a in ExchangeSource("bitget", http).discover(
                selection("bitget", market, "trades" if kind == "trades" else "order_book_l2"),
                interval,
            )
        ]
    assert len(result) == len({a.identity for a in result}) == expected
    if kind == "trades":
        assert sorted(Counter(a.interval.start for a in result).values()) == (
            [2, 5] if market == "linear" else [1, 2]
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", ["symbol", "filename", "outside"])
async def test_bitget_identity_and_interval_validation(
    interval: AcquisitionInterval, invalid: str
) -> None:
    body = json.loads((FIXTURES / "bitget_linear_trades.json").read_text())
    body["data"] = body["data"][:1]
    if invalid == "symbol":
        body["data"][0]["displayName"] = "ETHUSDT"
    elif invalid == "filename":
        body["data"][0]["fileName"] = "ETHUSDT-ETHUSDT_20261101_001.zip"
    else:
        body["data"][0].update(
            {
                "fileName": "BTCUSDT-BTCUSDT_20261001_001.zip",
                "dateTimeStr": "2026-10-01",
                "dateTime": int(datetime(2026, 9, 30, 16, tzinfo=UTC).timestamp() * 1000),
            }
        )
    async with open_http_client(
        limiter=ImmediateLimiter(1),
        transport=httpx.MockTransport(lambda _: httpx.Response(200, json=body)),
    ) as http:
        if invalid == "outside":
            assert [
                a
                async for a in ExchangeSource("bitget", http).discover(
                    selection("bitget", "linear"), interval
                )
            ] == []
        else:
            with pytest.raises(DiscoveryError):
                _ = [
                    a
                    async for a in ExchangeSource("bitget", http).discover(
                        selection("bitget", "linear"), interval
                    )
                ]


@pytest.mark.asyncio
async def test_numeric_instrument_identifier_cannot_be_misparsed_as_archive_date(
    interval: AcquisitionInterval,
) -> None:
    request = selection("bybit").model_copy(
        update={
            "scope": selection("bybit").scope.model_copy(update={"identifier": "TEST_99999999"})
        }
    )
    page = '<pre><a href=../>../</a><a href="TEST_99999999-2026-11-01.csv.gz">file</a></pre>'
    async with open_http_client(
        limiter=ImmediateLimiter(1),
        transport=httpx.MockTransport(lambda _: httpx.Response(200, text=page)),
    ) as http:
        result = [a async for a in ExchangeSource("bybit", http).discover(request, interval)]
    assert result[0].interval.start == interval.start


@pytest.mark.asyncio
@pytest.mark.parametrize("market", ["spot", "linear", "inverse"])
async def test_binance_trade_market_prefix_regression(
    interval: AcquisitionInterval, market: str
) -> None:
    request = selection("binance", market)
    root = {
        "spot": "data/spot/daily/trades/",
        "linear": "data/futures/um/daily/trades/",
        "inverse": "data/futures/cm/daily/trades/",
    }[market]
    key = (
        root + request.scope.identifier + "/" + request.scope.identifier + "-trades-2026-11-01.zip"
    )

    def handler(item: httpx.Request) -> httpx.Response:
        assert item.url.params["prefix"] == root + request.scope.identifier + "/"
        return httpx.Response(
            200,
            text=f'<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><IsTruncated>false</IsTruncated><Contents><Key>{key}</Key></Contents></ListBucketResult>',
        )

    async with open_http_client(
        limiter=ImmediateLimiter(1), transport=httpx.MockTransport(handler)
    ) as http:
        result = [a async for a in ExchangeSource("binance", http).discover(request, interval)]
    assert len(result) == 1
    assert result[0].request.market_category == market


@pytest.mark.asyncio
async def test_gate_generic_html_404_cannot_establish_empty_availability(
    interval: AcquisitionInterval,
) -> None:
    async with open_http_client(
        limiter=ImmediateLimiter(1),
        transport=httpx.MockTransport(
            lambda _: httpx.Response(404, headers={"Content-Type": "text/html"})
        ),
    ) as http:
        with pytest.raises(DiscoveryError):
            _ = [
                a
                async for a in ExchangeSource("gateio", http).discover(
                    selection("gateio"), interval
                )
            ]


@pytest.mark.asyncio
async def test_bitget_queries_source_calendar_through_exclusive_utc_midnight() -> None:
    interval = AcquisitionInterval(
        start=datetime(2026, 11, 1, tzinfo=UTC), end=datetime(2026, 11, 2, tzinfo=UTC)
    )
    body = json.loads((FIXTURES / "bitget_linear_trades.json").read_text())

    def handler(request: httpx.Request) -> httpx.Response:
        query = json.loads(request.content)
        assert query["beginTimeStr"] == "2026-11-01"
        assert query["endTimeStr"] == "2026-11-02"
        return httpx.Response(200, json=body)

    async with open_http_client(
        limiter=ImmediateLimiter(1), transport=httpx.MockTransport(handler)
    ) as http:
        results = [
            item
            async for item in ExchangeSource("bitget", http).discover(
                selection("bitget", "linear"), interval
            )
        ]
    assert len(results) == 7  # Nov 2 source partitions begin Nov 1 at 16:00 UTC.


@pytest.mark.asyncio
async def test_gate_probes_overlap_with_bounded_workers(interval: AcquisitionInterval) -> None:
    import asyncio

    entered = asyncio.Event()
    release = asyncio.Event()
    active = 0
    peak = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        if active == 4:
            entered.set()
        await release.wait()
        active -= 1
        return httpx.Response(404)

    async with open_http_client(
        limiter=ImmediateLimiter(1), transport=httpx.MockTransport(handler)
    ) as http:

        async def run() -> None:
            _ = [
                a
                async for a in ExchangeSource("gateio", http).discover(
                    selection("gateio", data_type="order_book_l2"), interval
                )
            ]

        task = asyncio.create_task(run())
        try:
            await asyncio.wait_for(entered.wait(), 1)
        finally:
            release.set()
        await task
    assert peak == 4


@pytest.mark.asyncio
@pytest.mark.parametrize("data_type", ["trades", "order_book_l2"])
@pytest.mark.parametrize(
    "invalid",
    [
        None,
        "family",
        "file_family",
        "swap_file",
        "type",
        "missing_type",
        "instid",
        "bundle_instid",
        "data_type",
        "suffix",
        "timestamp",
    ],
)
async def test_okx_dated_futures_family_bundles(data_type: str, invalid: str | None) -> None:
    req = selection("okx", "inverse", data_type).model_copy(update={"instrument_type": "future"})
    start = datetime(2026, 10, 27, tzinfo=UTC)
    interval = AcquisitionInterval(start=start, end=datetime(2026, 10, 28, tzinfo=UTC))
    stamp = datetime(2026, 10, 26, 16, tzinfo=UTC) if data_type == "trades" else start
    suffix = (
        "trades-2026-10-27.zip" if data_type == "trades" else "L2orderbook-5000lv-2026-10-27.tar.gz"
    )
    filename = "BTC-USD-futureschain-" + suffix
    detail = {"instType": "FUTURES", "instFamily": "BTC-USD", "instId": ""}
    if invalid == "family":
        detail["instFamily"] = "ETH-USD"
    elif invalid == "file_family":
        filename = filename.replace("BTC-USD", "ETH-USD")
    elif invalid == "swap_file":
        filename = filename.replace("futureschain", "SWAP")
    elif invalid == "type":
        detail["instType"] = "SWAP"
    elif invalid == "missing_type":
        del detail["instType"]
    elif invalid == "instid":
        detail["instId"] = "BTC-USD-261225"
    elif invalid == "bundle_instid":
        detail["instId"] = "BTC-USD-futureschain"
    elif invalid == "data_type":
        filename = (
            filename.replace("trades", "books")
            if data_type == "trades"
            else filename.replace("L2orderbook", "trades")
        )
    elif invalid == "suffix":
        filename += ".unexpected"
    elif invalid == "timestamp":
        stamp = datetime(2026, 10, 25, tzinfo=UTC)
    body = {
        "code": "0",
        "data": [
            {
                "details": [
                    dict(
                        detail,
                        groupDetails=[
                            {
                                "filename": filename,
                                "dateTs": str(int(stamp.timestamp() * 1000)),
                                "url": "https://archive.test/" + filename,
                            }
                        ],
                    )
                ]
            }
        ],
    }
    async with open_http_client(
        limiter=ImmediateLimiter(1),
        transport=httpx.MockTransport(lambda _: httpx.Response(200, json=body)),
    ) as http:

        async def discover():
            return [a async for a in ExchangeSource("okx", http).discover(req, interval)]

        if invalid:
            with pytest.raises(DiscoveryError):
                await discover()
        else:
            results = await discover()
            assert len(results) == 1
            assert results[0].request.scope == req.scope
            assert results[0].filename == filename
