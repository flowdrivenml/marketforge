"""Exchange fixture parsers; these tests make no live availability claims."""

from datetime import UTC, datetime

import httpx
import pytest

from forge.historical.archives.config import AcquisitionInterval, ArchiveRequest
from forge.historical.archives.sources.base import DiscoveryError
from forge.historical.archives.sources.exchanges import ExchangeSource
from forge.shared.http.client import open_http_client
from forge.shared.http.rate_limit import RequestRateLimiter


class ImmediateLimiter(RequestRateLimiter):
    async def wait(self) -> None:
        pass


@pytest.fixture
def interval() -> AcquisitionInterval:
    return AcquisitionInterval(
        start=datetime(2026, 9, 1, tzinfo=UTC), end=datetime(2026, 9, 2, tzinfo=UTC)
    )


def request(
    exchange: str,
    *,
    kind: str = "instrument",
    symbol: str = "BTCUSDT",
    instrument_type: str = "spot",
    category: str = "spot",
    data_type: str = "trades",
) -> ArchiveRequest:
    return ArchiveRequest.model_validate(
        {
            "exchange": exchange,
            "instrument_type": instrument_type,
            "market_category": category,
            "data_type": data_type,
            "scope": {"kind": kind, "identifier": symbol},
        }
    )


def s3(*, truncated: bool = False, marker: str = "", key: str | None = None) -> str:
    contents = f"<Contents><Key>{key}</Key><Size>100</Size></Contents>" if key else ""
    return f'<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><IsTruncated>{str(truncated).lower()}</IsTruncated><NextMarker>{marker}</NextMarker>{contents}</ListBucketResult>'


@pytest.mark.asyncio
async def test_binance_paginated_listing(interval: AcquisitionInterval) -> None:
    calls: list[httpx.Request] = []
    key = "data/spot/daily/trades/BTCUSDT/BTCUSDT-trades-2026-09-01.zip"

    def handler(item: httpx.Request) -> httpx.Response:
        calls.append(item)
        return httpx.Response(
            200,
            text=(s3(truncated=True, marker="next", key=key) if len(calls) == 1 else s3()),
        )

    async with open_http_client(
        limiter=ImmediateLimiter(1), transport=httpx.MockTransport(handler)
    ) as http:
        result = [
            item
            async for item in ExchangeSource("binance", http).discover(request("binance"), interval)
        ]
    assert len(result) == 1
    assert result[0].source_object_key == key
    assert calls[1].url.params["marker"] == "next"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "body",
    [
        "<html>denied</html>",
        "<Error><Code>AccessDenied</Code></Error>",
        "<ListBucketResult/>",
    ],
)
async def test_binance_malformed_listing_fails(interval: AcquisitionInterval, body: str) -> None:
    async with open_http_client(
        limiter=ImmediateLimiter(1),
        transport=httpx.MockTransport(lambda _: httpx.Response(200, text=body)),
    ) as http:
        with pytest.raises(DiscoveryError):
            _ = [
                item
                async for item in ExchangeSource("binance", http).discover(
                    request("binance"), interval
                )
            ]


@pytest.mark.asyncio
async def test_repeated_s3_cursor_is_failure(interval: AcquisitionInterval) -> None:
    async with open_http_client(
        limiter=ImmediateLimiter(1),
        transport=httpx.MockTransport(
            lambda _: httpx.Response(200, text=s3(truncated=True, marker="same"))
        ),
    ) as http:
        with pytest.raises(DiscoveryError, match="Repeated"):
            _ = [
                item
                async for item in ExchangeSource("binance", http).discover(
                    request("binance"), interval
                )
            ]


@pytest.mark.asyncio
async def test_bybit_directory_accepts_delisted_instrument(
    interval: AcquisitionInterval,
) -> None:
    def handler(item: httpx.Request) -> httpx.Response:
        assert item.url.host == "public.bybit.com"
        return httpx.Response(
            200,
            text='<pre><a href=../>../</a><a href="BTCUSDT2026-09-01.csv.gz">archive</a><a href="BTCUSDT2026-09.csv.gz">monthly</a></pre>',
        )

    async with open_http_client(
        limiter=ImmediateLimiter(1), transport=httpx.MockTransport(handler)
    ) as http:
        result = [
            item
            async for item in ExchangeSource("bybit", http).discover(request("bybit"), interval)
        ]
    assert len(result) == 1


@pytest.mark.asyncio
async def test_bybit_option_windows_and_scope(interval: AcquisitionInterval) -> None:
    def handler(item: httpx.Request) -> httpx.Response:
        assert item.url.params["symbols"] == "BTC"
        return httpx.Response(
            200,
            json={
                "retCode": 0,
                "result": {
                    "files": [
                        {
                            "filename": "BTC-2026-09-01.csv.gz",
                            "url": "https://download.test/option.csv.gz?signature=secret",
                        }
                    ]
                },
            },
        )

    selection = request(
        "bybit",
        kind="base_coin",
        symbol="BTC",
        instrument_type="option",
        category="option",
    )
    async with open_http_client(
        limiter=ImmediateLimiter(1), transport=httpx.MockTransport(handler)
    ) as http:
        result = [
            item async for item in ExchangeSource("bybit", http).discover(selection, interval)
        ]
    assert result[0].request.scope.kind == "base_coin"
    assert "secret" not in result[0].source_object_key


@pytest.mark.asyncio
async def test_bitget_readonly_post_and_shape(interval: AcquisitionInterval) -> None:
    calls: list[httpx.Request] = []

    def handler(item: httpx.Request) -> httpx.Response:
        calls.append(item)
        assert item.method == "POST"
        if item.url.path.endswith("getSymbolList"):
            return httpx.Response(200, json={"code": "200", "data": ["BTC/USDT"]})
        assert b"BTC/USDT" in item.content
        return httpx.Response(
            200,
            json={
                "code": "200",
                "data": [
                    {
                        "fileName": "BTC/USDT-BTCUSDT_20260901_001.zip",
                        "fileUrl": "https://download.test/file.zip",
                        "dateTimeStr": "2026-09-01",
                    }
                ],
            },
        )

    async with open_http_client(
        limiter=ImmediateLimiter(1), transport=httpx.MockTransport(handler)
    ) as http:
        result = [
            item
            async for item in ExchangeSource("bitget", http).discover(request("bitget"), interval)
        ]
    assert len(calls) == 2
    assert len(result) == 1


@pytest.mark.asyncio
async def test_okx_family_results_have_concrete_scope(
    interval: AcquisitionInterval,
) -> None:
    def handler(item: httpx.Request) -> httpx.Response:
        assert item.url.params["instFamilyList"] == "BTC-USDT"
        assert item.url.params["module"] == "5"
        return httpx.Response(
            200,
            json={
                "code": "0",
                "data": [
                    {
                        "details": [
                            {
                                "instFamily": "BTC-USDT",
                                "instId": "BTC-USDT-SWAP",
                                "groupDetails": [
                                    {
                                        "filename": "BTC-USDT-SWAP-books-2026-09-01.zip",
                                        "url": "https://download.test/file.zip",
                                        "dateTs": "1788220800000",
                                    }
                                ],
                            }
                        ]
                    }
                ],
            },
        )

    selection = request(
        "okx",
        kind="family",
        symbol="BTC-USDT",
        instrument_type="perpetual",
        category="linear",
        data_type="order_book_l2",
    )
    async with open_http_client(
        limiter=ImmediateLimiter(1), transport=httpx.MockTransport(handler)
    ) as http:
        result = [item async for item in ExchangeSource("okx", http).discover(selection, interval)]
    assert len(result) == 1
    assert result[0].request.scope.identifier == "BTC-USDT-SWAP"
    assert result[0].request.scope.kind == "instrument"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "exchange,body",
    [
        ("bybit", {"retCode": 10001, "result": []}),
        ("bitget", {"code": "200", "data": {}}),
        ("okx", {"code": "0", "data": [{"invalid": True}]}),
    ],
)
async def test_bad_response_is_not_empty_coverage(
    interval: AcquisitionInterval, exchange: str, body: dict[str, object]
) -> None:
    selection = request(exchange)
    if exchange == "bybit":
        selection = request(
            exchange,
            kind="base_coin",
            symbol="BTC",
            instrument_type="option",
            category="option",
        )
    async with open_http_client(
        limiter=ImmediateLimiter(1),
        transport=httpx.MockTransport(lambda _: httpx.Response(200, json=body)),
    ) as http:
        with pytest.raises(DiscoveryError):
            _ = [
                item async for item in ExchangeSource(exchange, http).discover(selection, interval)
            ]


@pytest.mark.asyncio
async def test_gateio_only_probes_existence(interval: AcquisitionInterval) -> None:
    calls: list[httpx.Request] = []

    def handler(item: httpx.Request) -> httpx.Response:
        calls.append(item)
        return httpx.Response(200)

    async with open_http_client(
        limiter=ImmediateLimiter(1), transport=httpx.MockTransport(handler)
    ) as http:
        result = [
            item
            async for item in ExchangeSource("gateio", http).discover(
                request("gateio", symbol="BTC_USDT"), interval
            )
        ]
    assert len(result) == 1
    assert calls[0].method == "HEAD"
    assert calls[0].url.path == "/spot/deals/202609/BTC_USDT-202609.csv.gz"


def test_signed_url_expiration_is_metadata_not_identity() -> None:
    from forge.historical.archives.sources.base import archive

    start = datetime(2026, 9, 1, tzinfo=UTC)
    first = archive(
        request("bybit"),
        "file.csv.gz",
        "https://download.test/file.csv.gz?X-Amz-Date=20260901T000000Z&X-Amz-Expires=60&X-Amz-Signature=private",
        start,
    )
    second = archive(
        request("bybit"),
        "file.csv.gz",
        "https://other.test/file.csv.gz?Expires=1788220920&Signature=other",
        start,
    )
    assert first.identity == second.identity
    assert first.url_expires_at == datetime(2026, 9, 1, 0, 1, tzinfo=UTC)


@pytest.mark.asyncio
async def test_bybit_windows_bound_calendar_days_for_partial_days() -> None:
    from datetime import timedelta

    calls = 0

    def handler(item: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        begin = datetime.fromisoformat(item.url.params["startDay"])
        end = datetime.fromisoformat(item.url.params["endDay"])
        assert end - begin <= timedelta(days=6)
        return httpx.Response(200, json={"retCode": 0, "result": {"files": []}})

    selection = request(
        "bybit",
        kind="base_coin",
        symbol="BTC",
        instrument_type="option",
        category="option",
    )
    interval = AcquisitionInterval(
        start=datetime(2026, 9, 1, 12, tzinfo=UTC),
        end=datetime(2026, 9, 16, 12, tzinfo=UTC),
    )
    async with open_http_client(
        limiter=ImmediateLimiter(1), transport=httpx.MockTransport(handler)
    ) as http:
        result = [
            item async for item in ExchangeSource("bybit", http).discover(selection, interval)
        ]
    assert result == []
    assert calls == 3


@pytest.mark.asyncio
async def test_gate_candidates_use_utc_calendar() -> None:
    from datetime import timedelta, timezone

    def handler(item: httpx.Request) -> httpx.Response:
        assert "/202608/" in item.url.path
        return httpx.Response(404)

    interval = AcquisitionInterval(
        start=datetime(2026, 9, 1, tzinfo=timezone(timedelta(hours=2))),
        end=datetime(2026, 9, 1, 2, tzinfo=timezone(timedelta(hours=2))),
    )
    async with open_http_client(
        limiter=ImmediateLimiter(1), transport=httpx.MockTransport(handler)
    ) as http:
        assert [
            item
            async for item in ExchangeSource("gateio", http).discover(
                request("gateio", symbol="BTC_USDT"), interval
            )
        ] == []


@pytest.mark.asyncio
async def test_bybit_error_page_with_pre_is_not_empty_listing(
    interval: AcquisitionInterval,
) -> None:
    async with open_http_client(
        limiter=ImmediateLimiter(1),
        transport=httpx.MockTransport(
            lambda _: httpx.Response(200, text="<pre>Access denied</pre>")
        ),
    ) as http:
        with pytest.raises(DiscoveryError):
            _ = [
                item
                async for item in ExchangeSource("bybit", http).discover(request("bybit"), interval)
            ]


@pytest.mark.asyncio
async def test_bybit_unordered_list_directory(
    interval: AcquisitionInterval,
) -> None:
    page = """
    <!DOCTYPE HTML>
    <html>
    <head><title>Directory listing for /spot/BTCUSDT/</title></head>
    <body>
    <h1>Directory listing for /spot/BTCUSDT/</h1>
    <ul>
        <li><a href="BTCUSDT-2026-09.csv.gz">monthly</a></li>
        <li><a href="BTCUSDT_2026-09-01.csv.gz">daily</a></li>
    </ul>
    </body>
    </html>
    """

    async with open_http_client(
        limiter=ImmediateLimiter(1),
        transport=httpx.MockTransport(lambda _: httpx.Response(200, text=page)),
    ) as http:
        result = [
            item
            async for item in ExchangeSource("bybit", http).discover(
                request("bybit"),
                interval,
            )
        ]

    assert len(result) == 1
    assert result[0].filename == "BTCUSDT_2026-09-01.csv.gz"
