"""Representative public metadata shapes; no symbol-based contract classification."""

from datetime import UTC, datetime
from decimal import Decimal

import httpx
import pytest

from forge.markets.capabilities import SCOPES, resolve_scopes
from forge.markets.models import MarketDataError
from forge.markets.providers.public import PublicProvider
from forge.shared.http.client import ResponseSizeLimitError, open_http_client
from forge.shared.http.rate_limit import RequestRateLimiter


@pytest.mark.asyncio
async def test_dated_contract_fields_and_perpetual_placeholder_expiry() -> None:
    rows: dict[str, dict[str, object]] = {
        "binance": {
            "symbol": "BTCUSDT_261225",
            "contractType": "CURRENT_QUARTER",
            "status": "TRADING",
            "baseAsset": "BTC",
            "quoteAsset": "USDT",
            "marginAsset": "USDT",
            "onboardDate": 1700000000000,
            "deliveryDate": 1798185600000,
            "filters": [
                {"filterType": "LOT_SIZE", "stepSize": ".001", "minQty": ".001", "maxQty": "10"},
                {"filterType": "PRICE_FILTER", "tickSize": ".1"},
            ],
        },
        "bybit": {
            "symbol": "BTCUSDT-25DEC26",
            "contractType": "LinearFutures",
            "status": "Trading",
            "baseCoin": "BTC",
            "quoteCoin": "USDT",
            "settleCoin": "USDT",
            "launchTime": "1700000000000",
            "deliveryTime": "1798185600000",
            "priceFilter": {"tickSize": ".1"},
            "lotSizeFilter": {"qtyStep": ".001", "minOrderQty": ".001", "maxOrderQty": "10"},
        },
        "okx": {
            "instId": "BTC-USDT-261225",
            "instType": "FUTURES",
            "ctType": "linear",
            "instFamily": "BTC-USDT",
            "settleCcy": "USDT",
            "state": "live",
            "listTime": "1700000000000",
            "expTime": "1798185600000",
            "ctVal": ".01",
            "ctMult": "1",
            "ctValCcy": "BTC",
            "tickSz": ".1",
            "lotSz": "1",
            "minSz": "1",
        },
        "gateio": {
            "name": "BTC_USDT_20261225",
            "underlying": "BTC_USDT",
            "type": "direct",
            "_collection_type": "future",
            "expire_time": 1798185600,
            "create_time": 1700000000,
            "quanto_multiplier": ".0001",
            "order_price_round": ".1",
            "order_size_min": 1,
            "order_size_max": 10,
        },
        "bitget": {
            "symbol": "BTCUSDT_261225",
            "category": "USDT-FUTURES",
            "type": "delivery",
            "status": "online",
            "baseCoin": "BTC",
            "quoteCoin": "USDT",
            "settleCoin": "USDT",
            "launchTime": "1700000000000",
            "deliveryTime": "1798185600000",
            "priceMultiplier": ".1",
            "quantityMultiplier": ".001",
            "minOrderQty": ".001",
            "maxOrderQty": "10",
        },
    }
    async with open_http_client(limiter=RequestRateLimiter(1)) as client:
        for exchange in ("binance", "bybit", "okx", "gateio", "bitget"):
            provider = PublicProvider(exchange, client)
            row = rows[exchange]
            future = provider.instrument("linear", row)
            assert future is not None
            assert future.instrument_type == "future" and future.market == "linear"
            assert future.expiry_ns == 1798185600000000000
            assert future.launch_time_ns == 1700000000000000000
            assert future.tick_size == Decimal(".1")
            assert future.min_qty is not None
            assert (future.max_qty is None) == (exchange == "okx")
            changed = dict(row)
            if exchange == "binance":
                changed["contractType"] = "PERPETUAL"
            elif exchange == "bybit":
                changed["contractType"] = "LinearPerpetual"
            elif exchange == "okx":
                changed.update(instType="SWAP", instId="BTC-USDT-SWAP")
            elif exchange == "gateio":
                changed.update(_collection_type="perpetual", name="BTC_USDT")
            else:
                changed["type"] = "perpetual"
            perpetual = provider.instrument("linear", changed)
            assert perpetual is not None and perpetual.instrument_type == "perpetual"
            assert perpetual.expiry_ns is None
            assert provider.instrument("linear", {**row, "instType": "OPTION"}) is None


def test_scope_resolution_is_shared_and_filters_are_optional() -> None:
    assert len(resolve_scopes()) == 14
    assert resolve_scopes() == SCOPES
    assert resolve_scopes(["bybit"]) == (
        ("bybit", "spot"),
        ("bybit", "linear"),
        ("bybit", "inverse"),
    )
    assert len(resolve_scopes(markets=["linear"])) == 5
    assert resolve_scopes(["bybit"], ["linear"]) == (("bybit", "linear"),)
    with pytest.raises(MarketDataError, match="No supported"):
        resolve_scopes(["bitget"], ["inverse"])


@pytest.mark.asyncio
async def test_okx_collects_swap_then_futures_and_never_hides_second_endpoint_failure() -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        kind = request.url.params["instType"]
        calls.append(kind)
        return httpx.Response(200, json={"code": "0" if kind == "SWAP" else "500", "data": []})

    async def immediate(self: RequestRateLimiter) -> None:
        pass

    from unittest.mock import patch

    with patch.object(RequestRateLimiter, "wait", immediate):
        async with open_http_client(
            limiter=RequestRateLimiter(1), transport=httpx.MockTransport(handler)
        ) as client:
            with pytest.raises(MarketDataError, match="OKX rejected"):
                _ = [
                    page
                    async for page in PublicProvider("okx", client).pages("inverse", metadata=True)
                ]
    assert calls == ["SWAP", "FUTURES"]


@pytest.mark.asyncio
async def test_metadata_limit_is_32_mib_and_error_is_actionable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from forge.shared.http.client import HttpClient

    async def oversized(
        self: HttpClient, url: str, *, params: object = None, max_response_bytes: int | None = None
    ) -> httpx.Response:
        assert max_response_bytes == 32 * 1024 * 1024
        raise ResponseSizeLimitError(
            max_response_bytes, httpx.Request("GET", "https://example.test/?secret=private")
        )

    monkeypatch.setattr(HttpClient, "get", oversized)
    async with open_http_client(limiter=RequestRateLimiter(1)) as client:
        with pytest.raises(MarketDataError, match="33,554,432 bytes") as caught:
            await PublicProvider("binance", client).get("https://example.test")
    assert "secret" not in str(caught.value) and "private" not in str(caught.value)


@pytest.mark.asyncio
async def test_future_statistics_native_turnover_is_not_multiplied_again() -> None:
    from forge.markets.models import Instrument

    item = Instrument(
        exchange="gateio",
        market="linear",
        instrument_type="future",
        symbol="BTC_USDT_20261225",
        base="BTC",
        quote="USDT",
        quantity_type="contracts",
        contract_value=Decimal(".01"),
        contract_unit="BTC",
    )
    async with open_http_client(limiter=RequestRateLimiter(1)) as client:
        observation = PublicProvider("gateio", client).observation(
            item,
            {"volume_24h_quote": "123.456789123456789", "funding_rate": "0.01"},
            fetched=datetime.now(UTC),
        )
    assert observation.turnover == Decimal("123.456789123456789")
    assert observation.funding_rate is None


@pytest.mark.asyncio
async def test_gate_contract_pagination_and_delivery_route(monkeypatch: pytest.MonkeyPatch) -> None:
    offsets: list[str] = []
    paths: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.path)
        if "/delivery/" in request.url.path:
            return httpx.Response(200, json=[])
        offset = request.url.params["offset"]
        offsets.append(offset)
        start = int(offset)
        assert request.url.params["limit"] == "100"
        count = min(100, 205 - start)
        return httpx.Response(
            200,
            json=[
                {
                    "name": f"T{i}_USDT",
                    "type": "direct",
                    "quanto_multiplier": ".01",
                    "order_price_round": ".1",
                }
                for i in range(start, start + count)
            ],
        )

    async def immediate(self: RequestRateLimiter) -> None:
        pass

    monkeypatch.setattr(RequestRateLimiter, "wait", immediate)
    async with open_http_client(
        limiter=RequestRateLimiter(1), transport=httpx.MockTransport(handler)
    ) as client:
        pages = [p async for p in PublicProvider("gateio", client).pages("linear", metadata=True)]
    assert [len(p) for p in pages] == [100, 100, 5, 0]
    assert {str(r["name"]) for page in pages for r in page} == {f"T{i}_USDT" for i in range(205)}
    assert offsets == ["0", "100", "200"]
    assert paths[-1] == "/api/v4/delivery/usdt/contracts"


@pytest.mark.asyncio
async def test_live_observed_contract_variants_preserve_categories_and_units() -> None:
    async with open_http_client(limiter=RequestRateLimiter(1)) as client:
        binance = PublicProvider("binance", client).instrument(
            "linear",
            {
                "symbol": "XAUUSDT",
                "contractType": "TRADIFI_PERPETUAL",
                "baseAsset": "XAU",
                "quoteAsset": "USDT",
                "marginAsset": "USDT",
                "status": "TRADING",
                "deliveryDate": 4133404800000,
                "filters": [],
            },
        )
        assert (
            binance is not None
            and binance.instrument_type == "perpetual"
            and binance.expiry_ns is None
        )
        assert (
            PublicProvider("bybit", client).instrument("linear", {"contractType": "InverseFutures"})
            is None
        )
        assert (
            PublicProvider("bybit", client).instrument(
                "inverse", {"contractType": "LinearPerpetual"}
            )
            is None
        )
        row: dict[str, object] = {
            "instId": "BTC-USD_UM-261225",
            "instType": "FUTURES",
            "ctType": "linear",
            "instFamily": "BTC-USD_UM",
            "uly": "BTC-USD",
            "settleCcy": "USD",
            "state": "live",
            "ctVal": ".01",
            "ctMult": "1",
            "ctValCcy": "BTC",
        }
        provider = PublicProvider("okx", client)
        item = provider.instrument("linear", row)
        assert item is not None and item.quote == "USD" and item.symbol == row["instId"]
        legacy = provider.instrument(
            "linear",
            {**row, "instId": "BTC-USDC-261225", "instFamily": "BTC-USDC", "settleCcy": "USDC"},
        )
        assert legacy is not None and legacy.quote == "USDC"
        with pytest.raises(MarketDataError, match="concrete contract"):
            provider.instrument("linear", {**row, "instId": "BTC-USD-futureschain"})


@pytest.mark.asyncio
async def test_inverse_dated_contract_types_and_missing_specifications() -> None:
    async with open_http_client(limiter=RequestRateLimiter(1)) as client:
        binance = PublicProvider("binance", client).instrument(
            "inverse",
            {
                "symbol": "BTCUSD_261225",
                "contractType": "NEXT_QUARTER",
                "baseAsset": "BTC",
                "quoteAsset": "USD",
                "marginAsset": "BTC",
                "contractStatus": "TRADING",
                "filters": [],
                "deliveryDate": 1798185600000,
            },
        )
        bybit = PublicProvider("bybit", client).instrument(
            "inverse",
            {
                "symbol": "BTCUSDH27",
                "contractType": "InverseFutures",
                "baseCoin": "BTC",
                "quoteCoin": "USD",
                "settleCoin": "BTC",
                "status": "Trading",
                "priceFilter": {},
                "lotSizeFilter": {},
                "deliveryTime": "1806048000000",
            },
        )
        okx = PublicProvider("okx", client).instrument(
            "inverse",
            {
                "instId": "BTC-USD-261225",
                "instFamily": "BTC-USD",
                "instType": "FUTURES",
                "ctType": "inverse",
                "ctVal": "100",
                "ctValCcy": "USD",
                "ctMult": "1",
                "settleCcy": "BTC",
                "state": "live",
                "expTime": "1798185600000",
            },
        )
        for item in (binance, bybit, okx):
            assert (
                item is not None and item.instrument_type == "future" and item.market == "inverse"
            )
            assert item.expiry_ns is not None and item.settlement == "BTC" and item.quote == "USD"
        assert binance is not None and binance.contract_value is None
        assert bybit is not None and bybit.quantity_type == "quote"
        assert okx is not None and okx.contract_value == Decimal(100) and okx.contract_unit == "USD"


@pytest.mark.asyncio
async def test_unknown_contract_value_denomination_never_becomes_monetary_turnover() -> None:
    from forge.markets.models import Instrument

    async with open_http_client(limiter=RequestRateLimiter(1)) as client:
        for exchange in ("binance", "okx"):
            item = Instrument(
                exchange=exchange,
                market="inverse",
                instrument_type="future",
                symbol="CONTRACT",
                base="BTC",
                quote="USD",
                settlement="BTC",
                quantity_type="contracts",
                contract_value=Decimal(100),
                contract_unit="BTC",
            )
            row: dict[str, object] = {"volume": "10", "vol24h": "10"}
            result = PublicProvider(exchange, client).observation(
                item, row, fetched=datetime.now(UTC)
            )
            assert result.turnover is None and result.turnover_unit is None
            row.update(quoteVolume="123.456", volCcyQuote24h="123.456")
            result = PublicProvider(exchange, client).observation(
                item, row, fetched=datetime.now(UTC)
            )
            if exchange == "binance":
                assert result.turnover == Decimal("123.456") and result.turnover_unit == "USD"
            else:
                # OKX tickers do not document those quote-volume fields; do not infer units from an unknown field.
                assert result.turnover is None and result.turnover_unit is None
