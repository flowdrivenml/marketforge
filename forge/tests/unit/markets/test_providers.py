"""Native-unit statistics and fail-closed metadata paging."""

from datetime import UTC, datetime
from decimal import Decimal

import httpx
import pytest

from forge.markets.models import Instrument, MarketDataError, number
from forge.markets.providers.public import PublicProvider
from forge.shared.http.client import open_http_client
from forge.shared.http.rate_limit import RequestRateLimiter


class Immediate(RequestRateLimiter):
    async def wait(self) -> None:
        pass


@pytest.mark.asyncio
async def test_units_missing_and_estimates() -> None:
    instrument = Instrument(
        exchange="okx",
        market="linear",
        symbol="BTC-USDT-SWAP",
        base="BTC",
        quote="USDT",
        settlement="USDT",
        quantity_type="contracts",
        contract_value=Decimal(".01"),
        contract_unit="BTC",
    )
    async with open_http_client(
        limiter=Immediate(1), transport=httpx.MockTransport(lambda _: httpx.Response(200))
    ) as client:
        provider = PublicProvider("okx", client)
        result = provider.observation(
            instrument,
            {"last": "100", "vol24h": "200", "volCcy24h": "2"},
            fetched=datetime.now(UTC),
        )
    assert result.turnover == Decimal("200")
    assert result.turnover_unit == "USDT"
    assert result.turnover_estimated
    assert result.funding_rate is None
    assert result.source_observed_at is None
    assert result.window_basis == "fetch_time"


@pytest.mark.asyncio
async def test_bybit_inverse_native_values_are_not_relabelled() -> None:
    instrument = Instrument(
        exchange="bybit", market="inverse", symbol="BTCUSD", base="BTC", quote="USD"
    )
    async with open_http_client(limiter=Immediate(1)) as client:
        result = PublicProvider("bybit", client).observation(
            instrument,
            {
                "lastPrice": "100",
                "volume24h": "500",
                "turnover24h": "5",
                "openInterest": "200",
                "fundingRate": "-0.001",
            },
            fetched=datetime.now(UTC),
        )
    assert result.base_volume == Decimal(5)
    assert result.turnover == Decimal(500) and result.turnover_unit == "USD"
    assert result.open_interest_unit == "USD"
    assert result.funding_rate == Decimal("-0.001")


@pytest.mark.parametrize("value", ["NaN", "Infinity", "-1", True])
def test_nonfinite_and_invalid_quantities_rejected(value: object) -> None:
    with pytest.raises(MarketDataError):
        number(value)
    assert number(None) is None


@pytest.mark.asyncio
async def test_metadata_cursor_failure_never_appears_complete() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(
            200,
            json={
                "retCode": 0,
                "result": {"category": "linear", "list": [], "nextPageCursor": "repeated"},
            },
        )

    async with open_http_client(
        limiter=Immediate(1), transport=httpx.MockTransport(handler)
    ) as client:
        with pytest.raises(MarketDataError, match="Repeated"):
            _ = [
                page
                async for page in PublicProvider("bybit", client).pages("linear", metadata=True)
            ]
    assert calls == 2


@pytest.mark.asyncio
async def test_binance_inverse_uses_contract_status_and_exact_usd_contract_value() -> None:
    async with open_http_client(limiter=Immediate(1)) as client:
        provider = PublicProvider("binance", client)
        instrument = provider.instrument(
            "inverse",
            {
                "symbol": "BTCUSD_PERP",
                "baseAsset": "BTC",
                "quoteAsset": "USD",
                "marginAsset": "BTC",
                "contractType": "PERPETUAL",
                "contractStatus": "TRADING",
                "contractSize": 100,
                "filters": [
                    {"filterType": "PRICE_FILTER", "tickSize": "0.1"},
                    {"filterType": "LOT_SIZE", "stepSize": "1"},
                ],
            },
        )
        assert instrument is not None and instrument.active
        result = provider.observation(
            instrument,
            {"lastPrice": "50000", "volume": "12", "baseVolume": "0.024"},
            fetched=datetime.now(UTC),
        )
    assert result.turnover == Decimal(1200)
    assert result.turnover_unit == "USD"
    assert result.contract_volume == Decimal(12)
    assert not result.turnover_estimated


@pytest.mark.asyncio
async def test_gate_linear_turnover_preserves_usdt_unit() -> None:
    instrument = Instrument(
        exchange="gateio",
        market="linear",
        symbol="BTC_USDT",
        base="BTC",
        quote="USDT",
        settlement="USDT",
        quantity_type="contracts",
        contract_value=Decimal(".0001"),
        contract_unit="BTC",
    )
    async with open_http_client(limiter=Immediate(1)) as client:
        result = PublicProvider("gateio", client).observation(
            instrument,
            {
                "last": "100",
                "volume_24h": "10",
                "volume_24h_base": ".001",
                "volume_24h_quote": ".1",
                "total_size": "3",
                "funding_rate": "0",
            },
            fetched=datetime.now(UTC),
        )
    assert result.turnover_unit == "USDT" and result.open_interest_unit == "contracts"
    assert result.turnover == Decimal(".1")
    assert result.funding_rate == 0


@pytest.mark.asyncio
async def test_bitget_category_and_perpetual_identity_are_explicit() -> None:
    async with open_http_client(limiter=Immediate(1)) as client:
        provider = PublicProvider("bitget", client)
        row: dict[str, object] = {
            "category": "USDT-FUTURES",
            "symbol": "BTCUSDT",
            "baseCoin": "BTC",
            "quoteCoin": "USDT",
            "type": "perpetual",
            "symbolType": "crypto",
            "status": "online",
            "settleCoin": "USDT",
        }
        item = provider.instrument("linear", row)
        assert item is not None and item.reference == "bitget.linear.BTCUSDT"
        future = provider.instrument("linear", {**row, "type": "delivery"})
        assert (
            future is not None and future.instrument_type == "future" and future.expiry_ns is None
        )
        with pytest.raises(MarketDataError, match="contract semantics"):
            provider.instrument("linear", {**row, "type": "unknown"})
        with pytest.raises(MarketDataError, match="market category"):
            provider.instrument("spot", row)
