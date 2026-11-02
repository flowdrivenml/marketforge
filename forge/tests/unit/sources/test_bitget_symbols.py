from unittest.mock import Mock

import pytest

from marketforge.acquisition.sources.bitget import BitgetSource
from marketforge.models import (
    DataType,
    Exchange,
    Instrument,
    InstrumentType,
    MarketCategory,
)


@pytest.fixture
def source():
    return BitgetSource()


def spot(symbol: str) -> Instrument:
    return Instrument(
        exchange=Exchange.BITGET,
        instrument_type=InstrumentType.SPOT,
        market_category=MarketCategory.SPOT,
        symbol=symbol,
    )


@pytest.mark.parametrize(
    ("official_symbol", "historical_symbol"),
    [
        ("BTCUSDT", "BTC/USDT"),
        ("BTCUSDC", "BTC/USDC"),
        ("ETHBTC", "ETH/BTC"),
        ("BTCUSD1", "BTC/USD1"),
        ("PUMPBTCUSDT", "PUMPBTC/USDT"),
        ("BGBTCUSDT", "BGBTC/USDT"),
    ],
)
def test_spot_historical_symbol_mapping(
    source,
    official_symbol,
    historical_symbol,
):
    source._historical_symbols = Mock(
        return_value=[
            historical_symbol,
        ]
    )

    result = source._historical_display_symbol(
        spot(official_symbol),
        DataType.TRADE_TICKS,
    )

    assert result == historical_symbol


def test_historical_symbol_mapping_uses_cache(
    source,
):
    source._historical_symbols = Mock(
        return_value=[
            "BTC/USDT",
        ]
    )

    instrument = spot("BTCUSDT")

    first = source._historical_display_symbol(
        instrument,
        DataType.TRADE_TICKS,
    )

    second = source._historical_display_symbol(
        instrument,
        DataType.TRADE_TICKS,
    )

    assert first == "BTC/USDT"
    assert second == "BTC/USDT"

    source._historical_symbols.assert_called_once()


def test_missing_historical_symbol_raises(
    source,
):
    source._historical_symbols = Mock(return_value=[])

    with pytest.raises(
        ValueError,
        match="No Bitget historical symbol mapping",
    ):
        source._historical_display_symbol(
            spot("BTCUSDT"),
            DataType.TRADE_TICKS,
        )


def test_ambiguous_historical_symbol_raises(
    source,
):
    source._historical_symbols = Mock(
        return_value=[
            "AB/CD",
            "A/BCD",
        ]
    )

    with pytest.raises(
        ValueError,
        match="Ambiguous",
    ):
        source._historical_display_symbol(
            spot("ABCD"),
            DataType.TRADE_TICKS,
        )


def test_futures_do_not_require_mapping(
    source,
):
    instrument = Instrument(
        exchange=Exchange.BITGET,
        instrument_type=InstrumentType.PERPETUAL,
        market_category=MarketCategory.LINEAR,
        symbol="BTCUSDT",
    )

    source._historical_symbols = Mock()

    result = source._historical_display_symbol(
        instrument,
        DataType.TRADE_TICKS,
    )

    assert result == "BTCUSDT"
    source._historical_symbols.assert_not_called()
