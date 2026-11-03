import pytest

from marketforge.acquisition.sources.binance import BinanceSource
from marketforge.models import DataType, InstrumentType, MarketCategory


@pytest.fixture(scope="module")
def source():
    return BinanceSource()


VALID_COMBINATIONS = [
    (
        InstrumentType.SPOT,
        MarketCategory.SPOT,
        DataType.TRADE_TICKS,
    ),
    (
        InstrumentType.PERPETUAL,
        MarketCategory.LINEAR,
        DataType.TRADE_TICKS,
    ),
    (
        InstrumentType.FUTURE,
        MarketCategory.LINEAR,
        DataType.TRADE_TICKS,
    ),
    (
        InstrumentType.PERPETUAL,
        MarketCategory.INVERSE,
        DataType.TRADE_TICKS,
    ),
    (
        InstrumentType.FUTURE,
        MarketCategory.INVERSE,
        DataType.TRADE_TICKS,
    ),
]


@pytest.mark.parametrize(
    (
        "instrument_type",
        "market_category",
        "data_type",
    ),
    VALID_COMBINATIONS,
)
def test_instruments_parameter_grid(
    source,
    instrument_type,
    market_category,
    data_type,
):
    instruments = source.instruments(
        instrument_type=instrument_type,
        market_category=market_category,
        data_type=data_type,
    )

    print("\n")
    print("=" * 80)
    print(f"Instrument Type : " f"{instrument_type.value}")
    print(f"Market Category : " f"{market_category.value}")
    print(f"Data Type       : " f"{data_type.value}")
    print(f"Total           : " f"{len(instruments)}")
    print("-" * 80)

    print("Sample:")

    for instrument in instruments[:10]:
        print(
            f"  {instrument.symbol:<30} "
            f"type="
            f"{instrument.instrument_type.value:<12} "
            f"category="
            f"{instrument.market_category.value}"
        )

    assert instruments

    for instrument in instruments:
        assert instrument.instrument_type == instrument_type

        assert instrument.market_category == market_category

        assert instrument.exchange.value == "binance"
        assert instrument.symbol


def test_binance_l2_is_unsupported(source):
    assert not source.supports(
        InstrumentType.SPOT,
        MarketCategory.SPOT,
        DataType.ORDER_BOOK_L2,
    )

    assert not source.supports(
        InstrumentType.PERPETUAL,
        MarketCategory.LINEAR,
        DataType.ORDER_BOOK_L2,
    )


def test_binance_options_are_unsupported(source):
    assert not source.supports(
        InstrumentType.OPTION,
        MarketCategory.OPTION,
        DataType.TRADE_TICKS,
    )
