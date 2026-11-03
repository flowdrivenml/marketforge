import pytest

from marketforge.acquisition.sources.bybit import BybitSource
from marketforge.models import DataType, InstrumentType, MarketCategory


@pytest.fixture(scope="module")
def source():
    return BybitSource()


VALID_COMBINATIONS = [
    # Spot
    (
        InstrumentType.SPOT,
        MarketCategory.SPOT,
        DataType.TRADE_TICKS,
    ),
    (
        InstrumentType.SPOT,
        MarketCategory.SPOT,
        DataType.ORDER_BOOK_L2,
    ),
    # Linear perpetuals
    (
        InstrumentType.PERPETUAL,
        MarketCategory.LINEAR,
        DataType.TRADE_TICKS,
    ),
    (
        InstrumentType.PERPETUAL,
        MarketCategory.LINEAR,
        DataType.ORDER_BOOK_L2,
    ),
    # Linear futures
    (
        InstrumentType.FUTURE,
        MarketCategory.LINEAR,
        DataType.TRADE_TICKS,
    ),
    (
        InstrumentType.FUTURE,
        MarketCategory.LINEAR,
        DataType.ORDER_BOOK_L2,
    ),
    # Inverse perpetuals
    (
        InstrumentType.PERPETUAL,
        MarketCategory.INVERSE,
        DataType.TRADE_TICKS,
    ),
    (
        InstrumentType.PERPETUAL,
        MarketCategory.INVERSE,
        DataType.ORDER_BOOK_L2,
    ),
    # Inverse futures
    (
        InstrumentType.FUTURE,
        MarketCategory.INVERSE,
        DataType.TRADE_TICKS,
    ),
    (
        InstrumentType.FUTURE,
        MarketCategory.INVERSE,
        DataType.ORDER_BOOK_L2,
    ),
    # Options
    (
        InstrumentType.OPTION,
        MarketCategory.OPTION,
        DataType.TRADE_TICKS,
    ),
    (
        InstrumentType.OPTION,
        MarketCategory.OPTION,
        DataType.ORDER_BOOK_L2,
    ),
]


@pytest.mark.parametrize(
    ("instrument_type", "market_category", "data_type"),
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
    print(f"Instrument Type : {instrument_type.value}")
    print(f"Market Category : {market_category.value}")
    print(f"Data Type       : {data_type.value}")
    print(f"Total           : {len(instruments)}")
    print("-" * 80)

    print("Sample:")
    for instrument in instruments[:10]:
        print(
            f"  {instrument.symbol:<25} "
            f"type={instrument.instrument_type.value:<12} "
            f"category={instrument.market_category.value}"
        )

    assert instruments, (
        f"No instruments returned for "
        f"{instrument_type=}, "
        f"{market_category=}, "
        f"{data_type=}"
    )

    for instrument in instruments:
        assert instrument.instrument_type == instrument_type
        assert instrument.market_category == market_category
        assert instrument.symbol
        assert instrument.exchange.value == "bybit"


@pytest.mark.parametrize(
    "data_type",
    [
        DataType.TRADE_TICKS,
        DataType.ORDER_BOOK_L2,
    ],
)
def test_option_base_coins(
    source,
    data_type,
):
    base_coins = source.base_coins(
        instrument_type=InstrumentType.OPTION,
        data_type=data_type,
    )

    print("\n")
    print("=" * 80)
    print("OPTION BASE COINS")
    print(f"Data Type : {data_type.value}")
    print(f"Total     : {len(base_coins)}")
    print("-" * 80)

    for base_coin in base_coins:
        print(f"  {base_coin}")

    assert base_coins
    assert all(isinstance(base_coin, str) for base_coin in base_coins)
    assert all(base_coin for base_coin in base_coins)

    # Base coins should be unique.
    assert len(base_coins) == len(set(base_coins))
