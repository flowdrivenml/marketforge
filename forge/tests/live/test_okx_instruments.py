import pytest

from marketforge.acquisition.sources.okx import OKXSource
from marketforge.models import DataType, Exchange, InstrumentType, MarketCategory


@pytest.fixture(scope="module")
def source():
    return OKXSource()


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
    print("=" * 90)
    print("OKX INSTRUMENT DISCOVERY")
    print(f"Instrument Type : {instrument_type.value}")
    print(f"Market Category : {market_category.value}")
    print(f"Data Type       : {data_type.value}")
    print(f"Total           : {len(instruments)}")
    print("-" * 90)

    print("Sample:")

    for instrument in instruments[:10]:
        print(
            f"  {instrument.symbol:<35} "
            f"family={str(instrument.family):<18} "
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
        assert instrument.exchange == Exchange.OKX
        assert instrument.instrument_type == instrument_type
        assert instrument.market_category == market_category
        assert instrument.symbol

        # Non-spot OKX historical requests use instFamilyList.
        if instrument_type != InstrumentType.SPOT:
            assert instrument.family


def test_all_instruments(source):
    """Inspect the complete OKX instrument universe."""

    instruments = source.instruments()

    print("\n")
    print("=" * 90)
    print("OKX COMPLETE INSTRUMENT UNIVERSE")
    print(f"Total : {len(instruments)}")
    print("-" * 90)

    counts = {}

    for instrument in instruments:
        key = (
            instrument.instrument_type.value,
            instrument.market_category.value,
        )

        counts[key] = counts.get(key, 0) + 1

    for (
        instrument_type,
        market_category,
    ), count in sorted(counts.items()):
        print(f"{instrument_type:<12} " f"{market_category:<10} " f"{count:>6}")

    assert instruments


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
    print("=" * 90)
    print("OKX OPTION BASE COINS")
    print(f"Data Type : {data_type.value}")
    print(f"Total     : {len(base_coins)}")
    print("-" * 90)

    for base_coin in base_coins:
        print(f"  {base_coin}")

    assert base_coins

    assert all(isinstance(base_coin, str) for base_coin in base_coins)

    assert len(base_coins) == len(set(base_coins))


def test_unsupported_combinations(source):
    """Verify obviously invalid MarketForge combinations."""

    assert not source.supports(
        InstrumentType.SPOT,
        MarketCategory.LINEAR,
        DataType.TRADE_TICKS,
    )

    assert not source.supports(
        InstrumentType.PERPETUAL,
        MarketCategory.SPOT,
        DataType.TRADE_TICKS,
    )

    assert not source.supports(
        InstrumentType.OPTION,
        MarketCategory.LINEAR,
        DataType.TRADE_TICKS,
    )


def test_option_families(source):
    families = source._option_families()

    print("\n")
    print("=" * 90)
    print("OKX OPTION FAMILIES")
    print(f"Total : {len(families)}")
    print("-" * 90)

    for family in families:
        print(f"  {family}")

    assert families
    assert len(families) == len(set(families))

    for family in families:
        assert isinstance(family, str)
        assert family


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
    print("=" * 90)
    print("OKX OPTION BASE COINS")
    print(f"Data Type : {data_type.value}")
    print(f"Total     : {len(base_coins)}")
    print("-" * 90)

    for base_coin in base_coins:
        print(f"  {base_coin}")

    assert base_coins
    assert len(base_coins) == len(set(base_coins))

    for base_coin in base_coins:
        assert isinstance(base_coin, str)
        assert base_coin
