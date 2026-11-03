import pytest

from marketforge.acquisition.sources.gateio import GateIOSource
from marketforge.models import DataType, Exchange, InstrumentType, MarketCategory


@pytest.fixture(scope="module")
def source():
    return GateIOSource()


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
    # Linear delivery futures
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
    print("GATE.IO INSTRUMENT DISCOVERY")
    print(f"Instrument Type : {instrument_type.value}")
    print(f"Market Category : {market_category.value}")
    print(f"Data Type       : {data_type.value}")
    print(f"Total           : {len(instruments)}")
    print("-" * 90)

    for instrument in instruments[:20]:
        print(
            f"  {instrument.symbol:<30} "
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
        assert instrument.exchange == Exchange.GATEIO
        assert instrument.instrument_type == instrument_type
        assert instrument.market_category == market_category
        assert instrument.symbol


def test_complete_instrument_universe(source):
    instruments = source.instruments()

    print("\n")
    print("=" * 90)
    print("GATE.IO COMPLETE INSTRUMENT UNIVERSE")
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
        category,
    ), count in sorted(counts.items()):
        print(f"{instrument_type:<12} " f"{category:<10} " f"{count:>6}")

    assert instruments


def test_unsupported_combinations(source):
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
        InstrumentType.FUTURE,
        MarketCategory.SPOT,
        DataType.ORDER_BOOK_L2,
    )
