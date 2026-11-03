from datetime import datetime, timezone

import pytest

from marketforge.acquisition.sources.bitget import BitgetSource
from marketforge.models import (
    DataType,
    Exchange,
    Instrument,
    InstrumentType,
    MarketCategory,
)


@pytest.fixture(scope="module")
def source():
    return BitgetSource()


CASES = [
    # Spot trades
    (
        Instrument(
            exchange=Exchange.BITGET,
            instrument_type=InstrumentType.SPOT,
            market_category=MarketCategory.SPOT,
            symbol="BTC/USDT",
        ),
        DataType.TRADE_TICKS,
    ),
    # Spot depth
    (
        Instrument(
            exchange=Exchange.BITGET,
            instrument_type=InstrumentType.SPOT,
            market_category=MarketCategory.SPOT,
            symbol="BTC/USDT",
        ),
        DataType.ORDER_BOOK_L2,
    ),
    # Linear perpetual trades
    (
        Instrument(
            exchange=Exchange.BITGET,
            instrument_type=InstrumentType.PERPETUAL,
            market_category=MarketCategory.LINEAR,
            symbol="BTCUSDT",
        ),
        DataType.TRADE_TICKS,
    ),
    # Linear perpetual depth
    (
        Instrument(
            exchange=Exchange.BITGET,
            instrument_type=InstrumentType.PERPETUAL,
            market_category=MarketCategory.LINEAR,
            symbol="BTCUSDT",
        ),
        DataType.ORDER_BOOK_L2,
    ),
]


@pytest.mark.parametrize(
    ("instrument", "data_type"),
    CASES,
)
def test_availability(
    source,
    instrument,
    data_type,
):
    start = datetime(
        2026,
        9,
        20,
        tzinfo=timezone.utc,
    )

    end = datetime(
        2026,
        9,
        23,
        tzinfo=timezone.utc,
    )

    availability = source.availability(
        target=instrument,
        data_type=data_type,
        start=start,
        end=end,
    )

    assert availability is not None

    print("\n")
    print("=" * 90)
    print("BITGET HISTORICAL AVAILABILITY")
    print(f"Symbol      : {instrument.symbol}")
    print(f"Type        : {instrument.instrument_type.value}")
    print(f"Category    : {instrument.market_category.value}")
    print(f"Data Type   : {data_type.value}")
    print(f"Requested   : {start.date()} -> {end.date()} [exclusive]")
    print(f"Files       : {availability.file_count}")
    print(f"Start       : {availability.start}")
    print(f"End         : {availability.end}")
    print("-" * 90)

    for file in availability.files:
        print(f"{file.start.date()}  " f"{file.filename}  " f"{file.url}")

    assert availability.files

    # --------------------------------------------------------------
    # Chronological ordering
    # --------------------------------------------------------------

    starts = [file.start for file in availability.files]

    assert starts == sorted(starts)

    # --------------------------------------------------------------
    # RemoteFile metadata
    # --------------------------------------------------------------

    for file in availability.files:
        assert file.exchange == Exchange.BITGET
        assert file.instrument_type == instrument.instrument_type
        assert file.market_category == instrument.market_category
        assert file.data_type == data_type
        assert file.symbol == instrument.symbol

        assert file.start < end
        assert file.end > start

        assert file.filename
        assert file.url

    # --------------------------------------------------------------
    # Detect gaps
    # --------------------------------------------------------------

    gaps = []

    for previous, current in zip(
        availability.files,
        availability.files[1:],
    ):
        if current.start > previous.end:
            gaps.append(
                (
                    previous.end,
                    current.start,
                )
            )

    print("-" * 90)
    print(f"Gaps        : {len(gaps)}")

    for gap_start, gap_end in gaps[:20]:
        print(f"  {gap_start.date()} " f"-> {gap_end.date()}")


def test_invalid_interval(source):
    instrument = Instrument(
        exchange=Exchange.BITGET,
        instrument_type=InstrumentType.PERPETUAL,
        market_category=MarketCategory.LINEAR,
        symbol="BTCUSDT",
    )

    start = datetime(
        2026,
        9,
        23,
        tzinfo=timezone.utc,
    )

    end = datetime(
        2026,
        9,
        20,
        tzinfo=timezone.utc,
    )

    with pytest.raises(ValueError):
        source.availability(
            target=instrument,
            data_type=DataType.TRADE_TICKS,
            start=start,
            end=end,
        )
