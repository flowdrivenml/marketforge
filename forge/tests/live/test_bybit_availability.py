from datetime import datetime, timezone

import pytest

from marketforge.acquisition.sources.bybit import BybitSource
from marketforge.models import (
    BaseCoin,
    DataType,
    Exchange,
    Instrument,
    InstrumentType,
    MarketCategory,
)


@pytest.fixture(scope="module")
def source():
    return BybitSource()


ARCHIVE_CASES = [
    # Spot trades
    (
        Instrument(
            exchange=Exchange.BYBIT,
            instrument_type=InstrumentType.SPOT,
            market_category=MarketCategory.SPOT,
            symbol="BTCUSDT",
        ),
        DataType.TRADE_TICKS,
    ),
    # Linear perpetual trades
    (
        Instrument(
            exchange=Exchange.BYBIT,
            instrument_type=InstrumentType.PERPETUAL,
            market_category=MarketCategory.LINEAR,
            symbol="BTCUSDT",
        ),
        DataType.TRADE_TICKS,
    ),
    # Inverse perpetual trades
    (
        Instrument(
            exchange=Exchange.BYBIT,
            instrument_type=InstrumentType.PERPETUAL,
            market_category=MarketCategory.INVERSE,
            symbol="BTCUSD",
        ),
        DataType.TRADE_TICKS,
    ),
    # Spot L2
    (
        Instrument(
            exchange=Exchange.BYBIT,
            instrument_type=InstrumentType.SPOT,
            market_category=MarketCategory.SPOT,
            symbol="BTCUSDT",
        ),
        DataType.ORDER_BOOK_L2,
    ),
    # Linear L2
    (
        Instrument(
            exchange=Exchange.BYBIT,
            instrument_type=InstrumentType.PERPETUAL,
            market_category=MarketCategory.LINEAR,
            symbol="BTCUSDT",
        ),
        DataType.ORDER_BOOK_L2,
    ),
    # Inverse L2
    (
        Instrument(
            exchange=Exchange.BYBIT,
            instrument_type=InstrumentType.PERPETUAL,
            market_category=MarketCategory.INVERSE,
            symbol="BTCUSD",
        ),
        DataType.ORDER_BOOK_L2,
    ),
]


@pytest.mark.parametrize(
    ("instrument", "data_type"),
    ARCHIVE_CASES,
)
def test_archive_availability(
    source,
    instrument,
    data_type,
):
    availability = source.availability(
        target=instrument,
        data_type=data_type,
    )

    assert availability is not None

    print("\n")
    print("=" * 80)
    print("BYBIT ARCHIVE AVAILABILITY")
    print(f"Symbol      : {instrument.symbol}")
    print(f"Type        : {instrument.instrument_type.value}")
    print(f"Category    : {instrument.market_category.value}")
    print(f"Data Type   : {data_type.value}")
    print(f"Files       : {availability.file_count}")
    print(f"Start       : {availability.start}")
    print(f"End         : {availability.end}")
    print("-" * 80)

    print("First files:")
    for file in availability.files[:5]:
        print(f"  {file.start.isoformat()}  " f"{file.filename}")

    print("Last files:")
    for file in availability.files[-5:]:
        print(f"  {file.start.isoformat()}  " f"{file.filename}")

    assert availability.files
    assert availability.start is not None
    assert availability.end is not None
    assert availability.start < availability.end

    # Files must already be chronologically ordered.
    starts = [file.start for file in availability.files]
    assert starts == sorted(starts)

    # Monthly archives must have been excluded.
    for file in availability.files:
        assert (file.end - file.start).days == 1

    # Detect gaps between consecutive daily files.
    gaps = []

    for previous, current in zip(
        availability.files,
        availability.files[1:],
    ):
        if current.start > previous.end:
            gaps.append((previous.end, current.start))

    print("-" * 80)
    print(f"Gaps        : {len(gaps)}")

    for gap_start, gap_end in gaps[:20]:
        print(f"  {gap_start.isoformat()} " f"-> {gap_end.isoformat()}")

    if len(gaps) > 20:
        print(f"  ... {len(gaps) - 20} more gaps")


def test_option_trade_availability(source):
    base_coin = BaseCoin(
        exchange=Exchange.BYBIT,
        market_category=MarketCategory.OPTION,
        symbol="BTC",
    )

    start = datetime(
        2025,
        9,
        22,
        tzinfo=timezone.utc,
    )

    # Exclusive internally.
    # Becomes endDay=2026-09-28 for Bybit.
    end = datetime(
        2026,
        9,
        29,
        tzinfo=timezone.utc,
    )

    availability = source.availability(
        target=base_coin,
        data_type=DataType.TRADE_TICKS,
        start=start,
        end=end,
    )

    assert availability is not None

    print("\n")
    print("=" * 80)
    print("BYBIT OPTION AVAILABILITY")
    print(f"Base Coin   : {base_coin.symbol}")
    print(f"Data Type   : {DataType.TRADE_TICKS.value}")
    print(f"Requested   : {start.date()} -> {end.date()} [exclusive]")
    print(f"Files       : {availability.file_count}")
    print(f"Start       : {availability.start}")
    print(f"End         : {availability.end}")
    print("-" * 80)

    for file in availability.files:
        print(f"{file.start.date()}  " f"{file.filename}  " f"{file.url}")

    assert availability.files

    starts = [file.start for file in availability.files]
    assert starts == sorted(starts)

    for file in availability.files:
        assert file.exchange == Exchange.BYBIT
        assert file.instrument_type == InstrumentType.OPTION
        assert file.market_category == MarketCategory.OPTION
        assert file.data_type == DataType.TRADE_TICKS
        assert file.symbol == "BTC"
        assert start <= file.start < end
