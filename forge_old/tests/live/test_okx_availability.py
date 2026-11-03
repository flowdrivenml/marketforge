from datetime import datetime, timezone

import pytest

from marketforge.acquisition.sources.okx import OKXSource
from marketforge.models import (
    DataType,
    Exchange,
    Instrument,
    InstrumentType,
    MarketCategory,
)


@pytest.fixture(scope="module")
def source():
    return OKXSource()


CASES = [
    (
        Instrument(
            exchange=Exchange.OKX,
            instrument_type=InstrumentType.SPOT,
            market_category=MarketCategory.SPOT,
            symbol="BTC-USDT",
        ),
        DataType.TRADE_TICKS,
    ),
    (
        Instrument(
            exchange=Exchange.OKX,
            instrument_type=InstrumentType.PERPETUAL,
            market_category=MarketCategory.LINEAR,
            symbol="BTC-USDT-SWAP",
            family="BTC-USDT",
        ),
        DataType.TRADE_TICKS,
    ),
    (
        Instrument(
            exchange=Exchange.OKX,
            instrument_type=InstrumentType.PERPETUAL,
            market_category=MarketCategory.INVERSE,
            symbol="BTC-USD-SWAP",
            family="BTC-USD",
        ),
        DataType.TRADE_TICKS,
    ),
    (
        Instrument(
            exchange=Exchange.OKX,
            instrument_type=InstrumentType.PERPETUAL,
            market_category=MarketCategory.LINEAR,
            symbol="BTC-USDT-SWAP",
            family="BTC-USDT",
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
        1,
        tzinfo=timezone.utc,
    )

    end = datetime(
        2026,
        9,
        29,
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
    print("OKX HISTORICAL AVAILABILITY")
    print(f"Symbol      : {instrument.symbol}")
    print(f"Family      : {instrument.family}")
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

    starts = [file.start for file in availability.files]

    assert starts == sorted(starts)

    for file in availability.files:
        assert file.exchange == Exchange.OKX
        assert file.instrument_type == instrument.instrument_type
        assert file.market_category == instrument.market_category
        assert file.data_type == data_type
        assert file.symbol == instrument.symbol

        assert file.start < end
        assert file.end > start

        assert file.filename
        assert file.url

    # Detect historical gaps.
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

    if len(gaps) > 20:
        print(f"  ... {len(gaps) - 20} more gaps")
