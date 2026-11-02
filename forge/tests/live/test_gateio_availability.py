from datetime import datetime, timezone

import pytest

from marketforge.acquisition.sources.gateio import GateIOSource
from marketforge.models import (
    DataType,
    Exchange,
    Instrument,
    InstrumentType,
    MarketCategory,
)


@pytest.fixture(scope="module")
def source():
    return GateIOSource()


CASES = [
    # Spot trades — monthly archive.
    (
        Instrument(
            exchange=Exchange.GATEIO,
            instrument_type=InstrumentType.SPOT,
            market_category=MarketCategory.SPOT,
            symbol="BTC_USDT",
        ),
        DataType.TRADE_TICKS,
        datetime(
            2026,
            8,
            10,
            tzinfo=timezone.utc,
        ),
        datetime(
            2026,
            8,
            11,
            tzinfo=timezone.utc,
        ),
    ),
    # Spot order book — hourly archives.
    #
    # Keep this deliberately small because availability requires
    # one HEAD request per candidate hourly archive.
    (
        Instrument(
            exchange=Exchange.GATEIO,
            instrument_type=InstrumentType.SPOT,
            market_category=MarketCategory.SPOT,
            symbol="BTC_USDT",
        ),
        DataType.ORDER_BOOK_L2,
        datetime(
            2026,
            9,
            27,
            20,
            tzinfo=timezone.utc,
        ),
        datetime(
            2026,
            9,
            27,
            23,
            tzinfo=timezone.utc,
        ),
    ),
]


@pytest.mark.parametrize(
    (
        "instrument",
        "data_type",
        "start",
        "end",
    ),
    CASES,
)
def test_availability(
    source,
    instrument,
    data_type,
    start,
    end,
):
    availability = source.availability(
        target=instrument,
        data_type=data_type,
        start=start,
        end=end,
    )

    assert availability is not None

    print("\n")
    print("=" * 90)
    print("GATE.IO HISTORICAL AVAILABILITY")
    print(f"Symbol      : {instrument.symbol}")
    print(f"Type        : {instrument.instrument_type.value}")
    print(f"Category    : {instrument.market_category.value}")
    print(f"Data Type   : {data_type.value}")
    print(f"Requested   : {start} -> {end} [exclusive]")
    print(f"Files       : {availability.file_count}")
    print(f"Start       : {availability.start}")
    print(f"End         : {availability.end}")
    print("-" * 90)

    for file in availability.files:
        print(f"{file.start.isoformat()}  " f"{file.filename}  " f"{file.url}")

    assert availability.files
    assert availability.start is not None
    assert availability.end is not None

    starts = [file.start for file in availability.files]

    assert starts == sorted(starts)

    for file in availability.files:
        assert file.exchange == Exchange.GATEIO
        assert file.instrument_type == instrument.instrument_type
        assert file.market_category == instrument.market_category
        assert file.data_type == data_type
        assert file.symbol == instrument.symbol

        assert file.start < end
        assert file.end > start

        assert file.filename
        assert file.url


def test_invalid_interval(source):
    instrument = Instrument(
        exchange=Exchange.GATEIO,
        instrument_type=InstrumentType.SPOT,
        market_category=MarketCategory.SPOT,
        symbol="BTC_USDT",
    )

    start = datetime(
        2026,
        9,
        28,
        tzinfo=timezone.utc,
    )

    end = datetime(
        2026,
        9,
        27,
        tzinfo=timezone.utc,
    )

    with pytest.raises(ValueError):
        source.availability(
            target=instrument,
            data_type=DataType.TRADE_TICKS,
            start=start,
            end=end,
        )
