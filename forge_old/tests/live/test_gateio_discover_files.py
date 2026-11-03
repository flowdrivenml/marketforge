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
    # Spot trades
    (
        Instrument(
            exchange=Exchange.GATEIO,
            instrument_type=InstrumentType.SPOT,
            market_category=MarketCategory.SPOT,
            symbol="BTC_USDT",
        ),
        DataType.TRADE_TICKS,
    ),
    # Spot order book
    (
        Instrument(
            exchange=Exchange.GATEIO,
            instrument_type=InstrumentType.SPOT,
            market_category=MarketCategory.SPOT,
            symbol="BTC_USDT",
        ),
        DataType.ORDER_BOOK_L2,
    ),
]


@pytest.mark.parametrize(
    ("instrument", "data_type"),
    CASES,
)
def test_discover_files(
    source,
    instrument,
    data_type,
):
    # Gate spot trades are stored in monthly archives.
    # Use a completed month rather than the current month.
    if data_type == DataType.TRADE_TICKS:
        start = datetime(
            2026,
            8,
            10,
            tzinfo=timezone.utc,
        )

        end = datetime(
            2026,
            8,
            11,
            tzinfo=timezone.utc,
        )

    # Gate spot order books are stored hourly.
    else:
        start = datetime(
            2026,
            9,
            27,
            tzinfo=timezone.utc,
        )

        end = datetime(
            2026,
            9,
            28,
            tzinfo=timezone.utc,
        )

    files = source.discover_files(
        target=instrument,
        data_type=data_type,
        start=start,
        end=end,
    )

    print("\n")
    print("=" * 90)
    print("GATE.IO FILE DISCOVERY")
    print(f"Symbol      : {instrument.symbol}")
    print(f"Type        : {instrument.instrument_type.value}")
    print(f"Category    : {instrument.market_category.value}")
    print(f"Data Type   : {data_type.value}")
    print(f"Requested   : {start} -> {end} [exclusive]")
    print(f"Files       : {len(files)}")
    print("-" * 90)

    for file in files:
        print(f"{file.start.isoformat()}  " f"{file.filename}  " f"{file.url}")

    assert files

    # --------------------------------------------------------------
    # Chronological ordering
    # --------------------------------------------------------------

    starts = [file.start for file in files]

    assert starts == sorted(starts)

    # --------------------------------------------------------------
    # Metadata
    # --------------------------------------------------------------

    for file in files:
        assert file.exchange == Exchange.GATEIO
        assert file.instrument_type == instrument.instrument_type
        assert file.market_category == instrument.market_category
        assert file.data_type == data_type
        assert file.symbol == instrument.symbol

        # The returned archive must overlap the requested interval.
        #
        # For trades this is especially important because Gate's
        # archive is monthly even if the requested interval is only
        # one day.
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
        source.discover_files(
            target=instrument,
            data_type=DataType.TRADE_TICKS,
            start=start,
            end=end,
        )
