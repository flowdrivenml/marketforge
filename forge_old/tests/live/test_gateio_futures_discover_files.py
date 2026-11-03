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
    # --------------------------------------------------------------
    # USDT-settled / linear perpetual
    # --------------------------------------------------------------
    (
        Instrument(
            exchange=Exchange.GATEIO,
            instrument_type=InstrumentType.PERPETUAL,
            market_category=MarketCategory.LINEAR,
            symbol="BTC_USDT",
        ),
        DataType.TRADE_TICKS,
    ),
    (
        Instrument(
            exchange=Exchange.GATEIO,
            instrument_type=InstrumentType.PERPETUAL,
            market_category=MarketCategory.LINEAR,
            symbol="BTC_USDT",
        ),
        DataType.ORDER_BOOK_L2,
    ),
    # --------------------------------------------------------------
    # BTC-settled / inverse perpetual
    # --------------------------------------------------------------
    (
        Instrument(
            exchange=Exchange.GATEIO,
            instrument_type=InstrumentType.PERPETUAL,
            market_category=MarketCategory.INVERSE,
            symbol="BTC_USD",
        ),
        DataType.TRADE_TICKS,
    ),
    (
        Instrument(
            exchange=Exchange.GATEIO,
            instrument_type=InstrumentType.PERPETUAL,
            market_category=MarketCategory.INVERSE,
            symbol="BTC_USD",
        ),
        DataType.ORDER_BOOK_L2,
    ),
]


@pytest.mark.parametrize(
    ("instrument", "data_type"),
    CASES,
)
def test_futures_discover_files(
    source,
    instrument,
    data_type,
):
    # Trades are monthly archives.
    # Use a completed month.
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

    # Order books are hourly.
    # Keep this deliberately tiny because every hour requires HEAD.
    else:
        start = datetime(
            2026,
            9,
            27,
            20,
            tzinfo=timezone.utc,
        )

        end = datetime(
            2026,
            9,
            27,
            23,
            tzinfo=timezone.utc,
        )

    files = source.discover_files(
        target=instrument,
        data_type=data_type,
        start=start,
        end=end,
    )

    print("\n")
    print("=" * 100)
    print("GATE.IO FUTURES FILE DISCOVERY")
    print(f"Symbol      : {instrument.symbol}")
    print(f"Type        : {instrument.instrument_type.value}")
    print(f"Category    : {instrument.market_category.value}")
    print(f"Data Type   : {data_type.value}")
    print(f"Requested   : {start} -> {end} [exclusive]")
    print(f"Files       : {len(files)}")
    print("-" * 100)

    for file in files:
        print(f"{file.start.isoformat()}  " f"{file.filename}  " f"{file.url}")

    assert files

    starts = [file.start for file in files]

    assert starts == sorted(starts)

    for file in files:
        assert file.exchange == Exchange.GATEIO
        assert file.instrument_type == InstrumentType.PERPETUAL
        assert file.market_category == instrument.market_category
        assert file.data_type == data_type
        assert file.symbol == instrument.symbol

        assert file.start < end
        assert file.end > start

        assert file.filename
        assert file.url
