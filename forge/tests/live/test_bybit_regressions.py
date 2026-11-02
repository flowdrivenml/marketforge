from datetime import datetime, timezone

import pytest

from marketforge.acquisition.sources.bybit import BybitSource
from marketforge.models import DataType, InstrumentType, MarketCategory

REPORTED_SYMBOLS = [
    "DOGEUSDT",
    "XRPUSDT",
]


@pytest.fixture
def source():
    return BybitSource()


@pytest.mark.parametrize(
    "symbol",
    REPORTED_SYMBOLS,
)
def test_reported_symbols_are_discoverable(
    source,
    symbol,
):
    """Regression test for symbols skipped by the old UI scraper."""

    instruments = source.instruments(
        instrument_type=InstrumentType.PERPETUAL,
        market_category=MarketCategory.LINEAR,
        data_type=DataType.TRADE_TICKS,
    )

    symbols = {instrument.symbol for instrument in instruments}

    print()
    print("=" * 90)
    print("BYBIT SYMBOL DISCOVERY REGRESSION")
    print("=" * 90)
    print(f"Symbol     : {symbol}")
    print(f"Discovered : {symbol in symbols}")
    print(f"Universe   : {len(instruments)}")
    print("-" * 90)

    assert symbol in symbols, (
        f"{symbol} was not discovered in the "
        "Bybit linear perpetual instrument universe"
    )


@pytest.mark.parametrize(
    "symbol",
    REPORTED_SYMBOLS,
)
def test_reported_symbols_have_historical_trade_files(
    source,
    symbol,
):
    """Regression test for the original DOGEUSDT/XRPUSDT issue."""

    instruments = source.instruments(
        instrument_type=InstrumentType.PERPETUAL,
        market_category=MarketCategory.LINEAR,
        data_type=DataType.TRADE_TICKS,
    )

    instrument = next(
        (instrument for instrument in instruments if instrument.symbol == symbol),
        None,
    )

    assert instrument is not None, f"{symbol} was not discovered"

    # Original reported range:
    #
    #     --start 2026-08-01
    #     --end   2026-08-05
    #
    # MarketForge uses [start, end), therefore this represents:
    #
    #     2026-08-01
    #     2026-08-02
    #     2026-08-03
    #     2026-08-04

    start = datetime(
        2026,
        8,
        1,
        tzinfo=timezone.utc,
    )

    end = datetime(
        2026,
        8,
        5,
        tzinfo=timezone.utc,
    )

    files = source.discover_files(
        target=instrument,
        data_type=DataType.TRADE_TICKS,
        start=start,
        end=end,
    )

    print()
    print("=" * 90)
    print("BYBIT HISTORICAL FILE REGRESSION")
    print("=" * 90)
    print(f"Symbol : {symbol}")
    print(f"Range  : {start.isoformat()} " f"-> {end.isoformat()} [exclusive]")
    print(f"Files  : {len(files)}")
    print("-" * 90)

    for file in files:
        print(f"{file.start.isoformat()}  " f"{file.filename}")

    expected_dates = {
        datetime(
            2026,
            8,
            day,
            tzinfo=timezone.utc,
        ).date()
        for day in range(1, 5)
    }

    actual_dates = {file.start.date() for file in files}

    assert len(files) == 4, (
        f"Expected 4 daily trade archives for " f"{symbol}, got {len(files)}"
    )

    assert actual_dates == expected_dates, (
        f"Historical archive dates for {symbol} "
        f"do not match the requested interval: "
        f"expected={sorted(expected_dates)}, "
        f"actual={sorted(actual_dates)}"
    )
