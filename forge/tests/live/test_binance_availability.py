import pytest

from marketforge.acquisition.sources.binance import BinanceSource
from marketforge.models import (
    DataType,
    Exchange,
    Instrument,
    InstrumentType,
    MarketCategory,
)


@pytest.fixture(scope="module")
def source():
    return BinanceSource()


ARCHIVE_CASES = [
    # Spot
    Instrument(
        exchange=Exchange.BINANCE,
        instrument_type=InstrumentType.SPOT,
        market_category=MarketCategory.SPOT,
        symbol="BTCUSDT",
    ),
    # Linear perpetual
    Instrument(
        exchange=Exchange.BINANCE,
        instrument_type=InstrumentType.PERPETUAL,
        market_category=MarketCategory.LINEAR,
        symbol="BTCUSDT",
    ),
    # Inverse perpetual
    Instrument(
        exchange=Exchange.BINANCE,
        instrument_type=InstrumentType.PERPETUAL,
        market_category=MarketCategory.INVERSE,
        symbol="BTCUSD_PERP",
    ),
]


@pytest.mark.parametrize(
    "instrument",
    ARCHIVE_CASES,
)
def test_archive_availability(
    source,
    instrument,
):
    availability = source.availability(
        target=instrument,
        data_type=DataType.TRADE_TICKS,
    )

    assert availability is not None

    print("\n")
    print("=" * 80)
    print("BINANCE ARCHIVE AVAILABILITY")
    print(f"Symbol      : {instrument.symbol}")
    print(f"Type        : {instrument.instrument_type.value}")
    print(f"Category    : {instrument.market_category.value}")
    print(f"Data Type   : {DataType.TRADE_TICKS.value}")
    print(f"Files       : {availability.file_count}")
    print(f"Start       : {availability.start}")
    print(f"End         : {availability.end}")
    print("-" * 80)

    print("First files:")

    for file in availability.files[:5]:
        print(
            f"  {file.start.date()}  " f"{file.filename}  " f"{file.size_bytes} bytes"
        )

    print("Last files:")

    for file in availability.files[-5:]:
        print(
            f"  {file.start.date()}  " f"{file.filename}  " f"{file.size_bytes} bytes"
        )

    # --------------------------------------------------------------
    # Basic availability
    # --------------------------------------------------------------

    assert availability.files
    assert availability.start is not None
    assert availability.end is not None
    assert availability.start < availability.end

    # --------------------------------------------------------------
    # Chronological ordering
    # --------------------------------------------------------------

    starts = [file.start for file in availability.files]

    assert starts == sorted(starts)

    # --------------------------------------------------------------
    # Validate every RemoteFile
    # --------------------------------------------------------------

    for file in availability.files:
        assert file.exchange == Exchange.BINANCE
        assert file.instrument_type == instrument.instrument_type
        assert file.market_category == instrument.market_category
        assert file.data_type == DataType.TRADE_TICKS
        assert file.symbol == instrument.symbol

        assert file.filename.endswith(".zip")
        assert not file.filename.endswith(".CHECKSUM")

        assert file.url
        assert file.size_bytes is not None
        assert file.size_bytes > 0

        # Current Binance acquisition scope uses daily archives.
        assert file.end - file.start == file.end - file.start
        assert (file.end - file.start).days == 1

    # --------------------------------------------------------------
    # Detect historical gaps
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

    print("-" * 80)
    print(f"Gaps        : {len(gaps)}")

    for gap_start, gap_end in gaps[:20]:
        print(f"  {gap_start.date()} " f"-> {gap_end.date()}")

    if len(gaps) > 20:
        print(f"  ... {len(gaps) - 20} more gaps")
