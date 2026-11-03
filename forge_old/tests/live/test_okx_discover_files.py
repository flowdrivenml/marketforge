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
    # Spot trades
    (
        Instrument(
            exchange=Exchange.OKX,
            instrument_type=InstrumentType.SPOT,
            market_category=MarketCategory.SPOT,
            symbol="BTC-USDT",
            family=None,
        ),
        DataType.TRADE_TICKS,
    ),
    # Linear perpetual trades
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
    # Linear perpetual L2
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
def test_discover_files(
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

    files = source.discover_files(
        target=instrument,
        data_type=data_type,
        start=start,
        end=end,
    )

    print("\n")
    print("=" * 80)
    print("OKX FILE DISCOVERY")
    print(f"Symbol      : {instrument.symbol}")
    print(f"Family      : {instrument.family}")
    print(f"Type        : {instrument.instrument_type.value}")
    print(f"Category    : {instrument.market_category.value}")
    print(f"Data Type   : {data_type.value}")
    print(f"Requested   : {start.date()} -> {end.date()} [exclusive]")
    print(f"Files       : {len(files)}")
    print("-" * 80)

    for file in files:
        print(
            f"{file.start.date()}  "
            f"{file.filename}  "
            f"{file.size_bytes} bytes  "
            f"{file.url}"
        )

    assert files

    starts = [file.start for file in files]

    assert starts == sorted(starts)

    for file in files:
        assert file.exchange == Exchange.OKX
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
        exchange=Exchange.OKX,
        instrument_type=InstrumentType.SPOT,
        market_category=MarketCategory.SPOT,
        symbol="BTC-USDT",
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
        source.discover_files(
            target=instrument,
            data_type=DataType.TRADE_TICKS,
            start=start,
            end=end,
        )


def test_option_trade_discover_files(source):
    instrument = Instrument(
        exchange=Exchange.OKX,
        instrument_type=InstrumentType.OPTION,
        market_category=MarketCategory.OPTION,
        symbol="BTC-USD-260930-78000-C",
        family="BTC-USD",
    )

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

    files = source.discover_files(
        target=instrument,
        data_type=DataType.TRADE_TICKS,
        start=start,
        end=end,
    )

    print("\n")
    print("=" * 90)
    print("OKX OPTION FILE DISCOVERY")
    print(f"Symbol      : {instrument.symbol}")
    print(f"Family      : {instrument.family}")
    print(f"Data Type   : {DataType.TRADE_TICKS.value}")
    print(f"Requested   : {start.date()} -> {end.date()} [exclusive]")
    print(f"Files       : {len(files)}")
    print("-" * 90)

    for file in files:
        print(
            f"{file.start.date()}  "
            f"{file.filename}  "
            f"{file.size_bytes} bytes  "
            f"{file.url}"
        )

    assert files

    starts = [file.start for file in files]

    assert starts == sorted(starts)

    for file in files:
        assert file.exchange == Exchange.OKX
        assert file.instrument_type == InstrumentType.OPTION
        assert file.market_category == MarketCategory.OPTION
        assert file.data_type == DataType.TRADE_TICKS

        assert file.start < end
        assert file.end > start

        assert file.filename
        assert file.url
