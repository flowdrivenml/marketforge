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
    # Inverse perpetual trades
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
    # Spot L2
    (
        Instrument(
            exchange=Exchange.OKX,
            instrument_type=InstrumentType.SPOT,
            market_category=MarketCategory.SPOT,
            symbol="BTC-USDT",
            family=None,
        ),
        DataType.ORDER_BOOK_L2,
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
    # Inverse perpetual L2
    (
        Instrument(
            exchange=Exchange.OKX,
            instrument_type=InstrumentType.PERPETUAL,
            market_category=MarketCategory.INVERSE,
            symbol="BTC-USD-SWAP",
            family="BTC-USD",
        ),
        DataType.ORDER_BOOK_L2,
    ),
]


@pytest.mark.parametrize(
    ("instrument", "data_type"),
    ARCHIVE_CASES,
)
def test_archive_discover_files(
    source,
    instrument,
    data_type,
):
    start = datetime(
        2026,
        7,
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
    print("BYBIT FILE DISCOVERY")
    print(f"Symbol      : {instrument.symbol}")
    print(f"Type        : {instrument.instrument_type.value}")
    print(f"Category    : {instrument.market_category.value}")
    print(f"Data Type   : {data_type.value}")
    print(f"Requested   : {start.date()} -> {end.date()} [exclusive]")
    print(f"Files       : {len(files)}")
    print("-" * 80)

    for file in files:
        print(f"{file.start.date()}  " f"{file.filename}  " f"{file.url}")

    assert files

    # Files must be chronological.
    starts = [file.start for file in files]
    assert starts == sorted(starts)

    for file in files:
        assert file.exchange == Exchange.BYBIT
        assert file.instrument_type == instrument.instrument_type
        assert file.market_category == instrument.market_category
        assert file.data_type == data_type
        assert file.symbol == instrument.symbol

        # Every returned file must overlap the requested interval.
        assert file.start < end
        assert file.end > start

        assert file.url
        assert file.filename


def test_option_trade_discover_files(source):
    base_coin = BaseCoin(
        exchange=Exchange.BYBIT,
        market_category=MarketCategory.OPTION,
        symbol="BTC",
    )

    start = datetime(
        2026,
        9,
        22,
        tzinfo=timezone.utc,
    )

    end = datetime(
        2026,
        9,
        29,
        tzinfo=timezone.utc,
    )

    files = source.discover_files(
        target=base_coin,
        data_type=DataType.TRADE_TICKS,
        start=start,
        end=end,
    )

    print("\n")
    print("=" * 80)
    print("BYBIT OPTION FILE DISCOVERY")
    print(f"Base Coin   : {base_coin.symbol}")
    print(f"Data Type   : {DataType.TRADE_TICKS.value}")
    print(f"Requested   : {start.date()} -> {end.date()} [exclusive]")
    print(f"Files       : {len(files)}")
    print("-" * 80)

    for file in files:
        print(f"{file.start.date()}  " f"{file.filename}  " f"{file.url}")

    assert files

    starts = [file.start for file in files]
    assert starts == sorted(starts)

    for file in files:
        assert file.exchange == Exchange.BYBIT
        assert file.instrument_type == InstrumentType.OPTION
        assert file.market_category == MarketCategory.OPTION
        assert file.data_type == DataType.TRADE_TICKS
        assert file.symbol == "BTC"

        assert start <= file.start < end

        assert file.url
        assert file.filename


def test_discover_files_invalid_interval(source):
    instrument = Instrument(
        exchange=Exchange.BYBIT,
        instrument_type=InstrumentType.SPOT,
        market_category=MarketCategory.SPOT,
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
        source.discover_files(
            target=instrument,
            data_type=DataType.TRADE_TICKS,
            start=start,
            end=end,
        )
