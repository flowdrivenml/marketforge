from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock

from marketforge.acquisition.planning import AcquisitionPlanner
from marketforge.models import (
    AcquisitionRequest,
    DataType,
    Exchange,
    Instrument,
    InstrumentType,
    MarketCategory,
    RemoteFile,
)


def test_planner_discovers_files():
    instrument = Instrument(
        exchange=Exchange.BYBIT,
        instrument_type=InstrumentType.PERPETUAL,
        market_category=MarketCategory.LINEAR,
        symbol="BTCUSDT",
    )

    request = AcquisitionRequest(
        target=instrument,
        data_type=DataType.TRADE_TICKS,
        start=datetime(
            2026,
            9,
            1,
            tzinfo=timezone.utc,
        ),
        end=datetime(
            2026,
            9,
            3,
            tzinfo=timezone.utc,
        ),
    )

    remote_file = RemoteFile(
        exchange=Exchange.BYBIT,
        instrument_type=InstrumentType.PERPETUAL,
        market_category=MarketCategory.LINEAR,
        data_type=DataType.TRADE_TICKS,
        symbol="BTCUSDT",
        start=datetime(
            2026,
            9,
            1,
            tzinfo=timezone.utc,
        ),
        end=datetime(
            2026,
            9,
            2,
            tzinfo=timezone.utc,
        ),
        url=("https://example.com/" "BTCUSDT2026-09-01.csv.gz"),
        filename="BTCUSDT2026-09-01.csv.gz",
        size_bytes=None,
    )

    source = Mock()
    source.discover_files.return_value = [remote_file]

    planner = AcquisitionPlanner(
        source_factories={
            Exchange.BYBIT: lambda http=None: source,
        }
    )

    files = planner.discover(request)

    assert files == [remote_file]

    source.discover_files.assert_called_once_with(
        target=instrument,
        data_type=DataType.TRADE_TICKS,
        start=request.start,
        end=request.end,
    )


def test_planner_selects_source_from_target_exchange():
    instrument = Instrument(
        exchange=Exchange.OKX,
        instrument_type=InstrumentType.PERPETUAL,
        market_category=MarketCategory.LINEAR,
        symbol="BTC-USDT-SWAP",
        family="BTC-USDT",
    )

    request = AcquisitionRequest(
        target=instrument,
        data_type=DataType.TRADE_TICKS,
        start=datetime(
            2026,
            9,
            1,
            tzinfo=timezone.utc,
        ),
        end=datetime(
            2026,
            9,
            2,
            tzinfo=timezone.utc,
        ),
    )

    bybit = Mock()
    okx = Mock()

    okx.discover_files.return_value = []

    planner = AcquisitionPlanner(
        source_factories={
            Exchange.BYBIT: lambda: bybit,
            Exchange.OKX: lambda http=None: okx,
        }
    )

    files = planner.discover(request)

    assert files == []

    okx.discover_files.assert_called_once()
    bybit.discover_files.assert_not_called()


def test_plan_builds_local_destinations():
    instrument = Instrument(
        exchange=Exchange.BYBIT,
        instrument_type=InstrumentType.PERPETUAL,
        market_category=MarketCategory.LINEAR,
        symbol="BTCUSDT",
    )

    request = AcquisitionRequest(
        target=instrument,
        data_type=DataType.TRADE_TICKS,
        start=datetime(
            2026,
            9,
            1,
            tzinfo=timezone.utc,
        ),
        end=datetime(
            2026,
            9,
            2,
            tzinfo=timezone.utc,
        ),
    )

    remote_file = RemoteFile(
        exchange=Exchange.BYBIT,
        instrument_type=InstrumentType.PERPETUAL,
        market_category=MarketCategory.LINEAR,
        data_type=DataType.TRADE_TICKS,
        symbol="BTCUSDT",
        start=request.start,
        end=request.end,
        url="https://example.com/BTCUSDT.zip",
        filename="BTCUSDT.zip",
        size_bytes=1000,
    )

    source = Mock()
    source.discover_files.return_value = [remote_file]

    planner = AcquisitionPlanner(
        source_factories={
            Exchange.BYBIT: lambda http=None: source,
        },
        data_root=Path("/tmp/marketforge"),
    )

    plan = planner.plan(request)

    assert plan.request == request
    assert plan.file_count == 1

    assert plan.downloads[0].remote_file == remote_file

    assert plan.downloads[0].destination == Path(
        "/tmp/marketforge/"
        "raw/"
        "bybit/"
        "perpetual/"
        "linear/"
        "trade_ticks/"
        "BTCUSDT/"
        "BTCUSDT.zip"
    )


def test_plan_size_summary():
    instrument = Instrument(
        exchange=Exchange.BYBIT,
        instrument_type=InstrumentType.PERPETUAL,
        market_category=MarketCategory.LINEAR,
        symbol="BTCUSDT",
    )

    request = AcquisitionRequest(
        target=instrument,
        data_type=DataType.TRADE_TICKS,
        start=datetime(
            2026,
            9,
            1,
            tzinfo=timezone.utc,
        ),
        end=datetime(
            2026,
            9,
            4,
            tzinfo=timezone.utc,
        ),
    )

    files = [
        RemoteFile(
            exchange=Exchange.BYBIT,
            instrument_type=InstrumentType.PERPETUAL,
            market_category=MarketCategory.LINEAR,
            data_type=DataType.TRADE_TICKS,
            symbol="BTCUSDT",
            start=datetime(
                2026,
                9,
                1,
                tzinfo=timezone.utc,
            ),
            end=datetime(
                2026,
                9,
                2,
                tzinfo=timezone.utc,
            ),
            url="https://example.com/1.zip",
            filename="1.zip",
            size_bytes=100,
        ),
        RemoteFile(
            exchange=Exchange.BYBIT,
            instrument_type=InstrumentType.PERPETUAL,
            market_category=MarketCategory.LINEAR,
            data_type=DataType.TRADE_TICKS,
            symbol="BTCUSDT",
            start=datetime(
                2026,
                9,
                2,
                tzinfo=timezone.utc,
            ),
            end=datetime(
                2026,
                9,
                3,
                tzinfo=timezone.utc,
            ),
            url="https://example.com/2.zip",
            filename="2.zip",
            size_bytes=250,
        ),
        RemoteFile(
            exchange=Exchange.BYBIT,
            instrument_type=InstrumentType.PERPETUAL,
            market_category=MarketCategory.LINEAR,
            data_type=DataType.TRADE_TICKS,
            symbol="BTCUSDT",
            start=datetime(
                2026,
                9,
                3,
                tzinfo=timezone.utc,
            ),
            end=datetime(
                2026,
                9,
                4,
                tzinfo=timezone.utc,
            ),
            url="https://example.com/3.zip",
            filename="3.zip",
            size_bytes=None,
        ),
    ]

    source = Mock()
    source.discover_files.return_value = files

    planner = AcquisitionPlanner(
        source_factories={
            Exchange.BYBIT: lambda http=None: source,
        }
    )

    plan = planner.plan(request)

    assert plan.file_count == 3
    assert plan.known_size_bytes == 350
    assert plan.unknown_size_count == 1


def test_plan_can_be_empty():
    instrument = Instrument(
        exchange=Exchange.BYBIT,
        instrument_type=InstrumentType.PERPETUAL,
        market_category=MarketCategory.LINEAR,
        symbol="BTCUSDT",
    )

    request = AcquisitionRequest(
        target=instrument,
        data_type=DataType.TRADE_TICKS,
        start=datetime(
            2026,
            1,
            1,
            tzinfo=timezone.utc,
        ),
        end=datetime(
            2026,
            1,
            2,
            tzinfo=timezone.utc,
        ),
    )

    source = Mock()
    source.discover_files.return_value = []

    planner = AcquisitionPlanner(
        source_factories={
            Exchange.BYBIT: lambda http=None: source,
        }
    )

    plan = planner.plan(request)

    assert plan.file_count == 0
    assert plan.downloads == ()
    assert plan.known_size_bytes == 0
    assert plan.unknown_size_count == 0
