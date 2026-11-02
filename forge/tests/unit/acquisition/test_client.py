from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock

from marketforge.acquisition.client import MarketForgeClient
from marketforge.models import (
    AcquisitionPlan,
    AcquisitionRequest,
    DataType,
    DownloadResult,
    DownloadStatus,
    Exchange,
    Instrument,
    InstrumentType,
    MarketCategory,
)


def make_request():
    instrument = Instrument(
        exchange=Exchange.BYBIT,
        instrument_type=InstrumentType.PERPETUAL,
        market_category=MarketCategory.LINEAR,
        symbol="BTCUSDT",
    )

    return AcquisitionRequest(
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


def test_client_plan_delegates_to_planner(
    tmp_path,
):
    request = make_request()

    source = Mock()
    source.discover_files.return_value = []

    client = MarketForgeClient(
        data_root=tmp_path,
        source_factories={
            Exchange.BYBIT: lambda http=None: source,
        },
    )

    plan = client.plan(request)

    assert isinstance(
        plan,
        AcquisitionPlan,
    )

    assert plan.request == request
    assert plan.downloads == ()

    source.discover_files.assert_called_once_with(
        target=request.target,
        data_type=request.data_type,
        start=request.start,
        end=request.end,
    )

    client.close()


def test_client_download_delegates_to_downloader(
    tmp_path,
):
    request = make_request()

    source = Mock()
    source.discover_files.return_value = []

    client = MarketForgeClient(
        data_root=tmp_path,
        source_factories={
            Exchange.BYBIT: lambda http=None: source,
        },
    )

    plan = AcquisitionPlan(
        request=request,
        downloads=(),
    )

    client.downloader.download_many = Mock(return_value=[])

    results = client.download(plan)

    assert results == []

    client.downloader.download_many.assert_called_once_with(
        plan,
        policy=None,
    )

    client.close()


def test_client_context_manager_closes_http(
    tmp_path,
):
    http = Mock()

    with MarketForgeClient(
        data_root=tmp_path,
        http=http,
        source_factories={},
    ) as client:
        assert client.http is http

    http.close.assert_called_once()
