import hashlib
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock

import pytest

from marketforge.acquisition.download import Downloader
from marketforge.models import (
    AcquisitionPlan,
    AcquisitionRequest,
    DataType,
    DownloadAction,
    DownloadStatus,
    Exchange,
    Instrument,
    InstrumentType,
    MarketCategory,
    PlannedDownload,
    RawFileRecord,
    RemoteFile,
)
from marketforge.storage.manifest import (
    RawFileRecord,
    manifest_path,
    read_raw_record,
    write_raw_record,
)


class FakeResponse:
    def __init__(
        self,
        chunks,
    ):
        self.chunks = chunks
        self.closed = False

    def iter_content(
        self,
        chunk_size,
    ):
        yield from self.chunks

    def close(self):
        self.closed = True


def make_plan(
    tmp_path: Path,
    *,
    size_bytes=None,
) -> PlannedDownload:
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
        url="https://example.com/BTCUSDT.zip",
        filename="BTCUSDT.zip",
        size_bytes=size_bytes,
    )

    return PlannedDownload(
        remote_file=remote_file,
        destination=(tmp_path / "raw" / "BTCUSDT.zip"),
        action=DownloadAction.DOWNLOAD,
    )


def test_download_streams_to_destination(
    tmp_path,
):
    content = b"hello " b"marketforge"

    response = FakeResponse(
        [
            b"hello ",
            b"market",
            b"forge",
        ]
    )

    http = Mock()
    http.get.return_value = response

    downloader = Downloader(
        http=http,
        chunk_size=4,
    )

    plan = make_plan(
        tmp_path,
        size_bytes=len(content),
    )

    result = downloader.download(plan)

    assert plan.destination.read_bytes() == content

    assert result.path == plan.destination
    assert result.bytes_written == len(content)

    assert result.sha256 == hashlib.sha256(content).hexdigest()

    assert result.status == DownloadStatus.DOWNLOADED

    assert response.closed

    assert not Path(str(plan.destination) + ".part").exists()

    http.get.assert_called_once_with(
        plan.remote_file.url,
        policy=None,
        stream=True,
    )

    manifest = manifest_path(plan.destination)

    assert manifest.exists()

    record = read_raw_record(plan.destination)

    assert record is not None
    assert record.remote_file == plan.remote_file
    assert record.path == plan.destination
    assert record.bytes_written == len(content)
    assert record.sha256 == result.sha256
    assert record.downloaded_at.tzinfo is not None


def test_download_creates_parent_directories(
    tmp_path,
):
    response = FakeResponse(
        [
            b"data",
        ]
    )

    http = Mock()
    http.get.return_value = response

    plan = make_plan(
        tmp_path,
        size_bytes=4,
    )

    assert not plan.destination.parent.exists()

    Downloader(http=http).download(plan)

    assert plan.destination.exists()


def test_download_ignores_empty_chunks(
    tmp_path,
):
    response = FakeResponse(
        [
            b"abc",
            b"",
            b"def",
        ]
    )

    http = Mock()
    http.get.return_value = response

    plan = make_plan(
        tmp_path,
        size_bytes=6,
    )

    result = Downloader(http=http).download(plan)

    assert result.bytes_written == 6

    assert plan.destination.read_bytes() == b"abcdef"


class FailingResponse:
    def __init__(self):
        self.closed = False

    def iter_content(
        self,
        chunk_size,
    ):
        yield b"partial-data"

        raise OSError("connection interrupted")

    def close(self):
        self.closed = True


def test_interrupted_download_is_cleaned_up(
    tmp_path,
):
    response = FailingResponse()

    http = Mock()
    http.get.return_value = response

    plan = make_plan(tmp_path)

    downloader = Downloader(http=http)

    with pytest.raises(
        OSError,
        match="connection interrupted",
    ):
        downloader.download(plan)

    assert not plan.destination.exists()

    partial = Path(str(plan.destination) + ".part")

    assert not partial.exists()

    assert response.closed


def test_stale_partial_file_is_replaced(
    tmp_path,
):
    response = FakeResponse(
        [
            b"new-data",
        ]
    )

    http = Mock()
    http.get.return_value = response

    plan = make_plan(
        tmp_path,
        size_bytes=8,
    )

    plan.destination.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    partial = Path(str(plan.destination) + ".part")

    partial.write_bytes(b"old-partial-data")

    Downloader(http=http).download(plan)

    assert plan.destination.read_bytes() == b"new-data"

    assert not partial.exists()


def test_verified_existing_file_is_reused(
    tmp_path,
):
    content = b"already-downloaded"

    plan = make_plan(
        tmp_path,
        size_bytes=len(content),
    )

    plan.destination.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    plan.destination.write_bytes(content)

    record = RawFileRecord(
        remote_file=plan.remote_file,
        path=plan.destination,
        bytes_written=len(content),
        sha256=hashlib.sha256(content).hexdigest(),
        downloaded_at=datetime(
            2026,
            9,
            30,
            tzinfo=timezone.utc,
        ),
    )

    write_raw_record(record)

    http = Mock()

    result = Downloader(http=http).download(plan)

    assert result.status == DownloadStatus.REUSED
    assert result.path == plan.destination
    assert result.bytes_written == len(content)
    assert result.sha256 == record.sha256

    http.get.assert_not_called()


def test_download_many(
    tmp_path,
):
    content_1 = b"first-file"
    content_2 = b"second-file"

    plan_1 = make_plan(
        tmp_path,
        size_bytes=len(content_1),
    )

    remote_2 = RemoteFile(
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
        url="https://example.com/BTCUSDT-2.zip",
        filename="BTCUSDT-2.zip",
        size_bytes=len(content_2),
    )

    plan_2 = PlannedDownload(
        remote_file=remote_2,
        destination=(tmp_path / "raw" / "BTCUSDT-2.zip"),
        action=DownloadAction.DOWNLOAD,
    )

    request = AcquisitionRequest(
        target=Instrument(
            exchange=Exchange.BYBIT,
            instrument_type=InstrumentType.PERPETUAL,
            market_category=MarketCategory.LINEAR,
            symbol="BTCUSDT",
        ),
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

    acquisition_plan = AcquisitionPlan(
        request=request,
        downloads=(
            plan_1,
            plan_2,
        ),
    )

    http = Mock()

    http.get.side_effect = [
        FakeResponse([content_1]),
        FakeResponse([content_2]),
    ]

    downloader = Downloader(http=http)

    results = downloader.download_many(acquisition_plan)

    assert len(results) == 2

    assert results[0].status == DownloadStatus.DOWNLOADED

    assert results[1].status == DownloadStatus.DOWNLOADED

    assert plan_1.destination.read_bytes() == content_1

    assert plan_2.destination.read_bytes() == content_2

    assert http.get.call_count == 2


def test_download_many_mixes_reuse_and_download(
    tmp_path,
):
    reused_content = b"already-here"
    new_content = b"new-file"

    # --------------------------------------------------------------
    # Existing verified archive
    # --------------------------------------------------------------

    reused_plan = make_plan(
        tmp_path,
        size_bytes=len(reused_content),
    )

    reused_plan.destination.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    reused_plan.destination.write_bytes(reused_content)

    reused_record = RawFileRecord(
        remote_file=reused_plan.remote_file,
        path=reused_plan.destination,
        bytes_written=len(reused_content),
        sha256=hashlib.sha256(reused_content).hexdigest(),
        downloaded_at=datetime(
            2026,
            9,
            30,
            tzinfo=timezone.utc,
        ),
    )

    write_raw_record(reused_record)

    # --------------------------------------------------------------
    # Missing archive
    # --------------------------------------------------------------

    remote_2 = RemoteFile(
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
        url="https://example.com/BTCUSDT-2.zip",
        filename="BTCUSDT-2.zip",
        size_bytes=len(new_content),
    )

    new_plan = PlannedDownload(
        remote_file=remote_2,
        destination=(tmp_path / "raw" / "BTCUSDT-2.zip"),
        action=DownloadAction.DOWNLOAD,
    )

    request = AcquisitionRequest(
        target=Instrument(
            exchange=Exchange.BYBIT,
            instrument_type=InstrumentType.PERPETUAL,
            market_category=MarketCategory.LINEAR,
            symbol="BTCUSDT",
        ),
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

    acquisition_plan = AcquisitionPlan(
        request=request,
        downloads=(
            reused_plan,
            new_plan,
        ),
    )

    http = Mock()

    http.get.return_value = FakeResponse([new_content])

    results = Downloader(http=http).download_many(acquisition_plan)

    assert len(results) == 2

    assert results[0].status == DownloadStatus.REUSED

    assert results[1].status == DownloadStatus.DOWNLOADED

    # Only the missing archive touched the network.
    assert http.get.call_count == 1

    assert reused_plan.destination.read_bytes() == reused_content

    assert new_plan.destination.read_bytes() == new_content


def test_download_many_empty_plan(
    tmp_path,
):
    request = AcquisitionRequest(
        target=Instrument(
            exchange=Exchange.BYBIT,
            instrument_type=InstrumentType.PERPETUAL,
            market_category=MarketCategory.LINEAR,
            symbol="BTCUSDT",
        ),
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

    plan = AcquisitionPlan(
        request=request,
        downloads=(),
    )

    http = Mock()

    results = Downloader(http=http).download_many(plan)

    assert results == []
    http.get.assert_not_called()


def test_download_reports_progress(
    tmp_path,
):
    content = b"abcdef"

    response = FakeResponse(
        [
            b"ab",
            b"cd",
            b"ef",
        ]
    )

    http = Mock()
    http.get.return_value = response

    plan = make_plan(
        tmp_path,
        size_bytes=len(content),
    )

    events = []

    def progress(
        planned,
        bytes_written,
        total_bytes,
    ):
        events.append(
            (
                planned,
                bytes_written,
                total_bytes,
            )
        )

    result = Downloader(
        http=http,
    ).download(
        plan,
        progress=progress,
    )

    assert result.status == DownloadStatus.DOWNLOADED

    assert events == [
        (
            plan,
            2,
            6,
        ),
        (
            plan,
            4,
            6,
        ),
        (
            plan,
            6,
            6,
        ),
    ]
