from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from pathlib import Path


class Exchange(StrEnum):
    BYBIT = "bybit"
    BINANCE = "binance"
    OKX = "okx"
    GATEIO = "gateio"
    BITGET = "bitget"


class InstrumentType(StrEnum):
    SPOT = "spot"
    PERPETUAL = "perpetual"
    FUTURE = "future"
    OPTION = "option"


class MarketCategory(StrEnum):
    SPOT = "spot"
    LINEAR = "linear"
    INVERSE = "inverse"
    OPTION = "option"


class DataType(StrEnum):
    TRADE_TICKS = "trade_ticks"
    ORDER_BOOK_L2 = "order_book_l2"


@dataclass(frozen=True)
class Instrument:
    exchange: Exchange
    instrument_type: InstrumentType
    market_category: MarketCategory
    symbol: str
    family: str | None = None


@dataclass(frozen=True)
class BaseCoin:
    exchange: Exchange
    market_category: MarketCategory
    symbol: str


@dataclass(frozen=True)
class RemoteFile:
    exchange: Exchange
    instrument_type: InstrumentType
    market_category: MarketCategory
    data_type: DataType
    symbol: str
    start: datetime
    end: datetime
    url: str
    filename: str
    size_bytes: int | None = None


@dataclass(frozen=True)
class Availability:
    exchange: Exchange
    target: Instrument | BaseCoin
    data_type: DataType
    files: tuple[RemoteFile, ...]

    @property
    def start(self) -> datetime | None:
        if not self.files:
            return None

        return min(file.start for file in self.files)

    @property
    def end(self) -> datetime | None:
        if not self.files:
            return None

        return max(file.end for file in self.files)

    @property
    def file_count(self) -> int:
        return len(self.files)


@dataclass(frozen=True)
class AcquisitionRequest:
    """Describe one MarketForge historical-data acquisition request.

    The interval follows MarketForge's standard half-open convention:

        [start, end)

    `target` identifies what should be acquired.
    `data_type` identifies the requested dataset.
    """

    target: Instrument | BaseCoin
    data_type: DataType
    start: datetime
    end: datetime

    def __post_init__(self) -> None:
        if self.start.tzinfo is None:
            raise ValueError("AcquisitionRequest.start must be timezone-aware")

        if self.end.tzinfo is None:
            raise ValueError("AcquisitionRequest.end must be timezone-aware")

        if self.start >= self.end:
            raise ValueError("AcquisitionRequest.start must be earlier than end")


class DownloadStatus(StrEnum):
    DOWNLOADED = "downloaded"
    REUSED = "reused"


@dataclass(frozen=True)
class DownloadResult:
    """Describe the result of one completed raw archive download."""

    remote_file: RemoteFile
    path: Path
    bytes_written: int
    sha256: str
    status: DownloadStatus


@dataclass(frozen=True)
class RawFileRecord:
    """Persisted verification record for one raw archive."""

    remote_file: RemoteFile
    path: Path
    bytes_written: int
    sha256: str
    downloaded_at: datetime


class DownloadAction(StrEnum):
    DOWNLOAD = "download"
    REUSE = "reuse"


@dataclass(frozen=True)
class PlannedDownload:
    """Describe one remote archive and its local acquisition action."""

    remote_file: RemoteFile
    destination: Path
    action: DownloadAction


@dataclass(frozen=True)
class AcquisitionPlan:
    """Describe the physical archives required for one acquisition request."""

    request: AcquisitionRequest
    downloads: tuple[PlannedDownload, ...]

    @property
    def file_count(self) -> int:
        """Return the number of physical remote archives."""

        return len(self.downloads)

    @property
    def known_size_bytes(self) -> int:
        """Return the sum of known remote archive sizes."""

        return sum(
            download.remote_file.size_bytes
            for download in self.downloads
            if download.remote_file.size_bytes is not None
        )

    @property
    def unknown_size_count(self) -> int:
        """Return the number of archives without known size metadata."""

        return sum(
            download.remote_file.size_bytes is None for download in self.downloads
        )

    @property
    def download_count(self) -> int:
        """Return the number of archives requiring download."""

        return sum(item.action == DownloadAction.DOWNLOAD for item in self.downloads)

    @property
    def reuse_count(self) -> int:
        """Return the number of verified reusable archives."""

        return sum(item.action == DownloadAction.REUSE for item in self.downloads)
