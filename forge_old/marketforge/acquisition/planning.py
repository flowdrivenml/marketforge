from __future__ import annotations

from collections.abc import Callable

from marketforge.acquisition.http import HttpClient
from marketforge.acquisition.sources.base import Source
from marketforge.acquisition.sources.binance import BinanceSource
from marketforge.acquisition.sources.bitget import BitgetSource
from marketforge.acquisition.sources.bybit import BybitSource
from marketforge.acquisition.sources.gateio import GateIOSource
from marketforge.acquisition.sources.okx import OKXSource
from marketforge.models import AcquisitionRequest, DownloadAction, Exchange, RemoteFile
from marketforge.storage.manifest import verify_raw_file

SourceFactory = Callable[
    [HttpClient | None],
    Source,
]

SOURCE_FACTORIES: dict[
    Exchange,
    SourceFactory,
] = {
    Exchange.BYBIT: lambda http=None: BybitSource(http=http),
    Exchange.BINANCE: lambda http=None: BinanceSource(http=http),
    Exchange.OKX: lambda http=None: OKXSource(http=http),
    Exchange.BITGET: lambda http=None: BitgetSource(http=http),
    Exchange.GATEIO: lambda http=None: GateIOSource(http=http),
}


from pathlib import Path

from marketforge.models import (
    AcquisitionPlan,
    AcquisitionRequest,
    Exchange,
    PlannedDownload,
    RemoteFile,
)
from marketforge.storage.paths import DEFAULT_DATA_ROOT, raw_file_path


class AcquisitionPlanner:
    """Resolve acquisition requests into remote files."""

    def __init__(
        self,
        source_factories: (
            dict[
                Exchange,
                SourceFactory,
            ]
            | None
        ) = None,
        *,
        data_root: Path = DEFAULT_DATA_ROOT,
        http: HttpClient | None = None,
    ) -> None:
        self._source_factories = (
            source_factories if source_factories is not None else SOURCE_FACTORIES
        )

        self.data_root = Path(data_root)
        self.http = http

    def discover(
        self,
        request: AcquisitionRequest,
    ) -> list[RemoteFile]:
        """Discover remote files required by an acquisition request."""

        source = self.source(request.target.exchange)

        return source.discover_files(
            target=request.target,
            data_type=request.data_type,
            start=request.start,
            end=request.end,
        )

    def source(
        self,
        exchange: Exchange,
    ) -> Source:
        """Construct the source adapter for an exchange."""

        factory = self._source_factories.get(exchange)

        if factory is None:
            raise ValueError(
                "No acquisition source registered for " f"{exchange.value}"
            )

        return factory(self.http)

    def plan(
        self,
        request: AcquisitionRequest,
    ) -> AcquisitionPlan:
        """Build an acquisition plan without downloading archive contents."""

        remote_files = self.discover(request)

        downloads = tuple(
            self._planned_download(remote_file) for remote_file in remote_files
        )

        return AcquisitionPlan(
            request=request,
            downloads=downloads,
        )

    @property
    def download_count(self) -> int:
        """Return the number of archives requiring network download."""

        return sum(
            download.action == DownloadAction.DOWNLOAD for download in self.downloads
        )

    @property
    def reuse_count(self) -> int:
        """Return the number of verified reusable archives."""

        return sum(
            download.action == DownloadAction.REUSE for download in self.downloads
        )

    def _planned_download(
        self,
        remote_file: RemoteFile,
    ) -> PlannedDownload:
        """Build the local acquisition action for one remote archive."""

        destination = raw_file_path(
            remote_file,
            data_root=self.data_root,
        )

        existing = verify_raw_file(
            destination,
            expected_remote_file=remote_file,
        )

        action = (
            DownloadAction.REUSE if existing is not None else DownloadAction.DOWNLOAD
        )

        return PlannedDownload(
            remote_file=remote_file,
            destination=destination,
            action=action,
        )
