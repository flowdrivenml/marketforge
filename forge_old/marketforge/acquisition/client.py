from __future__ import annotations

from datetime import datetime
from pathlib import Path

from marketforge.acquisition.download import Downloader, ProgressCallback
from marketforge.acquisition.http import HttpClient, RequestPolicy
from marketforge.acquisition.planning import (
    SOURCE_FACTORIES,
    AcquisitionPlanner,
    SourceFactory,
)
from marketforge.models import (
    AcquisitionPlan,
    AcquisitionRequest,
    Availability,
    BaseCoin,
    DataType,
    DownloadResult,
    Exchange,
    Instrument,
    InstrumentType,
    MarketCategory,
)
from marketforge.storage.paths import DEFAULT_DATA_ROOT


class MarketForgeClient:
    """High-level interface for historical data acquisition."""

    def __init__(
        self,
        *,
        data_root: Path = DEFAULT_DATA_ROOT,
        http: HttpClient | None = None,
        source_factories: (
            dict[
                Exchange,
                SourceFactory,
            ]
            | None
        ) = None,
    ) -> None:
        self.http = http or HttpClient()

        self.planner = AcquisitionPlanner(
            source_factories=(
                source_factories if source_factories is not None else SOURCE_FACTORIES
            ),
            data_root=data_root,
        )

        self.downloader = Downloader(http=self.http)

    def plan(
        self,
        request: AcquisitionRequest,
    ) -> AcquisitionPlan:
        """Build an acquisition plan."""

        return self.planner.plan(request)

    def download(
        self,
        plan: AcquisitionPlan,
        *,
        policy: RequestPolicy | None = None,
        progress: ProgressCallback | None = None,
    ) -> list[DownloadResult]:
        """Execute an acquisition plan."""

        return self.downloader.download_many(
            plan,
            policy=policy,
        )

    def close(self) -> None:
        """Close client-owned HTTP resources."""

        self.http.close()

    def __enter__(
        self,
    ) -> MarketForgeClient:
        return self

    def __exit__(
        self,
        exc_type,
        exc_value,
        traceback,
    ) -> None:
        self.close()

    def instruments(
        self,
        *,
        exchange: Exchange,
        instrument_type: InstrumentType,
        market_category: MarketCategory,
        data_type: DataType,
    ) -> list[Instrument]:
        """Discover instruments supported by an exchange."""

        source = self.planner.source(exchange)

        return source.instruments(
            instrument_type=instrument_type,
            market_category=market_category,
            data_type=data_type,
        )

    def availability(
        self,
        *,
        target: Instrument | BaseCoin,
        data_type: DataType,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> Availability:
        """Discover historical availability for a target."""

        source = self.planner.source(target.exchange)

        return source.availability(
            target=target,
            data_type=data_type,
            start=start,
            end=end,
        )
