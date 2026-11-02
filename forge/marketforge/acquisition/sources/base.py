from abc import ABC, abstractmethod
from datetime import datetime

from marketforge.models import (
    Availability,
    BaseCoin,
    DataType,
    Instrument,
    InstrumentType,
    MarketCategory,
    RemoteFile,
)


class Source(ABC):
    """Common acquisition interface implemented by every exchange source.

    The interface follows the three stages of historical-data discovery:

    1. instruments()
       What instruments are available for a particular instrument type,
       market category, and dataset?

    2. availability()
       How much historical data exists for an instrument?
       This is optional because some exchanges expose this directly while
       others require expensive iterative API calls or URL probing.

    3. discover_files()
       What exact remote files cover a requested time interval?
       This is the final standardized output consumed by the downloader.

    Exchange implementations may use completely different mechanisms
    internally (HTML listings, APIs, deterministic URLs, HEAD requests,
    etc.), but expose the same interface to the rest of MarketForge.
    """

    @abstractmethod
    def instruments(
        self,
        instrument_type: InstrumentType | None = None,
        market_category: MarketCategory | None = None,
        data_type: DataType | None = None,
    ) -> list[Instrument]:
        """Discover instruments matching the requested filters.

        With no filters, return all instruments known to the source.
        """
        raise NotImplementedError

    def availability(
        self,
        target: Instrument | BaseCoin,
        data_type: DataType,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> Availability | None:
        return None

    @abstractmethod
    def discover_files(
        self,
        target: Instrument | BaseCoin,
        data_type: DataType,
        start: datetime,
        end: datetime,
    ) -> list[RemoteFile]:
        """Return downloadable files covering the requested interval."""
        raise NotImplementedError

    def base_coins(
        self,
        instrument_type: InstrumentType,
        data_type: DataType | None = None,
    ) -> list[str]:
        """Return available base coins when supported by the source."""
        return []

    @abstractmethod
    def supports(
        self,
        instrument_type: InstrumentType,
        market_category: MarketCategory,
        data_type: DataType,
    ) -> bool:
        """Return whether this source supports the requested data combination."""
        raise NotImplementedError
