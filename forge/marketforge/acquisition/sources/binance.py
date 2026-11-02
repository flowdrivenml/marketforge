from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone

from marketforge.acquisition.http import HttpClient
from marketforge.acquisition.sources.base import Source
from marketforge.models import (
    Availability,
    BaseCoin,
    DataType,
    Exchange,
    Instrument,
    InstrumentType,
    MarketCategory,
    RemoteFile,
)

# ----------------------------------------------------------------------
# Capabilities
# ----------------------------------------------------------------------

BINANCE_CAPABILITIES = {
    # Spot trades
    (
        InstrumentType.SPOT,
        MarketCategory.SPOT,
        DataType.TRADE_TICKS,
    ),
    # Linear perpetual trades
    (
        InstrumentType.PERPETUAL,
        MarketCategory.LINEAR,
        DataType.TRADE_TICKS,
    ),
    # Linear dated futures
    (
        InstrumentType.FUTURE,
        MarketCategory.LINEAR,
        DataType.TRADE_TICKS,
    ),
    # Inverse perpetual trades
    (
        InstrumentType.PERPETUAL,
        MarketCategory.INVERSE,
        DataType.TRADE_TICKS,
    ),
    # Inverse dated futures
    (
        InstrumentType.FUTURE,
        MarketCategory.INVERSE,
        DataType.TRADE_TICKS,
    ),
}


class BinanceSource(Source):
    """Binance historical market-data source.

    Instrument discovery:
        Spot    -> api.binance.com
        Linear  -> fapi.binance.com
        Inverse -> dapi.binance.com

    Historical trade discovery:
        Spot    -> data/spot/daily/trades/
        Linear  -> data/futures/um/daily/trades/
        Inverse -> data/futures/cm/daily/trades/

    Binance's data.binance.vision website renders its file listing
    dynamically using the underlying public S3 bucket. MarketForge
    queries that S3 listing directly rather than executing browser
    JavaScript.

    Current MarketForge Binance scope:
        - Spot trade ticks
        - Linear perpetual/future trade ticks
        - Inverse perpetual/future trade ticks

    L2 order-book and option archives are not currently supported.
    """

    # ------------------------------------------------------------------
    # API endpoints
    # ------------------------------------------------------------------

    SPOT_EXCHANGE_INFO = "https://api.binance.com/api/v3/exchangeInfo"

    LINEAR_EXCHANGE_INFO = "https://fapi.binance.com/fapi/v1/exchangeInfo"

    INVERSE_EXCHANGE_INFO = "https://dapi.binance.com/dapi/v1/exchangeInfo"

    # ------------------------------------------------------------------
    # Historical archive
    # ------------------------------------------------------------------

    ARCHIVE_BASE = "https://data.binance.vision/"

    S3_BUCKET_URL = "https://s3-ap-northeast-1.amazonaws.com/" "data.binance.vision"

    ARCHIVE_PREFIXES = {
        MarketCategory.SPOT: ("data/spot/daily/trades/"),
        MarketCategory.LINEAR: ("data/futures/um/daily/trades/"),
        MarketCategory.INVERSE: ("data/futures/cm/daily/trades/"),
    }

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    def __init__(
        self,
        http: HttpClient | None = None,
    ) -> None:
        self.http = http or HttpClient()

    # ------------------------------------------------------------------
    # Public Source interface
    # ------------------------------------------------------------------

    @property
    def exchange(self) -> Exchange:
        return Exchange.BINANCE

    def supports(
        self,
        instrument_type: InstrumentType,
        market_category: MarketCategory,
        data_type: DataType,
    ) -> bool:
        """Return whether Binance supports this data combination."""

        return (
            instrument_type,
            market_category,
            data_type,
        ) in BINANCE_CAPABILITIES

    def instruments(
        self,
        instrument_type: InstrumentType | None = None,
        market_category: MarketCategory | None = None,
        data_type: DataType | None = None,
    ) -> list[Instrument]:
        """Discover Binance instruments matching the requested filters."""

        # If all dimensions are known, reject unsupported combinations
        # before making unnecessary network requests.
        if (
            instrument_type is not None
            and market_category is not None
            and data_type is not None
            and not self.supports(
                instrument_type,
                market_category,
                data_type,
            )
        ):
            return []

        categories = (
            [market_category]
            if market_category is not None
            else [
                MarketCategory.SPOT,
                MarketCategory.LINEAR,
                MarketCategory.INVERSE,
            ]
        )

        instruments: list[Instrument] = []

        for category in categories:
            # Binance options are outside the current MarketForge scope.
            if category == MarketCategory.OPTION:
                continue

            category_instruments = self._api_instruments(category)

            if instrument_type is not None:
                category_instruments = [
                    instrument
                    for instrument in category_instruments
                    if instrument.instrument_type == instrument_type
                ]

            if data_type is not None:
                category_instruments = [
                    instrument
                    for instrument in category_instruments
                    if self.supports(
                        instrument.instrument_type,
                        instrument.market_category,
                        data_type,
                    )
                ]

            instruments.extend(category_instruments)

        return instruments

    def base_coins(
        self,
        instrument_type: InstrumentType,
        data_type: DataType | None = None,
    ) -> list[str]:
        """Base-coin discovery is not required for Binance."""

        return []

    def availability(
        self,
        target: Instrument | BaseCoin,
        data_type: DataType,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> Availability | None:
        """Discover complete Binance historical availability."""

        if isinstance(target, BaseCoin):
            raise ValueError("Binance BaseCoin availability is not supported")

        if not isinstance(target, Instrument):
            raise TypeError(
                "Unsupported Binance availability target: " f"{type(target).__name__}"
            )

        if not self.supports(
            target.instrument_type,
            target.market_category,
            data_type,
        ):
            raise ValueError("Unsupported Binance data combination")

        files = self._discover_all_archive_files(
            instrument=target,
            data_type=data_type,
        )

        return Availability(
            exchange=self.exchange,
            target=target,
            data_type=data_type,
            files=tuple(files),
        )

    def discover_files(
        self,
        target: Instrument | BaseCoin,
        data_type: DataType,
        start: datetime,
        end: datetime,
    ) -> list[RemoteFile]:
        """Discover Binance files covering [start, end)."""

        if isinstance(target, BaseCoin):
            raise ValueError("Binance BaseCoin file discovery is not supported")

        if not isinstance(target, Instrument):
            raise TypeError(
                "Unsupported Binance discovery target: " f"{type(target).__name__}"
            )

        if not self.supports(
            target.instrument_type,
            target.market_category,
            data_type,
        ):
            raise ValueError("Unsupported Binance data combination")

        self._validate_interval(
            start,
            end,
        )

        files = self._discover_all_archive_files(
            instrument=target,
            data_type=data_type,
        )

        return [
            file
            for file in files
            if self._overlaps(
                file.start,
                file.end,
                start,
                end,
            )
        ]

    # ------------------------------------------------------------------
    # Instrument discovery
    # ------------------------------------------------------------------

    def _api_instruments(
        self,
        category: MarketCategory,
    ) -> list[Instrument]:
        """Retrieve instruments from the appropriate exchangeInfo API."""

        url = self._exchange_info_url(category)

        response = self.http.get(url)
        payload = response.json()

        instruments: list[Instrument] = []

        for item in payload.get(
            "symbols",
            [],
        ):
            instrument = self._instrument_from_api(
                category=category,
                item=item,
            )

            if instrument is not None:
                instruments.append(instrument)

        return instruments

    def _instrument_from_api(
        self,
        category: MarketCategory,
        item: dict,
    ) -> Instrument | None:
        """Convert Binance instrument metadata to MarketForge."""

        symbol = item.get("symbol")

        if not symbol:
            return None

        if category == MarketCategory.SPOT:
            instrument_type = InstrumentType.SPOT

        else:
            contract_type = item.get("contractType")

            instrument_type = self._instrument_type(contract_type)

            # Ignore contract types MarketForge does not understand.
            if instrument_type is None:
                return None

        return Instrument(
            exchange=self.exchange,
            instrument_type=instrument_type,
            market_category=category,
            symbol=symbol,
        )

    @staticmethod
    def _instrument_type(
        contract_type: str | None,
    ) -> InstrumentType | None:
        """Map Binance contract types into MarketForge types."""

        if contract_type == "PERPETUAL":
            return InstrumentType.PERPETUAL

        if contract_type in {
            "CURRENT_QUARTER",
            "NEXT_QUARTER",
            "CURRENT_QUARTER_DELIVERING",
            "NEXT_QUARTER_DELIVERING",
        }:
            return InstrumentType.FUTURE

        return None

    def _exchange_info_url(
        self,
        category: MarketCategory,
    ) -> str:
        """Return the exchangeInfo endpoint for a category."""

        if category == MarketCategory.SPOT:
            return self.SPOT_EXCHANGE_INFO

        if category == MarketCategory.LINEAR:
            return self.LINEAR_EXCHANGE_INFO

        if category == MarketCategory.INVERSE:
            return self.INVERSE_EXCHANGE_INFO

        raise ValueError("Unsupported Binance market category: " f"{category.value}")

    # ------------------------------------------------------------------
    # Historical archive discovery
    # ------------------------------------------------------------------

    def _discover_all_archive_files(
        self,
        instrument: Instrument,
        data_type: DataType,
    ) -> list[RemoteFile]:
        """Enumerate all daily Binance trade files for an instrument.

        The data.binance.vision frontend loads its listing dynamically
        from Binance's public S3 bucket.

        MarketForge queries that bucket directly and follows S3
        pagination until all objects under the symbol prefix have
        been discovered.
        """

        if data_type != DataType.TRADE_TICKS:
            raise ValueError(
                "Binance historical L2 order-book data " "is not supported"
            )

        prefix = self._archive_prefix(instrument.market_category)

        symbol_prefix = f"{prefix}{instrument.symbol}/"

        objects = self._list_s3_objects(
            prefix=symbol_prefix,
        )

        files: list[RemoteFile] = []

        for object_key, size_bytes in objects:
            # Binance publishes a CHECKSUM object beside each archive:
            #
            # BTCUSDT-trades-2026-09-28.zip
            # BTCUSDT-trades-2026-09-28.zip.CHECKSUM
            #
            # Only the actual ZIP archive is acquisition data.
            if not object_key.endswith(".zip"):
                continue

            filename = object_key.rsplit(
                "/",
                1,
            )[-1]

            file_interval = self._parse_daily_interval(filename)

            if file_interval is None:
                continue

            file_start, file_end = file_interval

            files.append(
                RemoteFile(
                    exchange=self.exchange,
                    instrument_type=(instrument.instrument_type),
                    market_category=(instrument.market_category),
                    data_type=data_type,
                    symbol=instrument.symbol,
                    start=file_start,
                    end=file_end,
                    url=self._direct_file_url(object_key),
                    filename=filename,
                    size_bytes=size_bytes,
                )
            )

        return sorted(
            files,
            key=lambda file: file.start,
        )

    # ------------------------------------------------------------------
    # S3 listing
    # ------------------------------------------------------------------

    def _list_s3_objects(
        self,
        prefix: str,
    ) -> list[tuple[str, int]]:
        """List every Binance S3 object under a prefix.

        Binance's S3 listing returns at most 1000 objects per response.

        When IsTruncated=true, NextMarker is supplied and must be used
        in the next request. Continue until the complete prefix has
        been enumerated.
        """

        objects: list[tuple[str, int]] = []

        marker: str | None = None

        while True:
            params = {
                "delimiter": "/",
                "prefix": prefix,
            }

            if marker is not None:
                params["marker"] = marker

            response = self.http.get(
                self.S3_BUCKET_URL,
                params=params,
            )

            (
                page_objects,
                is_truncated,
                next_marker,
            ) = self._parse_s3_listing(response.text)

            objects.extend(page_objects)

            if not is_truncated:
                break

            if not next_marker:
                raise ValueError(
                    "Binance S3 response is truncated " "but did not provide NextMarker"
                )

            marker = next_marker

        return objects

    @staticmethod
    def _parse_s3_listing(
        xml: str,
    ) -> tuple[
        list[tuple[str, int]],
        bool,
        str | None,
    ]:
        """Parse a Binance S3 ListBucket XML response.

        Returns:
            objects:
                List of (object_key, size_bytes).

            is_truncated:
                Whether another S3 page exists.

            next_marker:
                Marker required to request the next page.
        """

        root = ET.fromstring(xml)

        namespace = {"s3": ("http://s3.amazonaws.com/" "doc/2006-03-01/")}

        objects: list[tuple[str, int]] = []

        for content in root.findall(
            "s3:Contents",
            namespace,
        ):
            key_element = content.find(
                "s3:Key",
                namespace,
            )

            size_element = content.find(
                "s3:Size",
                namespace,
            )

            if key_element is None or key_element.text is None:
                continue

            key = key_element.text

            size_bytes = 0

            if size_element is not None and size_element.text is not None:
                size_bytes = int(size_element.text)

            objects.append(
                (
                    key,
                    size_bytes,
                )
            )

        truncated_element = root.find(
            "s3:IsTruncated",
            namespace,
        )

        is_truncated = (
            truncated_element is not None and truncated_element.text == "true"
        )

        marker_element = root.find(
            "s3:NextMarker",
            namespace,
        )

        next_marker = (
            marker_element.text
            if (marker_element is not None and marker_element.text)
            else None
        )

        return (
            objects,
            is_truncated,
            next_marker,
        )

    # ------------------------------------------------------------------
    # Archive paths
    # ------------------------------------------------------------------

    def _archive_prefix(
        self,
        category: MarketCategory,
    ) -> str:
        """Return Binance's S3 prefix for a market category."""

        try:
            return self.ARCHIVE_PREFIXES[category]

        except KeyError as exc:
            raise ValueError(
                "No Binance historical archive for " f"{category.value}"
            ) from exc

    def _direct_file_url(
        self,
        object_key: str,
    ) -> str:
        """Convert an S3 object key into its public download URL."""

        return f"{self.ARCHIVE_BASE}" f"{object_key}"

    # ------------------------------------------------------------------
    # Filename parsing
    # ------------------------------------------------------------------

    DAILY_DATE = re.compile(r"(?P<date>\d{4}-\d{2}-\d{2})")

    @classmethod
    def _parse_daily_interval(
        cls,
        filename: str,
    ) -> (
        tuple[
            datetime,
            datetime,
        ]
        | None
    ):
        """Parse the daily interval represented by a Binance filename.

        Example:

            BTCUSDT-trades-2026-09-28.zip

        becomes:

            [2026-09-28 00:00 UTC,
             2026-09-29 00:00 UTC)
        """

        match = cls.DAILY_DATE.search(filename)

        if match is None:
            return None

        day = datetime.strptime(
            match.group("date"),
            "%Y-%m-%d",
        ).replace(tzinfo=timezone.utc)

        return (
            day,
            day + timedelta(days=1),
        )

    # ------------------------------------------------------------------
    # General helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _validate_interval(
        start: datetime,
        end: datetime,
    ) -> None:
        """Validate MarketForge's [start, end) interval."""

        if start >= end:
            raise ValueError("start must be earlier than end")

    @staticmethod
    def _overlaps(
        file_start: datetime,
        file_end: datetime,
        requested_start: datetime,
        requested_end: datetime,
    ) -> bool:
        """Return whether two half-open intervals overlap."""

        return file_start < requested_end and file_end > requested_start
