from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone

from marketforge.acquisition.http import HttpClient, RequestPolicy
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
# Request policy
# ----------------------------------------------------------------------

# Bitget's historical-download endpoints are internal website endpoints.
# Their exact rate limits are not documented.
#
# MarketForge therefore uses conservative throttling.
BITGET_HISTORY_POLICY = RequestPolicy(
    request_interval=2.0,
    max_retries=5,
    backoff=2.0,
)


# ----------------------------------------------------------------------
# Capabilities
# ----------------------------------------------------------------------

BITGET_CAPABILITIES = {
    # Spot
    (
        InstrumentType.SPOT,
        MarketCategory.SPOT,
        DataType.TRADE_TICKS,
    ),
    (
        InstrumentType.SPOT,
        MarketCategory.SPOT,
        DataType.ORDER_BOOK_L2,
    ),
    # Linear perpetuals
    (
        InstrumentType.PERPETUAL,
        MarketCategory.LINEAR,
        DataType.TRADE_TICKS,
    ),
    (
        InstrumentType.PERPETUAL,
        MarketCategory.LINEAR,
        DataType.ORDER_BOOK_L2,
    ),
    # Inverse perpetuals
    (
        InstrumentType.PERPETUAL,
        MarketCategory.INVERSE,
        DataType.TRADE_TICKS,
    ),
    (
        InstrumentType.PERPETUAL,
        MarketCategory.INVERSE,
        DataType.ORDER_BOOK_L2,
    ),
    # Inverse futures
    (
        InstrumentType.FUTURE,
        MarketCategory.INVERSE,
        DataType.TRADE_TICKS,
    ),
    (
        InstrumentType.FUTURE,
        MarketCategory.INVERSE,
        DataType.ORDER_BOOK_L2,
    ),
}


class BitgetSource(Source):
    """Bitget historical market-data source.

    Instrument discovery uses Bitget's official market API:

        GET /api/v3/market/instruments

    Historical archive discovery uses Bitget's internal download-page
    endpoints:

        POST /v1/statistics/public/download/getSymbolList
        POST /v1/statistics/public/download/getPublicDataV2

    Historical endpoint mappings observed:

        businessLine=1 -> Spot
        businessLine=2 -> Futures

        businessType=2 -> Trades
        businessType=3 -> Depth

        dateType=1     -> Daily
        deptType=2     -> Depth configuration

    MarketForge uses URLs returned by getPublicDataV2 directly rather
    than constructing Bitget CDN paths.
    """

    # ------------------------------------------------------------------
    # Official API
    # ------------------------------------------------------------------

    INSTRUMENTS_URL = "https://api.bitget.com/api/v3/market/instruments"

    # ------------------------------------------------------------------
    # Historical download interface
    # ------------------------------------------------------------------

    WEB_BASE = "https://www.bitget.com"

    SYMBOLS_URL = f"{WEB_BASE}" "/v1/statistics/public/download/getSymbolList"

    FILES_URL = f"{WEB_BASE}" "/v1/statistics/public/download/getPublicDataV2"

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    def __init__(
        self,
        http: HttpClient | None = None,
        history_policy: RequestPolicy | None = None,
    ) -> None:
        self.http = http or HttpClient()

        self.history_policy = history_policy or BITGET_HISTORY_POLICY

        self._historical_symbol_cache: dict[
            tuple[str, DataType],
            str,
        ] = {}

    # ------------------------------------------------------------------
    # Public Source interface
    # ------------------------------------------------------------------

    @property
    def exchange(self) -> Exchange:
        return Exchange.BITGET

    def supports(
        self,
        instrument_type: InstrumentType,
        market_category: MarketCategory,
        data_type: DataType,
    ) -> bool:
        """Return whether Bitget supports this MarketForge combination."""

        return (
            instrument_type,
            market_category,
            data_type,
        ) in BITGET_CAPABILITIES

    def instruments(
        self,
        instrument_type: InstrumentType | None = None,
        market_category: MarketCategory | None = None,
        data_type: DataType | None = None,
    ) -> list[Instrument]:
        """Discover Bitget instruments using the official market API."""

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

        instruments: dict[
            tuple[
                InstrumentType,
                MarketCategory,
                str,
            ],
            Instrument,
        ] = {}

        categories = [
            "SPOT",
            "USDT-FUTURES",
            "USDC-FUTURES",
            "COIN-FUTURES",
        ]

        for category in categories:
            response = self.http.get(
                self.INSTRUMENTS_URL,
                params={
                    "category": category,
                },
            )

            payload = response.json()
            self._check_official_response(payload)

            for item in payload.get(
                "data",
                [],
            ):
                instrument = self._instrument_from_api(
                    item=item,
                    requested_category=category,
                )

                if instrument is None:
                    continue

                if (
                    instrument_type is not None
                    and instrument.instrument_type != instrument_type
                ):
                    continue

                if (
                    market_category is not None
                    and instrument.market_category != market_category
                ):
                    continue

                if data_type is not None and not self.supports(
                    instrument.instrument_type,
                    instrument.market_category,
                    data_type,
                ):
                    continue

                key = (
                    instrument.instrument_type,
                    instrument.market_category,
                    instrument.symbol,
                )

                instruments[key] = instrument

        return sorted(
            instruments.values(),
            key=lambda instrument: (
                instrument.instrument_type.value,
                instrument.market_category.value,
                instrument.symbol,
            ),
        )

    def base_coins(
        self,
        instrument_type: InstrumentType,
        data_type: DataType | None = None,
    ) -> list[str]:
        """Bitget does not require BaseCoin grouping in current scope."""

        return []

    def availability(
        self,
        target: Instrument | BaseCoin,
        data_type: DataType,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> Availability | None:
        """Discover Bitget availability over a requested range.

        Bitget does not expose complete lifetime availability through
        one inexpensive request, therefore start/end are required.
        """

        if start is None or end is None:
            raise ValueError("Bitget availability requires start and end dates")

        self._validate_interval(
            start,
            end,
        )

        if isinstance(
            target,
            BaseCoin,
        ):
            raise ValueError("Bitget BaseCoin availability is not supported")

        if not isinstance(
            target,
            Instrument,
        ):
            raise TypeError(
                "Unsupported Bitget availability target: " f"{type(target).__name__}"
            )

        self._validate_supported(
            target,
            data_type,
        )

        files = self._discover_history_files(
            instrument=target,
            data_type=data_type,
            start=start,
            end=end,
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
        """Discover Bitget historical files covering [start, end)."""

        self._validate_interval(
            start,
            end,
        )

        if isinstance(
            target,
            BaseCoin,
        ):
            raise ValueError("Bitget BaseCoin file discovery is not supported")

        if not isinstance(
            target,
            Instrument,
        ):
            raise TypeError(
                "Unsupported Bitget discovery target: " f"{type(target).__name__}"
            )

        self._validate_supported(
            target,
            data_type,
        )

        return self._discover_history_files(
            instrument=target,
            data_type=data_type,
            start=start,
            end=end,
        )

    # ------------------------------------------------------------------
    # Official instrument discovery
    # ------------------------------------------------------------------

    def _instrument_from_api(
        self,
        item: dict,
        requested_category: str,
    ) -> Instrument | None:
        """Convert Bitget instrument metadata into MarketForge.

        Bitget's `type` field identifies the contract lifecycle:

            perpetual -> InstrumentType.PERPETUAL
            delivery  -> InstrumentType.FUTURE

        `symbolType` describes the underlying asset class
        (crypto, stock, metal, commodity, etc.) and must not be used
        to determine perpetual vs dated futures.
        """

        symbol = item.get("symbol")

        if not symbol:
            return None

        # --------------------------------------------------------------
        # Spot
        # --------------------------------------------------------------

        if requested_category == "SPOT":
            return Instrument(
                exchange=self.exchange,
                instrument_type=InstrumentType.SPOT,
                market_category=MarketCategory.SPOT,
                symbol=symbol,
            )

        # --------------------------------------------------------------
        # Futures contract type
        # --------------------------------------------------------------

        contract_type = str(
            item.get(
                "type",
                "",
            )
        ).lower()

        if contract_type == "perpetual":
            instrument_type = InstrumentType.PERPETUAL

        elif contract_type == "delivery":
            instrument_type = InstrumentType.FUTURE

        else:
            return None

        # --------------------------------------------------------------
        # Linear / inverse
        # --------------------------------------------------------------

        if requested_category in {
            "USDT-FUTURES",
            "USDC-FUTURES",
        }:
            market_category = MarketCategory.LINEAR

        elif requested_category == "COIN-FUTURES":
            market_category = MarketCategory.INVERSE

        else:
            return None

        return Instrument(
            exchange=self.exchange,
            instrument_type=instrument_type,
            market_category=market_category,
            symbol=symbol,
        )

    # ------------------------------------------------------------------
    # Historical symbol search
    # ------------------------------------------------------------------

    def _historical_symbols(
        self,
        search: str,
        business_line: int,
        data_type: DataType,
    ) -> list[str]:
        """Search Bitget's historical-download symbol interface.

        getSymbolList requires a non-empty displaySymbol search term.
        It is therefore not used as MarketForge's canonical instrument
        enumeration mechanism.
        """

        if not search:
            raise ValueError("Bitget historical symbol search cannot be empty")

        payload = {
            "displaySymbol": search,
            "businessLine": business_line,
            "businessType": self._business_type(data_type),
            "languageType": 12,
        }

        response = self.http.post(
            self.SYMBOLS_URL,
            json=payload,
            policy=self.history_policy,
        )

        body = response.json()
        self._check_web_response(body)

        symbols: list[str] = []

        for item in self._extract_symbol_items(body):
            symbol = self._extract_symbol(item)

            if symbol:
                symbols.append(symbol)

        return sorted(set(symbols))

    @staticmethod
    def _extract_symbol_items(
        payload: dict,
    ) -> list:
        """Extract symbol records from getSymbolList."""

        data = payload.get("data")

        if isinstance(
            data,
            list,
        ):
            return data

        if isinstance(
            data,
            dict,
        ):
            for key in (
                "list",
                "data",
                "symbols",
            ):
                value = data.get(key)

                if isinstance(
                    value,
                    list,
                ):
                    return value

        return []

    @staticmethod
    def _extract_symbol(
        item,
    ) -> str | None:
        """Extract Bitget's historical display symbol."""

        if isinstance(
            item,
            str,
        ):
            return item

        if not isinstance(
            item,
            dict,
        ):
            return None

        for key in (
            "displaySymbol",
            "symbol",
            "symbolName",
            "instId",
        ):
            value = item.get(key)

            if (
                isinstance(
                    value,
                    str,
                )
                and value
            ):
                return value

        return None

    # ------------------------------------------------------------------
    # Historical file discovery
    # ------------------------------------------------------------------

    def _discover_history_files(
        self,
        instrument: Instrument,
        data_type: DataType,
        start: datetime,
        end: datetime,
    ) -> list[RemoteFile]:
        """Discover Bitget historical files through getPublicDataV2."""

        files: dict[
            str,
            RemoteFile,
        ] = {}

        for (
            window_start,
            window_end,
        ) in self._history_windows(
            start,
            end,
        ):
            payload = self._history_payload(
                instrument=instrument,
                data_type=data_type,
                start=window_start,
                end=window_end,
            )

            response = self.http.post(
                self.FILES_URL,
                json=payload,
                policy=self.history_policy,
            )

            body = response.json()
            self._check_web_response(body)

            for item in self._extract_file_items(body):
                remote_file = self._remote_file(
                    instrument=instrument,
                    data_type=data_type,
                    item=item,
                )

                if remote_file is None:
                    continue

                if not self._overlaps(
                    remote_file.start,
                    remote_file.end,
                    start,
                    end,
                ):
                    continue

                files[remote_file.url] = remote_file

        return sorted(
            files.values(),
            key=lambda file: file.start,
        )

    def _history_payload(
        self,
        instrument: Instrument,
        data_type: DataType,
        start: datetime,
        end: datetime,
    ) -> dict:
        """Build getPublicDataV2 request body."""

        business_line = 1 if instrument.instrument_type == InstrumentType.SPOT else 2

        begin_date, end_date = self._historical_date_bounds(
            start,
            end,
        )

        payload = {
            "displaySymbol": [
                self._historical_display_symbol(
                    instrument,
                    data_type,
                )
            ],
            "businessLine": business_line,
            "businessType": self._business_type(data_type),
            "dateType": 1,
            "beginTimeStr": begin_date,
            "endTimeStr": end_date,
        }

        if data_type == DataType.ORDER_BOOK_L2:
            payload["deptType"] = 2

        return payload

    def _historical_display_symbol(
        self,
        instrument: Instrument,
        data_type: DataType,
    ) -> str:
        """Resolve Bitget's historical-download display symbol."""

        # Futures use the same symbol representation in the historical API.
        if instrument.instrument_type != InstrumentType.SPOT:
            return instrument.symbol

        # Already in historical display form.
        if "/" in instrument.symbol:
            return instrument.symbol

        cache_key = (
            instrument.symbol,
            data_type,
        )

        # --------------------------------------------------------------
        # Cache
        # --------------------------------------------------------------

        cached = self._historical_symbol_cache.get(cache_key)

        if cached is not None:
            return cached

        # --------------------------------------------------------------
        # Ask Bitget for the authoritative historical representation.
        # --------------------------------------------------------------

        historical_symbols = self._historical_symbols(
            search=instrument.symbol,
            business_line=1,
            data_type=data_type,
        )

        matches = [
            symbol
            for symbol in historical_symbols
            if symbol.replace("/", "") == instrument.symbol
        ]

        if not matches:
            raise ValueError(
                "No Bitget historical symbol mapping found for " f"{instrument.symbol}"
            )

        if len(matches) > 1:
            raise ValueError(
                "Ambiguous Bitget historical symbol mapping for "
                f"{instrument.symbol}: {matches}"
            )

        historical_symbol = matches[0]

        # --------------------------------------------------------------
        # Store successful mapping.
        # --------------------------------------------------------------

        self._historical_symbol_cache[cache_key] = historical_symbol

        return historical_symbol

    # ------------------------------------------------------------------
    # Historical response parsing
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_file_items(
        payload: dict,
    ) -> list:
        """Extract historical file records from Bitget response."""

        data = payload.get("data")

        if isinstance(
            data,
            list,
        ):
            return data

        if isinstance(
            data,
            dict,
        ):
            for key in (
                "list",
                "data",
                "files",
                "records",
            ):
                value = data.get(key)

                if isinstance(
                    value,
                    list,
                ):
                    return value

        return []

    def _remote_file(
        self,
        instrument: Instrument,
        data_type: DataType,
        item,
    ) -> RemoteFile | None:
        """Convert Bitget historical file metadata into RemoteFile."""

        if not isinstance(
            item,
            dict,
        ):
            return None

        url = item.get("fileUrl")

        filename = item.get("fileName")

        if not url or not filename:
            return None

        # Bitget explicitly provides the UTC day represented by the file.
        timestamp = item.get("dateTime")

        date_string = item.get("dateTimeStr")

        if timestamp is not None:
            file_start = datetime.fromtimestamp(
                int(timestamp) / 1000,
                tz=timezone.utc,
            )

        elif date_string:
            file_start = datetime.strptime(
                date_string,
                "%Y-%m-%d",
            ).replace(tzinfo=timezone.utc)

        else:
            # Defensive fallback for unexpected future responses.
            interval = self._file_interval(
                filename=filename,
                item=item,
            )

            if interval is None:
                return None

            file_start, file_end = interval

            return RemoteFile(
                exchange=self.exchange,
                instrument_type=instrument.instrument_type,
                market_category=instrument.market_category,
                data_type=data_type,
                symbol=instrument.symbol,
                start=file_start,
                end=file_end,
                url=url,
                filename=filename,
                size_bytes=None,
            )

        file_end = file_start + timedelta(days=1)

        return RemoteFile(
            exchange=self.exchange,
            instrument_type=instrument.instrument_type,
            market_category=instrument.market_category,
            data_type=data_type,
            symbol=instrument.symbol,
            start=file_start,
            end=file_end,
            url=url,
            filename=filename,
            size_bytes=None,
        )

    # ------------------------------------------------------------------
    # Bitget mappings
    # ------------------------------------------------------------------

    @staticmethod
    def _business_type(
        data_type: DataType,
    ) -> int:
        """Map MarketForge DataType to Bitget businessType."""

        if data_type == DataType.TRADE_TICKS:
            return 2

        if data_type == DataType.ORDER_BOOK_L2:
            return 3

        raise ValueError("Unsupported Bitget data type: " f"{data_type.value}")

    # ------------------------------------------------------------------
    # Date parsing
    # ------------------------------------------------------------------

    @staticmethod
    def _file_interval(
        filename: str,
        item: dict,
    ) -> (
        tuple[
            datetime,
            datetime,
        ]
        | None
    ):
        """Extract a daily interval from Bitget file metadata/name."""

        match = re.search(
            r"(?P<date>\d{4}[-_]?\d{2}[-_]?\d{4})",
            filename,
        )

        if match is None:
            for key in (
                "date",
                "dateStr",
                "dataDate",
            ):
                value = item.get(key)

                if not isinstance(
                    value,
                    str,
                ):
                    continue

                match = re.search(
                    r"(?P<date>\d{4}[-_]?\d{2}[-_]?\d{2})",
                    value,
                )

                if match is not None:
                    break

        if match is None:
            return None

        raw_date = match.group("date").replace("_", "-")

        if "-" not in raw_date:
            day = datetime.strptime(
                raw_date,
                "%Y%m%d",
            )

        else:
            day = datetime.strptime(
                raw_date,
                "%Y-%m-%d",
            )

        day = day.replace(tzinfo=timezone.utc)

        return (
            day,
            day + timedelta(days=1),
        )

    # ------------------------------------------------------------------
    # Historical request windows
    # ------------------------------------------------------------------

    @staticmethod
    def _history_windows(
        start: datetime,
        end: datetime,
    ):
        """Yield conservative 7-day MarketForge [start, end) windows."""

        current = start

        while current < end:
            window_end = min(
                current + timedelta(days=7),
                end,
            )

            yield (
                current,
                window_end,
            )

            current = window_end

    # ------------------------------------------------------------------
    # Response helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _first_string(
        item: dict,
        keys: tuple[str, ...],
    ) -> str | None:
        """Return the first non-empty string under candidate keys."""

        for key in keys:
            value = item.get(key)

            if (
                isinstance(
                    value,
                    str,
                )
                and value
            ):
                return value

        return None

    @staticmethod
    def _file_size(
        item: dict,
    ) -> int | None:
        """Extract file size when Bitget provides one."""

        for key in (
            "size",
            "fileSize",
            "sizeBytes",
        ):
            value = item.get(key)

            if isinstance(
                value,
                int,
            ):
                return value

            if isinstance(
                value,
                float,
            ):
                return int(value)

            if isinstance(
                value,
                str,
            ):
                try:
                    return int(value)

                except ValueError:
                    continue

        return None

    # ------------------------------------------------------------------
    # API response validation
    # ------------------------------------------------------------------

    @staticmethod
    def _check_official_response(
        payload: dict,
    ) -> None:
        """Validate Bitget official API responses."""

        code = str(
            payload.get(
                "code",
                "",
            )
        )

        if code != "00000":
            raise ValueError("Bitget API error " f"{code}: " f"{payload.get('msg')}")

    @staticmethod
    def _check_web_response(
        payload: dict,
    ) -> None:
        """Validate Bitget historical-download web API responses.

        The internal historical interface uses code="200" for
        successful responses, unlike the official API's "00000".
        """

        code = str(
            payload.get(
                "code",
                "",
            )
        )

        if code != "200":
            raise ValueError(
                "Bitget historical API error " f"{code}: " f"{payload.get('msg')}"
            )

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def _validate_supported(
        self,
        instrument: Instrument,
        data_type: DataType,
    ) -> None:
        """Reject unsupported Bitget acquisition combinations."""

        if not self.supports(
            instrument.instrument_type,
            instrument.market_category,
            data_type,
        ):
            raise ValueError("Unsupported Bitget data combination")

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

    @staticmethod
    def _historical_date_bounds(
        start: datetime,
        end: datetime,
    ) -> tuple[str, str]:
        """Convert MarketForge [start, end) to Bitget inclusive dates."""

        if start >= end:
            raise ValueError("start must be earlier than end")

        begin_date = start.date()

        # Bitget's end date is inclusive.
        #
        # If MarketForge's exclusive end is exactly midnight,
        # that calendar day contains no requested data.
        if (
            end.hour == 0
            and end.minute == 0
            and end.second == 0
            and end.microsecond == 0
        ):
            end_date = (end - timedelta(days=1)).date()

        # If the exclusive end is intraday, that day contains
        # requested data and therefore must be included.
        else:
            end_date = end.date()

        return (
            begin_date.isoformat(),
            end_date.isoformat(),
        )
