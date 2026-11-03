from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from marketforge.acquisition.http import HttpClient, RequestPolicy
from marketforge.acquisition.sources.base import Source
from marketforge.models import (
    Availability,
    BaseCoin,
    DataType,
    Instrument,
    InstrumentType,
    MarketCategory,
    RemoteFile,
)

BYBIT_OPTION_HISTORY_POLICY = RequestPolicy(
    request_interval=2.0,
    max_retries=5,
    backoff=1.0,
)

BYBIT_WEB_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:156.0) "
        "Gecko/20100101 Firefox/156.0"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.bybit.com/derivatives/en/history-data",
}

BYBIT_CAPABILITIES = {
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
    # Linear futures
    (
        InstrumentType.FUTURE,
        MarketCategory.LINEAR,
        DataType.TRADE_TICKS,
    ),
    (
        InstrumentType.FUTURE,
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
    # Options
    (
        InstrumentType.OPTION,
        MarketCategory.OPTION,
        DataType.TRADE_TICKS,
    ),
    (
        InstrumentType.OPTION,
        MarketCategory.OPTION,
        DataType.ORDER_BOOK_L2,
    ),
}


class BybitSource(Source):
    """Bybit historical market-data source.

    Discovery is split between:

    - Bybit V5 API for current instrument metadata.
    - Browsable public archives for spot/linear/inverse historical files.
    - Bybit's historical download endpoint for option files.

    The source performs discovery only. It does not download archive
    contents.
    """

    API_BASE = "https://api.bybit.com"

    TRADE_ROOTS = {
        MarketCategory.SPOT: "https://public.bybit.com/spot/",
        MarketCategory.LINEAR: "https://public.bybit.com/trading/",
        MarketCategory.INVERSE: "https://public.bybit.com/trading/",
    }

    BOOK_ROOTS = {
        MarketCategory.SPOT: "https://quote-saver.bycsi.com/orderbook/spot/",
        MarketCategory.LINEAR: "https://quote-saver.bycsi.com/orderbook/linear/",
        MarketCategory.INVERSE: "https://quote-saver.bycsi.com/orderbook/inverse/",
    }

    OPTION_FILES_URL = (
        "https://www.bybit.com/x-api/quote/public/support/download/list-files"
    )

    def __init__(
        self,
        http: HttpClient | None = None,
        option_history_policy: RequestPolicy | None = None,
    ) -> None:
        self.http = http or HttpClient()

        self.option_history_policy = (
            option_history_policy or BYBIT_OPTION_HISTORY_POLICY
        )

    # ------------------------------------------------------------------
    # Public Source interface
    # ------------------------------------------------------------------
    def supports(
        self,
        instrument_type: InstrumentType,
        market_category: MarketCategory,
        data_type: DataType,
    ) -> bool:
        return (
            instrument_type,
            market_category,
            data_type,
        ) in BYBIT_CAPABILITIES

    def instruments(
        self,
        instrument_type: InstrumentType | None = None,
        market_category: MarketCategory | None = None,
        data_type: DataType | None = None,
    ) -> list[Instrument]:
        """Discover current Bybit instruments.

        data_type is accepted because it is part of the common Source
        interface. Bybit's V5 instrument endpoint itself is not
        dataset-specific, so historical archive availability is used
        when dataset-specific confirmation is required.
        """

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
            [market_category] if market_category is not None else list(MarketCategory)
        )

        instruments: list[Instrument] = []

        for category in categories:
            category_instruments = self._api_instruments(category)

            if instrument_type is not None:
                category_instruments = [
                    instrument
                    for instrument in category_instruments
                    if instrument.instrument_type == instrument_type
                ]

            instruments.extend(category_instruments)

        if data_type is not None:
            instruments = [
                instrument
                for instrument in instruments
                if self._supports_data_type(instrument, data_type)
            ]

        return instruments

    def base_coins(
        self,
        instrument_type: InstrumentType,
        data_type: DataType | None = None,
    ) -> list[str]:
        """Return available Bybit option base coins."""

        if instrument_type != InstrumentType.OPTION:
            return []

        response = self.http.get(f"{self.API_BASE}/v5/market/option-base-coins")

        payload = response.json()
        self._check_api_response(payload)

        items = payload["result"]["list"]

        return [item["baseCoin"] for item in items if item.get("baseCoin")]

    def availability(
        self,
        target: Instrument | BaseCoin,
        data_type: DataType,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> Availability | None:

        if isinstance(target, BaseCoin):
            if not self.supports(
                InstrumentType.OPTION,
                MarketCategory.OPTION,
                data_type,
            ):
                raise ValueError("Unsupported Bybit option data combination")

            if start is None or end is None:
                raise ValueError(
                    "Option availability currently requires start and end dates"
                )

            return self._option_availability(
                base_coin=target,
                data_type=data_type,
                start=start,
                end=end,
            )

        if isinstance(target, Instrument):
            if not self.supports(
                target.instrument_type,
                target.market_category,
                data_type,
            ):
                raise ValueError("Unsupported Bybit data combination")

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

        raise TypeError(f"Unsupported availability target: {type(target).__name__}")

    def discover_files(
        self,
        target: Instrument | BaseCoin,
        data_type: DataType,
        start: datetime,
        end: datetime,
    ) -> list[RemoteFile]:
        """Discover downloadable Bybit files covering [start, end)."""

        if isinstance(target, Instrument):
            if not self.supports(
                target.instrument_type,
                target.market_category,
                data_type,
            ):
                raise ValueError("Unsupported Bybit data combination")

        if isinstance(target, BaseCoin):
            if not self.supports(
                InstrumentType.OPTION,
                MarketCategory.OPTION,
                data_type,
            ):
                raise ValueError("Unsupported Bybit option data combination")

        self._validate_interval(start, end)

        if isinstance(target, BaseCoin):
            availability = self._option_availability(
                base_coin=target,
                data_type=data_type,
                start=start,
                end=end,
            )

            return list(availability.files)

        return self._discover_archive_files(
            instrument=target,
            data_type=data_type,
            start=start,
            end=end,
        )

    # ------------------------------------------------------------------
    # Instrument discovery
    # ------------------------------------------------------------------

    def _api_instruments(
        self,
        category: MarketCategory,
    ) -> list[Instrument]:
        url = f"{self.API_BASE}/v5/market/instruments-info"

        params: dict[str, str | int] = {
            "category": category.value,
        }

        if category != MarketCategory.SPOT:
            params["limit"] = 1000

        if category == MarketCategory.OPTION:
            params["baseCoin"] = "All"

        instruments: list[Instrument] = []
        cursor: str | None = None

        while True:
            request_params = dict(params)

            if cursor:
                request_params["cursor"] = cursor

            response = self.http.get(
                url,
                params=request_params,
            )

            payload = response.json()
            self._check_api_response(payload)

            result = payload["result"]

            for item in result.get("list", []):
                instruments.append(
                    self._instrument_from_api(
                        category=category,
                        item=item,
                    )
                )

            cursor = result.get("nextPageCursor")

            if not cursor:
                break

            # Spot does not use cursor pagination.
            if category == MarketCategory.SPOT:
                break

        return instruments

    def _instrument_from_api(
        self,
        category: MarketCategory,
        item: dict,
    ) -> Instrument:
        instrument_type = self._instrument_type(
            category=category,
            contract_type=item.get("contractType"),
        )

        return Instrument(
            exchange=self.exchange,
            instrument_type=instrument_type,
            market_category=category,
            symbol=item["symbol"],
        )

    @staticmethod
    def _instrument_type(
        category: MarketCategory,
        contract_type: str | None,
    ) -> InstrumentType:
        if category == MarketCategory.SPOT:
            return InstrumentType.SPOT

        if category == MarketCategory.OPTION:
            return InstrumentType.OPTION

        if contract_type and "Perpetual" in contract_type:
            return InstrumentType.PERPETUAL

        if contract_type and "Futures" in contract_type:
            return InstrumentType.FUTURE

        raise ValueError(f"Unknown Bybit contract type: {contract_type!r}")

    # ------------------------------------------------------------------
    # Browsable archive discovery
    # ------------------------------------------------------------------

    def _discover_archive_files(
        self,
        instrument: Instrument,
        data_type: DataType,
        start: datetime,
        end: datetime,
    ) -> list[RemoteFile]:
        """Return archive files overlapping the requested interval."""

        files = self._discover_all_archive_files(
            instrument=instrument,
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

    def _archive_root(
        self,
        category: MarketCategory,
        data_type: DataType,
    ) -> str:
        if data_type == DataType.TRADE_TICKS:
            roots = self.TRADE_ROOTS

        elif data_type == DataType.ORDER_BOOK_L2:
            roots = self.BOOK_ROOTS

        else:
            raise ValueError(f"Unsupported Bybit data type: {data_type}")

        try:
            return roots[category]
        except KeyError as exc:
            raise ValueError(
                f"No browsable Bybit archive for " f"{category.value}/{data_type.value}"
            ) from exc

    # ------------------------------------------------------------------
    # Option discovery
    # ------------------------------------------------------------------

    @staticmethod
    def _option_product_id(data_type: DataType) -> str:
        if data_type == DataType.TRADE_TICKS:
            return "trade"

        if data_type == DataType.ORDER_BOOK_L2:
            # Keep this mapping isolated because it should be verified
            # against the exact Bybit web endpoint payload before relying
            # on it in production.
            return "orderbook"

        raise ValueError(f"Unsupported Bybit option data type: {data_type}")

    @staticmethod
    def _extract_option_file_items(payload: dict) -> list[dict]:
        """Extract file records from the historical-file response.

        Kept isolated because this is an undocumented Bybit web endpoint
        and its response structure may change independently of V5.
        """

        result = payload.get("result", {})

        if isinstance(result, list):
            return result

        for key in ("files", "list"):
            value = result.get(key)

            if isinstance(value, list):
                return value

        return []

    def _option_remote_file(
        self,
        instrument: Instrument,
        data_type: DataType,
        item: dict,
    ) -> RemoteFile | None:
        url = item.get("url")
        filename = item.get("filename") or item.get("fileName")

        if not url or not filename:
            return None

        file_interval = self._parse_option_daily_interval(filename)

        if file_interval is None:
            return None

        file_start, file_end = file_interval

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
            size_bytes=self._size_bytes(item),
        )

    # ------------------------------------------------------------------
    # Parsing helpers
    # ------------------------------------------------------------------

    DAILY_DATE = re.compile(r"(?P<date>\d{4}-\d{2}-\d{2})")

    @classmethod
    def _parse_daily_interval(
        cls,
        filename: str,
    ) -> tuple[datetime, datetime] | None:
        """Parse daily archive files and reject monthly files."""

        match = cls.DAILY_DATE.search(filename)

        if match is None:
            return None

        day = datetime.strptime(
            match.group("date"),
            "%Y-%m-%d",
        ).replace(tzinfo=timezone.utc)

        return day, day + timedelta(days=1)

    @classmethod
    def _parse_option_daily_interval(
        cls,
        filename: str,
    ) -> tuple[datetime, datetime] | None:
        return cls._parse_daily_interval(filename)

    # ------------------------------------------------------------------
    # General helpers
    # ------------------------------------------------------------------

    @property
    def exchange(self):
        from marketforge.models import Exchange

        return Exchange.BYBIT

    @staticmethod
    def _option_base(symbol: str) -> str:
        """Extract option base coin from a Bybit option symbol."""

        return symbol.split("-", 1)[0].upper()

    @staticmethod
    def _supports_data_type(
        instrument: Instrument,
        data_type: DataType,
    ) -> bool:
        # Current MarketForge Bybit scope supports both historical
        # trade ticks and L2 order-book data for all four categories.
        return data_type in {
            DataType.TRADE_TICKS,
            DataType.ORDER_BOOK_L2,
        }

    @staticmethod
    def _validate_interval(
        start: datetime,
        end: datetime,
    ) -> None:
        if start >= end:
            raise ValueError("start must be earlier than end")

    @staticmethod
    def _overlaps(
        file_start: datetime,
        file_end: datetime,
        requested_start: datetime,
        requested_end: datetime,
    ) -> bool:
        return file_start < requested_end and file_end > requested_start

    @staticmethod
    def _seven_day_windows(
        start: datetime,
        end: datetime,
    ):
        """Split an interval into Bybit-compatible option windows."""

        current = start

        while current < end:
            window_end = min(
                current + timedelta(days=7),
                end,
            )

            yield current, window_end

            current = window_end

    @staticmethod
    def _check_api_response(payload: dict) -> None:
        ret_code = payload.get("retCode")

        if ret_code not in (None, 0):
            raise ValueError(f"Bybit API error {ret_code}: " f"{payload.get('retMsg')}")

    @staticmethod
    def _size_bytes(item: dict) -> int | None:
        value = item.get("size")

        if isinstance(value, int):
            return value

        return None

    def _discover_all_archive_files(
        self,
        instrument: Instrument,
        data_type: DataType,
    ) -> list[RemoteFile]:
        """Enumerate every daily historical file in a browsable archive."""

        root = self._archive_root(
            instrument.market_category,
            data_type,
        )

        symbol_url = urljoin(
            root,
            f"{instrument.symbol}/",
        )

        response = self.http.get(symbol_url)

        soup = BeautifulSoup(
            response.text,
            "html.parser",
        )

        files: list[RemoteFile] = []

        for link in soup.find_all("a", href=True):
            href = link["href"]
            filename = href.rsplit("/", 1)[-1]

            file_interval = self._parse_daily_interval(filename)

            # This automatically rejects:
            #
            # BTCUSDT-2026-05.csv.gz
            #
            # because it contains no YYYY-MM-DD date.
            if file_interval is None:
                continue

            file_start, file_end = file_interval

            files.append(
                RemoteFile(
                    exchange=self.exchange,
                    instrument_type=instrument.instrument_type,
                    market_category=instrument.market_category,
                    data_type=data_type,
                    symbol=instrument.symbol,
                    start=file_start,
                    end=file_end,
                    url=urljoin(symbol_url, href),
                    filename=filename,
                )
            )

        return sorted(
            files,
            key=lambda file: file.start,
        )

    def _option_availability(
        self,
        base_coin: BaseCoin,
        data_type: DataType,
        start: datetime,
        end: datetime,
    ) -> Availability:
        """Discover Bybit option historical files within a requested interval.

        Bybit's option archive is not browsable. Historical files are discovered
        through the list-files web endpoint, which accepts at most seven calendar
        days per request.

        Requests are therefore split into inclusive 7-day windows:
            Sep 22 -> Sep 28
            Sep 29 -> Oct 05
            ...

        HTTP throttling is handled by HttpClient through
        self.option_history_policy.
        """

        self._validate_interval(start, end)

        product_id = self._option_product_id(data_type)

        files: dict[str, RemoteFile] = {}

        for window_start, window_end in self._option_windows(start, end):
            params = {
                "bizType": "option",
                "productId": product_id,
                "symbols": base_coin.symbol,
                "interval": "daily",
                "periods": "",
                "startDay": window_start.date().isoformat(),
                "endDay": window_end.date().isoformat(),
            }

            response = self.http.get(
                self.OPTION_FILES_URL,
                params=params,
                headers=BYBIT_WEB_HEADERS,
                policy=self.option_history_policy,
            )

            payload = response.json()

            for item in self._extract_option_file_items(payload):
                remote_file = self._option_remote_file_from_base_coin(
                    base_coin=base_coin,
                    data_type=data_type,
                    item=item,
                )

                if remote_file is not None:
                    files[remote_file.url] = remote_file

        ordered_files = tuple(
            sorted(
                files.values(),
                key=lambda file: file.start,
            )
        )

        return Availability(
            exchange=self.exchange,
            target=base_coin,
            data_type=data_type,
            files=ordered_files,
        )

    def _option_windows(
        self,
        start: datetime,
        end: datetime,
    ):
        """Yield inclusive Bybit option-history windows of at most 7 days."""

        current = start

        while current < end:
            window_end = min(
                current + timedelta(days=6),
                end - timedelta(days=1),
            )

            yield current, window_end

            current = window_end + timedelta(days=1)

    def _option_remote_file_from_base_coin(
        self,
        base_coin: BaseCoin,
        data_type: DataType,
        item: dict,
    ) -> RemoteFile | None:
        url = item.get("url")
        filename = item.get("filename") or item.get("fileName")

        if not url or not filename:
            return None

        file_interval = self._parse_option_daily_interval(filename)

        if file_interval is None:
            return None

        file_start, file_end = file_interval

        return RemoteFile(
            exchange=self.exchange,
            instrument_type=InstrumentType.OPTION,
            market_category=MarketCategory.OPTION,
            data_type=data_type,
            symbol=base_coin.symbol,
            start=file_start,
            end=file_end,
            url=url,
            filename=filename,
            size_bytes=self._size_bytes(item),
        )
