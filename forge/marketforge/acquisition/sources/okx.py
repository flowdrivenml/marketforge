from __future__ import annotations

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

# OKX documents:
#     5 requests / 2 seconds / IP
#
# MarketForge uses a conservative 0.5-second interval between historical
# requests.
OKX_HISTORY_POLICY = RequestPolicy(
    request_interval=0.5,
    max_retries=5,
    backoff=1.0,
)


# ----------------------------------------------------------------------
# Capabilities
# ----------------------------------------------------------------------

OKX_CAPABILITIES = {
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


class OKXSource(Source):
    """OKX historical market-data source.

    Instrument discovery:
        GET /api/v5/public/instruments

    Option underlying discovery:
        GET /api/v5/public/underlying

    Historical file discovery:
        GET /api/v5/public/market-data-history

    Historical modules used by MarketForge:
        module=1 -> tick-by-tick trades
        module=4 -> 400-level order book

    OKX daily historical requests are conservatively split into
    windows of at most 9 days.

    OKX's /public/underlying endpoint currently advertises some OPTION
    underlyings which /public/instruments rejects as instFamily values.
    MarketForge therefore distinguishes advertised underlyings from
    currently verified/queryable option families.
    """

    # ------------------------------------------------------------------
    # Endpoints
    # ------------------------------------------------------------------

    API_BASE = "https://www.okx.com"

    INSTRUMENTS_URL = f"{API_BASE}/api/v5/public/instruments"

    UNDERLYING_URL = f"{API_BASE}/api/v5/public/underlying"

    HISTORY_URL = f"{API_BASE}/api/v5/public/market-data-history"

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    def __init__(
        self,
        http: HttpClient | None = None,
        history_policy: RequestPolicy | None = None,
    ) -> None:
        self.http = http or HttpClient()

        self.history_policy = history_policy or OKX_HISTORY_POLICY

    # ------------------------------------------------------------------
    # Public Source interface
    # ------------------------------------------------------------------

    @property
    def exchange(self) -> Exchange:
        return Exchange.OKX

    def supports(
        self,
        instrument_type: InstrumentType,
        market_category: MarketCategory,
        data_type: DataType,
    ) -> bool:
        """Return whether OKX supports this MarketForge combination."""

        return (
            instrument_type,
            market_category,
            data_type,
        ) in OKX_CAPABILITIES

    def instruments(
        self,
        instrument_type: InstrumentType | None = None,
        market_category: MarketCategory | None = None,
        data_type: DataType | None = None,
    ) -> list[Instrument]:
        """Discover OKX instruments matching the requested filters."""

        # Reject impossible combinations before making network requests.
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

        instruments: list[Instrument] = []

        # --------------------------------------------------------------
        # Spot / Swaps / Futures
        # --------------------------------------------------------------

        okx_types = self._requested_okx_types(instrument_type)

        for okx_type in okx_types:
            response = self.http.get(
                self.INSTRUMENTS_URL,
                params={
                    "instType": okx_type,
                },
            )

            payload = response.json()
            self._check_response(payload)

            for item in payload.get(
                "data",
                [],
            ):
                instrument = self._instrument_from_api(item)

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

                instruments.append(instrument)

        # --------------------------------------------------------------
        # Options
        # --------------------------------------------------------------
        #
        # OKX requires instFamily when querying OPTION instruments.
        #
        # Therefore:
        #
        # /underlying?instType=OPTION
        #       ↓
        # valid option families
        #       ↓
        # /instruments?instType=OPTION&instFamily=...

        include_options = (
            instrument_type is None or instrument_type == InstrumentType.OPTION
        )

        category_allows_options = (
            market_category is None or market_category == MarketCategory.OPTION
        )

        if include_options and category_allows_options:
            option_instruments = self._option_instruments()

            for instrument in option_instruments:
                if data_type is not None and not self.supports(
                    instrument.instrument_type,
                    instrument.market_category,
                    data_type,
                ):
                    continue

                instruments.append(instrument)

        return instruments

    def base_coins(
        self,
        instrument_type: InstrumentType,
        data_type: DataType | None = None,
    ) -> list[str]:
        """Return currently queryable OKX option base currencies."""

        if instrument_type != InstrumentType.OPTION:
            return []

        if data_type is not None and not self.supports(
            InstrumentType.OPTION,
            MarketCategory.OPTION,
            data_type,
        ):
            return []

        return sorted(
            {
                family.split(
                    "-",
                    1,
                )[0]
                for family in self._option_families()
            }
        )

    def availability(
        self,
        target: Instrument | BaseCoin,
        data_type: DataType,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> Availability | None:
        """Discover OKX historical availability in a requested range.

        OKX does not expose complete lifetime historical availability
        through one inexpensive operation, therefore start and end are
        required.
        """

        if start is None or end is None:
            raise ValueError("OKX availability requires start and end dates")

        self._validate_interval(
            start,
            end,
        )

        if isinstance(
            target,
            BaseCoin,
        ):
            instrument = self._option_instrument_from_base_coin(target)

            if not self.supports(
                InstrumentType.OPTION,
                MarketCategory.OPTION,
                data_type,
            ):
                raise ValueError("Unsupported OKX option data combination")

            files = self._discover_history_files(
                instrument=instrument,
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

        if not isinstance(
            target,
            Instrument,
        ):
            raise TypeError(
                "Unsupported OKX availability target: " f"{type(target).__name__}"
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
        """Discover OKX historical files covering [start, end)."""

        self._validate_interval(
            start,
            end,
        )

        if isinstance(
            target,
            BaseCoin,
        ):
            if not self.supports(
                InstrumentType.OPTION,
                MarketCategory.OPTION,
                data_type,
            ):
                raise ValueError("Unsupported OKX option data combination")

            instrument = self._option_instrument_from_base_coin(target)

            return self._discover_history_files(
                instrument=instrument,
                data_type=data_type,
                start=start,
                end=end,
            )

        if not isinstance(
            target,
            Instrument,
        ):
            raise TypeError(
                "Unsupported OKX discovery target: " f"{type(target).__name__}"
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
    # Standard instrument discovery
    # ------------------------------------------------------------------

    @staticmethod
    def _requested_okx_types(
        instrument_type: InstrumentType | None,
    ) -> list[str]:
        """Return instTypes handled by generic instrument discovery."""

        if instrument_type == InstrumentType.SPOT:
            return ["SPOT"]

        if instrument_type == InstrumentType.PERPETUAL:
            return ["SWAP"]

        if instrument_type == InstrumentType.FUTURE:
            return ["FUTURES"]

        # OPTION has its own family-based discovery path.
        if instrument_type == InstrumentType.OPTION:
            return []

        return [
            "SPOT",
            "SWAP",
            "FUTURES",
        ]

    def _instrument_from_api(
        self,
        item: dict,
    ) -> Instrument | None:
        """Convert OKX instrument metadata into MarketForge."""

        inst_id = item.get("instId")

        inst_type = item.get("instType")

        if not inst_id or not inst_type:
            return None

        instrument_type = self._instrument_type(inst_type)

        if instrument_type is None:
            return None

        category = self._market_category(
            item=item,
            instrument_type=instrument_type,
        )

        if category is None:
            return None

        family = item.get("instFamily") or None

        return Instrument(
            exchange=self.exchange,
            instrument_type=instrument_type,
            market_category=category,
            symbol=inst_id,
            family=family,
        )

    @staticmethod
    def _instrument_type(
        inst_type: str,
    ) -> InstrumentType | None:
        """Map OKX instType into MarketForge InstrumentType."""

        mapping = {
            "SPOT": InstrumentType.SPOT,
            "SWAP": InstrumentType.PERPETUAL,
            "FUTURES": InstrumentType.FUTURE,
            "OPTION": InstrumentType.OPTION,
        }

        return mapping.get(inst_type)

    @staticmethod
    def _market_category(
        item: dict,
        instrument_type: InstrumentType,
    ) -> MarketCategory | None:
        """Map OKX metadata into MarketForge MarketCategory.

        OKX explicitly exposes derivative structure through ctType.
        Use that field rather than attempting to infer linear/inverse
        from settlement currency.
        """

        if instrument_type == InstrumentType.SPOT:
            return MarketCategory.SPOT

        if instrument_type == InstrumentType.OPTION:
            return MarketCategory.OPTION

        ct_type = item.get("ctType")

        if ct_type == "linear":
            return MarketCategory.LINEAR

        if ct_type == "inverse":
            return MarketCategory.INVERSE

        return None

    # ------------------------------------------------------------------
    # Option discovery
    # ------------------------------------------------------------------

    def _option_underlyings(
        self,
    ) -> list[str]:
        """Return OPTION underlyings advertised by OKX.

        This endpoint may advertise underlyings that are not currently
        accepted as instFamily values by /public/instruments.
        """

        response = self.http.get(
            self.UNDERLYING_URL,
            params={
                "instType": "OPTION",
            },
        )

        payload = response.json()
        self._check_response(payload)

        underlyings: set[str] = set()

        for group in payload.get(
            "data",
            [],
        ):
            if not isinstance(
                group,
                list,
            ):
                continue

            for underlying in group:
                if (
                    isinstance(
                        underlying,
                        str,
                    )
                    and underlying
                ):
                    underlyings.add(underlying)

        return sorted(underlyings)

    def _option_families(
        self,
    ) -> list[str]:
        """Return OPTION families currently accepted by instruments API.

        Observed OKX behavior:

            /public/underlying?instType=OPTION

        currently advertises:

            BTC-USD
            ETH-USD
            SOL-USD
            XAU-USD

        while:

            /public/instruments
                ?instType=OPTION
                &instFamily=SOL-USD

        and XAU-USD return:

            code=51000
            "Parameter instFamily error"

        Until OKX exposes a reliable family-enumeration mechanism,
        MarketForge restricts option contract discovery to families
        verified as accepted by /public/instruments.
        """

        verified_families = {
            "BTC-USD",
            "ETH-USD",
        }

        return [
            family
            for family in self._option_underlyings()
            if family in verified_families
        ]

    def _option_instruments(
        self,
    ) -> list[Instrument]:
        """Discover all currently queryable OKX option contracts."""

        instruments: list[Instrument] = []

        for family in self._option_families():
            response = self.http.get(
                self.INSTRUMENTS_URL,
                params={
                    "instType": "OPTION",
                    "instFamily": family,
                },
            )

            payload = response.json()
            self._check_response(payload)

            for item in payload.get(
                "data",
                [],
            ):
                instrument = self._instrument_from_api(item)

                if instrument is not None:
                    instruments.append(instrument)

        return instruments

    # ------------------------------------------------------------------
    # Historical discovery
    # ------------------------------------------------------------------

    def _discover_history_files(
        self,
        instrument: Instrument,
        data_type: DataType,
        start: datetime,
        end: datetime,
    ) -> list[RemoteFile]:
        """Query OKX history API over conservative <=9-day windows."""

        module = self._module(data_type)

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
            params = self._history_params(
                instrument=instrument,
                module=module,
                start=window_start,
                end=window_end,
            )

            response = self.http.get(
                self.HISTORY_URL,
                params=params,
                policy=self.history_policy,
            )

            payload = response.json()
            self._check_response(payload)

            for item in self._extract_history_items(payload):
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

    def _history_params(
        self,
        instrument: Instrument,
        module: int,
        start: datetime,
        end: datetime,
    ) -> dict[str, str | int]:
        """Build market-data-history parameters."""

        inst_type = self._okx_inst_type(instrument.instrument_type)

        params: dict[
            str,
            str | int,
        ] = {
            "module": module,
            "instType": inst_type,
            "dateAggrType": "daily",
            "begin": self._milliseconds(start),
            "end": self._milliseconds(end),
        }

        if instrument.instrument_type == InstrumentType.SPOT:
            params["instIdList"] = instrument.symbol

        else:
            if not instrument.family:
                raise ValueError(
                    "OKX non-spot historical discovery " "requires Instrument.family"
                )

            params["instFamilyList"] = instrument.family

        return params

    # ------------------------------------------------------------------
    # Historical response parsing
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_history_items(
        payload: dict,
    ) -> list[dict]:
        """Flatten data/details/groupDetails into file records."""

        items: list[dict] = []

        for data_group in payload.get(
            "data",
            [],
        ):
            for detail in data_group.get(
                "details",
                [],
            ):
                for item in detail.get(
                    "groupDetails",
                    [],
                ):
                    items.append(item)

        return items

    def _remote_file(
        self,
        instrument: Instrument,
        data_type: DataType,
        item: dict,
    ) -> RemoteFile | None:
        """Convert OKX historical metadata into RemoteFile."""

        url = item.get("url")

        filename = item.get("filename")

        if not url or not filename:
            return None

        # Actual OKX responses use dateTs. Some documentation has
        # referred to the same field as dataTs, so accept both.
        timestamp = item.get("dateTs") or item.get("dataTs")

        if timestamp is None:
            return None

        file_start = datetime.fromtimestamp(
            int(timestamp) / 1000,
            tz=timezone.utc,
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
            size_bytes=self._size_bytes(item.get("sizeMB")),
        )

    # ------------------------------------------------------------------
    # Data modules
    # ------------------------------------------------------------------

    @staticmethod
    def _module(
        data_type: DataType,
    ) -> int:
        """Map MarketForge DataType to OKX historical module.

        Current mapping:

            TRADE_TICKS   -> module 1
            ORDER_BOOK_L2 -> module 4 (400-level depth)

        OKX module 5 provides 5000-level depth. If MarketForge later
        needs to distinguish depth levels explicitly, that should be
        represented separately rather than silently changing this
        mapping.
        """

        if data_type == DataType.TRADE_TICKS:
            return 1

        if data_type == DataType.ORDER_BOOK_L2:
            return 5

        raise ValueError("Unsupported OKX data type: " f"{data_type.value}")

    # ------------------------------------------------------------------
    # Historical request windows
    # ------------------------------------------------------------------

    @staticmethod
    def _history_windows(
        start: datetime,
        end: datetime,
    ):
        """Yield conservative OKX daily-history API windows.

        OKX documents a maximum daily interval of 10 days.

        Live testing showed that requests sitting on the full boundary
        can return:

            code=50076
            "The time interval between the start date and end date
             cannot exceed 10 days."

        MarketForge therefore conservatively uses <=9-day windows.

        MarketForge uses [start, end) internally while OKX begin/end
        timestamps are inclusive.
        """

        current = start

        while current < end:
            window_end_exclusive = min(
                current + timedelta(days=9),
                end,
            )

            # OKX end is inclusive. Subtract one millisecond so
            # adjacent MarketForge windows do not overlap.
            api_end = window_end_exclusive - timedelta(milliseconds=1)

            yield (
                current,
                api_end,
            )

            current = window_end_exclusive

    # ------------------------------------------------------------------
    # Validation / mapping helpers
    # ------------------------------------------------------------------

    def _validate_supported(
        self,
        instrument: Instrument,
        data_type: DataType,
    ) -> None:
        """Reject unsupported OKX acquisition combinations."""

        if not self.supports(
            instrument.instrument_type,
            instrument.market_category,
            data_type,
        ):
            raise ValueError("Unsupported OKX data combination")

    @staticmethod
    def _okx_inst_type(
        instrument_type: InstrumentType,
    ) -> str:
        """Map MarketForge InstrumentType to OKX instType."""

        mapping = {
            InstrumentType.SPOT: "SPOT",
            InstrumentType.PERPETUAL: "SWAP",
            InstrumentType.FUTURE: "FUTURES",
            InstrumentType.OPTION: "OPTION",
        }

        try:
            return mapping[instrument_type]

        except KeyError as exc:
            raise ValueError(
                "Unsupported OKX instrument type: " f"{instrument_type.value}"
            ) from exc

    # ------------------------------------------------------------------
    # General helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _milliseconds(
        value: datetime,
    ) -> int:
        """Convert datetime to Unix milliseconds."""

        return int(value.timestamp() * 1000)

    @staticmethod
    def _size_bytes(
        size_mb: str | None,
    ) -> int | None:
        """Convert OKX sizeMB metadata to bytes."""

        if size_mb is None:
            return None

        try:
            return int(float(size_mb) * 1024 * 1024)

        except (
            TypeError,
            ValueError,
        ):
            return None

    @staticmethod
    def _family_base(
        family: str | None,
    ) -> str | None:
        """Extract base currency from an OKX instrument family.

        Examples:

            BTC-USD    -> BTC
            ETH-USDT   -> ETH
            BTC-USD_UM -> BTC
        """

        if not family:
            return None

        return family.split(
            "-",
            1,
        )[0]

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
    def _check_response(
        payload: dict,
    ) -> None:
        """Validate an OKX API response.

        OKX uses code="0" for successful responses.

        A successful historical query containing no files is not an
        error. In that case the response still has code="0" and the
        historical parser naturally returns an empty list.
        """

        code = payload.get("code")

        if code != "0":
            raise ValueError("OKX API error " f"{code}: " f"{payload.get('msg')}")

    def _option_instrument_from_base_coin(
        self,
        base_coin: BaseCoin,
    ) -> Instrument:
        """Resolve an option BaseCoin to its OKX historical instrument family."""

        if base_coin.market_category != MarketCategory.OPTION:
            raise ValueError("OKX BaseCoin targets require " "MarketCategory.OPTION")

        prefix = base_coin.symbol.upper() + "-"

        families = sorted(
            family
            for family in self._option_families()
            if family.upper().startswith(prefix)
        )

        if not families:
            raise ValueError(
                "No OKX option family found for " f"base coin {base_coin.symbol}"
            )

        if len(families) != 1:
            raise ValueError(
                "Multiple OKX option families found for "
                f"{base_coin.symbol}: {families}"
            )

        family = families[0]

        return Instrument(
            exchange=self.exchange,
            instrument_type=InstrumentType.OPTION,
            market_category=MarketCategory.OPTION,
            symbol=base_coin.symbol,
            family=family,
        )
