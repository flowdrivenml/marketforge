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

from ...errors import AcquisitionError

# ----------------------------------------------------------------------
# Request policy
# ----------------------------------------------------------------------

# Archive availability can require many HEAD requests, especially for
# hourly order-book files. Start conservatively.
GATEIO_ARCHIVE_POLICY = RequestPolicy(
    request_interval=1.0,
    max_retries=5,
    backoff=2.0,
)


# ----------------------------------------------------------------------
# Capabilities
# ----------------------------------------------------------------------

GATEIO_CAPABILITIES = {
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
    # Linear delivery futures
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
    # Inverse delivery futures
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


class GateIOSource(Source):
    """Gate.io historical market-data source.

    Instrument discovery uses Gate API v4.

    Historical archive discovery uses deterministic gatedata URLs and
    HEAD requests so archive files are not downloaded merely to test
    their existence.

    Exact spot archive patterns currently implemented:

        Trades:
            /spot/deals/{YYYYMM}/{SYMBOL}-{YYYYMM}.csv.gz

        Order books:
            /spot/orderbooks/{YYYYMM}/{SYMBOL}-{YYYYMMDDHH}.csv.gz

    Futures archive patterns should be added only after their real
    GateData paths have been verified.
    """

    API_BASE = "https://api.gateio.ws/api/v4"
    ARCHIVE_BASE = "https://download.gatedata.org"

    SPOT_INSTRUMENTS_URL = f"{API_BASE}/spot/currency_pairs"

    def __init__(
        self,
        http: HttpClient | None = None,
        archive_policy: RequestPolicy | None = None,
    ) -> None:
        self.http = http or HttpClient()

        self.archive_policy = archive_policy or GATEIO_ARCHIVE_POLICY

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    @property
    def exchange(self) -> Exchange:
        return Exchange.GATEIO

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
        ) in GATEIO_CAPABILITIES

    def instruments(
        self,
        instrument_type: InstrumentType | None = None,
        market_category: MarketCategory | None = None,
        data_type: DataType | None = None,
    ) -> list[Instrument]:
        """Discover Gate.io instruments through public API v4."""

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

        # --------------------------------------------------------------
        # Spot
        # --------------------------------------------------------------

        if instrument_type in (
            None,
            InstrumentType.SPOT,
        ):
            if market_category in (
                None,
                MarketCategory.SPOT,
            ):
                for instrument in self._spot_instruments():
                    self._add_if_matching(
                        instruments=instruments,
                        instrument=instrument,
                        instrument_type=instrument_type,
                        market_category=market_category,
                        data_type=data_type,
                    )

        # --------------------------------------------------------------
        # Perpetual futures
        # --------------------------------------------------------------

        if instrument_type in (
            None,
            InstrumentType.PERPETUAL,
        ):
            # USDT-settled
            if market_category in (
                None,
                MarketCategory.LINEAR,
            ):
                for instrument in self._perpetual_instruments(
                    settle="usdt",
                    category=MarketCategory.LINEAR,
                ):
                    self._add_if_matching(
                        instruments=instruments,
                        instrument=instrument,
                        instrument_type=instrument_type,
                        market_category=market_category,
                        data_type=data_type,
                    )

            # BTC-settled
            if market_category in (
                None,
                MarketCategory.INVERSE,
            ):
                for instrument in self._perpetual_instruments(
                    settle="btc",
                    category=MarketCategory.INVERSE,
                ):
                    self._add_if_matching(
                        instruments=instruments,
                        instrument=instrument,
                        instrument_type=instrument_type,
                        market_category=market_category,
                        data_type=data_type,
                    )

        # --------------------------------------------------------------
        # Delivery futures
        # --------------------------------------------------------------

        if instrument_type in (
            None,
            InstrumentType.FUTURE,
        ):
            if market_category in (
                None,
                MarketCategory.LINEAR,
            ):
                for instrument in self._delivery_instruments(
                    settle="usdt",
                    category=MarketCategory.LINEAR,
                ):
                    self._add_if_matching(
                        instruments=instruments,
                        instrument=instrument,
                        instrument_type=instrument_type,
                        market_category=market_category,
                        data_type=data_type,
                    )

            if market_category in (
                None,
                MarketCategory.INVERSE,
            ):
                for instrument in self._delivery_instruments(
                    settle="btc",
                    category=MarketCategory.INVERSE,
                ):
                    self._add_if_matching(
                        instruments=instruments,
                        instrument=instrument,
                        instrument_type=instrument_type,
                        market_category=market_category,
                        data_type=data_type,
                    )

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
        """Gate options are outside the current acquisition scope."""

        return []

    def availability(
        self,
        target: Instrument | BaseCoin,
        data_type: DataType,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> Availability | None:
        """Check GateData availability over [start, end)."""

        if start is None or end is None:
            raise ValueError("Gate.io availability requires start and end dates")

        self._validate_interval(
            start,
            end,
        )

        if isinstance(target, BaseCoin):
            raise ValueError("Gate.io BaseCoin availability is not supported")

        if not isinstance(
            target,
            Instrument,
        ):
            raise TypeError(
                "Unsupported Gate.io availability target: " f"{type(target).__name__}"
            )

        self._validate_supported(
            target,
            data_type,
        )

        files = self._discover_archive_files(
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
        """Discover existing GateData files covering [start, end)."""

        self._validate_interval(
            start,
            end,
        )

        if isinstance(target, BaseCoin):
            raise ValueError("Gate.io BaseCoin file discovery is not supported")

        if not isinstance(
            target,
            Instrument,
        ):
            raise TypeError(
                "Unsupported Gate.io discovery target: " f"{type(target).__name__}"
            )

        self._validate_supported(
            target,
            data_type,
        )

        return self._discover_archive_files(
            instrument=target,
            data_type=data_type,
            start=start,
            end=end,
        )

    # ------------------------------------------------------------------
    # Instrument discovery
    # ------------------------------------------------------------------

    def _spot_instruments(
        self,
    ) -> list[Instrument]:
        response = self.http.get(self.SPOT_INSTRUMENTS_URL)

        payload = response.json()

        instruments: list[Instrument] = []

        for item in payload:
            symbol = item.get("id")

            if not symbol:
                continue

            instruments.append(
                Instrument(
                    exchange=self.exchange,
                    instrument_type=InstrumentType.SPOT,
                    market_category=MarketCategory.SPOT,
                    symbol=symbol,
                )
            )

        return instruments

    def _perpetual_instruments(
        self,
        settle: str,
        category: MarketCategory,
    ) -> list[Instrument]:
        response = self.http.get(f"{self.API_BASE}/futures/{settle}/contracts")

        payload = response.json()

        instruments: list[Instrument] = []

        for item in payload:
            symbol = item.get("name")

            if not symbol:
                continue

            instruments.append(
                Instrument(
                    exchange=self.exchange,
                    instrument_type=InstrumentType.PERPETUAL,
                    market_category=category,
                    symbol=symbol,
                )
            )

        return instruments

    def _delivery_instruments(
        self,
        settle: str,
        category: MarketCategory,
    ) -> list[Instrument]:
        response = self.http.get(f"{self.API_BASE}/delivery/{settle}/contracts")

        payload = response.json()

        instruments: list[Instrument] = []

        for item in payload:
            symbol = item.get("name")

            if not symbol:
                continue

            instruments.append(
                Instrument(
                    exchange=self.exchange,
                    instrument_type=InstrumentType.FUTURE,
                    market_category=category,
                    symbol=symbol,
                )
            )

        return instruments

    def _add_if_matching(
        self,
        instruments: dict,
        instrument: Instrument,
        instrument_type: InstrumentType | None,
        market_category: MarketCategory | None,
        data_type: DataType | None,
    ) -> None:
        if (
            instrument_type is not None
            and instrument.instrument_type != instrument_type
        ):
            return

        if (
            market_category is not None
            and instrument.market_category != market_category
        ):
            return

        if data_type is not None and not self.supports(
            instrument.instrument_type,
            instrument.market_category,
            data_type,
        ):
            return

        key = (
            instrument.instrument_type,
            instrument.market_category,
            instrument.symbol,
        )

        instruments[key] = instrument

    # ------------------------------------------------------------------
    # Archive discovery
    # ------------------------------------------------------------------

    def _discover_archive_files(
        self,
        instrument: Instrument,
        data_type: DataType,
        start: datetime,
        end: datetime,
    ) -> list[RemoteFile]:
        """Generate candidate URLs and retain those that exist."""

        candidates = self._archive_candidates(
            instrument=instrument,
            data_type=data_type,
            start=start,
            end=end,
        )

        files: list[RemoteFile] = []

        for candidate in candidates:
            if self._archive_exists(candidate.url):
                files.append(candidate)

        return files

    def _archive_candidates(
        self,
        instrument: Instrument,
        data_type: DataType,
        start: datetime,
        end: datetime,
    ) -> list[RemoteFile]:
        if data_type == DataType.TRADE_TICKS:
            return self._trade_candidates(
                instrument=instrument,
                start=start,
                end=end,
            )

        if data_type == DataType.ORDER_BOOK_L2:
            return self._orderbook_candidates(
                instrument=instrument,
                start=start,
                end=end,
            )

        raise ValueError("Unsupported Gate.io data type: " f"{data_type.value}")

    def _trade_candidates(
        self,
        instrument: Instrument,
        start: datetime,
        end: datetime,
    ) -> list[RemoteFile]:
        """Generate monthly Gate trade archive candidates."""

        candidates: list[RemoteFile] = []

        current = datetime(
            start.year,
            start.month,
            1,
            tzinfo=timezone.utc,
        )

        while current < end:
            next_month = self._next_month(current)

            month = current.strftime("%Y%m")
            filename = f"{instrument.symbol}-{month}.csv.gz"

            url = self._archive_url(
                instrument=instrument,
                data_type=DataType.TRADE_TICKS,
                timestamp=current,
            )

            candidates.append(
                RemoteFile(
                    exchange=self.exchange,
                    instrument_type=instrument.instrument_type,
                    market_category=instrument.market_category,
                    data_type=DataType.TRADE_TICKS,
                    symbol=instrument.symbol,
                    start=current,
                    end=next_month,
                    url=url,
                    filename=filename,
                    size_bytes=None,
                )
            )

            current = next_month

        return candidates

    def _orderbook_candidates(
        self,
        instrument: Instrument,
        start: datetime,
        end: datetime,
    ) -> list[RemoteFile]:
        """Generate hourly Gate order-book archive candidates."""

        candidates: list[RemoteFile] = []

        current = start.replace(
            minute=0,
            second=0,
            microsecond=0,
        )

        while current < end:
            file_end = current + timedelta(hours=1)

            timestamp = current.strftime("%Y%m%d%H")
            filename = f"{instrument.symbol}-{timestamp}.csv.gz"

            url = self._archive_url(
                instrument=instrument,
                data_type=DataType.ORDER_BOOK_L2,
                timestamp=current,
            )

            candidates.append(
                RemoteFile(
                    exchange=self.exchange,
                    instrument_type=instrument.instrument_type,
                    market_category=instrument.market_category,
                    data_type=DataType.ORDER_BOOK_L2,
                    symbol=instrument.symbol,
                    start=current,
                    end=file_end,
                    url=url,
                    filename=filename,
                    size_bytes=None,
                )
            )

            current = file_end

        return candidates

    # ------------------------------------------------------------------
    # Archive probing
    # ------------------------------------------------------------------

    def _archive_exists(
        self,
        url: str,
    ) -> bool:
        """Return whether a GateData archive exists."""

        response = self.http.head(
            url,
            policy=GATEIO_ARCHIVE_POLICY,
            allowed_status_codes={404},
            allow_redirects=True,
        )

        if response.status_code == 200:
            return True

        if response.status_code == 404:
            return False

        raise AcquisitionError(
            "Unexpected Gate.io archive response: " f"{response.status_code} for {url}"
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _validate_supported(
        self,
        instrument: Instrument,
        data_type: DataType,
    ) -> None:
        if not self.supports(
            instrument.instrument_type,
            instrument.market_category,
            data_type,
        ):
            raise ValueError("Unsupported Gate.io data combination")

    @staticmethod
    def _validate_interval(
        start: datetime,
        end: datetime,
    ) -> None:
        if start >= end:
            raise ValueError("start must be earlier than end")

    @staticmethod
    def _next_month(
        value: datetime,
    ) -> datetime:
        if value.month == 12:
            return datetime(
                value.year + 1,
                1,
                1,
                tzinfo=timezone.utc,
            )

        return datetime(
            value.year,
            value.month + 1,
            1,
            tzinfo=timezone.utc,
        )

    def _archive_market(
        self,
        instrument: Instrument,
    ) -> str:
        """Map a MarketForge instrument to its GateData archive market."""

        if instrument.instrument_type == InstrumentType.SPOT:
            return "spot"

        if instrument.market_category == MarketCategory.LINEAR:
            return "futures_usdt"

        if instrument.market_category == MarketCategory.INVERSE:
            return "futures_btc"

        raise ValueError("Unsupported Gate.io archive market")

    @staticmethod
    def _archive_dataset(
        instrument: Instrument,
        data_type: DataType,
    ) -> str:
        """Map a MarketForge data type to its GateData dataset."""

        if data_type == DataType.ORDER_BOOK_L2:
            return "orderbooks"

        if data_type == DataType.TRADE_TICKS:
            if instrument.instrument_type == InstrumentType.SPOT:
                return "deals"

            return "trades"

        raise ValueError("Unsupported Gate.io data type: " f"{data_type.value}")

    def _archive_url(
        self,
        instrument: Instrument,
        data_type: DataType,
        timestamp: datetime,
    ) -> str:
        """Construct a deterministic GateData archive URL."""

        market = self._archive_market(instrument)

        dataset = self._archive_dataset(
            instrument,
            data_type,
        )

        month = timestamp.strftime("%Y%m")

        if data_type == DataType.ORDER_BOOK_L2:
            filename = f"{instrument.symbol}-" f"{timestamp:%Y%m%d%H}.csv.gz"

        else:
            filename = f"{instrument.symbol}-" f"{month}.csv.gz"

        return (
            f"{self.ARCHIVE_BASE}/" f"{market}/" f"{dataset}/" f"{month}/" f"{filename}"
        )
