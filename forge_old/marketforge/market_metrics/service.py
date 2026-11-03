from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from psycopg import Connection

from marketforge.market_metrics.models import InstrumentMarketView, InstrumentMetrics
from marketforge.market_metrics.providers import binance, bitget, bybit, gateio, okx
from marketforge.market_metrics.repository import MarketMetricsRepository

PROVIDERS = {
    "bybit": bybit.fetch,
    "binance": binance.fetch,
    "okx": okx.fetch,
    "bitget": bitget.fetch,
    "gateio": gateio.fetch,
}


class MarketMetricsService:
    """Refresh and query current instrument-market metrics."""

    def __init__(
        self,
        conn: Connection,
    ) -> None:
        self.conn = conn
        self.repository = MarketMetricsRepository(conn)

    def refresh(
        self,
        *,
        exchange: str | None = None,
    ) -> dict[str, int]:
        """
        Refresh current market metrics.

        If exchange is omitted, refresh every supported exchange.

        Returns the number of metric rows written per exchange.
        """

        exchanges = [exchange] if exchange is not None else list(PROVIDERS)

        unknown = [code for code in exchanges if code not in PROVIDERS]

        if unknown:
            raise ValueError(
                "Unsupported market-metrics exchange: " + ", ".join(unknown)
            )

        result: dict[str, int] = {}

        for code in exchanges:
            instruments = self._load_instruments(exchange=code)

            metrics = PROVIDERS[code](instruments)

            count = self.repository.upsert_many(metrics)

            result[code] = count

        return result

    def get(
        self,
        instrument_id: int,
    ) -> InstrumentMetrics | None:
        """Return current metrics for one instrument."""

        return self.repository.get(instrument_id)

    def get_many(
        self,
        instrument_ids: Iterable[int],
    ) -> list[InstrumentMetrics]:
        """Return current metrics for selected instruments."""

        return self.repository.get_many(instrument_ids)

    def rank_by_turnover(
        self,
        instrument_ids: Iterable[int],
    ) -> list[InstrumentMetrics]:
        """
        Rank selected instruments by normalized 24h turnover.

        Highest turnover receives highest priority.

        Instruments without turnover are placed last using
        instrument_id as the deterministic fallback.
        """

        ids = list(dict.fromkeys(instrument_ids))

        if not ids:
            return []

        metrics = self.repository.get_many(ids)

        by_id = {item.instrument_id: item for item in metrics}

        with_turnover = [
            by_id[instrument_id]
            for instrument_id in ids
            if (
                instrument_id in by_id and by_id[instrument_id].turnover_24h is not None
            )
        ]

        without_turnover = [
            by_id[instrument_id]
            for instrument_id in ids
            if (instrument_id in by_id and by_id[instrument_id].turnover_24h is None)
        ]

        with_turnover.sort(
            key=lambda item: (
                -item.turnover_24h,
                item.instrument_id,
            )
        )

        without_turnover.sort(key=lambda item: item.instrument_id)

        return with_turnover + without_turnover

    def stream_ranks(
        self,
        instrument_ids: Iterable[int],
    ) -> dict[int, int]:
        """
        Build deterministic stream ranks for selected instruments.

        Rank zero has the highest turnover priority.
        """

        ids = list(dict.fromkeys(instrument_ids))

        if not ids:
            return {}

        metrics = self.repository.get_many(ids)

        by_id = {item.instrument_id: item for item in metrics}

        def key(
            instrument_id: int,
        ) -> tuple[int, object, int]:
            metrics_item = by_id.get(instrument_id)

            if metrics_item is None or metrics_item.turnover_24h is None:
                return (
                    1,
                    0,
                    instrument_id,
                )

            return (
                0,
                -metrics_item.turnover_24h,
                instrument_id,
            )

        ordered = sorted(
            ids,
            key=key,
        )

        return {instrument_id: rank for rank, instrument_id in enumerate(ordered)}

    def _load_instruments(
        self,
        *,
        exchange: str,
    ) -> list[dict[str, Any]]:
        """
        Load catalog metadata required by a metrics provider.

        Options are intentionally excluded from the current
        market-metrics implementation.
        """

        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    i.id AS instrument_id,
                    e.code AS exchange,

                    i.symbol,
                    i.instrument_type,
                    i.market_category,

                    i.base_asset,
                    i.quote_asset,
                    i.settlement_asset,

                    i.status,

                    s.quantity_type,
                    s.contract_value,
                    s.contract_value_asset

                FROM catalog.instruments AS i

                JOIN catalog.exchanges AS e
                    ON e.id = i.exchange_id

                LEFT JOIN catalog.instrument_specs AS s
                    ON s.instrument_id = i.id

                WHERE e.code = %s
                  AND i.instrument_type <> 'option'

                ORDER BY
                    i.instrument_type,
                    i.market_category,
                    i.symbol
                """,
                (exchange,),
            )

            return list(cursor.fetchall())

    def list_instruments(
        self,
        *,
        exchange: str | None = None,
        instrument_type: str | None = None,
        market_category: str | None = None,
        symbol: str | None = None,
        sort: str = "turnover",
    ) -> list[InstrumentMarketView]:
        """
        Query catalog instruments together with current market metrics.

        Results may be filtered by instrument metadata and sorted by
        market activity.
        """

        instruments = self.repository.list_instruments(
            exchange=exchange,
            instrument_type=instrument_type,
            market_category=market_category,
            symbol=symbol,
        )

        if sort == "turnover":
            return sorted(
                instruments,
                key=lambda item: (
                    item.turnover_24h is None,
                    -item.turnover_24h if item.turnover_24h is not None else 0,
                    item.exchange,
                    item.symbol,
                    item.instrument_id,
                ),
            )

        if sort == "open_interest":
            return sorted(
                instruments,
                key=lambda item: (
                    item.open_interest_value is None,
                    (
                        -item.open_interest_value
                        if item.open_interest_value is not None
                        else 0
                    ),
                    item.exchange,
                    item.symbol,
                    item.instrument_id,
                ),
            )

        if sort == "symbol":
            return sorted(
                instruments,
                key=lambda item: (
                    item.symbol,
                    item.exchange,
                    item.instrument_id,
                ),
            )

        raise ValueError("Unsupported instrument sort: " f"{sort!r}")
