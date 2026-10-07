from __future__ import annotations

from collections.abc import Iterable

from psycopg import Connection

from marketforge.market_metrics.models import InstrumentMarketView, InstrumentMetrics


class MarketMetricsRepository:
    """Persistence for current instrument-market metrics."""

    def __init__(
        self,
        conn: Connection,
    ) -> None:
        self.conn = conn

    def upsert(
        self,
        metrics: InstrumentMetrics,
    ) -> None:
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO catalog.instrument_metrics (
                    instrument_id,

                    last_price,

                    high_24h,
                    low_24h,
                    price_change_pct_24h,

                    volume_24h_native,
                    volume_24h_base,
                    volume_24h_quote,
                    volume_24h_contracts,

                    turnover_24h,
                    turnover_denomination,

                    open_interest,
                    open_interest_value,

                    funding_rate,

                    measured_at
                )
                VALUES (
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s
                )
                ON CONFLICT (instrument_id)
                DO UPDATE SET
                    last_price =
                        EXCLUDED.last_price,

                    high_24h =
                        EXCLUDED.high_24h,

                    low_24h =
                        EXCLUDED.low_24h,

                    price_change_pct_24h =
                        EXCLUDED.price_change_pct_24h,

                    volume_24h_native =
                        EXCLUDED.volume_24h_native,

                    volume_24h_base =
                        EXCLUDED.volume_24h_base,

                    volume_24h_quote =
                        EXCLUDED.volume_24h_quote,

                    volume_24h_contracts =
                        EXCLUDED.volume_24h_contracts,

                    turnover_24h =
                        EXCLUDED.turnover_24h,

                    turnover_denomination =
                        EXCLUDED.turnover_denomination,

                    open_interest =
                        EXCLUDED.open_interest,

                    open_interest_value =
                        EXCLUDED.open_interest_value,

                    funding_rate =
                        EXCLUDED.funding_rate,

                    measured_at =
                        EXCLUDED.measured_at,

                    updated_at = NOW()
                """,
                (
                    metrics.instrument_id,
                    metrics.last_price,
                    metrics.high_24h,
                    metrics.low_24h,
                    metrics.price_change_pct_24h,
                    metrics.volume_24h_native,
                    metrics.volume_24h_base,
                    metrics.volume_24h_quote,
                    metrics.volume_24h_contracts,
                    metrics.turnover_24h,
                    metrics.turnover_denomination,
                    metrics.open_interest,
                    metrics.open_interest_value,
                    metrics.funding_rate,
                    metrics.measured_at,
                ),
            )

    def upsert_many(
        self,
        metrics: Iterable[InstrumentMetrics],
    ) -> int:
        rows = list(metrics)

        if not rows:
            return 0

        with self.conn.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO catalog.instrument_metrics (
                    instrument_id,

                    last_price,

                    high_24h,
                    low_24h,
                    price_change_pct_24h,

                    volume_24h_native,
                    volume_24h_base,
                    volume_24h_quote,
                    volume_24h_contracts,

                    turnover_24h,
                    turnover_denomination,

                    open_interest,
                    open_interest_value,

                    funding_rate,

                    measured_at
                )
                VALUES (
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s
                )
                ON CONFLICT (instrument_id)
                DO UPDATE SET
                    last_price =
                        EXCLUDED.last_price,

                    high_24h =
                        EXCLUDED.high_24h,

                    low_24h =
                        EXCLUDED.low_24h,

                    price_change_pct_24h =
                        EXCLUDED.price_change_pct_24h,

                    volume_24h_native =
                        EXCLUDED.volume_24h_native,

                    volume_24h_base =
                        EXCLUDED.volume_24h_base,

                    volume_24h_quote =
                        EXCLUDED.volume_24h_quote,

                    volume_24h_contracts =
                        EXCLUDED.volume_24h_contracts,

                    turnover_24h =
                        EXCLUDED.turnover_24h,

                    turnover_denomination =
                        EXCLUDED.turnover_denomination,

                    open_interest =
                        EXCLUDED.open_interest,

                    open_interest_value =
                        EXCLUDED.open_interest_value,

                    funding_rate =
                        EXCLUDED.funding_rate,

                    measured_at =
                        EXCLUDED.measured_at,

                    updated_at = NOW()
                """,
                [
                    (
                        item.instrument_id,
                        item.last_price,
                        item.high_24h,
                        item.low_24h,
                        item.price_change_pct_24h,
                        item.volume_24h_native,
                        item.volume_24h_base,
                        item.volume_24h_quote,
                        item.volume_24h_contracts,
                        item.turnover_24h,
                        item.turnover_denomination,
                        item.open_interest,
                        item.open_interest_value,
                        item.funding_rate,
                        item.measured_at,
                    )
                    for item in rows
                ],
            )

        return len(rows)

    def get(
        self,
        instrument_id: int,
    ) -> InstrumentMetrics | None:
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    instrument_id,

                    last_price,

                    high_24h,
                    low_24h,
                    price_change_pct_24h,

                    volume_24h_native,
                    volume_24h_base,
                    volume_24h_quote,
                    volume_24h_contracts,

                    turnover_24h,
                    turnover_denomination,

                    open_interest,
                    open_interest_value,

                    funding_rate,

                    measured_at

                FROM catalog.instrument_metrics

                WHERE instrument_id = %s
                """,
                (instrument_id,),
            )

            row = cursor.fetchone()

        if row is None:
            return None

        return self._from_row(row)

    def get_many(
        self,
        instrument_ids: Iterable[int],
    ) -> list[InstrumentMetrics]:
        ids = list(dict.fromkeys(instrument_ids))

        if not ids:
            return []

        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    instrument_id,

                    last_price,

                    high_24h,
                    low_24h,
                    price_change_pct_24h,

                    volume_24h_native,
                    volume_24h_base,
                    volume_24h_quote,
                    volume_24h_contracts,

                    turnover_24h,
                    turnover_denomination,

                    open_interest,
                    open_interest_value,

                    funding_rate,

                    measured_at

                FROM catalog.instrument_metrics

                WHERE instrument_id = ANY(%s)
                """,
                (ids,),
            )

            rows = cursor.fetchall()

        return [self._from_row(row) for row in rows]

    @staticmethod
    def _from_row(
        row: dict,
    ) -> InstrumentMetrics:
        return InstrumentMetrics(
            instrument_id=row["instrument_id"],
            last_price=row["last_price"],
            high_24h=row["high_24h"],
            low_24h=row["low_24h"],
            price_change_pct_24h=(row["price_change_pct_24h"]),
            volume_24h_native=(row["volume_24h_native"]),
            volume_24h_base=(row["volume_24h_base"]),
            volume_24h_quote=(row["volume_24h_quote"]),
            volume_24h_contracts=(row["volume_24h_contracts"]),
            turnover_24h=row["turnover_24h"],
            turnover_denomination=(row["turnover_denomination"]),
            open_interest=row["open_interest"],
            open_interest_value=(row["open_interest_value"]),
            funding_rate=row["funding_rate"],
            measured_at=row["measured_at"],
        )

    def list_instruments(
        self,
        *,
        exchange: str | None = None,
        instrument_type: str | None = None,
        market_category: str | None = None,
        symbol: str | None = None,
    ) -> list[InstrumentMarketView]:
        """
        Return catalog instruments enriched with current market metrics.

        Instruments without current metrics are preserved.
        """

        conditions: list[str] = [
            "i.instrument_type <> 'option'",
        ]
        params: list[object] = []

        if exchange is not None:
            conditions.append("e.code = %s")
            params.append(exchange)

        if instrument_type is not None:
            conditions.append("i.instrument_type = %s")
            params.append(instrument_type)

        if market_category is not None:
            conditions.append("i.market_category = %s")
            params.append(market_category)

        if symbol is not None:
            conditions.append("i.symbol ILIKE %s")
            params.append(f"%{symbol}%")

        where_clause = "WHERE " + " AND ".join(conditions)

        query = f"""
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

                m.last_price,

                m.high_24h,
                m.low_24h,
                m.price_change_pct_24h,

                m.volume_24h_native,
                m.volume_24h_base,
                m.volume_24h_quote,
                m.volume_24h_contracts,

                m.turnover_24h,
                m.turnover_denomination,

                m.open_interest,
                m.open_interest_value,

                m.funding_rate,

                m.measured_at

            FROM catalog.instruments AS i

            JOIN catalog.exchanges AS e
                ON e.id = i.exchange_id

            LEFT JOIN catalog.instrument_metrics AS m
                ON m.instrument_id = i.id

            {where_clause}

            ORDER BY
                e.code,
                i.instrument_type,
                i.market_category,
                i.symbol,
                i.id
        """

        with self.conn.cursor() as cursor:
            cursor.execute(
                query,
                params,
            )

            rows = cursor.fetchall()

        return [InstrumentMarketView.model_validate(row) for row in rows]
