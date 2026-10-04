from __future__ import annotations

from datetime import datetime
from typing import Any

from psycopg import Connection
from psycopg.types.json import Jsonb

from .models import Exchange, Instrument, InstrumentSpec


class CatalogRepository:
    def __init__(self, conn: Connection) -> None:
        self.conn = conn

    # ------------------------------------------------------------------
    # Exchanges
    # ------------------------------------------------------------------

    def get_exchange(self, code: str) -> Exchange | None:
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    id,
                    code,
                    name
                FROM catalog.exchanges
                WHERE code = %s
                """,
                (code,),
            )

            row = cursor.fetchone()

        if row is None:
            return None

        return Exchange(
            id=row["id"],
            code=row["code"],
            name=row["name"],
        )

    # ------------------------------------------------------------------
    # Instruments
    # ------------------------------------------------------------------

    def upsert_instrument(self, instrument: Instrument) -> int:
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO catalog.instruments (
                    exchange_id,
                    symbol,
                    instrument_type,
                    market_category,
                    base_asset,
                    quote_asset,
                    settlement_asset,
                    launch_time_ns,
                    expiry_ns,
                    strike,
                    option_type,
                    status
                )
                VALUES (
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    %s, %s, %s
                )
                ON CONFLICT (
                    exchange_id,
                    instrument_type,
                    market_category,
                    symbol
                )
                DO UPDATE SET
                    base_asset = EXCLUDED.base_asset,
                    quote_asset = EXCLUDED.quote_asset,
                    settlement_asset = EXCLUDED.settlement_asset,
                    launch_time_ns = EXCLUDED.launch_time_ns,
                    expiry_ns = EXCLUDED.expiry_ns,
                    strike = EXCLUDED.strike,
                    option_type = EXCLUDED.option_type,
                    status = EXCLUDED.status,
                    updated_at = NOW()
                RETURNING id
                """,
                (
                    instrument.exchange_id,
                    instrument.symbol,
                    instrument.instrument_type,
                    instrument.market_category,
                    instrument.base_asset,
                    instrument.quote_asset,
                    instrument.settlement_asset,
                    instrument.launch_time_ns,
                    instrument.expiry_ns,
                    instrument.strike,
                    instrument.option_type,
                    instrument.status,
                ),
            )

            row = cursor.fetchone()

        if row is None:
            raise RuntimeError(f"Failed to upsert instrument: {instrument.symbol}")

        return int(row["id"])

    def get_instrument(
        self,
        *,
        exchange_id: int,
        instrument_type: str,
        market_category: str,
        symbol: str,
    ) -> Instrument | None:
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    id,
                    exchange_id,
                    symbol,
                    instrument_type,
                    market_category,
                    base_asset,
                    quote_asset,
                    settlement_asset,
                    launch_time_ns,
                    expiry_ns,
                    strike,
                    option_type,
                    status
                FROM catalog.instruments
                WHERE exchange_id = %s
                  AND instrument_type = %s
                  AND market_category = %s
                  AND symbol = %s
                """,
                (
                    exchange_id,
                    instrument_type,
                    market_category,
                    symbol,
                ),
            )

            row = cursor.fetchone()

        if row is None:
            return None

        return Instrument(
            id=row["id"],
            exchange_id=row["exchange_id"],
            symbol=row["symbol"],
            instrument_type=row["instrument_type"],
            market_category=row["market_category"],
            base_asset=row["base_asset"],
            quote_asset=row["quote_asset"],
            settlement_asset=row["settlement_asset"],
            launch_time_ns=row["launch_time_ns"],
            expiry_ns=row["expiry_ns"],
            strike=row["strike"],
            option_type=row["option_type"],
            status=row["status"],
        )

    # ------------------------------------------------------------------
    # Instrument specifications
    # ------------------------------------------------------------------

    def upsert_instrument_spec(
        self,
        spec: InstrumentSpec,
    ) -> None:
        if spec.instrument_id is None:
            raise ValueError(
                "instrument_id is required before persisting InstrumentSpec"
            )

        if spec.fetched_at is None:
            raise ValueError("fetched_at is required before persisting InstrumentSpec")

        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO catalog.instrument_specs (
                    instrument_id,
                    quantity_type,
                    contract_value,
                    contract_value_asset,
                    tick_size,
                    qty_step,
                    min_qty,
                    max_qty,
                    min_notional,
                    fetched_at
                )
                VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s
                )
                ON CONFLICT (instrument_id)
                DO UPDATE SET
                    quantity_type = EXCLUDED.quantity_type,
                    contract_value = EXCLUDED.contract_value,
                    contract_value_asset = EXCLUDED.contract_value_asset,
                    tick_size = EXCLUDED.tick_size,
                    qty_step = EXCLUDED.qty_step,
                    min_qty = EXCLUDED.min_qty,
                    max_qty = EXCLUDED.max_qty,
                    min_notional = EXCLUDED.min_notional,
                    fetched_at = EXCLUDED.fetched_at
                """,
                (
                    spec.instrument_id,
                    spec.quantity_type,
                    spec.contract_value,
                    spec.contract_value_asset,
                    spec.tick_size,
                    spec.qty_step,
                    spec.min_qty,
                    spec.max_qty,
                    spec.min_notional,
                    spec.fetched_at,
                ),
            )

    def get_instrument_spec(
        self,
        instrument_id: int,
    ) -> InstrumentSpec | None:
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    instrument_id,
                    quantity_type,
                    contract_value,
                    contract_value_asset,
                    tick_size,
                    qty_step,
                    min_qty,
                    max_qty,
                    min_notional,
                    fetched_at
                FROM catalog.instrument_specs
                WHERE instrument_id = %s
                """,
                (instrument_id,),
            )

            row = cursor.fetchone()

        if row is None:
            return None

        return InstrumentSpec(
            instrument_id=row["instrument_id"],
            quantity_type=row["quantity_type"],
            contract_value=row["contract_value"],
            contract_value_asset=row["contract_value_asset"],
            tick_size=row["tick_size"],
            qty_step=row["qty_step"],
            min_qty=row["min_qty"],
            max_qty=row["max_qty"],
            min_notional=row["min_notional"],
            fetched_at=row["fetched_at"],
        )

    # ------------------------------------------------------------------
    # Raw exchange metadata
    # ------------------------------------------------------------------

    def store_raw_instrument_metadata(
        self,
        *,
        instrument_id: int,
        fetched_at: datetime,
        data: dict[str, Any],
    ) -> int:
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO catalog.instrument_api_raw (
                    instrument_id,
                    fetched_at,
                    raw_json
                )
                VALUES (%s, %s, %s)
                RETURNING id
                """,
                (
                    instrument_id,
                    fetched_at,
                    Jsonb(data),
                ),
            )

            row = cursor.fetchone()

        if row is None:
            raise RuntimeError("Failed to store raw instrument metadata")

        return int(row["id"])

    def list_instruments(
        self,
        *,
        exchange: str | None = None,
        instrument_type: str | None = None,
        market_category: str | None = None,
        symbol: str | None = None,
        base_asset: str | None = None,
        quote_asset: str | None = None,
        settlement_asset: str | None = None,
        status: str | None = None,
        quantity_type: str | None = None,
        contract_value_asset: str | None = None,
        limit: int | None = None,
    ) -> list[dict]:
        """List instruments with optional catalog filters."""

        conditions: list[str] = []
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
            conditions.append("i.symbol = %s")
            params.append(symbol)

        if base_asset is not None:
            conditions.append("i.base_asset = %s")
            params.append(base_asset)

        if quote_asset is not None:
            conditions.append("i.quote_asset = %s")
            params.append(quote_asset)

        if settlement_asset is not None:
            conditions.append("i.settlement_asset = %s")
            params.append(settlement_asset)

        if status is not None:
            conditions.append("i.status = %s")
            params.append(status)

        if quantity_type is not None:
            conditions.append("s.quantity_type = %s")
            params.append(quantity_type)

        if contract_value_asset is not None:
            conditions.append("s.contract_value_asset = %s")
            params.append(contract_value_asset)

        where_clause = ""

        if conditions:
            where_clause = "WHERE " + " AND ".join(conditions)

        limit_clause = ""

        if limit is not None:
            if limit <= 0:
                raise ValueError("limit must be greater than zero")

            limit_clause = "LIMIT %s"
            params.append(limit)

        query = f"""
            SELECT
                e.code AS exchange,

                i.symbol,
                i.instrument_type,
                i.market_category,

                i.base_asset,
                i.quote_asset,
                i.settlement_asset,

                i.launch_time_ns,
                i.expiry_ns,

                i.strike,
                i.option_type,

                i.status,

                s.quantity_type,
                s.contract_value,
                s.contract_value_asset,

                s.tick_size,
                s.qty_step,
                s.min_qty,
                s.max_qty,
                s.min_notional,

                s.fetched_at

            FROM catalog.instruments i

            JOIN catalog.exchanges e
                ON e.id = i.exchange_id

            JOIN catalog.instrument_specs s
                ON s.instrument_id = i.id

            {where_clause}

            ORDER BY
                e.code,
                i.instrument_type,
                i.market_category,
                i.symbol

            {limit_clause}
        """

        with self.conn.cursor() as cursor:
            cursor.execute(
                query,
                params,
            )

            return list(cursor.fetchall())

    def upsert_raw_format(
        self,
        *,
        exchange_id: int,
        format_code: str,
        dataset: str,
        instrument_type: str,
        market_category: str,
        container_format: str,
        compression: str | None,
        record_format: str,
        schema: dict,
        notes: str | None = None,
    ) -> int:
        """Insert or update a raw-format specification."""

        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO catalog.raw_formats (
                    exchange_id,
                    format_code,
                    dataset,
                    instrument_type,
                    market_category,
                    container_format,
                    compression,
                    record_format,
                    schema_json,
                    notes
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
                    %s
                )
                ON CONFLICT (
                    exchange_id,
                    format_code,
                    dataset,
                    instrument_type,
                    market_category
                )
                DO UPDATE SET
                    container_format = EXCLUDED.container_format,
                    compression = EXCLUDED.compression,
                    record_format = EXCLUDED.record_format,
                    schema_json = EXCLUDED.schema_json,
                    notes = EXCLUDED.notes,
                    updated_at = NOW()
                RETURNING id
                """,
                (
                    exchange_id,
                    format_code,
                    dataset,
                    instrument_type,
                    market_category,
                    container_format,
                    compression,
                    record_format,
                    Jsonb(schema),
                    notes,
                ),
            )

            row = cursor.fetchone()

            if row is None:
                raise RuntimeError("Raw-format upsert returned no row.")

            return row["id"]

    def list_raw_formats(
        self,
        *,
        exchange: str | None = None,
        format_code: str | None = None,
        dataset: str | None = None,
        instrument_type: str | None = None,
        market_category: str | None = None,
    ) -> list[dict]:
        """List raw-format specifications with optional filters."""

        conditions: list[str] = []
        params: list[object] = []

        if exchange is not None:
            conditions.append("e.code = %s")
            params.append(exchange)

        if format_code is not None:
            conditions.append("r.format_code = %s")
            params.append(format_code)

        if dataset is not None:
            conditions.append("r.dataset = %s")
            params.append(dataset)

        if instrument_type is not None:
            conditions.append("r.instrument_type = %s")
            params.append(instrument_type)

        if market_category is not None:
            conditions.append("r.market_category = %s")
            params.append(market_category)

        where_clause = ""

        if conditions:
            where_clause = "WHERE " + " AND ".join(conditions)

        query = f"""
            SELECT
                e.code AS exchange,
                r.format_code,
                r.dataset,
                r.instrument_type,
                r.market_category,
                r.container_format,
                r.compression,
                r.record_format,
                r.schema_json,
                r.notes
            FROM catalog.raw_formats r
            JOIN catalog.exchanges e
                ON e.id = r.exchange_id

            {where_clause}

            ORDER BY
                e.code,
                r.format_code,
                r.instrument_type,
                r.market_category
        """

        with self.conn.cursor() as cursor:
            cursor.execute(
                query,
                params,
            )

            return list(cursor.fetchall())

    def upsert_normalization_rule(
        self,
        *,
        raw_format_id: int,
        target_schema: str,
        rules: dict,
        notes: str | None = None,
    ) -> int:
        """Insert or update a normalization rule."""

        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO catalog.normalization_rules (
                    raw_format_id,
                    target_schema,
                    rules_json,
                    notes
                )
                VALUES (
                    %s,
                    %s,
                    %s,
                    %s
                )
                ON CONFLICT (
                    raw_format_id,
                    target_schema
                )
                DO UPDATE SET
                    rules_json = EXCLUDED.rules_json,
                    notes = EXCLUDED.notes,
                    updated_at = NOW()
                RETURNING id
                """,
                (
                    raw_format_id,
                    target_schema,
                    Jsonb(rules),
                    notes,
                ),
            )

            row = cursor.fetchone()

            if row is None:
                raise RuntimeError("Normalization-rule upsert returned no row.")

            return row["id"]

    def get_raw_format_id(
        self,
        *,
        exchange: str,
        format_code: str,
        dataset: str,
        instrument_type: str,
        market_category: str,
    ) -> int | None:
        """Return the ID of a raw-format application."""

        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT r.id
                FROM catalog.raw_formats r
                JOIN catalog.exchanges e
                    ON e.id = r.exchange_id
                WHERE e.code = %s
                AND r.format_code = %s
                AND r.dataset = %s
                AND r.instrument_type = %s
                AND r.market_category = %s
                """,
                (
                    exchange,
                    format_code,
                    dataset,
                    instrument_type,
                    market_category,
                ),
            )

            row = cursor.fetchone()

            if row is None:
                return None

            return row["id"]

    def list_normalization_rules(
        self,
        *,
        exchange: str | None = None,
        format_code: str | None = None,
        dataset: str | None = None,
        instrument_type: str | None = None,
        market_category: str | None = None,
        target_schema: str | None = None,
    ) -> list[dict]:
        """List normalization rules with optional filters."""

        conditions: list[str] = []
        params: list[object] = []

        if exchange is not None:
            conditions.append("e.code = %s")
            params.append(exchange)

        if format_code is not None:
            conditions.append("r.format_code = %s")
            params.append(format_code)

        if dataset is not None:
            conditions.append("r.dataset = %s")
            params.append(dataset)

        if instrument_type is not None:
            conditions.append("r.instrument_type = %s")
            params.append(instrument_type)

        if market_category is not None:
            conditions.append("r.market_category = %s")
            params.append(market_category)

        if target_schema is not None:
            conditions.append("n.target_schema = %s")
            params.append(target_schema)

        where_clause = ""

        if conditions:
            where_clause = "WHERE " + " AND ".join(conditions)

        query = f"""
            SELECT
                e.code AS exchange,
                r.format_code,
                r.dataset,
                r.instrument_type,
                r.market_category,
                n.target_schema,
                n.rules_json,
                n.notes
            FROM catalog.normalization_rules n
            JOIN catalog.raw_formats r
                ON r.id = n.raw_format_id
            JOIN catalog.exchanges e
                ON e.id = r.exchange_id

            {where_clause}

            ORDER BY
                e.code,
                r.format_code,
                r.instrument_type,
                r.market_category,
                n.target_schema
        """

        with self.conn.cursor() as cursor:
            cursor.execute(
                query,
                params,
            )

            return list(cursor.fetchall())
