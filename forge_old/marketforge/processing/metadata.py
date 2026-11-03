from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from psycopg import Connection

from marketforge.config.models import ProcessingProfile
from marketforge.config.repository import ConfigRepository


@dataclass(frozen=True)
class InstrumentMetadata:
    id: int

    exchange_id: int

    exchange: str
    symbol: str

    instrument_type: str
    market_category: str

    base_asset: str
    quote_asset: str
    settlement_asset: str | None

    status: str | None

    quantity_type: str

    contract_value: Any | None
    contract_value_asset: str | None

    tick_size: Any


@dataclass(frozen=True)
class RawFormatMetadata:
    id: int

    format_code: str
    dataset: str

    instrument_type: str
    market_category: str

    container_format: str
    compression: str | None
    record_format: str

    schema: dict[str, Any]


@dataclass(frozen=True)
class NormalizationMetadata:
    id: int
    raw_format_id: int

    target_schema: str

    rules: dict[str, Any]


@dataclass(frozen=True)
class ProcessingMetadata:
    instrument: InstrumentMetadata
    raw_format: RawFormatMetadata
    normalizations: tuple[NormalizationMetadata, ...]
    profile: ProcessingProfile


class ProcessingMetadataResolver:
    """
    Resolve PostgreSQL metadata required to construct processing jobs.

    This class performs database resolution only. It does not inspect raw
    archives, allocate job IDs, construct output paths, or invoke Rust.
    """

    def __init__(
        self,
        conn: Connection,
    ) -> None:
        self.conn = conn

        self.config_repository = ConfigRepository(conn)

    def resolve(
        self,
        *,
        exchange: str,
        symbol: str,
        instrument_type: str,
        market_category: str,
        dataset: str,
        profile: str = "default",
    ) -> ProcessingMetadata:
        instrument = self.resolve_instrument(
            exchange=exchange,
            symbol=symbol,
            instrument_type=instrument_type,
            market_category=market_category,
        )

        raw_format = self.resolve_raw_format(
            exchange=exchange,
            instrument_type=instrument_type,
            market_category=market_category,
            dataset=dataset,
        )

        normalizations = self.resolve_normalizations(
            raw_format_id=raw_format.id,
        )

        processing_profile = self.resolve_profile(profile)

        return ProcessingMetadata(
            instrument=instrument,
            raw_format=raw_format,
            normalizations=normalizations,
            profile=processing_profile,
        )

    def resolve_instrument(
        self,
        *,
        exchange: str,
        symbol: str,
        instrument_type: str,
        market_category: str,
    ) -> InstrumentMetadata:
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    i.id,
                    i.exchange_id,

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
                    s.contract_value_asset,

                    s.tick_size

                FROM catalog.instruments AS i

                JOIN catalog.exchanges AS e
                    ON e.id = i.exchange_id

                LEFT JOIN catalog.instrument_specs AS s
                    ON s.instrument_id = i.id

                WHERE e.code = %s
                  AND i.symbol = %s
                  AND i.instrument_type = %s
                  AND i.market_category = %s
                """,
                (
                    exchange,
                    symbol,
                    instrument_type,
                    market_category,
                ),
            )

            rows = cursor.fetchall()

        if not rows:
            raise LookupError(
                "Instrument not found: "
                f"{exchange=} "
                f"{symbol=} "
                f"{instrument_type=} "
                f"{market_category=}"
            )

        if len(rows) > 1:
            raise RuntimeError(
                "Ambiguous instrument metadata: "
                f"{exchange=} "
                f"{symbol=} "
                f"{instrument_type=} "
                f"{market_category=}"
            )

        row = rows[0]

        if row["quantity_type"] is None:
            raise RuntimeError(
                "Instrument has no instrument_specs: " f"{exchange=} {symbol=}"
            )

        if row["tick_size"] is None:
            raise RuntimeError("Instrument has no tick_size: " f"{exchange=} {symbol=}")

        return InstrumentMetadata(
            id=row["id"],
            exchange=row["exchange"],
            symbol=row["symbol"],
            instrument_type=row["instrument_type"],
            market_category=row["market_category"],
            base_asset=row["base_asset"],
            quote_asset=row["quote_asset"],
            settlement_asset=row["settlement_asset"],
            status=row["status"],
            quantity_type=row["quantity_type"],
            contract_value=row["contract_value"],
            contract_value_asset=row["contract_value_asset"],
            tick_size=row["tick_size"],
            exchange_id=row["exchange_id"],
        )

    def resolve_raw_format(
        self,
        *,
        exchange: str,
        instrument_type: str,
        market_category: str,
        dataset: str,
    ) -> RawFormatMetadata:
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    rf.id,
                    rf.format_code,
                    rf.dataset,
                    rf.instrument_type,
                    rf.market_category,
                    rf.container_format,
                    rf.compression,
                    rf.record_format,
                    rf.schema_json

                FROM catalog.raw_formats AS rf

                JOIN catalog.exchanges AS e
                    ON e.id = rf.exchange_id

                WHERE e.code = %s
                  AND rf.instrument_type = %s
                  AND rf.market_category = %s
                  AND rf.dataset = %s
                """,
                (
                    exchange,
                    instrument_type,
                    market_category,
                    dataset,
                ),
            )

            rows = cursor.fetchall()

        if not rows:
            raise LookupError(
                "Raw format not found: "
                f"{exchange=} "
                f"{instrument_type=} "
                f"{market_category=} "
                f"{dataset=}"
            )

        if len(rows) > 1:
            formats = ", ".join(row["format_code"] for row in rows)

            raise RuntimeError(
                "Raw format is ambiguous: "
                f"{exchange=} "
                f"{instrument_type=} "
                f"{market_category=} "
                f"{dataset=} "
                f"formats=[{formats}]"
            )

        row = rows[0]

        return RawFormatMetadata(
            id=row["id"],
            format_code=row["format_code"],
            dataset=row["dataset"],
            instrument_type=row["instrument_type"],
            market_category=row["market_category"],
            container_format=row["container_format"],
            compression=row["compression"],
            record_format=row["record_format"],
            schema=row["schema_json"],
        )

    def resolve_normalizations(
        self,
        *,
        raw_format_id: int,
    ) -> tuple[NormalizationMetadata, ...]:
        with self.conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    id,
                    raw_format_id,
                    target_schema,
                    rules_json

                FROM catalog.normalization_rules

                WHERE raw_format_id = %s

                ORDER BY
                    target_schema,
                    id
                """,
                (raw_format_id,),
            )

            rows = cursor.fetchall()

        if not rows:
            raise LookupError(
                "Normalization rules not found for " f"raw_format_id={raw_format_id}"
            )

        return tuple(
            NormalizationMetadata(
                id=row["id"],
                raw_format_id=row["raw_format_id"],
                target_schema=row["target_schema"],
                rules=row["rules_json"],
            )
            for row in rows
        )

    def resolve_profile(
        self,
        name: str,
    ) -> ProcessingProfile:
        profile = self.config_repository.get_processing_profile(name)

        if profile is None:
            raise LookupError("Processing profile not found: " f"{name}")

        return profile
