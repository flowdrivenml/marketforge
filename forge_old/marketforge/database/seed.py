from __future__ import annotations

from psycopg import Connection

from marketforge.catalog.repository import CatalogRepository
from marketforge.catalog.specs.normalization import NORMALIZATION_RULES
from marketforge.catalog.specs.raw_formats import RAW_FORMATS
from marketforge.config import ensure_default_processing_profiles

EXCHANGES = (
    ("bybit", "Bybit"),
    ("binance", "Binance"),
    ("okx", "OKX"),
    ("bitget", "Bitget"),
    ("gateio", "Gate.io"),
)


def seed_exchanges(conn: Connection) -> None:
    """
    Insert the exchanges supported by MarketForge.

    Safe to run repeatedly.
    """
    with conn.cursor() as cursor:
        cursor.executemany(
            """
            INSERT INTO catalog.exchanges (
                code,
                name
            )
            VALUES (%s, %s)
            ON CONFLICT (code)
            DO UPDATE SET
                name = EXCLUDED.name
            """,
            EXCHANGES,
        )


def seed_raw_formats(
    repo: CatalogRepository,
) -> int:
    """Seed MarketForge raw-format specifications."""

    count = 0

    for exchange_code, formats in RAW_FORMATS.items():
        exchange = repo.get_exchange(exchange_code)

        if exchange is None or exchange.id is None:
            raise RuntimeError(f"Exchange is not seeded: {exchange_code}")

        for raw_format in formats:
            repo.upsert_raw_format(
                exchange_id=exchange.id,
                format_code=raw_format["format_code"],
                dataset=raw_format["dataset"],
                instrument_type=raw_format["instrument_type"],
                market_category=raw_format["market_category"],
                container_format=raw_format["container_format"],
                compression=raw_format["compression"],
                record_format=raw_format["record_format"],
                schema=raw_format["schema"],
                notes=raw_format.get("notes"),
            )

            count += 1

    return count


def seed_normalization_rules(
    repo: CatalogRepository,
) -> int:
    """Seed MarketForge normalization rules."""

    count = 0

    for exchange_code, rules in NORMALIZATION_RULES.items():
        for rule in rules:
            raw_format_id = repo.get_raw_format_id(
                exchange=exchange_code,
                format_code=rule["format_code"],
                dataset=rule["dataset"],
                instrument_type=rule["instrument_type"],
                market_category=rule["market_category"],
            )

            if raw_format_id is None:
                raise RuntimeError(
                    "Raw format not found for normalization rule: "
                    f"{exchange_code} "
                    f"{rule['format_code']} "
                    f"{rule['dataset']} "
                    f"{rule['instrument_type']} "
                    f"{rule['market_category']}"
                )

            repo.upsert_normalization_rule(
                raw_format_id=raw_format_id,
                target_schema=rule["target_schema"],
                rules=rule["rules"],
                notes=rule.get("notes"),
            )

            count += 1

    return count


def seed_database(
    conn: Connection,
) -> None:
    seed_exchanges(conn)

    repo = CatalogRepository(conn)

    seed_raw_formats(repo)
    seed_normalization_rules(repo)

    ensure_default_processing_profiles(conn)
