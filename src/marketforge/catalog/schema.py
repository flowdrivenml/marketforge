from __future__ import annotations

import sqlite3

SCHEMA_VERSION = 1

DDL = """
CREATE TABLE IF NOT EXISTS schema_meta (
    key     TEXT PRIMARY KEY,
    value   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS exchanges (
    id          INTEGER PRIMARY KEY,
    code        TEXT NOT NULL UNIQUE,
    name        TEXT NOT NULL,
    created_at  TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS instruments (
    id                  INTEGER PRIMARY KEY,

    exchange_id         INTEGER NOT NULL,
    symbol              TEXT NOT NULL,

    instrument_type     TEXT NOT NULL,
    market_category     TEXT NOT NULL,

    base_asset          TEXT,
    quote_asset         TEXT,
    settlement_asset    TEXT,

    launch_time_ns      INTEGER,
    expiry_ns           INTEGER,

    strike              TEXT,
    option_type         TEXT,

    status              TEXT,

    created_at          TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (exchange_id)
        REFERENCES exchanges(id),

    UNIQUE(exchange_id, symbol)
);

CREATE TABLE IF NOT EXISTS instrument_specs (
    instrument_id          INTEGER PRIMARY KEY,

    quantity_type          TEXT NOT NULL,

    contract_value         TEXT,
    contract_value_asset   TEXT,

    tick_size              TEXT,
    qty_step               TEXT,
    min_qty                TEXT,
    max_qty                TEXT,
    min_notional           TEXT,

    fetched_at             TEXT NOT NULL,

    FOREIGN KEY (instrument_id)
        REFERENCES instruments(id)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS instrument_api_raw (
    id              INTEGER PRIMARY KEY,
    instrument_id   INTEGER NOT NULL,

    fetched_at      TEXT NOT NULL,
    raw_json        TEXT NOT NULL,

    FOREIGN KEY (instrument_id)
        REFERENCES instruments(id)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS catalog.raw_formats (
    id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    exchange_id         BIGINT NOT NULL
                            REFERENCES catalog.exchanges(id),

    format_code         TEXT NOT NULL,

    dataset             TEXT NOT NULL,
    instrument_type     TEXT NOT NULL,
    market_category     TEXT NOT NULL,

    container_format    TEXT NOT NULL,
    compression         TEXT,
    record_format       TEXT NOT NULL,

    schema_json         JSONB NOT NULL,

    notes               TEXT,

    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (
        exchange_id,
        format_code,
        dataset,
        instrument_type,
        market_category
    )
);

CREATE TABLE IF NOT EXISTS normalization_rules (
    id                  INTEGER PRIMARY KEY,

    raw_format_id       TEXT NOT NULL,

    source_field        TEXT NOT NULL,
    canonical_field     TEXT,

    classification      TEXT NOT NULL,
    transformation      TEXT,
    notes               TEXT,

    FOREIGN KEY (raw_format_id)
        REFERENCES raw_formats(id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_instruments_exchange
    ON instruments(exchange_id);

CREATE INDEX IF NOT EXISTS idx_instruments_symbol
    ON instruments(symbol);

CREATE INDEX IF NOT EXISTS idx_raw_formats_exchange
    ON raw_formats(exchange_id);

CREATE INDEX IF NOT EXISTS idx_rules_format
    ON normalization_rules(raw_format_id);
"""


def initialize_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(DDL)

    row = conn.execute(
        "SELECT value FROM schema_meta WHERE key = 'schema_version'"
    ).fetchone()

    if row is None:
        conn.execute(
            """
            INSERT INTO schema_meta (key, value)
            VALUES ('schema_version', ?)
            """,
            (str(SCHEMA_VERSION),),
        )
        return

    version = int(row["value"])

    if version != SCHEMA_VERSION:
        raise RuntimeError(
            f"Unsupported catalog schema version: {version}; "
            f"expected {SCHEMA_VERSION}"
        )
