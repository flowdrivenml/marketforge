from __future__ import annotations

from psycopg import Connection

SCHEMA_VERSION = 5


DDL = """
CREATE SCHEMA IF NOT EXISTS catalog;
CREATE SCHEMA IF NOT EXISTS live;


CREATE TABLE IF NOT EXISTS catalog.schema_meta (
    key     TEXT PRIMARY KEY,
    value   TEXT NOT NULL
);


CREATE TABLE IF NOT EXISTS catalog.exchanges (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    code        TEXT NOT NULL UNIQUE,
    name        TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


CREATE TABLE IF NOT EXISTS catalog.instruments (
    id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    exchange_id         BIGINT NOT NULL
                            REFERENCES catalog.exchanges(id),

    symbol              TEXT NOT NULL,

    instrument_type     TEXT NOT NULL,
    market_category     TEXT NOT NULL,

    base_asset          TEXT,
    quote_asset         TEXT,
    settlement_asset    TEXT,

    launch_time_ns      BIGINT,
    expiry_ns           BIGINT,

    strike              NUMERIC,
    option_type         TEXT,

    status              TEXT,

    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (
        exchange_id,
        instrument_type,
        market_category,
        symbol
    )
);


CREATE TABLE IF NOT EXISTS catalog.instrument_specs (
    instrument_id          BIGINT PRIMARY KEY
                               REFERENCES catalog.instruments(id)
                               ON DELETE CASCADE,

    quantity_type          TEXT NOT NULL,

    contract_value         NUMERIC,
    contract_value_asset   TEXT,

    tick_size              NUMERIC,
    qty_step               NUMERIC,
    min_qty                NUMERIC,
    max_qty                NUMERIC,
    min_notional           NUMERIC,

    fetched_at             TIMESTAMPTZ NOT NULL
);


CREATE TABLE IF NOT EXISTS catalog.instrument_api_raw (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    instrument_id   BIGINT NOT NULL
                        REFERENCES catalog.instruments(id)
                        ON DELETE CASCADE,

    fetched_at      TIMESTAMPTZ NOT NULL,
    raw_json        JSONB NOT NULL
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
        format_code
    )
);


CREATE INDEX IF NOT EXISTS idx_instruments_exchange
    ON catalog.instruments(exchange_id);


CREATE INDEX IF NOT EXISTS idx_instruments_symbol
    ON catalog.instruments(symbol);


CREATE INDEX IF NOT EXISTS idx_instrument_api_raw_instrument
    ON catalog.instrument_api_raw(instrument_id);


CREATE INDEX IF NOT EXISTS idx_instrument_api_raw_fetched
    ON catalog.instrument_api_raw(
        instrument_id,
        fetched_at DESC
    );


CREATE INDEX IF NOT EXISTS idx_raw_formats_exchange
    ON catalog.raw_formats(exchange_id);


CREATE INDEX IF NOT EXISTS idx_raw_formats_market
    ON catalog.raw_formats(
        exchange_id,
        dataset,
        instrument_type,
        market_category
    );


CREATE TABLE IF NOT EXISTS catalog.normalization_rules (
    id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    raw_format_id       BIGINT NOT NULL
                            REFERENCES catalog.raw_formats(id)
                            ON DELETE CASCADE,

    target_schema       TEXT NOT NULL,

    rules_json          JSONB NOT NULL,

    notes               TEXT,

    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (
        raw_format_id,
        target_schema
    ),

    CHECK (
        target_schema IN (
            'trade',
            'l2_snapshot',
            'l2_update'
        )
    )
);


CREATE TABLE IF NOT EXISTS live.trades (
    id                      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    instrument_id           BIGINT NOT NULL
                                REFERENCES catalog.instruments(id),

    event_timestamp_ns      BIGINT NOT NULL,

    trade_id                TEXT,
    sequence                BIGINT,

    side                    TEXT NOT NULL,

    price                   NUMERIC NOT NULL,

    quantity_base           NUMERIC,
    quantity_quote          NUMERIC,
    quantity_contracts      NUMERIC,

    is_rpi                  BOOLEAN,

    trade_iv                NUMERIC,
    mark_iv                 NUMERIC,
    index_price             NUMERIC,
    mark_price              NUMERIC,

    CHECK (
        side IN ('buy', 'sell')
    )
);


CREATE TABLE IF NOT EXISTS live.l2_snapshots (
    id                      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    instrument_id           BIGINT NOT NULL
                                REFERENCES catalog.instruments(id),

    event_timestamp_ns      BIGINT NOT NULL,
    system_timestamp_ns     BIGINT,

    sequence_first          BIGINT,
    sequence_last           BIGINT,
    sequence_previous       BIGINT,
    cross_sequence          BIGINT,

    CHECK (
        sequence_first IS NULL
        OR sequence_last IS NULL
        OR sequence_first <= sequence_last
    )
);


CREATE TABLE IF NOT EXISTS live.l2_snapshot_levels (
    snapshot_id             BIGINT NOT NULL
                                REFERENCES live.l2_snapshots(id)
                                ON DELETE CASCADE,

    side                    TEXT NOT NULL,
    level                   INTEGER NOT NULL,

    price                   NUMERIC NOT NULL,

    quantity_base           NUMERIC,
    quantity_quote          NUMERIC,
    quantity_contracts      NUMERIC,

    order_count             BIGINT,

    PRIMARY KEY (
        snapshot_id,
        side,
        level
    ),

    CHECK (
        side IN ('bid', 'ask')
    ),

    CHECK (
        level >= 0
    ),

    CHECK (
        order_count IS NULL
        OR order_count >= 0
    )
);


CREATE TABLE IF NOT EXISTS live.l2_updates (
    id                      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    instrument_id           BIGINT NOT NULL
                                REFERENCES catalog.instruments(id),

    event_timestamp_ns      BIGINT NOT NULL,
    system_timestamp_ns     BIGINT,

    sequence_first          BIGINT,
    sequence_last           BIGINT,
    sequence_previous       BIGINT,
    cross_sequence          BIGINT,

    CHECK (
        sequence_first IS NULL
        OR sequence_last IS NULL
        OR sequence_first <= sequence_last
    )
);


CREATE TABLE IF NOT EXISTS live.l2_update_levels (
    update_id               BIGINT NOT NULL
                                REFERENCES live.l2_updates(id)
                                ON DELETE CASCADE,

    change_index            INTEGER NOT NULL,

    side                    TEXT NOT NULL,
    action                  TEXT NOT NULL,

    price                   NUMERIC NOT NULL,

    quantity_base           NUMERIC,
    quantity_quote          NUMERIC,
    quantity_contracts      NUMERIC,

    order_count             BIGINT,

    PRIMARY KEY (
        update_id,
        change_index
    ),

    CHECK (
        change_index >= 0
    ),

    CHECK (
        side IN ('bid', 'ask')
    ),

    CHECK (
        action IN ('set', 'delete')
    ),

    CHECK (
        order_count IS NULL
        OR order_count >= 0
    )
);


CREATE INDEX IF NOT EXISTS idx_normalization_rules_raw_format
    ON catalog.normalization_rules(raw_format_id);


CREATE INDEX IF NOT EXISTS idx_normalization_rules_target
    ON catalog.normalization_rules(target_schema);


CREATE INDEX IF NOT EXISTS idx_live_trades_instrument_time
    ON live.trades(
        instrument_id,
        event_timestamp_ns
    );


CREATE INDEX IF NOT EXISTS idx_live_l2_snapshots_instrument_time
    ON live.l2_snapshots(
        instrument_id,
        event_timestamp_ns
    );


CREATE INDEX IF NOT EXISTS idx_live_l2_snapshot_levels_snapshot
    ON live.l2_snapshot_levels(snapshot_id);


CREATE INDEX IF NOT EXISTS idx_live_l2_updates_instrument_time
    ON live.l2_updates(
        instrument_id,
        event_timestamp_ns
    );


CREATE INDEX IF NOT EXISTS idx_live_l2_update_levels_update
    ON live.l2_update_levels(update_id);


CREATE TABLE IF NOT EXISTS catalog.datasets (
    id                      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    exchange_id             BIGINT
                                REFERENCES catalog.exchanges(id),

    instrument_id           BIGINT
                                REFERENCES catalog.instruments(id),

    data_type               TEXT NOT NULL,

    start_timestamp_ns      BIGINT NOT NULL,
    end_timestamp_ns        BIGINT NOT NULL,

    storage_format          TEXT NOT NULL,
    storage_path            TEXT,

    status                  TEXT NOT NULL,

    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CHECK (
        data_type IN (
            'trade',
            'l2_snapshot',
            'l2_update',
            'trade_l2'
        )
    ),

    CHECK (
        status IN (
            'pending',
            'processing',
            'complete',
            'failed'
        )
    ),

    CHECK (
        end_timestamp_ns > start_timestamp_ns
    )
);


CREATE TABLE IF NOT EXISTS catalog.dataset_inputs (
    dataset_id              BIGINT NOT NULL
                                REFERENCES catalog.datasets(id)
                                ON DELETE CASCADE,

    input_dataset_id        BIGINT NOT NULL
                                REFERENCES catalog.datasets(id),

    PRIMARY KEY (
        dataset_id,
        input_dataset_id
    ),

    CHECK (
        dataset_id <> input_dataset_id
    )
);
"""


def initialize_schema(conn: Connection) -> None:
    """
    Create the MarketForge PostgreSQL schemas and tables.

    The current schema version is recorded in catalog.schema_meta.
    """
    with conn.cursor() as cursor:
        cursor.execute(DDL)

        cursor.execute("""
            SELECT value
            FROM catalog.schema_meta
            WHERE key = 'schema_version'
            """)

        row = cursor.fetchone()

        if row is None:
            cursor.execute(
                """
                INSERT INTO catalog.schema_meta (key, value)
                VALUES ('schema_version', %s)
                """,
                (str(SCHEMA_VERSION),),
            )
            return

        version = int(row["value"])

        if version != SCHEMA_VERSION:
            raise RuntimeError(
                "Unsupported MarketForge database schema version: "
                f"{version}; expected {SCHEMA_VERSION}"
            )
