CREATE SCHEMA IF NOT EXISTS live;

-- ============================================================
-- Streams
-- ============================================================

CREATE TABLE live.streams (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    stream_id TEXT NOT NULL UNIQUE,

    instrument_id BIGINT NOT NULL
        REFERENCES catalog.instruments(id)
        ON DELETE RESTRICT,

    stream_kind SMALLINT NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT live_stream_id_nonempty
        CHECK (length(btrim(stream_id)) > 0),

    CONSTRAINT live_stream_kind_valid
        CHECK (stream_kind IN (1, 2))
);

COMMENT ON COLUMN live.streams.stream_kind IS
    '1 = trade, 2 = l2';

-- ============================================================
-- Trades
-- ============================================================

CREATE TABLE live.trades (
    stream_key BIGINT NOT NULL
        REFERENCES live.streams(id)
        ON DELETE RESTRICT,

    log_epoch NUMERIC NOT NULL,
    event_offset NUMERIC NOT NULL,

    event_timestamp_ns BIGINT NOT NULL,
    system_timestamp_ns BIGINT,

    trade_id TEXT,
    sequence NUMERIC,

    side SMALLINT NOT NULL,

    price NUMERIC NOT NULL,

    quantity_base NUMERIC,
    quantity_quote NUMERIC,
    quantity_contracts NUMERIC,

    is_rpi BOOLEAN,

    trade_iv NUMERIC,
    mark_iv NUMERIC,

    index_price NUMERIC,
    mark_price NUMERIC,

    CONSTRAINT live_trades_pkey
        PRIMARY KEY (stream_key, log_epoch, event_offset),

    CONSTRAINT live_trades_epoch_u64
        CHECK (
            log_epoch = trunc(log_epoch)
            AND log_epoch >= 0
            AND log_epoch < 18446744073709551616
        ),

    CONSTRAINT live_trades_offset_u64
        CHECK (
            event_offset = trunc(event_offset)
            AND event_offset >= 0
            AND event_offset < 18446744073709551616
        ),

    CONSTRAINT live_trades_sequence_u64
        CHECK (
            sequence IS NULL
            OR (
                sequence = trunc(sequence)
                AND sequence >= 0
                AND sequence < 18446744073709551616
            )
        ),

    CONSTRAINT live_trades_side_valid
        CHECK (side IN (1, 2)),

    CONSTRAINT live_trades_price_positive
        CHECK (price > 0),

    CONSTRAINT live_trades_quantity_present
        CHECK (
            quantity_base IS NOT NULL
            OR quantity_quote IS NOT NULL
            OR quantity_contracts IS NOT NULL
        ),

    CONSTRAINT live_trades_quantities_nonnegative
        CHECK (
            (quantity_base IS NULL OR quantity_base >= 0)
            AND (quantity_quote IS NULL OR quantity_quote >= 0)
            AND (quantity_contracts IS NULL OR quantity_contracts >= 0)
        )
);

COMMENT ON COLUMN live.trades.side IS
    '1 = buy, 2 = sell';

-- ============================================================
-- Absolute L2 updates
-- ============================================================

CREATE TABLE live.l2_updates (
    stream_key BIGINT NOT NULL
        REFERENCES live.streams(id)
        ON DELETE RESTRICT,

    log_epoch NUMERIC NOT NULL,
    event_offset NUMERIC NOT NULL,

    event_timestamp_ns BIGINT NOT NULL,
    system_timestamp_ns BIGINT,

    side SMALLINT NOT NULL,

    price NUMERIC NOT NULL,

    quantity_base NUMERIC,
    quantity_quote NUMERIC,
    quantity_contracts NUMERIC,

    order_count NUMERIC,

    CONSTRAINT live_l2_updates_pkey
        PRIMARY KEY (stream_key, log_epoch, event_offset),

    CONSTRAINT live_l2_epoch_u64
        CHECK (
            log_epoch = trunc(log_epoch)
            AND log_epoch >= 0
            AND log_epoch < 18446744073709551616
        ),

    CONSTRAINT live_l2_offset_u64
        CHECK (
            event_offset = trunc(event_offset)
            AND event_offset >= 0
            AND event_offset < 18446744073709551616
        ),

    CONSTRAINT live_l2_side_valid
        CHECK (side IN (1, 2)),

    CONSTRAINT live_l2_price_positive
        CHECK (price > 0),

    CONSTRAINT live_l2_quantity_present
        CHECK (
            quantity_base IS NOT NULL
            OR quantity_quote IS NOT NULL
            OR quantity_contracts IS NOT NULL
        ),

    CONSTRAINT live_l2_quantities_nonnegative
        CHECK (
            (quantity_base IS NULL OR quantity_base >= 0)
            AND (quantity_quote IS NULL OR quantity_quote >= 0)
            AND (quantity_contracts IS NULL OR quantity_contracts >= 0)
        ),

    CONSTRAINT live_l2_order_count_u64
        CHECK (
            order_count IS NULL
            OR (
                order_count = trunc(order_count)
                AND order_count >= 0
                AND order_count < 18446744073709551616
            )
        )
);

COMMENT ON COLUMN live.l2_updates.side IS
    '1 = bid, 2 = ask';

COMMENT ON TABLE live.l2_updates IS
    'Canonical absolute L2 price-level updates. Zero quantity represents deletion. NULL means unavailable quantity representation.';

-- ============================================================
-- Verified recording coverage
-- ============================================================

CREATE TABLE live.recording_coverage (
    stream_key BIGINT NOT NULL
        REFERENCES live.streams(id)
        ON DELETE RESTRICT,

    log_epoch NUMERIC NOT NULL,

    first_offset NUMERIC NOT NULL,
    last_offset NUMERIC NOT NULL,

    verified_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT live_recording_coverage_pkey
        PRIMARY KEY (stream_key, log_epoch, first_offset),

    CONSTRAINT live_coverage_epoch_u64
        CHECK (
            log_epoch = trunc(log_epoch)
            AND log_epoch >= 0
            AND log_epoch < 18446744073709551616
        ),

    CONSTRAINT live_coverage_offsets_valid
        CHECK (
            first_offset = trunc(first_offset)
            AND last_offset = trunc(last_offset)
            AND first_offset >= 0
            AND last_offset >= first_offset
            AND last_offset < 18446744073709551616
        )
);
