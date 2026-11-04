-- MarketForge core baseline, migration 0001.
-- Fresh Forge catalogue/historical schemas only. NOT an upgrade for schema 9.
-- Execute the entire migration in one transaction through the migration runner.
-- The runner owns migration history/checksums; this file does not self-register.
-- Rust owns live.* and its SQLx migration history; neither is modified here.
-- Configuration and raw-format definitions are maintained in TOML.

DO $guard$
BEGIN
    IF to_regnamespace('catalog') IS NOT NULL
       OR to_regnamespace('historical') IS NOT NULL THEN
        RAISE EXCEPTION
            'Baseline 0001 requires absent catalog and historical schemas; use the explicit legacy transition for an existing database';
    END IF;
END;
$guard$;

CREATE SCHEMA catalog;
CREATE SCHEMA historical;

CREATE TABLE catalog.exchanges (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    code        TEXT NOT NULL UNIQUE,
    name        TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE catalog.instruments (
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

CREATE TABLE catalog.instrument_specs (
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

CREATE TABLE catalog.instrument_api_raw (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    instrument_id   BIGINT NOT NULL
                        REFERENCES catalog.instruments(id)
                        ON DELETE CASCADE,

    fetched_at      TIMESTAMPTZ NOT NULL,
    raw_json        JSONB NOT NULL
);

CREATE INDEX idx_instruments_exchange
    ON catalog.instruments(exchange_id);

CREATE INDEX idx_instruments_symbol
    ON catalog.instruments(symbol);

CREATE INDEX idx_instrument_api_raw_instrument
    ON catalog.instrument_api_raw(instrument_id);

CREATE INDEX idx_instrument_api_raw_fetched
    ON catalog.instrument_api_raw(
        instrument_id,
        fetched_at DESC
    );

CREATE TABLE catalog.datasets (
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

    failure_message         TEXT,

    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at            TIMESTAMPTZ,

    CHECK (
        completed_at IS NULL
        OR status = 'complete'
    ),

    CHECK (
        data_type IN (
            'trade',
            'l2',
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

CREATE TABLE catalog.dataset_inputs (
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

CREATE TABLE catalog.instrument_metrics (
    instrument_id               BIGINT PRIMARY KEY
                                    REFERENCES catalog.instruments(id)
                                    ON DELETE CASCADE,

    last_price                  NUMERIC,

    high_24h                    NUMERIC,
    low_24h                     NUMERIC,
    price_change_pct_24h        NUMERIC,

    volume_24h_native           NUMERIC,
    volume_24h_base             NUMERIC,
    volume_24h_quote            NUMERIC,
    volume_24h_contracts        NUMERIC,

    turnover_24h                NUMERIC,
    turnover_denomination       TEXT,

    open_interest               NUMERIC,
    open_interest_value         NUMERIC,

    funding_rate                NUMERIC,

    measured_at                 TIMESTAMPTZ NOT NULL,
    updated_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CHECK (
        last_price IS NULL
        OR last_price >= 0
    ),

    CHECK (
        high_24h IS NULL
        OR high_24h >= 0
    ),

    CHECK (
        low_24h IS NULL
        OR low_24h >= 0
    ),

    CHECK (
        volume_24h_native IS NULL
        OR volume_24h_native >= 0
    ),

    CHECK (
        volume_24h_base IS NULL
        OR volume_24h_base >= 0
    ),

    CHECK (
        volume_24h_quote IS NULL
        OR volume_24h_quote >= 0
    ),

    CHECK (
        volume_24h_contracts IS NULL
        OR volume_24h_contracts >= 0
    ),

    CHECK (
        turnover_24h IS NULL
        OR turnover_24h >= 0
    ),

    CHECK (
        open_interest IS NULL
        OR open_interest >= 0
    ),

    CHECK (
        open_interest_value IS NULL
        OR open_interest_value >= 0
    )
);

CREATE INDEX idx_instrument_metrics_turnover
    ON catalog.instrument_metrics(
        turnover_24h DESC
    );

CREATE INDEX idx_instrument_metrics_measured
    ON catalog.instrument_metrics(
        measured_at DESC
    );

CREATE INDEX idx_instrument_metrics_open_interest_value
    ON catalog.instrument_metrics(
        open_interest_value DESC
    );

CREATE INDEX idx_datasets_status
    ON catalog.datasets(status);

CREATE INDEX idx_datasets_exchange
    ON catalog.datasets(exchange_id);

CREATE INDEX idx_datasets_instrument
    ON catalog.datasets(instrument_id);

CREATE INDEX idx_datasets_created
    ON catalog.datasets(created_at DESC);

-- Produced dataset stage and publication metadata.
ALTER TABLE catalog.datasets
    ADD COLUMN operation TEXT NOT NULL
        CHECK (operation IN ('normalize', 'merge', 'convert')),
    ADD COLUMN storage_root_key TEXT,
    ADD COLUMN manifest_path TEXT,
    ADD COLUMN result_report_path TEXT,
    ADD CONSTRAINT datasets_complete_publication_check CHECK (
        status <> 'complete'
        OR (
            storage_root_key IS NOT NULL
            AND BTRIM(storage_root_key) <> ''
            AND storage_path IS NOT NULL
            AND BTRIM(storage_path) <> ''
            AND completed_at IS NOT NULL
        )
    );

-- Stable source archive identity, separate from a refreshable download URL.
-- scope_json includes instrument_type, market_category, kind, identifier,
-- and every source-specific variant affecting the archive collection.
CREATE TABLE historical.remote_archives (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    exchange_id BIGINT NOT NULL REFERENCES catalog.exchanges(id),
    scope_key TEXT NOT NULL CHECK (BTRIM(scope_key) <> ''),
    scope_json JSONB NOT NULL CHECK (jsonb_typeof(scope_json) = 'object'),
    data_type TEXT NOT NULL CHECK (data_type IN ('trades', 'order_book_l2')),
    source_object_key TEXT NOT NULL CHECK (BTRIM(source_object_key) <> ''),
    filename TEXT NOT NULL CHECK (BTRIM(filename) <> ''),
    start_at TIMESTAMPTZ NOT NULL,
    end_at TIMESTAMPTZ NOT NULL,
    download_url TEXT,
    url_expires_at TIMESTAMPTZ,
    source_etag TEXT,
    source_last_modified TIMESTAMPTZ,
    remote_status TEXT NOT NULL DEFAULT 'available'
        CHECK (remote_status IN ('available', 'unavailable', 'unknown')),
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_checked_at TIMESTAMPTZ,
    source_metadata JSONB NOT NULL DEFAULT '{}'::jsonb
        CHECK (jsonb_typeof(source_metadata) = 'object'),
    CONSTRAINT remote_archives_identity_key
        UNIQUE (exchange_id, scope_key, data_type, source_object_key),
    CHECK (end_at > start_at)
);

CREATE INDEX remote_archives_selection_idx
    ON historical.remote_archives(exchange_id, scope_key, data_type, start_at);

-- A missing archive row is not proof that discovery searched an interval.
-- Completed scans, including empty results, establish observed search coverage.
CREATE TABLE historical.discovery_scans (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    exchange_id BIGINT NOT NULL REFERENCES catalog.exchanges(id),
    scope_key TEXT NOT NULL CHECK (BTRIM(scope_key) <> ''),
    scope_json JSONB NOT NULL CHECK (jsonb_typeof(scope_json) = 'object'),
    data_type TEXT NOT NULL CHECK (data_type IN ('trades', 'order_book_l2')),
    searched_start_at TIMESTAMPTZ NOT NULL,
    searched_end_at TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL DEFAULT 'running'
        CHECK (status IN ('running', 'complete', 'partial', 'failed', 'cancelled')),
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    refresh_after TIMESTAMPTZ,
    archives_found BIGINT NOT NULL DEFAULT 0 CHECK (archives_found >= 0),
    adapter_version TEXT,
    error_message TEXT,
    scan_metadata JSONB NOT NULL DEFAULT '{}'::jsonb
        CHECK (jsonb_typeof(scan_metadata) = 'object'),
    CHECK (searched_end_at > searched_start_at),
    CHECK (
        status <> 'complete'
        OR (completed_at IS NOT NULL AND refresh_after IS NOT NULL)
    )
);

CREATE INDEX discovery_scans_coverage_idx
    ON historical.discovery_scans(
        exchange_id, scope_key, data_type,
        searched_start_at, searched_end_at, refresh_after
    ) WHERE status = 'complete';

-- Each row represents a particular local acquisition/copy.
-- A revised source object must use a new acquisition row and destination.
-- Never overwrite lineage-referenced acquisition identity or checksum.
CREATE TABLE historical.local_archives (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    remote_archive_id BIGINT NOT NULL
        REFERENCES historical.remote_archives(id),
    storage_root_key TEXT NOT NULL CHECK (BTRIM(storage_root_key) <> ''),
    relative_path TEXT NOT NULL CHECK (
        relative_path <> ''
        AND LEFT(relative_path, 1) <> '/'
        AND RIGHT(relative_path, 1) <> '/'
        AND POSITION(CHR(92) IN relative_path) = 0
        AND relative_path !~ '(^|/)[.]{1,2}(/|$)'
    ),
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN (
            'pending', 'downloading', 'complete',
            'missing', 'corrupt', 'failed', 'deleted'
        )),
    bytes_written BIGINT CHECK (bytes_written >= 0),
    sha256 TEXT CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    source_etag TEXT,
    source_last_modified TIMESTAMPTZ,
    downloaded_at TIMESTAMPTZ,
    last_verified_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    failure_message TEXT,
    UNIQUE (storage_root_key, relative_path),
    CHECK (
        status <> 'complete'
        OR (
            bytes_written IS NOT NULL
            AND sha256 IS NOT NULL
            AND downloaded_at IS NOT NULL
        )
    )
);

CREATE INDEX local_archives_remote_idx
    ON historical.local_archives(remote_archive_id);

CREATE INDEX local_archives_status_idx
    ON historical.local_archives(status);

-- Link normalized outputs to the actual acquired bytes, not just source URLs.
CREATE TABLE catalog.dataset_raw_inputs (
    dataset_id BIGINT NOT NULL REFERENCES catalog.datasets(id),
    local_archive_id BIGINT NOT NULL REFERENCES historical.local_archives(id),
    input_sha256 TEXT NOT NULL CHECK (input_sha256 ~ '^[0-9a-f]{64}$'),
    PRIMARY KEY (dataset_id, local_archive_id)
);

CREATE INDEX dataset_raw_inputs_archive_idx
    ON catalog.dataset_raw_inputs(local_archive_id);

-- Static source catalogue only. No format/rule/profile seeds.
INSERT INTO catalog.exchanges(code, name) VALUES
    ('bybit', 'Bybit'),
    ('binance', 'Binance'),
    ('okx', 'OKX'),
    ('bitget', 'Bitget'),
    ('gateio', 'Gate.io');

COMMENT ON TABLE historical.remote_archives IS
    'Observed archive identities and refreshable source locations; not content coverage certification.';
COMMENT ON TABLE historical.discovery_scans IS
    'Discovery coverage and freshness, including successful searches finding no archives.';
COMMENT ON TABLE historical.local_archives IS
    'Registered local acquisitions. Actual bytes and checksums do not require remote size lookup.';
