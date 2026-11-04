CREATE TABLE catalog.metric_refreshes (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    scope_json JSONB NOT NULL CHECK (jsonb_typeof(scope_json)='object'),
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    status TEXT NOT NULL CHECK (status IN ('running','complete','partial','failed','cancelled')),
    source_outcomes JSONB NOT NULL DEFAULT '[]' CHECK(jsonb_typeof(source_outcomes)='array'),
    CHECK(status='running' OR completed_at IS NOT NULL)
);
CREATE TABLE catalog.metric_observations (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    refresh_id BIGINT NOT NULL REFERENCES catalog.metric_refreshes(id),
    instrument_id BIGINT NOT NULL REFERENCES catalog.instruments(id),
    fetched_at TIMESTAMPTZ NOT NULL,
    source_observed_at TIMESTAMPTZ,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    window_basis TEXT NOT NULL CHECK(window_basis IN ('source','fetch_time')),
    base_unit TEXT NOT NULL CHECK(btrim(base_unit)<>''),
    quote_unit TEXT NOT NULL CHECK(btrim(quote_unit)<>''),
    last_price NUMERIC,
    base_volume NUMERIC,
    quote_volume NUMERIC,
    contract_volume NUMERIC,
    turnover NUMERIC,
    turnover_unit TEXT,
    turnover_estimated BOOLEAN NOT NULL DEFAULT FALSE,
    open_interest NUMERIC,
    open_interest_unit TEXT,
    funding_rate NUMERIC,
    mapping_version TEXT NOT NULL,
    raw_json JSONB NOT NULL CHECK(jsonb_typeof(raw_json)='object'),
    UNIQUE(refresh_id,instrument_id),
    CHECK(window_end>window_start),
    CHECK((turnover IS NULL)=(turnover_unit IS NULL)),
    CHECK((open_interest IS NULL)=(open_interest_unit IS NULL)),
    CHECK(turnover_unit IS NULL OR btrim(turnover_unit)<>''),
    CHECK(last_price IS NULL OR (last_price>=0 AND last_price<'Infinity'::numeric)),
    CHECK(base_volume IS NULL OR (base_volume>=0 AND base_volume<'Infinity'::numeric)),
    CHECK(quote_volume IS NULL OR (quote_volume>=0 AND quote_volume<'Infinity'::numeric)),
    CHECK(contract_volume IS NULL OR (contract_volume>=0 AND contract_volume<'Infinity'::numeric)),
    CHECK(turnover IS NULL OR (turnover>=0 AND turnover<'Infinity'::numeric)),
    CHECK(open_interest IS NULL OR (open_interest>=0 AND open_interest<'Infinity'::numeric)),
    CHECK(funding_rate IS NULL OR (funding_rate>'-Infinity'::numeric AND funding_rate<'Infinity'::numeric))
);
CREATE INDEX metric_observations_latest_idx ON catalog.metric_observations(instrument_id,window_end DESC,fetched_at DESC,id DESC);
CREATE INDEX metric_refreshes_complete_idx ON catalog.metric_refreshes(completed_at DESC) WHERE status='complete';
ALTER TABLE catalog.instrument_metrics
    ADD COLUMN observation_id BIGINT REFERENCES catalog.metric_observations(id),
    ADD COLUMN turnover_estimated BOOLEAN NOT NULL DEFAULT FALSE;
