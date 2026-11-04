-- Completeness of a metadata universe must survive a failed/paginated sync.
CREATE TABLE catalog.instrument_syncs (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    scope_json JSONB NOT NULL CHECK(jsonb_typeof(scope_json)='object'),
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    status TEXT NOT NULL CHECK(status IN ('running','complete','partial','failed','cancelled')),
    source_outcomes JSONB NOT NULL DEFAULT '[]' CHECK(jsonb_typeof(source_outcomes)='array'),
    CHECK(status='running' OR completed_at IS NOT NULL)
);
CREATE INDEX instrument_syncs_completed_idx ON catalog.instrument_syncs(completed_at DESC);
