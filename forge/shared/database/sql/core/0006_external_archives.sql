CREATE TABLE historical.external_sources (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_key TEXT NOT NULL UNIQUE CHECK(btrim(source_key)<>''),
    source_kind TEXT NOT NULL CHECK(source_kind IN ('recording','purchased','other')),
    root_uri TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE historical.external_archives (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    external_source_id BIGINT NOT NULL REFERENCES historical.external_sources(id),
    source_object_key TEXT NOT NULL CHECK(btrim(source_object_key)<>''),
    sha256 TEXT NOT NULL CHECK(sha256 ~ '^[a-f0-9]{64}$'),
    bytes_expected BIGINT NOT NULL CHECK(bytes_expected>=0),
    instrument_id BIGINT REFERENCES catalog.instruments(id),
    identity_json JSONB NOT NULL CHECK(jsonb_typeof(identity_json)='object'),
    specifications_json JSONB,
    data_type TEXT NOT NULL CHECK(data_type IN ('trades','order_book_l2')),
    declared_start_at TIMESTAMPTZ,
    declared_end_at TIMESTAMPTZ,
    format_profile TEXT,
    normalization_readiness TEXT NOT NULL CHECK(normalization_readiness IN ('requires_profile','requires_historical_specs','pending_engine_validation')),
    import_provenance JSONB NOT NULL CHECK(jsonb_typeof(import_provenance)='object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(external_source_id,source_object_key,sha256),
    CHECK((declared_start_at IS NULL)=(declared_end_at IS NULL)),
    CHECK(declared_end_at IS NULL OR declared_end_at>declared_start_at)
);
ALTER TABLE historical.local_archives ALTER COLUMN remote_archive_id DROP NOT NULL;
ALTER TABLE historical.local_archives ADD COLUMN external_archive_id BIGINT REFERENCES historical.external_archives(id);
ALTER TABLE historical.local_archives ADD CONSTRAINT local_archive_exactly_one_origin CHECK(num_nonnulls(remote_archive_id,external_archive_id)=1);
CREATE INDEX local_archives_external_idx ON historical.local_archives(external_archive_id);
-- Retained ranking input provenance must not change underneath a published ranking.
CREATE FUNCTION ranking.guard_observation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS(SELECT 1 FROM ranking.entries e JOIN ranking.snapshots s ON s.id=e.snapshot_id
              WHERE e.observation_id=OLD.id AND s.status='published') THEN
        RAISE EXCEPTION 'Published ranking observations are immutable';
    END IF;
    RETURN COALESCE(NEW,OLD);
END $$;
CREATE TRIGGER observation_ranking_guard BEFORE UPDATE OR DELETE ON catalog.metric_observations FOR EACH ROW EXECUTE FUNCTION ranking.guard_observation();
