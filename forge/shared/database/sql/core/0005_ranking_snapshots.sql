CREATE SCHEMA ranking;
CREATE TABLE ranking.snapshots (
    id UUID PRIMARY KEY,
    policy_name TEXT NOT NULL,
    policy_json JSONB NOT NULL CHECK(jsonb_typeof(policy_json)='object'),
    policy_sha256 TEXT NOT NULL CHECK(policy_sha256 ~ '^[a-f0-9]{64}$'),
    algorithm_version TEXT NOT NULL,
    comparison_currency TEXT NOT NULL,
    comparison_basis JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('draft','published')),
    partial_universe BOOLEAN NOT NULL,
    universe_count INTEGER NOT NULL CHECK(universe_count>0),
    included_count INTEGER NOT NULL CHECK(included_count>0 AND included_count<=universe_count)
);
CREATE INDEX ranking_snapshots_created_idx ON ranking.snapshots(created_at DESC,id);
CREATE TABLE ranking.entries (
    snapshot_id UUID NOT NULL REFERENCES ranking.snapshots(id),
    instrument_id BIGINT NOT NULL REFERENCES catalog.instruments(id),
    instrument_reference TEXT NOT NULL,
    observation_id BIGINT REFERENCES catalog.metric_observations(id),
    position INTEGER CHECK(position>0),
    score NUMERIC CHECK(score>=0 AND score<'Infinity'::numeric),
    exclusion_reason TEXT,
    fallback_reason TEXT,
    PRIMARY KEY(snapshot_id,instrument_id),
    UNIQUE(snapshot_id,position),
    UNIQUE(snapshot_id,instrument_reference),
    CHECK((position IS NULL)=(exclusion_reason IS NOT NULL)),
    CHECK(position IS NULL OR (score IS NOT NULL AND observation_id IS NOT NULL))
);
CREATE INDEX ranking_entries_order_idx ON ranking.entries(snapshot_id,position);
CREATE FUNCTION ranking.guard_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_TABLE_NAME='snapshots' THEN
        IF OLD.status='published' THEN RAISE EXCEPTION 'Published ranking snapshots are immutable'; END IF;
    ELSE
        IF EXISTS(SELECT 1 FROM ranking.snapshots WHERE id=COALESCE(NEW.snapshot_id,OLD.snapshot_id) AND status='published') THEN
            RAISE EXCEPTION 'Published ranking entries are immutable';
        END IF;
    END IF;
    RETURN COALESCE(NEW,OLD);
END $$;
CREATE TRIGGER ranking_snapshot_immutable BEFORE UPDATE OR DELETE ON ranking.snapshots FOR EACH ROW EXECUTE FUNCTION ranking.guard_immutable();
CREATE TRIGGER ranking_entry_immutable BEFORE INSERT OR UPDATE OR DELETE ON ranking.entries FOR EACH ROW EXECUTE FUNCTION ranking.guard_immutable();
