-- Older snapshots do not carry a verifiable publication timestamp.
ALTER TABLE ranking.snapshots ADD COLUMN published_at TIMESTAMPTZ;

CREATE INDEX ranking_historical_snapshots_idx
ON ranking.snapshots (policy_name, published_at DESC, id DESC)
WHERE status = 'published' AND published_at IS NOT NULL;

CREATE FUNCTION ranking.stamp_publication() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.status = 'published' THEN
        NEW.published_at := clock_timestamp();
    END IF;
    RETURN NEW;
END $$;

CREATE TRIGGER ranking_stamp_publication
BEFORE INSERT OR UPDATE OF status ON ranking.snapshots
FOR EACH ROW EXECUTE FUNCTION ranking.stamp_publication();
