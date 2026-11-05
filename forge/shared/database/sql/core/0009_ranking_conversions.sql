-- Reference-price valuations are immutable ranking evidence, never native metrics.
CREATE TABLE ranking.conversions (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    snapshot_id UUID NOT NULL REFERENCES ranking.snapshots(id),
    evidence_sha256 TEXT NOT NULL CHECK(evidence_sha256 ~ '^[a-f0-9]{64}$'),
    method TEXT NOT NULL CHECK(method IN ('identity','manual','equivalence','direct','bridged')),
    source_currency TEXT NOT NULL,
    comparison_currency TEXT NOT NULL,
    target_exchange TEXT NOT NULL,
    rate NUMERIC NOT NULL CHECK(rate>0 AND rate<'Infinity'::numeric),
    numerator TEXT NOT NULL CHECK(numerator ~ '^[1-9][0-9]*$'),
    denominator TEXT NOT NULL CHECK(denominator ~ '^[1-9][0-9]*$'),
    evidence_json JSONB NOT NULL CHECK(jsonb_typeof(evidence_json)='object'),
    UNIQUE(snapshot_id,evidence_sha256),
    UNIQUE(snapshot_id,id)
);
CREATE TABLE ranking.conversion_inputs (
    conversion_id BIGINT NOT NULL REFERENCES ranking.conversions(id),
    observation_id BIGINT NOT NULL REFERENCES catalog.metric_observations(id),
    leg_index SMALLINT NOT NULL CHECK(leg_index IN (0,1)),
    PRIMARY KEY(conversion_id,leg_index),
    UNIQUE(conversion_id,observation_id)
);
CREATE INDEX conversion_inputs_observation_idx ON ranking.conversion_inputs(observation_id);
ALTER TABLE ranking.entries
    ADD COLUMN conversion_id BIGINT,
    ADD COLUMN native_turnover NUMERIC CHECK(native_turnover>=0 AND native_turnover<'Infinity'::numeric),
    ADD COLUMN native_unit TEXT,
    ADD CONSTRAINT ranking_entry_conversion_snapshot_fk FOREIGN KEY(snapshot_id,conversion_id)
        REFERENCES ranking.conversions(snapshot_id,id);

CREATE FUNCTION ranking.guard_conversion_evidence() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE old_snapshot UUID; new_snapshot UUID;
BEGIN
    IF TG_TABLE_NAME='conversions' THEN
        IF TG_OP<>'INSERT' THEN old_snapshot:=OLD.snapshot_id; END IF;
        IF TG_OP<>'DELETE' THEN new_snapshot:=NEW.snapshot_id; END IF;
    ELSE
        IF TG_OP<>'INSERT' THEN SELECT snapshot_id INTO old_snapshot FROM ranking.conversions WHERE id=OLD.conversion_id; END IF;
        IF TG_OP<>'DELETE' THEN SELECT snapshot_id INTO new_snapshot FROM ranking.conversions WHERE id=NEW.conversion_id; END IF;
    END IF;
    IF EXISTS(SELECT 1 FROM ranking.snapshots WHERE id IN (old_snapshot,new_snapshot) AND status='published') THEN
        RAISE EXCEPTION 'Published ranking conversion evidence is immutable';
    END IF;
    RETURN COALESCE(NEW,OLD);
END $$;
CREATE TRIGGER conversion_immutable BEFORE INSERT OR UPDATE OR DELETE ON ranking.conversions
    FOR EACH ROW EXECUTE FUNCTION ranking.guard_conversion_evidence();
CREATE TRIGGER conversion_input_immutable BEFORE INSERT OR UPDATE OR DELETE ON ranking.conversion_inputs
    FOR EACH ROW EXECUTE FUNCTION ranking.guard_conversion_evidence();

CREATE FUNCTION ranking.guard_conversion_observation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS(SELECT 1 FROM ranking.conversion_inputs x JOIN ranking.conversions c ON c.id=x.conversion_id
              JOIN ranking.snapshots s ON s.id=c.snapshot_id WHERE x.observation_id=OLD.id AND s.status='published') THEN
        RAISE EXCEPTION 'Published ranking conversion observations are immutable';
    END IF;
    RETURN COALESCE(NEW,OLD);
END $$;
CREATE TRIGGER conversion_observation_guard BEFORE UPDATE OR DELETE ON catalog.metric_observations
    FOR EACH ROW EXECUTE FUNCTION ranking.guard_conversion_observation();

CREATE FUNCTION ranking.validate_conversion_publication() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.status='published' AND NEW.algorithm_version='turnover24h-spotfx-rational80-v2' THEN
        IF EXISTS(SELECT 1 FROM ranking.entries WHERE snapshot_id=NEW.id AND position IS NOT NULL
                  AND (conversion_id IS NULL OR native_turnover IS NULL OR native_unit IS NULL)) THEN
            RAISE EXCEPTION 'New ranking entries require native turnover and conversion evidence';
        END IF;
        IF EXISTS(SELECT 1 FROM ranking.conversions c WHERE c.snapshot_id=NEW.id AND
                  (SELECT count(*) FROM ranking.conversion_inputs x WHERE x.conversion_id=c.id)<>
                  CASE c.method WHEN 'direct' THEN 1 WHEN 'bridged' THEN 2 ELSE 0 END) THEN
            RAISE EXCEPTION 'Conversion input count disagrees with conversion method';
        END IF;
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER conversion_publication_guard BEFORE UPDATE ON ranking.snapshots
    FOR EACH ROW EXECUTE FUNCTION ranking.validate_conversion_publication();
