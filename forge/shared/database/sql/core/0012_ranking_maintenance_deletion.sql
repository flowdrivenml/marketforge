-- Maintenance permits DELETE of exactly one snapshot's ranking-owned records.
-- It never permits edits, inserts, or deletion of catalogue/observation records.
CREATE OR REPLACE FUNCTION ranking.guard_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE target UUID;
BEGIN
    IF TG_TABLE_NAME='snapshots' THEN target := OLD.id;
    ELSIF TG_OP='INSERT' THEN target := NEW.snapshot_id;
    ELSE target := OLD.snapshot_id; END IF;
    IF TG_OP='DELETE' AND current_setting('ranking.maintenance_delete',true)=target::text THEN
        RETURN OLD;
    END IF;
    IF TG_TABLE_NAME='snapshots' THEN
        IF OLD.status='published' THEN RAISE EXCEPTION 'Published ranking snapshots are immutable'; END IF;
    ELSE
        IF EXISTS(SELECT 1 FROM ranking.snapshots WHERE id=COALESCE(NEW.snapshot_id,OLD.snapshot_id) AND status='published') THEN
            RAISE EXCEPTION 'Published ranking entries are immutable';
        END IF;
    END IF;
    RETURN COALESCE(NEW,OLD);
END $$;

CREATE OR REPLACE FUNCTION ranking.guard_conversion_evidence() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE old_snapshot UUID; new_snapshot UUID;
BEGIN
    IF TG_TABLE_NAME='conversions' THEN
        IF TG_OP<>'INSERT' THEN old_snapshot:=OLD.snapshot_id; END IF;
        IF TG_OP<>'DELETE' THEN new_snapshot:=NEW.snapshot_id; END IF;
    ELSE
        IF TG_OP<>'INSERT' THEN SELECT snapshot_id INTO old_snapshot FROM ranking.conversions WHERE id=OLD.conversion_id; END IF;
        IF TG_OP<>'DELETE' THEN SELECT snapshot_id INTO new_snapshot FROM ranking.conversions WHERE id=NEW.conversion_id; END IF;
    END IF;
    IF TG_OP='DELETE' AND current_setting('ranking.maintenance_delete',true)=old_snapshot::text THEN
        RETURN OLD;
    END IF;
    IF EXISTS(SELECT 1 FROM ranking.snapshots WHERE id IN (old_snapshot,new_snapshot) AND status='published') THEN
        RAISE EXCEPTION 'Published ranking conversion evidence is immutable';
    END IF;
    RETURN COALESCE(NEW,OLD);
END $$;

CREATE FUNCTION ranking.delete_snapshot(target UUID) RETURNS BOOLEAN LANGUAGE plpgsql AS $$
DECLARE previous TEXT;
BEGIN
    PERFORM pg_advisory_xact_lock(506746721904);
    PERFORM 1 FROM ranking.snapshots WHERE id=target FOR UPDATE;
    IF NOT FOUND THEN RETURN false; END IF;
    previous := current_setting('ranking.maintenance_delete',true);
    PERFORM set_config('ranking.maintenance_delete',target::text,true);
    DELETE FROM ranking.entries WHERE snapshot_id=target;
    DELETE FROM ranking.conversion_inputs WHERE conversion_id IN
        (SELECT id FROM ranking.conversions WHERE snapshot_id=target);
    DELETE FROM ranking.conversions WHERE snapshot_id=target;
    DELETE FROM ranking.snapshots WHERE id=target;
    PERFORM set_config('ranking.maintenance_delete',COALESCE(previous,''),true);
    RETURN true;
EXCEPTION WHEN OTHERS THEN
    PERFORM set_config('ranking.maintenance_delete',COALESCE(previous,''),true);
    RAISE;
END $$;
