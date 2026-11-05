-- A successful current listing does not delist absent historical records.
-- Retain its exact observed universe independently of catalog instrument status.
CREATE TABLE catalog.instrument_sync_members (
    sync_id BIGINT NOT NULL REFERENCES catalog.instrument_syncs(id) ON DELETE CASCADE,
    instrument_id BIGINT NOT NULL REFERENCES catalog.instruments(id) ON DELETE CASCADE,
    PRIMARY KEY (sync_id, instrument_id)
);
CREATE INDEX instrument_sync_members_instrument_idx
    ON catalog.instrument_sync_members(instrument_id, sync_id DESC);
