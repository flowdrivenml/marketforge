-- Freeze native identity and assets at publication, for either ranking metric.
-- Older snapshots are left intact; inspection labels retained/current fallbacks.
ALTER TABLE ranking.entries ADD COLUMN instrument_metadata JSONB
    CHECK (jsonb_typeof(instrument_metadata)='object');
-- The existing entry immutability trigger protects this column after publication.
