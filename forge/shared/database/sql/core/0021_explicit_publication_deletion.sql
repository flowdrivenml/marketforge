ALTER TABLE historical.publication_supersessions ALTER COLUMN canonical_id DROP NOT NULL;

CREATE TABLE historical.converted_output_deletions (
    registry_id uuid NOT NULL,
    work_key text NOT NULL CHECK (length(work_key) = 64),
    cleanup_id uuid NOT NULL,
    state text NOT NULL CHECK (state IN ('retiring','removed')),
    evidence jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    removed_at timestamptz,
    PRIMARY KEY (registry_id, work_key),
    CHECK ((state='removed') = (removed_at IS NOT NULL))
);
