CREATE TABLE historical.publication_supersessions (
    publication_id bigint PRIMARY KEY REFERENCES historical.dataset_publications(id),
    canonical_id bigint NOT NULL REFERENCES historical.dataset_publications(id),
    state text NOT NULL CHECK (state IN ('retained', 'retiring', 'removed')),
    cleanup_id uuid NOT NULL,
    evidence jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    removed_at timestamptz,
    CHECK (publication_id <> canonical_id),
    CHECK ((state = 'removed') = (removed_at IS NOT NULL))
);

CREATE INDEX publication_supersessions_canonical
    ON historical.publication_supersessions (canonical_id);

CREATE TABLE historical.publication_work_evidence (
    publication_id bigint PRIMARY KEY REFERENCES historical.dataset_publications(id),
    work_key text NOT NULL CHECK (length(work_key) = 64),
    evidence jsonb NOT NULL,
    verified_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

ALTER TABLE historical.datasets DROP CONSTRAINT datasets_status_check;
ALTER TABLE historical.datasets ADD CONSTRAINT datasets_status_check
    CHECK (status IN ('pending', 'processing', 'complete', 'failed', 'superseded'));
ALTER TABLE historical.datasets DROP CONSTRAINT datasets_check;
ALTER TABLE historical.datasets ADD CONSTRAINT datasets_completion_status_check
    CHECK (completed_at IS NULL OR status IN ('complete', 'superseded'));
