CREATE TABLE historical.execution_run_deletions (
    run_id text NOT NULL,
    database_id uuid NOT NULL,
    database_purpose text NOT NULL CHECK (database_purpose IN ('production','testing')),
    mode text NOT NULL CHECK (mode IN ('history_only','full')),
    state text NOT NULL CHECK (state IN ('hidden','retiring','removed')),
    plan_id uuid NOT NULL,
    plan_digest text NOT NULL,
    operation text NOT NULL,
    original_status text NOT NULL,
    parent_run_id text,
    receipt_path text NOT NULL,
    artifact_roots text[] NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (database_id,run_id),
    CHECK ((mode='history_only') = (state='hidden'))
);
CREATE INDEX execution_run_deletions_plan_idx
    ON historical.execution_run_deletions(database_id,plan_id);
