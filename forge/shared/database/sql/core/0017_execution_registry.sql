CREATE TABLE historical.execution_runs (
    run_id text PRIMARY KEY,
    operation text NOT NULL CHECK (operation IN ('acquisition','normalize','merge','convert','pipeline')),
    action text NOT NULL CHECK (length(action) BETWEEN 1 AND 64),
    recipe text CHECK (length(recipe) <= 1024),
    recipe_source text CHECK (length(recipe_source) <= 4096),
    database_purpose text NOT NULL CHECK (database_purpose IN ('production','testing')),
    database_id uuid NOT NULL,
    parent_run_id text REFERENCES historical.execution_runs(run_id),
    started_at timestamptz NOT NULL,
    finished_at timestamptz,
    status text NOT NULL CHECK (status IN ('pending','running','completed','prepared','partial','failed','blocked','interrupted','cancelled','stopped','unknown')),
    summary jsonb NOT NULL DEFAULT '{}' CHECK (octet_length(summary::text) <= 16384),
    artifact_root text NOT NULL,
    report_path text,
    error_summary text CHECK (length(error_summary) <= 512),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (parent_run_id IS NULL OR parent_run_id <> run_id),
    CHECK (finished_at IS NULL OR finished_at >= started_at),
    CHECK (run_id ~ ('^(' || operation || '-)?[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$')
        OR run_id ~ ('^' || operation || '-(execute|prepare|retry|register|recovery)-[0-9a-f-]{1,64}$'))
);

CREATE INDEX execution_runs_started_idx ON historical.execution_runs (started_at DESC, run_id);
CREATE INDEX execution_runs_operation_idx ON historical.execution_runs (operation, action, started_at DESC);
CREATE INDEX execution_runs_status_idx ON historical.execution_runs (status, started_at DESC);
CREATE INDEX execution_runs_recipe_idx ON historical.execution_runs (recipe, started_at DESC) WHERE recipe IS NOT NULL;
CREATE INDEX execution_runs_parent_idx ON historical.execution_runs (parent_run_id) WHERE parent_run_id IS NOT NULL;

CREATE TABLE historical.execution_attempts (
    attempt_id uuid PRIMARY KEY,
    run_id text NOT NULL REFERENCES historical.execution_runs(run_id),
    number integer NOT NULL CHECK (number > 0),
    action text NOT NULL,
    started_at timestamptz NOT NULL,
    finished_at timestamptz,
    status text NOT NULL CHECK (status IN ('running','completed','prepared','partial','failed','blocked','interrupted','cancelled','stopped','unknown')),
    outcome_path text,
    report_path text,
    error_summary text CHECK (length(error_summary) <= 512),
    UNIQUE (run_id, number),
    CHECK (finished_at IS NULL OR finished_at >= started_at)
);

CREATE TABLE historical.execution_resources (
    attempt_id uuid NOT NULL REFERENCES historical.execution_attempts(attempt_id),
    stage text NOT NULL CHECK (stage IN ('','acquire','normalize','merge')),
    kind text NOT NULL CHECK (kind IN ('discovery_scan','remote_archive','local_archive','engine_execution','engine_publication','primary_file_dataset','prepared_batch','prepared_job','conversion_output','report','child_run','source_publication','registry_publication')),
    reference text NOT NULL CHECK (length(reference) BETWEEN 1 AND 4096),
    PRIMARY KEY (attempt_id, stage, kind, reference)
);
