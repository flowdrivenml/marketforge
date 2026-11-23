CREATE TABLE historical.resolved_work (
    registry_id uuid NOT NULL,
    operation text NOT NULL CHECK (operation IN ('normalize', 'merge', 'convert')),
    work_key text NOT NULL CHECK (length(work_key) = 64),
    owner text NOT NULL,
    evidence jsonb NOT NULL,
    result jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (registry_id, operation, work_key)
);
