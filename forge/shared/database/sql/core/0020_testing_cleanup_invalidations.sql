CREATE TABLE historical.publication_invalidations (
    publication_id bigint PRIMARY KEY REFERENCES historical.dataset_publications(id),
    source_publication_id bigint NOT NULL REFERENCES historical.dataset_publications(id),
    cleanup_id uuid NOT NULL,
    evidence jsonb NOT NULL,
    invalidated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (publication_id <> source_publication_id)
);
