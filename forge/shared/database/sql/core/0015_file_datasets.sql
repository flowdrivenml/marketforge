CREATE TABLE historical.dataset_publications (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    instrument_namespace TEXT NOT NULL,
    engine_dataset_id TEXT NOT NULL,
    operation TEXT NOT NULL CHECK (operation IN ('normalize','merge')),
    storage_root TEXT NOT NULL,
    manifest_path TEXT NOT NULL,
    evidence JSONB NOT NULL,
    UNIQUE (instrument_namespace, engine_dataset_id)
);

ALTER TABLE historical.datasets
    ADD COLUMN publication_id BIGINT REFERENCES historical.dataset_publications(id),
    ADD COLUMN primary_file TEXT,
    ADD COLUMN file_sha256 TEXT CHECK (file_sha256 ~ '^[0-9a-f]{64}$'),
    ADD CONSTRAINT datasets_file_identity CHECK (
        num_nonnulls(publication_id, primary_file, file_sha256) IN (0, 3)
    );

DROP INDEX historical.datasets_engine_identity;
CREATE UNIQUE INDEX datasets_legacy_engine_identity ON historical.datasets
    ((operation_details->>'instrument_namespace'),(operation_details->>'engine_dataset_id'))
    WHERE publication_id IS NULL AND operation_details ? 'engine_dataset_id';
CREATE UNIQUE INDEX datasets_publication_file ON historical.datasets (publication_id, primary_file)
    WHERE primary_file IS NOT NULL;

CREATE TABLE historical.dataset_registration_migrations (
    legacy_dataset_id BIGINT PRIMARY KEY REFERENCES historical.datasets(id),
    publication_id BIGINT NOT NULL REFERENCES historical.dataset_publications(id)
);
