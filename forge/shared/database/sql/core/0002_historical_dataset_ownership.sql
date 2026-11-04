-- Historical processing owns produced datasets and their lineage.
-- The migration runner owns the transaction and migration metadata.

ALTER TABLE catalog.datasets
    SET SCHEMA historical;

ALTER TABLE catalog.dataset_inputs
    SET SCHEMA historical;

ALTER TABLE catalog.dataset_raw_inputs
    SET SCHEMA historical;

-- Flexible operation-specific metadata, such as engine report details.
-- Dataset identities, storage locations, status, and lineage remain structured.
ALTER TABLE historical.datasets
    ADD COLUMN operation_details JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD CONSTRAINT datasets_operation_details_object_check
        CHECK (jsonb_typeof(operation_details) = 'object');

COMMENT ON TABLE historical.datasets IS
    'Historical outputs from normalization, merge, conversion, and future processing stages.';

COMMENT ON TABLE historical.dataset_inputs IS
    'Dataset-to-dataset lineage, including merge and conversion inputs.';

COMMENT ON TABLE historical.dataset_raw_inputs IS
    'Dataset-to-acquired-archive lineage, including the checksum of consumed bytes.';

COMMENT ON COLUMN historical.datasets.operation_details IS
    'Operation-specific metadata and engine report details; authoritative input relationships remain in lineage tables.';
