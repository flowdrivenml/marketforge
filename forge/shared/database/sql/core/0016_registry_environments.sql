ALTER TABLE historical.registrations
    DROP CONSTRAINT registrations_environment_check,
    ADD CONSTRAINT registrations_environment_check CHECK (environment IN ('production', 'testing'));

ALTER TABLE historical.metadata_enrichment_runs
    DROP CONSTRAINT metadata_enrichment_runs_environment_check,
    ADD CONSTRAINT metadata_enrichment_runs_environment_check CHECK (environment IN ('production', 'testing'));
