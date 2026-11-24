ALTER TABLE historical.execution_resources
    DROP CONSTRAINT execution_resources_stage_check,
    ADD CONSTRAINT execution_resources_stage_check
        CHECK (stage IN ('','acquire','normalize','merge','convert')),
    DROP CONSTRAINT execution_resources_kind_check,
    ADD CONSTRAINT execution_resources_kind_check
        CHECK (kind IN ('discovery_scan','remote_archive','local_archive','engine_execution',
                        'engine_publication','primary_file_dataset','prepared_batch','prepared_job',
                        'conversion_output','conversion_output_ref','report','child_run',
                        'source_publication','registry_publication'));
