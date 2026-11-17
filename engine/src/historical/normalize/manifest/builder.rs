use std::path::Path;

use {
    crate::historical::job::ProcessingJob,
    crate::historical::normalize::metrics::IntegrityEvaluation,
    crate::historical::normalize::metrics::IntegrityStatus,
    crate::historical::normalize::metrics::ProcessingMetricsReport,
    crate::shared::error::MarketForgeError, crate::shared::error::Result,
};

use super::{
    discovery::{inspect_auxiliary_files, inspect_parquet_files},
    models::{DATASET_MANIFEST_VERSION, DatasetManifest},
};

impl DatasetManifest {
    pub fn from_processing_job(
        job: &ProcessingJob,
        dataset_root: &Path,
        processing_metrics: ProcessingMetricsReport,
        integrity_evaluation: IntegrityEvaluation,
    ) -> Result<Self> {
        Self::build_with_metrics(
            job,
            dataset_root,
            processing_metrics,
            integrity_evaluation,
            job.resources.normalization_limits,
            None,
        )
    }
    pub(crate) fn from_processing_job_ref<'a>(
        job: &ProcessingJob,
        dataset_root: &Path,
        processing_metrics: &'a ProcessingMetricsReport,
        integrity_evaluation: IntegrityEvaluation,
        limits: crate::shared::historical::limits::NormalizationLimits,
        policies: serde_json::Value,
    ) -> Result<DatasetManifest<&'a ProcessingMetricsReport>> {
        DatasetManifest::build_with_metrics(
            job,
            dataset_root,
            processing_metrics,
            integrity_evaluation,
            limits,
            Some(policies),
        )
    }
}
impl<M: std::borrow::Borrow<ProcessingMetricsReport>> DatasetManifest<M> {
    fn build_with_metrics(
        job: &ProcessingJob,
        dataset_root: &Path,
        metrics: M,
        integrity_evaluation: IntegrityEvaluation,
        limits: crate::shared::historical::limits::NormalizationLimits,
        effective_policies: Option<serde_json::Value>,
    ) -> Result<Self> {
        let processing_metrics = metrics.borrow();
        let files = inspect_parquet_files(dataset_root)?;
        let streams = crate::historical::normalize::streams::read_outputs(dataset_root)?;
        let tasks: std::collections::HashMap<_, _> =
            job.tasks.iter().map(|t| (t.task_id.0, t)).collect();
        let reports: std::collections::HashMap<_, _> = processing_metrics
            .tasks
            .iter()
            .map(|t| (t.task_id, t))
            .collect();
        if tasks.len() != job.tasks.len() || reports.len() != processing_metrics.tasks.len() {
            return Err(MarketForgeError::InvalidConfiguration(
                "duplicate task/report identity".into(),
            ));
        }
        let mut seen = std::collections::HashSet::new();
        let mut contributions = std::collections::BTreeMap::<u64, u64>::new();
        for stream in &streams {
            let mut offset = 0u64;
            for source in &stream.sources {
                let task = tasks.get(&source.task_id).ok_or_else(|| {
                    MarketForgeError::InvalidConfiguration("unknown stream source task".into())
                })?;
                let metrics = reports.get(&source.task_id).ok_or_else(|| {
                    MarketForgeError::InvalidConfiguration("missing stream source metrics".into())
                })?;
                seen.insert(source.task_id);
                let contribution = contributions.entry(source.task_id).or_default();
                *contribution = contribution
                    .checked_add(source.row_count()?)
                    .ok_or_else(|| {
                        MarketForgeError::InvalidConfiguration(
                            "source contribution overflow".into(),
                        )
                    })?;
                if task.stream_id.0 != stream.stream_id
                    || *contribution > metrics.counters.events_written
                {
                    return Err(MarketForgeError::InvalidConfiguration(
                        "invalid stream source contribution".into(),
                    ));
                }
                offset = offset.checked_add(source.row_count()?).ok_or_else(|| {
                    MarketForgeError::InvalidConfiguration("source inventory overflow".into())
                })?;
            }
            let rows = files
                .iter()
                .filter(|f| f.path.starts_with(&format!("{}/", stream.directory)))
                .try_fold(0u64, |n, f| {
                    n.checked_add(f.rows).ok_or_else(|| {
                        MarketForgeError::InvalidConfiguration("stream row overflow".into())
                    })
                })?;
            if rows != offset {
                return Err(MarketForgeError::InvalidConfiguration(
                    "persisted stream/source row mismatch".into(),
                ));
            }
        }
        for (id, rows) in &contributions {
            if *rows != reports[id].counters.events_written {
                return Err(MarketForgeError::InvalidConfiguration(
                    "source contribution/metrics mismatch".into(),
                ));
            }
        }
        let absent_candidates = processing_metrics
            .tasks
            .iter()
            .filter(|task| {
                !seen.contains(&task.task_id)
                    && task.counters.events_written == 0
                    && task.source.as_ref().is_some_and(|source| {
                        source.family_candidate && source.observed_selected_records == 0
                    })
            })
            .count();
        if dataset_root.join("streams").exists()
            && seen.len() + absent_candidates != job.tasks.len()
        {
            return Err(MarketForgeError::InvalidConfiguration(
                "missing source contributions".into(),
            ));
        }

        let auxiliary_files = inspect_auxiliary_files(dataset_root)?;
        if job.output.content_type == crate::historical::job::ContentType::Trades {
            for stream in &streams {
                let required = stream.sources.iter().any(|span| {
                    reports[&span.task_id]
                        .source
                        .as_ref()
                        .is_some_and(|s| s.processing.policy_extension)
                });
                let path = dataset_root.join(&stream.directory).join("sources.jsonl");
                if required {
                    super::positions::validate(&path, stream, &reports)?;
                }
            }
        }
        if job.output.content_type == crate::historical::job::ContentType::Depth {
            use crate::shared::persistence::parquet::read_depth_segments;
            if !streams.is_empty() {
                for stream in &streams {
                    let rows = stream.sources.last().map_or(0, |s| s.end_row);
                    let directory = dataset_root.join(&stream.directory);
                    let mut indexed_counts = std::collections::BTreeMap::<u64, u64>::new();
                    crate::historical::normalize::streams_depth::validate(
                        &directory,
                        stream,
                        rows,
                        crate::historical::normalize::resources::boundary_read_limits(
                            job,
                            limits,
                            stream.sources.len(),
                        )?,
                        |source| {
                            let task = tasks.get(&source.task_id).ok_or_else(|| {
                                MarketForgeError::InvalidConfiguration(
                                    "unknown depth source".into(),
                                )
                            })?;
                            if source.boundary.exchange != task.exchange
                                || source.boundary.instrument_id != task.instrument_id
                                || source.boundary.symbol != task.symbol
                                || source.boundary.stream_id != task.stream_id.0
                            {
                                return Err(MarketForgeError::InvalidConfiguration(
                                    "depth source identity/event accounting mismatch".into(),
                                ));
                            }
                            let count = indexed_counts.entry(source.task_id).or_default();
                            *count = count
                                .checked_add(source.end_event - source.start_event)
                                .ok_or_else(|| {
                                    MarketForgeError::InvalidConfiguration(
                                        "depth indexed event overflow".into(),
                                    )
                                })?;
                            Ok(())
                        },
                    )?;
                    for (id, count) in indexed_counts {
                        let metrics = reports[&id];
                        let expected = metrics
                            .source
                            .as_ref()
                            .filter(|s| s.processing.logical_source_events > 0)
                            .map(|s| s.processing.published_logical_events)
                            .or_else(|| {
                                metrics.accepted_depth_events.and_then(|n| {
                                    n.checked_sub(
                                        metrics.source.as_ref().map_or(0, |s| s.duplicates),
                                    )
                                })
                            });
                        if Some(count) != expected {
                            return Err(MarketForgeError::InvalidConfiguration(format!(
                                "depth logical source event accounting mismatch: task={id}, indexed={count}, expected={expected:?}"
                            )));
                        }
                    }
                }
            } else {
                for task in &job.tasks {
                    let prefix = format!("tasks/task-{:06}/depth/", task.task_id.0);
                    for name in ["events.parquet", "segments.json", "boundary.json"] {
                        if !auxiliary_files
                            .iter()
                            .any(|file| file.path == format!("{prefix}{name}"))
                        {
                            return Err(MarketForgeError::InvalidConfiguration(format!(
                                "missing required depth artifact {prefix}{name}"
                            )));
                        }
                    }
                    let directory = dataset_root.join(&prefix);
                    let segments = read_depth_segments(&directory)?;
                    let rows = files
                        .iter()
                        .filter(|f| f.path.starts_with(&prefix))
                        .try_fold(0u64, |sum, f| {
                            sum.checked_add(f.rows).ok_or_else(|| {
                                MarketForgeError::InvalidConfiguration("depth row overflow".into())
                            })
                        })?;
                    if segments.total_levels != rows {
                        return Err(MarketForgeError::InvalidConfiguration(format!(
                            "depth reconstruction row mismatch for task {}",
                            task.task_id.0
                        )));
                    }
                }
            }
        }

        let events_written = files.iter().try_fold(0u64, |total, file| {
            total.checked_add(file.rows).ok_or_else(|| {
                MarketForgeError::InvalidConfiguration("dataset row count overflow".to_owned())
            })
        })?;

        let files_written = files.len() as u64;

        let start_timestamp_ns = files
            .iter()
            .filter_map(|file| file.start_timestamp_ns)
            .min();

        let end_timestamp_ns = files.iter().filter_map(|file| file.end_timestamp_ns).max();

        if integrity_evaluation.status == IntegrityStatus::Failed {
            return Err(MarketForgeError::InvalidConfiguration(
                "cannot construct committed dataset manifest with failed integrity".to_owned(),
            ));
        }

        if processing_metrics.counters.events_written != events_written {
            return Err(MarketForgeError::InvalidConfiguration(format!(
                "processing metrics/Parquet row mismatch: metrics={}, parquet={}",
                processing_metrics.counters.events_written, events_written,
            )));
        }

        let mut concrete = std::collections::BTreeMap::<i64, serde_json::Value>::new();
        let mut namespace = None;
        let mut catalog_bytes = 0u64;
        for task in &job.tasks {
            let source = &task.raw_schema["marketforge_source"];
            if let Some(value) = source["instrument_namespace"].as_str() {
                if namespace.as_deref().is_some_and(|old| old != value) {
                    return Err(MarketForgeError::InvalidConfiguration(
                        "conflicting instrument namespaces in dataset".into(),
                    ));
                }
                namespace = Some(value.to_owned());
            }
            if source["instrument"].is_null() {
                continue;
            }
            let entry = serde_json::json!({"instrument":source["instrument"],"metadata_origin":source["metadata_origin"],"family":source["inherited_family"],"family_specification_sha256":source["family_specification_sha256"],"identity_strategy":source["identity_strategy"]});
            if let Some(old) = concrete.get(&task.instrument_id) {
                if old["instrument"] != entry["instrument"] {
                    return Err(MarketForgeError::InvalidConfiguration(
                        "conflicting resolved concrete catalog entries".into(),
                    ));
                }
                if old["family"].is_null() && !entry["family"].is_null() {
                    concrete.insert(task.instrument_id, entry);
                }
            } else {
                catalog_bytes = catalog_bytes
                    .checked_add(
                        crate::historical::normalize::resources::estimate::prepared_json_memory(
                            &entry,
                        )?
                        .checked_add(1024)
                        .ok_or_else(|| {
                            MarketForgeError::InvalidConfiguration(
                                "resolved catalog accounting overflow".into(),
                            )
                        })?,
                    )
                    .ok_or_else(|| {
                        MarketForgeError::InvalidConfiguration(
                            "resolved catalog accounting overflow".into(),
                        )
                    })?;
                crate::shared::historical::limits::check(
                    "resolved catalog entries",
                    limits.metric_entries,
                    concrete.len() as u64 + 1,
                    "manifest concrete catalog",
                )?;
                crate::shared::historical::limits::check(
                    "resolved catalog bytes",
                    limits.reconciliation_bytes,
                    catalog_bytes,
                    "manifest concrete catalog",
                )?;
                concrete.insert(task.instrument_id, entry);
            }
        }
        let mut logical = 0u64;
        let mut duplicates = 0u64;
        let mut known = 0u64;
        let mut unverified = 0u64;
        let mut native = false;
        let mut extension = false;
        let mut continuity = true;
        for task in &processing_metrics.tasks {
            if let Some(source) = &task.source {
                let o = &source.processing;
                logical = logical
                    .checked_add(o.logical_source_events)
                    .ok_or_else(|| {
                        MarketForgeError::InvalidConfiguration(
                            "logical event count overflow".into(),
                        )
                    })?;
                duplicates = duplicates.checked_add(source.duplicates).ok_or_else(|| {
                    MarketForgeError::InvalidConfiguration("duplicate count overflow".into())
                })?;
                known = known
                    .checked_add(o.known_unique_event_exclusions)
                    .ok_or_else(|| {
                        MarketForgeError::InvalidConfiguration("unique exclusions overflow".into())
                    })?;
                unverified = unverified
                    .checked_add(o.unverified_observation_exclusions)
                    .ok_or_else(|| {
                        MarketForgeError::InvalidConfiguration(
                            "unverified exclusions overflow".into(),
                        )
                    })?;
                native |= o.published_native_order
                    || (job.output.content_type == crate::historical::job::ContentType::Depth
                        && source.ordering_validated == Some(false));
                extension |= o.policy_extension;
                continuity &= !o.unproven_continuity;
            }
        }
        let quality = super::models::ProcessingQuality {
            effective_policies,
            timestamp_ordering: if native {
                "native_source_order"
            } else {
                "event_timestamp_ns"
            }
            .into(),
            native_reconstruction_valid: true,
            supplied_observations_preserved: known == 0
                && unverified == 0
                && processing_metrics.counters.records_rejected == 0,
            continuity_proven: continuity,
            external_historical_coverage_established: false,
            inherited_metadata_is_batch_assertion: job
                .tasks
                .iter()
                .any(|t| !t.raw_schema["marketforge_source"]["inherited_family"].is_null()),
            logical_source_events: logical,
            verified_duplicate_observations: duplicates,
            known_unique_event_exclusions: known,
            unverified_observation_exclusions: unverified,
            loss_rate_denominator: logical.checked_sub(duplicates).ok_or_else(|| {
                MarketForgeError::InvalidConfiguration(
                    "duplicate count exceeds observations".into(),
                )
            })?,
            integrity_rejected_physical_rows: processing_metrics.counters.records_rejected,
        };
        let manifest = Self {
            protocol_version: if native || extension {
                5
            } else if streams
                .iter()
                .any(|s| s.sources.iter().any(|source| !source.ranges.is_empty()))
                || processing_metrics.counters.rows_suppressed > 0
                || absent_candidates > 0
            {
                super::models::RECONCILED_MANIFEST_VERSION
            } else {
                DATASET_MANIFEST_VERSION
            },

            job_id: job.job_id.0,
            dataset_id: job.dataset_id.0,

            content_type: job.output.content_type,
            processing_quality: Some(quality),

            events_written,
            files_written,

            start_timestamp_ns,
            end_timestamp_ns,

            source_tasks: job.tasks.iter().map(|task| task.task_id.0).collect(),
            instrument_namespace: namespace,
            resolved_instruments: concrete.into_values().collect(),

            files,
            streams,
            auxiliary_files,

            integrity_status: integrity_evaluation.status,
            integrity_evaluation,
            processing_metrics: metrics,
        };
        manifest.validate_streams()?;
        Ok(manifest)
    }
}
