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
        )
    }
    pub(crate) fn from_processing_job_ref<'a>(
        job: &ProcessingJob,
        dataset_root: &Path,
        processing_metrics: &'a ProcessingMetricsReport,
        integrity_evaluation: IntegrityEvaluation,
        limits: crate::shared::historical::limits::NormalizationLimits,
    ) -> Result<DatasetManifest<&'a ProcessingMetricsReport>> {
        DatasetManifest::build_with_metrics(
            job,
            dataset_root,
            processing_metrics,
            integrity_evaluation,
            limits,
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
        for stream in &streams {
            let mut offset = 0u64;
            for source in &stream.sources {
                let task = tasks.get(&source.task_id).ok_or_else(|| {
                    MarketForgeError::InvalidConfiguration("unknown stream source task".into())
                })?;
                let metrics = reports.get(&source.task_id).ok_or_else(|| {
                    MarketForgeError::InvalidConfiguration("missing stream source metrics".into())
                })?;
                if !seen.insert(source.task_id)
                    || task.stream_id.0 != stream.stream_id
                    || source.row_count()? != metrics.counters.events_written
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
        if job.output.content_type == crate::historical::job::ContentType::Depth {
            use crate::shared::persistence::parquet::read_depth_segments;
            if !streams.is_empty() {
                for stream in &streams {
                    let rows = stream.sources.last().map_or(0, |s| s.end_row);
                    let directory = dataset_root.join(&stream.directory);
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
                            let metrics = reports.get(&source.task_id).ok_or_else(|| {
                                MarketForgeError::InvalidConfiguration(
                                    "missing depth metrics".into(),
                                )
                            })?;
                            if source.boundary.exchange != task.exchange
                                || source.boundary.instrument_id != task.instrument_id
                                || source.boundary.symbol != task.symbol
                                || source.boundary.stream_id != task.stream_id.0
                                || Some(source.end_event - source.start_event)
                                    != metrics.accepted_depth_events
                            {
                                return Err(MarketForgeError::InvalidConfiguration(
                                    "depth source identity/event accounting mismatch".into(),
                                ));
                            }
                            Ok(())
                        },
                    )?;
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

        let manifest = Self {
            protocol_version: if streams
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

            events_written,
            files_written,

            start_timestamp_ns,
            end_timestamp_ns,

            source_tasks: job.tasks.iter().map(|task| task.task_id.0).collect(),

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
