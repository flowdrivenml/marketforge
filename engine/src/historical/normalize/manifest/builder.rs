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
        let files = inspect_parquet_files(dataset_root)?;
        let auxiliary_files = inspect_auxiliary_files(dataset_root)?;
        if job.output.content_type == crate::historical::job::ContentType::Depth {
            use crate::shared::persistence::parquet::read_depth_segments;
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

        Ok(Self {
            protocol_version: DATASET_MANIFEST_VERSION,

            job_id: job.job_id.0,
            dataset_id: job.dataset_id.0,

            content_type: job.output.content_type,

            events_written,
            files_written,

            start_timestamp_ns,
            end_timestamp_ns,

            source_tasks: job.tasks.iter().map(|task| task.task_id.0).collect(),

            files,
            auxiliary_files,

            integrity_status: integrity_evaluation.status,
            integrity_evaluation,
            processing_metrics,
        })
    }
}
