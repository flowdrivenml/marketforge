use std::{
    fs,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::{
    error::{MarketForgeError, Result},
    job::ProcessingJob,
};

use super::{
    ProcessingErrorCode,
    metrics::{IntegrityEvaluation, ProcessingMetricsReport},
};

pub const PROCESSING_FAILURE_VERSION: u32 = 1;
pub const PROCESSING_FAILURE_FILENAME: &str = "failure.json";

// -----------------------------------------------------------------------------
// Failed processing report
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessingFailureReport {
    pub protocol_version: u32,

    pub job_id: u64,
    pub dataset_id: u64,

    pub failed_task_id: Option<u64>,

    pub error_code: ProcessingErrorCode,
    pub error_message: String,

    pub processing_metrics: ProcessingMetricsReport,

    pub integrity_evaluation: Option<IntegrityEvaluation>,
}

impl ProcessingFailureReport {
    pub fn new(
        job: &ProcessingJob,
        failed_task_id: Option<u64>,
        error_code: ProcessingErrorCode,
        error_message: impl Into<String>,
        processing_metrics: ProcessingMetricsReport,
        integrity_evaluation: Option<IntegrityEvaluation>,
    ) -> Self {
        Self {
            protocol_version: PROCESSING_FAILURE_VERSION,

            job_id: job.job_id.0,
            dataset_id: job.dataset_id.0,

            failed_task_id,

            error_code,
            error_message: error_message.into(),

            processing_metrics,
            integrity_evaluation,
        }
    }

    pub fn write_to(&self, staging: &Path) -> Result<PathBuf> {
        if !staging.is_dir() {
            return Err(MarketForgeError::InvalidConfiguration(format!(
                "failure-report staging directory does not exist: {}",
                staging.display(),
            )));
        }

        let path = staging.join(PROCESSING_FAILURE_FILENAME);

        if path.exists() {
            return Err(MarketForgeError::InvalidConfiguration(format!(
                "failure report already exists: {}",
                path.display(),
            )));
        }

        let contents = serde_json::to_vec_pretty(self).map_err(|error| {
            MarketForgeError::InvalidConfiguration(format!(
                "failed to serialize processing failure report: {error}",
            ))
        })?;

        fs::write(&path, contents).map_err(|error| {
            MarketForgeError::InvalidConfiguration(format!(
                "failed to write processing failure report {}: {error}",
                path.display(),
            ))
        })?;

        Ok(path)
    }
}
