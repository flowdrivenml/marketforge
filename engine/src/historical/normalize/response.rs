//! Versioned CLI boundary, independent of batch and manifest versions.
use super::{ProcessingFailure, ProcessingResult, ProcessingStatus};
use crate::historical::job::{DatasetId, JobId};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NormalizationJobResult {
    pub job_id: JobId,
    pub dataset_id: DatasetId,
    pub status: ProcessingStatus,
    pub manifest_path: Option<String>,
    pub failure_report_path: Option<String>,
    pub events_written: u64,
    pub files_written: u64,
    pub start_timestamp_ns: Option<i64>,
    pub end_timestamp_ns: Option<i64>,
    pub failure: Option<ProcessingFailure>,
}
impl From<ProcessingResult> for NormalizationJobResult {
    fn from(r: ProcessingResult) -> Self {
        Self {
            job_id: r.job_id,
            dataset_id: r.dataset_id,
            status: r.status,
            manifest_path: r.manifest_path,
            failure_report_path: r.failure_report_path,
            events_written: r.events_written,
            files_written: r.files_written,
            start_timestamp_ns: r.start_timestamp_ns,
            end_timestamp_ns: r.end_timestamp_ns,
            failure: r.failure,
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NormalizationSummary {
    pub jobs_total: usize,
    pub jobs_complete: usize,
    pub jobs_failed: usize,
    pub jobs_not_reported: usize,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NormalizationError {
    pub code: String,
    pub message: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NormalizationResponse {
    pub schema_version: u32,
    pub kind: String,
    pub run_id: Option<String>,
    pub status: ProcessingStatus,
    pub jobs: Vec<NormalizationJobResult>,
    pub summary: NormalizationSummary,
    pub error: Option<NormalizationError>,
}
impl NormalizationResponse {
    /// Callers supply the identified requested job count, never invented results.
    pub fn new(
        run_id: Option<String>,
        total: usize,
        jobs: Vec<ProcessingResult>,
        error: Option<NormalizationError>,
    ) -> Self {
        assert!(
            jobs.len() <= total,
            "more definitive results than requested jobs"
        );
        let complete = jobs
            .iter()
            .filter(|j| j.status == ProcessingStatus::Complete)
            .count();
        let failed = jobs.len() - complete;
        let missing = total - jobs.len();
        Self {
            schema_version: 1,
            kind: "normalization_result".into(),
            run_id,
            status: if total > 0 && complete == total && error.is_none() {
                ProcessingStatus::Complete
            } else {
                ProcessingStatus::Failed
            },
            jobs: jobs.into_iter().map(Into::into).collect(),
            summary: NormalizationSummary {
                jobs_total: total,
                jobs_complete: complete,
                jobs_failed: failed,
                jobs_not_reported: missing,
            },
            error,
        }
    }
}
