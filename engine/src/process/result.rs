use serde::{Deserialize, Serialize};

use crate::job::{DatasetId, JobId, PROCESSING_PROTOCOL_VERSION};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessingStatus {
    Complete,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessingErrorCode {
    InvalidJob,
    MissingSource,
    CorruptSource,
    DecompressionFailure,
    ScratchBudgetExceeded,
    ProcessingFailure,
    OutputFailure,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessingFailure {
    pub code: ProcessingErrorCode,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessingResult {
    pub protocol_version: u32,

    pub job_id: JobId,
    pub dataset_id: DatasetId,

    pub status: ProcessingStatus,

    pub manifest_path: Option<String>,

    pub events_written: u64,
    pub files_written: u64,

    pub start_timestamp_ns: Option<i64>,
    pub end_timestamp_ns: Option<i64>,

    pub failure: Option<ProcessingFailure>,
}

impl ProcessingResult {
    pub fn complete(job_id: JobId, dataset_id: DatasetId) -> Self {
        Self {
            protocol_version: PROCESSING_PROTOCOL_VERSION,

            job_id,
            dataset_id,

            status: ProcessingStatus::Complete,

            manifest_path: None,

            events_written: 0,
            files_written: 0,

            start_timestamp_ns: None,
            end_timestamp_ns: None,

            failure: None,
        }
    }

    pub fn failed(
        job_id: JobId,
        dataset_id: DatasetId,
        code: ProcessingErrorCode,
        message: impl Into<String>,
    ) -> Self {
        Self {
            protocol_version: PROCESSING_PROTOCOL_VERSION,

            job_id,
            dataset_id,

            status: ProcessingStatus::Failed,

            manifest_path: None,

            events_written: 0,
            files_written: 0,

            start_timestamp_ns: None,
            end_timestamp_ns: None,

            failure: Some(ProcessingFailure {
                code,
                message: message.into(),
            }),
        }
    }
}
