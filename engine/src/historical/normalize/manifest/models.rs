use serde::{Deserialize, Serialize};

use crate::{
    job::ContentType,
    process::metrics::{IntegrityEvaluation, IntegrityStatus, ProcessingMetricsReport},
};

pub const DATASET_MANIFEST_VERSION: u32 = 2;
pub const DATASET_MANIFEST_FILENAME: &str = "manifest.json";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatasetFile {
    pub path: String,
    pub rows: u64,
    pub size_bytes: u64,

    pub start_timestamp_ns: Option<i64>,
    pub end_timestamp_ns: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuxiliaryFileKind {
    DepthEvents,
    DepthSegments,
    DepthBoundary,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuxiliaryFile {
    pub path: String,
    pub kind: AuxiliaryFileKind,
    pub size_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatasetManifest {
    pub protocol_version: u32,

    pub job_id: u64,
    pub dataset_id: u64,

    pub content_type: ContentType,

    pub events_written: u64,
    pub files_written: u64,

    pub start_timestamp_ns: Option<i64>,
    pub end_timestamp_ns: Option<i64>,

    pub source_tasks: Vec<u64>,
    pub files: Vec<DatasetFile>,

    #[serde(default)]
    pub auxiliary_files: Vec<AuxiliaryFile>,

    pub integrity_status: IntegrityStatus,
    pub integrity_evaluation: IntegrityEvaluation,
    pub processing_metrics: ProcessingMetricsReport,
}
