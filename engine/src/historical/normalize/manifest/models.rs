use serde::{Deserialize, Serialize};

use {
    crate::historical::job::ContentType,
    crate::historical::normalize::metrics::IntegrityEvaluation,
    crate::historical::normalize::metrics::IntegrityStatus,
    crate::historical::normalize::metrics::ProcessingMetricsReport,
};

pub const DATASET_MANIFEST_VERSION: u32 = 3;
/// Interleaved source row ownership / explicit cross-source suppression.
pub const RECONCILED_MANIFEST_VERSION: u32 = 4;
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
    TradeSourcePositions,
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
pub struct DatasetManifest<M = ProcessingMetricsReport> {
    pub protocol_version: u32,

    pub job_id: u64,
    pub dataset_id: u64,

    pub content_type: ContentType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub processing_quality: Option<ProcessingQuality>,

    pub events_written: u64,
    pub files_written: u64,

    pub start_timestamp_ns: Option<i64>,
    pub end_timestamp_ns: Option<i64>,

    pub source_tasks: Vec<u64>,
    /// Concrete authority and explicit family assertions used by this dataset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instrument_namespace: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resolved_instruments: Vec<serde_json::Value>,
    #[serde(default)]
    pub streams: Vec<crate::historical::normalize::streams::StreamOutput>,
    pub files: Vec<DatasetFile>,

    #[serde(default)]
    pub auxiliary_files: Vec<AuxiliaryFile>,

    pub integrity_status: IntegrityStatus,
    pub integrity_evaluation: IntegrityEvaluation,
    pub processing_metrics: M,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessingQuality {
    pub effective_policies: Option<serde_json::Value>,
    pub timestamp_ordering: String,
    pub native_reconstruction_valid: bool,
    pub supplied_observations_preserved: bool,
    pub continuity_proven: bool,
    pub external_historical_coverage_established: bool,
    pub inherited_metadata_is_batch_assertion: bool,
    pub logical_source_events: u64,
    pub verified_duplicate_observations: u64,
    pub known_unique_event_exclusions: u64,
    pub unverified_observation_exclusions: u64,
    pub loss_rate_denominator: u64,
    pub integrity_rejected_physical_rows: u64,
}
