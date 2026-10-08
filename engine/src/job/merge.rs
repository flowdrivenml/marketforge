use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use super::{DatasetId, StreamId};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DatasetType {
    Trade,
    L2,
    TradeL2,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MergeInput {
    pub dataset_id: DatasetId,
    pub stream_id: StreamId,
    pub input_path: PathBuf,
    pub data_type: DatasetType,
    pub start_timestamp_ns: i64,
    pub end_timestamp_ns: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimeRange {
    pub start_timestamp_ns: i64,
    pub end_timestamp_ns: i64,
}
