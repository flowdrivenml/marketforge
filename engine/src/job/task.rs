use std::path::PathBuf;

use super::{InstrumentSpec, NormalizationConfig};
use crate::canonical::Exchange;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TaskId(pub u64);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StreamId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct FormatCode(pub String);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkTask {
    pub task_id: TaskId,
    pub stream_id: StreamId,

    pub input_path: PathBuf,
    pub format_code: FormatCode,

    pub exchange: Exchange,
    pub instrument_id: i64,
    pub symbol: String,

    pub raw_schema: Value,
    pub instrument: InstrumentSpec,
    pub normalizations: Vec<NormalizationConfig>,

    pub source_ordering: SourceOrdering,
    pub source_compression: SourceContainer,
    pub archive_member: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceOrdering {
    SourceOrdered,
    TimestampSortable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceContainer {
    Plain,
    Gzip,
    Zip,
    TarGzip,
}
