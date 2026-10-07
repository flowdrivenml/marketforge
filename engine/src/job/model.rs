use serde::{Deserialize, Serialize};

use super::{
    DatasetId, IntegrityPolicy, JobId, OrderingConfig, OutputConfig, ProcessingOperation,
    ResourceConfig, StreamConfig, WorkTask,
};

pub const PROCESSING_PROTOCOL_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessingJob {
    pub protocol_version: u32,
    pub job_id: JobId,
    pub dataset_id: DatasetId,
    pub operation: ProcessingOperation,

    pub tasks: Vec<WorkTask>,
    pub streams: Vec<StreamConfig>,
    pub ordering: OrderingConfig,

    pub integrity_policy: IntegrityPolicy,

    pub resources: ResourceConfig,

    pub output: OutputConfig,
}
