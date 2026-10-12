pub mod boundary;
pub mod config;
mod executor;
pub mod failure;
pub mod manifest;
pub mod metrics;
pub mod parquet;
pub mod resources;
mod result;
pub mod scheduler;
pub mod sequence_policy;
pub mod session;
pub mod task;
pub mod worker;
pub use executor::execute_processing_job;
pub mod merge;

pub use result::{ProcessingErrorCode, ProcessingFailure, ProcessingResult, ProcessingStatus};

pub use config::{
    ProcessingConfig, load_processing_config, validate_integrity_policy, validate_resources,
};
pub use sequence_policy::resolve_sequence_policy;
