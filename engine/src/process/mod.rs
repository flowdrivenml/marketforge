pub mod config;
mod executor;
mod result;

pub mod boundary;
pub mod failure;
pub mod manifest;
pub mod metrics;
pub mod parquet;
pub mod scheduler;
pub mod session;
pub mod task;
pub mod worker;

pub use executor::execute_processing_job;

pub use result::{ProcessingErrorCode, ProcessingFailure, ProcessingResult, ProcessingStatus};

pub use config::{
    ProcessingConfig, load_processing_config, validate_integrity_policy, validate_resources,
};
