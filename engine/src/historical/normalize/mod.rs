pub mod benchmark;
pub mod boundary;
pub mod config;
pub use super::convert;
pub(crate) mod executor;
pub mod failure;
pub mod manifest;
pub use super::merge;
pub mod metrics;
pub mod parquet;
pub mod progress;
pub mod resources;
pub mod response;
mod result;
pub mod scheduler;
pub mod sequence_policy;
pub mod session;
pub mod task;
pub mod worker;

pub use config::{
    ProcessingConfig, load_processing_config, validate_integrity_policy,
    validate_processing_config, validate_resources,
};
pub use executor::{execute_processing_job, execute_processing_job_with_progress};
pub use result::{ProcessingErrorCode, ProcessingFailure, ProcessingResult, ProcessingStatus};
pub use sequence_policy::resolve_sequence_policy;

pub mod streams;

mod streams_depth;

pub(crate) mod reconcile;
mod reconciled;

pub(crate) mod family;

/// Render digest bytes without allocating a temporary String for each byte.
fn hex_digest(bytes: impl AsRef<[u8]>) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let bytes = bytes.as_ref();
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        encoded.push(char::from(DIGITS[usize::from(byte >> 4)]));
        encoded.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    encoded
}

pub(crate) mod policies;

mod preparation;

mod coverage;
