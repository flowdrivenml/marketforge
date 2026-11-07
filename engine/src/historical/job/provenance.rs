//! Read the immutable interpretation committed alongside normalized data.
use super::ProcessingJob;
use crate::shared::error::{MarketForgeError, Result};
use sha2::{Digest, Sha256};
use std::path::Path;
pub fn load_committed_processing_job(dataset: &Path) -> Result<ProcessingJob> {
    let invalid = |message: String| MarketForgeError::InvalidConfiguration(message);
    let bytes = std::fs::read(dataset.join("processing-job.json"))
        .map_err(|e| invalid(format!("committed job: {e}")))?;
    let hash: String = Sha256::digest(&bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let checksum = std::fs::read_to_string(dataset.join("processing-job.sha256"))
        .map_err(|e| invalid(format!("committed checksum: {e}")))?;
    if checksum != format!("{hash}  processing-job.json\n") {
        return Err(invalid("committed processing job checksum mismatch".into()));
    }
    let job = serde_json::from_slice(&bytes).map_err(|e| invalid(format!("committed job: {e}")))?;
    super::validate_processing_job_structure(&job)?;
    Ok(job)
}
