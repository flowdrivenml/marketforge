use std::{fs::File, io::BufReader, path::Path};

use crate::error::{MarketForgeError, Result};

use super::ProcessingJob;

pub fn load_processing_job(path: impl AsRef<Path>) -> Result<ProcessingJob> {
    let path = path.as_ref();

    let file = File::open(path).map_err(|error| {
        MarketForgeError::InvalidConfiguration(format!(
            "failed to open processing job {}: {error}",
            path.display(),
        ))
    })?;

    serde_json::from_reader(BufReader::new(file)).map_err(|error| {
        MarketForgeError::InvalidConfiguration(format!(
            "failed to parse processing job {}: {error}",
            path.display(),
        ))
    })
}
