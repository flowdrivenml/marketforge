use std::{fs::File, io::BufReader, path::Path};

use crate::shared::error::{MarketForgeError, Result};

use super::merge::MergeJob;

pub fn load_merge_job(path: impl AsRef<Path>) -> Result<MergeJob> {
    let path = path.as_ref();

    let file = File::open(path).map_err(|error| {
        MarketForgeError::InvalidConfiguration(format!(
            "failed to open merge job {}: {error}",
            path.display()
        ))
    })?;

    let job: MergeJob = serde_json::from_reader(BufReader::new(file)).map_err(|error| {
        MarketForgeError::InvalidConfiguration(format!(
            "failed to parse merge job {}: {error}",
            path.display()
        ))
    })?;

    job.validate()?;

    Ok(job)
}
