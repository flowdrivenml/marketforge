use std::{fs::File, io::BufReader, path::Path};

use crate::shared::error::{MarketForgeError, Result};

use super::convert::ConvertJob;

pub fn load_convert_job(path: impl AsRef<Path>) -> Result<ConvertJob> {
    let path = path.as_ref();

    let file = File::open(path).map_err(|error| {
        MarketForgeError::InvalidConfiguration(format!(
            "failed to open conversion job {}: {error}",
            path.display()
        ))
    })?;

    let job: ConvertJob = serde_json::from_reader(BufReader::new(file)).map_err(|error| {
        MarketForgeError::InvalidConfiguration(format!(
            "failed to parse conversion job {}: {error}",
            path.display()
        ))
    })?;

    job.validate()?;

    Ok(job)
}
