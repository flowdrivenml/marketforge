use super::ProcessingJob;
use crate::shared::error::Result;
use std::path::Path;
pub fn load_processing_job(path: impl AsRef<Path>) -> Result<ProcessingJob> {
    #[cfg(feature = "historical")]
    {
        crate::historical::config::load_document(path)
    }
    #[cfg(not(feature = "historical"))]
    {
        let path = path.as_ref();
        let text = std::fs::read(path).map_err(|e| {
            crate::shared::error::MarketForgeError::InvalidConfiguration(format!(
                "processing job {}: {e}",
                path.display()
            ))
        })?;
        serde_json::from_slice(&text).map_err(|e| {
            crate::shared::error::MarketForgeError::InvalidConfiguration(format!(
                "processing job {}: {e}",
                path.display()
            ))
        })
    }
}
