use std::{
    fs,
    path::{Path, PathBuf},
};

use crate::shared::error::{MarketForgeError, Result};

use super::models::{DATASET_MANIFEST_FILENAME, DatasetManifest};

// -----------------------------------------------------------------------------
// Dataset manifest persistence
// -----------------------------------------------------------------------------

impl<M: serde::Serialize> DatasetManifest<M> {
    /// Persist the completed dataset manifest.
    ///
    /// The manifest is written only after all processing tasks
    /// have completed and dataset integrity has been evaluated.
    ///
    /// Existing manifests are never overwritten.
    pub fn write_to(&self, dataset_root: &Path) -> Result<PathBuf> {
        let path = dataset_root.join(DATASET_MANIFEST_FILENAME);

        // ---------------------------------------------------------------------
        // Prevent accidental overwrite
        // ---------------------------------------------------------------------

        if path.exists() {
            return Err(MarketForgeError::InvalidConfiguration(format!(
                "dataset manifest already exists: {}",
                path.display(),
            )));
        }

        // ---------------------------------------------------------------------
        // Serialize manifest
        // ---------------------------------------------------------------------

        let file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|error| {
                MarketForgeError::InvalidConfiguration(format!(
                    "failed to create report {}: {error}",
                    path.display()
                ))
            })?;
        let mut writer = std::io::BufWriter::new(file);
        serde_json::to_writer_pretty(&mut writer, self).map_err(|error| {
            MarketForgeError::InvalidConfiguration(format!(
                "failed to persist report {}: {error}",
                path.display()
            ))
        })?;
        std::io::Write::flush(&mut writer).map_err(|error| {
            MarketForgeError::InvalidConfiguration(format!(
                "failed to flush report {}: {error}",
                path.display()
            ))
        })?;

        Ok(path)
    }
}
