use std::{
    fs,
    path::{Path, PathBuf},
};

use crate::shared::error::{MarketForgeError, Result};

use super::models::{DATASET_MANIFEST_FILENAME, DatasetManifest};

// -----------------------------------------------------------------------------
// Dataset manifest persistence
// -----------------------------------------------------------------------------

impl DatasetManifest {
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

        let contents = serde_json::to_vec_pretty(self).map_err(|error| {
            MarketForgeError::InvalidConfiguration(format!(
                "failed to serialize dataset manifest: {error}",
            ))
        })?;

        // ---------------------------------------------------------------------
        // Write manifest
        // ---------------------------------------------------------------------

        fs::write(&path, contents).map_err(|error| {
            MarketForgeError::InvalidConfiguration(format!(
                "failed to write dataset manifest {}: {error}",
                path.display(),
            ))
        })?;

        Ok(path)
    }
}
