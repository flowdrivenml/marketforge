use std::path::{Path, PathBuf};

use crate::{
    error::{MarketForgeError, Result},
    process::executor::{commit_dataset, prepare_staging_path},
};

// -----------------------------------------------------------------------------
// Merge staging
// -----------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct MergeCommitPaths {
    pub staging_path: PathBuf,
    pub dataset_path: PathBuf,
}

impl MergeCommitPaths {
    pub fn new(dataset_path: &Path, job_id: u64) -> Result<Self> {
        if !dataset_path.is_absolute() {
            return invalid("merge dataset path must be absolute");
        }

        let parent = dataset_path.parent().ok_or_else(|| {
            MarketForgeError::InvalidConfiguration(
                "merge dataset path has no parent directory".to_owned(),
            )
        })?;

        let name = dataset_path.file_name().ok_or_else(|| {
            MarketForgeError::InvalidConfiguration("merge dataset path has no filename".to_owned())
        })?;

        let staging_path = parent.join(format!(".{}.staging-{job_id}", name.to_string_lossy(),));

        Ok(Self {
            staging_path,
            dataset_path: dataset_path.to_path_buf(),
        })
    }

    pub fn prepare(&self) -> Result<()> {
        if self.dataset_path.exists() {
            return invalid(format!(
                "merged dataset already exists: {}",
                self.dataset_path.display(),
            ));
        }

        prepare_staging_path(&self.staging_path)
    }

    pub fn commit(&self) -> Result<()> {
        commit_dataset(&self.staging_path, &self.dataset_path)
    }
}

// -----------------------------------------------------------------------------
// Errors
// -----------------------------------------------------------------------------

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(MarketForgeError::InvalidConfiguration(message.into()))
}
