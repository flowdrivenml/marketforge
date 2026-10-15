use std::{
    fs,
    path::{Path, PathBuf},
};

use crate::error::{MarketForgeError, Result};

#[derive(Debug, Clone)]
pub struct ScratchWorkspace {
    root: PathBuf,
    budget_bytes: u64,
}

impl ScratchWorkspace {
    pub fn new(root: impl AsRef<Path>, budget_bytes: u64) -> Result<Self> {
        if budget_bytes == 0 {
            return Err(MarketForgeError::InvalidConfiguration(
                "scratch budget must be greater than zero".to_owned(),
            ));
        }

        let root = root.as_ref().to_path_buf();

        fs::create_dir_all(&root).map_err(|error| {
            MarketForgeError::InvalidConfiguration(format!(
                "failed to create scratch workspace {}: {error}",
                root.display()
            ))
        })?;

        Ok(Self { root, budget_bytes })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn budget_bytes(&self) -> u64 {
        self.budget_bytes
    }

    pub fn used_bytes(&self) -> Result<u64> {
        directory_size(&self.root)
    }

    pub fn available_bytes(&self) -> Result<u64> {
        Ok(self.budget_bytes.saturating_sub(self.used_bytes()?))
    }

    pub fn ensure_capacity(&self, additional_bytes: u64) -> Result<()> {
        let used = self.used_bytes()?;

        let required = used.checked_add(additional_bytes).ok_or_else(|| {
            MarketForgeError::InvalidConfiguration("scratch usage overflow".to_owned())
        })?;

        if required > self.budget_bytes {
            return Err(MarketForgeError::InvalidConfiguration(format!(
                "scratch budget exceeded: used={}, requested={}, budget={}",
                used, additional_bytes, self.budget_bytes,
            )));
        }

        Ok(())
    }
}

fn directory_size(path: &Path) -> Result<u64> {
    let mut total = 0u64;

    for entry in fs::read_dir(path).map_err(|error| {
        MarketForgeError::InvalidConfiguration(format!(
            "failed to inspect scratch directory {}: {error}",
            path.display()
        ))
    })? {
        let entry =
            entry.map_err(|error| MarketForgeError::InvalidConfiguration(error.to_string()))?;

        let metadata = entry
            .metadata()
            .map_err(|error| MarketForgeError::InvalidConfiguration(error.to_string()))?;

        let size = if metadata.is_dir() {
            directory_size(&entry.path())?
        } else {
            metadata.len()
        };

        total = total.checked_add(size).ok_or_else(|| {
            MarketForgeError::InvalidConfiguration("scratch directory size overflow".to_owned())
        })?;
    }

    Ok(total)
}
