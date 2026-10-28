use std::path::Path;

use crate::shared::error::{MarketForgeError, Result};

#[cfg(target_os = "linux")]
pub fn atomic_publish_directory(staging: &Path, final_path: &Path) -> Result<()> {
    use rustix::fs::{CWD, RenameFlags, renameat_with};

    if !staging.is_dir() {
        return invalid("publication staging directory does not exist");
    }

    if staging == final_path {
        return invalid("publication source and destination are identical");
    }

    renameat_with(CWD, staging, CWD, final_path, RenameFlags::NOREPLACE).map_err(|error| {
        MarketForgeError::InvalidConfiguration(format!(
            "atomic conversion publication failed: \
             {} -> {}: {error}",
            staging.display(),
            final_path.display(),
        ))
    })?;

    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub fn atomic_publish_directory(_staging: &Path, _final_path: &Path) -> Result<()> {
    invalid("atomic no-replace publication is currently supported only on Linux")
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(MarketForgeError::InvalidConfiguration(message.into()))
}
