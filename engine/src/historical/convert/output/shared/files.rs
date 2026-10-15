use std::{
    fs,
    path::{Path, PathBuf},
};

use crate::error::{MarketForgeError, Result};

pub fn discover_conversion_files(directory: &Path) -> Result<Vec<PathBuf>> {
    if !directory.is_dir() {
        return invalid(format!(
            "conversion directory does not exist: {}",
            directory.display()
        ));
    }

    let mut files = Vec::new();

    discover_recursive(directory, &mut files)?;

    files.sort();

    if files.is_empty() {
        return invalid("conversion dataset contains no Parquet files");
    }

    Ok(files)
}

fn discover_recursive(directory: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    let entries = fs::read_dir(directory).map_err(|error| {
        invalid_error(format!(
            "cannot read conversion directory {}: {error}",
            directory.display()
        ))
    })?;

    for entry in entries {
        let entry = entry.map_err(|error| invalid_error(error.to_string()))?;

        let path = entry.path();

        let metadata =
            fs::symlink_metadata(&path).map_err(|error| invalid_error(error.to_string()))?;

        // Never follow symbolic links.
        if metadata.file_type().is_symlink() {
            return invalid(format!(
                "symbolic links are not permitted: {}",
                path.display()
            ));
        }

        if metadata.is_dir() {
            discover_recursive(&path, files)?;
        } else if metadata.is_file() && path.extension().is_some_and(|ext| ext == "parquet") {
            files.push(path);
        }
    }

    Ok(())
}

fn invalid_error(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(message.into())
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(invalid_error(message))
}
