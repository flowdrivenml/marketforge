use std::{fs, path::Path};

use crate::error::{MarketForgeError, Result};

use super::materialize_manifest::{PhysicalMergeManifest, write_physical_merge_manifest};

pub fn prepare_physical_publication(
    staging: &Path,
    manifest: &PhysicalMergeManifest,
) -> Result<()> {
    manifest.validate()?;

    let logical = staging.join("logical");
    let physical = staging.join("physical");

    let physical_timeline = physical.join("timeline");
    let published_timeline = staging.join("timeline");

    if !physical_timeline.is_dir() {
        return invalid("physical timeline directory is missing");
    }

    if published_timeline.exists() {
        return invalid("publication timeline already exists");
    }

    if staging.join("manifest.json").exists() {
        return invalid("publication manifest already exists");
    }

    // Move validated physical timeline into publication layout.
    fs::rename(&physical_timeline, &published_timeline).map_err(|error| {
        invalid_error(format!(
            "cannot prepare physical publication timeline: {error}"
        ))
    })?;

    // Write the manifest after the physical timeline is in place.
    write_physical_merge_manifest(staging, manifest)?;

    // Remove intermediate logical references.
    if logical.exists() {
        fs::remove_dir_all(&logical).map_err(|error| {
            invalid_error(format!("cannot remove logical merge staging: {error}"))
        })?;
    }

    // Remove the now-empty physical staging directory.
    if physical.exists() {
        fs::remove_dir_all(&physical).map_err(|error| {
            invalid_error(format!("cannot remove physical merge staging: {error}"))
        })?;
    }

    Ok(())
}

fn invalid_error(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(message.into())
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(invalid_error(message))
}
