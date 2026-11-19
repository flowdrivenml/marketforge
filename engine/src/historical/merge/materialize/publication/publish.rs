use std::collections::HashSet;
use std::{fs, path::Path};

use crate::shared::error::{MarketForgeError, Result};

use {
    crate::historical::merge::materialize::publication::manifest::PhysicalMergeManifest,
    crate::historical::merge::materialize::publication::manifest::write_physical_merge_manifest,
};

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

/// Re-read the physical Parquet with the same validation used during publication.
pub fn validate_committed_merge(root: &Path) -> Result<()> {
    use crate::historical::merge::{MaterializationPartitionResult, validate_physical_dataset};

    let load = |name: &str| {
        fs::read(root.join(name)).map_err(|error| invalid_error(format!("{name}: {error}")))
    };
    let manifest: PhysicalMergeManifest = serde_json::from_slice(&load("manifest.json")?)
        .map_err(|error| invalid_error(error.to_string()))?;
    manifest.validate()?;
    use crate::historical::job::{MergeJob, MergeJobDocument};

    let saved = load("merge-job.json")?;
    let (path, job_id, interval, publications) =
        if let Ok(job) = serde_json::from_slice::<MergeJobDocument>(&saved) {
            job.validate()?;
            let mut seen = HashSet::new();
            let descriptors = job
                .inputs
                .iter()
                .filter_map(|input| {
                    let id = input.publication.engine_dataset_id;
                    seen.insert(id).then_some(&input.publication)
                })
                .collect::<Vec<_>>();
            let input_hashes: Vec<String> =
                serde_json::from_slice(&load("merge-input-checksums.json")?)
                    .map_err(|error| invalid_error(error.to_string()))?;
            if descriptors
                .iter()
                .map(|publication| &publication.manifest_sha256)
                .ne(&input_hashes)
                || descriptors
                    .iter()
                    .map(|publication| publication.manifest_path.parent())
                    .ne(manifest
                        .input_datasets
                        .iter()
                        .map(|input| Some(Path::new(&input.dataset_path))))
            {
                return invalid("committed merge publication paths or input fingerprints changed");
            }
            let publications = descriptors
                .iter()
                .map(|publication| publication.engine_dataset_id)
                .collect();
            (
                job.output.dataset_path,
                job.job_id,
                job.time_range,
                publications,
            )
        } else {
            // Previously committed publications stored the internally resolved legacy job.
            let job: MergeJob =
                serde_json::from_slice(&saved).map_err(|e| invalid_error(e.to_string()))?;
            job.validate()?;
            (
                job.output.dataset_path,
                job.job_id,
                job.time_range,
                job.inputs,
            )
        };
    if path != root
        || job_id != manifest.job_id
        || interval != manifest.time_range
        || publications
            .into_iter()
            .ne(manifest.input_datasets.iter().map(|input| input.dataset_id))
    {
        return invalid("committed merge path, job, interval or input identity mismatch");
    }
    let partitions: Vec<MaterializationPartitionResult> =
        serde_json::from_slice(&load("merge-partitions.json")?)
            .map_err(|error| invalid_error(error.to_string()))?;
    let resolved = validate_physical_dataset(root, manifest.mode, &partitions)?;
    if resolved.files != manifest.files
        || resolved.events != manifest.events_written
        || resolved.trade_events != manifest.trade_events
        || resolved.depth_events != manifest.depth_events
        || resolved.canonical_rows != manifest.canonical_rows
        || resolved.row_groups != manifest.row_groups
        || resolved.first_timestamp_ns != manifest.start_timestamp_ns
        || resolved.last_timestamp_ns != manifest.end_timestamp_ns
    {
        return invalid("committed merge inventory or decoded metrics differ from manifest");
    }
    Ok(())
}
