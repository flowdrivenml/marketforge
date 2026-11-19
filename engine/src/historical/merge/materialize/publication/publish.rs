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
    use crate::historical::job::MergeJob;

    let saved = load("merge-job.json")?;
    let (document, legacy_file_job) = committed_file_job(&saved, &manifest)?;

    let (path, job_id, interval, publications) = if let Some(job) = document {
        job.validate()?;
        use crate::historical::job::ContentType;
        use crate::historical::merge::UnifiedMergeStreamKind;
        let expected = job
            .inputs
            .iter()
            .map(|file| {
                (
                    file.publication.engine_dataset_id,
                    file.stream_rank,
                    file.stream_id.as_str(),
                    file.content_type == ContentType::Trades,
                )
            })
            .collect::<HashSet<_>>();
        let actual = manifest
            .streams
            .iter()
            .map(|stream| {
                (
                    stream.dataset_id,
                    stream.stream_rank,
                    stream.stream_id.as_str(),
                    stream.kind == UnifiedMergeStreamKind::Trade,
                )
            })
            .collect::<HashSet<_>>();
        if expected != actual {
            return invalid("committed merge stream metadata differs from the submitted job");
        }
        if !legacy_file_job {
            let inventory: std::collections::HashMap<String, String> =
                serde_json::from_slice(&load("publication-checksums.json")?)
                    .map_err(|error| invalid_error(error.to_string()))?;
            let runtime = "merge-runtime.json";
            let checksum = crate::historical::normalize::manifest::file_sha256(&root.join(runtime))
                .map_err(|error| invalid_error(format!("{runtime}: {error}")))?;
            if inventory.get(runtime) != Some(&checksum) {
                return invalid("committed merge runtime checksum mismatch");
            }
        }
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

// Old file-based publications recorded one stream per publication. Verify them
// using their committed stream inventory; the submission loader stays strict.
fn committed_file_job(
    saved: &[u8],
    manifest: &PhysicalMergeManifest,
) -> Result<(Option<crate::historical::job::MergeJobDocument>, bool)> {
    use crate::historical::merge::UnifiedMergeStreamKind;
    use serde_json::Value;
    let mut job: Value =
        serde_json::from_slice(saved).map_err(|error| invalid_error(error.to_string()))?;
    let Some(files) = job.get_mut("inputs").and_then(Value::as_array_mut) else {
        return Ok((None, false));
    };
    if files.is_empty() || !files.iter().all(Value::is_object) {
        return Ok((None, false));
    }
    let legacy = files.iter().all(|file| {
        ["stream_id", "stream_rank", "content_type"]
            .iter()
            .all(|field| file.get(field).is_none())
    });
    if legacy {
        for file in files {
            let publication = file
                .pointer("/publication/engine_dataset_id")
                .and_then(Value::as_str);
            let id = publication.and_then(|text| text.parse::<u64>().ok());
            let mut matches = manifest
                .streams
                .iter()
                .filter(|stream| Some(stream.dataset_id) == id);
            let stream = matches
                .next()
                .filter(|_| matches.next().is_none())
                .ok_or_else(|| {
                    invalid_error("legacy merge output has no unique committed stream")
                })?;
            file["stream_id"] = Value::from(stream.stream_id.clone());
            file["stream_rank"] = Value::from(stream.stream_rank);
            file["content_type"] = Value::from(if stream.kind == UnifiedMergeStreamKind::Trade {
                "trades"
            } else {
                "depth"
            });
        }
    }
    let job = serde_json::from_value(job).map_err(|error| invalid_error(error.to_string()))?;
    Ok((Some(job), legacy))
}
