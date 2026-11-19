use std::{
    collections::HashSet,
    fs,
    path::{Component, Path, PathBuf},
};

use {
    crate::historical::job::MergeJob, crate::historical::normalize::manifest::AuxiliaryFile,
    crate::historical::normalize::manifest::DATASET_MANIFEST_FILENAME,
    crate::historical::normalize::manifest::DATASET_MANIFEST_VERSION,
    crate::historical::normalize::manifest::DatasetFile,
    crate::historical::normalize::manifest::DatasetManifest,
    crate::shared::error::MarketForgeError, crate::shared::error::Result,
};

// -----------------------------------------------------------------------------
// Resolved dataset
// -----------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct ResolvedMergeInput {
    pub dataset_id: u64,
    pub dataset_path: PathBuf,
    pub manifest: DatasetManifest,
    pub canonical_files: Vec<PathBuf>,
    pub auxiliary_files: Vec<PathBuf>,
}

impl ResolvedMergeInput {
    /// Determine selected stream membership using committed manifest ownership.
    pub fn selected_streams(
        &self,
    ) -> Result<Vec<&crate::historical::normalize::streams::StreamOutput>> {
        let mut seen = HashSet::new();
        let mut result = Vec::new();
        for file in &self.canonical_files {
            if self.manifest.streams.is_empty() {
                break; // Older manifests are interpreted by the committed processing job.
            }
            let mut owners = self.manifest.streams.iter().filter(|stream| {
                file.parent() == Some(self.dataset_path.join(&stream.directory).as_path())
            });
            let owner = owners
                .next()
                .filter(|_| owners.next().is_none())
                .ok_or_else(|| {
                    MarketForgeError::InvalidConfiguration(format!(
                        "publication {}: no unique manifest stream owns {}",
                        self.dataset_id,
                        file.display()
                    ))
                })?;
            if seen.insert(&owner.stream_id) {
                result.push(owner);
            }
        }
        Ok(result)
    }
}

// -----------------------------------------------------------------------------
// Resolve merge inputs
// -----------------------------------------------------------------------------

pub fn resolve_merge_inputs(
    job: &MergeJob,
    project_root: &Path,
) -> Result<Vec<ResolvedMergeInput>> {
    resolve_merge_inputs_with_progress(job, project_root, |_| {})
}

pub(crate) fn resolve_merge_inputs_with_progress(
    job: &MergeJob,
    project_root: &Path,
    mut checked: impl FnMut(usize),
) -> Result<Vec<ResolvedMergeInput>> {
    job.validate()?;

    let processed_root = project_root.join("data/processed");

    let mut verified_files = 0;
    let mut resolved = Vec::with_capacity(job.inputs.len());
    let mut seen = HashSet::new();
    #[cfg(unix)]
    let mut physical_files = HashSet::new();

    for &dataset_id in &job.inputs {
        if !seen.insert(dataset_id) {
            return invalid(format!("duplicate merge input dataset ID: {dataset_id}"));
        }

        let dataset_path = match job.input_paths.get(&dataset_id) {
            Some(path) if path.is_absolute() => path.clone(),

            Some(path) => project_root.join(path),

            None if job.submission.is_some() => {
                return invalid(format!(
                    "publication {dataset_id}: missing pinned manifest path"
                ));
            }
            None => processed_root.join(dataset_id.to_string()),
        };

        if !dataset_path.is_dir() {
            return invalid(format!(
                "merge input dataset {dataset_id} does not exist: {}",
                dataset_path.display()
            ));
        }

        let dataset_path = fs::canonicalize(&dataset_path).map_err(|error| {
            MarketForgeError::InvalidConfiguration(format!(
                "failed to resolve dataset directory {}: {error}",
                dataset_path.display()
            ))
        })?;

        let manifest_path = dataset_path.join(DATASET_MANIFEST_FILENAME);

        if job.submission.is_some()
            && (dataset_path
                != *job
                    .input_paths
                    .get(&dataset_id)
                    .expect("explicit publication")
                || manifest_path.canonicalize().ok().as_ref() != Some(&manifest_path))
        {
            return invalid(format!(
                "publication {dataset_id}: expected a regular committed manifest at {}",
                manifest_path.display()
            ));
        }
        let manifest_bytes = fs::read(&manifest_path).map_err(|error| {
            MarketForgeError::InvalidConfiguration(format!(
                "failed to open dataset manifest {}: {error}",
                manifest_path.display()
            ))
        })?;
        if let Some(selection) = job.primary_files.get(&dataset_id) {
            use sha2::{Digest, Sha256};
            let digest = crate::historical::normalize::hex_digest(Sha256::digest(&manifest_bytes));
            if selection.iter().any(|file| file.manifest_sha256 != digest) {
                return invalid(format!(
                    "publication {dataset_id}: manifest fingerprint changed at {}",
                    manifest_path.display()
                ));
            }
        }
        let manifest: DatasetManifest =
            serde_json::from_slice(&manifest_bytes).map_err(|error| {
                MarketForgeError::InvalidConfiguration(format!(
                    "invalid dataset manifest {}: {error}",
                    manifest_path.display()
                ))
            })?;

        manifest.validate_streams()?;

        // ---------------------------------------------------------------------
        // Manifest identity
        // ---------------------------------------------------------------------

        if !matches!(manifest.protocol_version, 2 | 4 | 5)
            && manifest.protocol_version != DATASET_MANIFEST_VERSION
        {
            return invalid(format!(
                "dataset {dataset_id}: unsupported manifest version {}",
                manifest.protocol_version
            ));
        }

        if manifest.dataset_id != dataset_id {
            return invalid(format!(
                "dataset identity mismatch: requested={dataset_id}, manifest={}",
                manifest.dataset_id
            ));
        }

        // ---------------------------------------------------------------------
        // Timestamp range
        // ---------------------------------------------------------------------

        let (Some(start), Some(end)) = (manifest.start_timestamp_ns, manifest.end_timestamp_ns)
        else {
            return invalid(format!(
                "dataset {dataset_id}: missing timestamp boundaries"
            ));
        };

        if start > end {
            return invalid(format!(
                "dataset {dataset_id}: invalid timestamp boundaries"
            ));
        }

        // Dataset timestamps are inclusive observations.
        // Merge job intervals are half-open: [start_ns, end_ns).
        if end < job.time_range.start_ns || start >= job.time_range.end_ns {
            return invalid(format!(
                "dataset {dataset_id}: no overlap with requested merge interval"
            ));
        }

        // ---------------------------------------------------------------------
        // Canonical file inventory
        // ---------------------------------------------------------------------

        if manifest.files.is_empty() {
            return invalid(format!("dataset {dataset_id}: no canonical files"));
        }

        if manifest.files_written != manifest.files.len() as u64 {
            return invalid(format!(
                "dataset {dataset_id}: canonical file count mismatch"
            ));
        }

        let mut canonical_files = Vec::with_capacity(manifest.files.len());
        let mut auxiliary_files = Vec::with_capacity(manifest.auxiliary_files.len());

        let mut paths = HashSet::new();

        if let Some(selection) = job.primary_files.get(&dataset_id) {
            let descriptors = job
                .submission
                .as_ref()
                .into_iter()
                .flat_map(|job| &job.inputs)
                .filter(|input| input.publication.engine_dataset_id == dataset_id)
                .map(|input| (input.path.as_path(), input.dataset_id))
                .collect::<std::collections::HashMap<_, _>>();
            let entries = manifest
                .files
                .iter()
                .map(|entry| (entry.path.as_str(), entry))
                .collect::<std::collections::HashMap<_, _>>();
            if entries.len() != manifest.files.len() {
                return invalid(format!(
                    "publication {dataset_id}: duplicate primary manifest paths"
                ));
            }
            for item in selection {
                let specified = dataset_path.join(&item.path);
                let label = descriptors.get(specified.as_path()).map_or_else(
                    || "selected file".to_owned(),
                    |id| format!("registered file {id}"),
                );
                let context = format!(
                    "{label} at {} (publication {dataset_id})",
                    specified.display()
                );
                let entry = entries.get(item.path.as_str()).ok_or_else(|| {
                    MarketForgeError::InvalidConfiguration(format!(
                        "{context}: not a primary manifest member"
                    ))
                })?;
                let path = resolve_file(&dataset_path, &entry.path, dataset_id, &mut paths)
                    .map_err(|error| {
                        MarketForgeError::InvalidConfiguration(format!("{context}: {error}"))
                    })?;
                if job.submission.is_some() && path != specified {
                    return invalid(format!("{context}: expected canonical file path"));
                }
                #[cfg(unix)]
                {
                    use std::os::unix::fs::MetadataExt;
                    let metadata = path.metadata().map_err(|error| {
                        MarketForgeError::InvalidConfiguration(format!("{context}: {error}"))
                    })?;
                    if !physical_files.insert((metadata.dev(), metadata.ino())) {
                        return invalid(format!("{context}: duplicate physical file"));
                    }
                }
                validate_canonical_file(dataset_id, entry, &path)?;
                let sha =
                    crate::historical::normalize::manifest::file_sha256(&path).map_err(|e| {
                        MarketForgeError::InvalidConfiguration(format!("{context}: {e}"))
                    })?;
                if sha != item.sha256 {
                    return invalid(format!("{context}: SHA-256 mismatch"));
                }
                canonical_files.push(path);
                verified_files += 1;
                checked(verified_files);
            }
        } else {
            for entry in &manifest.files {
                let path = resolve_file(&dataset_path, &entry.path, dataset_id, &mut paths)?;
                validate_canonical_file(dataset_id, entry, &path)?;
                canonical_files.push(path);
            }
        }

        // ---------------------------------------------------------------------
        // Auxiliary file inventory
        // ---------------------------------------------------------------------

        let authoritative_depth = manifest.content_type
            == crate::historical::job::ContentType::Depth
            && manifest.protocol_version >= 4;
        let checksums = if authoritative_depth {
            let checksums_url = dataset_path.join("publication-checksums.json");
            let contents = fs::read(&checksums_url).map_err(|error| {
                MarketForgeError::InvalidConfiguration(format!(
                    "publication {dataset_id} missing auxiliary checksums: {error}"
                ))
            })?;
            Some(
                serde_json::from_slice::<std::collections::HashMap<String, String>>(&contents)
                    .map_err(|error| {
                        MarketForgeError::InvalidConfiguration(format!(
                            "publication {dataset_id} has invalid auxiliary checksums: {error}"
                        ))
                    })?,
            )
        } else {
            None
        };
        let selected_directories = canonical_files
            .iter()
            .filter_map(|file| file.parent())
            .collect::<HashSet<_>>();
        for entry in &manifest.auxiliary_files {
            if authoritative_depth
                && !selected_directories.contains(
                    dataset_path
                        .join(&entry.path)
                        .parent()
                        .expect("publication auxiliary has a parent"),
                )
            {
                continue;
            }
            let path = resolve_file(&dataset_path, &entry.path, dataset_id, &mut paths)?;

            validate_auxiliary_file(dataset_id, entry, &path)?;
            if let Some(checksums) = &checksums {
                let actual = crate::historical::normalize::manifest::file_sha256(&path).map_err(
                    |error| {
                        MarketForgeError::InvalidConfiguration(format!(
                            "cannot hash {}: {error}",
                            path.display()
                        ))
                    },
                )?;
                if checksums.get(&entry.path) != Some(&actual) {
                    return invalid(format!(
                        "publication {dataset_id} auxiliary SHA-256 mismatch: {}",
                        entry.path
                    ));
                }
            }

            auxiliary_files.push(path);
        }

        resolved.push(ResolvedMergeInput {
            dataset_id,
            dataset_path,
            manifest,
            canonical_files,
            auxiliary_files,
        });
    }

    Ok(resolved)
}

// -----------------------------------------------------------------------------
// Safe file resolution
// -----------------------------------------------------------------------------

fn resolve_file(
    dataset_path: &Path,
    relative_path: &str,
    dataset_id: u64,
    seen: &mut HashSet<PathBuf>,
) -> Result<PathBuf> {
    let relative = Path::new(relative_path);

    if relative.as_os_str().is_empty()
        || relative.is_absolute()
        || relative
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return invalid(format!(
            "dataset {dataset_id}: unsafe manifest path: {relative_path}"
        ));
    }

    let path = dataset_path.join(relative);

    let canonical = fs::canonicalize(&path).map_err(|error| {
        MarketForgeError::InvalidConfiguration(format!(
            "dataset {dataset_id}: cannot resolve {}: {error}",
            path.display()
        ))
    })?;

    if !canonical.starts_with(dataset_path) {
        return invalid(format!(
            "dataset {dataset_id}: file escapes dataset directory: {relative_path}"
        ));
    }

    if !seen.insert(canonical.clone()) {
        return invalid(format!(
            "dataset {dataset_id}: duplicate file reference: {relative_path}"
        ));
    }

    if !canonical.is_file() {
        return invalid(format!(
            "dataset {dataset_id}: expected regular file: {}",
            canonical.display()
        ));
    }

    Ok(canonical)
}

// -----------------------------------------------------------------------------
// File validation
// -----------------------------------------------------------------------------

fn validate_canonical_file(dataset_id: u64, entry: &DatasetFile, path: &Path) -> Result<()> {
    let size = fs::metadata(path)
        .map_err(|error| {
            MarketForgeError::InvalidConfiguration(format!(
                "dataset {dataset_id}: cannot inspect {}: {error}",
                path.display()
            ))
        })?
        .len();

    if size != entry.size_bytes {
        return invalid(format!(
            "dataset {dataset_id}: canonical file size mismatch: {}",
            path.display()
        ));
    }

    if entry.rows == 0 {
        return invalid(format!(
            "dataset {dataset_id}: canonical file has zero rows: {}",
            path.display()
        ));
    }

    if let (Some(start), Some(end)) = (entry.start_timestamp_ns, entry.end_timestamp_ns) {
        if start > end {
            return invalid(format!(
                "dataset {dataset_id}: invalid canonical file timestamp range: {}",
                path.display()
            ));
        }
    }

    Ok(())
}

fn validate_auxiliary_file(dataset_id: u64, entry: &AuxiliaryFile, path: &Path) -> Result<()> {
    let size = fs::metadata(path)
        .map_err(|error| {
            MarketForgeError::InvalidConfiguration(format!(
                "dataset {dataset_id}: cannot inspect {}: {error}",
                path.display()
            ))
        })?
        .len();

    if size != entry.size_bytes {
        return invalid(format!(
            "dataset {dataset_id}: auxiliary file size mismatch: {}",
            path.display()
        ));
    }

    Ok(())
}

// -----------------------------------------------------------------------------
// Errors
// -----------------------------------------------------------------------------

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(MarketForgeError::InvalidConfiguration(message.into()))
}
