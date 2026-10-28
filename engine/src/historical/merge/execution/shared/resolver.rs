use std::{
    collections::HashSet,
    fs::{self, File},
    io::BufReader,
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

// -----------------------------------------------------------------------------
// Resolve merge inputs
// -----------------------------------------------------------------------------

pub fn resolve_merge_inputs(
    job: &MergeJob,
    project_root: &Path,
) -> Result<Vec<ResolvedMergeInput>> {
    job.validate()?;

    let processed_root = project_root.join("data/processed");

    let mut resolved = Vec::with_capacity(job.inputs.len());
    let mut seen = HashSet::new();

    for &dataset_id in &job.inputs {
        if !seen.insert(dataset_id) {
            return invalid(format!("duplicate merge input dataset ID: {dataset_id}"));
        }

        let dataset_path = match job.input_paths.get(&dataset_id) {
            Some(path) if path.is_absolute() => path.clone(),

            Some(path) => project_root.join(path),

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

        let file = File::open(&manifest_path).map_err(|error| {
            MarketForgeError::InvalidConfiguration(format!(
                "failed to open dataset manifest {}: {error}",
                manifest_path.display()
            ))
        })?;

        let manifest: DatasetManifest =
            serde_json::from_reader(BufReader::new(file)).map_err(|error| {
                MarketForgeError::InvalidConfiguration(format!(
                    "invalid dataset manifest {}: {error}",
                    manifest_path.display()
                ))
            })?;

        // ---------------------------------------------------------------------
        // Manifest identity
        // ---------------------------------------------------------------------

        if manifest.protocol_version != DATASET_MANIFEST_VERSION {
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

        for entry in &manifest.files {
            let path = resolve_file(&dataset_path, &entry.path, dataset_id, &mut paths)?;

            validate_canonical_file(dataset_id, entry, &path)?;

            canonical_files.push(path);
        }

        // ---------------------------------------------------------------------
        // Auxiliary file inventory
        // ---------------------------------------------------------------------

        for entry in &manifest.auxiliary_files {
            let path = resolve_file(&dataset_path, &entry.path, dataset_id, &mut paths)?;

            validate_auxiliary_file(dataset_id, entry, &path)?;

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
