use std::{
    fs,
    path::{Path, PathBuf},
};

use crate::shared::error::{MarketForgeError, Result};

use super::{
    inspection::inspect_parquet_file,
    models::{AuxiliaryFile, AuxiliaryFileKind, DatasetFile},
};

pub fn inspect_parquet_files(dataset_root: &Path) -> Result<Vec<DatasetFile>> {
    validate_artifact_tree(dataset_root)?;
    let mut paths = Vec::<PathBuf>::new();

    collect_canonical_parts(&dataset_root.join("trades"), &mut paths)?;

    let tasks_dir = dataset_root.join("tasks");

    if tasks_dir.is_dir() {
        for task_dir in sorted_task_directories(&tasks_dir)? {
            collect_canonical_parts(&task_dir.join("trades"), &mut paths)?;
            collect_canonical_parts(&task_dir.join("depth"), &mut paths)?;
        }
    }

    let streams_dir = dataset_root.join("streams");
    if streams_dir.is_dir() {
        for stream in sorted_task_directories(&streams_dir)? {
            collect_canonical_parts(&stream.join("trades"), &mut paths)?;
            collect_canonical_parts(&stream.join("depth"), &mut paths)?;
        }
    }

    paths.sort();

    if paths.is_empty() {
        return Err(MarketForgeError::InvalidConfiguration(
            "dataset contains no canonical Parquet files".to_owned(),
        ));
    }

    paths
        .iter()
        .map(|path| inspect_parquet_file(dataset_root, path))
        .collect()
}

pub fn inspect_auxiliary_files(dataset_root: &Path) -> Result<Vec<AuxiliaryFile>> {
    validate_artifact_tree(dataset_root)?;
    let mut files = Vec::new();

    let tasks_dir = dataset_root.join("tasks");

    let mut directories = Vec::new();
    for root in [&tasks_dir, &dataset_root.join("streams")] {
        if root.is_dir() {
            directories.extend(sorted_task_directories(root)?);
        }
    }
    for task_dir in directories {
        let provenance = task_dir.join("trades/sources.jsonl");
        if provenance.is_file() {
            files.push(AuxiliaryFile {
                path: provenance
                    .strip_prefix(dataset_root)
                    .map_err(|e| MarketForgeError::InvalidConfiguration(e.to_string()))?
                    .to_string_lossy()
                    .replace('\\', "/"),
                kind: AuxiliaryFileKind::TradeSourcePositions,
                size_bytes: std::fs::metadata(&provenance)
                    .map_err(|e| MarketForgeError::InvalidConfiguration(e.to_string()))?
                    .len(),
            });
        }
        let depth_dir = task_dir.join("depth");

        if !depth_dir.is_dir() {
            continue;
        }

        for (filename, kind) in [
            ("events.parquet", AuxiliaryFileKind::DepthEvents),
            ("segments.json", AuxiliaryFileKind::DepthSegments),
            ("boundary.json", AuxiliaryFileKind::DepthBoundary),
        ] {
            let path = depth_dir.join(filename);

            if !path.is_file() {
                return Err(MarketForgeError::InvalidConfiguration(format!(
                    "depth task missing required auxiliary file: {}",
                    path.display(),
                )));
            }

            let metadata = fs::metadata(&path).map_err(|error| {
                MarketForgeError::InvalidConfiguration(format!(
                    "failed to inspect auxiliary file {}: {error}",
                    path.display(),
                ))
            })?;

            let relative_path = path
                .strip_prefix(dataset_root)
                .map_err(|error| MarketForgeError::InvalidConfiguration(error.to_string()))?
                .to_string_lossy()
                .replace('\\', "/");

            files.push(AuxiliaryFile {
                path: relative_path,
                kind,
                size_bytes: metadata.len(),
            });
        }
    }

    files.sort_by(|a, b| a.path.cmp(&b.path));

    Ok(files)
}

fn collect_canonical_parts(directory: &Path, paths: &mut Vec<PathBuf>) -> Result<()> {
    if !directory.is_dir() {
        return Ok(());
    }

    for entry in fs::read_dir(directory).map_err(|error| {
        MarketForgeError::InvalidConfiguration(format!(
            "failed to read canonical directory {}: {error}",
            directory.display(),
        ))
    })? {
        let path = entry
            .map_err(|error| MarketForgeError::InvalidConfiguration(error.to_string()))?
            .path();

        let is_canonical = path.is_file()
            && path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("part-") && name.ends_with(".parquet"));

        if is_canonical {
            paths.push(path);
        }
    }

    Ok(())
}

fn sorted_task_directories(tasks_dir: &Path) -> Result<Vec<PathBuf>> {
    let mut directories = fs::read_dir(tasks_dir)
        .map_err(|error| {
            MarketForgeError::InvalidConfiguration(format!(
                "failed to read task directory {}: {error}",
                tasks_dir.display(),
            ))
        })?
        .map(|entry| {
            entry
                .map(|entry| entry.path())
                .map_err(|error| MarketForgeError::InvalidConfiguration(error.to_string()))
        })
        .collect::<Result<Vec<_>>>()?;

    directories.sort();
    directories.retain(|path| path.is_dir());

    Ok(directories)
}

// Reject symlinks before discovery: lexical relative paths alone do not prove
// that an artifact remains inside the assigned dataset.
fn validate_artifact_tree(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path).map_err(|e| {
        MarketForgeError::InvalidConfiguration(format!("inspect artifact {}: {e}", path.display()))
    })?;
    if metadata.file_type().is_symlink() || (!metadata.is_dir() && !metadata.is_file()) {
        return Err(MarketForgeError::InvalidConfiguration(format!(
            "artifact must be a regular file/directory, never a symlink: {}",
            path.display()
        )));
    }
    if metadata.is_dir() {
        for entry in
            fs::read_dir(path).map_err(|e| MarketForgeError::InvalidConfiguration(e.to_string()))?
        {
            validate_artifact_tree(
                &entry
                    .map_err(|e| MarketForgeError::InvalidConfiguration(e.to_string()))?
                    .path(),
            )?;
        }
    }
    Ok(())
}
