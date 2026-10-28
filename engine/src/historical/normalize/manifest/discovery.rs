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
    let mut paths = Vec::<PathBuf>::new();

    collect_canonical_parts(&dataset_root.join("trades"), &mut paths)?;

    let tasks_dir = dataset_root.join("tasks");

    if tasks_dir.is_dir() {
        for task_dir in sorted_task_directories(&tasks_dir)? {
            collect_canonical_parts(&task_dir.join("trades"), &mut paths)?;
            collect_canonical_parts(&task_dir.join("depth"), &mut paths)?;
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
    let mut files = Vec::new();

    let tasks_dir = dataset_root.join("tasks");

    if !tasks_dir.is_dir() {
        return Ok(files);
    }

    for task_dir in sorted_task_directories(&tasks_dir)? {
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
