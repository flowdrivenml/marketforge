use std::{
    collections::{BTreeMap, HashSet},
    fs::File,
    io::BufReader,
    path::PathBuf,
};

use crate::{
    error::{MarketForgeError, Result},
    process::{
        boundary::DepthBoundaryManifest,
        parquet::{DepthSegmentManifest, read_depth_segments},
    },
};

use super::resolver::ResolvedMergeInput;

// -----------------------------------------------------------------------------
// Depth task inventory
// -----------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct DepthTaskInventory {
    pub dataset_id: u64,
    pub task_id: u64,

    pub canonical_files: Vec<PathBuf>,

    pub events_path: PathBuf,
    pub segments_path: PathBuf,
    pub boundary_path: PathBuf,

    pub canonical_rows: u64,

    pub segments: DepthSegmentManifest,
    pub boundary: DepthBoundaryManifest,
}

// -----------------------------------------------------------------------------
// Inventory builder
// -----------------------------------------------------------------------------

pub fn build_depth_inventory(inputs: &[ResolvedMergeInput]) -> Result<Vec<DepthTaskInventory>> {
    let mut inventory = Vec::new();

    for input in inputs {
        let mut tasks: BTreeMap<u64, TaskFiles> = BTreeMap::new();

        // ---------------------------------------------------------------------
        // Canonical files
        // ---------------------------------------------------------------------

        for entry in &input.manifest.files {
            let (task_id, filename) = parse_task_path(&entry.path)?;

            let task = tasks.entry(task_id).or_default();

            if !filename.starts_with("part-") || !filename.ends_with(".parquet") {
                return invalid(format!(
                    "dataset {}: unexpected canonical filename: {}",
                    input.dataset_id, filename
                ));
            }

            task.canonical_files.push((
                filename.to_owned(),
                input.dataset_path.join(&entry.path),
                entry.rows,
            ));
        }

        // ---------------------------------------------------------------------
        // Auxiliary files
        // ---------------------------------------------------------------------

        for entry in &input.manifest.auxiliary_files {
            let (task_id, filename) = parse_task_path(&entry.path)?;

            let task = tasks.entry(task_id).or_default();

            match filename {
                "events.parquet" => {
                    set_once(&mut task.events_path, input.dataset_path.join(&entry.path))?;
                }

                "segments.json" => {
                    set_once(
                        &mut task.segments_path,
                        input.dataset_path.join(&entry.path),
                    )?;
                }

                "boundary.json" => {
                    set_once(
                        &mut task.boundary_path,
                        input.dataset_path.join(&entry.path),
                    )?;
                }

                _ => {
                    return invalid(format!(
                        "dataset {}: unexpected auxiliary file: {}",
                        input.dataset_id, filename
                    ));
                }
            }
        }

        // ---------------------------------------------------------------------
        // Validate task inventory
        // ---------------------------------------------------------------------

        let expected_tasks: HashSet<u64> = input.manifest.source_tasks.iter().copied().collect();

        let actual_tasks: HashSet<u64> = tasks.keys().copied().collect();

        if expected_tasks != actual_tasks {
            return invalid(format!(
                "dataset {}: source-task inventory mismatch",
                input.dataset_id
            ));
        }

        for (task_id, mut files) in tasks {
            files.canonical_files.sort_by(|a, b| a.0.cmp(&b.0));

            let canonical_rows = files
                .canonical_files
                .iter()
                .try_fold(0_u64, |total, entry| {
                    total.checked_add(entry.2).ok_or_else(|| {
                        MarketForgeError::InvalidConfiguration(
                            "canonical row count overflow".to_owned(),
                        )
                    })
                })?;

            let events_path = required(
                files.events_path,
                input.dataset_id,
                task_id,
                "events.parquet",
            )?;

            let segments_path = required(
                files.segments_path,
                input.dataset_id,
                task_id,
                "segments.json",
            )?;

            let boundary_path = required(
                files.boundary_path,
                input.dataset_id,
                task_id,
                "boundary.json",
            )?;

            // -----------------------------------------------------------------
            // Read reconstruction segments
            // -----------------------------------------------------------------

            let depth_directory = segments_path.parent().ok_or_else(|| {
                MarketForgeError::InvalidConfiguration("invalid depth segment directory".to_owned())
            })?;

            let segments = read_depth_segments(depth_directory)?;

            if segments.total_levels != canonical_rows {
                return invalid(format!(
                    "dataset {} task {}: segment level count mismatch: segments={}, canonical={}",
                    input.dataset_id, task_id, segments.total_levels, canonical_rows,
                ));
            }

            // -----------------------------------------------------------------
            // Read boundary metadata
            // -----------------------------------------------------------------

            let file = File::open(&boundary_path).map_err(|error| {
                MarketForgeError::InvalidConfiguration(format!(
                    "failed to open {}: {error}",
                    boundary_path.display()
                ))
            })?;

            let boundary: DepthBoundaryManifest = serde_json::from_reader(BufReader::new(file))
                .map_err(|error| {
                    MarketForgeError::InvalidConfiguration(format!(
                        "invalid depth boundary manifest {}: {error}",
                        boundary_path.display()
                    ))
                })?;

            // -----------------------------------------------------------------
            // Store task inventory
            // -----------------------------------------------------------------

            inventory.push(DepthTaskInventory {
                dataset_id: input.dataset_id,
                task_id,

                canonical_files: files
                    .canonical_files
                    .into_iter()
                    .map(|(_, path, _)| path)
                    .collect(),

                events_path,
                segments_path,
                boundary_path,

                canonical_rows,
                segments,
                boundary,
            });
        }
    }

    Ok(inventory)
}

// -----------------------------------------------------------------------------
// Internal task files
// -----------------------------------------------------------------------------

#[derive(Default)]
struct TaskFiles {
    canonical_files: Vec<(String, PathBuf, u64)>,

    events_path: Option<PathBuf>,
    segments_path: Option<PathBuf>,
    boundary_path: Option<PathBuf>,
}

// -----------------------------------------------------------------------------
// Task path parsing
// -----------------------------------------------------------------------------

fn parse_task_path(path: &str) -> Result<(u64, &str)> {
    let parts: Vec<&str> = path.split('/').collect();

    if parts.len() != 4 || parts[0] != "tasks" || parts[2] != "depth" {
        return invalid(format!("invalid depth task path: {path}"));
    }

    let task_id = parts[1]
        .strip_prefix("task-")
        .ok_or_else(|| {
            MarketForgeError::InvalidConfiguration(format!("invalid task directory: {}", parts[1]))
        })?
        .parse::<u64>()
        .map_err(|_| {
            MarketForgeError::InvalidConfiguration(format!("invalid task ID in path: {path}"))
        })?;

    Ok((task_id, parts[3]))
}

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

fn set_once(slot: &mut Option<PathBuf>, value: PathBuf) -> Result<()> {
    if slot.is_some() {
        return invalid("duplicate depth auxiliary file");
    }

    *slot = Some(value);

    Ok(())
}

fn required(
    path: Option<PathBuf>,
    dataset_id: u64,
    task_id: u64,
    filename: &str,
) -> Result<PathBuf> {
    path.ok_or_else(|| {
        MarketForgeError::InvalidConfiguration(format!(
            "dataset {dataset_id} task {task_id}: missing {filename}"
        ))
    })
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(MarketForgeError::InvalidConfiguration(message.into()))
}
