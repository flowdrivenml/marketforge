use std::{
    collections::{BTreeMap, HashSet},
    path::{Path, PathBuf},
};

use crate::{
    historical::job::ContentType,
    shared::error::{MarketForgeError, Result},
};

use crate::historical::merge::execution::shared::resolver::ResolvedMergeInput;

// -----------------------------------------------------------------------------
// Trade file inventory
// -----------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct TradeFileInventory {
    pub path: PathBuf,

    pub part_index: u64,

    // Task-local canonical row range.
    pub row_offset: u64,
    pub rows: u64,

    pub start_timestamp_ns: i64,
    pub end_timestamp_ns: i64,
}

// -----------------------------------------------------------------------------
// Trade task inventory
// -----------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct TradeTaskInventory {
    pub dataset_id: u64,
    pub task_id: u64,

    pub files: Vec<TradeFileInventory>,

    pub canonical_rows: u64,

    pub start_timestamp_ns: i64,
    pub end_timestamp_ns: i64,
}

// -----------------------------------------------------------------------------
// Build trade inventory
// -----------------------------------------------------------------------------

pub fn build_trade_inventory(inputs: &[ResolvedMergeInput]) -> Result<Vec<TradeTaskInventory>> {
    let mut inventory = Vec::new();

    for input in inputs {
        let mut dataset_rows = 0_u64;
        if input.manifest.content_type != ContentType::Trades {
            return invalid(format!(
                "dataset {} is not a trade dataset",
                input.dataset_id
            ));
        }

        let mut tasks: BTreeMap<u64, Vec<(u64, PathBuf, u64, i64, i64)>> = BTreeMap::new();

        // ---------------------------------------------------------------------
        // Collect canonical files
        // ---------------------------------------------------------------------

        for entry in &input.manifest.files {
            let (task_id, part_index) = parse_trade_path(&entry.path)?;

            let start = entry.start_timestamp_ns.ok_or_else(|| {
                invalid_error(format!(
                    "dataset {}: missing file start timestamp",
                    input.dataset_id
                ))
            })?;

            let end = entry.end_timestamp_ns.ok_or_else(|| {
                invalid_error(format!(
                    "dataset {}: missing file end timestamp",
                    input.dataset_id
                ))
            })?;

            if start > end {
                return invalid(format!(
                    "dataset {}: invalid trade file timestamp range",
                    input.dataset_id
                ));
            }

            if entry.rows == 0 {
                return invalid(format!("dataset {}: empty trade file", input.dataset_id));
            }

            tasks.entry(task_id).or_default().push((
                part_index,
                input.dataset_path.join(&entry.path),
                entry.rows,
                start,
                end,
            ));
        }

        // ---------------------------------------------------------------------
        // Validate source-task identities
        // ---------------------------------------------------------------------

        let expected: HashSet<u64> = input.manifest.source_tasks.iter().copied().collect();

        let actual: HashSet<u64> = tasks.keys().copied().collect();

        // Every materialized task must belong to the source manifest.
        // Tasks producing zero canonical rows may have no Parquet files.
        if !actual.is_subset(&expected) {
            let unknown: Vec<u64> = actual.difference(&expected).copied().collect();

            return invalid(format!(
                "dataset {}: canonical files reference unknown source tasks: {:?}",
                input.dataset_id, unknown
            ));
        }

        // ---------------------------------------------------------------------
        // Build task inventories
        // ---------------------------------------------------------------------

        for (task_id, mut entries) in tasks {
            entries.sort_by_key(|entry| entry.0);

            let mut files = Vec::with_capacity(entries.len());

            let mut row_offset = 0_u64;
            let mut previous_end = None;
            let mut previous_part_index = None;

            for entry in &entries {
                if previous_part_index == Some(entry.0) {
                    return invalid(format!(
                        "dataset {} task {}: duplicate trade part index {}",
                        input.dataset_id, task_id, entry.0,
                    ));
                }

                previous_part_index = Some(entry.0);
            }

            for (part_index, path, rows, start, end) in entries {
                if let Some(previous) = previous_end {
                    if start < previous {
                        return invalid(format!(
                            "dataset {} task {}: timestamp regression across files",
                            input.dataset_id, task_id
                        ));
                    }
                }

                files.push(TradeFileInventory {
                    path,
                    part_index,

                    row_offset,
                    rows,

                    start_timestamp_ns: start,
                    end_timestamp_ns: end,
                });

                row_offset = row_offset
                    .checked_add(rows)
                    .ok_or_else(|| invalid_error("trade canonical row offset overflow"))?;

                previous_end = Some(end);
            }

            let first = files
                .first()
                .ok_or_else(|| invalid_error("trade task contains no files"))?;

            let last = files
                .last()
                .ok_or_else(|| invalid_error("trade task contains no files"))?;
            dataset_rows = dataset_rows
                .checked_add(row_offset)
                .ok_or_else(|| invalid_error("trade dataset row count overflow"))?;
            inventory.push(TradeTaskInventory {
                dataset_id: input.dataset_id,
                task_id,

                canonical_rows: row_offset,

                start_timestamp_ns: first.start_timestamp_ns,
                end_timestamp_ns: last.end_timestamp_ns,

                files,
            });
        }
        if dataset_rows != input.manifest.events_written {
            return invalid(format!(
                "dataset {}: canonical trade row mismatch: manifest={}, inventory={}",
                input.dataset_id, input.manifest.events_written, dataset_rows,
            ));
        }
    }

    inventory.sort_by_key(|task| (task.dataset_id, task.task_id));

    Ok(inventory)
}

// -----------------------------------------------------------------------------
// Parse canonical trade file path
// -----------------------------------------------------------------------------

fn parse_trade_path(path: &str) -> Result<(u64, u64)> {
    let components = Path::new(path)
        .components()
        .map(|component| component.as_os_str().to_string_lossy().into_owned())
        .collect::<Vec<_>>();

    if components.len() != 4 || components[0] != "tasks" || components[2] != "trades" {
        return invalid(format!("invalid canonical trade path: {path}"));
    }

    let task_id = components[1]
        .strip_prefix("task-")
        .ok_or_else(|| invalid_error("invalid trade task directory"))?
        .parse::<u64>()
        .map_err(|_| invalid_error("invalid trade task ID"))?;

    let part_index = components[3]
        .strip_prefix("part-")
        .and_then(|value| value.strip_suffix(".parquet"))
        .ok_or_else(|| invalid_error("invalid trade part filename"))?
        .parse::<u64>()
        .map_err(|_| invalid_error("invalid trade part index"))?;

    Ok((task_id, part_index))
}

// -----------------------------------------------------------------------------
// Errors
// -----------------------------------------------------------------------------

fn invalid_error(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(message.into())
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(invalid_error(message))
}
