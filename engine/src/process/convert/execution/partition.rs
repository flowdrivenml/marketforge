use std::{fs::File, path::Path};

use parquet::file::reader::{FileReader, SerializedFileReader};

use crate::{
    error::{MarketForgeError, Result},
    process::merge::PhysicalMergeManifest,
};

// -----------------------------------------------------------------------------
// Physical row-group descriptor
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConversionRowGroup {
    pub file_index: usize,
    pub row_group_index: usize,

    pub global_start_row: u64,
    pub rows: u64,

    pub compressed_bytes: u64,
}

// -----------------------------------------------------------------------------
// Conversion partition
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConversionPartition {
    pub partition_id: u32,

    pub first_row_group: usize,
    pub end_row_group: usize,

    pub start_row: u64,
    pub end_row: u64,

    pub rows: u64,
}

// -----------------------------------------------------------------------------
// Partition plan
// -----------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct ConversionPartitionPlan {
    pub row_groups: Vec<ConversionRowGroup>,
    pub partitions: Vec<ConversionPartition>,

    pub total_rows: u64,
    pub total_compressed_bytes: u64,
}

// -----------------------------------------------------------------------------
// Build physical row-group index
// -----------------------------------------------------------------------------

pub fn build_conversion_partition_plan(
    dataset_path: &Path,
    manifest: &PhysicalMergeManifest,
    requested_workers: usize,
) -> Result<ConversionPartitionPlan> {
    if requested_workers == 0 {
        return invalid("conversion worker count must be positive");
    }

    manifest.validate()?;

    let mut files = manifest.files.clone();

    files.sort_by(|a, b| (a.partition_id, &a.path).cmp(&(b.partition_id, &b.path)));

    let mut row_groups = Vec::new();

    let mut global_row = 0_u64;
    let mut total_compressed_bytes = 0_u64;

    for (file_index, entry) in files.iter().enumerate() {
        let path = dataset_path.join(&entry.path);

        let file = File::open(&path).map_err(|error| {
            invalid_error(format!(
                "cannot open conversion input {}: {error}",
                path.display()
            ))
        })?;

        let reader = SerializedFileReader::new(file).map_err(|error| {
            invalid_error(format!(
                "cannot inspect conversion input {}: {error}",
                path.display()
            ))
        })?;

        let metadata = reader.metadata();

        for row_group_index in 0..metadata.num_row_groups() {
            let group = metadata.row_group(row_group_index);

            let rows = u64::try_from(group.num_rows())
                .map_err(|_| invalid_error("negative Parquet row count"))?;

            if rows == 0 {
                return invalid("empty conversion input row group");
            }

            let compressed_bytes = u64::try_from(group.compressed_size())
                .map_err(|_| invalid_error("negative compressed row-group size"))?;

            row_groups.push(ConversionRowGroup {
                file_index,
                row_group_index,
                global_start_row: global_row,
                rows,
                compressed_bytes,
            });

            global_row = global_row
                .checked_add(rows)
                .ok_or_else(|| invalid_error("conversion row count overflow"))?;

            total_compressed_bytes = total_compressed_bytes
                .checked_add(compressed_bytes)
                .ok_or_else(|| invalid_error("conversion compressed byte count overflow"))?;
        }
    }

    if row_groups.is_empty() {
        return invalid("conversion input contains no row groups");
    }

    if global_row != manifest.events_written {
        return invalid(format!(
            "conversion row-group count mismatch: \
             manifest={}, parquet={}",
            manifest.events_written, global_row,
        ));
    }

    // -------------------------------------------------------------------------
    // Balance contiguous row-group ranges
    // -------------------------------------------------------------------------

    let partition_count = requested_workers.min(row_groups.len());

    let mut partitions = Vec::with_capacity(partition_count);

    let mut group_start = 0_usize;
    let mut rows_assigned = 0_u64;

    for partition_index in 0..partition_count {
        let remaining_partitions = partition_count - partition_index;

        let max_end = if remaining_partitions == 1 {
            row_groups.len()
        } else {
            row_groups.len() - (remaining_partitions - 1)
        };

        let target_total = ((partition_index as u128 + 1) * u128::from(global_row)
            / partition_count as u128) as u64;

        let mut group_end = group_start;
        let mut partition_rows = 0_u64;

        while group_end < max_end {
            let next_rows = row_groups[group_end].rows;

            if remaining_partitions > 1
                && group_end > group_start
                && rows_assigned + partition_rows + next_rows > target_total
            {
                break;
            }

            partition_rows += next_rows;
            group_end += 1;
        }

        if group_end == group_start {
            partition_rows = row_groups[group_start].rows;
            group_end += 1;
        }

        let start_row = rows_assigned;

        rows_assigned += partition_rows;

        partitions.push(ConversionPartition {
            partition_id: partition_index as u32,
            first_row_group: group_start,
            end_row_group: group_end,
            start_row,
            end_row: rows_assigned,
            rows: partition_rows,
        });

        group_start = group_end;
    }

    if group_start != row_groups.len() {
        return invalid("conversion partition planner left unassigned row groups");
    }

    if rows_assigned != global_row {
        return invalid("conversion partition planner row count mismatch");
    }

    Ok(ConversionPartitionPlan {
        row_groups,
        partitions,
        total_rows: global_row,
        total_compressed_bytes,
    })
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
