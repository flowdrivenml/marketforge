use std::{
    collections::HashSet,
    path::{Path, PathBuf},
};

use crate::error::{MarketForgeError, Result};

use super::{
    manifest::MergeTimelineFile, materialize_partition::MaterializationPartitionResult,
    materialize_validate::validate_physical_partition, unified_manifest::UnifiedMergeMode,
};

// -----------------------------------------------------------------------------
// Global validation result
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct ValidatedPhysicalDataset {
    pub events: u64,
    pub trade_events: u64,
    pub depth_events: u64,
    pub canonical_rows: u64,

    pub files_written: u64,
    pub row_groups: u64,

    pub first_timestamp_ns: Option<i64>,
    pub last_timestamp_ns: Option<i64>,

    pub files: Vec<MergeTimelineFile>,
}

// -----------------------------------------------------------------------------
// Global physical validation
// -----------------------------------------------------------------------------

pub fn validate_physical_dataset(
    staging: &Path,
    mode: UnifiedMergeMode,
    partitions: &[MaterializationPartitionResult],
) -> Result<ValidatedPhysicalDataset> {
    if partitions.is_empty() {
        return invalid("physical dataset contains no partitions");
    }

    let staging = staging.canonicalize().map_err(|error| {
        invalid_error(format!(
            "cannot resolve physical staging directory: {error}"
        ))
    })?;

    let mut result = ValidatedPhysicalDataset::default();

    let mut seen_partitions = HashSet::new();
    let mut seen_files = HashSet::<PathBuf>::new();

    let mut previous_partition_end = None;
    let mut previous_timestamp = None;

    for partition in partitions {
        // ---------------------------------------------------------------------
        // Partition identity and ordering
        // ---------------------------------------------------------------------

        if !seen_partitions.insert(partition.partition_id) {
            return invalid(format!(
                "duplicate physical partition ID {}",
                partition.partition_id
            ));
        }

        if partition.start_ns >= partition.end_ns {
            return invalid(format!(
                "invalid physical partition interval: {}",
                partition.partition_id
            ));
        }

        if let Some(previous_end) = previous_partition_end {
            if partition.start_ns < previous_end {
                return invalid("physical partitions overlap or are unordered");
            }
        }

        previous_partition_end = Some(partition.end_ns);

        // ---------------------------------------------------------------------
        // Validate actual physical Parquet
        // ---------------------------------------------------------------------

        let validated = validate_physical_partition(&staging, mode, partition)?;

        if let (Some(previous), Some(first)) = (previous_timestamp, validated.first_timestamp_ns) {
            if first < previous {
                return invalid("physical cross-partition timestamp regression");
            }
        }

        if let Some(first) = validated.first_timestamp_ns {
            result.first_timestamp_ns.get_or_insert(first);
        }

        if let Some(last) = validated.last_timestamp_ns {
            previous_timestamp = Some(last);
            result.last_timestamp_ns = Some(last);
        }

        // ---------------------------------------------------------------------
        // Aggregate counts
        // ---------------------------------------------------------------------

        result.events = checked_add(
            result.events,
            validated.events,
            "physical event count overflow",
        )?;

        result.trade_events = checked_add(
            result.trade_events,
            validated.trade_events,
            "physical trade count overflow",
        )?;

        result.depth_events = checked_add(
            result.depth_events,
            validated.depth_events,
            "physical depth count overflow",
        )?;

        result.canonical_rows = checked_add(
            result.canonical_rows,
            validated.canonical_rows,
            "physical canonical row count overflow",
        )?;

        result.row_groups = checked_add(
            result.row_groups,
            validated.row_groups,
            "physical row-group count overflow",
        )?;

        // ---------------------------------------------------------------------
        // Construct file metadata
        // ---------------------------------------------------------------------

        for file in &validated.file_metadata {
            let path = &file.path;
            let canonical = path.canonicalize().map_err(|error| {
                invalid_error(format!(
                    "cannot resolve physical file {}: {error}",
                    path.display()
                ))
            })?;

            if !canonical.starts_with(&staging) {
                return invalid("physical file escapes staging directory");
            }

            if !seen_files.insert(canonical.clone()) {
                return invalid("duplicate physical output file");
            }

            let relative = canonical
                .strip_prefix(&staging)
                .map_err(|_| invalid_error("invalid physical relative path"))?;

            let metadata = canonical.metadata().map_err(|error| {
                invalid_error(format!("cannot inspect physical file metadata: {error}"))
            })?;

            if !metadata.is_file() || metadata.len() == 0 {
                return invalid("invalid physical output file");
            }

            let relative_name = relative
                .to_str()
                .ok_or_else(|| invalid_error("non-UTF8 physical file path"))?
                .to_owned();

            result.files.push(MergeTimelineFile {
                path: relative_name,
                size_bytes: metadata.len(),
                partition_id: Some(partition.partition_id),
                rows: Some(file.rows),
                start_timestamp_ns: Some(file.first_timestamp_ns),
                end_timestamp_ns: Some(file.last_timestamp_ns),
            });
        }
    }

    // -------------------------------------------------------------------------
    // Global consistency
    // -------------------------------------------------------------------------

    result.files_written = u64::try_from(result.files.len())
        .map_err(|_| invalid_error("physical file count overflow"))?;

    if result.events
        != checked_add(
            result.trade_events,
            result.depth_events,
            "physical event-kind total overflow",
        )?
    {
        return invalid("physical event-kind totals mismatch");
    }

    let expected_files = partitions.iter().try_fold(0_u64, |total, partition| {
        checked_add(
            total,
            partition.writer_metrics.files_written,
            "expected physical file count overflow",
        )
    })?;

    if result.files_written != expected_files {
        return invalid("global physical file count mismatch");
    }

    match mode {
        UnifiedMergeMode::Trades if result.depth_events != 0 => {
            return invalid("trade-only physical dataset contains depth events");
        }

        UnifiedMergeMode::Depth if result.trade_events != 0 => {
            return invalid("depth-only physical dataset contains trade events");
        }

        _ => {}
    }

    Ok(result)
}

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

fn checked_add(left: u64, right: u64, message: &'static str) -> Result<u64> {
    left.checked_add(right)
        .ok_or_else(|| invalid_error(message))
}

fn invalid_error(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(message.into())
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(invalid_error(message))
}
