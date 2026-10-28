use std::{
    collections::HashSet,
    fs::File,
    path::{Component, Path},
};

use arrow_array::{Array, Int64Array};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::shared::error::{MarketForgeError, Result};

use {
    crate::historical::merge::output::manifests::manifest::MergeTimelineFile,
    crate::historical::merge::output::writers::timeline::timeline_schema,
    crate::historical::merge::partition::shared::partition_executor::MergePartitionResult,
};

// -----------------------------------------------------------------------------
// Validated output
// -----------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct ValidatedMergeOutput {
    pub events_written: u64,
    pub files_written: u64,

    pub start_timestamp_ns: Option<i64>,
    pub end_timestamp_ns: Option<i64>,

    pub files: Vec<MergeTimelineFile>,
}

// -----------------------------------------------------------------------------
// Validate physical partition outputs
// -----------------------------------------------------------------------------

pub fn validate_merge_output(
    staging: &Path,
    partitions: &[MergePartitionResult],
) -> Result<ValidatedMergeOutput> {
    if partitions.is_empty() {
        return invalid("merge output contains no partitions");
    }

    let staging = staging.canonicalize().map_err(|error| {
        invalid_error(format!(
            "failed to resolve merge staging directory {}: {error}",
            staging.display()
        ))
    })?;

    let mut files = Vec::new();
    let mut seen_paths = HashSet::new();

    let mut total_events = 0_u64;
    let mut total_files = 0_u64;

    let mut global_first = None;
    let mut global_last = None;
    let mut previous_global_timestamp = None;

    // -------------------------------------------------------------------------
    // Validate partitions in chronological order
    // -------------------------------------------------------------------------

    for partition in partitions {
        let mut partition_rows = 0_u64;
        let mut partition_first = None;
        let mut partition_last = None;

        let mut previous_file_timestamp = None;

        for file in &partition.files {
            // -----------------------------------------------------------------
            // Validate relative path
            // -----------------------------------------------------------------

            let relative = Path::new(&file.path);

            if relative.is_absolute()
                || relative
                    .components()
                    .any(|component| !matches!(component, Component::Normal(_)))
            {
                return invalid(format!("unsafe merge output path: {}", file.path));
            }

            let expected_directory = format!("timeline/partition-{:06}", partition.partition_id);

            if !relative.starts_with(&expected_directory) {
                return invalid(format!(
                    "partition {} references file outside its directory: {}",
                    partition.partition_id, file.path
                ));
            }

            if !seen_paths.insert(file.path.clone()) {
                return invalid(format!("duplicate merge output file: {}", file.path));
            }

            let path = staging.join(relative);

            let canonical = path.canonicalize().map_err(|error| {
                invalid_error(format!(
                    "failed to resolve merge output {}: {error}",
                    path.display()
                ))
            })?;

            if !canonical.starts_with(&staging) {
                return invalid(format!(
                    "merge output escapes staging directory: {}",
                    file.path
                ));
            }

            // -----------------------------------------------------------------
            // Validate file size
            // -----------------------------------------------------------------

            let metadata = canonical.metadata().map_err(|error| {
                invalid_error(format!(
                    "failed to inspect merge output {}: {error}",
                    canonical.display()
                ))
            })?;

            if !metadata.is_file() {
                return invalid(format!("merge output is not a regular file: {}", file.path));
            }

            if metadata.len() != file.size_bytes {
                return invalid(format!("merge file size mismatch: {}", file.path));
            }

            // -----------------------------------------------------------------
            // Open Parquet reader
            // -----------------------------------------------------------------

            let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(&canonical).map_err(
                |error| {
                    invalid_error(format!(
                        "failed to open merge Parquet file {}: {error}",
                        canonical.display()
                    ))
                },
            )?)
            .map_err(|error| {
                invalid_error(format!("invalid merge Parquet file {}: {error}", file.path))
            })?;

            // -----------------------------------------------------------------
            // Validate schema
            // -----------------------------------------------------------------

            if reader.schema().as_ref() != timeline_schema().as_ref() {
                return invalid(format!("unexpected merge timeline schema: {}", file.path));
            }

            let metadata_rows = u64::try_from(reader.metadata().file_metadata().num_rows())
                .map_err(|_| invalid_error("invalid Parquet row count"))?;

            let mut reader = reader
                .with_batch_size(8192)
                .build()
                .map_err(|error| invalid_error(format!("failed to build merge reader: {error}")))?;

            // -----------------------------------------------------------------
            // Validate actual event timestamps
            // -----------------------------------------------------------------

            let mut actual_rows = 0_u64;
            let mut file_first = None;
            let mut file_last = None;

            for batch in &mut reader {
                let batch = batch.map_err(|error| {
                    invalid_error(format!("failed to read merge Parquet batch: {error}"))
                })?;

                let timestamps = batch
                    .column_by_name("event_timestamp_ns")
                    .ok_or_else(|| invalid_error("merge timeline missing event_timestamp_ns"))?
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| invalid_error("merge timestamp column is not Int64"))?;

                for row in 0..batch.num_rows() {
                    if timestamps.is_null(row) {
                        return invalid(format!("null timestamp in {}", file.path));
                    }

                    let timestamp = timestamps.value(row);

                    if timestamp < partition.start_ns || timestamp >= partition.end_ns {
                        return invalid(format!(
                            "timestamp {} outside partition {} interval",
                            timestamp, partition.partition_id
                        ));
                    }

                    if let Some(previous) = file_last {
                        if timestamp < previous {
                            return invalid(format!("timestamp regression inside {}", file.path));
                        }
                    }

                    if file_first.is_none() {
                        file_first = Some(timestamp);
                    }

                    file_last = Some(timestamp);

                    actual_rows = actual_rows
                        .checked_add(1)
                        .ok_or_else(|| invalid_error("merge row count overflow"))?;
                }
            }

            // -----------------------------------------------------------------
            // Verify physical row count
            // -----------------------------------------------------------------

            if actual_rows != metadata_rows {
                return invalid(format!(
                    "Parquet row count mismatch in {}: metadata={}, actual={}",
                    file.path, metadata_rows, actual_rows
                ));
            }

            // -----------------------------------------------------------------
            // Verify ordering across files
            // -----------------------------------------------------------------

            if let (Some(previous), Some(first)) = (previous_file_timestamp, file_first) {
                if first < previous {
                    return invalid(format!(
                        "timestamp regression across files in partition {}",
                        partition.partition_id
                    ));
                }
            }

            if file_first.is_some() {
                previous_file_timestamp = file_last;
            }

            if partition_first.is_none() {
                partition_first = file_first;
            }

            if file_last.is_some() {
                partition_last = file_last;
            }

            partition_rows = partition_rows
                .checked_add(actual_rows)
                .ok_or_else(|| invalid_error("partition row count overflow"))?;

            total_files = total_files
                .checked_add(1)
                .ok_or_else(|| invalid_error("merge file count overflow"))?;

            // -----------------------------------------------------------------
            // Populate validated file metadata
            // -----------------------------------------------------------------

            files.push(MergeTimelineFile {
                path: file.path.clone(),
                size_bytes: metadata.len(),

                partition_id: Some(partition.partition_id),
                rows: Some(actual_rows),

                start_timestamp_ns: file_first,
                end_timestamp_ns: file_last,
            });
        }

        // ---------------------------------------------------------------------
        // Verify partition metrics
        // ---------------------------------------------------------------------

        if partition_rows != partition.events_written {
            return invalid(format!(
                "partition {} event count mismatch: reported={}, actual={}",
                partition.partition_id, partition.events_written, partition_rows
            ));
        }

        if partition_first != partition.first_timestamp_ns
            || partition_last != partition.last_timestamp_ns
        {
            return invalid(format!(
                "partition {} timestamp metrics mismatch",
                partition.partition_id
            ));
        }

        if partition.files.len() as u64 != partition.files_written {
            return invalid(format!(
                "partition {} file count mismatch",
                partition.partition_id
            ));
        }

        // ---------------------------------------------------------------------
        // Verify global ordering
        // ---------------------------------------------------------------------

        if let (Some(previous), Some(first)) = (previous_global_timestamp, partition_first) {
            if first < previous {
                return invalid(format!(
                    "global timestamp regression at partition {}",
                    partition.partition_id
                ));
            }
        }

        if global_first.is_none() {
            global_first = partition_first;
        }

        if partition_last.is_some() {
            global_last = partition_last;
            previous_global_timestamp = partition_last;
        }

        total_events = total_events
            .checked_add(partition_rows)
            .ok_or_else(|| invalid_error("global event count overflow"))?;
    }

    Ok(ValidatedMergeOutput {
        events_written: total_events,
        files_written: total_files,

        start_timestamp_ns: global_first,
        end_timestamp_ns: global_last,

        files,
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
