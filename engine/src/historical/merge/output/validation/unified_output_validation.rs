use std::{
    collections::HashSet,
    fs::File,
    path::{Component, Path},
};

use crate::historical::merge::output::writers::unified_timeline::UnifiedTimelineRecord;
use crate::shared::error::{MarketForgeError, Result};
use arrow_array::{Array, Int64Array, UInt8Array, UInt32Array, UInt64Array};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use {
    crate::historical::merge::core::ordering::MergeKey,
    crate::historical::merge::output::manifests::manifest::MergeTimelineFile,
    crate::historical::merge::output::manifests::unified_manifest::UnifiedMergeStream,
    crate::historical::merge::output::manifests::unified_manifest::UnifiedMergeStreamKind,
    crate::historical::merge::output::validation::output_validation::ValidatedMergeOutput,
    crate::historical::merge::output::writers::unified_timeline::unified_timeline_schema,
    crate::historical::merge::partition::unified::partition_executor::UnifiedPartitionResult,
};
use {
    crate::historical::merge::depth::inventory::DepthTaskInventory,
    crate::historical::merge::output::validation::unified_reference_validation::UnifiedReferenceValidator,
    crate::historical::merge::trades::inventory::TradeTaskInventory,
};

// -----------------------------------------------------------------------------
// Validated unified output
// -----------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct ValidatedUnifiedMergeOutput {
    pub output: ValidatedMergeOutput,
    pub trade_events: u64,
    pub depth_events: u64,
}

// -----------------------------------------------------------------------------
// Physical output validation
// -----------------------------------------------------------------------------

pub fn validate_unified_merge_output(
    staging: &Path,
    partitions: &[UnifiedPartitionResult],
    streams: &[UnifiedMergeStream],
    trade_inventory: &[TradeTaskInventory],
    depth_inventory: &[DepthTaskInventory],
) -> Result<ValidatedUnifiedMergeOutput> {
    if partitions.is_empty() {
        return invalid("unified output contains no partitions");
    }

    if streams.is_empty() {
        return invalid("unified output contains no streams");
    }

    let staging = staging
        .canonicalize()
        .map_err(|error| invalid_error(format!("invalid staging directory: {error}")))?;

    let mut reference_validator = UnifiedReferenceValidator::new(trade_inventory, depth_inventory)?;

    let mut seen_files = HashSet::new();

    let mut validated_files = Vec::new();

    let mut total_events = 0_u64;
    let mut total_files = 0_u64;
    let mut total_trades = 0_u64;
    let mut total_depth = 0_u64;

    let mut global_first = None;
    let mut global_last = None;
    let mut previous_key: Option<MergeKey> = None;

    for partition in partitions {
        let mut partition_events = 0_u64;
        let mut partition_trades = 0_u64;
        let mut partition_depth = 0_u64;

        let mut partition_first = None;
        let mut partition_last = None;

        for file in &partition.files {
            // -------------------------------------------------------------
            // Validate file path
            // -------------------------------------------------------------

            let relative = Path::new(&file.path);

            if relative.is_absolute()
                || relative
                    .components()
                    .any(|component| !matches!(component, Component::Normal(_)))
            {
                return invalid(format!("unsafe unified output path: {}", file.path));
            }

            let expected_directory = format!("timeline/partition-{:06}", partition.partition_id);

            if !relative.starts_with(&expected_directory) {
                return invalid(format!(
                    "partition {} references incorrect output directory",
                    partition.partition_id
                ));
            }

            if !seen_files.insert(file.path.clone()) {
                return invalid(format!("duplicate unified output file: {}", file.path));
            }

            let path = staging.join(relative);

            let canonical = path.canonicalize().map_err(|error| {
                invalid_error(format!(
                    "failed to resolve unified file {}: {error}",
                    file.path
                ))
            })?;

            if !canonical.starts_with(&staging) {
                return invalid("unified output escapes staging directory");
            }

            let metadata = canonical.metadata().map_err(|error| {
                invalid_error(format!("failed to inspect unified output: {error}"))
            })?;

            if !metadata.is_file() || metadata.len() != file.size_bytes {
                return invalid(format!("invalid unified file metadata: {}", file.path));
            }

            // -------------------------------------------------------------
            // Open Parquet
            // -------------------------------------------------------------

            let builder =
                ParquetRecordBatchReaderBuilder::try_new(File::open(&canonical).map_err(
                    |error| invalid_error(format!("failed to open unified Parquet: {error}")),
                )?)
                .map_err(|error| invalid_error(format!("invalid unified Parquet: {error}")))?;

            if builder.schema().as_ref() != unified_timeline_schema().as_ref() {
                return invalid(format!("unexpected unified schema: {}", file.path));
            }

            let metadata_rows = u64::try_from(builder.metadata().file_metadata().num_rows())
                .map_err(|_| invalid_error("invalid Parquet row count"))?;

            let reader = builder.with_batch_size(8192).build().map_err(|error| {
                invalid_error(format!("failed to build unified reader: {error}"))
            })?;

            let mut file_rows = 0_u64;
            let mut file_first = None;
            let mut file_last = None;

            // -------------------------------------------------------------
            // Validate every physical record
            // -------------------------------------------------------------

            for batch in reader {
                let batch = batch.map_err(|error| {
                    invalid_error(format!("failed to read unified batch: {error}"))
                })?;

                macro_rules! column {
                    ($name:expr, $type:ty) => {
                        batch
                            .column_by_name($name)
                            .ok_or_else(|| {
                                invalid_error(format!("missing unified column: {}", $name))
                            })?
                            .as_any()
                            .downcast_ref::<$type>()
                            .ok_or_else(|| {
                                invalid_error(format!("invalid unified column type: {}", $name))
                            })?
                    };
                }

                let timestamps = column!("event_timestamp_ns", Int64Array);
                let ranks = column!("stream_rank", UInt32Array);
                let kinds = column!("event_kind", UInt8Array);

                let datasets = column!("dataset_id", UInt64Array);
                let tasks = column!("task_id", UInt64Array);
                let ordinals = column!("event_ordinal", UInt64Array);

                let offsets = column!("canonical_row_offset", UInt64Array);
                let counts = column!("canonical_row_count", UInt64Array);

                let boundaries = column!("event_boundary", UInt8Array);
                let operations = column!("source_operation", UInt8Array);

                for row in 0..batch.num_rows() {
                    let timestamp = timestamps.value(row);
                    let rank = ranks.value(row);
                    let kind = kinds.value(row);
                    let dataset_id = datasets.value(row);
                    let task_id = tasks.value(row);
                    let ordinal = ordinals.value(row);
                    let offset = offsets.value(row);
                    let count = counts.value(row);

                    // -----------------------------------------------------
                    // Timestamp interval
                    // -----------------------------------------------------

                    if timestamp < partition.start_ns || timestamp >= partition.end_ns {
                        return invalid(format!(
                            "event outside partition {} interval",
                            partition.partition_id
                        ));
                    }

                    // -----------------------------------------------------
                    // Stream identity
                    // -----------------------------------------------------

                    let stream = streams.get(rank as usize).ok_or_else(|| {
                        invalid_error(format!("invalid unified stream rank {rank}"))
                    })?;

                    if stream.stream_rank != rank || stream.dataset_id != dataset_id {
                        return invalid(format!("unified stream identity mismatch: rank={rank}"));
                    }

                    // -----------------------------------------------------
                    // Event kind and metadata
                    // -----------------------------------------------------

                    match kind {
                        0 => {
                            if stream.kind != UnifiedMergeStreamKind::Trade {
                                return invalid("trade event belongs to non-trade stream");
                            }

                            if count != 1 || !boundaries.is_null(row) || !operations.is_null(row) {
                                return invalid("invalid trade canonical reference");
                            }

                            partition_trades = partition_trades
                                .checked_add(1)
                                .ok_or_else(|| invalid_error("trade count overflow"))?;
                        }

                        1 => {
                            if stream.kind != UnifiedMergeStreamKind::Depth {
                                return invalid("depth event belongs to non-depth stream");
                            }

                            // Depth events may legitimately reference zero canonical rows.
                            // Reconstruction events can exist without materialized level changes.
                            if boundaries.is_null(row) || operations.is_null(row) {
                                return invalid("invalid depth canonical reference");
                            }

                            if boundaries.value(row) > 1 || operations.value(row) > 2 {
                                return invalid("invalid depth event metadata");
                            }

                            partition_depth = partition_depth
                                .checked_add(1)
                                .ok_or_else(|| invalid_error("depth count overflow"))?;
                        }

                        _ => {
                            return invalid(format!("unsupported unified event kind {kind}"));
                        }
                    }

                    // -----------------------------------------------------
                    // Canonical reference arithmetic
                    // -----------------------------------------------------

                    offset
                        .checked_add(count)
                        .ok_or_else(|| invalid_error("canonical row range overflow"))?;

                    // -----------------------------------------------------
                    // Complete deterministic ordering
                    // -----------------------------------------------------

                    let key = MergeKey {
                        timestamp_ns: timestamp,
                        stream_rank: rank,
                        task_rank: task_id,
                        event_ordinal: ordinal,
                    };

                    if let Some(previous) = previous_key {
                        if key < previous {
                            return invalid(format!(
                                "unified ordering regression: \
             previous={previous:?}, current={key:?}"
                            ));
                        }
                    }

                    previous_key = Some(key);

                    // -----------------------------------------------------
                    // Canonical reference arithmetic
                    // -----------------------------------------------------

                    offset
                        .checked_add(count)
                        .ok_or_else(|| invalid_error("canonical row range overflow"))?;

                    // -----------------------------------------------------
                    // Canonical source-reference validation
                    // -----------------------------------------------------

                    let record = UnifiedTimelineRecord {
                        event_timestamp_ns: timestamp,
                        stream_rank: rank,
                        event_kind: kind,

                        dataset_id,
                        task_id,
                        event_ordinal: ordinal,

                        canonical_row_offset: offset,
                        canonical_row_count: count,

                        event_boundary: (!boundaries.is_null(row)).then(|| boundaries.value(row)),

                        source_operation: (!operations.is_null(row)).then(|| operations.value(row)),
                    };

                    reference_validator.validate(&record)?;

                    // -----------------------------------------------------
                    // Complete deterministic ordering
                    // -----------------------------------------------------

                    let key = MergeKey {
                        timestamp_ns: timestamp,
                        stream_rank: rank,
                        task_rank: task_id,
                        event_ordinal: ordinal,
                    };

                    if let Some(previous) = previous_key {
                        if key < previous {
                            return invalid(format!(
                                "unified ordering regression: \
                                 previous={previous:?}, current={key:?}"
                            ));
                        }
                    }

                    previous_key = Some(key);

                    if file_first.is_none() {
                        file_first = Some(timestamp);
                    }

                    file_last = Some(timestamp);

                    file_rows = file_rows
                        .checked_add(1)
                        .ok_or_else(|| invalid_error("file row count overflow"))?;
                }
            }

            if file_rows != metadata_rows {
                return invalid(format!("Parquet row count mismatch: {}", file.path));
            }

            partition_events = partition_events
                .checked_add(file_rows)
                .ok_or_else(|| invalid_error("partition count overflow"))?;

            if partition_first.is_none() {
                partition_first = file_first;
            }

            if file_last.is_some() {
                partition_last = file_last;
            }

            validated_files.push(MergeTimelineFile {
                path: file.path.clone(),
                size_bytes: metadata.len(),
                partition_id: Some(partition.partition_id),
                rows: Some(file_rows),
                start_timestamp_ns: file_first,
                end_timestamp_ns: file_last,
            });

            total_files = total_files
                .checked_add(1)
                .ok_or_else(|| invalid_error("file count overflow"))?;
        }

        // -----------------------------------------------------------------
        // Validate partition metrics
        // -----------------------------------------------------------------

        if partition_events != partition.events_written
            || partition_trades != partition.trade_events
            || partition_depth != partition.depth_events
        {
            return invalid(format!(
                "partition {} physical event count mismatch",
                partition.partition_id
            ));
        }

        if partition_first != partition.first_timestamp_ns
            || partition_last != partition.last_timestamp_ns
        {
            return invalid(format!(
                "partition {} timestamp metadata mismatch",
                partition.partition_id
            ));
        }

        if partition.files.len() as u64 != partition.files_written {
            return invalid(format!(
                "partition {} physical file count mismatch",
                partition.partition_id
            ));
        }

        total_events = total_events
            .checked_add(partition_events)
            .ok_or_else(|| invalid_error("total event count overflow"))?;

        total_trades = total_trades
            .checked_add(partition_trades)
            .ok_or_else(|| invalid_error("total trade count overflow"))?;

        total_depth = total_depth
            .checked_add(partition_depth)
            .ok_or_else(|| invalid_error("total depth count overflow"))?;

        if global_first.is_none() {
            global_first = partition_first;
        }

        if partition_last.is_some() {
            global_last = partition_last;
        }
    }

    Ok(ValidatedUnifiedMergeOutput {
        output: ValidatedMergeOutput {
            events_written: total_events,
            files_written: total_files,
            start_timestamp_ns: global_first,
            end_timestamp_ns: global_last,
            files: validated_files,
        },
        trade_events: total_trades,
        depth_events: total_depth,
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
