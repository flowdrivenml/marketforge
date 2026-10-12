use std::{
    fs::File,
    path::{Path, PathBuf},
};

use arrow_array::{
    Array, Int64Array, ListArray, RecordBatch, StructArray, UInt8Array, UInt32Array, UInt64Array,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::error::{MarketForgeError, Result};

use super::{
    materialize_partition::MaterializationPartitionResult,
    materialize_schema::{physical_combined_schema, physical_depth_schema, physical_trade_schema},
    unified_manifest::UnifiedMergeMode,
};

type OrderingKey = (i64, u32, u64, u64);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PhysicalFileValidation {
    pub path: PathBuf,
    pub rows: u64,
    pub row_groups: u64,
    pub first_timestamp_ns: i64,
    pub last_timestamp_ns: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PhysicalPartitionValidation {
    pub partition_id: u32,

    pub events: u64,
    pub trade_events: u64,
    pub depth_events: u64,
    pub canonical_rows: u64,

    pub files: Vec<PathBuf>,
    pub file_metadata: Vec<PhysicalFileValidation>,

    pub row_groups: u64,

    pub first_timestamp_ns: Option<i64>,
    pub last_timestamp_ns: Option<i64>,
}

pub fn validate_physical_partition(
    staging: &Path,
    mode: UnifiedMergeMode,
    result: &MaterializationPartitionResult,
) -> Result<PhysicalPartitionValidation> {
    let directory = staging
        .join("timeline")
        .join(format!("partition-{:06}", result.partition_id));

    let mut files = Vec::new();

    for entry in std::fs::read_dir(&directory).map_err(|error| {
        invalid_error(format!(
            "cannot inspect physical partition {}: {error}",
            directory.display()
        ))
    })? {
        let entry = entry
            .map_err(|error| invalid_error(format!("cannot inspect physical file: {error}")))?;

        let path = entry.path();

        let file_type = entry.file_type().map_err(|error| {
            invalid_error(format!("cannot inspect physical directory entry: {error}"))
        })?;

        if !file_type.is_file() {
            return invalid(format!(
                "unexpected physical partition entry: {}",
                path.display()
            ));
        }

        if path.extension().and_then(|ext| ext.to_str()) != Some("parquet") {
            return invalid(format!(
                "unexpected non-Parquet file in physical partition: {}",
                path.display()
            ));
        }

        files.push(path);
    }

    files.sort();

    if files.len() as u64 != result.writer_metrics.files_written {
        return invalid("physical partition file-count mismatch");
    }

    let expected_schema = match mode {
        UnifiedMergeMode::Trades => physical_trade_schema(),
        UnifiedMergeMode::Depth => physical_depth_schema(),
        UnifiedMergeMode::Combined => physical_combined_schema(),
    };

    let mut validation = PhysicalPartitionValidation {
        partition_id: result.partition_id,
        ..Default::default()
    };

    let mut previous_key: Option<OrderingKey> = None;

    for (index, path) in files.iter().enumerate() {
        let expected_name = format!("part-{index:06}.parquet");

        if path.file_name().and_then(|name| name.to_str()) != Some(expected_name.as_str()) {
            return invalid("nonconsecutive physical Parquet filenames");
        }

        let file = File::open(path).map_err(|error| {
            invalid_error(format!(
                "cannot open physical Parquet {}: {error}",
                path.display()
            ))
        })?;

        let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|error| {
            invalid_error(format!(
                "cannot inspect physical Parquet {}: {error}",
                path.display()
            ))
        })?;

        if builder.schema().as_ref() != expected_schema.as_ref() {
            return invalid(format!("physical schema mismatch: {}", path.display()));
        }

        let row_groups = builder.metadata().num_row_groups() as u64;

        let expected_rows = builder.metadata().file_metadata().num_rows();

        validation.row_groups += row_groups;

        if expected_rows <= 0 {
            return invalid("empty physical Parquet file");
        }

        let reader = builder.build().map_err(|error| {
            invalid_error(format!("cannot initialize physical reader: {error}"))
        })?;

        let mut file_rows = 0_u64;
        let mut file_first_timestamp = None;
        let mut file_last_timestamp = None;

        for batch in reader {
            let batch = batch.map_err(|error| {
                invalid_error(format!("physical Parquet decoding failed: {error}"))
            })?;

            file_rows += batch.num_rows() as u64;

            validate_batch(
                &batch,
                mode,
                result.start_ns,
                result.end_ns,
                &mut previous_key,
                &mut validation,
            )?;

            if batch.num_rows() > 0 {
                let timestamps = batch
                    .column_by_name("event_timestamp_ns")
                    .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
                    .ok_or_else(|| invalid_error("missing physical event timestamps"))?;

                file_first_timestamp.get_or_insert(timestamps.value(0));

                file_last_timestamp = Some(timestamps.value(batch.num_rows() - 1));
            }
        }

        if file_rows != expected_rows as u64 {
            return invalid(format!(
                "physical Parquet row-count mismatch: file={}, metadata={}, decoded={}",
                path.display(),
                expected_rows,
                file_rows
            ));
        }

        let first_timestamp_ns = file_first_timestamp
            .ok_or_else(|| invalid_error("physical file has no first timestamp"))?;

        let last_timestamp_ns = file_last_timestamp
            .ok_or_else(|| invalid_error("physical file has no last timestamp"))?;

        validation.file_metadata.push(PhysicalFileValidation {
            path: path.clone(),
            rows: file_rows,
            row_groups,
            first_timestamp_ns,
            last_timestamp_ns,
        });

        validation.files.push(path.clone());
    }

    if validation.events != result.references_read
        || validation.events != result.writer_metrics.events_written
        || validation.trade_events != result.writer_metrics.trade_events
        || validation.depth_events != result.writer_metrics.depth_events
        || validation.canonical_rows != result.writer_metrics.canonical_rows
    {
        return invalid(format!(
            "physical partition metrics mismatch: partition={}",
            result.partition_id
        ));
    }

    Ok(validation)
}

fn validate_batch(
    batch: &RecordBatch,
    mode: UnifiedMergeMode,
    start_ns: i64,
    end_ns: i64,
    previous_key: &mut Option<OrderingKey>,
    validation: &mut PhysicalPartitionValidation,
) -> Result<()> {
    validate_required_fields(batch)?;
    let timestamps = column::<Int64Array>(batch, "event_timestamp_ns")?;
    let ranks = column::<UInt32Array>(batch, "stream_rank")?;
    let tasks = column::<UInt64Array>(batch, "task_id")?;
    let ordinals = column::<UInt64Array>(batch, "event_ordinal")?;
    let canonical_counts = column::<UInt64Array>(batch, "canonical_row_count")?;

    let kinds = if mode == UnifiedMergeMode::Combined {
        Some(column::<UInt8Array>(batch, "event_kind")?)
    } else {
        None
    };

    let changes = if mode == UnifiedMergeMode::Depth {
        Some(column::<ListArray>(batch, "changes")?)
    } else {
        None
    };

    let boundaries = if mode == UnifiedMergeMode::Depth {
        Some(column::<UInt8Array>(batch, "event_boundary")?)
    } else {
        None
    };

    let operations = if mode == UnifiedMergeMode::Depth {
        Some(column::<UInt8Array>(batch, "source_operation")?)
    } else {
        None
    };

    let trade_payload = if mode == UnifiedMergeMode::Combined {
        Some(column::<StructArray>(batch, "trade")?)
    } else {
        None
    };

    let depth_payload = if mode == UnifiedMergeMode::Combined {
        Some(column::<StructArray>(batch, "depth")?)
    } else {
        None
    };

    for row in 0..batch.num_rows() {
        let timestamp = timestamps.value(row);

        if timestamp < start_ns || timestamp >= end_ns {
            return invalid("physical event outside partition bounds");
        }

        let key = (
            timestamp,
            ranks.value(row),
            tasks.value(row),
            ordinals.value(row),
        );

        if previous_key.is_some_and(|previous| key < previous) {
            return invalid("physical event ordering regression");
        }

        *previous_key = Some(key);

        let canonical_rows = canonical_counts.value(row);
        let canonical_offsets = column::<UInt64Array>(batch, "canonical_row_offset")?;
        canonical_offsets
            .value(row)
            .checked_add(canonical_rows)
            .ok_or_else(|| invalid_error("physical canonical row range overflow"))?;

        match mode {
            UnifiedMergeMode::Trades => {
                if canonical_rows != 1 {
                    return invalid("physical trade canonical count must equal one");
                }

                validation.trade_events += 1;
            }

            UnifiedMergeMode::Depth => {
                let list = changes.unwrap();

                validate_depth_metadata(
                    boundaries.unwrap().value(row),
                    operations.unwrap().value(row),
                )?;

                validate_depth_levels(list, row)?;

                if list.value_length(row) as u64 != canonical_rows {
                    return invalid("physical depth canonical level-count mismatch");
                }

                validation.depth_events += 1;
            }

            UnifiedMergeMode::Combined => {
                let kind = kinds.unwrap().value(row);

                let trade = trade_payload.unwrap();
                let depth = depth_payload.unwrap();

                match kind {
                    0 => {
                        if trade.is_null(row) || !depth.is_null(row) {
                            return invalid("invalid combined trade payload");
                        }

                        if canonical_rows != 1 {
                            return invalid("combined trade canonical count mismatch");
                        }

                        validation.trade_events += 1;
                    }

                    1 => {
                        if depth.is_null(row) || !trade.is_null(row) {
                            return invalid("invalid combined depth payload");
                        }

                        let nested = depth
                            .column_by_name("changes")
                            .ok_or_else(|| invalid_error("missing combined depth changes"))?
                            .as_any()
                            .downcast_ref::<ListArray>()
                            .ok_or_else(|| invalid_error("invalid combined depth changes"))?;

                        if nested.is_null(row) || nested.value_length(row) as u64 != canonical_rows
                        {
                            return invalid("combined depth canonical count mismatch");
                        }

                        validation.depth_events += 1;
                    }

                    _ => return invalid("unsupported physical event kind"),
                }
            }
        }

        validation.events += 1;
        validation.canonical_rows += canonical_rows;

        if validation.first_timestamp_ns.is_none() {
            validation.first_timestamp_ns = Some(timestamp);
        }

        validation.last_timestamp_ns = Some(timestamp);
    }

    Ok(())
}

fn column<'a, T: Array + 'static>(batch: &'a RecordBatch, name: &str) -> Result<&'a T> {
    batch
        .column_by_name(name)
        .and_then(|array| array.as_any().downcast_ref::<T>())
        .ok_or_else(|| invalid_error(format!("missing or invalid physical column: {name}")))
}

fn invalid_error(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(message.into())
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(invalid_error(message))
}

fn validate_required_fields(batch: &RecordBatch) -> Result<()> {
    let schema = batch.schema();

    for (field, column) in schema.fields().iter().zip(batch.columns()) {
        if !field.is_nullable() && column.null_count() != 0 {
            return invalid(format!(
                "null values in required physical field: {}",
                field.name()
            ));
        }
    }

    Ok(())
}
fn validate_depth_metadata(boundary: u8, operation: u8) -> Result<()> {
    if boundary > 1 {
        return invalid("invalid physical depth event boundary");
    }

    if operation > 2 {
        return invalid("invalid physical depth source operation");
    }

    Ok(())
}

fn validate_depth_levels(changes: &ListArray, row: usize) -> Result<()> {
    if changes.is_null(row) {
        return invalid("null physical depth changes");
    }

    let levels = changes.value(row);

    let levels = levels
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| invalid_error("depth changes must contain structs"))?;

    if levels.null_count() != 0 {
        return invalid("null depth level struct");
    }

    for (field, column) in levels.fields().iter().zip(levels.columns()) {
        if !field.is_nullable() && column.null_count() != 0 {
            return invalid(format!("null required depth level field: {}", field.name()));
        }
    }

    Ok(())
}
