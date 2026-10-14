use std::{fs::File, path::Path};

use arrow_array::{
    Array, Decimal256Array, Int64Array, ListArray, RecordBatch, StringArray, StructArray,
    UInt8Array, UInt32Array, UInt64Array,
};

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::{
    error::{MarketForgeError, Result},
    process::convert::reference::ReferenceKey,
};

use super::{
    combined_numeric::validate_combined_numeric_batch,
    files::discover_conversion_files,
    schema::{
        CONVERSION_FAILURE_ARITHMETIC, CONVERSION_FAILURE_FUTURE,
        CONVERSION_FAILURE_IDENTITY_MISMATCH, CONVERSION_FAILURE_MISSING,
        CONVERSION_FAILURE_NO_PATH, CONVERSION_FAILURE_STALE, CONVERSION_STATUS_CONVERTED,
        CONVERSION_STATUS_IDENTITY, CONVERSION_STATUS_UNAVAILABLE,
    },
};

#[derive(Debug, Clone, Default)]
pub struct CombinedValidationMetrics {
    pub events: u64,
    pub trade_events: u64,
    pub depth_events: u64,

    pub identity: u64,
    pub converted: u64,
    pub unavailable: u64,

    pub depth_levels: u64,
    pub depth_levels_converted: u64,
    pub depth_levels_unavailable: u64,

    pub numerical_trade_values: u64,
    pub numerical_depth_levels: u64,

    pub references_checked: u64,

    pub files: u64,
    pub row_groups: u64,
}

#[derive(Debug, Clone)]
pub struct CombinedFileValidationResult {
    pub metrics: CombinedValidationMetrics,
    pub first_key: ReferenceKey,
    pub last_key: ReferenceKey,
}

impl CombinedValidationMetrics {
    pub fn accumulate(&mut self, other: &Self) -> Result<()> {
        macro_rules! add {
            ($field:ident) => {
                self.$field = self.$field.checked_add(other.$field).ok_or_else(|| {
                    invalid_error(concat!(
                        "combined validation metric overflow: ",
                        stringify!($field)
                    ))
                })?;
            };
        }

        add!(events);
        add!(trade_events);
        add!(depth_events);

        add!(identity);
        add!(converted);
        add!(unavailable);

        add!(depth_levels);
        add!(depth_levels_converted);
        add!(depth_levels_unavailable);

        add!(numerical_trade_values);
        add!(numerical_depth_levels);

        add!(references_checked);
        add!(files);
        add!(row_groups);

        Ok(())
    }
}

// -----------------------------------------------------------------------------
// Validate one physical Parquet file
// -----------------------------------------------------------------------------

pub fn validate_combined_file(
    path: &Path,
    max_staleness_ns: u64,
) -> Result<CombinedFileValidationResult> {
    if max_staleness_ns == 0 {
        return invalid("combined validation staleness must be positive");
    }

    let file = File::open(path).map_err(|error| invalid_error(error.to_string()))?;

    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|error| invalid_error(error.to_string()))?;

    let row_groups = builder.metadata().num_row_groups() as u64;

    let reader = builder
        .with_batch_size(4096)
        .build()
        .map_err(|error| invalid_error(error.to_string()))?;

    let mut metrics = CombinedValidationMetrics {
        files: 1,
        row_groups,
        ..Default::default()
    };

    let mut first_key = None;
    let mut previous_key = None;

    for batch in reader {
        let batch = batch.map_err(|error| invalid_error(error.to_string()))?;

        if batch.num_rows() == 0 {
            continue;
        }

        validate_combined_batch(
            &batch,
            max_staleness_ns,
            &mut first_key,
            &mut previous_key,
            &mut metrics,
        )?;
    }

    Ok(CombinedFileValidationResult {
        metrics,
        first_key: first_key.ok_or_else(|| invalid_error("empty combined Parquet file"))?,
        last_key: previous_key.ok_or_else(|| invalid_error("missing combined final key"))?,
    })
}

// -----------------------------------------------------------------------------
// Validate one Arrow batch
// -----------------------------------------------------------------------------

fn validate_combined_batch(
    batch: &RecordBatch,
    max_staleness_ns: u64,
    first_key: &mut Option<ReferenceKey>,
    previous_key: &mut Option<ReferenceKey>,
    metrics: &mut CombinedValidationMetrics,
) -> Result<()> {
    let timestamps = column::<Int64Array>(batch, "event_timestamp_ns")?;
    let ranks = column::<UInt32Array>(batch, "stream_rank")?;
    let tasks = column::<UInt64Array>(batch, "task_id")?;
    let ordinals = column::<UInt64Array>(batch, "event_ordinal")?;

    let kinds = column::<UInt8Array>(batch, "event_kind")?;
    let statuses = column::<UInt8Array>(batch, "conversion_status")?;
    let failures = column::<UInt8Array>(batch, "conversion_failure")?;

    let targets = column::<StringArray>(batch, "target_quote")?;
    let rates = column::<Decimal256Array>(batch, "conversion_rate")?;
    let paths = column::<ListArray>(batch, "conversion_path")?;
    let references = column::<ListArray>(batch, "conversion_references")?;

    let trades = column::<StructArray>(batch, "trade")?;
    let trade_targets = column::<StructArray>(batch, "trade_target")?;

    let depths = column::<StructArray>(batch, "depth")?;
    let depth_targets = column::<ListArray>(batch, "depth_target")?;

    let source_changes = depths
        .column_by_name("changes")
        .and_then(|array| array.as_any().downcast_ref::<ListArray>())
        .ok_or_else(|| invalid_error("missing combined depth changes"))?;

    let reference_values = references
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| invalid_error("invalid combined references"))?;

    let reference_timestamps = struct_column::<Int64Array>(reference_values, "event_timestamp_ns")?;

    let reference_ranks = struct_column::<UInt32Array>(reference_values, "stream_rank")?;

    let reference_tasks = struct_column::<UInt64Array>(reference_values, "task_id")?;

    let reference_ordinals = struct_column::<UInt64Array>(reference_values, "event_ordinal")?;

    for row in 0..batch.num_rows() {
        if timestamps.is_null(row)
            || ranks.is_null(row)
            || tasks.is_null(row)
            || ordinals.is_null(row)
            || kinds.is_null(row)
            || statuses.is_null(row)
            || targets.is_null(row)
            || references.is_null(row)
        {
            return invalid("null required combined conversion field");
        }

        let key = ReferenceKey::new(
            timestamps.value(row),
            ranks.value(row),
            tasks.value(row),
            ordinals.value(row),
        );

        if first_key.is_none() {
            *first_key = Some(key);
        }

        if let Some(previous) = previous_key {
            if key < *previous {
                return invalid("combined timeline ordering regression");
            }
        }

        *previous_key = Some(key);

        if targets.value(row).is_empty() {
            return invalid("empty combined target currency");
        }

        let kind = kinds.value(row);
        let status = statuses.value(row);

        match kind {
            0 => {
                if trades.is_null(row)
                    || !depths.is_null(row)
                    || trade_targets.is_null(row)
                    || !depth_targets.is_null(row)
                {
                    return invalid("invalid combined trade payload");
                }

                metrics.trade_events += 1;
            }

            1 => {
                if !trades.is_null(row)
                    || depths.is_null(row)
                    || !trade_targets.is_null(row)
                    || depth_targets.is_null(row)
                {
                    return invalid("invalid combined depth payload");
                }

                let original_levels = source_changes.value_length(row);

                let converted_levels = depth_targets.value_length(row);

                if original_levels != converted_levels {
                    return invalid(format!("combined depth level count mismatch at row {row}"));
                }

                let levels = original_levels as u64;

                metrics.depth_levels += levels;
                metrics.depth_events += 1;

                match status {
                    CONVERSION_STATUS_IDENTITY | CONVERSION_STATUS_CONVERTED => {
                        metrics.depth_levels_converted += levels;
                    }

                    CONVERSION_STATUS_UNAVAILABLE => {
                        metrics.depth_levels_unavailable += levels;
                    }

                    _ => return invalid("invalid combined depth conversion status"),
                }
            }

            _ => return invalid("unknown combined event kind"),
        }

        let reference_start = references.value_offsets()[row] as usize;

        let reference_end = references.value_offsets()[row + 1] as usize;

        match status {
            CONVERSION_STATUS_IDENTITY => {
                metrics.identity += 1;

                if rates.is_null(row)
                    || !failures.is_null(row)
                    || paths.is_null(row)
                    || reference_start != reference_end
                {
                    return invalid("invalid combined identity metadata");
                }
            }

            CONVERSION_STATUS_CONVERTED => {
                metrics.converted += 1;

                if rates.is_null(row)
                    || !failures.is_null(row)
                    || paths.is_null(row)
                    || reference_start == reference_end
                {
                    return invalid("invalid combined converted metadata");
                }
            }

            CONVERSION_STATUS_UNAVAILABLE => {
                metrics.unavailable += 1;

                if !rates.is_null(row)
                    || !paths.is_null(row)
                    || reference_start != reference_end
                    || failures.is_null(row)
                {
                    return invalid("invalid combined unavailable metadata");
                }

                if !matches!(
                    failures.value(row),
                    CONVERSION_FAILURE_MISSING
                        | CONVERSION_FAILURE_STALE
                        | CONVERSION_FAILURE_FUTURE
                        | CONVERSION_FAILURE_IDENTITY_MISMATCH
                        | CONVERSION_FAILURE_ARITHMETIC
                        | CONVERSION_FAILURE_NO_PATH
                ) {
                    return invalid("unknown combined conversion failure");
                }
            }

            _ => return invalid("unknown combined conversion status"),
        }

        // -----------------------------------------------------------------
        // Validate reference chronology
        // -----------------------------------------------------------------

        for index in reference_start..reference_end {
            if reference_timestamps.is_null(index)
                || reference_ranks.is_null(index)
                || reference_tasks.is_null(index)
                || reference_ordinals.is_null(index)
            {
                return invalid("null combined reference ordering key");
            }

            let reference_key = ReferenceKey::new(
                reference_timestamps.value(index),
                reference_ranks.value(index),
                reference_tasks.value(index),
                reference_ordinals.value(index),
            );

            if reference_key >= key {
                return invalid("combined conversion contains future reference");
            }

            let age = i128::from(key.timestamp_ns) - i128::from(reference_key.timestamp_ns);

            if age < 0 || age as u128 > u128::from(max_staleness_ns) {
                return invalid("combined conversion contains stale reference");
            }

            metrics.references_checked += 1;
        }

        metrics.events += 1;
    }

    // ---------------------------------------------------------------------
    // Independent numerical validation
    // ---------------------------------------------------------------------

    let numerical = validate_combined_numeric_batch(batch)?;

    metrics.numerical_trade_values += numerical.trade_values_checked;

    metrics.numerical_depth_levels += numerical.depth_levels_checked;

    Ok(())
}

// -----------------------------------------------------------------------------
// Validate complete combined dataset
// -----------------------------------------------------------------------------

pub fn validate_converted_combined(
    directory: &Path,
    max_staleness_ns: u64,
) -> Result<CombinedValidationMetrics> {
    let files = discover_conversion_files(directory)?;

    let mut metrics = CombinedValidationMetrics::default();
    let mut previous_key = None;

    for path in files {
        let result = validate_combined_file(&path, max_staleness_ns)?;

        if let Some(previous) = previous_key {
            if result.first_key < previous {
                return invalid("combined ordering regression across files");
            }
        }

        previous_key = Some(result.last_key);

        metrics.accumulate(&result.metrics)?;
    }

    if metrics.events != metrics.trade_events + metrics.depth_events {
        return invalid("combined event-kind accounting mismatch");
    }

    if metrics.events != metrics.identity + metrics.converted + metrics.unavailable {
        return invalid("combined conversion status accounting mismatch");
    }

    if metrics.events == 0 {
        return invalid("empty converted combined dataset");
    }

    let accounted_levels = metrics
        .depth_levels_converted
        .checked_add(metrics.depth_levels_unavailable)
        .ok_or_else(|| invalid_error("combined depth level counter overflow"))?;

    if accounted_levels != metrics.depth_levels {
        return invalid("combined depth level accounting mismatch");
    }

    if metrics.numerical_depth_levels != metrics.depth_levels_converted {
        return invalid("combined numerical depth level accounting mismatch");
    }

    Ok(metrics)
}

// -----------------------------------------------------------------------------
// Arrow helpers
// -----------------------------------------------------------------------------

fn column<'a, T: Array + 'static>(batch: &'a RecordBatch, name: &str) -> Result<&'a T> {
    batch
        .column_by_name(name)
        .and_then(|array| array.as_any().downcast_ref::<T>())
        .ok_or_else(|| invalid_error(format!("missing combined column: {name}")))
}

fn struct_column<'a, T: Array + 'static>(array: &'a StructArray, name: &str) -> Result<&'a T> {
    array
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref::<T>())
        .ok_or_else(|| invalid_error(format!("missing combined reference field: {name}")))
}

fn invalid_error(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(message.into())
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(invalid_error(message))
}
