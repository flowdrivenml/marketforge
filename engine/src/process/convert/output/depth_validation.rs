use super::depth_numeric::validate_depth_numeric_batch;
use super::files::discover_conversion_files;
use crate::process::convert::reference::ReferenceKey;
use crate::{
    error::{MarketForgeError, Result},
    process::convert::output::{
        CONVERSION_STATUS_CONVERTED, CONVERSION_STATUS_IDENTITY, CONVERSION_STATUS_UNAVAILABLE,
    },
};
use arrow_array::UInt32Array;
use arrow_array::UInt64Array;
use arrow_array::{
    Array, Decimal256Array, Int64Array, ListArray, RecordBatch, StructArray, UInt8Array,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::{fs::File, path::Path};
// -----------------------------------------------------------------------------
// Validation metrics
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct DepthConversionValidationMetrics {
    pub events: u64,

    pub identity: u64,
    pub converted: u64,
    pub unavailable: u64,

    pub levels: u64,
    pub levels_converted: u64,
    pub levels_unavailable: u64,

    pub references_checked: u64,

    pub files: u64,
    pub row_groups: u64,
    pub numerical_levels_checked: u64,
    pub numerical_references_checked: u64,
}

pub struct DepthFileValidationResult {
    pub metrics: DepthConversionValidationMetrics,
    pub first_key: ReferenceKey,
    pub last_key: ReferenceKey,
}

impl DepthConversionValidationMetrics {
    pub fn accumulate(&mut self, other: &Self) -> Result<()> {
        macro_rules! add {
            ($field:ident) => {
                self.$field = self.$field.checked_add(other.$field).ok_or_else(|| {
                    invalid_error(concat!(
                        "depth validation metric overflow: ",
                        stringify!($field)
                    ))
                })?;
            };
        }

        add!(events);

        add!(identity);
        add!(converted);
        add!(unavailable);

        add!(levels);
        add!(levels_converted);
        add!(levels_unavailable);

        add!(references_checked);

        add!(files);
        add!(row_groups);

        add!(numerical_levels_checked);
        add!(numerical_references_checked);

        Ok(())
    }
}
// -----------------------------------------------------------------------------
// Validate physical depth conversion
// -----------------------------------------------------------------------------

pub fn validate_converted_depth(
    directory: &Path,
    max_staleness_ns: u64,
) -> Result<DepthConversionValidationMetrics> {
    if max_staleness_ns == 0 {
        return invalid("depth conversion staleness limit must be positive");
    }

    let files = discover_conversion_files(directory)?;
    let mut metrics = DepthConversionValidationMetrics::default();
    let mut previous_key = None;

    for path in files {
        let result = validate_depth_file(&path, max_staleness_ns)?;

        if let Some(previous) = previous_key {
            if result.first_key < previous {
                return invalid("converted depth ordering regressed across files");
            }
        }

        previous_key = Some(result.last_key);
        metrics.accumulate(&result.metrics)?;
    }

    reconcile_depth_validation(&metrics)?;

    Ok(metrics)
}
// -----------------------------------------------------------------------------
// Validate Arrow batch
// -----------------------------------------------------------------------------

fn validate_batch(
    batch: &RecordBatch,
    max_staleness_ns: u64,
    previous_key: &mut Option<ReferenceKey>,
    metrics: &mut DepthConversionValidationMetrics,
) -> Result<()> {
    if batch.num_columns() != 24 {
        return invalid("invalid converted depth column count");
    }

    let timestamps = column::<Int64Array>(batch, "event_timestamp_ns")?;
    let ranks = column::<UInt32Array>(batch, "stream_rank")?;
    let tasks = column::<UInt64Array>(batch, "task_id")?;
    let ordinals = column::<UInt64Array>(batch, "event_ordinal")?;
    let changes = column::<ListArray>(batch, "changes")?;

    let target = column::<ListArray>(batch, "changes_target")?;

    let statuses = column::<UInt8Array>(batch, "conversion_status")?;

    let references = column::<ListArray>(batch, "conversion_references")?;

    // -------------------------------------------------------------------------
    // Validate nested offsets
    // -------------------------------------------------------------------------

    if changes.value_offsets() != target.value_offsets() {
        return invalid("converted depth list offsets differ from source");
    }

    let levels = target
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| invalid_error("invalid converted depth level structure"))?;

    let prices = levels
        .column_by_name("price_target")
        .and_then(|array| array.as_any().downcast_ref::<Decimal256Array>())
        .ok_or_else(|| invalid_error("missing converted depth prices"))?;

    let quantities = levels
        .column_by_name("quantity_quote_target")
        .and_then(|array| array.as_any().downcast_ref::<Decimal256Array>())
        .ok_or_else(|| invalid_error("missing converted depth quantities"))?;

    let reference_values = references
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| invalid_error("invalid depth conversion references"))?;

    let reference_timestamps = reference_values
        .column_by_name("event_timestamp_ns")
        .and_then(|array| array.as_any().downcast_ref::<Int64Array>())
        .ok_or_else(|| invalid_error("missing reference timestamps"))?;

    // -------------------------------------------------------------------------
    // Validate events
    // -------------------------------------------------------------------------

    for row in 0..batch.num_rows() {
        if timestamps.is_null(row)
            || changes.is_null(row)
            || target.is_null(row)
            || statuses.is_null(row)
            || references.is_null(row)
        {
            return invalid("null required depth conversion field");
        }

        let timestamp = timestamps.value(row);

        if ranks.is_null(row) || tasks.is_null(row) || ordinals.is_null(row) {
            return invalid("null depth ordering field");
        }

        let key = ReferenceKey::new(
            timestamp,
            ranks.value(row),
            tasks.value(row),
            ordinals.value(row),
        );

        if let Some(previous) = previous_key {
            if key < *previous {
                return invalid("converted depth ordering key regressed");
            }
        }

        *previous_key = Some(key);

        let start = usize::try_from(target.value_offsets()[row])
            .map_err(|_| invalid_error("invalid depth list offset"))?;

        let end = usize::try_from(target.value_offsets()[row + 1])
            .map_err(|_| invalid_error("invalid depth list offset"))?;

        let level_count = end - start;

        let reference_start = references.value_offsets()[row] as usize;

        let reference_end = references.value_offsets()[row + 1] as usize;

        for index in reference_start..reference_end {
            let reference_timestamp = reference_timestamps.value(index);

            if reference_timestamp >= timestamp {
                return invalid("future depth conversion reference");
            }

            let age = i128::from(timestamp) - i128::from(reference_timestamp);

            if age < 0 || age as u128 > u128::from(max_staleness_ns) {
                return invalid("stale depth conversion reference");
            }

            metrics.references_checked += 1;
        }

        match statuses.value(row) {
            CONVERSION_STATUS_IDENTITY => {
                metrics.identity += 1;
                metrics.levels_converted += level_count as u64;

                for index in start..end {
                    if prices.is_null(index) {
                        return invalid("identity depth price is null");
                    }
                }
            }

            CONVERSION_STATUS_CONVERTED => {
                metrics.converted += 1;
                metrics.levels_converted += level_count as u64;

                if reference_start == reference_end {
                    return invalid("converted depth event has no reference provenance");
                }

                for index in start..end {
                    if prices.is_null(index) {
                        return invalid("converted depth price is null");
                    }
                }
            }

            CONVERSION_STATUS_UNAVAILABLE => {
                metrics.unavailable += 1;
                metrics.levels_unavailable += level_count as u64;

                for index in start..end {
                    if !prices.is_null(index) || !quantities.is_null(index) {
                        return invalid("unavailable depth event contains converted values");
                    }
                }
            }

            _ => return invalid("invalid depth conversion status"),
        }

        metrics.levels += level_count as u64;
        metrics.events += 1;
    }
    let numerical = validate_depth_numeric_batch(batch)?;

    metrics.numerical_levels_checked = metrics
        .numerical_levels_checked
        .checked_add(numerical.levels_checked)
        .ok_or_else(|| invalid_error("numerical level counter overflow"))?;

    metrics.numerical_references_checked = metrics
        .numerical_references_checked
        .checked_add(numerical.references_checked)
        .ok_or_else(|| invalid_error("numerical reference counter overflow"))?;
    Ok(())
}

pub fn validate_depth_file(
    path: &Path,
    max_staleness_ns: u64,
) -> Result<DepthFileValidationResult> {
    if max_staleness_ns == 0 {
        return invalid("depth conversion staleness limit must be positive");
    }

    let file = File::open(path).map_err(|error| {
        invalid_error(format!(
            "cannot open converted depth Parquet {}: {error}",
            path.display()
        ))
    })?;

    let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|error| {
        invalid_error(format!(
            "cannot inspect converted depth Parquet {}: {error}",
            path.display()
        ))
    })?;

    let row_groups = builder.metadata().num_row_groups() as u64;

    let mut reader = builder.with_batch_size(4_096).build().map_err(|error| {
        invalid_error(format!("cannot construct depth validation reader: {error}"))
    })?;

    let mut metrics = DepthConversionValidationMetrics::default();

    metrics.files = 1;
    metrics.row_groups = row_groups;

    let mut previous_key = None;
    let mut first_key = None;

    while let Some(batch) = reader.next() {
        let batch = batch.map_err(|error| {
            invalid_error(format!("cannot read converted depth batch: {error}"))
        })?;

        if batch.num_rows() == 0 {
            continue;
        }
        let timestamps = column::<Int64Array>(&batch, "event_timestamp_ns")?;
        let ranks = column::<UInt32Array>(&batch, "stream_rank")?;
        let tasks = column::<UInt64Array>(&batch, "task_id")?;
        let ordinals = column::<UInt64Array>(&batch, "event_ordinal")?;

        if first_key.is_none() {
            if timestamps.is_null(0) || ranks.is_null(0) || tasks.is_null(0) || ordinals.is_null(0)
            {
                return invalid("null first depth ordering key");
            }

            first_key = Some(ReferenceKey::new(
                timestamps.value(0),
                ranks.value(0),
                tasks.value(0),
                ordinals.value(0),
            ));
        }

        validate_batch(&batch, max_staleness_ns, &mut previous_key, &mut metrics)?;
    }

    let first_key = first_key.ok_or_else(|| invalid_error("empty converted depth file"))?;

    let last_key = previous_key.ok_or_else(|| invalid_error("missing final depth ordering key"))?;

    Ok(DepthFileValidationResult {
        metrics,
        first_key,
        last_key,
    })
}

pub(crate) fn reconcile_depth_validation(metrics: &DepthConversionValidationMetrics) -> Result<()> {
    let events = metrics
        .identity
        .checked_add(metrics.converted)
        .and_then(|value| value.checked_add(metrics.unavailable))
        .ok_or_else(|| invalid_error("depth event counter overflow"))?;

    if events != metrics.events {
        return invalid("depth conversion event accounting mismatch");
    }

    let levels = metrics
        .levels_converted
        .checked_add(metrics.levels_unavailable)
        .ok_or_else(|| invalid_error("depth level counter overflow"))?;

    if levels != metrics.levels {
        return invalid("depth conversion level accounting mismatch");
    }

    if metrics.events == 0 {
        return invalid("converted depth dataset is empty");
    }

    if metrics.numerical_levels_checked != metrics.levels_converted {
        return invalid("depth numerical level accounting mismatch");
    }

    if metrics.numerical_references_checked != metrics.references_checked {
        return invalid("depth numerical reference accounting mismatch");
    }

    Ok(())
}

// -----------------------------------------------------------------------------
// Arrow column access
// -----------------------------------------------------------------------------

fn column<'a, T: Array + 'static>(batch: &'a RecordBatch, name: &str) -> Result<&'a T> {
    batch
        .column_by_name(name)
        .and_then(|array| array.as_any().downcast_ref::<T>())
        .ok_or_else(|| {
            invalid_error(format!(
                "missing or invalid depth conversion column: {name}"
            ))
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
