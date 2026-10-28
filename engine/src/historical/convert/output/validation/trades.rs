use std::{fs::File, path::Path};

use arrow_array::{
    Array, Decimal256Array, Int64Array, ListArray, RecordBatch, StringArray, UInt8Array,
    UInt32Array, UInt64Array,
};
use arrow_schema::SchemaRef;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::historical::convert::output::schema::{
    CONVERSION_FAILURE_ARITHMETIC, CONVERSION_FAILURE_FUTURE, CONVERSION_FAILURE_IDENTITY_MISMATCH,
    CONVERSION_FAILURE_MISSING, CONVERSION_FAILURE_NO_PATH, CONVERSION_FAILURE_STALE,
    CONVERSION_STATUS_CONVERTED, CONVERSION_STATUS_IDENTITY, CONVERSION_STATUS_UNAVAILABLE,
};
use crate::historical::convert::progress::ConversionProgressReporter;
use {
    crate::historical::convert::ConversionDecimal,
    crate::historical::convert::reference::ReferenceKey, crate::shared::error::MarketForgeError,
    crate::shared::error::Result,
};

pub const CONVERSION_VALIDATION_BATCH_SIZE: usize = 65_536;

#[derive(Debug, Clone, Default)]
pub struct ConversionValidationMetrics {
    pub events: u64,
    pub identity: u64,
    pub converted: u64,
    pub unavailable: u64,

    pub files: u64,
    pub row_groups: u64,

    pub references_checked: u64,
    pub first_timestamp_ns: Option<i64>,
    pub last_timestamp_ns: Option<i64>,
}

pub fn validate_converted_trades(
    directory: &Path,
    max_staleness_ns: u64,
) -> Result<ConversionValidationMetrics> {
    validate_converted_trades_with_progress(
        directory,
        max_staleness_ns,
        &ConversionProgressReporter::disabled(),
    )
}

pub fn validate_converted_trades_with_progress(
    directory: &Path,
    max_staleness_ns: u64,
    reporter: &ConversionProgressReporter,
) -> Result<ConversionValidationMetrics> {
    let files =
        crate::historical::convert::output::shared::files::discover_conversion_files(directory)?;
    reporter.validation_started(files.len() as u64, 0);
    let mut metrics = ConversionValidationMetrics::default();

    let mut expected_schema: Option<SchemaRef> = None;
    let mut previous_key: Option<ReferenceKey> = None;

    for path in files {
        let file = File::open(&path).map_err(|error| invalid_error(error.to_string()))?;

        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|error| invalid_error(error.to_string()))?;

        let schema = builder.schema().clone();

        if let Some(expected) = &expected_schema {
            if expected != &schema {
                return invalid(format!(
                    "converted Parquet schema mismatch: {}",
                    path.display()
                ));
            }
        } else {
            expected_schema = Some(schema);
        }

        metrics.files += 1;
        metrics.row_groups += builder.metadata().num_row_groups() as u64;

        let reader = builder
            .with_batch_size(CONVERSION_VALIDATION_BATCH_SIZE)
            .build()
            .map_err(|error| invalid_error(error.to_string()))?;

        for batch in reader {
            let batch = batch.map_err(|error| invalid_error(error.to_string()))?;

            validate_batch(&batch, max_staleness_ns, &mut previous_key, &mut metrics)?;
        }
        reporter.validation_progress(metrics.files, metrics.events, metrics.row_groups);
    }

    if metrics.events != metrics.identity + metrics.converted + metrics.unavailable {
        return invalid("conversion status accounting mismatch");
    }

    reporter.validation_completed(metrics.files, metrics.events, metrics.row_groups);

    Ok(metrics)
}

fn validate_batch(
    batch: &RecordBatch,
    max_staleness_ns: u64,
    previous_key: &mut Option<ReferenceKey>,
    metrics: &mut ConversionValidationMetrics,
) -> Result<()> {
    let timestamps = column::<Int64Array>(batch, "event_timestamp_ns")?;
    let ranks = column::<UInt32Array>(batch, "stream_rank")?;
    let tasks = column::<UInt64Array>(batch, "task_id")?;
    let ordinals = column::<UInt64Array>(batch, "event_ordinal")?;

    let prices = column::<Decimal256Array>(batch, "price")?;
    let quote_quantities = column::<Decimal256Array>(batch, "quantity_quote")?;

    let converted_prices = column::<Decimal256Array>(batch, "price_target")?;
    let converted_quantities = column::<Decimal256Array>(batch, "quantity_quote_target")?;

    let rates = column::<Decimal256Array>(batch, "conversion_rate")?;

    let statuses = column::<UInt8Array>(batch, "conversion_status")?;

    let failures = column::<UInt8Array>(batch, "conversion_failure")?;

    let targets = column::<StringArray>(batch, "target_quote")?;

    let paths = column::<ListArray>(batch, "conversion_path")?;

    let references = column::<ListArray>(batch, "conversion_references")?;

    for row in 0..batch.num_rows() {
        let key = ReferenceKey::new(
            timestamps.value(row),
            ranks.value(row),
            tasks.value(row),
            ordinals.value(row),
        );

        if let Some(previous) = previous_key {
            if key < *previous {
                return invalid("converted timeline ordering regression");
            }
        }

        *previous_key = Some(key);

        metrics.first_timestamp_ns = Some(metrics.first_timestamp_ns.unwrap_or(key.timestamp_ns));

        metrics.last_timestamp_ns = Some(key.timestamp_ns);
        metrics.events += 1;

        if targets.is_null(row) || targets.value(row).is_empty() {
            return invalid("invalid conversion target currency");
        }

        match statuses.value(row) {
            CONVERSION_STATUS_IDENTITY => {
                metrics.identity += 1;

                if rates.is_null(row)
                    || converted_prices.is_null(row)
                    || !failures.is_null(row)
                    || paths.is_null(row)
                    || references.is_null(row)
                    || references.value_length(row) != 0
                {
                    return invalid("invalid identity conversion fields");
                }

                let rate = ConversionDecimal::new(rates.value(row))?;

                if rate != ConversionDecimal::one() {
                    return invalid("identity conversion rate is not one");
                }

                if converted_prices.value(row) != prices.value(row) {
                    return invalid("identity conversion changed native price");
                }

                if converted_quantities.is_null(row) != quote_quantities.is_null(row) {
                    return invalid("identity quote quantity nullability mismatch");
                }

                if !quote_quantities.is_null(row)
                    && converted_quantities.value(row) != quote_quantities.value(row)
                {
                    return invalid("identity conversion changed quote quantity");
                }
            }

            CONVERSION_STATUS_CONVERTED => {
                metrics.converted += 1;

                if rates.is_null(row)
                    || converted_prices.is_null(row)
                    || !failures.is_null(row)
                    || paths.is_null(row)
                    || references.is_null(row)
                    || references.value_length(row) == 0
                {
                    return invalid("invalid converted trade fields");
                }

                let rate = ConversionDecimal::new(rates.value(row))?;

                if !rate.is_positive() {
                    return invalid("non-positive conversion rate");
                }

                if converted_quantities.is_null(row) != quote_quantities.is_null(row) {
                    return invalid("converted quote quantity nullability mismatch");
                }

                validate_references(references, row, key, max_staleness_ns, metrics)?;
            }

            CONVERSION_STATUS_UNAVAILABLE => {
                metrics.unavailable += 1;

                if !rates.is_null(row)
                    || !converted_prices.is_null(row)
                    || !converted_quantities.is_null(row)
                    || !paths.is_null(row)
                    || references.is_null(row)
                    || references.value_length(row) != 0
                    || failures.is_null(row)
                {
                    return invalid("invalid unavailable conversion fields");
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
                    return invalid("unknown conversion failure code");
                }
            }

            _ => return invalid("unknown conversion status"),
        }
    }

    Ok(())
}

fn validate_references(
    references: &ListArray,
    row: usize,
    source_key: ReferenceKey,
    max_staleness_ns: u64,
    metrics: &mut ConversionValidationMetrics,
) -> Result<()> {
    let values = references.value(row);

    let records = values
        .as_any()
        .downcast_ref::<arrow_array::StructArray>()
        .ok_or_else(|| invalid_error("invalid reference provenance structure"))?;

    let timestamps = column_struct::<Int64Array>(records, "event_timestamp_ns")?;

    let ranks = column_struct::<UInt32Array>(records, "stream_rank")?;

    let tasks = column_struct::<UInt64Array>(records, "task_id")?;

    let ordinals = column_struct::<UInt64Array>(records, "event_ordinal")?;

    let prices = column_struct::<Decimal256Array>(records, "reference_price")?;

    for index in 0..records.len() {
        let reference_key = ReferenceKey::new(
            timestamps.value(index),
            ranks.value(index),
            tasks.value(index),
            ordinals.value(index),
        );

        if reference_key >= source_key {
            return invalid("reference observation does not precede source event");
        }

        let age = i128::from(source_key.timestamp_ns) - i128::from(reference_key.timestamp_ns);

        if age < 0 || age as u128 > u128::from(max_staleness_ns) {
            return invalid("reference observation exceeds staleness limit");
        }

        let price = ConversionDecimal::new(prices.value(index))?;

        if !price.is_positive() {
            return invalid("non-positive reference price");
        }

        metrics.references_checked += 1;
    }

    Ok(())
}

fn column<'a, T: Array + 'static>(batch: &'a RecordBatch, name: &str) -> Result<&'a T> {
    batch
        .column_by_name(name)
        .and_then(|array| array.as_any().downcast_ref::<T>())
        .ok_or_else(|| invalid_error(format!("missing or invalid conversion column: {name}")))
}

fn column_struct<'a, T: Array + 'static>(
    batch: &'a arrow_array::StructArray,
    name: &str,
) -> Result<&'a T> {
    batch
        .column_by_name(name)
        .and_then(|array| array.as_any().downcast_ref::<T>())
        .ok_or_else(|| invalid_error(format!("invalid reference field: {name}")))
}

fn invalid_error(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(message.into())
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(invalid_error(message))
}
