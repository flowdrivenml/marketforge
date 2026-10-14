use std::path::Path;

use crate::{
    error::{MarketForgeError, Result},
    job::{ConvertJob, MergeParquetConfig},
    process::{
        convert::{
            ConversionPlan,
            output::{ConvertedParquetWriter, ConvertedTradeBatchBuilder},
            reference::ReferenceState,
        },
        merge::PhysicalMergeManifest,
    },
};

use super::{CONVERSION_READ_BATCH_SIZE, PhysicalConversionReader, TradeConversionKernel};

// -----------------------------------------------------------------------------
// Metrics
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct PhysicalConversionMetrics {
    pub events_read: u64,
    pub events_written: u64,

    pub identity_events: u64,
    pub converted_events: u64,
    pub unavailable_events: u64,

    pub reference_updates: u64,

    pub files_written: u64,
    pub row_groups_written: u64,
}

// -----------------------------------------------------------------------------
// Physical conversion
// -----------------------------------------------------------------------------

pub fn materialize_trade_conversion(
    job: &ConvertJob,
    manifest: &PhysicalMergeManifest,
    plan: &ConversionPlan,
    staging_directory: &Path,
    parquet_config: &MergeParquetConfig,
) -> Result<PhysicalConversionMetrics> {
    job.validate()?;
    manifest.validate()?;

    if !matches!(
        manifest.mode,
        crate::process::merge::UnifiedMergeMode::Trades
    ) {
        return invalid("physical trade conversion requires trades merge");
    }

    if staging_directory.exists() {
        return invalid(format!(
            "conversion staging directory already exists: {}",
            staging_directory.display()
        ));
    }

    let mut reader =
        PhysicalConversionReader::new(&job.input_merge, manifest, CONVERSION_READ_BATCH_SIZE)?;

    let kernel = TradeConversionKernel::new(job, plan)?;

    let mut state = ReferenceState::new();
    let mut metrics = PhysicalConversionMetrics::default();

    let mut builder = None;
    let mut writer = None;

    while let Some(batch) = reader.next_batch()? {
        if builder.is_none() {
            let trade_builder = ConvertedTradeBatchBuilder::new(batch.schema())?;

            let parquet_writer = ConvertedParquetWriter::new(
                staging_directory,
                trade_builder.output_schema(),
                parquet_config,
            )?;

            builder = Some(trade_builder);
            writer = Some(parquet_writer);
        }

        // -------------------------------------------------------------
        // Shared conversion logic
        // -------------------------------------------------------------

        let rows = kernel.convert_batch(&batch, &mut state, &mut metrics)?;

        // -------------------------------------------------------------
        // Construct converted Arrow batch
        // -------------------------------------------------------------

        let output = builder
            .as_ref()
            .expect("conversion builder initialized")
            .build(&batch, &rows)?;

        // -------------------------------------------------------------
        // Write converted Parquet
        // -------------------------------------------------------------

        writer
            .as_mut()
            .expect("conversion writer initialized")
            .write_batch(&output)?;
    }

    if let Some(writer) = writer.as_mut() {
        writer.finish()?;

        metrics.events_written = writer.metrics().events_written;

        metrics.files_written = writer.metrics().files_written;

        metrics.row_groups_written = writer.metrics().row_groups_written;
    }

    if metrics.events_read != manifest.events_written
        || metrics.events_written != manifest.events_written
    {
        return invalid("physical conversion event count mismatch");
    }

    let accounted = metrics
        .identity_events
        .checked_add(metrics.converted_events)
        .and_then(|value| value.checked_add(metrics.unavailable_events))
        .ok_or_else(|| invalid_error("conversion status counter overflow"))?;

    if accounted != metrics.events_written {
        return invalid("physical conversion status accounting mismatch");
    }

    Ok(metrics)
}

fn invalid_error(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(message.into())
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(invalid_error(message))
}
