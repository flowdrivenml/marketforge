use std::path::{Path, PathBuf};

use crate::{
    error::{MarketForgeError, Result},
    job::{ConvertJob, MergeParquetConfig},
    process::{
        convert::{
            ConversionPlan,
            output::{ConvertedParquetWriter, ConvertedTradeBatchBuilder},
        },
        merge::{PhysicalMergeManifest, UnifiedMergeMode},
    },
};

use super::{
    CONVERSION_READ_BATCH_SIZE, ConversionPartition, ConversionPartitionPlan,
    ConversionPartitionReader, ConversionReferenceCheckpoint, PhysicalConversionMetrics,
    TradeConversionKernel,
};

// -----------------------------------------------------------------------------
// Worker result
// -----------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct ConversionPartitionResult {
    pub partition_id: u32,

    pub start_row: u64,
    pub end_row: u64,

    pub output_directory: PathBuf,

    pub metrics: PhysicalConversionMetrics,
}

// -----------------------------------------------------------------------------
// Execute one conversion partition
// -----------------------------------------------------------------------------

pub fn execute_conversion_partition(
    job: &ConvertJob,
    manifest: &PhysicalMergeManifest,
    plan: &ConversionPlan,
    partitions: &ConversionPartitionPlan,
    partition: &ConversionPartition,
    checkpoint: &ConversionReferenceCheckpoint,
    staging_directory: &Path,
    parquet_config: &MergeParquetConfig,
) -> Result<ConversionPartitionResult> {
    job.validate()?;
    manifest.validate()?;

    // -------------------------------------------------------------------------
    // Validate input
    // -------------------------------------------------------------------------

    if !matches!(manifest.mode, UnifiedMergeMode::Trades) {
        return invalid("parallel conversion requires a trade-only physical merge");
    }

    if checkpoint.partition_id != partition.partition_id {
        return invalid("conversion worker checkpoint partition mismatch");
    }

    if checkpoint.events_before_partition != partition.start_row {
        return invalid("conversion worker checkpoint boundary mismatch");
    }

    if partition.rows == 0 {
        return invalid("conversion worker received empty partition");
    }

    if partition.end_row - partition.start_row != partition.rows {
        return invalid("conversion worker partition row count mismatch");
    }

    // -------------------------------------------------------------------------
    // Prepare output directory
    // -------------------------------------------------------------------------

    let output_directory =
        staging_directory.join(format!("partition-{:06}", partition.partition_id));

    if output_directory.exists() {
        return invalid(format!(
            "conversion partition output already exists: {}",
            output_directory.display()
        ));
    }

    // -------------------------------------------------------------------------
    // Initialize partition reader
    // -------------------------------------------------------------------------

    let mut reader = ConversionPartitionReader::new(
        &job.input_merge,
        manifest,
        partitions,
        partition,
        CONVERSION_READ_BATCH_SIZE,
    )?;

    // -------------------------------------------------------------------------
    // Initialize conversion state
    // -------------------------------------------------------------------------

    let kernel = TradeConversionKernel::new(job, plan)?;

    let mut state = checkpoint.state.snapshot();

    let mut metrics = PhysicalConversionMetrics::default();

    let mut builder = None;
    let mut writer = None;

    // -------------------------------------------------------------------------
    // Process assigned row groups
    // -------------------------------------------------------------------------

    while let Some(batch) = reader.next_batch()? {
        if builder.is_none() {
            let trade_builder = ConvertedTradeBatchBuilder::new(batch.schema())?;

            let parquet_writer = ConvertedParquetWriter::new(
                &output_directory,
                trade_builder.output_schema(),
                parquet_config,
            )?;

            builder = Some(trade_builder);
            writer = Some(parquet_writer);
        }

        // -------------------------------------------------------------
        // Shared financial conversion kernel
        // -------------------------------------------------------------

        let rows = kernel.convert_batch(&batch, &mut state, &mut metrics)?;

        // -------------------------------------------------------------
        // Build converted Arrow batch
        // -------------------------------------------------------------

        let output = builder
            .as_ref()
            .expect("conversion builder initialized")
            .build(&batch, &rows)?;

        // -------------------------------------------------------------
        // Write partition-specific Parquet
        // -------------------------------------------------------------

        writer
            .as_mut()
            .expect("conversion writer initialized")
            .write_batch(&output)?;
    }

    // -------------------------------------------------------------------------
    // Finalize writer
    // -------------------------------------------------------------------------

    if let Some(writer) = writer.as_mut() {
        writer.finish()?;

        metrics.events_written = writer.metrics().events_written;

        metrics.files_written = writer.metrics().files_written;

        metrics.row_groups_written = writer.metrics().row_groups_written;
    }

    // -------------------------------------------------------------------------
    // Validate worker accounting
    // -------------------------------------------------------------------------

    if metrics.events_read != partition.rows {
        return invalid(format!(
            "conversion partition {} read count mismatch: \
             expected {}, received {}",
            partition.partition_id, partition.rows, metrics.events_read,
        ));
    }

    if metrics.events_written != partition.rows {
        return invalid(format!(
            "conversion partition {} write count mismatch: \
             expected {}, received {}",
            partition.partition_id, partition.rows, metrics.events_written,
        ));
    }

    if reader.rows_read() != partition.rows {
        return invalid("conversion partition reader accounting mismatch");
    }

    let accounted = metrics
        .identity_events
        .checked_add(metrics.converted_events)
        .and_then(|value| value.checked_add(metrics.unavailable_events))
        .ok_or_else(|| invalid_error("conversion partition status counter overflow"))?;

    if accounted != metrics.events_written {
        return invalid("conversion partition status accounting mismatch");
    }

    if metrics.reference_updates > metrics.events_read {
        return invalid("conversion partition reference updates exceed events");
    }

    Ok(ConversionPartitionResult {
        partition_id: partition.partition_id,
        start_row: partition.start_row,
        end_row: partition.end_row,
        output_directory,
        metrics,
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
