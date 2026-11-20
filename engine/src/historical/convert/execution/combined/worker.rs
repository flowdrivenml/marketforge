use crate::historical::convert::execution::{
    CONVERSION_READ_BATCH_SIZE, CombinedConversionKernel, CombinedConversionMetrics,
    CombinedReferenceAlignment, CombinedReferenceCheckpoint, CombinedReferenceSelection,
    ConversionPartition, ConversionPartitionPlan, ConversionPartitionReader,
    PhysicalCombinedConversionMetrics,
};
use crate::historical::convert::progress::ConversionWorkerProgress;
use std::path::{Path, PathBuf};
use {
    crate::historical::convert::ConversionPlan,
    crate::historical::convert::output::ConvertedCombinedBatchBuilder,
    crate::historical::convert::output::ConvertedParquetWriter,
    crate::historical::convert::reference::ExternalReferenceCursor,
    crate::historical::job::ConvertJob, crate::historical::job::MergeParquetConfig,
    crate::historical::merge::PhysicalMergeManifest, crate::historical::merge::UnifiedMergeMode,
    crate::shared::error::MarketForgeError, crate::shared::error::Result,
};

#[derive(Debug, Clone)]
pub struct CombinedPartitionResult {
    pub partition_id: u32,
    pub start_row: u64,
    pub end_row: u64,
    pub output_directory: PathBuf,
    pub metrics: PhysicalCombinedConversionMetrics,
}

#[allow(clippy::too_many_arguments)]
pub fn execute_combined_partition(
    job: &ConvertJob,
    source_manifest: &PhysicalMergeManifest,
    reference_manifest: Option<&PhysicalMergeManifest>,
    plan: &ConversionPlan,
    partitions: &ConversionPartitionPlan,
    partition: &ConversionPartition,
    checkpoint: &CombinedReferenceCheckpoint,
    selection: &CombinedReferenceSelection,
    staging_directory: &Path,
    parquet_config: &MergeParquetConfig,
) -> Result<CombinedPartitionResult> {
    execute_combined_partition_impl(
        job,
        source_manifest,
        reference_manifest,
        plan,
        partitions,
        partition,
        checkpoint,
        selection,
        staging_directory,
        parquet_config,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn execute_combined_partition_with_progress(
    job: &ConvertJob,
    source_manifest: &PhysicalMergeManifest,
    reference_manifest: Option<&PhysicalMergeManifest>,
    plan: &ConversionPlan,
    partitions: &ConversionPartitionPlan,
    partition: &ConversionPartition,
    checkpoint: &CombinedReferenceCheckpoint,
    selection: &CombinedReferenceSelection,
    staging_directory: &Path,
    parquet_config: &MergeParquetConfig,
    progress: &mut ConversionWorkerProgress,
) -> Result<CombinedPartitionResult> {
    execute_combined_partition_impl(
        job,
        source_manifest,
        reference_manifest,
        plan,
        partitions,
        partition,
        checkpoint,
        selection,
        staging_directory,
        parquet_config,
        Some(progress),
    )
}

#[allow(clippy::too_many_arguments)]
fn execute_combined_partition_impl(
    job: &ConvertJob,
    source_manifest: &PhysicalMergeManifest,
    reference_manifest: Option<&PhysicalMergeManifest>,
    plan: &ConversionPlan,
    partitions: &ConversionPartitionPlan,
    partition: &ConversionPartition,
    checkpoint: &CombinedReferenceCheckpoint,
    selection: &CombinedReferenceSelection,
    staging_directory: &Path,
    parquet_config: &MergeParquetConfig,
    mut progress: Option<&mut ConversionWorkerProgress>,
) -> Result<CombinedPartitionResult> {
    job.validate()?;
    source_manifest.validate()?;

    if source_manifest.mode != UnifiedMergeMode::Combined {
        return invalid("combined worker requires combined source");
    }

    if checkpoint.partition_id != partition.partition_id
        || checkpoint.start_row != partition.start_row
    {
        return invalid("combined worker checkpoint/partition mismatch");
    }

    if partition.rows == 0 || partition.end_row - partition.start_row != partition.rows {
        return invalid("invalid combined partition range");
    }

    let output_directory =
        staging_directory.join(format!("partition-{:06}", partition.partition_id));

    if output_directory.exists() {
        return invalid(format!(
            "combined partition output already exists: {}",
            output_directory.display()
        ));
    }

    // -------------------------------------------------------------
    // Restore external reference cursor
    // -------------------------------------------------------------

    let external = if selection.external_streams.is_empty() {
        if checkpoint.external_position.is_some() {
            return invalid("unexpected external checkpoint position");
        }

        None
    } else {
        let directory = job
            .conversion
            .reference_merge
            .as_ref()
            .ok_or_else(|| invalid_error("missing external reference directory"))?;

        let manifest = reference_manifest
            .ok_or_else(|| invalid_error("missing external reference manifest"))?;

        let position = checkpoint
            .external_position
            .ok_or_else(|| invalid_error("missing external checkpoint position"))?;

        Some(ExternalReferenceCursor::resume(
            directory,
            manifest,
            selection.external_streams.iter().cloned(),
            position,
        )?)
    };

    // -------------------------------------------------------------
    // Restore historical reference state
    // -------------------------------------------------------------

    let mut alignment = CombinedReferenceAlignment::from_checkpoint(
        selection.clone(),
        external,
        checkpoint.state.snapshot(),
    )?;

    let kernel = CombinedConversionKernel::new(job, plan)?;

    let mut conversion_metrics = CombinedConversionMetrics::default();

    // -------------------------------------------------------------
    // Read assigned partition
    // -------------------------------------------------------------

    let mut reader = ConversionPartitionReader::new(
        &job.input_merge,
        source_manifest,
        partitions,
        partition,
        CONVERSION_READ_BATCH_SIZE,
    )?;

    let mut builder: Option<ConvertedCombinedBatchBuilder> = None;
    let mut writer: Option<ConvertedParquetWriter> = None;

    while let Some(batch) = reader.next_batch()? {
        if let Some(progress) = &progress {
            progress.check_interrupted()?;
        }
        if builder.is_none() {
            let combined_builder = ConvertedCombinedBatchBuilder::new(batch.schema())?;

            let parquet_writer = ConvertedParquetWriter::with_mode(
                &output_directory,
                UnifiedMergeMode::Combined,
                combined_builder.output_schema(),
                parquet_config,
            )?;

            builder = Some(combined_builder);
            writer = Some(parquet_writer);
        }

        let rows = kernel.convert_batch(&batch, &mut alignment, &mut conversion_metrics)?;

        let converted = builder
            .as_ref()
            .expect("combined builder initialized")
            .build(&batch, &rows)?;

        writer
            .as_mut()
            .expect("combined writer initialized")
            .write_batch(&converted)?;
        if let Some(progress) = progress.as_deref_mut() {
            let physical = writer
                .as_ref()
                .expect("combined writer initialized")
                .metrics();

            progress.maybe_update(
                &super::materialize::execution_metrics(&conversion_metrics, &alignment, physical)?
                    .progress(),
            );
        }
    }

    // -------------------------------------------------------------
    // Finalize physical output
    // -------------------------------------------------------------

    let mut writer =
        writer.ok_or_else(|| invalid_error("combined partition produced no output"))?;

    writer.finish()?;

    let physical = writer.metrics();

    let metrics = super::materialize::execution_metrics(&conversion_metrics, &alignment, physical)?;

    // -------------------------------------------------------------
    // Validate partition accounting
    // -------------------------------------------------------------

    if metrics.events_read != partition.rows
        || metrics.events_written != partition.rows
        || reader.rows_read() != partition.rows
    {
        return invalid(format!(
            "combined partition {} event count mismatch",
            partition.partition_id
        ));
    }

    if metrics.trade_events + metrics.depth_events != partition.rows {
        return invalid("combined partition event-kind accounting mismatch");
    }

    let accounted = metrics
        .identity_events
        .checked_add(metrics.converted_events)
        .and_then(|value| value.checked_add(metrics.unavailable_events))
        .and_then(|value| value.checked_add(metrics.skipped_events))
        .ok_or_else(|| invalid_error("combined partition status counter overflow"))?;

    if accounted != partition.rows {
        return invalid(format!(
            "combined partition conversion status mismatch: \
         partition={}, expected={}, identity={}, converted={}, unavailable={}, skipped={}, accounted={}",
            partition.partition_id,
            partition.rows,
            metrics.identity_events,
            metrics.converted_events,
            metrics.unavailable_events,
            metrics.skipped_events,
            accounted,
        ));
    }

    let accounted_levels = metrics
        .depth_levels_converted
        .checked_add(metrics.depth_levels_unavailable)
        .and_then(|value| value.checked_add(metrics.depth_levels_skipped))
        .ok_or_else(|| invalid_error("combined partition depth counter overflow"))?;

    if accounted_levels != metrics.depth_levels_read {
        return invalid(format!(
            "combined partition depth-level accounting mismatch: \
         partition={}, read={}, converted={}, unavailable={}, skipped={}",
            partition.partition_id,
            metrics.depth_levels_read,
            metrics.depth_levels_converted,
            metrics.depth_levels_unavailable,
            metrics.depth_levels_skipped,
        ));
    }

    if physical.trade_events != metrics.trade_events
        || physical.depth_events != metrics.depth_events
    {
        return invalid("combined partition physical event-kind mismatch");
    }
    if let Some(progress) = progress {
        progress.finish(&metrics.progress());
    }
    Ok(CombinedPartitionResult {
        partition_id: partition.partition_id,
        start_row: partition.start_row,
        end_row: partition.end_row,
        output_directory,
        metrics,
    })
}

fn invalid_error(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(message.into())
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(invalid_error(message))
}
