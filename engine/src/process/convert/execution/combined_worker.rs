use std::path::{Path, PathBuf};

use crate::{
    error::{MarketForgeError, Result},
    job::{ConvertJob, MergeParquetConfig},
    process::{
        convert::{
            ConversionPlan,
            output::{ConvertedCombinedBatchBuilder, ConvertedParquetWriter},
            reference::ExternalReferenceCursor,
        },
        merge::{PhysicalMergeManifest, UnifiedMergeMode},
    },
};

use super::{
    CONVERSION_READ_BATCH_SIZE, CombinedConversionKernel, CombinedConversionMetrics,
    CombinedReferenceAlignment, CombinedReferenceCheckpoint, CombinedReferenceSelection,
    ConversionPartition, ConversionPartitionPlan, ConversionPartitionReader,
    PhysicalCombinedConversionMetrics,
};

#[derive(Debug, Clone)]
pub struct CombinedPartitionResult {
    pub partition_id: u32,
    pub start_row: u64,
    pub end_row: u64,
    pub output_directory: PathBuf,
    pub metrics: PhysicalCombinedConversionMetrics,
}

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
    }

    // -------------------------------------------------------------
    // Finalize physical output
    // -------------------------------------------------------------

    let mut writer =
        writer.ok_or_else(|| invalid_error("combined partition produced no output"))?;

    writer.finish()?;

    let physical = writer.metrics();

    let metrics = PhysicalCombinedConversionMetrics {
        events_read: conversion_metrics.events_read,
        events_written: physical.events_written,

        trade_events: conversion_metrics.trade_events,
        depth_events: conversion_metrics.depth_events,

        identity_events: conversion_metrics.trade.identity_events
            + conversion_metrics.depth.identity_events,

        converted_events: conversion_metrics.trade.converted_events
            + conversion_metrics.depth.converted_events,

        unavailable_events: conversion_metrics.trade.unavailable_events
            + conversion_metrics.depth.unavailable_events,

        depth_levels_read: conversion_metrics.depth.levels_read,
        depth_levels_converted: conversion_metrics.depth.levels_converted,
        depth_levels_unavailable: conversion_metrics.depth.levels_unavailable,

        internal_reference_updates: alignment.internal_updates(),
        external_reference_updates: alignment.external_updates(),
        external_observations_read: alignment.observations_read(),

        files_written: physical.files_written,
        row_groups_written: physical.row_groups_written,
    };

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

    if metrics.identity_events + metrics.converted_events + metrics.unavailable_events
        != partition.rows
    {
        return invalid("combined partition conversion status mismatch");
    }

    if metrics.depth_levels_converted + metrics.depth_levels_unavailable
        != metrics.depth_levels_read
    {
        return invalid("combined partition depth-level accounting mismatch");
    }

    if physical.trade_events != metrics.trade_events
        || physical.depth_events != metrics.depth_events
    {
        return invalid("combined partition physical event-kind mismatch");
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
