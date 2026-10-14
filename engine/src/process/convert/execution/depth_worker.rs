use std::{
    collections::HashSet,
    path::{Path, PathBuf},
};

use crate::{
    error::{MarketForgeError, Result},
    job::{ConvertJob, MergeParquetConfig},
    process::{
        convert::{
            ConversionPlan, output::ConvertedParquetWriter, reference::ExternalReferenceCursor,
        },
        merge::{PhysicalMergeManifest, UnifiedMergeMode},
    },
};

use super::{
    CONVERSION_READ_BATCH_SIZE, ConversionPartition, ConversionPartitionPlan,
    ConversionPartitionReader, DepthConversionAlignment, DepthConversionKernel,
    DepthReferenceCheckpoint, PhysicalDepthConversionMetrics,
};

#[derive(Debug, Clone)]
pub struct DepthPartitionResult {
    pub partition_id: u32,
    pub start_row: u64,
    pub end_row: u64,
    pub output_directory: PathBuf,
    pub metrics: PhysicalDepthConversionMetrics,
}

pub fn execute_depth_partition(
    job: &ConvertJob,
    source_manifest: &PhysicalMergeManifest,
    reference_manifest: &PhysicalMergeManifest,
    plan: &ConversionPlan,
    partitions: &ConversionPartitionPlan,
    partition: &ConversionPartition,
    checkpoint: &DepthReferenceCheckpoint,
    staging_directory: &Path,
    parquet_config: &MergeParquetConfig,
) -> Result<DepthPartitionResult> {
    job.validate()?;
    source_manifest.validate()?;
    reference_manifest.validate()?;

    if source_manifest.mode != UnifiedMergeMode::Depth {
        return invalid("depth worker requires depth-only source merge");
    }

    if reference_manifest.mode != UnifiedMergeMode::Trades {
        return invalid("depth worker requires trade-only reference merge");
    }

    if checkpoint.partition_id != partition.partition_id
        || checkpoint.start_row != partition.start_row
    {
        return invalid("depth worker checkpoint/partition mismatch");
    }

    if partition.rows == 0 || partition.end_row - partition.start_row != partition.rows {
        return invalid("invalid depth worker partition range");
    }

    let output_directory =
        staging_directory.join(format!("partition-{:06}", partition.partition_id));

    if output_directory.exists() {
        return invalid(format!(
            "depth partition output already exists: {}",
            output_directory.display()
        ));
    }

    // -------------------------------------------------------------------------
    // Restore external trade-reference cursor
    // -------------------------------------------------------------------------

    let reference_directory = job
        .conversion
        .reference_merge
        .as_ref()
        .ok_or_else(|| invalid_error("depth worker requires external reference merge"))?;

    let reference_streams = plan
        .streams
        .iter()
        .flat_map(|stream| &stream.paths)
        .flat_map(|path| &path.edges)
        .map(|edge| edge.reference_stream_id.clone())
        .collect::<HashSet<_>>();

    let cursor = ExternalReferenceCursor::resume(
        reference_directory,
        reference_manifest,
        reference_streams,
        checkpoint.reference_position,
    )?;

    let kernel = DepthConversionKernel::new(job, plan)?;

    let mut alignment =
        DepthConversionAlignment::from_checkpoint(kernel, cursor, checkpoint.state.snapshot());

    // -------------------------------------------------------------------------
    // Read assigned depth row groups
    // -------------------------------------------------------------------------

    let mut reader = ConversionPartitionReader::new(
        &job.input_merge,
        source_manifest,
        partitions,
        partition,
        CONVERSION_READ_BATCH_SIZE,
    )?;

    let mut writer: Option<ConvertedParquetWriter> = None;

    while let Some(batch) = reader.next_batch()? {
        let converted = alignment.convert_batch(&batch)?;

        if writer.is_none() {
            writer = Some(ConvertedParquetWriter::with_mode(
                &output_directory,
                UnifiedMergeMode::Depth,
                converted.schema(),
                parquet_config,
            )?);
        }

        writer
            .as_mut()
            .expect("depth writer initialized")
            .write_batch(&converted)?;
    }

    // -------------------------------------------------------------------------
    // Finalize physical output
    // -------------------------------------------------------------------------

    let mut writer = writer.ok_or_else(|| invalid_error("depth partition produced no output"))?;

    writer.finish()?;

    let conversion = alignment.metrics();
    let physical = writer.metrics();

    let metrics = PhysicalDepthConversionMetrics {
        events_read: conversion.events_read,
        events_written: physical.events_written,

        identity_events: conversion.identity_events,
        converted_events: conversion.converted_events,
        unavailable_events: conversion.unavailable_events,

        levels_read: conversion.levels_read,
        levels_converted: conversion.levels_converted,
        levels_unavailable: conversion.levels_unavailable,

        reference_updates: alignment.reference_updates(),
        reference_observations_read: alignment.observations_read(),

        files_written: physical.files_written,
        row_groups_written: physical.row_groups_written,
    };

    // -------------------------------------------------------------------------
    // Validate worker accounting
    // -------------------------------------------------------------------------

    if metrics.events_read != partition.rows
        || metrics.events_written != partition.rows
        || reader.rows_read() != partition.rows
    {
        return invalid(format!(
            "depth partition {} event count mismatch",
            partition.partition_id
        ));
    }

    let events_accounted = metrics
        .identity_events
        .checked_add(metrics.converted_events)
        .and_then(|value| value.checked_add(metrics.unavailable_events))
        .ok_or_else(|| invalid_error("depth event counter overflow"))?;

    if events_accounted != metrics.events_written {
        return invalid("depth partition status accounting mismatch");
    }

    let levels_accounted = metrics
        .levels_converted
        .checked_add(metrics.levels_unavailable)
        .ok_or_else(|| invalid_error("depth level counter overflow"))?;

    if levels_accounted != metrics.levels_read {
        return invalid("depth partition level accounting mismatch");
    }

    if physical.depth_events != metrics.events_written || physical.trade_events != 0 {
        return invalid("depth partition physical event-kind mismatch");
    }

    Ok(DepthPartitionResult {
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
