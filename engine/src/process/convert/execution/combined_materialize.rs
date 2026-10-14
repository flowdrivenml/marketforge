use std::path::Path;

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
    CombinedReferenceAlignment, CombinedReferenceSelection, PhysicalConversionReader,
};

// -----------------------------------------------------------------------------
// Physical combined conversion metrics
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct PhysicalCombinedConversionMetrics {
    pub events_read: u64,
    pub events_written: u64,

    pub trade_events: u64,
    pub depth_events: u64,

    pub identity_events: u64,
    pub converted_events: u64,
    pub unavailable_events: u64,

    pub depth_levels_read: u64,
    pub depth_levels_converted: u64,
    pub depth_levels_unavailable: u64,

    pub internal_reference_updates: u64,
    pub external_reference_updates: u64,
    pub external_observations_read: u64,

    pub files_written: u64,
    pub row_groups_written: u64,
}

// -----------------------------------------------------------------------------
// Sequential combined materialization
// -----------------------------------------------------------------------------

pub fn materialize_combined_conversion(
    job: &ConvertJob,
    source_manifest: &PhysicalMergeManifest,
    reference_manifest: Option<&PhysicalMergeManifest>,
    plan: &ConversionPlan,
    staging_directory: &Path,
    parquet_config: &MergeParquetConfig,
    max_events: Option<u64>,
) -> Result<PhysicalCombinedConversionMetrics> {
    job.validate()?;
    source_manifest.validate()?;

    if source_manifest.mode != UnifiedMergeMode::Combined {
        return invalid("combined conversion requires a combined source merge");
    }

    if let Some(manifest) = reference_manifest {
        manifest.validate()?;
    }

    if staging_directory.exists() {
        return invalid(format!(
            "combined conversion staging directory already exists: {}",
            staging_directory.display()
        ));
    }

    if matches!(max_events, Some(0)) {
        return invalid("combined conversion event limit must be positive");
    }

    // -------------------------------------------------------------------------
    // Resolve internal and external references
    // -------------------------------------------------------------------------

    let selection = CombinedReferenceSelection::resolve(plan, source_manifest, reference_manifest)?;

    let external = if selection.external_streams.is_empty() {
        None
    } else {
        let directory = job
            .conversion
            .reference_merge
            .as_ref()
            .ok_or_else(|| invalid_error("missing external reference merge path"))?;

        let manifest = reference_manifest
            .ok_or_else(|| invalid_error("missing external reference manifest"))?;

        Some(ExternalReferenceCursor::new(
            directory,
            manifest,
            selection.external_streams.iter().cloned(),
        )?)
    };

    let mut alignment = CombinedReferenceAlignment::new(selection, external)?;

    // -------------------------------------------------------------------------
    // Initialize conversion engine
    // -------------------------------------------------------------------------

    let kernel = CombinedConversionKernel::new(job, plan)?;

    let mut conversion_metrics = CombinedConversionMetrics::default();

    let mut reader = PhysicalConversionReader::new(
        &job.input_merge,
        source_manifest,
        CONVERSION_READ_BATCH_SIZE,
    )?;

    let mut builder: Option<ConvertedCombinedBatchBuilder> = None;
    let mut writer: Option<ConvertedParquetWriter> = None;

    let mut remaining = max_events.unwrap_or(u64::MAX);

    // -------------------------------------------------------------------------
    // Stream combined events
    // -------------------------------------------------------------------------

    while remaining > 0 {
        let Some(batch) = reader.next_batch()? else {
            break;
        };

        let count = usize::try_from(remaining.min(batch.num_rows() as u64))
            .map_err(|_| invalid_error("combined batch size overflow"))?;

        let bounded = if count == batch.num_rows() {
            batch
        } else {
            batch.slice(0, count)
        };

        if builder.is_none() {
            let combined_builder = ConvertedCombinedBatchBuilder::new(bounded.schema())?;

            let parquet_writer = ConvertedParquetWriter::with_mode(
                staging_directory,
                UnifiedMergeMode::Combined,
                combined_builder.output_schema(),
                parquet_config,
            )?;

            builder = Some(combined_builder);
            writer = Some(parquet_writer);
        }

        // Convert the entire Arrow batch.
        let rows = kernel.convert_batch(&bounded, &mut alignment, &mut conversion_metrics)?;

        // Materialize converted columns.
        let converted = builder
            .as_ref()
            .expect("combined builder initialized")
            .build(&bounded, &rows)?;

        // Write physical Parquet.
        writer
            .as_mut()
            .expect("combined writer initialized")
            .write_batch(&converted)?;

        remaining -= count as u64;
    }

    // -------------------------------------------------------------------------
    // Finalize output
    // -------------------------------------------------------------------------

    let mut writer =
        writer.ok_or_else(|| invalid_error("combined conversion produced no output batches"))?;

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

    // -------------------------------------------------------------------------
    // Validate event accounting
    // -------------------------------------------------------------------------

    let expected = max_events
        .map(|limit| limit.min(source_manifest.events_written))
        .unwrap_or(source_manifest.events_written);

    if metrics.events_read != expected || metrics.events_written != expected {
        return invalid(format!(
            "combined event count mismatch: expected={}, read={}, written={}",
            expected, metrics.events_read, metrics.events_written
        ));
    }

    if metrics.trade_events + metrics.depth_events != expected {
        return invalid("combined trade/depth event accounting mismatch");
    }

    if metrics.identity_events + metrics.converted_events + metrics.unavailable_events != expected {
        return invalid("combined conversion status accounting mismatch");
    }

    if metrics.depth_levels_converted + metrics.depth_levels_unavailable
        != metrics.depth_levels_read
    {
        return invalid("combined depth level accounting mismatch");
    }

    if physical.trade_events != metrics.trade_events
        || physical.depth_events != metrics.depth_events
    {
        return invalid("combined physical event-kind accounting mismatch");
    }

    Ok(metrics)
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
