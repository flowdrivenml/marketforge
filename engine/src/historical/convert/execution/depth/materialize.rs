use std::path::Path;

use {
    crate::historical::convert::ConversionPlan,
    crate::historical::convert::output::ConvertedParquetWriter,
    crate::historical::convert::reference::ExternalReferenceCursor,
    crate::historical::job::ConvertJob, crate::historical::job::MergeParquetConfig,
    crate::historical::merge::PhysicalMergeManifest, crate::historical::merge::UnifiedMergeMode,
    crate::shared::error::MarketForgeError, crate::shared::error::Result,
};

use crate::historical::convert::execution::{
    CONVERSION_READ_BATCH_SIZE, DepthConversionAlignment, DepthConversionKernel,
    PhysicalConversionReader,
};

// -----------------------------------------------------------------------------
// Physical depth conversion metrics
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct PhysicalDepthConversionMetrics {
    pub events_read: u64,
    pub events_written: u64,

    pub identity_events: u64,
    pub converted_events: u64,
    pub unavailable_events: u64,

    pub levels_read: u64,
    pub levels_converted: u64,
    pub levels_unavailable: u64,

    pub reference_updates: u64,
    pub reference_observations_read: u64,

    pub files_written: u64,
    pub row_groups_written: u64,
}

// -----------------------------------------------------------------------------
// Sequential physical depth materialization
// -----------------------------------------------------------------------------

pub fn materialize_depth_conversion(
    job: &ConvertJob,
    source_manifest: &PhysicalMergeManifest,
    reference_manifest: &PhysicalMergeManifest,
    plan: &ConversionPlan,
    staging_directory: &Path,
    parquet_config: &MergeParquetConfig,
    max_events: Option<u64>,
) -> Result<PhysicalDepthConversionMetrics> {
    job.validate()?;
    source_manifest.validate()?;
    reference_manifest.validate()?;

    if source_manifest.mode != UnifiedMergeMode::Depth {
        return invalid("depth conversion requires a depth-only source merge");
    }

    if reference_manifest.mode != UnifiedMergeMode::Trades {
        return invalid("depth conversion requires a trade-only reference merge");
    }

    if staging_directory.exists() {
        return invalid(format!(
            "depth conversion staging directory already exists: {}",
            staging_directory.display()
        ));
    }

    if matches!(max_events, Some(0)) {
        return invalid("depth conversion event limit must be positive");
    }

    let reference_path = job
        .conversion
        .reference_merge
        .as_ref()
        .ok_or_else(|| invalid_error("depth conversion requires reference_merge"))?;

    // -------------------------------------------------------------------------
    // Construct reference cursor
    // -------------------------------------------------------------------------

    let reference_streams = plan
        .streams
        .iter()
        .flat_map(|stream| &stream.paths)
        .flat_map(|path| &path.edges)
        .map(|edge| edge.reference_stream_id.clone())
        .collect::<std::collections::HashSet<_>>();

    if reference_streams.is_empty() {
        return invalid("depth conversion requires at least one reference stream");
    }

    let cursor =
        ExternalReferenceCursor::new(reference_path, reference_manifest, reference_streams)?;

    let kernel = DepthConversionKernel::new(job, plan)?;

    let mut alignment = DepthConversionAlignment::new(kernel, cursor);

    // -------------------------------------------------------------------------
    // Construct source reader
    // -------------------------------------------------------------------------

    let mut reader = PhysicalConversionReader::new(
        &job.input_merge,
        source_manifest,
        CONVERSION_READ_BATCH_SIZE,
    )?;

    let mut writer: Option<ConvertedParquetWriter> = None;

    let mut remaining = max_events.unwrap_or(u64::MAX);

    // -------------------------------------------------------------------------
    // Stream physical depth batches
    // -------------------------------------------------------------------------

    while remaining > 0 {
        let Some(batch) = reader.next_batch()? else {
            break;
        };

        let count = usize::try_from(remaining.min(batch.num_rows() as u64))
            .map_err(|_| invalid_error("depth conversion batch size overflow"))?;

        let bounded = if count == batch.num_rows() {
            batch
        } else {
            batch.slice(0, count)
        };

        // ---------------------------------------------------------------------
        // Convert depth events using historical trade references
        // ---------------------------------------------------------------------

        let converted = alignment.convert_batch(&bounded)?;

        // ---------------------------------------------------------------------
        // Initialize writer using actual converted schema
        // ---------------------------------------------------------------------

        if writer.is_none() {
            writer = Some(ConvertedParquetWriter::with_mode(
                staging_directory,
                UnifiedMergeMode::Depth,
                converted.schema(),
                parquet_config,
            )?);
        }

        writer
            .as_mut()
            .expect("depth writer initialized")
            .write_batch(&converted)?;

        remaining -= count as u64;
    }

    // -------------------------------------------------------------------------
    // Finalize physical output
    // -------------------------------------------------------------------------

    let mut writer =
        writer.ok_or_else(|| invalid_error("depth conversion produced no output batches"))?;

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
    // Validate event accounting
    // -------------------------------------------------------------------------

    let expected_events = max_events
        .map(|limit| limit.min(source_manifest.events_written))
        .unwrap_or(source_manifest.events_written);

    if metrics.events_read != expected_events || metrics.events_written != expected_events {
        return invalid(format!(
            "depth conversion event count mismatch: \
             expected={}, read={}, written={}",
            expected_events, metrics.events_read, metrics.events_written,
        ));
    }

    let accounted = metrics
        .identity_events
        .checked_add(metrics.converted_events)
        .and_then(|value| value.checked_add(metrics.unavailable_events))
        .ok_or_else(|| invalid_error("depth conversion status counter overflow"))?;

    if accounted != metrics.events_written {
        return invalid("depth conversion status accounting mismatch");
    }

    let accounted_levels = metrics
        .levels_converted
        .checked_add(metrics.levels_unavailable)
        .ok_or_else(|| invalid_error("depth conversion level counter overflow"))?;

    if accounted_levels != metrics.levels_read {
        return invalid("depth conversion level accounting mismatch");
    }

    if physical.depth_events != metrics.events_written || physical.trade_events != 0 {
        return invalid("depth conversion physical event-kind mismatch");
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
