use crate::historical::convert::execution::{
    CONVERSION_READ_BATCH_SIZE, ConversionPartitionPlan, ConversionPartitionReader,
};
use crate::historical::convert::progress::ConversionProgressReporter;
use crate::historical::convert::reference::ExternalReferencePosition;
use arrow_array::{Array, Int64Array};
use std::time::{Duration, Instant};
use std::{collections::HashSet, path::Path};
use {
    crate::historical::convert::ConversionPlan,
    crate::historical::convert::reference::ExternalReferenceCursor,
    crate::historical::convert::reference::ReferenceState,
    crate::historical::merge::PhysicalMergeManifest, crate::shared::error::MarketForgeError,
    crate::shared::error::Result,
};

// -----------------------------------------------------------------------------
// External depth checkpoint
// -----------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct DepthReferenceCheckpoint {
    pub partition_id: u32,
    pub start_row: u64,
    pub start_timestamp_ns: i64,

    pub reference_position: ExternalReferencePosition,
    pub state: ReferenceState,
}

// -----------------------------------------------------------------------------
// Checkpoint prepass metrics
// -----------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct DepthReferencePrepass {
    pub checkpoints: Vec<DepthReferenceCheckpoint>,
    pub reference_updates: u64,
    pub observations_read: u64,
}

// -----------------------------------------------------------------------------
// Build external reference checkpoints
// -----------------------------------------------------------------------------

pub fn build_depth_reference_checkpoints(
    depth_directory: &Path,
    depth_manifest: &PhysicalMergeManifest,
    reference_directory: &Path,
    reference_manifest: &PhysicalMergeManifest,
    plan: &ConversionPlan,
    partitions: &ConversionPartitionPlan,
) -> Result<DepthReferencePrepass> {
    build_depth_reference_checkpoints_with_progress(
        depth_directory,
        depth_manifest,
        reference_directory,
        reference_manifest,
        plan,
        partitions,
        &ConversionProgressReporter::disabled(),
    )
}

pub fn build_depth_reference_checkpoints_with_progress(
    depth_directory: &Path,
    depth_manifest: &PhysicalMergeManifest,
    reference_directory: &Path,
    reference_manifest: &PhysicalMergeManifest,
    plan: &ConversionPlan,
    partitions: &ConversionPartitionPlan,
    reporter: &ConversionProgressReporter,
) -> Result<DepthReferencePrepass> {
    depth_manifest.validate()?;
    reference_manifest.validate()?;

    if partitions.partitions.is_empty() {
        return invalid("depth checkpoint plan contains no partitions");
    }

    if partitions.total_rows != depth_manifest.events_written {
        return invalid("depth checkpoint partition row count mismatch");
    }

    // -------------------------------------------------------------------------
    // Collect required external reference streams
    // -------------------------------------------------------------------------

    let selected_streams = plan
        .streams
        .iter()
        .flat_map(|stream| &stream.paths)
        .flat_map(|path| &path.edges)
        .map(|edge| edge.reference_stream_id.clone())
        .collect::<HashSet<_>>();

    if selected_streams.is_empty() {
        return invalid("depth checkpoints require reference streams");
    }

    let mut cursor =
        ExternalReferenceCursor::new(reference_directory, reference_manifest, selected_streams)?;
    let mut state = super::super::shared::reader::initial_reference_state(
        depth_directory,
        depth_manifest,
        reporter,
    )?;
    let mut checkpoints = Vec::with_capacity(partitions.partitions.len());
    let total_checkpoints = partitions.partitions.len();
    let mut rows = 0;
    let mut last_progress = Instant::now();
    reporter.reference_prepass_progress(0, depth_manifest.events_written, 0, total_checkpoints);

    for partition in &partitions.partitions {
        reporter.check_interrupted()?;
        let mut reader = ConversionPartitionReader::timestamps(
            depth_directory,
            depth_manifest,
            partitions,
            partition,
            CONVERSION_READ_BATCH_SIZE,
        )?;
        let mut first = true;
        while let Some(batch) = reader.next_batch()? {
            reporter.check_interrupted()?;
            let timestamps = batch
                .column_by_name("event_timestamp_ns")
                .and_then(|array| array.as_any().downcast_ref::<Int64Array>())
                .ok_or_else(|| invalid_error("missing depth partition timestamps"))?;
            if first {
                if timestamps.is_empty() || timestamps.is_null(0) {
                    return invalid("invalid depth partition boundary timestamp");
                }
                let timestamp_ns = timestamps.value(0);
                checkpoints.push(DepthReferenceCheckpoint {
                    partition_id: partition.partition_id,
                    start_row: partition.start_row,
                    start_timestamp_ns: timestamp_ns,
                    reference_position: cursor.position(),
                    state: state.snapshot(),
                });
                first = false;
            }
            for timestamp in timestamps.iter() {
                let timestamp = timestamp.ok_or_else(|| invalid_error("null depth timestamp"))?;
                cursor.advance_before(timestamp, &mut state)?;
                state.advance_watermark(timestamp);
                rows += 1;
            }
            if last_progress.elapsed() >= Duration::from_millis(500) {
                reporter.reference_prepass_progress(
                    rows,
                    depth_manifest.events_written,
                    checkpoints.len(),
                    total_checkpoints,
                );
                last_progress = Instant::now();
            }
        }
    }
    reporter.reference_prepass_progress(
        rows,
        depth_manifest.events_written,
        checkpoints.len(),
        total_checkpoints,
    );

    Ok(DepthReferencePrepass {
        checkpoints,
        reference_updates: cursor.reference_updates(),
        observations_read: cursor.observations_read(),
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
