use super::{CONVERSION_READ_BATCH_SIZE, ConversionPartitionPlan, ConversionPartitionReader};
use crate::process::convert::reference::ExternalReferencePosition;
use crate::{
    error::{MarketForgeError, Result},
    process::{
        convert::{
            ConversionPlan,
            reference::{ExternalReferenceCursor, ReferenceState},
        },
        merge::PhysicalMergeManifest,
    },
};
use arrow_array::{Array, Int64Array};
use std::{collections::HashSet, path::Path};

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

    // -------------------------------------------------------------------------
    // Discover partition boundary timestamps
    // -------------------------------------------------------------------------

    let mut boundaries = Vec::with_capacity(partitions.partitions.len());

    for partition in &partitions.partitions {
        let mut reader = ConversionPartitionReader::new(
            depth_directory,
            depth_manifest,
            partitions,
            partition,
            CONVERSION_READ_BATCH_SIZE,
        )?;

        let batch = reader.next_batch()?.ok_or_else(|| {
            invalid_error(format!(
                "depth partition {} contains no events",
                partition.partition_id
            ))
        })?;

        let timestamps = batch
            .column_by_name("event_timestamp_ns")
            .and_then(|array| array.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| invalid_error("missing depth partition timestamps"))?;

        if timestamps.is_empty() || timestamps.is_null(0) {
            return invalid("invalid depth partition boundary timestamp");
        }

        boundaries.push((
            partition.partition_id,
            partition.start_row,
            timestamps.value(0),
        ));
    }

    // -------------------------------------------------------------------------
    // Validate chronological partition boundaries
    // -------------------------------------------------------------------------

    for pair in boundaries.windows(2) {
        if pair[1].2 < pair[0].2 {
            return invalid("depth partition timestamps regressed");
        }
    }

    // -------------------------------------------------------------------------
    // Single chronological reference scan
    // -------------------------------------------------------------------------

    let mut cursor =
        ExternalReferenceCursor::new(reference_directory, reference_manifest, selected_streams)?;

    let mut state = ReferenceState::new();

    let mut checkpoints = Vec::with_capacity(boundaries.len());

    for (partition_id, start_row, timestamp_ns) in boundaries {
        cursor.advance_before(timestamp_ns, &mut state)?;

        checkpoints.push(DepthReferenceCheckpoint {
            partition_id,
            start_row,
            start_timestamp_ns: timestamp_ns,
            reference_position: cursor.position(),
            state: state.snapshot(),
        });
    }

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
