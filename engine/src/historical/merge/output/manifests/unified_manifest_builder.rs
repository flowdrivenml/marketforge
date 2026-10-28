use std::collections::HashSet;

use crate::{
    historical::job::MergeJob,
    shared::error::{MarketForgeError, Result},
};

use {
    crate::historical::merge::depth::boundary::DepthBoundaryLink,
    crate::historical::merge::depth::checkpoint::InitialMergeCheckpoint,
    crate::historical::merge::depth::checkpoint::MergeCheckpoint,
    crate::historical::merge::output::manifests::manifest::MergeBoundaryRecord,
    crate::historical::merge::output::manifests::manifest::MergeInputDataset,
    crate::historical::merge::output::manifests::unified_manifest::UNIFIED_MERGE_MANIFEST_VERSION,
    crate::historical::merge::output::manifests::unified_manifest::UnifiedMergeManifest,
    crate::historical::merge::output::manifests::unified_manifest::UnifiedMergeMode,
    crate::historical::merge::output::manifests::unified_manifest::UnifiedMergeStream,
    crate::historical::merge::output::manifests::unified_manifest::UnifiedMergeStreamKind,
    crate::historical::merge::output::validation::output_validation::ValidatedMergeOutput,
    crate::historical::merge::output::writers::unified_timeline::UNIFIED_TIMELINE_VERSION,
    crate::historical::merge::partition::unified::partition_executor::UnifiedPartitionResult,
};

// -----------------------------------------------------------------------------
// Build unified merge manifest
// -----------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
pub fn build_unified_merge_manifest(
    job: &MergeJob,
    inputs: Vec<MergeInputDataset>,
    streams: Vec<UnifiedMergeStream>,
    boundaries: &[DepthBoundaryLink],
    partitions: &[UnifiedPartitionResult],
    validated: ValidatedMergeOutput,
    checkpoints: Vec<MergeCheckpoint>,
    initial_checkpoints: Vec<InitialMergeCheckpoint>,
) -> Result<UnifiedMergeManifest> {
    if partitions.is_empty() {
        return invalid("cannot build unified manifest without partitions");
    }

    if inputs.len() != job.inputs.len() || streams.len() != job.inputs.len() {
        return invalid("unified merge input/stream count mismatch");
    }

    // -------------------------------------------------------------------------
    // Validate dataset and stream identities
    // -------------------------------------------------------------------------

    let mut seen_datasets = HashSet::new();

    for (index, (&dataset_id, input)) in job.inputs.iter().zip(&inputs).enumerate() {
        if input.dataset_id != dataset_id {
            return invalid(format!("unified input dataset mismatch at index {index}"));
        }

        if !seen_datasets.insert(dataset_id) {
            return invalid(format!("duplicate unified input dataset {dataset_id}"));
        }

        let stream = &streams[index];

        if stream.dataset_id != dataset_id {
            return invalid(format!("unified stream dataset mismatch at index {index}"));
        }

        if usize::try_from(stream.stream_rank).ok() != Some(index) {
            return invalid(format!("invalid unified stream rank at index {index}"));
        }
    }

    // -------------------------------------------------------------------------
    // Determine merge mode
    // -------------------------------------------------------------------------

    let has_trades = streams
        .iter()
        .any(|stream| stream.kind == UnifiedMergeStreamKind::Trade);

    let has_depth = streams
        .iter()
        .any(|stream| stream.kind == UnifiedMergeStreamKind::Depth);

    let mode = match (has_trades, has_depth) {
        (true, true) => UnifiedMergeMode::Combined,
        (true, false) => UnifiedMergeMode::Trades,
        (false, true) => UnifiedMergeMode::Depth,
        (false, false) => {
            return invalid("unified merge contains no recognized streams");
        }
    };

    // -------------------------------------------------------------------------
    // Aggregate partition metrics
    // -------------------------------------------------------------------------

    let mut trade_events = 0_u64;
    let mut depth_events = 0_u64;
    let mut total_events = 0_u64;
    let mut total_files = 0_u64;

    for partition in partitions {
        trade_events = trade_events
            .checked_add(partition.trade_events)
            .ok_or_else(|| invalid_error("trade event count overflow"))?;

        depth_events = depth_events
            .checked_add(partition.depth_events)
            .ok_or_else(|| invalid_error("depth event count overflow"))?;

        total_events = total_events
            .checked_add(partition.events_written)
            .ok_or_else(|| invalid_error("total event count overflow"))?;

        total_files = total_files
            .checked_add(partition.files_written)
            .ok_or_else(|| invalid_error("total file count overflow"))?;
    }

    if total_events != validated.events_written {
        return invalid(format!(
            "unified physical event mismatch: partitions={}, validated={}",
            total_events, validated.events_written
        ));
    }

    if total_files != validated.files_written {
        return invalid(format!(
            "unified physical file mismatch: partitions={}, validated={}",
            total_files, validated.files_written
        ));
    }

    // -------------------------------------------------------------------------
    // Collect skipped depth initializations
    // -------------------------------------------------------------------------

    let mut skipped_initializations = Vec::new();
    let mut seen_initializations = HashSet::new();

    for partition in partitions {
        for skipped in &partition.skipped_initializations {
            let identity = (skipped.dataset_id, skipped.task_id, skipped.event_ordinal);

            if !seen_initializations.insert(identity) {
                return invalid(format!(
                    "duplicate skipped initialization: dataset={}, task={}, ordinal={}",
                    skipped.dataset_id, skipped.task_id, skipped.event_ordinal
                ));
            }

            skipped_initializations.push(skipped.clone());
        }
    }

    skipped_initializations.sort_by_key(|entry| {
        (
            entry.timestamp_ns,
            entry.dataset_id,
            entry.task_id,
            entry.event_ordinal,
        )
    });

    // -------------------------------------------------------------------------
    // Convert depth boundary metadata
    // -------------------------------------------------------------------------

    let boundary_links = boundaries.iter().map(MergeBoundaryRecord::from).collect();

    // -------------------------------------------------------------------------
    // Construct manifest
    // -------------------------------------------------------------------------

    let manifest = UnifiedMergeManifest {
        protocol_version: UNIFIED_MERGE_MANIFEST_VERSION,
        timeline_schema_version: UNIFIED_TIMELINE_VERSION,

        job_id: job.job_id,
        mode,

        input_datasets: inputs,
        time_range: job.time_range,

        events_written: validated.events_written,
        trade_events,
        depth_events,
        files_written: validated.files_written,

        start_timestamp_ns: validated.start_timestamp_ns,
        end_timestamp_ns: validated.end_timestamp_ns,

        files: validated.files,
        streams,

        boundary_links,
        skipped_initializations,

        checkpoints,
        initial_checkpoints,
    };

    manifest.validate()?;

    Ok(manifest)
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
