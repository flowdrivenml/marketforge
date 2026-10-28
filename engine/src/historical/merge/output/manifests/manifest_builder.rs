use std::collections::HashSet;

use crate::{
    historical::job::MergeJob,
    shared::error::{MarketForgeError, Result},
};

use {
    crate::historical::merge::depth::boundary::DepthBoundaryLink,
    crate::historical::merge::depth::checkpoint::InitialMergeCheckpoint,
    crate::historical::merge::depth::checkpoint::MergeCheckpoint,
    crate::historical::merge::output::manifests::manifest::MergeInputDataset,
    crate::historical::merge::output::manifests::manifest::MergeManifest,
    crate::historical::merge::output::manifests::manifest::MergeStream,
    crate::historical::merge::output::validation::output_validation::ValidatedMergeOutput,
    crate::historical::merge::output::writers::writer::TimelineWriterMetrics,
    crate::historical::merge::partition::shared::partition_executor::MergePartitionResult,
};

// -----------------------------------------------------------------------------
// Build global merge manifest
// -----------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
pub fn build_parallel_merge_manifest(
    job: &MergeJob,
    inputs: Vec<MergeInputDataset>,
    streams: Vec<MergeStream>,
    boundaries: &[DepthBoundaryLink],
    partitions: &[MergePartitionResult],
    validated: ValidatedMergeOutput,
    checkpoints: Vec<MergeCheckpoint>,
    initial_checkpoints: Vec<InitialMergeCheckpoint>,
    timestamp_regressions_dropped: u64,
) -> Result<MergeManifest> {
    if partitions.is_empty() {
        return invalid("cannot build manifest without partitions");
    }

    if inputs.len() != job.inputs.len() {
        return invalid("merge input dataset count mismatch");
    }

    if streams.len() != job.inputs.len() {
        return invalid("merge stream count mismatch");
    }

    // -------------------------------------------------------------------------
    // Validate input dataset identities
    // -------------------------------------------------------------------------

    let mut seen_datasets = HashSet::new();

    for (index, input) in inputs.iter().enumerate() {
        if input.dataset_id != job.inputs[index] {
            return invalid(format!("merge input order mismatch at index {index}"));
        }

        if !seen_datasets.insert(input.dataset_id) {
            return invalid(format!(
                "duplicate merge input dataset {}",
                input.dataset_id
            ));
        }
    }

    // -------------------------------------------------------------------------
    // Validate stream identities and ordering
    // -------------------------------------------------------------------------

    let mut seen_ranks = HashSet::new();

    for (index, stream) in streams.iter().enumerate() {
        if stream.dataset_id != job.inputs[index] {
            return invalid(format!("merge stream dataset mismatch at index {index}"));
        }

        if !seen_ranks.insert(stream.stream_rank) {
            return invalid(format!(
                "duplicate merge stream rank {}",
                stream.stream_rank
            ));
        }

        if usize::try_from(stream.stream_rank).ok() != Some(index) {
            return invalid(format!(
                "unexpected merge stream rank {}",
                stream.stream_rank
            ));
        }
    }

    // -------------------------------------------------------------------------
    // Aggregate partition metrics
    // -------------------------------------------------------------------------

    let mut partition_events = 0_u64;
    let mut partition_files = 0_u64;

    for partition in partitions {
        partition_events = partition_events
            .checked_add(partition.events_written)
            .ok_or_else(|| invalid_error("partition event count overflow"))?;

        partition_files = partition_files
            .checked_add(partition.files_written)
            .ok_or_else(|| invalid_error("partition file count overflow"))?;
    }

    if partition_events != validated.events_written {
        return invalid(format!(
            "validated event count mismatch: partitions={}, physical={}",
            partition_events, validated.events_written
        ));
    }

    if partition_files != validated.files_written {
        return invalid(format!(
            "validated file count mismatch: partitions={}, physical={}",
            partition_files, validated.files_written
        ));
    }

    // -------------------------------------------------------------------------
    // Collect skipped initializations
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
    // Construct global writer metrics
    // -------------------------------------------------------------------------

    let metrics = TimelineWriterMetrics {
        events_written: validated.events_written,
        files_written: validated.files_written,
        start_timestamp_ns: validated.start_timestamp_ns,
        end_timestamp_ns: validated.end_timestamp_ns,
    };

    // -------------------------------------------------------------------------
    // Construct manifest
    // -------------------------------------------------------------------------

    let manifest = MergeManifest::new(
        job.job_id,
        job.time_range,
        inputs,
        streams,
        boundaries,
        skipped_initializations,
        &metrics,
        validated.files,
        checkpoints,
        initial_checkpoints,
        timestamp_regressions_dropped,
    );

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
