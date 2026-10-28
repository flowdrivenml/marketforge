use std::{
    fs,
    path::{Path, PathBuf},
};

use crate::historical::merge::core::source::DepthEventSource;
use crate::historical::merge::{
    DepthStream, KWayDepthMerger, MergeCommitPaths, MergeInputDataset, MergeManifest,
    MergeSkippedInitialization, MergeTimelineFile, TimelineRecord, TimelineWriter,
    build_depth_inventory, build_merge_checkpoints, build_merge_streams,
    reconcile_depth_boundaries, resolve_initial_checkpoints, resolve_merge_inputs,
    write_merge_manifest,
};
use crate::{
    historical::job::MergeJob,
    shared::error::{MarketForgeError, Result},
};

// -----------------------------------------------------------------------------
// Execution result
// -----------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct MergeExecutionResult {
    pub job_id: u64,
    pub dataset_path: PathBuf,
    pub manifest_path: PathBuf,

    pub events_written: u64,
    pub files_written: u64,
    pub skipped_initializations: u64,

    pub start_timestamp_ns: Option<i64>,
    pub end_timestamp_ns: Option<i64>,
}

// -----------------------------------------------------------------------------
// Sequential merge executor
// -----------------------------------------------------------------------------

pub fn execute_merge_job(job: &MergeJob, project_root: &Path) -> Result<MergeExecutionResult> {
    job.validate()?;

    // -------------------------------------------------------------------------
    // Resolve input datasets
    // -------------------------------------------------------------------------

    let inputs = resolve_merge_inputs(job, project_root)?;

    // -------------------------------------------------------------------------
    // Build depth inventory
    // -------------------------------------------------------------------------

    let inventory = build_depth_inventory(&inputs)?;

    // -------------------------------------------------------------------------
    // Reconcile boundaries
    // -------------------------------------------------------------------------

    let boundaries = reconcile_depth_boundaries(&inventory)?;

    // -------------------------------------------------------------------------
    // Build reconstruction checkpoints
    // -------------------------------------------------------------------------

    let checkpoints = build_merge_checkpoints(&inventory)?;

    let initial_checkpoints = resolve_initial_checkpoints(
        &checkpoints,
        &job.inputs,
        job.time_range.start_ns,
        job.time_range.end_ns,
    )?;

    // -------------------------------------------------------------------------
    // Build stream metadata
    // -------------------------------------------------------------------------

    let streams_metadata = build_merge_streams(&inventory, &job.inputs)?;

    let input_datasets = inputs
        .iter()
        .map(|input| MergeInputDataset {
            dataset_id: input.dataset_id,
            dataset_path: input.dataset_path.to_string_lossy().into_owned(),
            manifest_version: input.manifest.protocol_version,
        })
        .collect::<Vec<_>>();

    // -------------------------------------------------------------------------
    // Construct logical depth streams
    // -------------------------------------------------------------------------

    let mut streams: Vec<Box<dyn DepthEventSource>> = Vec::with_capacity(job.inputs.len());

    for &dataset_id in &job.inputs {
        let tasks = inventory
            .iter()
            .filter(|task| task.dataset_id == dataset_id)
            .cloned()
            .collect();

        streams.push(Box::new(DepthStream::new(dataset_id, tasks, &boundaries)?));
    }

    // -------------------------------------------------------------------------
    // Initialize chronological merger
    // -------------------------------------------------------------------------

    let mut merger = KWayDepthMerger::new(streams)?;

    // -------------------------------------------------------------------------
    // Prepare transactional staging
    // -------------------------------------------------------------------------

    let commit = MergeCommitPaths::new(&job.output.dataset_path, job.job_id)?;

    commit.prepare()?;

    // -------------------------------------------------------------------------
    // Execute merge into staging
    // -------------------------------------------------------------------------

    let result = execute_merge_into_staging(
        job,
        &commit.staging_path,
        &mut merger,
        input_datasets,
        streams_metadata,
        &boundaries,
        checkpoints,
        initial_checkpoints,
    );

    // Preserve staging on failure.
    let manifest = result?;

    // -------------------------------------------------------------------------
    // Commit dataset
    // -------------------------------------------------------------------------

    commit.commit()?;

    Ok(MergeExecutionResult {
        job_id: job.job_id,
        dataset_path: commit.dataset_path.clone(),
        manifest_path: commit.dataset_path.join("manifest.json"),

        events_written: manifest.events_written,
        files_written: manifest.files_written,

        skipped_initializations: u64::try_from(manifest.skipped_initializations.len())
            .map_err(|_| invalid_error("skipped initialization count overflow"))?,

        start_timestamp_ns: manifest.start_timestamp_ns,
        end_timestamp_ns: manifest.end_timestamp_ns,
    })
}

// -----------------------------------------------------------------------------
// Staged execution
// -----------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
fn execute_merge_into_staging(
    job: &MergeJob,
    staging: &Path,
    merger: &mut KWayDepthMerger,

    input_datasets: Vec<MergeInputDataset>,
    streams_metadata: Vec<crate::historical::merge::MergeStream>,

    boundaries: &[crate::historical::merge::DepthBoundaryLink],

    checkpoints: Vec<crate::historical::merge::MergeCheckpoint>,
    initial_checkpoints: Vec<crate::historical::merge::InitialMergeCheckpoint>,
) -> Result<MergeManifest> {
    let timeline_directory = staging.join("timeline");

    let mut writer = TimelineWriter::new(&timeline_directory, &job.parquet)?;

    let start_ns = job.time_range.start_ns;
    let end_ns = job.time_range.end_ns;

    // -------------------------------------------------------------------------
    // Chronological event processing
    // -------------------------------------------------------------------------

    while let Some(merged) = merger.next_event()? {
        let timestamp = merged.event.event.event_timestamp_ns;

        // The K-way merger guarantees global chronological ordering.
        if timestamp >= end_ns {
            break;
        }

        if timestamp < start_ns {
            continue;
        }

        writer.write(TimelineRecord::from(&merged))?;
    }

    // -------------------------------------------------------------------------
    // Finalize timeline
    // -------------------------------------------------------------------------

    writer.finish()?;

    if !writer.is_finished() || writer.is_failed() {
        return invalid("timeline writer did not finalize successfully");
    }

    let metrics = writer.metrics().clone();

    // -------------------------------------------------------------------------
    // Inspect output files
    // -------------------------------------------------------------------------

    let mut files = Vec::new();

    if timeline_directory.exists() {
        for entry in fs::read_dir(&timeline_directory).map_err(|error| {
            invalid_error(format!("failed to inspect timeline directory: {error}"))
        })? {
            let entry = entry.map_err(|error| {
                invalid_error(format!("failed to inspect timeline entry: {error}"))
            })?;

            let path = entry.path();

            if path.extension().and_then(|ext| ext.to_str()) != Some("parquet") {
                continue;
            }

            let metadata = fs::metadata(&path).map_err(|error| {
                invalid_error(format!("failed to inspect timeline file: {error}"))
            })?;

            let filename = path
                .file_name()
                .ok_or_else(|| invalid_error("invalid timeline filename"))?
                .to_string_lossy();

            files.push(MergeTimelineFile {
                path: format!("timeline/{filename}"),
                size_bytes: metadata.len(),

                partition_id: None,
                rows: None,
                start_timestamp_ns: None,
                end_timestamp_ns: None,
            });
        }
    }

    files.sort_by(|a, b| a.path.cmp(&b.path));

    if files.len() as u64 != metrics.files_written {
        return invalid(format!(
            "timeline file count mismatch: writer={}, discovered={}",
            metrics.files_written,
            files.len(),
        ));
    }

    // -------------------------------------------------------------------------
    // Collect skipped initialization references
    // -------------------------------------------------------------------------

    let skipped_initializations: Vec<MergeSkippedInitialization> = merger.skipped_initializations();

    // -------------------------------------------------------------------------
    // Build merge manifest
    // -------------------------------------------------------------------------

    let manifest = MergeManifest::new(
        job.job_id,
        job.time_range,
        input_datasets,
        streams_metadata,
        boundaries,
        skipped_initializations,
        &metrics,
        files,
        checkpoints,
        initial_checkpoints,
        0,
    );

    manifest.validate()?;

    // -------------------------------------------------------------------------
    // Persist manifest
    // -------------------------------------------------------------------------

    write_merge_manifest(staging, &manifest)?;

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
