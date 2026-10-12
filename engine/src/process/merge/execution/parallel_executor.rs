use std::path::{Path, PathBuf};

use crate::{
    error::{MarketForgeError, Result},
    job::MergeJob,
};

use super::{
    MergeCommitPaths, MergeInputDataset, MergeManifest, build_depth_inventory,
    build_merge_checkpoints, build_merge_streams, build_parallel_merge_manifest,
    execute_merge_partitions_parallel, plan_merge_partitions, plan_partition_inputs,
    reconcile_depth_boundaries, resolve_initial_checkpoints, resolve_merge_inputs,
    validate_merge_output, write_merge_manifest,
};

// -----------------------------------------------------------------------------
// Parallel merge execution result
// -----------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct ParallelMergeExecutionResult {
    pub job_id: u64,

    pub dataset_path: PathBuf,
    pub manifest_path: PathBuf,

    pub partitions_completed: usize,

    pub events_written: u64,
    pub files_written: u64,

    pub skipped_initializations: u64,
    pub timestamp_regressions_dropped: u64,

    pub start_timestamp_ns: Option<i64>,
    pub end_timestamp_ns: Option<i64>,
}

// -----------------------------------------------------------------------------
// Parallel merge executor
// -----------------------------------------------------------------------------

pub fn execute_parallel_merge_job(
    job: &MergeJob,
    project_root: &Path,
) -> Result<ParallelMergeExecutionResult> {
    job.validate()?;

    // -------------------------------------------------------------------------
    // Resolve source datasets
    // -------------------------------------------------------------------------

    let inputs = resolve_merge_inputs(job, project_root)?;

    let input_metadata = inputs
        .iter()
        .map(|input| MergeInputDataset {
            dataset_id: input.dataset_id,

            dataset_path: input.dataset_path.to_string_lossy().into_owned(),

            manifest_version: input.manifest.protocol_version,
        })
        .collect::<Vec<_>>();

    // -------------------------------------------------------------------------
    // Build source inventory
    // -------------------------------------------------------------------------

    let inventory = build_depth_inventory(&inputs)?;

    // -------------------------------------------------------------------------
    // Reconcile archive boundaries
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
    // Build logical stream metadata
    // -------------------------------------------------------------------------

    let streams = build_merge_streams(&inventory, &job.inputs)?;

    // -------------------------------------------------------------------------
    // Plan time partitions
    // -------------------------------------------------------------------------

    let partition_plan = plan_merge_partitions(job)?;

    let partition_inputs = plan_partition_inputs(&partition_plan.partitions, &inventory)?;

    // -------------------------------------------------------------------------
    // Prepare transactional staging
    // -------------------------------------------------------------------------

    let commit = MergeCommitPaths::new(&job.output.dataset_path, job.job_id)?;

    commit.prepare()?;

    // -------------------------------------------------------------------------
    // Execute and validate inside staging
    // -------------------------------------------------------------------------

    let manifest = execute_parallel_into_staging(
        job,
        &commit.staging_path,
        &inventory,
        &boundaries,
        input_metadata,
        streams,
        checkpoints,
        initial_checkpoints,
        partition_inputs,
    )?;

    // -------------------------------------------------------------------------
    // Publish completed dataset
    // -------------------------------------------------------------------------

    commit.commit()?;

    Ok(ParallelMergeExecutionResult {
        job_id: job.job_id,

        dataset_path: commit.dataset_path.clone(),
        manifest_path: commit.dataset_path.join("manifest.json"),

        partitions_completed: partition_plan.partition_count(),

        events_written: manifest.events_written,
        files_written: manifest.files_written,

        skipped_initializations: u64::try_from(manifest.skipped_initializations.len())
            .map_err(|_| invalid_error("skipped initialization count overflow"))?,

        timestamp_regressions_dropped: manifest.timestamp_regressions_dropped,

        start_timestamp_ns: manifest.start_timestamp_ns,
        end_timestamp_ns: manifest.end_timestamp_ns,
    })
}

// -----------------------------------------------------------------------------
// Execute complete pipeline into staging
// -----------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
fn execute_parallel_into_staging(
    job: &MergeJob,
    staging: &Path,

    inventory: &[super::DepthTaskInventory],
    boundaries: &[super::DepthBoundaryLink],

    input_metadata: Vec<MergeInputDataset>,
    streams: Vec<super::MergeStream>,

    checkpoints: Vec<super::MergeCheckpoint>,
    initial_checkpoints: Vec<super::InitialMergeCheckpoint>,

    partition_inputs: Vec<super::PartitionInputPlan>,
) -> Result<MergeManifest> {
    // -------------------------------------------------------------------------
    // Execute partitions
    // -------------------------------------------------------------------------

    let results =
        execute_merge_partitions_parallel(job, partition_inputs, inventory, boundaries, staging)?;
    // -------------------------------------------------------------------------
    // Aggregate dropped timestamp regressions
    // -------------------------------------------------------------------------

    let timestamp_regressions_dropped = results.iter().try_fold(0_u64, |total, partition| {
        total
            .checked_add(partition.timestamp_regressions_dropped)
            .ok_or_else(|| {
                MarketForgeError::InvalidConfiguration(
                    "timestamp regression counter overflow".to_owned(),
                )
            })
    })?;
    // -------------------------------------------------------------------------
    // Validate physical output
    // -------------------------------------------------------------------------

    let validated = validate_merge_output(staging, &results)?;

    // -------------------------------------------------------------------------
    // Build global manifest
    // -------------------------------------------------------------------------

    let manifest = build_parallel_merge_manifest(
        job,
        input_metadata,
        streams,
        boundaries,
        &results,
        validated,
        checkpoints,
        initial_checkpoints,
        timestamp_regressions_dropped,
    )?;

    // -------------------------------------------------------------------------
    // Validate global manifest
    // -------------------------------------------------------------------------

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
