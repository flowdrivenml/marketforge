use std::{
    fs::OpenOptions,
    io::BufWriter,
    path::{Path, PathBuf},
};

use crate::{
    error::{MarketForgeError, Result},
    job::{ContentType, MergeJob},
};

use super::{
    MergeCommitPaths, MergeInputDataset, UnifiedMergeManifest, UnifiedMergeMode,
    build_depth_inventory, build_merge_checkpoints, build_trade_inventory,
    build_unified_merge_manifest, depth_task_intervals, execute_unified_partitions_parallel,
    plan_merge_partitions, plan_unified_partition_inputs, reconcile_depth_boundaries,
    resolve_initial_checkpoints, resolve_merge_inputs, resolve_unified_merge_streams,
    trade_task_intervals, validate_unified_merge_output,
};

// -----------------------------------------------------------------------------
// Execution result
// -----------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct UnifiedMergeExecutionResult {
    pub job_id: u64,

    pub mode: UnifiedMergeMode,

    pub dataset_path: PathBuf,
    pub manifest_path: PathBuf,

    pub partitions_completed: usize,

    pub events_written: u64,
    pub trade_events: u64,
    pub depth_events: u64,

    pub files_written: u64,
    pub skipped_initializations: u64,

    pub start_timestamp_ns: Option<i64>,
    pub end_timestamp_ns: Option<i64>,
}

// -----------------------------------------------------------------------------
// Production unified merge execution
// -----------------------------------------------------------------------------

pub fn execute_unified_merge_job(
    job: &MergeJob,
    project_root: &Path,
) -> Result<UnifiedMergeExecutionResult> {
    job.validate()?;

    // -------------------------------------------------------------------------
    // Resolve and classify input datasets
    // -------------------------------------------------------------------------

    let resolved = resolve_merge_inputs(job, project_root)?;

    let streams = resolve_unified_merge_streams(&resolved, project_root)?;

    let mut trade_inputs = Vec::new();
    let mut depth_inputs = Vec::new();
    let mut depth_dataset_ids = Vec::new();

    for input in &resolved {
        match input.manifest.content_type {
            ContentType::Trades => {
                trade_inputs.push(input.clone());
            }

            ContentType::Depth => {
                depth_dataset_ids.push(input.dataset_id);
                depth_inputs.push(input.clone());
            }

            ContentType::Combined => {
                return invalid(format!(
                    "dataset {}: combined canonical inputs are unsupported",
                    input.dataset_id
                ));
            }
        }
    }

    // -------------------------------------------------------------------------
    // Build source inventories
    // -------------------------------------------------------------------------

    let trade_inventory = build_trade_inventory(&trade_inputs)?;

    let depth_inventory = build_depth_inventory(&depth_inputs)?;

    let boundaries = reconcile_depth_boundaries(&depth_inventory)?;

    // -------------------------------------------------------------------------
    // Build depth reconstruction checkpoints
    // -------------------------------------------------------------------------

    let checkpoints = build_merge_checkpoints(&depth_inventory)?;

    let initial_checkpoints = if depth_dataset_ids.is_empty() {
        Vec::new()
    } else {
        resolve_initial_checkpoints(
            &checkpoints,
            &depth_dataset_ids,
            job.time_range.start_ns,
            job.time_range.end_ns,
        )?
    };

    // -------------------------------------------------------------------------
    // Plan unified partitions
    // -------------------------------------------------------------------------

    let mut intervals = trade_task_intervals(&trade_inventory);

    intervals.extend(depth_task_intervals(&depth_inventory)?);

    let partition_plan = plan_merge_partitions(job)?;

    let plans = plan_unified_partition_inputs(&partition_plan.partitions, &intervals)?;

    // -------------------------------------------------------------------------
    // Prepare transactional staging
    // -------------------------------------------------------------------------

    let commit = MergeCommitPaths::new(&job.output.dataset_path, job.job_id)?;

    commit.prepare()?;

    // -------------------------------------------------------------------------
    // Execute parallel partitions
    // -------------------------------------------------------------------------

    let results = execute_unified_partitions_parallel(
        job,
        plans,
        &trade_inventory,
        &depth_inventory,
        &boundaries,
        &commit.staging_path,
    )?;

    // -------------------------------------------------------------------------
    // Validate physical output
    // -------------------------------------------------------------------------

    let validated = validate_unified_merge_output(
        &commit.staging_path,
        &results,
        &streams,
        &trade_inventory,
        &depth_inventory,
    )?;
    let expected_trade_events: u64 = results.iter().map(|result| result.trade_events).sum();

    let expected_depth_events: u64 = results.iter().map(|result| result.depth_events).sum();

    if validated.trade_events != expected_trade_events
        || validated.depth_events != expected_depth_events
    {
        return invalid("unified physical trade/depth event count mismatch");
    }

    // -------------------------------------------------------------------------
    // Construct input manifest metadata
    // -------------------------------------------------------------------------

    let inputs = resolved
        .iter()
        .map(|input| MergeInputDataset {
            dataset_id: input.dataset_id,
            dataset_path: input.dataset_path.to_string_lossy().into_owned(),
            manifest_version: input.manifest.protocol_version,
        })
        .collect();

    // -------------------------------------------------------------------------
    // Build unified manifest
    // -------------------------------------------------------------------------

    let manifest = build_unified_merge_manifest(
        job,
        inputs,
        streams,
        &boundaries,
        &results,
        validated.output,
        checkpoints,
        initial_checkpoints,
    )?;

    // -------------------------------------------------------------------------
    // Persist manifest
    // -------------------------------------------------------------------------

    write_unified_manifest(&commit.staging_path, &manifest)?;

    // -------------------------------------------------------------------------
    // Transactional publication
    // -------------------------------------------------------------------------

    commit.commit()?;

    // -------------------------------------------------------------------------
    // Return execution result
    // -------------------------------------------------------------------------

    Ok(UnifiedMergeExecutionResult {
        job_id: job.job_id,

        mode: manifest.mode,

        dataset_path: commit.dataset_path.clone(),

        manifest_path: commit.dataset_path.join("manifest.json"),

        partitions_completed: results.len(),

        events_written: manifest.events_written,
        trade_events: manifest.trade_events,
        depth_events: manifest.depth_events,

        files_written: manifest.files_written,

        skipped_initializations: manifest.skipped_initializations.len() as u64,

        start_timestamp_ns: manifest.start_timestamp_ns,
        end_timestamp_ns: manifest.end_timestamp_ns,
    })
}

// -----------------------------------------------------------------------------
// Manifest persistence
// -----------------------------------------------------------------------------

fn write_unified_manifest(directory: &Path, manifest: &UnifiedMergeManifest) -> Result<()> {
    manifest.validate()?;

    let path = directory.join("manifest.json");

    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .map_err(|error| {
            invalid_error(format!(
                "failed to create unified manifest {}: {error}",
                path.display()
            ))
        })?;

    serde_json::to_writer_pretty(BufWriter::new(file), manifest)
        .map_err(|error| invalid_error(format!("failed to write unified manifest: {error}")))?;

    Ok(())
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
