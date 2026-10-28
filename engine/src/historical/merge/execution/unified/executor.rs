use crate::historical::merge::progress::{MergePhase, MergeProgressReporter};
use crate::historical::merge::validate_physical_dataset_with_progress;
use crate::historical::merge::{
    MergeBoundaryRecord, MergeCommitPaths, MergeInputDataset, PhysicalMergeManifest,
    UnifiedMergeMode, UnifiedMergeStreamKind, build_depth_inventory, build_materialization_plans,
    build_merge_checkpoints, build_physical_checkpoints, build_trade_inventory,
    depth_task_intervals, plan_merge_partitions, plan_unified_partition_inputs,
    prepare_physical_publication, reconcile_depth_boundaries, resolve_initial_checkpoints,
    resolve_merge_inputs, resolve_physical_initial_reconstruction, resolve_unified_merge_streams,
    trade_task_intervals, validate_unified_merge_output,
};
use crate::{
    historical::job::{ContentType, MergeJob},
    shared::error::{MarketForgeError, Result},
};
use std::path::{Path, PathBuf};

use crate::historical::merge::{
    execute_parallel_materialization_with_progress,
    execute_unified_partitions_parallel_with_progress,
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
    execute_unified_merge_job_with_progress(job, project_root, &MergeProgressReporter::disabled())
}

pub fn execute_unified_merge_job_with_progress(
    job: &MergeJob,
    project_root: &Path,
    reporter: &MergeProgressReporter,
) -> Result<UnifiedMergeExecutionResult> {
    let result = execute_unified_merge_job_inner(job, project_root, reporter);

    reporter.finished(result.is_ok());

    result
}

fn execute_unified_merge_job_inner(
    job: &MergeJob,
    project_root: &Path,
    reporter: &MergeProgressReporter,
) -> Result<UnifiedMergeExecutionResult> {
    job.validate()?;
    let execution_started = std::time::Instant::now();

    // -------------------------------------------------------------------------
    // Resolve and classify input datasets
    // -------------------------------------------------------------------------
    reporter.phase_started(MergePhase::InputResolution);
    let resolved = profile_stage("input_resolution", || {
        resolve_merge_inputs(job, project_root)
    })?;
    reporter.phase_completed(MergePhase::InputResolution);
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
    reporter.phase_started(MergePhase::Inventory);
    let trade_inventory =
        profile_stage("trade_inventory", || build_trade_inventory(&trade_inputs))?;

    let depth_inventory =
        profile_stage("depth_inventory", || build_depth_inventory(&depth_inputs))?;
    reporter.phase_completed(MergePhase::Inventory);

    let boundaries = profile_stage("depth_boundaries", || {
        reconcile_depth_boundaries(&depth_inventory)
    })?;

    // -------------------------------------------------------------------------
    // Build depth reconstruction checkpoints
    // -------------------------------------------------------------------------

    let checkpoints = profile_stage("canonical_checkpoints", || {
        build_merge_checkpoints(&depth_inventory)
    })?;

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

    // Initial reconstruction availability must be preserved in the
    // physical manifest before publication is enabled.
    let _ = &initial_checkpoints;

    // -------------------------------------------------------------------------
    // Plan unified partitions
    // -------------------------------------------------------------------------

    let mut intervals = trade_task_intervals(&trade_inventory);

    intervals.extend(depth_task_intervals(&depth_inventory)?);

    let partition_plan = plan_merge_partitions(job)?;

    let plans = plan_unified_partition_inputs(&partition_plan.partitions, &intervals)?;

    // Preserve plans because logical execution consumes them.
    let materialization_input_plans = plans.clone();

    // -------------------------------------------------------------------------
    // Prepare transactional staging
    // -------------------------------------------------------------------------

    let commit = MergeCommitPaths::new(&job.output.dataset_path, job.job_id)?;

    commit.prepare()?;

    // -------------------------------------------------------------------------
    // Isolate logical and physical staging
    // -------------------------------------------------------------------------

    let logical_staging = commit.staging_path.join("logical");
    let physical_staging = commit.staging_path.join("physical");

    std::fs::create_dir_all(&logical_staging)
        .map_err(|error| invalid_error(format!("cannot create logical merge staging: {error}")))?;

    std::fs::create_dir_all(&physical_staging)
        .map_err(|error| invalid_error(format!("cannot create physical merge staging: {error}")))?;

    // -------------------------------------------------------------------------
    // Execute logical merge partitions
    // -------------------------------------------------------------------------

    reporter.phase_started(MergePhase::LogicalMerge);

    let results = profile_stage("logical_merge", || {
        execute_unified_partitions_parallel_with_progress(
            job,
            plans,
            &trade_inventory,
            &depth_inventory,
            &boundaries,
            &logical_staging,
            reporter,
        )
    })?;

    reporter.phase_completed(MergePhase::LogicalMerge);

    // -------------------------------------------------------------------------
    // Validate logical timeline references
    // -------------------------------------------------------------------------
    reporter.phase_started(MergePhase::LogicalValidation);
    let validated = profile_stage("logical_validation", || {
        validate_unified_merge_output(
            &logical_staging,
            &results,
            &streams,
            &trade_inventory,
            &depth_inventory,
        )
    })?;
    reporter.phase_completed(MergePhase::LogicalValidation);
    let expected_trade_events = results.iter().try_fold(0_u64, |total, result| {
        total
            .checked_add(result.trade_events)
            .ok_or_else(|| invalid_error("logical trade event count overflow"))
    })?;

    let expected_depth_events = results.iter().try_fold(0_u64, |total, result| {
        total
            .checked_add(result.depth_events)
            .ok_or_else(|| invalid_error("logical depth event count overflow"))
    })?;

    if validated.trade_events != expected_trade_events
        || validated.depth_events != expected_depth_events
    {
        return invalid("logical trade/depth event count mismatch");
    }

    // -------------------------------------------------------------------------
    // Construct physical materialization plans
    // -------------------------------------------------------------------------

    let materialization_plans = profile_stage("materialization_planning", || {
        build_materialization_plans(&logical_staging, &materialization_input_plans, &results)
    })?;

    // -------------------------------------------------------------------------
    // Determine physical output mode
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
            return invalid("materialization requires at least one market stream");
        }
    };

    // -------------------------------------------------------------------------
    // Execute parallel physical materialization
    // -------------------------------------------------------------------------

    reporter.phase_started(MergePhase::PhysicalMaterialization);

    let physical_results = profile_stage("physical_materialization", || {
        execute_parallel_materialization_with_progress(
            materialization_plans,
            &trade_inventory,
            &depth_inventory,
            &streams,
            mode,
            job,
            &physical_staging,
            reporter,
        )
    })?;

    reporter.phase_completed(MergePhase::PhysicalMaterialization);

    // -------------------------------------------------------------------------
    // Validate physical Parquet dataset
    // -------------------------------------------------------------------------
    reporter.phase_started(MergePhase::PhysicalValidation);
    let physical_validated = profile_stage("physical_validation", || {
        validate_physical_dataset_with_progress(
            &physical_staging,
            mode,
            &physical_results,
            reporter,
        )
    })?;
    reporter.phase_completed(MergePhase::PhysicalValidation);
    // -------------------------------------------------------------------------
    // Verify logical and physical event counts
    // -------------------------------------------------------------------------

    if physical_validated.events != validated.output.events_written
        || physical_validated.trade_events != validated.trade_events
        || physical_validated.depth_events != validated.depth_events
    {
        return invalid(format!(
            "logical/physical event mismatch: \
             logical={}, physical={}",
            validated.output.events_written, physical_validated.events,
        ));
    }

    // -------------------------------------------------------------------------
    // Collect skipped depth initializations
    // -------------------------------------------------------------------------

    let mut skipped_initializations = results
        .iter()
        .flat_map(|partition| partition.skipped_initializations.iter().cloned())
        .collect::<Vec<_>>();

    skipped_initializations.sort_by_key(|entry| {
        (
            entry.timestamp_ns,
            entry.dataset_id,
            entry.task_id,
            entry.event_ordinal,
        )
    });

    // -------------------------------------------------------------------------
    // Resolve physical reconstruction checkpoints
    // -------------------------------------------------------------------------

    reporter.phase_started(MergePhase::Checkpoints);

    let physical_checkpoints = profile_stage("physical_checkpoints", || {
        build_physical_checkpoints(
            &physical_staging,
            mode,
            &physical_validated,
            &checkpoints,
            &skipped_initializations,
            job.time_range,
        )
    })?;

    reporter.phase_completed(MergePhase::Checkpoints);

    // -------------------------------------------------------------------------
    // Resolve independent physical reconstruction availability
    // -------------------------------------------------------------------------

    let initial_reconstruction = resolve_physical_initial_reconstruction(
        &initial_checkpoints,
        &physical_checkpoints,
        job.time_range.start_ns,
        job.time_range.end_ns,
    )?;

    // -------------------------------------------------------------------------
    // Construct input dataset metadata
    // -------------------------------------------------------------------------

    let inputs = resolved
        .iter()
        .map(|input| MergeInputDataset {
            dataset_id: input.dataset_id,
            dataset_path: input.dataset_path.to_string_lossy().into_owned(),
            manifest_version: input.manifest.protocol_version,
        })
        .collect::<Vec<_>>();

    // -------------------------------------------------------------------------
    // Construct physical dataset manifest
    // -------------------------------------------------------------------------
    reporter.phase_started(MergePhase::Manifest);
    let physical_manifest = PhysicalMergeManifest::new(
        job.job_id,
        mode,
        inputs,
        job.time_range,
        streams,
        physical_validated,
        boundaries.iter().map(MergeBoundaryRecord::from).collect(),
        skipped_initializations,
        physical_checkpoints,
        initial_reconstruction,
    )?;
    // -------------------------------------------------------------------------
    // Validate physical manifest
    // -------------------------------------------------------------------------

    physical_manifest.validate()?;
    reporter.phase_completed(MergePhase::Manifest);
    // -------------------------------------------------------------------------
    // Prepare physical-only publication layout
    // -------------------------------------------------------------------------
    reporter.phase_started(MergePhase::PublicationPreparation);

    profile_stage("publication_preparation", || {
        prepare_physical_publication(&commit.staging_path, &physical_manifest)
    })?;

    reporter.phase_completed(MergePhase::PublicationPreparation);

    // -------------------------------------------------------------------------
    // Transactional publication
    // -------------------------------------------------------------------------

    reporter.phase_started(MergePhase::Commit);

    profile_stage("transactional_commit", || commit.commit())?;

    reporter.phase_completed(MergePhase::Commit);

    // -------------------------------------------------------------------------
    // Transactional publication
    // -------------------------------------------------------------------------

    if std::env::var_os("MARKETFORGE_PROFILE").is_some() {
        eprintln!(
            "[marketforge-profile] total_executor: {:.3}s",
            execution_started.elapsed().as_secs_f64(),
        );
    }

    // -------------------------------------------------------------------------
    // Return execution result
    // -------------------------------------------------------------------------

    Ok(UnifiedMergeExecutionResult {
        job_id: job.job_id,

        mode: physical_manifest.mode,

        dataset_path: commit.dataset_path.clone(),
        manifest_path: commit.dataset_path.join("manifest.json"),

        partitions_completed: physical_results.len(),

        events_written: physical_manifest.events_written,
        trade_events: physical_manifest.trade_events,
        depth_events: physical_manifest.depth_events,

        files_written: physical_manifest.files_written,

        skipped_initializations: physical_manifest.skipped_initializations.len() as u64,

        start_timestamp_ns: physical_manifest.start_timestamp_ns,
        end_timestamp_ns: physical_manifest.end_timestamp_ns,
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

fn profile_stage<T>(name: &str, operation: impl FnOnce() -> Result<T>) -> Result<T> {
    let started = std::time::Instant::now();

    let result = operation();

    if std::env::var_os("MARKETFORGE_PROFILE").is_some() {
        eprintln!(
            "[marketforge-profile] {name}: {:.3}s",
            started.elapsed().as_secs_f64(),
        );
    }

    result
}
