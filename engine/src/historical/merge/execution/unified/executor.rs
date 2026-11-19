use super::parallel::execute_ranked_partitions;
use crate::historical::merge::execution::shared::resolver::resolve_merge_inputs_with_progress;
use crate::historical::merge::output::validation::unified_output_validation::validate_ranked_unified_merge_output;
use crate::historical::merge::progress::{MergePhase, MergeProgressReporter};
use crate::historical::merge::{
    MergeBoundaryRecord, MergeCommitPaths, MergeInputDataset, PhysicalMergeManifest,
    UnifiedMergeMode, UnifiedMergeStreamKind, build_materialization_plans, build_merge_checkpoints,
    build_physical_checkpoints, build_trade_inventory, depth_task_intervals, plan_merge_partitions,
    plan_unified_partition_inputs, prepare_physical_publication, reconcile_depth_boundaries,
    resolve_initial_checkpoints, resolve_physical_initial_reconstruction,
    resolve_ranked_merge_streams, trade_task_intervals,
};
use crate::{
    historical::job::{ContentType, MergeJob},
    shared::error::{MarketForgeError, Result},
};
use std::path::{Path, PathBuf};

use crate::historical::merge::execute_parallel_materialization_with_progress;
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

    reporter.finished(result.as_ref().err().map(ToString::to_string));

    result
}

fn execute_unified_merge_job_inner(
    job: &MergeJob,
    project_root: &Path,
    reporter: &MergeProgressReporter,
) -> Result<UnifiedMergeExecutionResult> {
    job.validate()?;
    use crate::historical::job::MergeTimelineOrdering;
    use crate::historical::merge::depth::inventory::build_depth_inventory_with_order;
    let causal = job.timeline_ordering() == MergeTimelineOrdering::Causal;
    let execution_started = std::time::Instant::now();

    // -------------------------------------------------------------------------
    // Resolve and classify input datasets
    // -------------------------------------------------------------------------
    reporter.phase_started(MergePhase::InputResolution);
    let resolved = profile_stage("input_resolution", || {
        resolve_merge_inputs_with_progress(job, project_root, |files| {
            reporter.inputs_verified(files)
        })
    })?;
    let streams = resolve_ranked_merge_streams(&resolved, project_root, job.submission.as_ref())?;
    reporter.inputs_resolved(
        streams
            .iter()
            .map(|stream| stream.stream_rank)
            .collect::<std::collections::HashSet<_>>()
            .len(),
    );
    use crate::historical::normalize::manifest::{file_sha256, write_publication_checksums};
    let input_hashes = resolved
        .iter()
        .map(|input| {
            if let Some(files) = job.primary_files.get(&input.dataset_id) {
                Ok(files[0].manifest_sha256.clone())
            } else {
                file_sha256(&input.dataset_path.join("manifest.json"))
            }
        })
        .collect::<std::io::Result<Vec<_>>>()
        .map_err(|error| invalid_error(error.to_string()))?;

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
    reporter.phase_completed(MergePhase::InputResolution);
    reporter.phase_started(MergePhase::Inventory);
    let mut trade_inventory =
        profile_stage("trade_inventory", || build_trade_inventory(&trade_inputs))?;

    // Stream-layout inventories already honor selection. Apply the same file
    // filter to task-layout publications while preserving their original offsets.
    for task in &mut trade_inventory {
        if let Some(files) = job.primary_files.get(&task.dataset_id) {
            let input = resolved
                .iter()
                .find(|input| input.dataset_id == task.dataset_id)
                .expect("resolved trade publication");
            if !input.manifest.streams.is_empty() {
                continue;
            }
            let root = &input.dataset_path;
            task.files.retain(|file| {
                files
                    .iter()
                    .any(|selected| file.path == root.join(&selected.path))
            });
            if let (Some(start), Some(end)) = (
                task.files.iter().map(|file| file.start_timestamp_ns).min(),
                task.files.iter().map(|file| file.end_timestamp_ns).max(),
            ) {
                // A selected part can contain several tasks. Its envelope constrains
                // each task but cannot replace that task's own observed bounds.
                task.start_timestamp_ns = task.start_timestamp_ns.max(start);
                task.end_timestamp_ns = task.end_timestamp_ns.min(end);
            }
        }
    }
    trade_inventory.retain(|task| !task.files.is_empty());

    let mut trade_ranks = std::collections::HashMap::new();
    for stream in streams
        .iter()
        .filter(|stream| stream.kind == UnifiedMergeStreamKind::Trade)
    {
        let input = resolved
            .iter()
            .find(|input| input.dataset_id == stream.dataset_id)
            .expect("resolved publication");
        let sources = input
            .manifest
            .streams
            .iter()
            .find(|entry| entry.stream_id == stream.stream_id);
        for task in trade_inventory.iter().filter(|task| {
            task.dataset_id == stream.dataset_id
                && sources.is_none_or(|entry| {
                    entry
                        .sources
                        .iter()
                        .any(|source| source.task_id == task.task_id)
                })
        }) {
            if trade_ranks
                .insert((task.dataset_id, task.task_id), stream.stream_rank)
                .is_some()
            {
                return invalid("trade task belongs to multiple selected streams");
            }
        }
    }
    let depth_inventory = profile_stage("depth_inventory", || {
        build_depth_inventory_with_order(&depth_inputs, causal)
    })?;
    let mut depth_ranks = std::collections::HashMap::new();
    for stream in streams
        .iter()
        .filter(|stream| stream.kind == UnifiedMergeStreamKind::Depth)
    {
        for task in depth_inventory.iter().filter(|task| {
            task.dataset_id == stream.dataset_id
                && (task.boundary.stream_id == stream.stream_id || !causal)
        }) {
            if depth_ranks
                .insert((task.dataset_id, task.task_id), stream.stream_rank)
                .is_some()
            {
                return invalid("depth task belongs to multiple selected streams");
            }
        }
    }
    if depth_ranks.len() != depth_inventory.len() {
        return invalid("depth source has no selected concrete stream");
    }
    let ranked = super::parallel::RankedStreams {
        streams: &streams,
        trade_ranks: &trade_ranks,
        depth_ranks: &depth_ranks,
        causal,
    };
    reporter.phase_completed(MergePhase::Inventory);

    reporter.phase_started(MergePhase::BoundaryReconciliation);
    let boundaries = profile_stage("depth_boundaries", || {
        if causal {
            Ok(Vec::new())
        } else {
            reconcile_depth_boundaries(&depth_inventory)
        }
    })?;

    // -------------------------------------------------------------------------
    // Build depth reconstruction checkpoints
    // -------------------------------------------------------------------------

    let checkpoints = profile_stage("canonical_checkpoints", || {
        if causal {
            Ok(Vec::new())
        } else {
            build_merge_checkpoints(&depth_inventory)
        }
    })?;

    let initial_checkpoints = if causal || depth_dataset_ids.is_empty() {
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
    reporter.phase_completed(MergePhase::BoundaryReconciliation);

    // -------------------------------------------------------------------------
    // Plan unified partitions
    // -------------------------------------------------------------------------

    reporter.phase_started(MergePhase::PartitionPlanning);
    let mut intervals = trade_task_intervals(&trade_inventory);

    let plans = if causal {
        use crate::historical::merge::{MergePartition, MergeTaskInterval};
        intervals.extend(depth_inventory.iter().map(|task| MergeTaskInterval {
            dataset_id: task.dataset_id,
            task_id: task.task_id,
            start_timestamp_ns: job.time_range.start_ns,
            end_timestamp_ns: job.time_range.end_ns - 1,
        }));
        plan_unified_partition_inputs(
            &[MergePartition {
                partition_id: 0,
                start_ns: job.time_range.start_ns,
                end_ns: job.time_range.end_ns,
            }],
            &intervals,
        )?
    } else {
        intervals.extend(depth_task_intervals(&depth_inventory)?);
        let partition_plan = plan_merge_partitions(job)?;
        plan_unified_partition_inputs(&partition_plan.partitions, &intervals)?
    };

    // Preserve plans because logical execution consumes them.
    let materialization_input_plans = plans.clone();
    reporter.phase_completed(MergePhase::PartitionPlanning);

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
        execute_ranked_partitions(
            job,
            plans,
            &trade_inventory,
            &depth_inventory,
            &boundaries,
            &logical_staging,
            reporter,
            Some(&ranked),
        )
    })?;

    reporter.phase_completed(MergePhase::LogicalMerge);

    // -------------------------------------------------------------------------
    // Validate logical timeline references
    // -------------------------------------------------------------------------
    reporter.phase_started(MergePhase::LogicalValidation);
    let validated = profile_stage("logical_validation", || {
        validate_ranked_unified_merge_output(
            &logical_staging,
            &results,
            &streams,
            &trade_inventory,
            &depth_inventory,
            Some(&trade_ranks),
            causal,
        )
    })?;
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

    reporter.phase_completed(MergePhase::LogicalValidation);
    reporter.phase_started(MergePhase::MaterializationPlanning);
    let materialization_plans = profile_stage("materialization_planning", || {
        let plans =
            build_materialization_plans(&logical_staging, &materialization_input_plans, &results)?;
        if causal {
            crate::historical::merge::materialize::planning::plan::split_causal_materialization_plan(
                plans,
                validated.source_tasks_by_file,
            )
        } else {
            Ok(plans)
        }
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

    reporter.phase_completed(MergePhase::MaterializationPlanning);
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
        crate::historical::merge::materialize::validation::global_validate::validate_physical_dataset_with_ordering(
            &physical_staging,
            mode,
            &physical_results,
            reporter,
            causal,
        )
    })?;
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

    reporter.phase_completed(MergePhase::PhysicalValidation);
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
        if causal {
            return Ok(Vec::new());
        }
        build_physical_checkpoints(
            &physical_staging,
            mode,
            &physical_validated,
            &checkpoints,
            &skipped_initializations,
            job.time_range,
        )
    })?;

    // -------------------------------------------------------------------------
    // Resolve independent physical reconstruction availability
    // -------------------------------------------------------------------------

    let initial_reconstruction = if causal {
        Vec::new()
    } else {
        resolve_physical_initial_reconstruction(
            &initial_checkpoints,
            &physical_checkpoints,
            job.time_range.start_ns,
            job.time_range.end_ns,
        )?
    };

    // -------------------------------------------------------------------------
    // Construct input dataset metadata
    // -------------------------------------------------------------------------

    reporter.phase_completed(MergePhase::Checkpoints);
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
    let physical_manifest = PhysicalMergeManifest::new_with_ordering(
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
        job.timeline_ordering(),
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

    let provenance = (|| -> std::io::Result<()> {
        use std::io::Write;
        let mut file = std::fs::File::create(commit.staging_path.join("merge-job.json"))?;
        if let Some(submission) = &job.submission {
            serde_json::to_writer_pretty(&mut file, submission)?;
        } else {
            serde_json::to_writer_pretty(&mut file, job)?;
        }
        file.sync_all()?;
        let mut file =
            std::fs::File::create(commit.staging_path.join("merge-input-checksums.json"))?;
        serde_json::to_writer(&mut file, &input_hashes)?;
        file.flush()?;
        file.sync_all()?;
        let mut file = std::fs::File::create(commit.staging_path.join("merge-partitions.json"))?;
        serde_json::to_writer(&mut file, &physical_results)?;
        file.sync_all()?;
        let mut runtime = std::fs::File::create(commit.staging_path.join("merge-runtime.json"))?;
        serde_json::to_writer_pretty(
            &mut runtime,
            &crate::shared::historical::config::MergeRuntimeSettings {
                resources: job.resources.clone(),
                partitioning: job.partitioning.clone(),
                parquet: job.parquet.clone(),
            },
        )?;
        runtime.sync_all()?;
        write_publication_checksums(
            &commit.staging_path,
            [
                "manifest.json",
                "merge-job.json",
                "merge-input-checksums.json",
                "merge-partitions.json",
                "merge-runtime.json",
            ]
            .into_iter()
            .map(PathBuf::from)
            .chain(
                physical_manifest
                    .files
                    .iter()
                    .map(|file| PathBuf::from(&file.path)),
            ),
        )
    })();
    provenance.map_err(|error| invalid_error(format!("merge publication provenance: {error}")))?;
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
