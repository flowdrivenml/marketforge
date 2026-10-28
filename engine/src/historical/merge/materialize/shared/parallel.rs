use crate::historical::merge::DepthTaskInventory;
use crate::historical::merge::progress::MergeWorkerProgress;
use crate::historical::merge::progress::{MergePhase, MergeProgressReporter, PartitionKey};
use std::{
    collections::HashSet,
    path::{Path, PathBuf},
};
use {
    crate::historical::job::MergeJob, crate::historical::normalize::resources::ResourceBudget,
    crate::historical::normalize::scheduler::execute_parallel_budgeted,
    crate::shared::error::MarketForgeError, crate::shared::error::Result,
};
use {
    crate::historical::merge::materialize::planning::partition::MaterializationPartitionInput,
    crate::historical::merge::materialize::planning::partition::MaterializationPartitionResult,
    crate::historical::merge::materialize::planning::partition::MaterializationPartitionWorker,
    crate::historical::merge::materialize::planning::resources::estimate_materialization_resources,
    crate::historical::merge::output::manifests::unified_manifest::UnifiedMergeMode,
    crate::historical::merge::output::manifests::unified_manifest::UnifiedMergeStream,
    crate::historical::merge::trades::inventory::TradeTaskInventory,
};
// -----------------------------------------------------------------------------
// Partition plan
// -----------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct MaterializationPartitionPlan {
    pub partition_id: u32,
    pub start_ns: i64,
    pub end_ns: i64,

    pub timeline_files: Vec<PathBuf>,

    /// Source tasks relevant to this partition.
    pub source_tasks: Vec<(u64, u64)>,
}

// -----------------------------------------------------------------------------
// Parallel execution
// -----------------------------------------------------------------------------

pub fn execute_parallel_materialization(
    plans: Vec<MaterializationPartitionPlan>,
    trade_inventory: &[TradeTaskInventory],
    depth_inventory: &[DepthTaskInventory],
    streams: &[UnifiedMergeStream],
    mode: UnifiedMergeMode,
    job: &MergeJob,
    staging: &Path,
) -> Result<Vec<MaterializationPartitionResult>> {
    execute_parallel_materialization_with_progress(
        plans,
        trade_inventory,
        depth_inventory,
        streams,
        mode,
        job,
        staging,
        &MergeProgressReporter::disabled(),
    )
}

#[allow(clippy::too_many_arguments)]
pub fn execute_parallel_materialization_with_progress(
    plans: Vec<MaterializationPartitionPlan>,
    trade_inventory: &[TradeTaskInventory],
    depth_inventory: &[DepthTaskInventory],
    streams: &[UnifiedMergeStream],
    mode: UnifiedMergeMode,
    job: &MergeJob,
    staging: &Path,
    reporter: &MergeProgressReporter,
) -> Result<Vec<MaterializationPartitionResult>> {
    if plans.is_empty() {
        return invalid("parallel materialization requires partitions");
    }

    validate_plans(&plans)?;
    reporter.partitions_planned(MergePhase::PhysicalMaterialization, plans.len());
    const MIB: u64 = 1024 * 1024;

    let memory_budget_bytes = job
        .resources
        .memory_limit_mb
        .checked_mul(MIB)
        .ok_or_else(|| invalid_error("materialization memory budget overflow"))?;

    let max_open_files = u64::try_from(job.resources.max_open_files)
        .map_err(|_| invalid_error("materialization open-file limit overflow"))?;

    let budget = ResourceBudget::with_file_limit(memory_budget_bytes, 1, max_open_files)?;

    // -------------------------------------------------------------------------
    // Resource-aware parallel execution
    // -------------------------------------------------------------------------

    let results = execute_parallel_budgeted(
        plans,
        job.resources.workers,
        &budget,
        // Estimate each partition independently.
        |plan| {
            let key = PartitionKey {
                phase: MergePhase::PhysicalMaterialization,
                partition_id: plan.partition_id,
            };

            reporter.partition_started(key);
            let task_count = plan.source_tasks.len();

            estimate_materialization_resources(task_count.max(1), &job.parquet, &job.resources)
                .map(|estimate| estimate.requirements)
        },
        // Execute one partition per worker.
        |plan| {
            let key = PartitionKey {
                phase: MergePhase::PhysicalMaterialization,
                partition_id: plan.partition_id,
            };

            reporter.partition_started(key);

            let selected = plan.source_tasks.iter().copied().collect::<HashSet<_>>();

            let trades = trade_inventory
                .iter()
                .filter(|task| selected.contains(&(task.dataset_id, task.task_id)))
                .cloned()
                .collect::<Vec<_>>();

            let depths = depth_inventory
                .iter()
                .filter(|task| selected.contains(&(task.dataset_id, task.task_id)))
                .cloned()
                .collect::<Vec<_>>();

            let estimate = estimate_materialization_resources(
                plan.source_tasks.len().max(1),
                &job.parquet,
                &job.resources,
            )?;

            let input = MaterializationPartitionInput {
                partition_id: plan.partition_id,
                start_ns: plan.start_ns,
                end_ns: plan.end_ns,

                timeline_files: &plan.timeline_files,

                trade_inventory: &trades,
                depth_inventory: &depths,

                streams,

                mode,
                parquet: &job.parquet,
                limits: estimate.limits,

                staging,
            };

            let result = (|| {
                let mut worker = MaterializationPartitionWorker::new(&input)?;

                if reporter.is_enabled() {
                    let mut progress = MergeWorkerProgress::new(reporter, key);

                    worker.execute_with_progress(&input, &mut progress)
                } else {
                    worker.execute(&input)
                }
            })();

            match &result {
                Ok(partition) => {
                    reporter.partition_completed(key, partition.writer_metrics.events_written);
                }

                Err(error) => {
                    reporter.partition_failed(key, error.to_string());
                }
            }

            result
        },
    )?;

    // -------------------------------------------------------------------------
    // Deterministic result aggregation
    // -------------------------------------------------------------------------

    let mut completed = Vec::with_capacity(results.len());

    for (index, result) in results.into_iter().enumerate() {
        let partition = result.map_err(|error| {
            invalid_error(format!(
                "materialization partition at index {index} failed: {error}"
            ))
        })?;

        completed.push(partition);
    }

    completed.sort_by_key(|result| result.partition_id);

    validate_results(&completed)?;

    Ok(completed)
}

// -----------------------------------------------------------------------------
// Plan validation
// -----------------------------------------------------------------------------

fn validate_plans(plans: &[MaterializationPartitionPlan]) -> Result<()> {
    let mut seen = HashSet::new();
    let mut previous_end = None;

    for plan in plans {
        if !seen.insert(plan.partition_id) {
            return invalid(format!(
                "duplicate materialization partition ID {}",
                plan.partition_id
            ));
        }

        if plan.start_ns >= plan.end_ns {
            return invalid(format!(
                "invalid materialization partition interval: {}",
                plan.partition_id
            ));
        }

        if let Some(previous) = previous_end {
            if plan.start_ns < previous {
                return invalid("materialization partitions overlap or are unordered");
            }
        }

        previous_end = Some(plan.end_ns);

        let mut tasks = HashSet::new();

        for task in &plan.source_tasks {
            if !tasks.insert(*task) {
                return invalid(format!(
                    "duplicate source task in partition {}",
                    plan.partition_id
                ));
            }
        }

        for pair in plan.timeline_files.windows(2) {
            if pair[0] >= pair[1] {
                return invalid(format!(
                    "unordered timeline files in partition {}",
                    plan.partition_id
                ));
            }
        }
    }

    Ok(())
}

// -----------------------------------------------------------------------------
// Result validation
// -----------------------------------------------------------------------------

fn validate_results(results: &[MaterializationPartitionResult]) -> Result<()> {
    let mut previous_end = None;

    for result in results {
        if result.start_ns >= result.end_ns {
            return invalid("invalid completed partition interval");
        }

        if let Some(previous) = previous_end {
            if result.start_ns < previous {
                return invalid("completed materialization partitions overlap");
            }
        }

        previous_end = Some(result.end_ns);

        if result.writer_metrics.events_written != result.references_read {
            return invalid(format!(
                "materialization event-count mismatch: partition={}, \
                 references={}, written={}",
                result.partition_id, result.references_read, result.writer_metrics.events_written
            ));
        }

        if result.writer_metrics.trade_events + result.writer_metrics.depth_events
            != result.writer_metrics.events_written
        {
            return invalid(format!(
                "materialization event-kind count mismatch: partition={}",
                result.partition_id
            ));
        }
    }

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
