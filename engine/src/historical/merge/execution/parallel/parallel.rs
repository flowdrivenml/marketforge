use std::{collections::HashSet, path::Path};

use crate::{
    error::{MarketForgeError, Result},
    job::MergeJob,
    process::{resources::ResourceBudget, scheduler::execute_parallel_budgeted},
};

use super::{
    boundary::DepthBoundaryLink,
    inventory::DepthTaskInventory,
    partition_executor::{MergePartitionResult, execute_merge_partition},
    partition_input::PartitionInputPlan,
    resources::estimate_merge_partition_resources,
};

// -----------------------------------------------------------------------------
// Parallel partition execution
// -----------------------------------------------------------------------------

/// Execute independently planned merge partitions using MarketForge's
/// existing resource-aware worker pool.
///
/// Results are returned in deterministic partition order.
///
/// This function does not commit the merged dataset.
/// The caller owns staging, manifest construction, and publication.
pub fn execute_merge_partitions_parallel(
    job: &MergeJob,
    plans: Vec<PartitionInputPlan>,
    inventory: &[DepthTaskInventory],
    boundaries: &[DepthBoundaryLink],
    staging: &Path,
) -> Result<Vec<MergePartitionResult>> {
    if plans.is_empty() {
        return invalid("parallel merge requires at least one partition");
    }

    // -------------------------------------------------------------------------
    // Validate partition plan
    // -------------------------------------------------------------------------

    validate_partition_plans(&plans, job)?;

    // -------------------------------------------------------------------------
    // Construct resource budget
    // -------------------------------------------------------------------------

    const MIB: u64 = 1024 * 1024;

    let memory_budget_bytes = job
        .resources
        .memory_limit_mb
        .checked_mul(MIB)
        .ok_or_else(|| invalid_error("merge memory budget overflow"))?;

    let max_open_files = u64::try_from(job.resources.max_open_files)
        .map_err(|_| invalid_error("merge open-file limit overflow"))?;

    let budget = ResourceBudget::with_file_limit(memory_budget_bytes, 1, max_open_files)?;

    // -------------------------------------------------------------------------
    // Execute partitions
    // -------------------------------------------------------------------------

    let results = execute_parallel_budgeted(
        plans,
        job.resources.workers,
        &budget,
        |plan| estimate_merge_partition_resources(plan, &job.parquet),
        |plan| {
            execute_merge_partition(
                &plan,
                inventory,
                boundaries,
                &job.inputs,
                &job.parquet,
                staging,
            )
        },
    )?;

    // -------------------------------------------------------------------------
    // Collect results
    // -------------------------------------------------------------------------

    let mut completed = Vec::with_capacity(results.len());

    for (index, result) in results.into_iter().enumerate() {
        let partition = result.map_err(|error| {
            invalid_error(format!("merge partition at index {index} failed: {error}"))
        })?;

        completed.push(partition);
    }

    // -------------------------------------------------------------------------
    // Validate global results
    // -------------------------------------------------------------------------

    validate_partition_results(&completed, job)?;

    Ok(completed)
}

// -----------------------------------------------------------------------------
// Partition plan validation
// -----------------------------------------------------------------------------

fn validate_partition_plans(plans: &[PartitionInputPlan], job: &MergeJob) -> Result<()> {
    let mut seen = HashSet::new();

    let mut previous_end = None;

    for (index, plan) in plans.iter().enumerate() {
        let partition = plan.partition;

        if !seen.insert(partition.partition_id) {
            return invalid(format!(
                "duplicate merge partition ID {}",
                partition.partition_id
            ));
        }

        if partition.start_ns >= partition.end_ns {
            return invalid(format!(
                "partition {} has invalid timestamp interval",
                partition.partition_id
            ));
        }

        if index == 0 && partition.start_ns != job.time_range.start_ns {
            return invalid("partition plan does not begin at merge start");
        }

        if let Some(end) = previous_end {
            if end != partition.start_ns {
                return invalid(format!(
                    "partition {} does not follow previous partition",
                    partition.partition_id
                ));
            }
        }

        previous_end = Some(partition.end_ns);
    }

    if previous_end != Some(job.time_range.end_ns) {
        return invalid("partition plan does not cover merge end");
    }

    Ok(())
}

// -----------------------------------------------------------------------------
// Global partition result validation
// -----------------------------------------------------------------------------

pub fn validate_partition_results(results: &[MergePartitionResult], job: &MergeJob) -> Result<()> {
    if results.is_empty() {
        return invalid("merge contains no partition results");
    }

    let mut previous_end = None;
    let mut previous_timestamp = None;

    let mut total_events = 0_u64;
    let mut total_files = 0_u64;

    let mut seen_files = HashSet::new();

    for (index, result) in results.iter().enumerate() {
        // ---------------------------------------------------------------------
        // Partition interval validation
        // ---------------------------------------------------------------------

        if result.start_ns >= result.end_ns {
            return invalid(format!(
                "partition {} has invalid result interval",
                result.partition_id
            ));
        }

        if index == 0 && result.start_ns != job.time_range.start_ns {
            return invalid("partition results do not begin at merge start");
        }

        if let Some(end) = previous_end {
            if result.start_ns != end {
                return invalid(format!(
                    "partition {} is not contiguous",
                    result.partition_id
                ));
            }
        }

        previous_end = Some(result.end_ns);

        // ---------------------------------------------------------------------
        // Event timestamp validation
        // ---------------------------------------------------------------------

        if result.events_written > 0 {
            let first = result.first_timestamp_ns.ok_or_else(|| {
                invalid_error(format!(
                    "partition {} missing first timestamp",
                    result.partition_id
                ))
            })?;

            let last = result.last_timestamp_ns.ok_or_else(|| {
                invalid_error(format!(
                    "partition {} missing last timestamp",
                    result.partition_id
                ))
            })?;

            if first > last {
                return invalid(format!(
                    "partition {} has reversed timestamps",
                    result.partition_id
                ));
            }

            if first < result.start_ns || last >= result.end_ns {
                return invalid(format!(
                    "partition {} emitted timestamps outside its interval",
                    result.partition_id
                ));
            }

            if let Some(previous) = previous_timestamp {
                if first < previous {
                    return invalid(format!(
                        "global timestamp regression at partition {}",
                        result.partition_id
                    ));
                }
            }

            previous_timestamp = Some(last);
        } else if result.first_timestamp_ns.is_some() || result.last_timestamp_ns.is_some() {
            return invalid(format!(
                "empty partition {} contains timestamp metadata",
                result.partition_id
            ));
        }

        // ---------------------------------------------------------------------
        // File validation
        // ---------------------------------------------------------------------

        if result.files_written != result.files.len() as u64 {
            return invalid(format!(
                "partition {} file count mismatch",
                result.partition_id
            ));
        }

        for file in &result.files {
            if !seen_files.insert(file.path.clone()) {
                return invalid(format!("duplicate merge output file: {}", file.path));
            }
        }

        // ---------------------------------------------------------------------
        // Aggregate metrics
        // ---------------------------------------------------------------------

        total_events = total_events
            .checked_add(result.events_written)
            .ok_or_else(|| invalid_error("merge event count overflow"))?;

        total_files = total_files
            .checked_add(result.files_written)
            .ok_or_else(|| invalid_error("merge file count overflow"))?;
    }

    if previous_end != Some(job.time_range.end_ns) {
        return invalid("partition results do not cover merge end");
    }

    if total_events > 0 && total_files == 0 {
        return invalid("nonempty merge produced no Parquet files");
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
