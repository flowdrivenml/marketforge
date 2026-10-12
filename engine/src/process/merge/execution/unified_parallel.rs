use std::{collections::HashSet, path::Path};

use crate::{
    error::{MarketForgeError, Result},
    job::MergeJob,
    process::{resources::ResourceBudget, scheduler::execute_parallel_budgeted},
};

use super::{
    boundary::DepthBoundaryLink,
    inventory::DepthTaskInventory,
    resources::estimate_merge_stream_resources,
    trade_inventory::TradeTaskInventory,
    unified_partition_executor::{UnifiedPartitionResult, execute_unified_partition},
    unified_partition_input::UnifiedPartitionInputPlan,
};

// -----------------------------------------------------------------------------
// Parallel unified merge execution
// -----------------------------------------------------------------------------

pub fn execute_unified_partitions_parallel(
    job: &MergeJob,
    plans: Vec<UnifiedPartitionInputPlan>,
    trade_inventory: &[TradeTaskInventory],
    depth_inventory: &[DepthTaskInventory],
    boundaries: &[DepthBoundaryLink],
    staging: &Path,
) -> Result<Vec<UnifiedPartitionResult>> {
    if plans.is_empty() {
        return invalid("parallel unified merge requires partitions");
    }

    validate_plans(&plans, job)?;

    const MIB: u64 = 1024 * 1024;

    let memory_budget_bytes = job
        .resources
        .memory_limit_mb
        .checked_mul(MIB)
        .ok_or_else(|| invalid_error("unified merge memory budget overflow"))?;

    let max_open_files = u64::try_from(job.resources.max_open_files)
        .map_err(|_| invalid_error("unified merge open-file limit overflow"))?;

    let budget = ResourceBudget::with_file_limit(memory_budget_bytes, 1, max_open_files)?;

    // -------------------------------------------------------------------------
    // Execute partitions through the existing resource-aware scheduler
    // -------------------------------------------------------------------------

    let results = execute_parallel_budgeted(
        plans,
        job.resources.workers,
        &budget,
        |plan| {
            let stream_count = plan
                .inputs
                .iter()
                .map(|input| input.dataset_id)
                .collect::<HashSet<_>>()
                .len();

            estimate_merge_stream_resources(stream_count, &job.parquet)
        },
        |plan| {
            execute_unified_partition(
                &plan,
                trade_inventory,
                depth_inventory,
                boundaries,
                &job.inputs,
                &job.parquet,
                staging,
            )
        },
    )?;

    let mut completed = Vec::with_capacity(results.len());

    for (index, result) in results.into_iter().enumerate() {
        let partition = result.map_err(|error| {
            invalid_error(format!(
                "unified partition at index {index} failed: {error}"
            ))
        })?;

        completed.push(partition);
    }

    validate_unified_partition_results(&completed, job)?;

    Ok(completed)
}

// -----------------------------------------------------------------------------
// Validate partition plans
// -----------------------------------------------------------------------------

fn validate_plans(plans: &[UnifiedPartitionInputPlan], job: &MergeJob) -> Result<()> {
    let mut seen = HashSet::new();
    let mut previous_end = None;

    for (index, plan) in plans.iter().enumerate() {
        let partition = plan.partition;

        if !seen.insert(partition.partition_id) {
            return invalid(format!(
                "duplicate unified partition ID {}",
                partition.partition_id
            ));
        }

        if partition.start_ns >= partition.end_ns {
            return invalid(format!(
                "partition {} has invalid interval",
                partition.partition_id
            ));
        }

        if index == 0 && partition.start_ns != job.time_range.start_ns {
            return invalid("unified partition plan does not begin at merge start");
        }

        if let Some(end) = previous_end {
            if end != partition.start_ns {
                return invalid(format!(
                    "partition {} is not contiguous",
                    partition.partition_id
                ));
            }
        }

        previous_end = Some(partition.end_ns);
    }

    if previous_end != Some(job.time_range.end_ns) {
        return invalid("unified partition plan does not cover merge end");
    }

    Ok(())
}

// -----------------------------------------------------------------------------
// Validate partition results
// -----------------------------------------------------------------------------

pub fn validate_unified_partition_results(
    results: &[UnifiedPartitionResult],
    job: &MergeJob,
) -> Result<()> {
    if results.is_empty() {
        return invalid("unified merge has no partition results");
    }

    let mut previous_end = None;
    let mut previous_timestamp = None;

    let mut total_events = 0_u64;
    let mut total_files = 0_u64;

    let mut seen_files = HashSet::new();

    for (index, result) in results.iter().enumerate() {
        if result.start_ns >= result.end_ns {
            return invalid(format!(
                "partition {} has invalid result interval",
                result.partition_id
            ));
        }

        if index == 0 && result.start_ns != job.time_range.start_ns {
            return invalid("unified partition results do not begin at merge start");
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
        // Event accounting
        // ---------------------------------------------------------------------

        if result.events_written
            != result
                .trade_events
                .checked_add(result.depth_events)
                .ok_or_else(|| invalid_error("unified event count overflow"))?
        {
            return invalid(format!(
                "partition {}: trade/depth event count mismatch",
                result.partition_id
            ));
        }

        // ---------------------------------------------------------------------
        // Timestamp validation
        // ---------------------------------------------------------------------

        if result.events_written > 0 {
            let first = result
                .first_timestamp_ns
                .ok_or_else(|| invalid_error("missing first partition timestamp"))?;

            let last = result
                .last_timestamp_ns
                .ok_or_else(|| invalid_error("missing last partition timestamp"))?;

            if first > last || first < result.start_ns || last >= result.end_ns {
                return invalid(format!(
                    "partition {}: invalid event timestamp range",
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
        // File accounting
        // ---------------------------------------------------------------------

        if result.files_written != result.files.len() as u64 {
            return invalid(format!(
                "partition {}: file count mismatch",
                result.partition_id
            ));
        }

        for file in &result.files {
            if !seen_files.insert(file.path.clone()) {
                return invalid(format!("duplicate unified output file: {}", file.path));
            }
        }

        total_events = total_events
            .checked_add(result.events_written)
            .ok_or_else(|| invalid_error("unified event count overflow"))?;

        total_files = total_files
            .checked_add(result.files_written)
            .ok_or_else(|| invalid_error("unified file count overflow"))?;
    }

    if previous_end != Some(job.time_range.end_ns) {
        return invalid("unified partition results do not cover merge end");
    }

    if total_events > 0 && total_files == 0 {
        return invalid("nonempty unified merge produced no Parquet files");
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
