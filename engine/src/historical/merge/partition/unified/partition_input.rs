use std::collections::{BTreeMap, HashSet};

use crate::shared::error::{MarketForgeError, Result};

use {
    crate::historical::merge::depth::inventory::DepthTaskInventory,
    crate::historical::merge::partition::MergePartition,
    crate::historical::merge::trades::inventory::TradeTaskInventory,
};

// -----------------------------------------------------------------------------
// Unified task interval
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MergeTaskInterval {
    pub dataset_id: u64,
    pub task_id: u64,

    pub start_timestamp_ns: i64,
    pub end_timestamp_ns: i64,
}

// -----------------------------------------------------------------------------
// Unified partition input plan
// -----------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct UnifiedPartitionInputPlan {
    pub partition: MergePartition,
    pub inputs: Vec<MergeTaskInterval>,
}

// -----------------------------------------------------------------------------
// Collect trade task intervals
// -----------------------------------------------------------------------------

pub fn trade_task_intervals(inventory: &[TradeTaskInventory]) -> Vec<MergeTaskInterval> {
    inventory
        .iter()
        .map(|task| MergeTaskInterval {
            dataset_id: task.dataset_id,
            task_id: task.task_id,
            start_timestamp_ns: task.start_timestamp_ns,
            end_timestamp_ns: task.end_timestamp_ns,
        })
        .collect()
}

// -----------------------------------------------------------------------------
// Collect depth task intervals
// -----------------------------------------------------------------------------

pub fn depth_task_intervals(inventory: &[DepthTaskInventory]) -> Result<Vec<MergeTaskInterval>> {
    inventory
        .iter()
        .map(|task| {
            let start = task.boundary.first_timestamp_ns.ok_or_else(|| {
                invalid_error(format!(
                    "dataset {} task {}: missing first timestamp",
                    task.dataset_id, task.task_id
                ))
            })?;

            let end = task.boundary.last_timestamp_ns.ok_or_else(|| {
                invalid_error(format!(
                    "dataset {} task {}: missing last timestamp",
                    task.dataset_id, task.task_id
                ))
            })?;

            Ok(MergeTaskInterval {
                dataset_id: task.dataset_id,
                task_id: task.task_id,
                start_timestamp_ns: start,
                end_timestamp_ns: end,
            })
        })
        .collect()
}

// -----------------------------------------------------------------------------
// Plan unified partition inputs
// -----------------------------------------------------------------------------

pub fn plan_unified_partition_inputs(
    partitions: &[MergePartition],
    intervals: &[MergeTaskInterval],
) -> Result<Vec<UnifiedPartitionInputPlan>> {
    let mut datasets: BTreeMap<u64, Vec<&MergeTaskInterval>> = BTreeMap::new();

    let mut seen = HashSet::new();

    for interval in intervals {
        if interval.start_timestamp_ns > interval.end_timestamp_ns {
            return invalid(format!(
                "dataset {} task {}: invalid timestamp interval",
                interval.dataset_id, interval.task_id
            ));
        }

        if !seen.insert((interval.dataset_id, interval.task_id)) {
            return invalid(format!(
                "duplicate merge task: dataset={} task={}",
                interval.dataset_id, interval.task_id
            ));
        }

        datasets
            .entry(interval.dataset_id)
            .or_default()
            .push(interval);
    }

    let mut plans = Vec::with_capacity(partitions.len());

    for partition in partitions {
        if partition.start_ns >= partition.end_ns {
            return invalid(format!(
                "partition {}: invalid timestamp interval",
                partition.partition_id
            ));
        }

        let mut inputs = Vec::new();

        for tasks in datasets.values() {
            for task in tasks {
                // Task ranges contain inclusive observed timestamps.
                // Partition ranges are half-open.
                if task.end_timestamp_ns < partition.start_ns
                    || task.start_timestamp_ns >= partition.end_ns
                {
                    continue;
                }

                inputs.push(**task);
            }
        }

        inputs.sort_by_key(|task| (task.dataset_id, task.start_timestamp_ns, task.task_id));

        plans.push(UnifiedPartitionInputPlan {
            partition: *partition,
            inputs,
        });
    }

    Ok(plans)
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
