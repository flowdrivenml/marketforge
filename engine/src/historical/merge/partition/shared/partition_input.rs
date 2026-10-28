use std::collections::BTreeMap;

use crate::shared::error::{MarketForgeError, Result};

use {
    crate::historical::merge::depth::inventory::DepthTaskInventory,
    crate::historical::merge::partition::MergePartition,
};

// -----------------------------------------------------------------------------
// Partition input
// -----------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct PartitionTaskInput {
    pub dataset_id: u64,
    pub task_id: u64,

    pub start_timestamp_ns: i64,
    pub end_timestamp_ns: i64,
}

// -----------------------------------------------------------------------------
// Partition input plan
// -----------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct PartitionInputPlan {
    pub partition: MergePartition,
    pub inputs: Vec<PartitionTaskInput>,
}

// -----------------------------------------------------------------------------
// Build partition input plans
// -----------------------------------------------------------------------------

pub fn plan_partition_inputs(
    partitions: &[MergePartition],
    inventory: &[DepthTaskInventory],
) -> Result<Vec<PartitionInputPlan>> {
    let mut datasets: BTreeMap<u64, Vec<&DepthTaskInventory>> = BTreeMap::new();

    for task in inventory {
        datasets.entry(task.dataset_id).or_default().push(task);
    }

    let mut plans = Vec::with_capacity(partitions.len());

    for partition in partitions {
        let mut inputs = Vec::new();

        for (&dataset_id, tasks) in &datasets {
            for task in tasks {
                let Some(start) = task.boundary.first_timestamp_ns else {
                    return invalid(format!(
                        "dataset {dataset_id} task {}: missing first timestamp",
                        task.task_id
                    ));
                };

                let Some(end) = task.boundary.last_timestamp_ns else {
                    return invalid(format!(
                        "dataset {dataset_id} task {}: missing last timestamp",
                        task.task_id
                    ));
                };

                if start > end {
                    return invalid(format!(
                        "dataset {dataset_id} task {}: invalid timestamp range",
                        task.task_id
                    ));
                }

                // Source ranges are inclusive observations.
                // Partition ranges are half-open intervals.
                if end < partition.start_ns || start >= partition.end_ns {
                    continue;
                }

                inputs.push(PartitionTaskInput {
                    dataset_id,
                    task_id: task.task_id,
                    start_timestamp_ns: start,
                    end_timestamp_ns: end,
                });
            }
        }

        inputs.sort_by_key(|input| (input.dataset_id, input.start_timestamp_ns, input.task_id));

        plans.push(PartitionInputPlan {
            partition: *partition,
            inputs,
        });
    }

    Ok(plans)
}

// -----------------------------------------------------------------------------
// Errors
// -----------------------------------------------------------------------------

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(MarketForgeError::InvalidConfiguration(message.into()))
}
