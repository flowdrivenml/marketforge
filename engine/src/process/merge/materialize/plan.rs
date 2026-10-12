use std::{
    collections::{HashMap, HashSet},
    path::{Component, Path},
};

use crate::error::{MarketForgeError, Result};

use super::{
    materialize_parallel::MaterializationPartitionPlan,
    unified_partition_executor::UnifiedPartitionResult,
    unified_partition_input::UnifiedPartitionInputPlan,
};

pub fn build_materialization_plans(
    logical_staging: &Path,
    input_plans: &[UnifiedPartitionInputPlan],
    results: &[UnifiedPartitionResult],
) -> Result<Vec<MaterializationPartitionPlan>> {
    if input_plans.is_empty() || results.is_empty() {
        return invalid("materialization planning requires logical partitions");
    }

    if input_plans.len() != results.len() {
        return invalid("logical partition plan/result count mismatch");
    }

    let staging = logical_staging
        .canonicalize()
        .map_err(|error| invalid_error(format!("cannot resolve logical staging: {error}")))?;

    let mut by_partition = HashMap::new();

    for result in results {
        if by_partition.insert(result.partition_id, result).is_some() {
            return invalid("duplicate logical partition result");
        }
    }

    let mut plans = Vec::with_capacity(input_plans.len());
    let mut seen_partitions = HashSet::new();

    for input in input_plans {
        let partition = input.partition;

        if !seen_partitions.insert(partition.partition_id) {
            return invalid("duplicate logical partition plan");
        }

        let result = by_partition.get(&partition.partition_id).ok_or_else(|| {
            invalid_error(format!(
                "missing logical partition result: {}",
                partition.partition_id
            ))
        })?;

        if result.start_ns != partition.start_ns || result.end_ns != partition.end_ns {
            return invalid("logical partition interval mismatch");
        }

        let mut timeline_files = Vec::with_capacity(result.files.len());

        for file in &result.files {
            let relative = Path::new(&file.path);

            if relative.is_absolute()
                || relative
                    .components()
                    .any(|component| !matches!(component, Component::Normal(_)))
            {
                return invalid(format!("unsafe logical timeline path: {}", file.path));
            }

            let expected_directory = format!("timeline/partition-{:06}", result.partition_id);

            if !relative.starts_with(&expected_directory) {
                return invalid("logical timeline file belongs to wrong partition");
            }

            let path = staging.join(relative);

            let canonical = path.canonicalize().map_err(|error| {
                invalid_error(format!(
                    "cannot resolve logical timeline file {}: {error}",
                    path.display()
                ))
            })?;

            if !canonical.starts_with(&staging) {
                return invalid("logical timeline file escapes staging");
            }

            if !canonical.is_file() {
                return invalid("logical timeline path is not a file");
            }

            timeline_files.push(canonical);
        }

        timeline_files.sort();

        let mut source_tasks = input
            .inputs
            .iter()
            .map(|task| (task.dataset_id, task.task_id))
            .collect::<Vec<_>>();

        source_tasks.sort_unstable();
        source_tasks.dedup();

        plans.push(MaterializationPartitionPlan {
            partition_id: result.partition_id,
            start_ns: result.start_ns,
            end_ns: result.end_ns,
            timeline_files,
            source_tasks,
        });
    }

    plans.sort_by_key(|plan| (plan.start_ns, plan.partition_id));

    Ok(plans)
}

fn invalid_error(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(message.into())
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(invalid_error(message))
}
