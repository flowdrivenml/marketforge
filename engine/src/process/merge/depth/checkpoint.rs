use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::error::{MarketForgeError, Result};

use super::inventory::DepthTaskInventory;

// -----------------------------------------------------------------------------
// Reconstruction checkpoint
// -----------------------------------------------------------------------------
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointAvailability {
    Prior,
    InsideInterval,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InitialMergeCheckpoint {
    pub checkpoint: MergeCheckpoint,
    pub availability: CheckpointAvailability,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MergeCheckpoint {
    pub dataset_id: u64,
    pub task_id: u64,
    pub segment_id: u64,

    pub timestamp_ns: i64,

    pub canonical_row_offset: u64,
    pub canonical_row_count: u64,
}

// -----------------------------------------------------------------------------
// Build checkpoint inventory
// -----------------------------------------------------------------------------

pub fn build_merge_checkpoints(inventory: &[DepthTaskInventory]) -> Result<Vec<MergeCheckpoint>> {
    let mut checkpoints = Vec::new();

    for task in inventory {
        for segment in &task.segments.segments {
            checkpoints.push(MergeCheckpoint {
                dataset_id: task.dataset_id,
                task_id: task.task_id,
                segment_id: segment.segment_id,

                timestamp_ns: segment.start_timestamp_ns,

                canonical_row_offset: segment.initial_event_offset,
                canonical_row_count: segment.initial_level_count,
            });
        }
    }

    checkpoints.sort_by_key(|checkpoint| {
        (
            checkpoint.dataset_id,
            checkpoint.timestamp_ns,
            checkpoint.task_id,
            checkpoint.segment_id,
        )
    });

    let mut seen = HashSet::new();

    for checkpoint in &checkpoints {
        if !seen.insert((
            checkpoint.dataset_id,
            checkpoint.task_id,
            checkpoint.segment_id,
        )) {
            return invalid(format!(
                "duplicate reconstruction checkpoint: dataset={}, task={}, segment={}",
                checkpoint.dataset_id, checkpoint.task_id, checkpoint.segment_id,
            ));
        }
    }

    Ok(checkpoints)
}

// -----------------------------------------------------------------------------
// Select latest checkpoint at or before a timestamp
// -----------------------------------------------------------------------------

pub fn select_merge_checkpoints(
    checkpoints: &[MergeCheckpoint],
    dataset_ids: &[u64],
    timestamp_ns: i64,
) -> Result<Vec<MergeCheckpoint>> {
    let mut selected: HashMap<u64, MergeCheckpoint> = HashMap::new();

    for checkpoint in checkpoints {
        if checkpoint.timestamp_ns > timestamp_ns {
            continue;
        }

        let entry = selected.entry(checkpoint.dataset_id);

        match entry {
            std::collections::hash_map::Entry::Vacant(vacant) => {
                vacant.insert(checkpoint.clone());
            }

            std::collections::hash_map::Entry::Occupied(mut occupied) => {
                let previous = occupied.get();

                if (
                    checkpoint.timestamp_ns,
                    checkpoint.task_id,
                    checkpoint.segment_id,
                ) > (previous.timestamp_ns, previous.task_id, previous.segment_id)
                {
                    occupied.insert(checkpoint.clone());
                }
            }
        }
    }

    let mut result = Vec::with_capacity(dataset_ids.len());

    for &dataset_id in dataset_ids {
        let checkpoint = selected.remove(&dataset_id).ok_or_else(|| {
            MarketForgeError::InvalidConfiguration(format!(
                "dataset {dataset_id}: no reconstruction checkpoint at or before timestamp {timestamp_ns}"
            ))
        })?;

        result.push(checkpoint);
    }

    Ok(result)
}

// -----------------------------------------------------------------------------
// Errors
// -----------------------------------------------------------------------------

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(MarketForgeError::InvalidConfiguration(message.into()))
}

pub fn resolve_initial_checkpoints(
    checkpoints: &[MergeCheckpoint],
    dataset_ids: &[u64],
    start_ns: i64,
    end_ns: i64,
) -> Result<Vec<InitialMergeCheckpoint>> {
    if start_ns >= end_ns {
        return invalid("invalid merge time interval");
    }

    let mut resolved = Vec::with_capacity(dataset_ids.len());

    for &dataset_id in dataset_ids {
        let dataset_checkpoints = checkpoints
            .iter()
            .filter(|checkpoint| checkpoint.dataset_id == dataset_id);

        let prior = dataset_checkpoints
            .clone()
            .filter(|checkpoint| checkpoint.timestamp_ns <= start_ns)
            .max_by_key(|checkpoint| {
                (
                    checkpoint.timestamp_ns,
                    checkpoint.task_id,
                    checkpoint.segment_id,
                )
            });

        if let Some(checkpoint) = prior {
            resolved.push(InitialMergeCheckpoint {
                checkpoint: checkpoint.clone(),
                availability: CheckpointAvailability::Prior,
            });

            continue;
        }

        let inside = dataset_checkpoints
            .filter(|checkpoint| {
                checkpoint.timestamp_ns > start_ns && checkpoint.timestamp_ns < end_ns
            })
            .min_by_key(|checkpoint| {
                (
                    checkpoint.timestamp_ns,
                    checkpoint.task_id,
                    checkpoint.segment_id,
                )
            });

        let checkpoint = inside.ok_or_else(|| {
            MarketForgeError::InvalidConfiguration(format!(
                "dataset {dataset_id}: no usable reconstruction checkpoint \
                 before or inside requested interval"
            ))
        })?;

        resolved.push(InitialMergeCheckpoint {
            checkpoint: checkpoint.clone(),
            availability: CheckpointAvailability::InsideInterval,
        });
    }

    Ok(resolved)
}
