use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::error::{MarketForgeError, Result};

use super::{
    checkpoint::{CheckpointAvailability, InitialMergeCheckpoint},
    materialize_checkpoints::PhysicalMergeCheckpoint,
};

// -----------------------------------------------------------------------------
// Reconstruction availability
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PhysicalReconstructionAvailability {
    Available,
    Unavailable,
    PriorExternal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PhysicalInitialReconstruction {
    pub dataset_id: u64,

    pub availability: PhysicalReconstructionAvailability,

    pub first_physical_checkpoint_ns: Option<i64>,

    pub prior_canonical_checkpoint_ns: Option<i64>,
}

// -----------------------------------------------------------------------------
// Resolve physical reconstruction availability
// -----------------------------------------------------------------------------

pub fn resolve_physical_initial_reconstruction(
    initial_checkpoints: &[InitialMergeCheckpoint],
    physical_checkpoints: &[PhysicalMergeCheckpoint],
    start_ns: i64,
    end_ns: i64,
) -> Result<Vec<PhysicalInitialReconstruction>> {
    if start_ns >= end_ns {
        return invalid("invalid physical reconstruction interval");
    }

    let mut physical_first = HashMap::<u64, i64>::new();

    for checkpoint in physical_checkpoints {
        if checkpoint.timestamp_ns < start_ns || checkpoint.timestamp_ns >= end_ns {
            return invalid("physical reconstruction checkpoint outside interval");
        }

        physical_first
            .entry(checkpoint.dataset_id)
            .and_modify(|timestamp| {
                *timestamp = (*timestamp).min(checkpoint.timestamp_ns);
            })
            .or_insert(checkpoint.timestamp_ns);
    }

    let mut seen = HashSet::new();
    let mut result = Vec::with_capacity(initial_checkpoints.len());

    for initial in initial_checkpoints {
        let dataset_id = initial.checkpoint.dataset_id;

        if !seen.insert(dataset_id) {
            return invalid(format!(
                "duplicate initial reconstruction dataset: {dataset_id}"
            ));
        }

        let first_physical = physical_first.get(&dataset_id).copied();

        let prior_canonical = match initial.availability {
            CheckpointAvailability::Prior => Some(initial.checkpoint.timestamp_ns),

            CheckpointAvailability::InsideInterval => None,
        };

        let availability = match (first_physical, prior_canonical) {
            (Some(timestamp), _) if timestamp <= start_ns => {
                PhysicalReconstructionAvailability::Available
            }

            (_, Some(_)) => PhysicalReconstructionAvailability::PriorExternal,

            _ => PhysicalReconstructionAvailability::Unavailable,
        };

        result.push(PhysicalInitialReconstruction {
            dataset_id,
            availability,
            first_physical_checkpoint_ns: first_physical,
            prior_canonical_checkpoint_ns: prior_canonical,
        });
    }

    result.sort_by_key(|entry| entry.dataset_id);

    Ok(result)
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
