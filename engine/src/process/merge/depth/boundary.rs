use std::collections::BTreeMap;

use crate::{
    error::{MarketForgeError, Result},
    process::boundary::{BoundaryContinuity, BoundarySnapshot},
};

use super::inventory::DepthTaskInventory;

// -----------------------------------------------------------------------------
// Boundary relationship
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeBoundaryRelation {
    VerifiedContinuation,
    IndependentInitialization,
    SequenceGap,
    StateMismatch,
    Unverifiable,
}

// -----------------------------------------------------------------------------
// Reconciliation result
// -----------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct DepthBoundaryLink {
    pub dataset_id: u64,
    pub previous_task_id: u64,
    pub next_task_id: u64,

    pub relation: MergeBoundaryRelation,

    pub previous_timestamp_ns: Option<i64>,
    pub next_timestamp_ns: Option<i64>,

    pub previous_sequence: Option<u64>,
    pub next_sequence: Option<u64>,
    pub timestamp_delta_ns: Option<i64>,
}

// -----------------------------------------------------------------------------
// Reconcile adjacent tasks
// -----------------------------------------------------------------------------

pub fn reconcile_depth_boundaries(
    inventory: &[DepthTaskInventory],
) -> Result<Vec<DepthBoundaryLink>> {
    let mut datasets: BTreeMap<u64, Vec<&DepthTaskInventory>> = BTreeMap::new();

    for task in inventory {
        datasets.entry(task.dataset_id).or_default().push(task);
    }

    let mut links = Vec::new();

    for (dataset_id, mut tasks) in datasets {
        tasks.sort_by_key(|task| {
            (
                task.boundary.first_timestamp_ns.unwrap_or(i64::MAX),
                task.task_id,
            )
        });

        for pair in tasks.windows(2) {
            let previous = pair[0];
            let next = pair[1];

            validate_stream_identity(previous, next)?;

            let relation = classify_boundary(previous, next);

            let timestamp_delta_ns = match (
                previous.boundary.last_timestamp_ns,
                next.boundary.first_timestamp_ns,
            ) {
                (Some(previous), Some(next)) => next.checked_sub(previous),
                _ => None,
            };

            links.push(DepthBoundaryLink {
                dataset_id,
                previous_task_id: previous.task_id,
                next_task_id: next.task_id,

                relation,

                previous_timestamp_ns: previous.boundary.last_timestamp_ns,
                next_timestamp_ns: next.boundary.first_timestamp_ns,

                previous_sequence: previous.boundary.last_sequence,
                next_sequence: next.boundary.first_sequence,
                timestamp_delta_ns,
            });
        }
    }

    Ok(links)
}

// -----------------------------------------------------------------------------
// Stream identity
// -----------------------------------------------------------------------------

fn validate_stream_identity(
    previous: &DepthTaskInventory,
    next: &DepthTaskInventory,
) -> Result<()> {
    let a = &previous.boundary;
    let b = &next.boundary;

    if a.exchange != b.exchange
        || a.instrument_id != b.instrument_id
        || a.stream_id != b.stream_id
        || a.symbol != b.symbol
    {
        return Err(MarketForgeError::InvalidConfiguration(format!(
            "dataset {}: tasks {} and {} belong to different streams",
            previous.dataset_id, previous.task_id, next.task_id,
        )));
    }

    Ok(())
}

// -----------------------------------------------------------------------------
// Boundary classification
// -----------------------------------------------------------------------------

fn classify_boundary(
    previous: &DepthTaskInventory,
    next: &DepthTaskInventory,
) -> MergeBoundaryRelation {
    let a = &previous.boundary;
    let b = &next.boundary;

    let (Some(final_state), Some(initial_state)) = (&a.final_state, &b.initial) else {
        return MergeBoundaryRelation::Unverifiable;
    };

    // The next archive must begin with an independently reconstructable
    // initialization segment.
    let independent_initialization = next.segments.segments.first().is_some_and(|segment| {
        segment.initial_event_offset == 0 && segment.initial_level_count > 0
    });

    // Invalid or mismatched internal reconstruction prevents
    // continuity verification.
    if matches!(
        a.continuity,
        BoundaryContinuity::Invalid | BoundaryContinuity::BoundaryMismatch
    ) || matches!(
        b.continuity,
        BoundaryContinuity::Invalid | BoundaryContinuity::BoundaryMismatch
    ) {
        return MergeBoundaryRelation::StateMismatch;
    }

    // Native sequence IDs are available.
    if let (Some(previous_sequence), Some(next_sequence)) =
        (final_state.sequence, initial_state.sequence)
    {
        if previous_sequence == next_sequence {
            if comparable_snapshots(final_state, initial_state) {
                if final_state.fingerprint == initial_state.fingerprint {
                    return MergeBoundaryRelation::VerifiedContinuation;
                }

                return MergeBoundaryRelation::StateMismatch;
            }

            return MergeBoundaryRelation::Unverifiable;
        }

        if next_sequence > previous_sequence {
            return MergeBoundaryRelation::SequenceGap;
        }

        return MergeBoundaryRelation::Unverifiable;
    }

    // Without native sequences, we cannot prove continuity.
    // An authoritative initialization can still establish a new,
    // independently replayable reconstruction segment.
    if independent_initialization {
        return MergeBoundaryRelation::IndependentInitialization;
    }

    MergeBoundaryRelation::Unverifiable
}

// -----------------------------------------------------------------------------
// Snapshot comparability
// -----------------------------------------------------------------------------

fn comparable_snapshots(previous: &BoundarySnapshot, next: &BoundarySnapshot) -> bool {
    previous.coverage == next.coverage
}
