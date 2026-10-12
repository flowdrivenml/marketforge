#![cfg(feature = "process")]

use rust_decimal::Decimal;

use marketforge_engine::{
    book::SequencePolicy,
    canonical::Exchange,
    formats::depth::{DepthEventBoundary, DepthSourceEventMetadata},
    process::boundary::{
        BoundaryBookLevel, BoundaryBookState, BoundaryContinuity, DepthBoundaryTracker,
    },
};

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

fn state(price: i64) -> BoundaryBookState {
    BoundaryBookState {
        bids: vec![BoundaryBookLevel {
            price: Decimal::from(price),
            quantity_base: Some(Decimal::ONE),
            quantity_quote: None,
            quantity_contracts: None,
            order_count: None,
        }],
        asks: vec![BoundaryBookLevel {
            price: Decimal::from(price + 1),
            quantity_base: Some(Decimal::ONE),
            quantity_quote: None,
            quantity_contracts: None,
            order_count: None,
        }],
    }
}

fn tracker(policy: SequencePolicy) -> DepthBoundaryTracker {
    DepthBoundaryTracker::new(
        Exchange::Bybit,
        1,
        "BTCUSDT".to_owned(),
        "bybit:spot:BTCUSDT:l2".to_owned(),
        policy,
        Some(200),
    )
}

fn source(ordinal: u64, sequence: Option<u64>) -> DepthSourceEventMetadata {
    DepthSourceEventMetadata {
        source_event_ordinal: Some(ordinal),
        event_timestamp_ns: ordinal as i64 * 1_000_000,
        system_timestamp_ns: None,
        sequence_start: sequence,
        sequence_end: sequence,
    }
}

// -----------------------------------------------------------------------------
// Verified continuity
// -----------------------------------------------------------------------------

#[test]
fn consecutive_source_events_produce_verified_boundary() {
    let mut tracker = tracker(SequencePolicy::Consecutive);

    tracker.record_accepted(
        &source(1, Some(100)),
        DepthEventBoundary::Initialization,
        Some(state(100)),
    );

    tracker.record_accepted(&source(2, Some(101)), DepthEventBoundary::Changes, None);

    tracker.set_final_state(Some(state(101)));

    let manifest = tracker.finish();

    assert_eq!(manifest.continuity, BoundaryContinuity::Verified);
    assert_eq!(manifest.first_sequence, Some(100));
    assert_eq!(manifest.last_sequence, Some(101));

    assert!(manifest.initial.is_some());
    assert!(manifest.final_state.is_some());
}

// -----------------------------------------------------------------------------
// Missing initial snapshot
// -----------------------------------------------------------------------------

#[test]
fn missing_initial_snapshot_is_not_verified() {
    let mut tracker = tracker(SequencePolicy::Consecutive);

    tracker.record_accepted(&source(1, Some(100)), DepthEventBoundary::Changes, None);

    tracker.set_final_state(None);

    let manifest = tracker.finish();

    assert_eq!(manifest.continuity, BoundaryContinuity::Unverifiable);
    assert!(manifest.initial.is_none());
    assert!(manifest.final_state.is_none());
}

// -----------------------------------------------------------------------------
// Gap without recovery
// -----------------------------------------------------------------------------

#[test]
fn sequence_gap_invalidates_final_boundary() {
    let mut tracker = tracker(SequencePolicy::Consecutive);

    tracker.record_accepted(
        &source(1, Some(100)),
        DepthEventBoundary::Initialization,
        Some(state(100)),
    );

    tracker.invalidate();
    tracker.set_final_state(None);

    let manifest = tracker.finish();

    assert_eq!(manifest.continuity, BoundaryContinuity::GapDetected);
    assert!(manifest.final_state.is_none());
}

// -----------------------------------------------------------------------------
// Recovery through authoritative snapshot
// -----------------------------------------------------------------------------

#[test]
fn authoritative_snapshot_recovers_after_gap() {
    let mut tracker = tracker(SequencePolicy::Consecutive);

    tracker.record_accepted(
        &source(1, Some(100)),
        DepthEventBoundary::Initialization,
        Some(state(100)),
    );

    tracker.invalidate();

    tracker.record_accepted(
        &source(5, Some(200)),
        DepthEventBoundary::Initialization,
        Some(state(200)),
    );

    tracker.set_final_state(Some(state(201)));

    let manifest = tracker.finish();

    assert_eq!(
        manifest.continuity,
        BoundaryContinuity::RecoveredFromSnapshot
    );

    assert_eq!(manifest.first_sequence, Some(100));
    assert_eq!(manifest.last_sequence, Some(200));

    assert_eq!(manifest.final_state.as_ref().unwrap().sequence, Some(200));
}

// -----------------------------------------------------------------------------
// Unsequenced source
// -----------------------------------------------------------------------------

#[test]
fn unsequenced_source_cannot_claim_verified_continuity() {
    let mut tracker = tracker(SequencePolicy::Unsequenced);

    tracker.record_accepted(
        &source(1, None),
        DepthEventBoundary::Initialization,
        Some(state(100)),
    );

    tracker.record_accepted(&source(2, None), DepthEventBoundary::Changes, None);

    tracker.set_final_state(Some(state(101)));

    let manifest = tracker.finish();

    assert_eq!(manifest.continuity, BoundaryContinuity::Unverifiable);

    assert!(manifest.final_state.is_some());
}
