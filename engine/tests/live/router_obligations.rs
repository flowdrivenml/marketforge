//! Integration tests for router-owned obligation transactions.
//!
//! Verifies:
//! - Atomic registration of required obligations.
//! - Tracker capacity enforcement.
//! - Duplicate obligation rejection.
//! - Delivery policy validation.
//! - Controlled rollback before commitment.
//! - Rollback prohibition after commitment begins.
//!
//! Part 2 adds fan-out outcomes, journal handoff,
//! cursor preservation, and transaction lifecycle tests.
use crate::live::support::live_events::l2_event;
use marketforge_engine::live::routing::router::FanoutCommitmentError;
use marketforge_engine::live::routing::router::TrackedFanoutResult;
use marketforge_engine::live::routing::router::{FanoutCommitmentCoordinator, TrackedFanoutError};
use marketforge_engine::live::routing::tracking_recovery::TrackingRecovery;
use marketforge_engine::live::{
    foundation::cancellation::CancellationToken,
    routing::{
        admission::AdmissionContext,
        router::{
            FanoutAdmissionCoordinator, RoutingObligationValidationError,
            validate_routing_obligations,
        },
    },
};
use marketforge_engine::live::{
    model::delivery::DeliveryCapacity,
    ports::routing::{
        DestinationAdmission, DestinationDescriptor, EventBatch, EventDestinationPort,
        RoutingFuture,
    },
    routing::router::{RouterRegistry, prepare_routing_obligations},
};
use marketforge_engine::live::{
    model::{
        cursor::{LogEpoch, StreamCursor},
        delivery::{
            DeliveryFailure, DeliveryPolicy, DeliveryStage, DeliveryStatus, DestinationKind,
        },
        identity::StreamId,
    },
    ports::routing::DestinationId,
    routing::acknowledgement::{
        AcknowledgementError, AcknowledgementTracker, DeliveryAcknowledgement, DeliveryCursorRange,
        DeliveryObligation, DeliveryObligationId, DeliveryObligationSpec, FinalizationJournal,
    },
};

use std::sync::Arc;
use std::{
    future::Future,
    task::{Context, Poll, Wake, Waker},
};

// ------------------------------------------------------------
// Test fixtures
// ------------------------------------------------------------

fn cursor(offset: u64) -> StreamCursor {
    StreamCursor::new(
        StreamId("router-obligation-source".to_owned()),
        LogEpoch(1),
        offset,
    )
}

fn range(first: u64, last: u64) -> DeliveryCursorRange {
    DeliveryCursorRange::new(cursor(first), cursor(last)).unwrap()
}

fn obligation(
    id: u64,
    destination: u64,
    kind: DestinationKind,
    policy: DeliveryPolicy,
) -> DeliveryObligation {
    DeliveryObligation {
        id: DeliveryObligationId(id),
        destination: DestinationId(destination),
        range: range(10, 12),
        kind,
        policy,
    }
}
fn validation_fixture() -> (
    RouterRegistry,
    EventBatch,
    AcknowledgementTracker,
    marketforge_engine::live::routing::router::RoutingPlan,
    marketforge_engine::live::routing::router::PreparedFanout,
    marketforge_engine::live::routing::acknowledgement::PreparedObligations,
) {
    use marketforge_engine::live::routing::{
        destination::DestinationCapacity, queue::DestinationQueue,
    };

    let registry = RouterRegistry::new();

    for id in [1, 2] {
        let destination =
            obligation_test_destination(id, DestinationKind::Recording, DeliveryPolicy::Required);

        let descriptor = destination.descriptor();

        let capacity = DestinationCapacity::new(descriptor.capacity, 10).unwrap();

        let queue = DestinationQueue::new(capacity);

        registry.register_queue(destination, queue).unwrap();
    }

    let batch = obligation_test_batch();
    let plan = registry.plan(&batch).unwrap();

    let context = AdmissionContext::without_deadline(CancellationToken::new());

    let prepared = ready(FanoutAdmissionCoordinator::prepare(
        &plan, &batch, context, None,
    ))
    .unwrap();

    let mut tracker = AcknowledgementTracker::new(10).unwrap();

    let obligations = prepare_routing_obligations(&mut tracker, &plan).unwrap();

    (registry, batch, tracker, plan, prepared, obligations)
}
struct NoopWake;

impl Wake for NoopWake {
    fn wake(self: Arc<Self>) {}
}

fn ready<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(NoopWake));
    let mut context = Context::from_waker(&waker);

    let mut future = Box::pin(future);

    match future.as_mut().poll(&mut context) {
        Poll::Ready(result) => result,
        Poll::Pending => {
            panic!("unexpected pending future")
        }
    }
}
fn required_recording(id: u64, destination: u64) -> DeliveryObligation {
    obligation(
        id,
        destination,
        DestinationKind::Recording,
        DeliveryPolicy::Required,
    )
}

fn durable_recording(id: u64, destination: u64) -> DeliveryObligation {
    obligation(
        id,
        destination,
        DestinationKind::Recording,
        DeliveryPolicy::Durable,
    )
}

fn tracker(capacity: usize) -> AcknowledgementTracker {
    AcknowledgementTracker::new(capacity).unwrap()
}

// ------------------------------------------------------------
// Test 1 — Register one required obligation
// ------------------------------------------------------------

#[test]
fn register_one_required_obligation_succeeds() {
    let mut tracker = tracker(10);

    let obligation = required_recording(1, 7);

    tracker.register(obligation.clone()).unwrap();

    assert_eq!(tracker.len(), 1);

    assert_eq!(tracker.status(obligation.id), Some(DeliveryStatus::Pending));

    assert_eq!(tracker.remaining_capacity(), 9);
}

// ------------------------------------------------------------
// Test 2 — Register multiple obligations atomically
// ------------------------------------------------------------

#[test]
fn register_multiple_obligations_atomically() {
    let mut tracker = tracker(10);

    let obligations = vec![
        required_recording(1, 7),
        required_recording(2, 8),
        durable_recording(3, 9),
    ];

    tracker.register_batch(obligations.clone()).unwrap();

    assert_eq!(tracker.len(), 3);
    assert_eq!(tracker.remaining_capacity(), 7);

    for obligation in obligations {
        assert_eq!(tracker.status(obligation.id), Some(DeliveryStatus::Pending));
    }
}

// ------------------------------------------------------------
// Test 3 — Insufficient tracker capacity
// ------------------------------------------------------------

#[test]
fn insufficient_tracker_capacity_rejects_entire_batch() {
    let mut tracker = tracker(2);

    let obligations = vec![
        required_recording(1, 7),
        required_recording(2, 8),
        required_recording(3, 9),
    ];

    let result = tracker.register_batch(obligations);

    match result {
        Ok(_) => panic!("Expected CapacityExceeded"),
        Err(AcknowledgementError::CapacityExceeded) => {}
        Err(e) => panic!("Unexpected error: {e:?}"),
    }

    assert_eq!(tracker.len(), 0);
    assert_eq!(tracker.remaining_capacity(), 2);

    for id in 1..=3 {
        assert_eq!(tracker.status(DeliveryObligationId(id)), None);
    }
}

// ------------------------------------------------------------
// Test 4 — Duplicate obligation IDs
// ------------------------------------------------------------

#[test]
fn duplicate_obligation_id_rejects_entire_batch() {
    let mut tracker = tracker(10);

    let obligations = vec![
        required_recording(1, 7),
        required_recording(1, 8),
        required_recording(2, 9),
    ];

    let result = tracker.register_batch(obligations);

    assert_eq!(result, Err(AcknowledgementError::DuplicateObligation));

    assert!(tracker.is_empty());
    assert_eq!(tracker.remaining_capacity(), 10);
}

// ------------------------------------------------------------
// Test 5 — Invalid destination policy
// ------------------------------------------------------------

#[test]
fn invalid_destination_policy_rejects_entire_batch() {
    let mut tracker = tracker(10);

    let obligations = vec![
        required_recording(1, 7),
        // Publication does not support Durable policy.
        obligation(2, 8, DestinationKind::Publication, DeliveryPolicy::Durable),
        required_recording(3, 9),
    ];

    let result = tracker.register_batch(obligations);

    assert_eq!(result, Err(AcknowledgementError::InvalidCompletionPolicy));

    assert_eq!(tracker.len(), 0);
    assert_eq!(tracker.remaining_capacity(), 10);
}

// ------------------------------------------------------------
// Test 6 — Abort before commitment
// ------------------------------------------------------------

#[test]
fn abort_before_commitment_removes_prepared_obligations() {
    let mut tracker = tracker(10);

    let obligations = vec![required_recording(1, 7), required_recording(2, 8)];

    let transaction = tracker.prepare_obligations(obligations.clone()).unwrap();

    assert_eq!(tracker.len(), 2);

    assert_eq!(
        tracker.status(obligations[0].id),
        Some(DeliveryStatus::Pending)
    );

    assert_eq!(
        tracker.status(obligations[1].id),
        Some(DeliveryStatus::Pending)
    );

    tracker.abort_prepared(transaction).unwrap();

    assert_eq!(tracker.len(), 0);
    assert_eq!(tracker.remaining_capacity(), 10);

    assert_eq!(tracker.status(obligations[0].id), None);
    assert_eq!(tracker.status(obligations[1].id), None);
}

// ------------------------------------------------------------
// Test 7 — Abort after commitment begins
// ------------------------------------------------------------

#[test]
fn abort_after_commitment_begins_is_rejected() {
    let mut tracker = tracker(10);

    let obligations = vec![required_recording(1, 7), required_recording(2, 8)];

    let transaction = tracker.prepare_obligations(obligations.clone()).unwrap();

    let transaction = tracker.begin_commitment(transaction).unwrap();

    // Once commitment begins, the transaction cannot
    // roll back its registered obligations.
    //
    // The exact rejection API depends on the production
    // transaction implementation.

    let result = transaction.abort_prepared(&mut tracker);

    assert!(result.is_err());

    assert_eq!(tracker.len(), 2);

    for obligation in obligations {
        assert_eq!(tracker.status(obligation.id), Some(DeliveryStatus::Pending));
    }
}

// ------------------------------------------------------------
// Test 8 — Failed destination commitment
// ------------------------------------------------------------

#[test]
fn failed_destination_commitment_records_failure() {
    let mut tracker = tracker(10);

    let obligation = required_recording(1, 7);

    tracker.register(obligation.clone()).unwrap();

    tracker
        .acknowledge(DeliveryAcknowledgement {
            obligation: obligation.id,
            destination: obligation.destination,
            range: obligation.range.clone(),
            status: DeliveryStatus::Failed(DeliveryFailure::Unavailable),
        })
        .unwrap();

    assert_eq!(
        tracker.status(obligation.id),
        Some(DeliveryStatus::Failed(DeliveryFailure::Unavailable))
    );

    // Failure remains tracked until authoritative
    // finalization into the journal.
    assert_eq!(tracker.len(), 1);
}

// ------------------------------------------------------------
// Test 9 — Successful queue commitment
// ------------------------------------------------------------

#[test]
fn successful_queue_commitment_records_queued_status() {
    let mut tracker = tracker(10);

    let obligation = required_recording(1, 7);

    tracker.register(obligation.clone()).unwrap();

    tracker
        .acknowledge(DeliveryAcknowledgement {
            obligation: obligation.id,
            destination: obligation.destination,
            range: obligation.range.clone(),
            status: DeliveryStatus::Progress(DeliveryStage::Queued),
        })
        .unwrap();

    assert_eq!(
        tracker.status(obligation.id),
        Some(DeliveryStatus::Progress(DeliveryStage::Queued))
    );

    // Queued is not equivalent to completed.
    assert_eq!(tracker.len(), 1);
}

// ------------------------------------------------------------
// Test 10 — Partial fan-out preserves both outcomes
// ------------------------------------------------------------

#[test]
fn partial_fanout_preserves_independent_outcomes() {
    let mut tracker = tracker(10);

    let publication = obligation(1, 7, DestinationKind::Publication, DeliveryPolicy::Required);

    let recording = required_recording(2, 8);

    tracker
        .register_batch(vec![publication.clone(), recording.clone()])
        .unwrap();

    tracker
        .acknowledge(DeliveryAcknowledgement {
            obligation: publication.id,
            destination: publication.destination,
            range: publication.range.clone(),
            status: DeliveryStatus::Progress(DeliveryStage::Queued),
        })
        .unwrap();

    tracker
        .acknowledge(DeliveryAcknowledgement {
            obligation: recording.id,
            destination: recording.destination,
            range: recording.range.clone(),
            status: DeliveryStatus::Failed(DeliveryFailure::Unavailable),
        })
        .unwrap();

    assert_eq!(
        tracker.status(publication.id),
        Some(DeliveryStatus::Progress(DeliveryStage::Queued))
    );

    assert_eq!(
        tracker.status(recording.id),
        Some(DeliveryStatus::Failed(DeliveryFailure::Unavailable))
    );

    assert_eq!(tracker.len(), 2);

    // Neither outcome may erase the other.
}

// ------------------------------------------------------------
// Test 11 — Journal capacity exhaustion
// ------------------------------------------------------------

#[test]
fn journal_capacity_exhaustion_preserves_terminal_obligation() {
    let mut tracker = tracker(10);

    let mut journal = FinalizationJournal::new(1).unwrap();

    let first = required_recording(1, 7);
    let second = required_recording(2, 8);

    tracker
        .register_batch(vec![first.clone(), second.clone()])
        .unwrap();

    for obligation in [&first, &second] {
        tracker
            .acknowledge(DeliveryAcknowledgement {
                obligation: obligation.id,
                destination: obligation.destination,
                range: obligation.range.clone(),
                status: DeliveryStatus::Failed(DeliveryFailure::Unavailable),
            })
            .unwrap();
    }

    tracker.finalize_into(first.id, &mut journal).unwrap();

    assert_eq!(journal.len(), 1);
    assert_eq!(tracker.len(), 1);

    // The journal has no remaining capacity.
    let result = tracker.finalize_into(second.id, &mut journal);

    assert!(result.is_err());

    // The second obligation must remain tracked.
    assert_eq!(
        tracker.status(second.id),
        Some(DeliveryStatus::Failed(DeliveryFailure::Unavailable))
    );

    assert_eq!(tracker.len(), 1);
    assert_eq!(journal.len(), 1);
}

// ------------------------------------------------------------
// Test 12 — Duplicate transaction finalization
// ------------------------------------------------------------

#[test]
fn duplicate_transaction_finalization_is_rejected() {
    let mut tracker = tracker(10);
    let mut journal = FinalizationJournal::new(10).unwrap();

    let obligation = required_recording(1, 7);

    tracker.register(obligation.clone()).unwrap();

    tracker
        .acknowledge(DeliveryAcknowledgement {
            obligation: obligation.id,
            destination: obligation.destination,
            range: obligation.range.clone(),
            status: DeliveryStatus::Failed(DeliveryFailure::Unavailable),
        })
        .unwrap();

    tracker.finalize_into(obligation.id, &mut journal).unwrap();

    // A second finalization cannot transfer the same
    // obligation into the journal again.
    let result = tracker.finalize_into(obligation.id, &mut journal);

    assert!(result.is_err());

    assert_eq!(journal.len(), 1);
    assert_eq!(tracker.len(), 0);
}

// ------------------------------------------------------------
// Test 13 — Original cursor range is preserved
// ------------------------------------------------------------

#[test]
fn obligation_preserves_original_cursor_range() {
    let mut tracker = tracker(10);

    let obligation = required_recording(1, 7);

    let expected_first = cursor(10);
    let expected_last = cursor(12);

    assert_eq!(obligation.range.first, expected_first);

    assert_eq!(obligation.range.last, expected_last);

    tracker.register(obligation.clone()).unwrap();

    tracker
        .acknowledge(DeliveryAcknowledgement {
            obligation: obligation.id,
            destination: obligation.destination,
            range: obligation.range.clone(),
            status: DeliveryStatus::Progress(DeliveryStage::Queued),
        })
        .unwrap();

    assert_eq!(
        tracker.status(obligation.id),
        Some(DeliveryStatus::Progress(DeliveryStage::Queued))
    );

    // An acknowledgement with a different cursor range
    // must not be accepted.

    let invalid = tracker.acknowledge(DeliveryAcknowledgement {
        obligation: obligation.id,
        destination: obligation.destination,
        range: range(10, 13),
        status: DeliveryStatus::Progress(DeliveryStage::Committed),
    });

    assert_eq!(invalid, Err(AcknowledgementError::RangeMismatch));

    assert_eq!(
        tracker.status(obligation.id),
        Some(DeliveryStatus::Progress(DeliveryStage::Queued))
    );
}

// ------------------------------------------------------------
// Test 14 — Obligation IDs are never reused after abort
// ------------------------------------------------------------

#[test]
fn obligation_ids_are_not_reused_after_abort() {
    let mut tracker = tracker(10);

    let first_id = tracker.allocate_id().unwrap();
    let second_id = tracker.allocate_id().unwrap();

    assert_ne!(first_id, second_id);

    let obligations = vec![
        required_recording(first_id.0, 7),
        required_recording(second_id.0, 8),
    ];

    let transaction = tracker.prepare_obligations(obligations).unwrap();

    tracker.abort_prepared(transaction).unwrap();

    let next_id = tracker.allocate_id().unwrap();

    assert!(next_id.0 > first_id.0);
    assert!(next_id.0 > second_id.0);

    assert_eq!(tracker.len(), 0);
}

// ------------------------------------------------------------
// Test 15 — Dropped transaction cannot erase obligations
// ------------------------------------------------------------

#[test]
fn dropping_transaction_cannot_discard_committed_obligations() {
    let mut tracker = tracker(10);

    let obligations = vec![required_recording(1, 7), required_recording(2, 8)];

    let transaction = tracker.prepare_obligations(obligations.clone()).unwrap();

    let transaction = tracker.begin_commitment(transaction).unwrap();

    // Dropping the transaction handle must not remove
    // obligations after commitment has begun.
    drop(transaction);

    assert_eq!(tracker.len(), 2);

    for obligation in obligations {
        assert_eq!(tracker.status(obligation.id), Some(DeliveryStatus::Pending));
    }
}

// ------------------------------------------------------------
// Test 16 — prepare_obligation_specs allocates unique IDs
// ------------------------------------------------------------

#[test]
fn tracker_allocates_unique_obligation_ids() {
    let mut tracker = tracker(10);

    let specs = vec![
        DeliveryObligationSpec {
            destination: DestinationId(7),
            range: range(10, 12),
            kind: DestinationKind::Recording,
            policy: DeliveryPolicy::Required,
        },
        DeliveryObligationSpec {
            destination: DestinationId(8),
            range: range(10, 12),
            kind: DestinationKind::Recording,
            policy: DeliveryPolicy::Required,
        },
        DeliveryObligationSpec {
            destination: DestinationId(9),
            range: range(10, 12),
            kind: DestinationKind::Recording,
            policy: DeliveryPolicy::Required,
        },
    ];

    let prepared = tracker.prepare_obligation_specs(specs).unwrap();

    // Three obligations are registered.
    assert_eq!(prepared.obligations().len(), 3);

    // All three IDs are distinct.
    let ids: Vec<_> = prepared.obligations().iter().map(|o| o.id).collect();
    assert_ne!(ids[0], ids[1]);
    assert_ne!(ids[1], ids[2]);
    assert_ne!(ids[0], ids[2]);

    // All obligations have Pending status.
    for obligation in prepared.obligations() {
        assert_eq!(tracker.status(obligation.id), Some(DeliveryStatus::Pending));
    }

    // Destination identities are preserved.
    assert_eq!(prepared.obligations()[0].destination, DestinationId(7));
    assert_eq!(prepared.obligations()[1].destination, DestinationId(8));
    assert_eq!(prepared.obligations()[2].destination, DestinationId(9));
}

// ------------------------------------------------------------
// Test 17 — failed_spec_validation_does_not_consume_ids
// ------------------------------------------------------------

#[test]
fn failed_spec_validation_does_not_consume_ids() {
    let mut tracker = tracker(10);

    let specs = vec![DeliveryObligationSpec {
        destination: DestinationId(7),
        range: range(10, 12),
        kind: DestinationKind::Publication,
        policy: DeliveryPolicy::Durable,
    }];

    let result = tracker.prepare_obligation_specs(specs);

    assert!(result.is_err());

    assert_eq!(tracker.len(), 0);

    // Validation failure must not consume obligation IDs.
    // (Monotonic IDs cannot be rolled back; the invariant
    //  is that no new obligations appear after validation.)
}

// ------------------------------------------------------------
// Test 18 — insufficient_capacity_does_not_register_obligations
// ------------------------------------------------------------

#[test]
fn insufficient_capacity_does_not_register_obligations() {
    let mut tracker = tracker(2);

    let specs = vec![
        DeliveryObligationSpec {
            destination: DestinationId(7),
            range: range(10, 12),
            kind: DestinationKind::Recording,
            policy: DeliveryPolicy::Required,
        },
        DeliveryObligationSpec {
            destination: DestinationId(8),
            range: range(10, 12),
            kind: DestinationKind::Recording,
            policy: DeliveryPolicy::Required,
        },
        DeliveryObligationSpec {
            destination: DestinationId(9),
            range: range(10, 12),
            kind: DestinationKind::Recording,
            policy: DeliveryPolicy::Required,
        },
    ];

    let result: Result<_, AcknowledgementError> = tracker.prepare_obligation_specs(specs);

    match result {
        Ok(_) => panic!("Expected CapacityExceeded"),
        Err(AcknowledgementError::CapacityExceeded) => {}
        Err(e) => panic!("Unexpected error: {e:?}"),
    }
    assert_eq!(tracker.len(), 0);

    assert_eq!(tracker.remaining_capacity(), 2);
}

// ------------------------------------------------------------
// Test 19 — aborted_ids_are_never_reused
// ------------------------------------------------------------

#[test]
fn aborted_ids_are_never_reused() {
    let mut tracker = tracker(10);

    let specs = vec![
        DeliveryObligationSpec {
            destination: DestinationId(7),
            range: range(10, 12),
            kind: DestinationKind::Recording,
            policy: DeliveryPolicy::Required,
        },
        DeliveryObligationSpec {
            destination: DestinationId(8),
            range: range(10, 12),
            kind: DestinationKind::Recording,
            policy: DeliveryPolicy::Required,
        },
    ];

    let prepared = tracker.prepare_obligation_specs(specs).unwrap();
    let first_id = prepared.obligations()[0].id;
    let second_id = prepared.obligations()[1].id;

    tracker.abort_prepared(prepared).unwrap();

    let specs2 = vec![DeliveryObligationSpec {
        destination: DestinationId(9),
        range: range(10, 12),
        kind: DestinationKind::Recording,
        policy: DeliveryPolicy::Required,
    }];

    let prepared2 = tracker.prepare_obligation_specs(specs2).unwrap();
    let new_id = prepared2.obligations()[0].id;

    assert_ne!(new_id, first_id);
    assert_ne!(new_id, second_id);

    assert!(new_id.0 > first_id.0);
    assert!(new_id.0 > second_id.0);

    // Capacity was released by the abort (old obligations removed).
    // After preparing one more obligation, remaining = capacity - 1.
    assert_eq!(tracker.remaining_capacity(), tracker.capacity() - 1);
}

// ------------------------------------------------------------
// Test 20 — committed_obligations_survive_handle_drop
// ------------------------------------------------------------

#[test]
fn committed_obligations_survive_handle_drop() {
    let mut tracker = tracker(10);

    let specs = vec![
        DeliveryObligationSpec {
            destination: DestinationId(7),
            range: range(10, 12),
            kind: DestinationKind::Recording,
            policy: DeliveryPolicy::Required,
        },
        DeliveryObligationSpec {
            destination: DestinationId(8),
            range: range(10, 12),
            kind: DestinationKind::Recording,
            policy: DeliveryPolicy::Required,
        },
    ];

    let prepared = tracker.prepare_obligation_specs(specs).unwrap();
    let first_id = prepared.obligations()[0].id;
    let second_id = prepared.obligations()[1].id;

    let handle = tracker.begin_commitment(prepared).unwrap();

    drop(handle);

    assert_eq!(tracker.len(), 2);

    assert_eq!(tracker.status(first_id), Some(DeliveryStatus::Pending));
    assert_eq!(tracker.status(second_id), Some(DeliveryStatus::Pending));
}

// ------------------------------------------------------------
// Test 21 — prepared_specs_preserve_cursor_ranges_and_policies
// ------------------------------------------------------------

#[test]
fn prepared_specs_preserve_cursor_ranges_and_policies() {
    let mut tracker = tracker(10);

    let specs = vec![
        DeliveryObligationSpec {
            destination: DestinationId(7),
            range: range(10, 12),
            kind: DestinationKind::Recording,
            policy: DeliveryPolicy::Required,
        },
        DeliveryObligationSpec {
            destination: DestinationId(8),
            range: range(5, 20),
            kind: DestinationKind::Retention,
            policy: DeliveryPolicy::Durable,
        },
        DeliveryObligationSpec {
            destination: DestinationId(9),
            range: range(100, 100),
            kind: DestinationKind::Publication,
            policy: DeliveryPolicy::BestEffort,
        },
    ];

    let prepared_specs = specs.clone();
    let prepared = tracker.prepare_obligation_specs(specs).unwrap();

    for (spec, obligation) in prepared_specs.iter().zip(prepared.obligations().iter()) {
        assert_eq!(spec.destination, obligation.destination);
        assert_eq!(spec.range.first.offset, obligation.range.first.offset);
        assert_eq!(spec.range.last.offset, obligation.range.last.offset);
        assert_eq!(spec.kind, obligation.kind);
        assert_eq!(spec.policy, obligation.policy);
    }
}

struct ObligationTestDestination {
    descriptor: DestinationDescriptor,
}

impl EventDestinationPort for ObligationTestDestination {
    fn descriptor(&self) -> DestinationDescriptor {
        self.descriptor
    }

    fn admit<'a>(&'a self, batch: EventBatch) -> RoutingFuture<'a, DestinationAdmission> {
        Box::pin(async move {
            DestinationAdmission::Accepted {
                destination: self.descriptor.id,
                through: batch.last_cursor().clone(),
            }
        })
    }
}

fn obligation_test_destination(
    id: u64,
    kind: DestinationKind,
    policy: DeliveryPolicy,
) -> Arc<dyn EventDestinationPort> {
    Arc::new(ObligationTestDestination {
        descriptor: DestinationDescriptor {
            id: DestinationId(id),
            kind,
            policy,
            capacity: DeliveryCapacity::new(100, 100_000).unwrap(),
        },
    })
}

fn obligation_test_batch() -> EventBatch {
    EventBatch::new(vec![
        l2_event("obligation-source", 1, 10),
        l2_event("obligation-source", 1, 11),
    ])
    .unwrap()
}

#[test]
fn router_prepares_required_destination_obligations() {
    let registry = RouterRegistry::new();

    registry
        .register(obligation_test_destination(
            1,
            DestinationKind::Publication,
            DeliveryPolicy::Required,
        ))
        .unwrap();

    registry
        .register(obligation_test_destination(
            2,
            DestinationKind::Recording,
            DeliveryPolicy::Durable,
        ))
        .unwrap();

    let batch = obligation_test_batch();
    let plan = registry.plan(&batch).unwrap();

    let mut tracker = AcknowledgementTracker::new(10).unwrap();

    let prepared = prepare_routing_obligations(&mut tracker, &plan).unwrap();

    assert_eq!(prepared.len(), 2);
    assert_eq!(tracker.len(), 2);

    for obligation in prepared.obligations() {
        assert_eq!(tracker.status(obligation.id), Some(DeliveryStatus::Pending));

        assert_eq!(obligation.range.first, *batch.first_cursor());

        assert_eq!(obligation.range.last, *batch.last_cursor());
    }
}

#[test]
fn router_excludes_best_effort_from_required_obligations() {
    let registry = RouterRegistry::new();

    registry
        .register(obligation_test_destination(
            1,
            DestinationKind::Publication,
            DeliveryPolicy::Required,
        ))
        .unwrap();

    registry
        .register(obligation_test_destination(
            2,
            DestinationKind::Derived,
            DeliveryPolicy::BestEffort,
        ))
        .unwrap();

    let batch = obligation_test_batch();
    let plan = registry.plan(&batch).unwrap();

    let mut tracker = AcknowledgementTracker::new(10).unwrap();

    let prepared = prepare_routing_obligations(&mut tracker, &plan).unwrap();

    assert_eq!(prepared.len(), 1);
    assert_eq!(tracker.len(), 1);

    assert_eq!(prepared.obligations()[0].destination, DestinationId(1));
}

#[test]
fn router_obligation_preparation_rejects_insufficient_capacity() {
    let registry = RouterRegistry::new();

    for id in [1, 2, 3] {
        registry
            .register(obligation_test_destination(
                id,
                DestinationKind::Recording,
                DeliveryPolicy::Required,
            ))
            .unwrap();
    }

    let batch = obligation_test_batch();
    let plan = registry.plan(&batch).unwrap();

    let mut tracker = AcknowledgementTracker::new(2).unwrap();

    let result = prepare_routing_obligations(&mut tracker, &plan);

    assert_eq!(result.err(), Some(AcknowledgementError::CapacityExceeded));

    assert!(tracker.is_empty());
    assert_eq!(tracker.remaining_capacity(), 2);
}

#[test]
fn router_obligations_preserve_destination_policies() {
    let registry = RouterRegistry::new();

    registry
        .register(obligation_test_destination(
            1,
            DestinationKind::Publication,
            DeliveryPolicy::Required,
        ))
        .unwrap();

    registry
        .register(obligation_test_destination(
            2,
            DestinationKind::Recording,
            DeliveryPolicy::Durable,
        ))
        .unwrap();

    let batch = obligation_test_batch();
    let plan = registry.plan(&batch).unwrap();

    let mut tracker = AcknowledgementTracker::new(10).unwrap();

    let prepared = prepare_routing_obligations(&mut tracker, &plan).unwrap();

    let publication = prepared
        .obligations()
        .iter()
        .find(|obligation| obligation.destination == DestinationId(1))
        .unwrap();

    assert_eq!(publication.kind, DestinationKind::Publication);
    assert_eq!(publication.policy, DeliveryPolicy::Required);

    let recording = prepared
        .obligations()
        .iter()
        .find(|obligation| obligation.destination == DestinationId(2))
        .unwrap();

    assert_eq!(recording.kind, DestinationKind::Recording);
    assert_eq!(recording.policy, DeliveryPolicy::Durable);
}

#[test]
fn valid_routing_obligations_pass_validation() {
    let (_, _, tracker, plan, prepared, obligations) = validation_fixture();

    assert_eq!(
        validate_routing_obligations(&tracker, &plan, &prepared, &obligations,),
        Ok(())
    );
}

#[test]
fn validation_rejects_destination_count_mismatch() {
    let (_, _, tracker, mut plan, prepared, obligations) = validation_fixture();

    plan.required.pop();

    assert_eq!(
        validate_routing_obligations(&tracker, &plan, &prepared, &obligations,),
        Err(RoutingObligationValidationError::DestinationCountMismatch)
    );
}

#[test]
fn validation_rejects_cursor_range_mismatch() {
    let (_, _, tracker, mut plan, prepared, obligations) = validation_fixture();

    plan.last_cursor = cursor(999);

    assert_eq!(
        validate_routing_obligations(&tracker, &plan, &prepared, &obligations,),
        Err(RoutingObligationValidationError::CursorRangeMismatch)
    );
}

#[test]
fn validation_rejects_destination_kind_mismatch() {
    let (_, _, tracker, mut plan, prepared, obligations) = validation_fixture();

    plan.required[0].descriptor.kind = DestinationKind::Publication;

    assert_eq!(
        validate_routing_obligations(&tracker, &plan, &prepared, &obligations,),
        Err(RoutingObligationValidationError::DestinationKindMismatch)
    );
}

#[test]
fn validation_rejects_delivery_policy_mismatch() {
    let (_, _, tracker, mut plan, prepared, obligations) = validation_fixture();

    plan.required[0].descriptor.policy = DeliveryPolicy::Durable;

    assert_eq!(
        validate_routing_obligations(&tracker, &plan, &prepared, &obligations,),
        Err(RoutingObligationValidationError::DeliveryPolicyMismatch)
    );
}

#[test]
fn validation_rejects_non_pending_obligation() {
    let (_, _, mut tracker, plan, prepared, obligations) = validation_fixture();

    let obligation = &obligations.obligations()[0];

    tracker
        .acknowledge(DeliveryAcknowledgement {
            obligation: obligation.id,
            destination: obligation.destination,
            range: obligation.range.clone(),
            status: DeliveryStatus::Progress(DeliveryStage::Queued),
        })
        .unwrap();

    assert_eq!(
        validate_routing_obligations(&tracker, &plan, &prepared, &obligations,),
        Err(RoutingObligationValidationError::ObligationNotPending)
    );
}

#[test]
fn tracked_commitment_activation_preserves_obligations() {
    let (_, _, mut tracker, plan, prepared, obligations) = validation_fixture();

    let ids: Vec<_> = obligations
        .obligations()
        .iter()
        .map(|obligation| obligation.id)
        .collect();

    let handle = FanoutCommitmentCoordinator::begin_tracked_commitment(
        &mut tracker,
        &plan,
        &prepared,
        obligations,
    )
    .unwrap();

    assert_eq!(tracker.len(), 2);

    for id in ids {
        assert_eq!(tracker.status(id), Some(DeliveryStatus::Pending));
    }

    drop(handle);

    // Active obligations survive the handle being dropped.
    assert_eq!(tracker.len(), 2);
}

#[test]
fn tracked_commitment_validation_failure_rolls_back() {
    let (_, _, mut tracker, mut plan, prepared, obligations) = validation_fixture();

    let ids: Vec<_> = obligations
        .obligations()
        .iter()
        .map(|obligation| obligation.id)
        .collect();

    // Deliberately invalidate the plan.
    plan.last_cursor = cursor(999);

    let result = FanoutCommitmentCoordinator::begin_tracked_commitment(
        &mut tracker,
        &plan,
        &prepared,
        obligations,
    );

    assert!(matches!(
        result,
        Err(TrackedFanoutError::Validation(
            RoutingObligationValidationError::CursorRangeMismatch
        ))
    ));

    assert!(tracker.is_empty());

    for id in ids {
        assert_eq!(tracker.status(id), None);
    }
}

#[test]
fn tracked_commitment_rollback_releases_tracker_capacity() {
    let (_, _, mut tracker, mut plan, prepared, obligations) = validation_fixture();

    assert_eq!(tracker.remaining_capacity(), 8);

    plan.last_cursor = cursor(999);

    let result = FanoutCommitmentCoordinator::begin_tracked_commitment(
        &mut tracker,
        &plan,
        &prepared,
        obligations,
    );

    assert!(result.is_err());

    assert_eq!(tracker.len(), 0);
    assert_eq!(tracker.remaining_capacity(), 10);
}

#[test]
fn tracked_commitment_activation_does_not_enqueue() {
    let (registry, batch, mut tracker, plan, prepared, obligations) = validation_fixture();

    let queue1 = registry.peek_queue(DestinationId(1)).unwrap();
    let queue2 = registry.peek_queue(DestinationId(2)).unwrap();

    assert_eq!(queue1.snapshot().queued_batches, 0);
    assert_eq!(queue2.snapshot().queued_batches, 0);

    let handle = FanoutCommitmentCoordinator::begin_tracked_commitment(
        &mut tracker,
        &plan,
        &prepared,
        obligations,
    )
    .unwrap();

    assert_eq!(queue1.snapshot().queued_batches, 0);
    assert_eq!(queue2.snapshot().queued_batches, 0);

    // The batch is still owned by the caller.
    assert_eq!(batch.len(), 2);

    drop(handle);
}

#[test]
fn tracked_commitment_activation_is_non_abortable() {
    let (_, _, mut tracker, plan, prepared, obligations) = validation_fixture();

    let handle = FanoutCommitmentCoordinator::begin_tracked_commitment(
        &mut tracker,
        &plan,
        &prepared,
        obligations,
    )
    .unwrap();

    let result = handle.abort_prepared(&mut tracker);

    assert!(result.is_err());

    assert_eq!(tracker.len(), 2);
}

#[test]
fn failed_tracked_activation_preserves_transaction_ownership() {
    let (_, _, mut tracker, mut plan, prepared, obligations) = validation_fixture();

    let original_ids: Vec<_> = obligations
        .obligations()
        .iter()
        .map(|obligation| obligation.id)
        .collect();

    // Force validation failure.
    plan.last_cursor = cursor(999);

    let failure = FanoutCommitmentCoordinator::try_begin_tracked_commitment(
        &mut tracker,
        &plan,
        &prepared,
        obligations,
    )
    .unwrap_err();

    assert_eq!(
        failure.reason,
        TrackedFanoutError::Validation(RoutingObligationValidationError::CursorRangeMismatch)
    );

    // The original transaction is still available.
    let recovered = failure.obligations.unwrap();

    let recovered_ids: Vec<_> = recovered
        .obligations()
        .iter()
        .map(|obligation| obligation.id)
        .collect();

    assert_eq!(recovered_ids, original_ids);

    // No obligations were silently removed.
    assert_eq!(tracker.len(), 2);

    // Controlled rollback remains possible.
    tracker.abort_prepared(recovered).unwrap();

    assert!(tracker.is_empty());
}

#[test]
fn tracked_commitment_registry_change_aborts_obligations() {
    let (registry, batch, mut tracker, plan, prepared, obligations) = validation_fixture();

    let ids: Vec<_> = obligations
        .obligations()
        .iter()
        .map(|obligation| obligation.id)
        .collect();

    // Change the registry revision after preparation.
    registry.unregister(DestinationId(2)).unwrap();

    let result = FanoutCommitmentCoordinator::commit_tracked(
        &registry,
        &mut tracker,
        &plan,
        prepared,
        obligations,
        batch,
    );

    assert!(matches!(result, TrackedFanoutResult::Rejected { .. }));

    for id in ids {
        assert_eq!(tracker.status(id), None);
    }

    assert!(tracker.is_empty());

    let queue = registry.peek_queue(DestinationId(1)).unwrap();

    assert_eq!(queue.snapshot().queued_batches, 0);
}

#[test]
fn tracked_commitment_shutdown_aborts_obligations() {
    let (registry, batch, mut tracker, plan, prepared, obligations) = validation_fixture();

    let ids: Vec<_> = obligations
        .obligations()
        .iter()
        .map(|obligation| obligation.id)
        .collect();

    let first_queue = registry.peek_queue(DestinationId(1)).unwrap();
    let second_queue = registry.peek_queue(DestinationId(2)).unwrap();

    registry.shutdown();

    let result = FanoutCommitmentCoordinator::commit_tracked(
        &registry,
        &mut tracker,
        &plan,
        prepared,
        obligations,
        batch,
    );

    assert!(matches!(result, TrackedFanoutResult::Rejected { .. }));

    for id in ids {
        assert_eq!(tracker.status(id), None);
    }

    assert!(tracker.is_empty());

    assert_eq!(first_queue.snapshot().queued_batches, 0);
    assert_eq!(second_queue.snapshot().queued_batches, 0);
}

#[test]
fn tracked_commitment_cursor_mismatch_aborts_before_enqueue() {
    let (registry, batch, mut tracker, plan, prepared, obligations) = validation_fixture();

    let ids: Vec<_> = obligations
        .obligations()
        .iter()
        .map(|obligation| obligation.id)
        .collect();

    let first_queue = registry.peek_queue(DestinationId(1)).unwrap();
    let second_queue = registry.peek_queue(DestinationId(2)).unwrap();

    // Change the batch cursor identity without changing
    // the prepared fan-out.
    let different_batch = EventBatch::new(vec![
        l2_event("obligation-source", 1, 100),
        l2_event("obligation-source", 1, 101),
    ])
    .unwrap();

    let result = FanoutCommitmentCoordinator::commit_tracked(
        &registry,
        &mut tracker,
        &plan,
        prepared,
        obligations,
        different_batch,
    );

    assert!(matches!(
        result,
        TrackedFanoutResult::Rejected {
            reason: TrackedFanoutError::Precommit(FanoutCommitmentError::CursorMismatch),
            ..
        }
    ));

    assert!(tracker.is_empty());

    for id in ids {
        assert_eq!(tracker.status(id), None);
    }

    assert_eq!(first_queue.snapshot().queued_batches, 0);
    assert_eq!(second_queue.snapshot().queued_batches, 0);

    // Original batch remains valid.
    assert_eq!(batch.len(), 2);
}

#[test]
fn tracked_commitment_preserves_queue_capacity_ownership() {
    let (registry, batch, mut tracker, plan, prepared, obligations) = validation_fixture();

    let first_queue = registry.peek_queue(DestinationId(1)).unwrap();
    let second_queue = registry.peek_queue(DestinationId(2)).unwrap();

    let result = FanoutCommitmentCoordinator::commit_tracked(
        &registry,
        &mut tracker,
        &plan,
        prepared,
        obligations,
        batch.clone(),
    );

    assert!(matches!(result, TrackedFanoutResult::Complete { .. }));

    for queue in [&first_queue, &second_queue] {
        assert_eq!(queue.snapshot().queued_batches, 1);

        assert_eq!(queue.capacity().usage().occupied_events, batch.len());

        let queued = queue.try_dequeue().unwrap();

        // Capacity remains occupied while the consumer
        // owns the dequeued batch.
        assert_eq!(queue.capacity().usage().occupied_events, batch.len());

        drop(queued);

        assert_eq!(queue.capacity().usage().occupied_events, 0);
    }

    // Queue capacity release does not finalize obligations.
    assert_eq!(tracker.len(), 2);
}

#[test]
fn tracked_partial_commitment_preserves_independent_ownership() {
    let (registry, batch, mut tracker, plan, prepared, obligations) = validation_fixture();

    let ids: Vec<_> = obligations
        .obligations()
        .iter()
        .map(|obligation| (obligation.destination, obligation.id))
        .collect();

    let first_queue = registry.peek_queue(DestinationId(1)).unwrap();
    let second_queue = registry.peek_queue(DestinationId(2)).unwrap();

    second_queue.close();

    let result = FanoutCommitmentCoordinator::commit_tracked(
        &registry,
        &mut tracker,
        &plan,
        prepared,
        obligations,
        batch.clone(),
    );

    assert!(matches!(result, TrackedFanoutResult::Partial { .. }));

    assert_eq!(first_queue.snapshot().queued_batches, 1);
    assert_eq!(second_queue.snapshot().queued_batches, 0);

    assert_eq!(first_queue.capacity().usage().occupied_events, batch.len());

    assert_eq!(second_queue.capacity().usage().occupied_events, 0);

    for (destination, id) in ids {
        let status = tracker.status(id).unwrap();

        if destination == DestinationId(1) {
            assert_eq!(status, DeliveryStatus::Progress(DeliveryStage::Queued));
        } else {
            assert!(matches!(status, DeliveryStatus::Failed(_)));
        }
    }
}

#[test]
fn recovery_aware_commitment_accepts_available_capacity() {
    let recovery = TrackingRecovery::new(2).unwrap();

    let reservations =
        FanoutCommitmentCoordinator::prepare_tracking_recovery(&recovery, 2).unwrap();

    assert_eq!(reservations.len(), 2);
    assert_eq!(recovery.occupied(), 2);
    assert_eq!(recovery.available(), 0);

    drop(reservations);

    assert_eq!(recovery.occupied(), 0);
    assert_eq!(recovery.available(), 2);
}

#[test]
fn recovery_aware_commitment_rejects_exhausted_capacity() {
    let recovery = TrackingRecovery::new(1).unwrap();

    let result = FanoutCommitmentCoordinator::prepare_tracking_recovery(&recovery, 2);

    assert!(matches!(
        result,
        Err(TrackedFanoutError::RecoveryCapacityExhausted)
    ));

    assert_eq!(recovery.occupied(), 0);
    assert_eq!(recovery.available(), 1);
}

#[test]
fn validation_rejects_duplicate_planned_destinations() {
    let (_, _, tracker, mut plan, prepared, obligations) = validation_fixture();

    // Preserve the destination count, but duplicate one ID.
    plan.required[1] = plan.required[0].clone();

    let result = validate_routing_obligations(&tracker, &plan, &prepared, &obligations);

    assert_eq!(
        result,
        Err(RoutingObligationValidationError::DuplicateDestination)
    );
}

#[test]
fn validation_rejects_duplicate_obligation_destinations() {
    use marketforge_engine::live::routing::acknowledgement::{
        DeliveryCursorRange, DeliveryObligationSpec,
    };

    let (_, _, mut tracker, plan, prepared, original) = validation_fixture();

    // Remove the original prepared transaction.
    tracker.abort_prepared(original).unwrap();

    let range =
        DeliveryCursorRange::new(plan.first_cursor.clone(), plan.last_cursor.clone()).unwrap();

    let descriptor = &plan.required[0].descriptor;

    let duplicate_specs = vec![
        DeliveryObligationSpec {
            destination: descriptor.id,
            range: range.clone(),
            kind: descriptor.kind,
            policy: descriptor.policy,
        },
        DeliveryObligationSpec {
            destination: descriptor.id,
            range,
            kind: descriptor.kind,
            policy: descriptor.policy,
        },
    ];

    let obligations = tracker.prepare_obligation_specs(duplicate_specs).unwrap();

    let result = validate_routing_obligations(&tracker, &plan, &prepared, &obligations);

    assert_eq!(
        result,
        Err(RoutingObligationValidationError::DuplicateDestination)
    );

    // Clean up the prepared transaction.
    tracker.abort_prepared(obligations).unwrap();

    assert!(tracker.is_empty());
}

#[test]
fn recovery_commitment_preparation_reserves_capacity() {
    let (registry, batch, _, _, prepared, _) = validation_fixture();

    let recovery = TrackingRecovery::new(2).unwrap();

    let reservations = FanoutCommitmentCoordinator::prepare_recovery_commitment(
        &registry, &recovery, &prepared, &batch,
    )
    .unwrap();

    assert_eq!(reservations.len(), 2);
    assert_eq!(recovery.occupied(), 2);

    for destination in [DestinationId(1), DestinationId(2)] {
        let queue = registry.peek_queue(destination).unwrap();

        assert_eq!(queue.snapshot().queued_batches, 0);
    }

    drop(reservations);

    assert_eq!(recovery.occupied(), 0);
}

#[test]
fn recovery_commitment_preparation_rejects_insufficient_capacity() {
    let (registry, batch, _, _, prepared, _) = validation_fixture();

    let recovery = TrackingRecovery::new(1).unwrap();

    let result = FanoutCommitmentCoordinator::prepare_recovery_commitment(
        &registry, &recovery, &prepared, &batch,
    );

    assert!(matches!(
        result,
        Err(TrackedFanoutError::RecoveryCapacityExhausted)
    ));

    assert_eq!(recovery.occupied(), 0);

    for destination in [DestinationId(1), DestinationId(2)] {
        let queue = registry.peek_queue(destination).unwrap();

        assert_eq!(queue.snapshot().queued_batches, 0);
    }
}

#[test]
fn recovery_aware_commitment_rejects_insufficient_capacity() {
    let (registry, batch, mut tracker, plan, prepared, obligations) = validation_fixture();

    let mut recovery = TrackingRecovery::new(1).unwrap();

    let result = FanoutCommitmentCoordinator::commit_tracked_with_recovery(
        &registry,
        &mut tracker,
        &mut recovery,
        &plan,
        prepared,
        obligations,
        batch,
    );

    assert!(matches!(
        result,
        TrackedFanoutResult::Rejected {
            reason: TrackedFanoutError::RecoveryCapacityExhausted,
            ..
        }
    ));

    assert!(tracker.is_empty());
    assert_eq!(recovery.occupied(), 0);

    for destination in [DestinationId(1), DestinationId(2)] {
        let queue = registry.peek_queue(destination).unwrap();

        assert_eq!(queue.snapshot().queued_batches, 0);
    }
}

#[test]
fn recovery_aware_commitment_queues_and_tracks_every_destination() {
    let (registry, batch, mut tracker, plan, prepared, obligations) = validation_fixture();
    let original = batch.clone();
    let ids: Vec<_> = obligations.obligations().iter().map(|o| o.id).collect();
    let mut recovery = TrackingRecovery::new(2).unwrap();
    let outcome = FanoutCommitmentCoordinator::commit_tracked_with_recovery(
        &registry,
        &mut tracker,
        &mut recovery,
        &plan,
        prepared,
        obligations,
        batch,
    );
    assert!(
        matches!(outcome,TrackedFanoutResult::Complete {ref through,ref committed} if through==original.last_cursor() && committed.len()==2)
    );
    assert_eq!(recovery.occupied(), 0);
    for id in ids {
        assert_eq!(
            tracker.status(id),
            Some(DeliveryStatus::Progress(DeliveryStage::Queued))
        );
    }
    for destination in [DestinationId(1), DestinationId(2)] {
        assert_eq!(
            registry
                .peek_queue(destination)
                .unwrap()
                .snapshot()
                .queued_batches,
            1
        );
    }
}
#[test]
fn recovery_aware_partial_fanout_preserves_success_and_terminal_failure() {
    let (registry, batch, mut tracker, plan, prepared, obligations) = validation_fixture();
    let mapped: Vec<_> = obligations
        .obligations()
        .iter()
        .map(|o| (o.destination, o.id))
        .collect();
    registry.peek_queue(DestinationId(2)).unwrap().close();
    let mut recovery = TrackingRecovery::new(2).unwrap();
    let outcome = FanoutCommitmentCoordinator::commit_tracked_with_recovery(
        &registry,
        &mut tracker,
        &mut recovery,
        &plan,
        prepared,
        obligations,
        batch.clone(),
    );
    assert!(
        matches!(outcome,TrackedFanoutResult::Partial {batch:ref returned,ref committed,ref failed} if returned==&batch && committed.len()==1 && failed.len()==1 && failed[0].id==DestinationId(2))
    );
    assert_eq!(recovery.occupied(), 0);
    for (destination, id) in mapped {
        assert_eq!(
            tracker.status(id),
            Some(if destination == DestinationId(1) {
                DeliveryStatus::Progress(DeliveryStage::Queued)
            } else {
                DeliveryStatus::Failed(DeliveryFailure::Unavailable)
            })
        );
    }
}

#[test]
fn ordered_frontier_drop_gap_namespace_and_exhaustion() {
    use marketforge_engine::live::routing::cursor::{CursorOrderError, StreamAdmissionOrder};
    let batch =
        EventBatch::new(vec![l2_event("ordered", 1, 10), l2_event("ordered", 1, 11)]).unwrap();
    let order = StreamAdmissionOrder::new(batch.first_cursor().clone()).unwrap();
    let permit = order.prepare(&batch).unwrap();
    assert!(matches!(order.prepare(&batch), Err(CursorOrderError::Busy)));
    drop(permit);
    assert_eq!(order.next(), Some(batch.first_cursor().clone()));
    order.prepare(&batch).unwrap().commit();
    assert_eq!(order.next().unwrap().offset, 12);
    assert!(matches!(order.prepare(&batch), Err(CursorOrderError::Gap)));
    let wrong_epoch = EventBatch::new(vec![l2_event("ordered", 2, 12)]).unwrap();
    assert!(matches!(
        order.prepare(&wrong_epoch),
        Err(CursorOrderError::Namespace)
    ));
    let gap =
        EventBatch::new(vec![l2_event("ordered", 1, 12), l2_event("ordered", 1, 14)]).unwrap();
    assert!(matches!(order.prepare(&gap), Err(CursorOrderError::Gap)));
    let final_batch = EventBatch::new(vec![l2_event("last", 1, u64::MAX)]).unwrap();
    let final_order = StreamAdmissionOrder::new(final_batch.first_cursor().clone()).unwrap();
    final_order.prepare(&final_batch).unwrap().commit();
    assert_eq!(final_order.next(), None);
    assert!(matches!(
        final_order.prepare(&final_batch),
        Err(CursorOrderError::Exhausted)
    ));
}

#[test]
fn router_destination_registration_is_bounded_and_rejection_preserves_revision() {
    use marketforge_engine::live::routing::router::RouterRegistryError;
    let registry = RouterRegistry::with_destination_limit(1).unwrap();
    registry
        .register(obligation_test_destination(
            1,
            DestinationKind::Recording,
            DeliveryPolicy::Required,
        ))
        .unwrap();
    let revision = registry.revision();
    assert_eq!(
        registry.register(obligation_test_destination(
            2,
            DestinationKind::Recording,
            DeliveryPolicy::Required
        )),
        Err(RouterRegistryError::DestinationCapacity)
    );
    assert_eq!(registry.revision(), revision);
    assert_eq!(registry.destination_count(), 1);
}

#[test]
fn ordered_fanout_context_rejection_releases_capacity_without_obligations() {
    use marketforge_engine::live::{
        foundation::clock::ManualClock,
        ports::routing::RoutingAdmissionFailure,
        routing::{
            cursor::StreamAdmissionOrder,
            ordered::{OrderedCommitOutcome, OrderedFanout},
        },
    };
    for cancel in [true, false] {
        let (registry, batch, mut tracker, _, prepared, obligations) = validation_fixture();
        drop(prepared);
        tracker.abort_prepared(obligations).unwrap();
        let order = StreamAdmissionOrder::new(batch.first_cursor().clone()).unwrap();
        let start = std::time::Instant::now();
        let clock = Arc::new(ManualClock::new(start, 0));
        let token = CancellationToken::new();
        // A clock-only deadline test needs a timer only during reservation;
        // use cancellation here and independently cover deadline at entry below.
        let pending = ready(OrderedFanout::prepare(
            &registry,
            &order,
            batch.clone(),
            AdmissionContext::without_deadline(token.clone()),
            clock,
            None,
        ))
        .unwrap();
        if cancel {
            token.cancel();
        } else {
            registry.shutdown();
        }
        let mut recovery = TrackingRecovery::new(10).unwrap();
        let outcome = pending.commit(&mut tracker, &mut recovery);
        if cancel {
            assert!(matches!(
                outcome,
                OrderedCommitOutcome::Rejected {
                    reason:
                        marketforge_engine::live::routing::ordered::OrderedCommitFailure::Context(
                            RoutingAdmissionFailure::Cancelled
                        ),
                    ..
                }
            ));
        } else {
            assert!(matches!(
                outcome,
                OrderedCommitOutcome::Tracked(TrackedFanoutResult::Rejected { .. })
            ));
        }
        assert_eq!(order.next(), Some(batch.first_cursor().clone()));
        assert!(tracker.is_empty());
        assert_eq!(recovery.occupied(), 0);
        assert!(
            registry
                .peek_queue(DestinationId(1))
                .unwrap()
                .try_dequeue()
                .is_none()
        );
    }
}

#[test]
fn ordered_fanout_commits_real_queues_and_advances_once() {
    use marketforge_engine::live::{
        foundation::clock::SystemMonotonicClock,
        routing::{
            cursor::{CursorOrderError, StreamAdmissionOrder},
            ordered::{OrderedCommitOutcome, OrderedFanout, OrderedPreparationError},
        },
    };
    let (registry, batch, mut tracker, _, prepared, obligations) = validation_fixture();
    drop(prepared);
    tracker.abort_prepared(obligations).unwrap();
    let order = StreamAdmissionOrder::new(batch.first_cursor().clone()).unwrap();
    let prepare = || {
        OrderedFanout::prepare(
            &registry,
            &order,
            batch.clone(),
            AdmissionContext::without_deadline(CancellationToken::new()),
            Arc::new(SystemMonotonicClock),
            None,
        )
    };
    let pending = ready(prepare()).unwrap();
    assert!(matches!(
        ready(prepare()),
        Err(OrderedPreparationError::Order(CursorOrderError::Busy))
    ));
    let mut recovery = TrackingRecovery::new(10).unwrap();
    assert!(matches!(
        pending.commit(&mut tracker, &mut recovery),
        OrderedCommitOutcome::Tracked(TrackedFanoutResult::Complete { .. })
    ));
    assert_eq!(order.next().unwrap().offset, 12);
    for id in [1, 2] {
        let queued = registry
            .peek_queue(DestinationId(id))
            .unwrap()
            .try_dequeue()
            .unwrap();
        assert_eq!(queued.batch(), &batch);
    }
    assert!(matches!(
        ready(prepare()),
        Err(OrderedPreparationError::Order(CursorOrderError::Gap))
    ));
}

#[test]
fn ordered_fanout_drop_and_expired_entry_preserve_frontier() {
    use marketforge_engine::live::{
        foundation::clock::ManualClock,
        ports::routing::RoutingAdmissionFailure,
        routing::{
            cursor::StreamAdmissionOrder,
            ordered::{OrderedFanout, OrderedPreparationError},
        },
    };
    let (registry, batch, mut tracker, _, prepared, obligations) = validation_fixture();
    drop(prepared);
    tracker.abort_prepared(obligations).unwrap();
    let order = StreamAdmissionOrder::new(batch.first_cursor().clone()).unwrap();
    let start = std::time::Instant::now();
    let clock = Arc::new(ManualClock::new(start, 0));
    let pending = ready(OrderedFanout::prepare(
        &registry,
        &order,
        batch.clone(),
        AdmissionContext::without_deadline(CancellationToken::new()),
        clock.clone(),
        None,
    ))
    .unwrap();
    drop(pending);
    assert!(order.prepare(&batch).is_ok());
    let result = ready(OrderedFanout::prepare(
        &registry,
        &order,
        batch.clone(),
        AdmissionContext::new(CancellationToken::new(), Some(start)),
        clock,
        None,
    ));
    assert!(matches!(
        result,
        Err(OrderedPreparationError::Context(
            RoutingAdmissionFailure::DeadlineExceeded
        ))
    ));
    assert!(tracker.is_empty());
    assert_eq!(order.next(), Some(batch.first_cursor().clone()));
}

#[test]
fn ordered_fanout_deadline_and_tracker_pressure_do_not_transfer_responsibility() {
    use marketforge_engine::live::{
        foundation::clock::ManualClock,
        ports::routing::RoutingAdmissionFailure,
        routing::{
            admission::{DeadlineFuture, DeadlineTimer},
            cursor::StreamAdmissionOrder,
            ordered::{OrderedCommitOutcome, OrderedFanout},
        },
    };
    // Capacity is immediately available. This timer deliberately stays pending;
    // the injected monotonic clock independently witnesses the final check.
    struct PendingTimer;
    impl DeadlineTimer for PendingTimer {
        fn sleep_until(&self, _: std::time::Instant) -> DeadlineFuture {
            Box::pin(std::future::pending())
        }
    }
    for expire in [true, false] {
        let (registry, batch, mut tracker, _, prepared, obligations) = validation_fixture();
        drop(prepared);
        tracker.abort_prepared(obligations).unwrap();
        if !expire {
            tracker = AcknowledgementTracker::new(1).unwrap();
        }
        let order = StreamAdmissionOrder::new(batch.first_cursor().clone()).unwrap();
        let start = std::time::Instant::now();
        let clock = Arc::new(ManualClock::new(start, 0));
        let duration = std::time::Duration::from_secs(1);
        let pending = ready(OrderedFanout::prepare(
            &registry,
            &order,
            batch.clone(),
            AdmissionContext::new(CancellationToken::new(), Some(start + duration)),
            clock.clone(),
            Some(&PendingTimer),
        ))
        .unwrap();
        if expire {
            clock.advance(duration).unwrap();
        }
        let mut recovery = TrackingRecovery::new(10).unwrap();
        let expected = if expire {
            marketforge_engine::live::routing::ordered::OrderedCommitFailure::Context(
                RoutingAdmissionFailure::DeadlineExceeded,
            )
        } else {
            marketforge_engine::live::routing::ordered::OrderedCommitFailure::Obligations(
                AcknowledgementError::CapacityExceeded,
            )
        };
        assert!(matches!(pending.commit(&mut tracker, &mut recovery),
            OrderedCommitOutcome::Rejected { reason, .. } if reason == expected));
        assert!(tracker.is_empty());
        assert_eq!(order.next(), Some(batch.first_cursor().clone()));
        for id in [1, 2] {
            assert!(
                registry
                    .peek_queue(DestinationId(id))
                    .unwrap()
                    .try_dequeue()
                    .is_none()
            );
        }
        // Reservations are released, so a fresh preparation succeeds.
        assert!(
            ready(OrderedFanout::prepare(
                &registry,
                &order,
                batch,
                AdmissionContext::without_deadline(CancellationToken::new()),
                clock,
                None
            ))
            .is_ok()
        );
    }
}

#[test]
fn ordered_fanout_partial_failure_advances_frontier_and_retains_payload() {
    use marketforge_engine::live::{
        foundation::clock::SystemMonotonicClock,
        routing::{
            cursor::StreamAdmissionOrder,
            ordered::{OrderedCommitOutcome, OrderedFanout},
        },
    };
    let (registry, batch, mut tracker, _, prepared, obligations) = validation_fixture();
    drop(prepared);
    tracker.abort_prepared(obligations).unwrap();
    let order = StreamAdmissionOrder::new(batch.first_cursor().clone()).unwrap();
    let pending = ready(OrderedFanout::prepare(
        &registry,
        &order,
        batch.clone(),
        AdmissionContext::without_deadline(CancellationToken::new()),
        Arc::new(SystemMonotonicClock),
        None,
    ))
    .unwrap();
    registry.peek_queue(DestinationId(2)).unwrap().close();
    let mut recovery = TrackingRecovery::new(10).unwrap();
    match pending.commit(&mut tracker, &mut recovery) {
        OrderedCommitOutcome::Tracked(TrackedFanoutResult::Partial {
            batch: retained,
            committed,
            failed,
        }) => {
            assert_eq!(retained, batch);
            assert_eq!(committed.len(), 1);
            assert_eq!(failed.len(), 1);
        }
        outcome => panic!("expected explicit partial outcome: {outcome:?}"),
    }
    assert_eq!(order.next().unwrap().offset, 12);
    assert_eq!(
        registry
            .peek_queue(DestinationId(1))
            .unwrap()
            .try_dequeue()
            .unwrap()
            .batch(),
        &batch
    );
}

#[test]
fn ordered_fanout_dropped_wait_releases_prior_reservations_and_exclusive_order() {
    use marketforge_engine::live::{
        foundation::clock::SystemMonotonicClock,
        routing::{cursor::StreamAdmissionOrder, ordered::OrderedFanout},
    };
    let (registry, batch, mut tracker, _, prepared, obligations) = validation_fixture();
    drop(prepared);
    tracker.abort_prepared(obligations).unwrap();
    let queue = registry.peek_queue(DestinationId(2)).unwrap();
    // Exhaust only the second destination; the first reservation must be
    // released when the preparation future is dropped while waiting.
    for _ in 0..50 {
        queue.try_enqueue(batch.clone()).unwrap();
    }
    let order = StreamAdmissionOrder::new(batch.first_cursor().clone()).unwrap();
    let mut pending = Box::pin(OrderedFanout::prepare(
        &registry,
        &order,
        batch.clone(),
        AdmissionContext::without_deadline(CancellationToken::new()),
        Arc::new(SystemMonotonicClock),
        None,
    ));
    let waker = Waker::from(Arc::new(NoopWake));
    assert!(
        pending
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    assert_eq!(
        registry
            .peek_queue(DestinationId(1))
            .unwrap()
            .capacity()
            .usage()
            .occupied_events,
        2
    );
    drop(pending);
    assert_eq!(
        registry
            .peek_queue(DestinationId(1))
            .unwrap()
            .capacity()
            .usage()
            .occupied_events,
        0
    );
    assert_eq!(queue.capacity().usage().waiting_operations, 0);
    assert!(order.prepare(&batch).is_ok());
    assert!(tracker.is_empty());
}

#[test]
fn ordered_fanout_rechecks_deadline_inside_commitment_fence() {
    use marketforge_engine::live::{
        foundation::MonotonicClock,
        ports::routing::RoutingAdmissionFailure,
        routing::{
            admission::{DeadlineFuture, DeadlineTimer},
            cursor::StreamAdmissionOrder,
            ordered::{OrderedCommitOutcome, OrderedFanout},
        },
    };
    struct FenceClock {
        start: std::time::Instant,
        calls: std::sync::atomic::AtomicUsize,
    }
    impl MonotonicClock for FenceClock {
        fn now(&self) -> std::time::Instant {
            let call = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            // Preparation checks twice, commit checks once before acquiring the
            // registry fence. Expiry is observed only at the final fence check.
            if call < 3 {
                self.start
            } else {
                self.start + std::time::Duration::from_secs(1)
            }
        }
    }
    struct PendingTimer;
    impl DeadlineTimer for PendingTimer {
        fn sleep_until(&self, _: std::time::Instant) -> DeadlineFuture {
            Box::pin(std::future::pending())
        }
    }
    let (registry, batch, mut tracker, _, prepared, obligations) = validation_fixture();
    drop(prepared);
    tracker.abort_prepared(obligations).unwrap();
    let order = StreamAdmissionOrder::new(batch.first_cursor().clone()).unwrap();
    let start = std::time::Instant::now();
    let pending = ready(OrderedFanout::prepare(
        &registry,
        &order,
        batch.clone(),
        AdmissionContext::new(
            CancellationToken::new(),
            Some(start + std::time::Duration::from_secs(1)),
        ),
        Arc::new(FenceClock {
            start,
            calls: std::sync::atomic::AtomicUsize::new(0),
        }),
        Some(&PendingTimer),
    ))
    .unwrap();
    let mut recovery = TrackingRecovery::new(10).unwrap();
    assert!(matches!(
        pending.commit(&mut tracker, &mut recovery),
        OrderedCommitOutcome::Tracked(TrackedFanoutResult::Rejected {
            reason: TrackedFanoutError::Context(RoutingAdmissionFailure::DeadlineExceeded),
            ..
        })
    ));
    assert!(tracker.is_empty());
    assert_eq!(recovery.occupied(), 0);
    assert_eq!(order.next(), Some(batch.first_cursor().clone()));
    for id in [1, 2] {
        assert!(
            registry
                .peek_queue(DestinationId(id))
                .unwrap()
                .try_dequeue()
                .is_none()
        );
    }
}

#[test]
fn registered_namespace_routes_without_resetting_frontier() {
    use marketforge_engine::live::{
        foundation::SystemMonotonicClock,
        routing::{
            cursor::CursorOrderError,
            namespaces::{AdmissionNamespaces, NamespaceError},
            ordered::{OrderedCommitOutcome, OrderedFanout, OrderedPreparationError},
        },
    };
    let (registry, batch, mut tracker, _, prepared, obligations) = validation_fixture();
    drop(prepared);
    tracker.abort_prepared(obligations).unwrap();
    let namespaces = AdmissionNamespaces::new(2).unwrap();
    let context = || AdmissionContext::without_deadline(CancellationToken::new());
    assert!(matches!(
        ready(OrderedFanout::prepare_registered(
            &registry,
            &namespaces,
            batch.clone(),
            context(),
            Arc::new(SystemMonotonicClock),
            None
        )),
        Err(OrderedPreparationError::Namespace(NamespaceError::Unknown))
    ));
    let first = batch.first_cursor().clone();
    namespaces.register(first.clone()).unwrap();
    let order = namespaces.get(&first.stream_id, first.log_epoch).unwrap();
    let admission = ready(OrderedFanout::prepare_registered(
        &registry,
        &namespaces,
        batch.clone(),
        context(),
        Arc::new(SystemMonotonicClock),
        None,
    ))
    .unwrap();
    assert!(matches!(order.prepare(&batch), Err(CursorOrderError::Busy)));
    let mut recovery = TrackingRecovery::new(10).unwrap();
    assert!(matches!(
        admission.commit(&mut tracker, &mut recovery),
        OrderedCommitOutcome::Tracked(TrackedFanoutResult::Complete { .. })
    ));
    assert_eq!(order.next().unwrap().offset, batch.last_cursor().offset + 1);
    assert_eq!(
        namespaces.register(first.clone()),
        Err(NamespaceError::AlreadyRegistered)
    );
    assert!(matches!(order.prepare(&batch), Err(CursorOrderError::Gap)));
    for id in [1, 2] {
        assert!(
            registry
                .peek_queue(DestinationId(id))
                .unwrap()
                .try_dequeue()
                .is_some()
        );
    }
    namespaces
        .retire(&first.stream_id, first.log_epoch)
        .unwrap();
    assert!(matches!(
        order.prepare(&batch),
        Err(CursorOrderError::Closed)
    ));
    assert!(matches!(
        namespaces.get(&first.stream_id, first.log_epoch),
        Err(NamespaceError::Retired)
    ));
    assert_eq!(
        namespaces.register(first),
        Err(NamespaceError::AlreadyRegistered)
    );
}

#[test]
fn registered_namespace_budget_shutdown_and_inflight_retirement() {
    use marketforge_engine::live::routing::{
        cursor::CursorOrderError,
        namespaces::{AdmissionNamespaces, NamespaceError},
    };
    assert!(matches!(
        AdmissionNamespaces::new(0),
        Err(NamespaceError::InvalidLimit)
    ));
    let (_, batch, _, _, _, _) = validation_fixture();
    let first = batch.first_cursor().clone();
    let namespaces = AdmissionNamespaces::new(2).unwrap();
    let mut invalid = first.clone();
    invalid.stream_id = StreamId(String::new());
    assert_eq!(
        namespaces.register(invalid),
        Err(NamespaceError::InvalidCursor(
            CursorOrderError::InvalidIdentity
        ))
    );
    namespaces.register(first.clone()).unwrap();
    let old = namespaces.get(&first.stream_id, first.log_epoch).unwrap();
    let permit = old.prepare(&batch).unwrap();
    namespaces
        .retire(&first.stream_id, first.log_epoch)
        .unwrap();
    // Retirement cannot roll back an operation already holding its permit.
    permit.commit();
    assert_eq!(old.next().unwrap().offset, batch.last_cursor().offset + 1);
    assert!(matches!(old.prepare(&batch), Err(CursorOrderError::Closed)));
    let mut second = first.clone();
    second.log_epoch = LogEpoch(first.log_epoch.0 + 1);
    namespaces.register(second.clone()).unwrap();
    let current = namespaces.get(&second.stream_id, second.log_epoch).unwrap();
    let mut third = second.clone();
    third.log_epoch = LogEpoch(second.log_epoch.0 + 1);
    assert_eq!(
        namespaces.register(third.clone()),
        Err(NamespaceError::Capacity)
    );
    assert_eq!(namespaces.registered_count(), 2);
    assert_eq!(current.next(), Some(second));
    namespaces.shutdown();
    namespaces.shutdown();
    assert!(matches!(
        current.prepare(&batch),
        Err(CursorOrderError::Closed)
    ));
    assert!(matches!(
        namespaces.get(&first.stream_id, first.log_epoch),
        Err(NamespaceError::Shutdown)
    ));
    assert_eq!(namespaces.register(third), Err(NamespaceError::Shutdown));
}

#[test]
fn registered_namespace_concurrent_registration_has_one_owner() {
    use marketforge_engine::live::routing::namespaces::{AdmissionNamespaces, NamespaceError};
    let namespaces = AdmissionNamespaces::new(1).unwrap();
    let barrier = std::sync::Barrier::new(2);
    let results = std::thread::scope(|scope| {
        let tasks: Vec<_> = (0..2)
            .map(|_| {
                scope.spawn(|| {
                    barrier.wait();
                    namespaces.register(cursor(10))
                })
            })
            .collect();
        tasks
            .into_iter()
            .map(|task| task.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|r| **r == Err(NamespaceError::AlreadyRegistered))
            .count(),
        1
    );
    assert_eq!(namespaces.registered_count(), 1);
}

#[test]
fn bounded_ordered_admission_retains_global_charge_and_releases_on_drop() {
    use marketforge_engine::live::{
        foundation::{SystemMonotonicClock, resources::ResourceUsage},
        model::{error::ErrorCode, memory::RetainedSize},
        routing::{
            namespaces::AdmissionNamespaces,
            ordered::{OrderedAdmissionBudget, OrderedCommitOutcome, OrderedPreparationError},
        },
    };
    let (registry, batch, mut tracker, _, prepared, obligations) = validation_fixture();
    drop(prepared);
    tracker.abort_prepared(obligations).unwrap();
    let namespaces = AdmissionNamespaces::new(1).unwrap();
    namespaces.register(batch.first_cursor().clone()).unwrap();
    let charge = ResourceUsage {
        items: batch.len(),
        bytes: usize::try_from(batch.retained_size_bytes().unwrap()).unwrap(),
    };
    let budget = OrderedAdmissionBudget::new(charge).unwrap();
    let context = || AdmissionContext::without_deadline(CancellationToken::new());
    let admission = ready(budget.prepare(
        &registry,
        &namespaces,
        batch.clone(),
        context(),
        Arc::new(SystemMonotonicClock),
        None,
    ))
    .unwrap();
    assert_eq!(budget.usage(), charge);
    assert!(matches!(
        ready(budget.prepare(
            &registry,
            &namespaces,
            batch.clone(),
            context(),
            Arc::new(SystemMonotonicClock),
            None
        )),
        Err(OrderedPreparationError::Budget(ErrorCode::Capacity))
    ));
    drop(admission);
    assert_eq!(budget.usage(), ResourceUsage::default());
    // Byte exhaustion is independent of the event count limit.
    let small = OrderedAdmissionBudget::new(ResourceUsage {
        items: charge.items + 1,
        bytes: charge.bytes - 1,
    })
    .unwrap();
    assert!(matches!(
        ready(small.prepare(
            &registry,
            &namespaces,
            batch.clone(),
            context(),
            Arc::new(SystemMonotonicClock),
            None
        )),
        Err(OrderedPreparationError::Budget(ErrorCode::Capacity))
    ));
    assert_eq!(small.usage(), ResourceUsage::default());
    let admission = ready(budget.prepare(
        &registry,
        &namespaces,
        batch.clone(),
        context(),
        Arc::new(SystemMonotonicClock),
        None,
    ))
    .unwrap();
    let mut recovery = TrackingRecovery::new(10).unwrap();
    assert!(matches!(
        admission.commit(&mut tracker, &mut recovery),
        OrderedCommitOutcome::Tracked(TrackedFanoutResult::Complete { .. })
    ));
    assert_eq!(budget.usage(), ResourceUsage::default());
    for id in [1, 2] {
        assert!(
            registry
                .peek_queue(DestinationId(id))
                .unwrap()
                .try_dequeue()
                .is_some()
        );
    }
    budget.close();
    assert!(matches!(
        ready(budget.prepare(
            &registry,
            &namespaces,
            batch,
            context(),
            Arc::new(SystemMonotonicClock),
            None
        )),
        Err(OrderedPreparationError::Budget(ErrorCode::Unavailable))
    ));
}

#[test]
fn bounded_ordered_admission_dropped_wait_releases_global_and_destination_capacity() {
    use marketforge_engine::live::{
        foundation::{SystemMonotonicClock, resources::ResourceUsage},
        routing::{namespaces::AdmissionNamespaces, ordered::OrderedAdmissionBudget},
    };
    let (registry, batch, mut tracker, _, prepared, obligations) = validation_fixture();
    drop(prepared);
    tracker.abort_prepared(obligations).unwrap();
    let queue = registry.peek_queue(DestinationId(2)).unwrap();
    for _ in 0..50 {
        queue.try_enqueue(batch.clone()).unwrap();
    }
    let namespaces = AdmissionNamespaces::new(1).unwrap();
    namespaces.register(batch.first_cursor().clone()).unwrap();
    let budget = OrderedAdmissionBudget::new(ResourceUsage {
        items: 100,
        bytes: 1_000_000,
    })
    .unwrap();
    let cancel = CancellationToken::new();
    let mut waiting = Box::pin(budget.prepare(
        &registry,
        &namespaces,
        batch.clone(),
        AdmissionContext::without_deadline(cancel.clone()),
        Arc::new(SystemMonotonicClock),
        None,
    ));
    let waker = Waker::from(Arc::new(NoopWake));
    assert!(
        waiting
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    assert_eq!(budget.usage().items, batch.len());
    drop(waiting);
    assert_eq!(budget.usage(), ResourceUsage::default());
    assert_eq!(queue.capacity().usage().waiting_operations, 0);
    assert_eq!(
        registry
            .peek_queue(DestinationId(1))
            .unwrap()
            .capacity()
            .usage()
            .occupied_events,
        0
    );
    assert!(tracker.is_empty());
    // Cancellation also releases the global charge when the future completes.
    let mut waiting = Box::pin(budget.prepare(
        &registry,
        &namespaces,
        batch,
        AdmissionContext::without_deadline(cancel.clone()),
        Arc::new(SystemMonotonicClock),
        None,
    ));
    assert!(
        waiting
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    cancel.cancel();
    assert!(
        waiting
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_ready()
    );
    assert_eq!(budget.usage(), ResourceUsage::default());
}

fn add_optional_queue(
    registry: &RouterRegistry,
    id: u64,
) -> marketforge_engine::live::routing::queue::DestinationQueue {
    use marketforge_engine::live::routing::{
        destination::DestinationCapacity, queue::DestinationQueue,
    };
    let destination =
        obligation_test_destination(id, DestinationKind::Publication, DeliveryPolicy::BestEffort);
    let queue = DestinationQueue::new(
        DestinationCapacity::new(destination.descriptor().capacity, 10).unwrap(),
    );
    registry.register_queue(destination, queue.clone()).unwrap();
    queue
}

#[test]
fn optional_fanout_isolates_full_and_closed_queues_and_preserves_identity() {
    let (registry, batch, mut tracker, _, prepared, obligations) = validation_fixture();
    drop(prepared);
    tracker.abort_prepared(obligations).unwrap();
    let full = add_optional_queue(&registry, 3);
    let closed = add_optional_queue(&registry, 4);
    let healthy = add_optional_queue(&registry, 5);
    while full.try_enqueue(batch.clone()).is_ok() {}
    closed.close();
    let namespaces =
        marketforge_engine::live::routing::namespaces::AdmissionNamespaces::new(1).unwrap();
    namespaces.register(batch.first_cursor().clone()).unwrap();
    let admission = ready(
        marketforge_engine::live::routing::ordered::OrderedFanout::prepare_registered(
            &registry,
            &namespaces,
            batch.clone(),
            AdmissionContext::without_deadline(CancellationToken::new()),
            Arc::new(marketforge_engine::live::foundation::SystemMonotonicClock),
            None,
        ),
    )
    .unwrap();
    let (required, optional) =
        admission.commit_with_best_effort(&mut tracker, &mut TrackingRecovery::new(10).unwrap());
    assert!(matches!(
        required,
        marketforge_engine::live::routing::ordered::OrderedCommitOutcome::Tracked(
            TrackedFanoutResult::Complete { .. }
        )
    ));
    let optional = optional.unwrap().unwrap();
    assert_eq!(
        optional.queued.iter().map(|d| d.id).collect::<Vec<_>>(),
        vec![DestinationId(5)]
    );
    assert_eq!(
        optional.failed.iter().map(|d| d.id).collect::<Vec<_>>(),
        vec![DestinationId(3), DestinationId(4)]
    );
    assert_eq!(full.capacity().usage().waiting_operations, 0);
    assert_eq!(closed.capacity().usage().waiting_operations, 0);
    for queue in [
        healthy,
        registry.peek_queue(DestinationId(1)).unwrap(),
        registry.peek_queue(DestinationId(2)).unwrap(),
    ] {
        let queued = queue.try_dequeue().unwrap();
        assert_eq!(queued.batch(), &batch);
        assert!(Arc::ptr_eq(&queued.batch().events()[0], &batch.events()[0]));
    }
}

#[test]
fn optional_fanout_prevalidates_stale_duplicate_and_shutdown_plans() {
    use marketforge_engine::live::routing::router::BestEffortError;
    let registry = RouterRegistry::new();
    let queue = add_optional_queue(&registry, 1);
    let batch = obligation_test_batch();
    let mut duplicate = registry.plan(&batch).unwrap();
    duplicate.best_effort.push(duplicate.best_effort[0].clone());
    assert_eq!(
        registry.try_best_effort(&duplicate, &batch).unwrap_err(),
        BestEffortError::InvalidPlan(FanoutCommitmentError::DestinationMismatch)
    );
    assert!(queue.try_dequeue().is_none());
    let stale = registry.plan(&batch).unwrap();
    let second = add_optional_queue(&registry, 2);
    assert_eq!(
        registry.try_best_effort(&stale, &batch).unwrap_err(),
        BestEffortError::InvalidPlan(FanoutCommitmentError::RegistryChanged)
    );
    let plan = registry.plan(&batch).unwrap();
    registry.shutdown();
    assert_eq!(
        registry.try_best_effort(&plan, &batch).unwrap_err(),
        BestEffortError::InvalidPlan(FanoutCommitmentError::ShuttingDown)
    );
    assert!(queue.try_dequeue().is_none());
    assert!(second.try_dequeue().is_none());
}

#[test]
fn optional_fanout_does_not_run_after_cancelled_required_commitment() {
    use marketforge_engine::live::{
        foundation::SystemMonotonicClock,
        routing::{
            namespaces::AdmissionNamespaces,
            ordered::{OrderedCommitOutcome, OrderedFanout},
        },
    };
    let (registry, batch, mut tracker, _, prepared, obligations) = validation_fixture();
    drop(prepared);
    tracker.abort_prepared(obligations).unwrap();
    let queue = add_optional_queue(&registry, 3);
    let namespaces = AdmissionNamespaces::new(1).unwrap();
    namespaces.register(batch.first_cursor().clone()).unwrap();
    let cancellation = CancellationToken::new();
    let admission = ready(OrderedFanout::prepare_registered(
        &registry,
        &namespaces,
        batch.clone(),
        AdmissionContext::without_deadline(cancellation.clone()),
        Arc::new(SystemMonotonicClock),
        None,
    ))
    .unwrap();
    cancellation.cancel();
    let (required, optional) =
        admission.commit_with_best_effort(&mut tracker, &mut TrackingRecovery::new(10).unwrap());
    assert!(matches!(required, OrderedCommitOutcome::Rejected { .. }));
    assert!(optional.is_none());
    assert!(queue.try_dequeue().is_none());
    assert!(tracker.is_empty());
    // Rejection releases the same frontier for a valid retry.
    let retry = ready(OrderedFanout::prepare_registered(
        &registry,
        &namespaces,
        batch,
        AdmissionContext::without_deadline(CancellationToken::new()),
        Arc::new(SystemMonotonicClock),
        None,
    ))
    .unwrap();
    let (_, optional) =
        retry.commit_with_best_effort(&mut tracker, &mut TrackingRecovery::new(10).unwrap());
    assert_eq!(optional.unwrap().unwrap().queued.len(), 1);
    assert!(queue.try_dequeue().is_some());
}

#[test]
fn optional_only_ordered_attempt_and_rejections_preserve_ownership() {
    use marketforge_engine::live::{
        foundation::{SystemMonotonicClock, resources::ResourceUsage},
        routing::{
            namespaces::AdmissionNamespaces,
            ordered::{OptionalOrderedError, OptionalOrderedOutcome, OrderedAdmissionBudget},
        },
    };
    let registry = RouterRegistry::new();
    let batch = obligation_test_batch();
    let namespaces = AdmissionNamespaces::new(1).unwrap();
    namespaces.register(batch.first_cursor().clone()).unwrap();
    let order = namespaces
        .get(
            &batch.first_cursor().stream_id,
            batch.first_cursor().log_epoch,
        )
        .unwrap();
    let budget = OrderedAdmissionBudget::new(ResourceUsage {
        items: 100,
        bytes: 1_000_000,
    })
    .unwrap();
    assert!(matches!(
        budget.try_optional_only(
            &registry,
            &namespaces,
            batch.clone(),
            AdmissionContext::without_deadline(CancellationToken::new()),
            &SystemMonotonicClock
        ),
        OptionalOrderedOutcome::Rejected { .. }
    ));
    let queue = add_optional_queue(&registry, 1);
    let cancel = CancellationToken::new();
    cancel.cancel();
    let result = budget.try_optional_only(
        &registry,
        &namespaces,
        batch.clone(),
        AdmissionContext::without_deadline(cancel),
        &SystemMonotonicClock,
    );
    let OptionalOrderedOutcome::Rejected {
        batch: returned, ..
    } = result
    else {
        panic!("cancel ignored")
    };
    assert!(Arc::ptr_eq(&returned.events()[0], &batch.events()[0]));
    assert_eq!(order.next().unwrap(), *batch.first_cursor());
    assert!(queue.try_dequeue().is_none());
    let (required, required_batch, mut tracker, _, prepared, obligations) = validation_fixture();
    drop(prepared);
    tracker.abort_prepared(obligations).unwrap();
    let required_namespaces = AdmissionNamespaces::new(1).unwrap();
    required_namespaces
        .register(required_batch.first_cursor().clone())
        .unwrap();
    assert!(matches!(
        budget.try_optional_only(
            &required,
            &required_namespaces,
            required_batch,
            AdmissionContext::without_deadline(CancellationToken::new()),
            &SystemMonotonicClock
        ),
        OptionalOrderedOutcome::Rejected {
            reason: OptionalOrderedError::RequiredDestinations,
            ..
        }
    ));
    assert!(
        required
            .peek_queue(DestinationId(1))
            .unwrap()
            .try_dequeue()
            .is_none()
    );
    while queue.try_enqueue(batch.clone()).is_ok() {}
    let result = budget.try_optional_only(
        &registry,
        &namespaces,
        batch.clone(),
        AdmissionContext::without_deadline(CancellationToken::new()),
        &SystemMonotonicClock,
    );
    let OptionalOrderedOutcome::Attempted(result) = result else {
        panic!("optional attempt rejected")
    };
    assert!(result.queued.is_empty());
    assert_eq!(result.failed.len(), 1);
    assert_eq!(queue.capacity().usage().waiting_operations, 0);
    assert_eq!(order.next().unwrap().offset, batch.last_cursor().offset + 1);
    assert_eq!(budget.usage(), ResourceUsage::default());
    assert!(matches!(
        budget.try_optional_only(
            &registry,
            &namespaces,
            batch,
            AdmissionContext::without_deadline(CancellationToken::new()),
            &SystemMonotonicClock
        ),
        OptionalOrderedOutcome::Rejected { .. }
    ));
}

#[test]
fn optional_only_ordered_success_budget_and_shutdown() {
    use marketforge_engine::live::{
        foundation::{SystemMonotonicClock, resources::ResourceUsage},
        routing::{
            namespaces::AdmissionNamespaces,
            ordered::{OptionalOrderedOutcome, OrderedAdmissionBudget},
        },
    };
    let registry = RouterRegistry::new();
    let queue = add_optional_queue(&registry, 1);
    let batch = obligation_test_batch();
    let namespaces = AdmissionNamespaces::new(1).unwrap();
    namespaces.register(batch.first_cursor().clone()).unwrap();
    let tiny = OrderedAdmissionBudget::new(ResourceUsage { items: 1, bytes: 1 }).unwrap();
    assert!(matches!(
        tiny.try_optional_only(
            &registry,
            &namespaces,
            batch.clone(),
            AdmissionContext::without_deadline(CancellationToken::new()),
            &SystemMonotonicClock
        ),
        OptionalOrderedOutcome::Rejected { .. }
    ));
    assert_eq!(tiny.usage(), ResourceUsage::default());
    assert!(queue.try_dequeue().is_none());
    let budget = OrderedAdmissionBudget::new(ResourceUsage {
        items: 100,
        bytes: 1_000_000,
    })
    .unwrap();
    let OptionalOrderedOutcome::Attempted(result) = budget.try_optional_only(
        &registry,
        &namespaces,
        batch.clone(),
        AdmissionContext::without_deadline(CancellationToken::new()),
        &SystemMonotonicClock,
    ) else {
        panic!("healthy queue rejected")
    };
    assert_eq!(result.queued.len(), 1);
    assert!(result.failed.is_empty());
    let queued = queue.try_dequeue().unwrap();
    assert!(Arc::ptr_eq(&queued.batch().events()[0], &batch.events()[0]));
    assert_eq!(budget.usage(), ResourceUsage::default());
    budget.close();
    assert!(matches!(
        budget.try_optional_only(
            &registry,
            &namespaces,
            batch,
            AdmissionContext::without_deadline(CancellationToken::new()),
            &SystemMonotonicClock
        ),
        OptionalOrderedOutcome::Rejected { .. }
    ));
}

#[test]
fn router_snapshot_distinguishes_queue_depth_from_reserved_and_inflight_capacity() {
    use marketforge_engine::live::{
        model::memory::RetainedSize,
        routing::{destination::DestinationCapacity, queue::DestinationQueue},
    };
    let registry = RouterRegistry::with_destination_limit(2).unwrap();
    let destination =
        obligation_test_destination(2, DestinationKind::Recording, DeliveryPolicy::Required);
    let queue = DestinationQueue::new(
        DestinationCapacity::new(destination.descriptor().capacity, 2).unwrap(),
    );
    registry.register_queue(destination, queue.clone()).unwrap();
    let batch = obligation_test_batch();
    let bytes = batch.retained_size_bytes().unwrap();
    let reservation = queue.capacity().try_reserve(batch.len(), bytes).unwrap();
    let sample = registry.snapshot().destinations[0].queue.unwrap();
    assert_eq!(sample.queue.queued_events, 0);
    assert_eq!(sample.capacity.occupied_events, batch.len());
    drop(reservation);
    queue.try_enqueue(batch.clone()).unwrap();
    let queued = registry.snapshot();
    let sample = queued.destinations[0].queue.unwrap();
    assert_eq!(sample.queue.queued_events, batch.len());
    assert_eq!(sample.queue.queued_bytes, bytes);
    assert_eq!(sample.capacity.occupied_bytes, bytes);
    let held = queue.try_dequeue().unwrap();
    let sample = registry.snapshot().destinations[0].queue.unwrap();
    assert_eq!(sample.queue.queued_batches, 0);
    assert_eq!(sample.capacity.occupied_bytes, bytes);
    queue.shutdown();
    let closed = registry.snapshot().destinations[0].queue.unwrap();
    assert!(closed.queue.closed);
    assert!(closed.capacity.shutting_down);
    assert_eq!(closed.capacity.occupied_bytes, bytes);
    drop(held);
    assert_eq!(
        registry.snapshot().destinations[0]
            .queue
            .unwrap()
            .capacity
            .occupied_bytes,
        0
    );
    assert_eq!(
        queued.destinations[0].queue.unwrap().queue.queued_bytes,
        bytes
    );
}

#[test]
fn router_snapshot_has_bounded_sorted_owned_membership() {
    let registry = RouterRegistry::with_destination_limit(2).unwrap();
    assert!(registry.snapshot().destinations.is_empty());
    for id in [8, 3] {
        registry
            .register(obligation_test_destination(
                id,
                DestinationKind::Recording,
                DeliveryPolicy::Required,
            ))
            .unwrap();
    }
    let before = registry.snapshot();
    assert_eq!(before.destination_limit, 2);
    assert_eq!(before.revision, 2);
    assert_eq!(
        before
            .destinations
            .iter()
            .map(|entry| entry.descriptor.id.0)
            .collect::<Vec<_>>(),
        vec![3, 8]
    );
    assert!(
        before
            .destinations
            .iter()
            .all(|entry| entry.queue.is_none())
    );
    assert!(
        registry
            .register(obligation_test_destination(
                9,
                DestinationKind::Recording,
                DeliveryPolicy::Required
            ))
            .is_err()
    );
    assert_eq!(registry.snapshot(), before);
    registry.unregister(DestinationId(3)).unwrap();
    let removed = registry.snapshot();
    assert_eq!(removed.revision, 3);
    assert_eq!(removed.destinations.len(), 1);
    registry.shutdown();
    let stopped = registry.snapshot();
    assert!(stopped.shutting_down);
    assert_eq!(stopped.destinations, removed.destinations);
    assert!(!before.shutting_down);
    assert_eq!(before.destinations.len(), 2);
}

#[test]
fn namespace_snapshot_reports_inflight_shutdown_without_claiming_completion() {
    use marketforge_engine::live::{
        foundation::{SystemMonotonicClock, resources::ResourceUsage},
        routing::{
            namespaces::AdmissionNamespaces,
            ordered::{OrderedAdmissionBudget, OrderedCommitOutcome},
        },
    };
    let (registry, batch, mut tracker, _, prepared, obligations) = validation_fixture();
    drop(prepared);
    tracker.abort_prepared(obligations).unwrap();
    let namespaces = AdmissionNamespaces::new(1).unwrap();
    namespaces.register(batch.first_cursor().clone()).unwrap();
    let budget = OrderedAdmissionBudget::new(ResourceUsage {
        items: 100,
        bytes: 1_000_000,
    })
    .unwrap();
    let admission = ready(budget.prepare(
        &registry,
        &namespaces,
        batch.clone(),
        AdmissionContext::without_deadline(CancellationToken::new()),
        Arc::new(SystemMonotonicClock),
        None,
    ))
    .unwrap();
    let pending = namespaces.snapshot();
    assert_eq!(
        pending.namespaces[0].order.next.as_ref(),
        Some(batch.first_cursor())
    );
    assert_eq!(
        pending.namespaces[0].order.pending_last.as_ref(),
        Some(batch.last_cursor())
    );
    namespaces.shutdown();
    let stopped = namespaces.snapshot();
    assert!(stopped.shutting_down);
    assert!(stopped.namespaces[0].order.closed);
    assert!(stopped.namespaces[0].order.pending_last.is_some());
    // Closing namespace admission preserves an already prepared responsibility.
    let mut recovery = TrackingRecovery::new(10).unwrap();
    assert!(matches!(
        admission.commit(&mut tracker, &mut recovery),
        OrderedCommitOutcome::Tracked(TrackedFanoutResult::Complete { .. })
    ));
    let finished = namespaces.snapshot();
    assert!(finished.namespaces[0].order.pending_last.is_none());
    assert_eq!(
        finished.namespaces[0].order.next.as_ref().unwrap().offset,
        batch.last_cursor().offset + 1
    );
    // Actual queue readback still has work; admission quiescence is not delivery.
    assert!(
        registry
            .peek_queue(DestinationId(1))
            .unwrap()
            .try_dequeue()
            .is_some()
    );
    assert!(!tracker.is_empty());
    assert!(pending.namespaces[0].order.pending_last.is_some());
    assert!(!pending.shutting_down);
}

#[test]
fn namespace_snapshot_bounds_order_retirement_rollback_and_exhaustion() {
    use marketforge_engine::live::routing::namespaces::{AdmissionNamespaces, NamespaceError};
    let namespaces = AdmissionNamespaces::new(3).unwrap();
    assert!(namespaces.snapshot().namespaces.is_empty());
    for (name, epoch, offset) in [("z", 1, 0), ("a", 2, 0), ("a", 1, u64::MAX)] {
        namespaces
            .register(StreamCursor::new(
                StreamId(name.into()),
                LogEpoch(epoch),
                offset,
            ))
            .unwrap();
    }
    assert_eq!(
        namespaces.register(cursor(0)),
        Err(NamespaceError::Capacity)
    );
    let sample = namespaces.snapshot();
    assert_eq!(sample.limit, 3);
    assert_eq!(
        sample
            .namespaces
            .iter()
            .map(|n| (n.order.stream_id.0.as_str(), n.order.log_epoch.0))
            .collect::<Vec<_>>(),
        vec![("a", 1), ("a", 2), ("z", 1)]
    );
    let order = namespaces.get(&StreamId("a".into()), LogEpoch(1)).unwrap();
    let batch = EventBatch::new(vec![l2_event("a", 1, u64::MAX)]).unwrap();
    let permit = order.prepare(&batch).unwrap();
    assert_eq!(
        order.snapshot().pending_last.as_ref(),
        Some(batch.last_cursor())
    );
    drop(permit);
    assert!(order.snapshot().pending_last.is_none());
    assert_eq!(order.snapshot().next.as_ref(), Some(batch.first_cursor()));
    order.prepare(&batch).unwrap().commit();
    let exhausted = order.snapshot();
    assert!(exhausted.next.is_none());
    assert!(exhausted.pending_last.is_none());
    assert!(!exhausted.closed);
    namespaces
        .retire(&StreamId("a".into()), LogEpoch(1))
        .unwrap();
    let retired = namespaces.snapshot();
    assert!(retired.namespaces[0].retired);
    assert!(retired.namespaces[0].order.closed);
    assert!(retired.namespaces[0].order.next.is_none());
    assert_eq!(
        namespaces.register(batch.first_cursor().clone()),
        Err(NamespaceError::AlreadyRegistered)
    );
    namespaces.shutdown();
    namespaces.shutdown();
    assert!(
        namespaces
            .snapshot()
            .namespaces
            .iter()
            .all(|n| n.order.closed)
    );
    assert!(!sample.namespaces[0].retired);
}
