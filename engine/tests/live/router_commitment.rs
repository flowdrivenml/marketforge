//! Integration tests for required-destination fan-out commitment.
//!
//! Verifies:
//! - Required destination commitment.
//! - Partial commitment visibility.
//! - Registry revision validation.
//! - Router shutdown.
//! - Cursor preservation.
//! - Destination queue ownership.
//!
//! Stage 3A does not guarantee atomic multi-destination commitment.

use std::{
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
};

use marketforge_engine::live::{
    foundation::cancellation::CancellationToken,
    model::delivery::{DeliveryCapacity, DeliveryPolicy, DestinationKind},
    ports::routing::{
        DestinationAdmission, DestinationDescriptor, DestinationId, EventBatch,
        EventDestinationPort, RoutingFuture,
    },
    routing::{
        admission::AdmissionContext,
        destination::DestinationCapacity,
        queue::DestinationQueue,
        router::{
            FanoutAdmissionCoordinator, FanoutCommitResult, FanoutCommitmentCoordinator,
            FanoutCommitmentError, PreparedFanout, RouterRegistry,
        },
    },
};

use crate::live::support::live_events::l2_event;

// ------------------------------------------------------------
// Minimal runtime-independent future executor
// ------------------------------------------------------------

struct NoopWake;

impl Wake for NoopWake {
    fn wake(self: Arc<Self>) {}

    fn wake_by_ref(self: &Arc<Self>) {}
}

/// Poll a future that is expected to complete immediately.
///
/// This helper deliberately fails if the future becomes pending.
/// Tests requiring asynchronous wakeups should use the existing
/// deterministic polling infrastructure.
fn ready<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(NoopWake));
    let mut context = Context::from_waker(&waker);

    let mut future = Box::pin(future);

    match future.as_mut().poll(&mut context) {
        Poll::Ready(result) => result,
        Poll::Pending => {
            panic!("test operation unexpectedly returned Poll::Pending")
        }
    }
}

// ------------------------------------------------------------
// Destination fixtures
// ------------------------------------------------------------

struct TestDestination {
    descriptor: DestinationDescriptor,
}

impl EventDestinationPort for TestDestination {
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

fn descriptor(id: u64) -> DestinationDescriptor {
    DestinationDescriptor {
        id: DestinationId(id),
        kind: DestinationKind::Recording,
        policy: DeliveryPolicy::Required,
        capacity: DeliveryCapacity::new(100, 100_000).unwrap(),
    }
}

fn event_batch() -> EventBatch {
    EventBatch::new(vec![
        l2_event("commitment-source", 1, 1),
        l2_event("commitment-source", 1, 2),
    ])
    .unwrap()
}

fn different_batch() -> EventBatch {
    EventBatch::new(vec![
        l2_event("commitment-source", 1, 10),
        l2_event("commitment-source", 1, 11),
    ])
    .unwrap()
}

fn register_destination(registry: &RouterRegistry, id: u64) -> DestinationQueue {
    let descriptor = descriptor(id);
    let capacity = DestinationCapacity::new(descriptor.capacity, 10).unwrap();

    registry
        .register_queue(
            Arc::new(TestDestination { descriptor }),
            DestinationQueue::new(capacity),
        )
        .unwrap();

    registry.peek_queue(DestinationId(id)).unwrap()
}

fn prepare(registry: &RouterRegistry, batch: &EventBatch) -> PreparedFanout {
    let plan = registry.plan(batch).unwrap();

    let context = AdmissionContext::without_deadline(CancellationToken::new());

    ready(FanoutAdmissionCoordinator::prepare(
        &plan, batch, context, None,
    ))
    .unwrap()
}

fn commit(
    registry: &RouterRegistry,
    prepared: PreparedFanout,
    batch: EventBatch,
) -> FanoutCommitResult {
    FanoutCommitmentCoordinator::commit(registry, prepared, batch)
}

// ------------------------------------------------------------
// Test 1 — Single required destination
// ------------------------------------------------------------

#[test]
fn single_required_destination_commits_successfully() {
    let registry = RouterRegistry::new();
    let destination = register_destination(&registry, 1);

    let batch = event_batch();

    let prepared = prepare(&registry, &batch);

    let result = commit(&registry, prepared, batch.clone());

    match result {
        FanoutCommitResult::Complete { through, committed } => {
            assert_eq!(through, *batch.last_cursor());
            assert_eq!(committed.len(), 1);
            assert_eq!(committed[0].id, DestinationId(1));
        }

        _ => panic!("expected complete fan-out commitment"),
    }

    let snapshot = destination.snapshot();

    assert_eq!(snapshot.queued_batches, 1);
    assert_eq!(snapshot.queued_events, batch.len());

    assert_eq!(destination.capacity().usage().occupied_events, batch.len());
}

// ------------------------------------------------------------
// Test 2 — Two required destinations
// ------------------------------------------------------------

#[test]
fn two_required_destinations_commit_successfully() {
    let registry = RouterRegistry::new();

    let first = register_destination(&registry, 1);
    let second = register_destination(&registry, 2);

    let batch = event_batch();

    let prepared = prepare(&registry, &batch);

    let result = commit(&registry, prepared, batch.clone());

    match result {
        FanoutCommitResult::Complete { through, committed } => {
            assert_eq!(through, *batch.last_cursor());
            assert_eq!(committed.len(), 2);

            assert_eq!(committed[0].id, DestinationId(1));
            assert_eq!(committed[1].id, DestinationId(2));
        }

        _ => panic!("expected complete fan-out"),
    }

    assert_eq!(first.snapshot().queued_batches, 1);
    assert_eq!(second.snapshot().queued_batches, 1);

    assert_eq!(first.capacity().usage().occupied_events, batch.len());

    assert_eq!(second.capacity().usage().occupied_events, batch.len());
}

// ------------------------------------------------------------
// Test 3 — First queue closes before commitment
// ------------------------------------------------------------

#[test]
fn first_queue_closure_is_reported() {
    let registry = RouterRegistry::new();

    let first = register_destination(&registry, 1);
    let second = register_destination(&registry, 2);

    let batch = event_batch();

    let prepared = prepare(&registry, &batch);

    first.close();

    let result = commit(&registry, prepared, batch.clone());

    match result {
        FanoutCommitResult::Partial(partial) => {
            assert_eq!(partial.committed.len(), 1);
            assert_eq!(partial.failed.len(), 1);

            assert_eq!(partial.failed[0].id, DestinationId(1));

            assert_eq!(partial.committed[0].id, DestinationId(2));

            assert_eq!(partial.batch.last_cursor(), batch.last_cursor());
        }

        _ => panic!("expected explicit failed commitment"),
    }

    assert_eq!(first.snapshot().queued_batches, 0);
    assert_eq!(second.snapshot().queued_batches, 1);

    assert_eq!(first.capacity().usage().occupied_events, 0);
}

// ------------------------------------------------------------
// Test 4 — Second queue closes before commitment
// ------------------------------------------------------------

#[test]
fn second_queue_closure_preserves_partial_commitment() {
    let registry = RouterRegistry::new();

    let first = register_destination(&registry, 1);
    let second = register_destination(&registry, 2);

    let batch = event_batch();

    let prepared = prepare(&registry, &batch);

    second.close();

    let result = commit(&registry, prepared, batch.clone());

    match result {
        FanoutCommitResult::Partial(partial) => {
            assert_eq!(partial.committed.len(), 1);
            assert_eq!(partial.failed.len(), 1);

            assert_eq!(partial.committed[0].id, DestinationId(1));

            assert_eq!(partial.failed[0].id, DestinationId(2));
        }

        _ => panic!("expected partial fan-out"),
    }

    assert_eq!(first.snapshot().queued_batches, 1);
    assert_eq!(second.snapshot().queued_batches, 0);

    assert_eq!(second.capacity().usage().occupied_events, 0);
}

// ------------------------------------------------------------
// Test 5 — Registry revision changes
// ------------------------------------------------------------

#[test]
fn registry_revision_change_rejects_before_commitment() {
    let registry = RouterRegistry::new();

    let first = register_destination(&registry, 1);

    let batch = event_batch();

    let prepared = prepare(&registry, &batch);

    // Registering another destination changes the revision.
    let second = register_destination(&registry, 2);

    let result = commit(&registry, prepared, batch.clone());

    match result {
        FanoutCommitResult::Rejected(rejection) => {
            assert_eq!(rejection.reason, FanoutCommitmentError::RegistryChanged);

            assert_eq!(rejection.batch.last_cursor(), batch.last_cursor());
        }

        _ => panic!("expected registry revision rejection"),
    }

    assert_eq!(first.snapshot().queued_batches, 0);
    assert_eq!(second.snapshot().queued_batches, 0);

    assert_eq!(first.capacity().usage().occupied_events, 0);
}

// ------------------------------------------------------------
// Test 6 — Router shutdown
// ------------------------------------------------------------

#[test]
fn router_shutdown_rejects_prepared_commitment() {
    let registry = RouterRegistry::new();

    let destination = register_destination(&registry, 1);

    let batch = event_batch();

    let prepared = prepare(&registry, &batch);

    registry.shutdown();

    let result = commit(&registry, prepared, batch.clone());

    match result {
        FanoutCommitResult::Rejected(rejection) => {
            assert_eq!(rejection.reason, FanoutCommitmentError::ShuttingDown);
        }

        _ => panic!("expected shutdown rejection"),
    }

    assert_eq!(destination.snapshot().queued_batches, 0);

    assert_eq!(destination.capacity().usage().occupied_events, 0);
}

// ------------------------------------------------------------
// Test 7 — Cursor mismatch
// ------------------------------------------------------------

#[test]
fn cursor_mismatch_rejects_before_commitment() {
    let registry = RouterRegistry::new();

    let destination = register_destination(&registry, 1);

    let original = event_batch();

    let prepared = prepare(&registry, &original);

    let different = different_batch();

    let result = commit(&registry, prepared, different.clone());

    match result {
        FanoutCommitResult::Rejected(rejection) => {
            assert_eq!(rejection.reason, FanoutCommitmentError::CursorMismatch);

            assert_eq!(rejection.batch.last_cursor(), different.last_cursor());
        }

        _ => panic!("expected cursor mismatch rejection"),
    }

    assert_eq!(destination.snapshot().queued_batches, 0);

    assert_eq!(destination.capacity().usage().occupied_events, 0);
}

// ------------------------------------------------------------
// Test 9 — Dropped preparation releases reservations
// ------------------------------------------------------------

#[test]
fn dropping_prepared_fanout_releases_all_reservations() {
    let registry = RouterRegistry::new();

    let first = register_destination(&registry, 1);
    let second = register_destination(&registry, 2);

    let batch = event_batch();

    let prepared = prepare(&registry, &batch);

    assert_eq!(prepared.destination_count(), 2);

    assert_eq!(first.capacity().usage().occupied_events, batch.len());

    assert_eq!(second.capacity().usage().occupied_events, batch.len());

    drop(prepared);

    assert_eq!(first.capacity().usage().occupied_events, 0);

    assert_eq!(second.capacity().usage().occupied_events, 0);

    assert_eq!(first.snapshot().queued_batches, 0);
    assert_eq!(second.snapshot().queued_batches, 0);
}

// ------------------------------------------------------------
// Test 10 — Successful commitment retains capacity
// ------------------------------------------------------------

#[test]
fn successful_commitment_transfers_capacity_to_queue() {
    let registry = RouterRegistry::new();

    let destination = register_destination(&registry, 1);
    let batch = event_batch();

    let prepared = prepare(&registry, &batch);

    assert_eq!(destination.capacity().usage().occupied_events, batch.len());

    let result = commit(&registry, prepared, batch.clone());

    assert!(matches!(result, FanoutCommitResult::Complete { .. }));

    // Reservation ownership transfers to QueuedBatch.
    assert_eq!(destination.capacity().usage().occupied_events, batch.len());

    let queued = destination.try_dequeue().unwrap();

    assert_eq!(queued.events(), batch.len());

    // Capacity remains occupied while the consumer owns QueuedBatch.
    assert_eq!(destination.capacity().usage().occupied_events, batch.len());

    drop(queued);

    // Dropping QueuedBatch releases its capacity permit.
    assert_eq!(destination.capacity().usage().occupied_events, 0);

    assert_eq!(destination.capacity().usage().occupied_bytes, 0);
}

// ------------------------------------------------------------
// Test 11 — Partial commitment releases failed reservation
// ------------------------------------------------------------

#[test]
fn partial_commitment_releases_failed_destination_capacity() {
    let registry = RouterRegistry::new();

    let first = register_destination(&registry, 1);
    let second = register_destination(&registry, 2);

    let batch = event_batch();

    let prepared = prepare(&registry, &batch);

    second.close();

    let result = commit(&registry, prepared, batch.clone());

    match result {
        FanoutCommitResult::Partial(partial) => {
            assert_eq!(partial.committed.len(), 1);
            assert_eq!(partial.failed.len(), 1);

            assert_eq!(partial.committed[0].id, DestinationId(1));

            assert_eq!(partial.failed[0].id, DestinationId(2));

            assert_eq!(partial.batch.first_cursor(), batch.first_cursor());

            assert_eq!(partial.batch.last_cursor(), batch.last_cursor());
        }

        _ => panic!("expected partial commitment"),
    }

    // Successful destination retains responsibility.
    assert_eq!(first.snapshot().queued_batches, 1);

    assert_eq!(first.capacity().usage().occupied_events, batch.len());

    // Failed destination releases its reservation.
    assert_eq!(second.snapshot().queued_batches, 0);

    assert_eq!(second.capacity().usage().occupied_events, 0);

    assert_eq!(second.capacity().usage().occupied_bytes, 0);
}

// ------------------------------------------------------------
// Test 12 — Original Arc event identity preserved
// ------------------------------------------------------------

#[test]
fn commitment_preserves_original_event_identity() {
    let registry = RouterRegistry::new();

    let first = register_destination(&registry, 1);
    let second = register_destination(&registry, 2);

    let batch = event_batch();

    let original_first = batch.events()[0].clone();
    let original_second = batch.events()[1].clone();

    let prepared = prepare(&registry, &batch);

    let result = commit(&registry, prepared, batch.clone());

    assert!(matches!(result, FanoutCommitResult::Complete { .. }));

    let first_queued = first.try_dequeue().unwrap();
    let second_queued = second.try_dequeue().unwrap();

    assert!(Arc::ptr_eq(
        &original_first,
        &first_queued.batch().events()[0]
    ));

    assert!(Arc::ptr_eq(
        &original_first,
        &second_queued.batch().events()[0]
    ));

    assert!(Arc::ptr_eq(
        &original_second,
        &first_queued.batch().events()[1]
    ));

    assert!(Arc::ptr_eq(
        &original_second,
        &second_queued.batch().events()[1]
    ));

    assert_eq!(first_queued.batch().first_cursor(), batch.first_cursor());

    assert_eq!(second_queued.batch().last_cursor(), batch.last_cursor());

    drop(first_queued);
    drop(second_queued);

    assert_eq!(first.capacity().usage().occupied_events, 0);

    assert_eq!(second.capacity().usage().occupied_events, 0);
}

// ------------------------------------------------------------
// Test 13 — Commitment consumes prepared ownership
// ------------------------------------------------------------

#[test]
fn prepared_fanout_is_consumed_by_commitment() {
    let registry = RouterRegistry::new();

    let destination = register_destination(&registry, 1);
    let batch = event_batch();

    let prepared = prepare(&registry, &batch);

    let result = commit(&registry, prepared, batch.clone());

    assert!(matches!(result, FanoutCommitResult::Complete { .. }));

    // PreparedFanout is consumed by commit().
    // It cannot be reused without obtaining new reservations.
    //
    // Verify that exactly one batch was inserted.

    assert_eq!(destination.snapshot().queued_batches, 1);

    let queued = destination.try_dequeue().unwrap();

    assert_eq!(queued.batch().first_cursor(), batch.first_cursor());

    assert!(destination.try_dequeue().is_none());

    drop(queued);

    assert_eq!(destination.capacity().usage().occupied_events, 0);
}

// ------------------------------------------------------------
// Test 14 — Deterministic destination commitment order
// ------------------------------------------------------------

#[test]
fn commitment_follows_deterministic_destination_order() {
    let registry = RouterRegistry::new();

    // Register destinations in deliberately unsorted order.
    let ninth = register_destination(&registry, 9);
    let second = register_destination(&registry, 2);
    let seventh = register_destination(&registry, 7);
    let first = register_destination(&registry, 1);

    let batch = event_batch();

    let prepared = prepare(&registry, &batch);

    let prepared_ids: Vec<_> = prepared
        .destination_ids()
        .into_iter()
        .map(|id| id.0)
        .collect();

    assert_eq!(prepared_ids, vec![1, 2, 7, 9]);

    let result = commit(&registry, prepared, batch.clone());

    match result {
        FanoutCommitResult::Complete { through, committed } => {
            assert_eq!(through, *batch.last_cursor());

            let committed_ids: Vec<_> = committed
                .iter()
                .map(|destination| destination.id.0)
                .collect();

            assert_eq!(committed_ids, vec![1, 2, 7, 9]);
        }

        _ => panic!("expected complete commitment"),
    }

    for destination in [&first, &second, &seventh, &ninth] {
        assert_eq!(destination.snapshot().queued_batches, 1);

        assert_eq!(destination.capacity().usage().occupied_events, batch.len());

        let queued = destination.try_dequeue().unwrap();

        assert_eq!(queued.batch().last_cursor(), batch.last_cursor());

        drop(queued);

        assert_eq!(destination.capacity().usage().occupied_events, 0);
    }
}
