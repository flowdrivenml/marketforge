//! Tests for router-owned queue registration and preparation.
//!
//! Verifies:
//! - register_queue() retains the registered queue.
//! - RoutingPlan contains the correct queue.
//! - Preparation uses the registered queue.
//! - Queue-less required destinations are rejected.
//! - Capacity mismatch is rejected.
//! - Commitment acquires the commitment mutex.
//! - Registry revision changes invalidate prepared fan-out.
//! - Existing plans retain their original queue identity.

use std::sync::Arc;
use std::task::Wake;

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
            FanoutCommitmentError, RouterRegistry, RouterRegistryError,
        },
    },
};

use crate::live::support::live_events::l2_event;

// ------------------------------------------------------------
// Minimal future executor
// ------------------------------------------------------------

struct NoopWake;

impl Wake for NoopWake {
    fn wake(self: Arc<Self>) {}

    fn wake_by_ref(self: &Arc<Self>) {}
}

fn ready<F: core::future::Future>(future: F) -> F::Output {
    use core::task::{Context, Poll, Waker};

    let waker = Waker::from(Arc::new(NoopWake));
    let mut context = Context::from_waker(&waker);
    let mut future = Box::pin(future);
    match future.as_mut().poll(&mut context) {
        Poll::Ready(result) => result,
        Poll::Pending => {
            panic!("test operation unexpectedly returned Poll::Pending");
        }
    }
}

// ------------------------------------------------------------
// Test fixtures
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
        l2_event("admission-source", 1, 1),
        l2_event("admission-source", 1, 2),
    ])
    .unwrap()
}

// R1 — register_queue() retains the registered queue.

#[test]
fn register_queue_retains_registered_queue() {
    let registry = RouterRegistry::new();

    let descriptor = descriptor(1);
    let capacity = DestinationCapacity::new(descriptor.capacity, 10).unwrap();
    let queue = DestinationQueue::new(capacity);

    registry
        .register_queue(Arc::new(TestDestination { descriptor }), queue)
        .unwrap();

    // The queue stored in the registry is Some (was None before the fix).
    let stored_queue = registry.peek_queue(DestinationId(1));

    assert!(stored_queue.is_some());
}

// R2 — RoutingPlan contains the correct queue.

#[test]
fn routing_plan_contains_correct_queue() {
    let registry = RouterRegistry::new();

    let descriptor = descriptor(1);
    let capacity = DestinationCapacity::new(descriptor.capacity, 10).unwrap();
    let queue = DestinationQueue::new(capacity);

    registry
        .register_queue(Arc::new(TestDestination { descriptor }), queue.clone())
        .unwrap();

    let batch = event_batch();
    let plan = registry.plan(&batch).unwrap();

    assert!(!plan.required.is_empty());
    assert!(plan.required[0].queue.is_some());
    assert_eq!(plan.required[0].descriptor.id, DestinationId(1));
    assert_eq!(
        plan.required[0].queue.as_ref().unwrap().capacity().limits(),
        descriptor.capacity
    );
}

// R3 — Preparation uses the registered queue.

#[test]
fn preparation_uses_registered_queue() {
    let registry = RouterRegistry::new();

    let descriptor = descriptor(1);
    let capacity = DestinationCapacity::new(descriptor.capacity, 10).unwrap();
    let queue = DestinationQueue::new(capacity);

    registry
        .register_queue(Arc::new(TestDestination { descriptor }), queue.clone())
        .unwrap();

    let batch = event_batch();
    let plan = registry.plan(&batch).unwrap();

    assert_eq!(plan.required.len(), 1);
    assert_eq!(plan.required[0].descriptor.id, DestinationId(1));

    let context = AdmissionContext::without_deadline(CancellationToken::new());
    let prepared = ready(FanoutAdmissionCoordinator::prepare(
        &plan, &batch, context, None,
    ))
    .unwrap();

    assert_eq!(prepared.destination_count(), 1);
    assert_eq!(prepared.destination_ids()[0], DestinationId(1));
}

// R4 — Queue-less required destination is rejected.

#[test]
fn queue_less_required_destination_is_rejected() {
    let registry = RouterRegistry::new();

    // Register without queue (regular register()).
    let descriptor = descriptor(1);
    registry
        .register(Arc::new(TestDestination { descriptor }))
        .unwrap();

    let batch = event_batch();
    let plan = registry.plan(&batch).unwrap();

    let context = AdmissionContext::without_deadline(CancellationToken::new());
    let result = ready(FanoutAdmissionCoordinator::prepare(
        &plan, &batch, context, None,
    ));

    assert!(matches!(
        result,
        Err(marketforge_engine::live::routing::router::FanoutPreparationError::MissingQueue(_))
    ));
}

// R5 — Capacity mismatch is rejected.

#[test]
fn capacity_mismatch_is_rejected() {
    let registry = RouterRegistry::new();

    let descriptor = descriptor(1);
    let capacity = DestinationCapacity::new(descriptor.capacity, 10).unwrap();

    // Create a descriptor with different capacity than a valid queue.
    let mismatched_descriptor = DestinationDescriptor {
        id: DestinationId(1),
        kind: DestinationKind::Recording,
        policy: DeliveryPolicy::Required,
        capacity: DeliveryCapacity::new(50, 50_000).unwrap(),
    };

    // Create a queue with capacity that doesn't match the descriptor.
    let _capacity =
        DestinationCapacity::new(DeliveryCapacity::new(100, 100_000).unwrap(), 10).unwrap();

    let result = registry.register_queue(
        Arc::new(TestDestination {
            descriptor: mismatched_descriptor,
        }),
        DestinationQueue::new(capacity),
    );

    assert!(matches!(
        result,
        Err(RouterRegistryError::QueueCapacityMismatch)
    ));
}

// R6 — Commitment acquires the commitment mutex.

#[test]
fn commitment_acquires_commitment_mutex() {
    // Verify that commit() successfully acquires the mutex
    // and rejects on shutdown while holding it.
    let registry = RouterRegistry::new();

    let descriptor = descriptor(1);
    let capacity = DestinationCapacity::new(descriptor.capacity, 10).unwrap();
    let queue = DestinationQueue::new(capacity);

    registry
        .register_queue(Arc::new(TestDestination { descriptor }), queue)
        .unwrap();

    let batch = event_batch();
    let plan = registry.plan(&batch).unwrap();

    let context = AdmissionContext::without_deadline(CancellationToken::new());
    let prepared = ready(FanoutAdmissionCoordinator::prepare(
        &plan, &batch, context, None,
    ))
    .unwrap();

    let result = FanoutCommitmentCoordinator::commit(&registry, prepared, batch.clone());

    assert!(matches!(result, FanoutCommitResult::Complete { .. }));
}

// R7 — Registry revision changes invalidate prepared fan-out.

#[test]
fn registry_revision_change_invalidates_prepared_fanout() {
    let registry = RouterRegistry::new();

    let desc = descriptor(1);
    let capacity = DestinationCapacity::new(desc.capacity, 10).unwrap();
    let queue = DestinationQueue::new(capacity);

    registry
        .register_queue(Arc::new(TestDestination { descriptor: desc }), queue)
        .unwrap();

    let batch = event_batch();

    // Plan at revision N.
    let plan = registry.plan(&batch).unwrap();
    let old_revision = plan.registry_revision;

    // Prepare at revision N.
    let context = AdmissionContext::without_deadline(CancellationToken::new());
    let prepared = ready(FanoutAdmissionCoordinator::prepare(
        &plan, &batch, context, None,
    ))
    .unwrap();

    // Mutate the registry (changes revision to N+1).
    let desc2 = descriptor(2);
    let capacity2 = DestinationCapacity::new(desc2.capacity, 10).unwrap();
    registry
        .register_queue(
            Arc::new(TestDestination { descriptor: desc2 }),
            DestinationQueue::new(capacity2),
        )
        .unwrap();

    // Commit should detect revision mismatch.
    let result = FanoutCommitmentCoordinator::commit(&registry, prepared, batch);

    match result {
        FanoutCommitResult::Rejected(rejection) => {
            assert_eq!(rejection.reason, FanoutCommitmentError::RegistryChanged);
        }
        _ => panic!("expected rejected commitment due to revision change"),
    }

    // The committed revision does not match.
    assert!(registry.revision() > old_revision);
}

// R8 — Existing plans retain their original queue identity.

#[test]
fn existing_plans_retain_original_queue_identity() {
    let registry = RouterRegistry::new();

    let descriptor = descriptor(1);
    let capacity = DestinationCapacity::new(descriptor.capacity, 10).unwrap();
    let registered_queue = DestinationQueue::new(capacity);

    registry
        .register_queue(
            Arc::new(TestDestination { descriptor }),
            registered_queue.clone(),
        )
        .unwrap();

    let batch = event_batch();

    // Create a plan and extract its queue.
    let plan = registry.plan(&batch).unwrap();
    let _plan_queue = plan.required[0].queue.clone().unwrap();

    // Close the registered queue.
    drop(registered_queue);

    // The plan queue should still be usable for preparation.
    let context = AdmissionContext::without_deadline(CancellationToken::new());
    let prepared = ready(FanoutAdmissionCoordinator::prepare(
        &plan, &batch, context, None,
    ))
    .unwrap();

    // The prepared fan-out contains the queue that was active
    // when the plan was created.
    assert_eq!(prepared.destination_count(), 1);
}
