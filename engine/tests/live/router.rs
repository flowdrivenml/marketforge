use std::sync::Arc;

use marketforge_engine::live::{
    model::delivery::{DeliveryCapacity, DeliveryPolicy, DestinationKind},
    ports::routing::{
        DestinationAdmission, DestinationDescriptor, DestinationId, EventBatch,
        EventDestinationPort, RoutingFuture,
    },
    routing::router::{RouterRegistry, RouterRegistryError},
};

use crate::live::support::live_events::l2_event;

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

fn destination(
    id: u64,
    kind: DestinationKind,
    policy: DeliveryPolicy,
) -> Arc<dyn EventDestinationPort> {
    Arc::new(TestDestination {
        descriptor: DestinationDescriptor {
            id: DestinationId(id),
            kind,
            policy,
            capacity: DeliveryCapacity::new(100, 100_000).unwrap(),
        },
    })
}

fn batch() -> EventBatch {
    EventBatch::new(vec![
        l2_event("router-source", 1, 1),
        l2_event("router-source", 1, 2),
    ])
    .unwrap()
}

#[test]
fn registry_accepts_multiple_destinations() {
    let registry = RouterRegistry::new();

    registry
        .register(destination(
            1,
            DestinationKind::Publication,
            DeliveryPolicy::Required,
        ))
        .unwrap();

    registry
        .register(destination(
            2,
            DestinationKind::Recording,
            DeliveryPolicy::Durable,
        ))
        .unwrap();

    assert_eq!(registry.destination_count(), 2);
}

#[test]
fn registry_rejects_duplicate_ids() {
    let registry = RouterRegistry::new();

    registry
        .register(destination(
            1,
            DestinationKind::Publication,
            DeliveryPolicy::Required,
        ))
        .unwrap();

    assert_eq!(
        registry.register(destination(
            1,
            DestinationKind::Recording,
            DeliveryPolicy::Required,
        )),
        Err(RouterRegistryError::DuplicateDestination)
    );
}

#[test]
fn registry_rejects_invalid_policy() {
    let registry = RouterRegistry::new();

    assert_eq!(
        registry.register(destination(
            1,
            DestinationKind::Publication,
            DeliveryPolicy::Durable,
        )),
        Err(RouterRegistryError::InvalidDestinationPolicy)
    );
}

#[test]
fn plan_separates_required_and_best_effort() {
    let registry = RouterRegistry::new();

    registry
        .register(destination(
            1,
            DestinationKind::Publication,
            DeliveryPolicy::BestEffort,
        ))
        .unwrap();

    registry
        .register(destination(
            2,
            DestinationKind::Recording,
            DeliveryPolicy::Required,
        ))
        .unwrap();

    registry
        .register(destination(
            3,
            DestinationKind::Retention,
            DeliveryPolicy::Durable,
        ))
        .unwrap();

    let plan = registry.plan(&batch()).unwrap();

    assert_eq!(plan.required_count(), 2);
    assert_eq!(plan.best_effort_count(), 1);
    assert_eq!(plan.destination_count(), 3);
}

#[test]
fn plan_orders_destinations_deterministically() {
    let registry = RouterRegistry::new();

    for id in [9, 2, 7, 1] {
        registry
            .register(destination(
                id,
                DestinationKind::Publication,
                DeliveryPolicy::Required,
            ))
            .unwrap();
    }

    let plan = registry.plan(&batch()).unwrap();

    let ids: Vec<_> = plan
        .required
        .iter()
        .map(|entry| entry.descriptor.id.0)
        .collect();

    assert_eq!(ids, vec![1, 2, 7, 9]);
}

#[test]
fn plan_preserves_original_cursor_range() {
    let registry = RouterRegistry::new();

    registry
        .register(destination(
            1,
            DestinationKind::Recording,
            DeliveryPolicy::Required,
        ))
        .unwrap();

    let original = batch();
    let plan = registry.plan(&original).unwrap();

    assert_eq!(plan.first_cursor, *original.first_cursor());
    assert_eq!(plan.last_cursor, *original.last_cursor());
}

#[test]
fn unregister_changes_future_plans_not_existing_snapshots() {
    let registry = RouterRegistry::new();

    registry
        .register(destination(
            1,
            DestinationKind::Publication,
            DeliveryPolicy::Required,
        ))
        .unwrap();

    let first = registry.plan(&batch()).unwrap();

    registry.unregister(DestinationId(1)).unwrap();

    assert_eq!(first.destination_count(), 1);
    assert_eq!(registry.destination_count(), 0);
}

#[test]
fn shutdown_rejects_future_plans() {
    let registry = RouterRegistry::new();

    registry
        .register(destination(
            1,
            DestinationKind::Publication,
            DeliveryPolicy::Required,
        ))
        .unwrap();

    registry.shutdown();

    assert!(matches!(
        registry.plan(&batch()),
        Err(RouterRegistryError::ShuttingDown)
    ));
}

#[test]
fn registry_shutdown_wakes_capacity_waiters_outside_membership_fence_and_preserves_payloads() {
    use marketforge_engine::live::routing::{
        destination::{CapacityError, DestinationCapacity},
        queue::DestinationQueue,
    };
    use std::{
        future::Future,
        sync::atomic::{AtomicUsize, Ordering},
        task::{Context, Poll, Wake, Waker},
    };
    struct Observer {
        registry: Arc<RouterRegistry>,
        wakes: AtomicUsize,
    }
    impl Wake for Observer {
        fn wake(self: Arc<Self>) {
            assert!(self.registry.snapshot().shutting_down);
            assert!(matches!(
                self.registry.plan(&batch()),
                Err(RouterRegistryError::ShuttingDown)
            ));
            self.wakes.fetch_add(1, Ordering::SeqCst);
        }
    }
    let registry = Arc::new(RouterRegistry::new());
    let adapter = destination(1, DestinationKind::Publication, DeliveryPolicy::Required);
    let queue =
        DestinationQueue::new(DestinationCapacity::new(adapter.descriptor().capacity, 1).unwrap());
    registry.register_queue(adapter, queue.clone()).unwrap();
    queue.try_enqueue(batch()).unwrap();
    let before = queue.observe();
    let limits = queue.capacity().limits();
    let mut waiting = Box::pin(
        queue
            .capacity()
            .reserve(limits.max_pending_events, limits.max_pending_bytes),
    );
    let observer = Arc::new(Observer {
        registry: registry.clone(),
        wakes: AtomicUsize::new(0),
    });
    let waker = Waker::from(observer.clone());
    let mut cx = Context::from_waker(&waker);
    assert!(waiting.as_mut().poll(&mut cx).is_pending());
    registry.shutdown();
    assert_eq!(observer.wakes.load(Ordering::SeqCst), 1);
    assert!(matches!(
        waiting.as_mut().poll(&mut cx),
        Poll::Ready(Err(CapacityError::ShuttingDown))
    ));
    let after = queue.observe();
    assert!(after.queue.closed && after.capacity.shutting_down);
    assert_eq!(after.queue.queued_events, before.queue.queued_events);
    assert_eq!(
        after.capacity.occupied_events,
        before.capacity.occupied_events
    );
    assert!(queue.try_enqueue(batch()).is_err());
    assert!(matches!(
        queue.capacity().try_reserve(1, 1),
        Err(CapacityError::ShuttingDown)
    ));
    registry.shutdown();
    let retained = queue.try_dequeue().unwrap();
    assert_eq!(
        queue.capacity().usage().occupied_events,
        before.capacity.occupied_events
    );
    drop(retained);
    assert_eq!(queue.capacity().usage().occupied_events, 0);
    assert_eq!(queue.capacity().usage().occupied_bytes, 0);
}

#[test]
fn router_shutdown_composes_preparation_queue_payload_and_delivery_responsibility() {
    use marketforge_engine::live::{
        foundation::CancellationToken,
        foundation::{clock::ManualClock, resources::ResourceUsage},
        routing::{
            acknowledgement::{AcknowledgementTracker, FinalizationJournal},
            admission::{AdmissionContext, DeadlineFuture},
            destination::DestinationCapacity,
            driver::DeliveryDriver,
            namespaces::AdmissionNamespaces,
            ordered::OrderedAdmissionBudget,
            queue::DestinationQueue,
            shutdown::{RouterShutdownOwners, RouterShutdownStep},
            supervisor::DeliverySupervisor,
            tracking_recovery::TrackingRecovery,
            worker::DestinationWorkers,
        },
    };
    use std::{
        future::Future,
        num::NonZeroUsize,
        task::{Context, Poll, Waker},
        time::Instant,
    };
    let registry = RouterRegistry::new();
    let adapter = destination(1, DestinationKind::Publication, DeliveryPolicy::Required);
    let queue =
        DestinationQueue::new(DestinationCapacity::new(adapter.descriptor().capacity, 1).unwrap());
    registry.register_queue(adapter, queue.clone()).unwrap();
    queue.try_enqueue(batch()).unwrap();
    let retained = queue.try_dequeue().unwrap();
    let namespaces = AdmissionNamespaces::new(1).unwrap();
    let first = batch().first_cursor().clone();
    namespaces.register(first.clone()).unwrap();
    let budget = OrderedAdmissionBudget::new(ResourceUsage {
        items: 10,
        bytes: 100_000,
    })
    .unwrap();
    let mut prepare = Box::pin(budget.prepare(
        &registry,
        &namespaces,
        batch(),
        AdmissionContext::without_deadline(CancellationToken::new()),
        Arc::new(ManualClock::new(Instant::now(), 0)),
        None,
    ));
    let mut cx = Context::from_waker(Waker::noop());
    let Poll::Ready(Ok(prepared)) = prepare.as_mut().poll(&mut cx) else {
        panic!("capacity available")
    };
    drop(prepare);
    let limit = NonZeroUsize::new(1).unwrap();
    let mut delivery = DeliveryDriver::new(
        DeliverySupervisor::new(DestinationWorkers::new(limit), limit),
        AcknowledgementTracker::new(1).unwrap(),
        TrackingRecovery::new(1).unwrap(),
        FinalizationJournal::new(1).unwrap(),
    );
    let pending = delivery
        .admission_owners()
        .unwrap()
        .0
        .prepare_obligations(vec![])
        .unwrap();
    let mut owners = RouterShutdownOwners {
        namespaces: &namespaces,
        admission: &budget,
        destinations: &registry,
        delivery: &mut delivery,
    };
    assert!(!owners.snapshot().is_drained());
    let mut expired: DeadlineFuture = Box::pin(std::future::ready(()));
    assert!(matches!(
        owners.drive(&mut cx, &mut expired),
        RouterShutdownStep::DeadlineReached
    ));
    let snapshot = owners.snapshot();
    assert!(snapshot.namespaces.shutting_down && snapshot.destinations.shutting_down);
    assert_eq!(snapshot.admission_usage.items, 2);
    assert!(
        snapshot.namespaces.namespaces[0]
            .order
            .pending_last
            .is_some()
    );
    assert_eq!(snapshot.delivery.prepared_transactions, 1);
    assert!(owners.delivery.is_closed());
    let mut later: DeadlineFuture = Box::pin(std::future::pending());
    assert!(matches!(
        owners.drive(&mut cx, &mut later),
        RouterShutdownStep::Step(_)
    ));
    drop(prepared);
    assert_eq!(owners.snapshot().admission_usage, ResourceUsage::default());
    owners.delivery.abort_prepared(pending).unwrap();
    assert!(owners.snapshot().delivery.is_drained());
    assert!(!owners.snapshot().is_drained());
    drop(retained);
    assert!(matches!(
        owners.drive(&mut cx, &mut later),
        RouterShutdownStep::Drained
    ));
    assert!(matches!(
        owners.drive(&mut cx, &mut expired),
        RouterShutdownStep::Drained
    ));
    assert_eq!(
        owners.snapshot().namespaces.namespaces[0]
            .order
            .next
            .as_ref(),
        Some(&first)
    );
    assert!(owners.snapshot().is_drained());
}

#[test]
fn registry_shutdown_wakes_latest_coordinator_after_final_payload_release() {
    use marketforge_engine::live::routing::{
        destination::DestinationCapacity, queue::DestinationQueue,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Wake, Waker};
    struct Observer {
        registry: Arc<RouterRegistry>,
        wakes: AtomicUsize,
    }
    impl Wake for Observer {
        fn wake(self: Arc<Self>) {
            // Reentrant observation proves wake runs outside registry/capacity locks.
            assert!(self.registry.snapshot().shutting_down);
            self.wakes.fetch_add(1, Ordering::SeqCst);
        }
    }
    let registry = Arc::new(RouterRegistry::new());
    let adapter = destination(1, DestinationKind::Publication, DeliveryPolicy::Required);
    let queue =
        DestinationQueue::new(DestinationCapacity::new(adapter.descriptor().capacity, 1).unwrap());
    registry.register_queue(adapter, queue.clone()).unwrap();
    queue.try_enqueue(batch()).unwrap();
    let payload = queue.try_dequeue().unwrap();
    let make_observer = || {
        Arc::new(Observer {
            registry: registry.clone(),
            wakes: AtomicUsize::new(0),
        })
    };
    let old = make_observer();
    let latest = make_observer();
    let old_waker = Waker::from(old.clone());
    let latest_waker = Waker::from(latest.clone());
    assert!(
        registry
            .poll_shutdown(&mut Context::from_waker(&old_waker))
            .is_pending()
    );
    assert!(
        registry
            .poll_shutdown(&mut Context::from_waker(&latest_waker))
            .is_pending()
    );
    assert_eq!(queue.observe().capacity.occupied_events, 2);
    assert_eq!(latest.wakes.load(Ordering::SeqCst), 0);
    drop(payload);
    assert_eq!(old.wakes.load(Ordering::SeqCst), 0);
    assert_eq!(latest.wakes.load(Ordering::SeqCst), 1);
    assert!(
        registry
            .poll_shutdown(&mut Context::from_waker(&latest_waker))
            .is_ready()
    );
    assert!(
        registry
            .poll_shutdown(&mut Context::from_waker(&latest_waker))
            .is_ready()
    );
    assert_eq!(latest.wakes.load(Ordering::SeqCst), 1);
    assert!(queue.try_enqueue(batch()).is_err());
}
