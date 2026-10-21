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
    registry.unregister(DestinationId(1)).unwrap();
    assert_eq!(registry.snapshot().retired.len(), 1);
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

#[test]
fn registry_retirement_preserves_payloads_bounds_and_replacement_independence() {
    use marketforge_engine::live::routing::{
        destination::DestinationCapacity, queue::DestinationQueue, worker::WorkerStep,
    };
    use std::task::{Context, Poll, Waker};
    let registry = RouterRegistry::with_destination_limit(2).unwrap();
    let adapter = destination(1, DestinationKind::Publication, DeliveryPolicy::Required);
    let queue =
        DestinationQueue::new(DestinationCapacity::new(adapter.descriptor().capacity, 1).unwrap());
    registry
        .register_queue(adapter.clone(), queue.clone())
        .unwrap();
    let original = batch();
    queue.try_enqueue(original.clone()).unwrap();
    registry.unregister(DestinationId(1)).unwrap();
    assert_eq!(registry.destination_count(), 0);
    assert!(registry.plan(&original).is_err());
    assert!(queue.snapshot().closed);
    assert_eq!(registry.reap_retired(), 0);
    assert_eq!(
        registry.snapshot().retired[0]
            .queue
            .unwrap()
            .queue
            .queued_events,
        2
    );

    // An active replacement shares neither queue nor retirement responsibility.
    let replacement =
        DestinationQueue::new(DestinationCapacity::new(adapter.descriptor().capacity, 1).unwrap());
    registry
        .register_queue(adapter, replacement.clone())
        .unwrap();
    assert!(!replacement.snapshot().closed);
    assert_eq!(
        registry.register(destination(
            2,
            DestinationKind::Publication,
            DeliveryPolicy::Required
        )),
        Err(RouterRegistryError::DestinationCapacity)
    );
    let mut worker = registry.claim_retired_worker(DestinationId(1)).unwrap();
    assert!(registry.claim_retired_worker(DestinationId(1)).is_err());
    let mut cx = Context::from_waker(Waker::noop());
    assert_eq!(
        worker.poll_admission(&mut cx),
        Poll::Ready(WorkerStep::OutcomeReady)
    );
    let outcome = worker.take_outcome().unwrap();
    assert_eq!(outcome.payload.batch(), &original);
    assert!(Arc::ptr_eq(
        &outcome.payload.batch().events()[0],
        &original.events()[0]
    ));
    assert_eq!(registry.reap_retired(), 0); // transferred payload remains charged
    assert!(registry.poll_shutdown(&mut cx).is_pending());
    drop(outcome);
    assert!(registry.poll_shutdown(&mut cx).is_ready());
    assert_eq!(registry.reap_retired(), 1);
    assert_eq!(registry.reap_retired(), 0);
    assert!(registry.snapshot().retired.is_empty());
    assert_eq!(registry.snapshot().destinations.len(), 1);
}

#[test]
fn registry_retired_and_replacement_workers_share_bounded_scheduler() {
    use marketforge_engine::live::routing::{
        destination::DestinationCapacity,
        queue::DestinationQueue,
        worker::{DestinationWorkers, WorkerRegistrationError},
    };
    use std::{
        num::NonZeroUsize,
        task::{Context, Poll, Waker},
    };
    let registry = RouterRegistry::with_destination_limit(2).unwrap();
    let adapter = destination(1, DestinationKind::Publication, DeliveryPolicy::Required);
    let old =
        DestinationQueue::new(DestinationCapacity::new(adapter.descriptor().capacity, 1).unwrap());
    registry
        .register_queue(adapter.clone(), old.clone())
        .unwrap();
    old.try_enqueue(batch()).unwrap();
    registry.unregister(DestinationId(1)).unwrap();
    let replacement =
        DestinationQueue::new(DestinationCapacity::new(adapter.descriptor().capacity, 1).unwrap());
    registry
        .register_queue(adapter, replacement.clone())
        .unwrap();
    replacement.try_enqueue(batch()).unwrap();
    let mut workers = DestinationWorkers::new(NonZeroUsize::new(1).unwrap());
    assert!(
        workers
            .insert(registry.claim_worker(DestinationId(1)).unwrap())
            .is_ok()
    );
    let retired = registry.claim_retired_worker(DestinationId(1)).unwrap();
    let (reason, retired) = workers.insert(retired).err().unwrap();
    assert_eq!(reason, WorkerRegistrationError::Full);
    // Failed registration returns the exclusive retired owner intact.
    let active = workers;
    let mut workers = DestinationWorkers::new(NonZeroUsize::new(2).unwrap());
    assert!(workers.insert(retired).is_ok());
    drop(active);
    assert!(
        workers
            .insert(registry.claim_worker(DestinationId(1)).unwrap())
            .is_ok()
    );
    let mut cx = Context::from_waker(Waker::noop());
    let Poll::Ready(first) = workers.poll_outcome(&mut cx) else {
        panic!("retired queue stalled")
    };
    let Poll::Ready(second) = workers.poll_outcome(&mut cx) else {
        panic!("replacement queue stalled")
    };
    assert_eq!(first.descriptor.id, second.descriptor.id);
    assert_eq!(registry.reap_retired(), 0);
    assert!(workers.remove_drained(DestinationId(1)).is_none());
    drop(first);
    assert!(workers.remove_drained(DestinationId(1)).is_some());
    assert_eq!(registry.reap_retired(), 1);
    assert_eq!(workers.snapshot().workers.len(), 1);
    assert!(!workers.snapshot().workers[0].1.closed);
    workers.close();
    assert!(!workers.snapshot().is_drained());
    drop(second);
    assert!(workers.snapshot().is_drained());
}

#[test]
fn delivery_driver_reconciles_retired_workers_and_fences_shutdown() {
    use marketforge_engine::live::routing::{
        destination::DestinationCapacity,
        queue::DestinationQueue,
        worker::{DestinationWorkers, WorkerRegistrationError},
    };
    use std::num::NonZeroUsize;
    let registry = RouterRegistry::with_destination_limit(2).unwrap();
    let adapter = destination(1, DestinationKind::Publication, DeliveryPolicy::Required);
    let old =
        DestinationQueue::new(DestinationCapacity::new(adapter.descriptor().capacity, 1).unwrap());
    registry
        .register_queue(adapter.clone(), old.clone())
        .unwrap();
    old.try_enqueue(batch()).unwrap();
    registry.unregister(DestinationId(1)).unwrap();
    let replacement =
        DestinationQueue::new(DestinationCapacity::new(adapter.descriptor().capacity, 1).unwrap());
    registry
        .register_queue(adapter, replacement.clone())
        .unwrap();
    replacement.try_enqueue(batch()).unwrap();
    use marketforge_engine::live::routing::{
        acknowledgement::{AcknowledgementTracker, FinalizationJournal},
        driver::DeliveryDriver,
        supervisor::DeliverySupervisor,
        tracking_recovery::TrackingRecovery,
    };
    let limit = NonZeroUsize::new(1).unwrap();
    let mut driver = DeliveryDriver::new(
        DeliverySupervisor::new(DestinationWorkers::new(limit), limit),
        AcknowledgementTracker::new(2).unwrap(),
        TrackingRecovery::new(2).unwrap(),
        FinalizationJournal::new(2).unwrap(),
    );
    // Dequeue externally to isolate lifecycle accounting from correlation.
    let first = old.try_dequeue().unwrap();
    let second = replacement.try_dequeue().unwrap();
    let retired = registry.claim_retired_worker(DestinationId(1)).unwrap();
    let active = registry.claim_worker(DestinationId(1)).unwrap();
    assert!(driver.insert_worker(retired).is_ok());
    let (reason, active) = driver.insert_worker(active).err().unwrap();
    assert_eq!(reason, WorkerRegistrationError::Full);
    assert_eq!(driver.reap_drained_workers(), 0);
    assert_eq!(registry.reap_retired(), 0);
    drop(first);
    assert_eq!(driver.reap_drained_workers(), 1);
    assert_eq!(driver.reap_drained_workers(), 0);
    assert_eq!(registry.reap_retired(), 1);
    assert!(driver.insert_worker(active).is_ok());
    assert_eq!(driver.snapshot().workers.workers.len(), 1);
    assert!(!driver.snapshot().workers.workers[0].1.closed);
    driver.close();
    assert_eq!(driver.reap_drained_workers(), 0);
    drop(second);
    assert_eq!(driver.reap_drained_workers(), 1);
    assert!(driver.snapshot().is_drained());
    let worker = registry.claim_worker(DestinationId(1)).unwrap();
    let (reason, worker) = driver.insert_worker(worker).err().unwrap();
    assert_eq!(reason, WorkerRegistrationError::Closed);
    drop(worker);
    assert!(driver.snapshot().is_drained());
}

#[test]
fn registry_retirement_wakes_waiters_outside_locks_and_retains_reservations() {
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
            assert_eq!(self.registry.destination_count(), 0);
            assert_eq!(self.registry.snapshot().retired.len(), 1);
            assert_eq!(self.registry.reap_retired(), 0);
            self.wakes.fetch_add(1, Ordering::SeqCst);
        }
    }
    let registry = Arc::new(RouterRegistry::with_destination_limit(1).unwrap());
    let adapter = destination(1, DestinationKind::Publication, DeliveryPolicy::Required);
    let queue =
        DestinationQueue::new(DestinationCapacity::new(adapter.descriptor().capacity, 1).unwrap());
    registry
        .register_queue(adapter.clone(), queue.clone())
        .unwrap();
    let reservation = queue.capacity().try_reserve(100, 100_000).unwrap();
    let observer = Arc::new(Observer {
        registry: registry.clone(),
        wakes: AtomicUsize::new(0),
    });
    let waker = Waker::from(observer.clone());
    let mut cx = Context::from_waker(&waker);
    let mut waiting = Box::pin(queue.capacity().reserve(1, 1));
    assert!(waiting.as_mut().poll(&mut cx).is_pending());
    registry.unregister(DestinationId(1)).unwrap();
    assert_eq!(observer.wakes.load(Ordering::SeqCst), 1);
    assert!(matches!(
        waiting.as_mut().poll(&mut cx),
        Poll::Ready(Err(CapacityError::ShuttingDown))
    ));
    assert_eq!(registry.reap_retired(), 0);
    assert_eq!(
        registry.register(adapter.clone()),
        Err(RouterRegistryError::DestinationCapacity)
    );
    assert!(matches!(
        queue.capacity().try_reserve(1, 1),
        Err(CapacityError::ShuttingDown)
    ));
    drop(reservation);
    assert_eq!(registry.reap_retired(), 1);
    registry.register(adapter).unwrap();
    assert_eq!(registry.destination_count(), 1);
}

#[test]
fn registry_retirement_bounds_repeated_same_id_instances_and_releases_each_charge() {
    use marketforge_engine::live::routing::{
        destination::DestinationCapacity, queue::DestinationQueue, worker::WorkerStep,
    };
    use std::task::{Context, Poll, Waker};
    let registry = RouterRegistry::with_destination_limit(2).unwrap();
    let adapter = destination(1, DestinationKind::Publication, DeliveryPolicy::Required);
    let mut originals = Vec::new();
    for offset in [10, 20] {
        let queue = DestinationQueue::new(
            DestinationCapacity::new(adapter.descriptor().capacity, 1).unwrap(),
        );
        registry
            .register_queue(adapter.clone(), queue.clone())
            .unwrap();
        let original = EventBatch::new(vec![l2_event("router-source", 1, offset)]).unwrap();
        queue.try_enqueue(original.clone()).unwrap();
        registry.unregister(DestinationId(1)).unwrap();
        originals.push(original);
    }
    assert_eq!(registry.snapshot().retired.len(), 2);
    assert_eq!(
        registry.register(adapter.clone()),
        Err(RouterRegistryError::DestinationCapacity)
    );
    let mut first_worker = registry.claim_retired_worker(DestinationId(1)).unwrap();
    let mut second_worker = registry.claim_retired_worker(DestinationId(1)).unwrap();
    assert!(registry.claim_retired_worker(DestinationId(1)).is_err());
    let mut cx = Context::from_waker(Waker::noop());
    assert_eq!(
        first_worker.poll_admission(&mut cx),
        Poll::Ready(WorkerStep::OutcomeReady)
    );
    assert_eq!(
        second_worker.poll_admission(&mut cx),
        Poll::Ready(WorkerStep::OutcomeReady)
    );
    let first = first_worker.take_outcome().unwrap();
    let second = second_worker.take_outcome().unwrap();
    assert_eq!(first.payload.batch(), &originals[0]);
    assert_eq!(second.payload.batch(), &originals[1]);
    drop(first);
    assert_eq!(registry.reap_retired(), 1);
    assert_eq!(registry.snapshot().retired.len(), 1);
    registry.register(adapter).unwrap(); // only the released slot may be reused
    assert_eq!(registry.reap_retired(), 0);
    registry.shutdown();
    assert!(registry.poll_shutdown(&mut cx).is_pending());
    drop(second);
    assert!(registry.poll_shutdown(&mut cx).is_ready());
    assert_eq!(registry.reap_retired(), 1);
}

#[test]
fn delivery_driver_sync_registry_preserves_capacity_blocked_queues_and_reaps_retirement() {
    use marketforge_engine::live::routing::{
        acknowledgement::{AcknowledgementTracker, FinalizationJournal},
        destination::DestinationCapacity,
        driver::{DeliveryDriver, RegistryWorkerSyncError},
        queue::DestinationQueue,
        supervisor::DeliverySupervisor,
        tracking_recovery::TrackingRecovery,
        worker::{DestinationWorkers, WorkerRegistrationError},
    };
    use std::num::NonZeroUsize;
    let registry = RouterRegistry::with_destination_limit(3).unwrap();
    let first_adapter = destination(1, DestinationKind::Publication, DeliveryPolicy::Required);
    let first = DestinationQueue::new(
        DestinationCapacity::new(first_adapter.descriptor().capacity, 1).unwrap(),
    );
    registry
        .register_queue(first_adapter.clone(), first.clone())
        .unwrap();
    first.try_enqueue(batch()).unwrap();
    // Hold the real charge externally so maintenance cannot reap this instance.
    let held = first.try_dequeue().unwrap();
    registry.unregister(DestinationId(1)).unwrap();
    let replacement = DestinationQueue::new(
        DestinationCapacity::new(first_adapter.descriptor().capacity, 1).unwrap(),
    );
    registry
        .register_queue(first_adapter, replacement.clone())
        .unwrap();
    replacement.try_enqueue(batch()).unwrap();
    let limit = NonZeroUsize::new(1).unwrap();
    let mut driver = DeliveryDriver::new(
        DeliverySupervisor::new(DestinationWorkers::new(limit), limit),
        AcknowledgementTracker::new(2).unwrap(),
        TrackingRecovery::new(2).unwrap(),
        FinalizationJournal::new(2).unwrap(),
    );
    let turn = driver.sync_registry(&registry);
    assert_eq!(turn.attachment, Ok(true));
    assert_eq!(turn.retired_reaped, 0);
    assert!(driver.snapshot().workers.workers[0].1.closed);
    let turn = driver.sync_registry(&registry);
    assert_eq!(turn.workers_reaped, 0);
    assert_eq!(
        turn.attachment,
        Err(RegistryWorkerSyncError::Scheduler(
            WorkerRegistrationError::Full
        ))
    );
    // Rejected fresh lease was released, with the original queued payload intact.
    let retry = registry.claim_worker(DestinationId(1)).unwrap();
    assert_eq!(retry.snapshot().queued_batches, 1);
    drop(retry);
    drop(held);
    let turn = driver.sync_registry(&registry);
    assert_eq!(turn.workers_reaped, 1);
    assert_eq!(turn.retired_reaped, 1);
    assert_eq!(turn.attachment, Ok(true));
    assert!(!driver.snapshot().workers.workers[0].1.closed);
    assert_eq!(driver.snapshot().workers.workers[0].1.queued_batches, 1);
    assert_eq!(driver.sync_registry(&registry).attachment, Ok(false));
    driver.close();
    let turn = driver.sync_registry(&registry);
    assert_eq!(turn.attachment, Err(RegistryWorkerSyncError::Closed));
    assert_eq!(turn.workers_reaped, 0);
    assert_eq!(replacement.observe().queue.queued_batches, 1);
}

#[test]
fn registry_claim_next_worker_orders_active_ids_and_skips_existing_leases() {
    use marketforge_engine::live::routing::{
        destination::DestinationCapacity, queue::DestinationQueue,
    };
    let registry = RouterRegistry::with_destination_limit(3).unwrap();
    // Direct adapters have no queue and must not block queue reconciliation.
    registry
        .register(destination(
            0,
            DestinationKind::Publication,
            DeliveryPolicy::Required,
        ))
        .unwrap();
    for id in [2, 1] {
        let adapter = destination(id, DestinationKind::Publication, DeliveryPolicy::Required);
        let queue = DestinationQueue::new(
            DestinationCapacity::new(adapter.descriptor().capacity, 1).unwrap(),
        );
        registry.register_queue(adapter, queue).unwrap();
    }
    let first = registry.claim_next_worker().unwrap().unwrap();
    assert!(registry.claim_worker(DestinationId(1)).is_err());
    let second = registry.claim_next_worker().unwrap().unwrap();
    assert!(registry.claim_worker(DestinationId(2)).is_err());
    assert!(registry.claim_next_worker().unwrap().is_none());
    drop(first);
    assert!(registry.claim_worker(DestinationId(1)).is_ok());
    drop(second);
    registry.shutdown();
    // Closed membership can still be claimed by the shutdown coordinator.
    assert!(registry.claim_next_worker().unwrap().is_some());
}

#[test]
fn registry_membership_notifications_replace_waiter_and_wake_outside_fences() {
    use marketforge_engine::live::routing::{
        destination::DestinationCapacity, queue::DestinationQueue,
    };
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        task::{Context, Poll, Wake, Waker},
    };
    struct Observer {
        registry: Arc<RouterRegistry>,
        wakes: AtomicUsize,
    }
    impl Wake for Observer {
        fn wake(self: Arc<Self>) {
            // Both read and exclusive commitment paths may re-enter on wake.
            let snapshot = self.registry.snapshot();
            let _ = self.registry.plan(&batch());
            if let Some(entry) = snapshot.destinations.first() {
                let result = self.registry.register(destination(
                    entry.descriptor.id.0,
                    entry.descriptor.kind,
                    entry.descriptor.policy,
                ));
                assert!(matches!(
                    result,
                    Err(RouterRegistryError::DuplicateDestination
                        | RouterRegistryError::ShuttingDown)
                ));
            }
            assert_eq!(snapshot.revision, self.registry.revision());
            self.wakes.fetch_add(1, Ordering::SeqCst);
        }
    }
    let registry = Arc::new(RouterRegistry::with_destination_limit(2).unwrap());
    let old = Arc::new(Observer {
        registry: registry.clone(),
        wakes: AtomicUsize::new(0),
    });
    let current = Arc::new(Observer {
        registry: registry.clone(),
        wakes: AtomicUsize::new(0),
    });
    let old_waker = Waker::from(old.clone());
    let current_waker = Waker::from(current.clone());
    let mut old_cx = Context::from_waker(&old_waker);
    let mut cx = Context::from_waker(&current_waker);
    assert!(registry.poll_membership_change(0, &mut old_cx).is_pending());
    assert!(registry.poll_membership_change(0, &mut cx).is_pending());
    assert!(registry.poll_membership_change(0, &mut cx).is_pending());
    let adapter = destination(1, DestinationKind::Publication, DeliveryPolicy::Required);
    registry.register(adapter.clone()).unwrap();
    assert_eq!(old.wakes.load(Ordering::SeqCst), 0);
    assert_eq!(current.wakes.load(Ordering::SeqCst), 1);
    assert_eq!(
        registry.poll_membership_change(0, &mut cx),
        Poll::Ready(Ok(1))
    );
    assert!(registry.poll_membership_change(1, &mut cx).is_pending());
    assert_eq!(
        registry.register(adapter),
        Err(RouterRegistryError::DuplicateDestination)
    );
    assert_eq!(
        registry.unregister(DestinationId(99)),
        Err(RouterRegistryError::UnknownDestination)
    );
    assert_eq!(current.wakes.load(Ordering::SeqCst), 1);
    registry.unregister(DestinationId(1)).unwrap();
    assert_eq!(current.wakes.load(Ordering::SeqCst), 2);
    assert_eq!(
        registry.poll_membership_change(1, &mut cx),
        Poll::Ready(Ok(2))
    );
    assert!(registry.poll_membership_change(2, &mut cx).is_pending());
    assert_eq!(registry.reap_retired(), 1);
    assert_eq!(current.wakes.load(Ordering::SeqCst), 2);
    let adapter = destination(2, DestinationKind::Publication, DeliveryPolicy::Required);
    let queue =
        DestinationQueue::new(DestinationCapacity::new(adapter.descriptor().capacity, 1).unwrap());
    registry.register_queue(adapter, queue).unwrap();
    assert_eq!(current.wakes.load(Ordering::SeqCst), 3);
    assert_eq!(
        registry.poll_membership_change(2, &mut cx),
        Poll::Ready(Ok(3))
    );
    assert!(registry.poll_membership_change(3, &mut cx).is_pending());
    registry.shutdown();
    assert_eq!(current.wakes.load(Ordering::SeqCst), 4);
    assert_eq!(
        registry.poll_membership_change(3, &mut cx),
        Poll::Ready(Err(RouterRegistryError::ShuttingDown))
    );
    registry.shutdown();
    assert_eq!(current.wakes.load(Ordering::SeqCst), 4);
}

#[test]
fn registered_driver_bounds_reconciliation_and_avoids_capacity_shutdown_spins() {
    use marketforge_engine::live::routing::{
        acknowledgement::{AcknowledgementTracker, FinalizationJournal},
        destination::DestinationCapacity,
        driver::{DeliveryDriver, RegistryWorkerSyncError},
        queue::DestinationQueue,
        supervisor::DeliverySupervisor,
        tracking_recovery::TrackingRecovery,
        worker::{DestinationWorkers, WorkerRegistrationError},
    };
    use std::{
        num::NonZeroUsize,
        sync::atomic::{AtomicUsize, Ordering},
        task::{Context, Poll, Wake, Waker},
    };
    #[derive(Default)]
    struct Counter(AtomicUsize);
    impl Wake for Counter {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let registry = RouterRegistry::new();
    for id in [1, 2] {
        let adapter = destination(id, DestinationKind::Publication, DeliveryPolicy::Required);
        let queue = DestinationQueue::new(
            DestinationCapacity::new(adapter.descriptor().capacity, 1).unwrap(),
        );
        registry.register_queue(adapter, queue).unwrap();
    }
    let limit = NonZeroUsize::new(1).unwrap();
    let mut driver = DeliveryDriver::new(
        DeliverySupervisor::new(DestinationWorkers::new(limit), limit),
        AcknowledgementTracker::new(2).unwrap(),
        TrackingRecovery::new(2).unwrap(),
        FinalizationJournal::new(2).unwrap(),
    );
    let wakes = Arc::new(Counter::default());
    let waker = Waker::from(wakes.clone());
    let mut cx = Context::from_waker(&waker);
    let turn = driver.drive_registered(&registry, &mut cx);
    assert_eq!(turn.registry.attachment, Ok(true));
    assert!(turn.delivery.delivery.is_pending());
    assert!(turn.membership.is_pending());
    assert_eq!(driver.snapshot().workers.workers.len(), 1);
    assert_eq!(wakes.0.load(Ordering::SeqCst), 1);
    for _ in 0..2 {
        let turn = driver.drive_registered(&registry, &mut cx);
        assert_eq!(
            turn.registry.attachment,
            Err(RegistryWorkerSyncError::Scheduler(
                WorkerRegistrationError::Full
            ))
        );
        assert!(turn.delivery.delivery.is_pending());
    }
    assert_eq!(wakes.0.load(Ordering::SeqCst), 1);
    // Rejected attachment released the fresh lease.
    drop(registry.claim_worker(DestinationId(2)).unwrap());
    registry.unregister(DestinationId(1)).unwrap();
    assert!(wakes.0.load(Ordering::SeqCst) > 1);
    let turn = driver.drive_registered(&registry, &mut cx);
    assert_eq!(turn.registry.workers_reaped, 1);
    assert_eq!(turn.registry.retired_reaped, 1);
    assert_eq!(turn.registry.attachment, Ok(true));
    assert_eq!(driver.snapshot().workers.workers[0].0, DestinationId(2));
    registry.shutdown();
    driver.close();
    let before = wakes.0.load(Ordering::SeqCst);
    for _ in 0..2 {
        let turn = driver.drive_registered(&registry, &mut cx);
        assert_eq!(
            turn.registry.attachment,
            Err(RegistryWorkerSyncError::Closed)
        );
        assert_eq!(
            turn.membership,
            Poll::Ready(Err(RouterRegistryError::ShuttingDown))
        );
    }
    assert_eq!(wakes.0.load(Ordering::SeqCst), before);
    assert!(driver.snapshot().is_drained());
}

#[test]
fn registered_driver_catches_adapter_membership_race_and_retains_uncorrelated_payload() {
    use marketforge_engine::live::routing::{
        acknowledgement::{AcknowledgementTracker, FinalizationJournal},
        destination::DestinationCapacity,
        driver::DeliveryDriver,
        queue::DestinationQueue,
        supervisor::{DeliverySupervisor, SupervisionStep},
        tracking_recovery::TrackingRecovery,
        worker::DestinationWorkers,
    };
    use std::{
        num::NonZeroUsize,
        sync::atomic::{AtomicUsize, Ordering},
        task::{Context, Poll, Wake, Waker},
    };
    #[derive(Default)]
    struct Counter(AtomicUsize);
    impl Wake for Counter {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    struct ChangingAdapter {
        registry: Arc<RouterRegistry>,
        descriptor: DestinationDescriptor,
        calls: Arc<AtomicUsize>,
    }
    impl EventDestinationPort for ChangingAdapter {
        fn descriptor(&self) -> DestinationDescriptor {
            self.descriptor
        }
        fn admit<'a>(&'a self, batch: EventBatch) -> RoutingFuture<'a, DestinationAdmission> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                self.registry
                    .register(destination(
                        2,
                        DestinationKind::Publication,
                        DeliveryPolicy::BestEffort,
                    ))
                    .unwrap();
                DestinationAdmission::Accepted {
                    destination: self.descriptor.id,
                    through: batch.last_cursor().clone(),
                }
            })
        }
    }
    let registry = Arc::new(RouterRegistry::new());
    let calls = Arc::new(AtomicUsize::new(0));
    let adapter = Arc::new(ChangingAdapter {
        registry: registry.clone(),
        calls: calls.clone(),
        descriptor: destination(1, DestinationKind::Publication, DeliveryPolicy::Required)
            .descriptor(),
    });
    let queue =
        DestinationQueue::new(DestinationCapacity::new(adapter.descriptor.capacity, 1).unwrap());
    registry.register_queue(adapter, queue.clone()).unwrap();
    // Fault injection: queue an event without the mandatory tracking obligation.
    queue.try_enqueue(batch()).unwrap();
    let limit = NonZeroUsize::new(2).unwrap();
    let mut driver = DeliveryDriver::new(
        DeliverySupervisor::new(DestinationWorkers::new(limit), limit),
        AcknowledgementTracker::new(2).unwrap(),
        TrackingRecovery::new(2).unwrap(),
        FinalizationJournal::new(2).unwrap(),
    );
    let wakes = Arc::new(Counter::default());
    let waker = Waker::from(wakes.clone());
    let mut cx = Context::from_waker(&waker);
    let turn = driver.drive_registered(&registry, &mut cx);
    assert_eq!(turn.membership, Poll::Ready(Ok(registry.revision())));
    assert!(matches!(
        turn.delivery.delivery,
        Poll::Ready(SupervisionStep::OutcomeBlocked(_))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(!driver.snapshot().workers.workers[0].1.is_drained());
    let before = wakes.0.load(Ordering::SeqCst);
    let turn = driver.drive_registered(&registry, &mut cx);
    assert!(turn.membership.is_pending());
    assert!(matches!(
        turn.delivery.delivery,
        Poll::Ready(SupervisionStep::OutcomeBlocked(_))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(wakes.0.load(Ordering::SeqCst), before);
    registry.unregister(DestinationId(2)).unwrap();
    assert_eq!(wakes.0.load(Ordering::SeqCst), before + 1);
    let (_, retained) = driver.take_blocked_for_recovery().unwrap();
    assert_eq!(
        retained.payload.batch().first_cursor(),
        batch().first_cursor()
    );
    drop(retained);
    assert!(driver.snapshot().workers.workers[0].1.is_drained());
}

#[test]
fn registered_driver_advances_tracked_delivery_without_inventing_completion() {
    use marketforge_engine::live::{
        foundation::{cancellation::CancellationToken, clock::ManualClock},
        model::delivery::{DeliveryStage, DeliveryStatus},
        ports::routing::DestinationCompletion,
        routing::{
            acknowledgement::{AcknowledgementTracker, FinalizationJournal},
            admission::AdmissionContext,
            cursor::StreamAdmissionOrder,
            destination::DestinationCapacity,
            driver::DeliveryDriver,
            ordered::{OrderedCommitOutcome, OrderedFanout},
            queue::DestinationQueue,
            supervisor::{DeliverySupervisor, SupervisionStep},
            tracking_recovery::TrackingRecovery,
            worker::DestinationWorkers,
        },
    };
    use std::{
        future::Future,
        num::NonZeroUsize,
        sync::atomic::{AtomicUsize, Ordering},
        task::{Context, Poll, Wake, Waker},
        time::Instant,
    };
    #[derive(Default)]
    struct Counter(AtomicUsize);
    impl Wake for Counter {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let registry = RouterRegistry::new();
    let adapter = destination(1, DestinationKind::Publication, DeliveryPolicy::Required);
    let queue =
        DestinationQueue::new(DestinationCapacity::new(adapter.descriptor().capacity, 1).unwrap());
    registry.register_queue(adapter, queue).unwrap();
    let limit = NonZeroUsize::new(1).unwrap();
    let mut driver = DeliveryDriver::new(
        DeliverySupervisor::new(DestinationWorkers::new(limit), limit),
        AcknowledgementTracker::new(2).unwrap(),
        TrackingRecovery::new(2).unwrap(),
        FinalizationJournal::new(2).unwrap(),
    );
    let wakes = Arc::new(Counter::default());
    let waker = Waker::from(wakes.clone());
    let mut cx = Context::from_waker(&waker);
    let order = StreamAdmissionOrder::new(batch().first_cursor().clone()).unwrap();
    let mut preparation = Box::pin(OrderedFanout::prepare(
        &registry,
        &order,
        batch(),
        AdmissionContext::without_deadline(CancellationToken::new()),
        Arc::new(ManualClock::new(Instant::now(), 0)),
        None,
    ));
    let Poll::Ready(Ok(prepared)) = preparation.as_mut().poll(&mut cx) else {
        panic!("capacity available")
    };
    drop(preparation);
    let (tracker, recovery) = driver.admission_owners().unwrap();
    assert!(matches!(
        prepared.commit(tracker, recovery),
        OrderedCommitOutcome::Tracked(_)
    ));
    let turn = driver.drive_registered(&registry, &mut cx);
    assert!(matches!(
        turn.delivery.delivery,
        Poll::Ready(SupervisionStep::Retained(_))
    ));
    assert_eq!(driver.snapshot().retained_deliveries, 1);
    let before = wakes.0.load(Ordering::SeqCst);
    let turn = driver.drive_registered(&registry, &mut cx);
    assert!(matches!(
        turn.delivery.delivery,
        Poll::Ready(SupervisionStep::CapacityBlocked)
    ));
    assert_eq!(wakes.0.load(Ordering::SeqCst), before);
    assert_eq!(driver.snapshot().tracked_obligations, 1);
    driver
        .apply_completion(&DestinationCompletion {
            destination: DestinationId(1),
            through: batch().last_cursor().clone(),
            status: DeliveryStatus::Progress(DeliveryStage::Delivered),
        })
        .unwrap();
    let turn = driver.drive_registered(&registry, &mut cx);
    assert!(matches!(
        turn.delivery.delivery,
        Poll::Ready(SupervisionStep::Finalized(_))
    ));
    assert_eq!(wakes.0.load(Ordering::SeqCst), before + 1);
    // The returned terminal record still owns payload capacity until released.
    assert!(!driver.snapshot().workers.workers[0].1.is_drained());
    drop(turn);
    assert!(driver.snapshot().workers.workers[0].1.is_drained());
    assert_eq!(driver.snapshot().tracked_obligations, 0);
    assert_eq!(driver.snapshot().journal_records, 1);
}
