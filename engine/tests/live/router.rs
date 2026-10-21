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
    // Closed and fully drained queues need no replacement worker.
    assert!(registry.claim_next_worker().unwrap().is_none());
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
    // Park the coordinator before admission; commitment must schedule a turn
    // even if subsequent work is tracking recovery rather than queue delivery.
    let _ = driver.drive(&mut cx);
    let admission_wakes = wakes.0.load(Ordering::SeqCst);
    let order = StreamAdmissionOrder::new(batch().first_cursor().clone()).unwrap();
    let cancelled = CancellationToken::new();
    let mut rejected_preparation = Box::pin(OrderedFanout::prepare(
        &registry,
        &order,
        batch(),
        AdmissionContext::without_deadline(cancelled.clone()),
        Arc::new(ManualClock::new(Instant::now(), 0)),
        None,
    ));
    let Poll::Ready(Ok(rejected_prepared)) = rejected_preparation.as_mut().poll(&mut cx) else {
        panic!("capacity available before cancellation")
    };
    drop(rejected_preparation);
    cancelled.cancel();
    let Ok((rejected, optional)) = driver.commit_ordered(rejected_prepared) else {
        panic!("owner remains open")
    };
    assert!(matches!(rejected, OrderedCommitOutcome::Rejected { .. }));
    assert!(optional.is_none());
    assert_eq!(wakes.0.load(Ordering::SeqCst), admission_wakes);
    assert_eq!(driver.snapshot().tracked_obligations, 0);
    // A rejected commitment preserves both the frontier and coordinator waiter.
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
    let Ok((required, optional)) = driver.commit_ordered(prepared) else {
        panic!("open delivery owner accepts ordered commitment")
    };
    assert_eq!(wakes.0.load(Ordering::SeqCst), admission_wakes + 1);
    assert!(matches!(required, OrderedCommitOutcome::Tracked(_)));
    assert!(optional.unwrap().unwrap().queued.is_empty());
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
    // Rejected correlation preserves the armed completion notification.
    assert!(
        driver
            .apply_completion(&DestinationCompletion {
                destination: DestinationId(999),
                through: batch().last_cursor().clone(),
                status: DeliveryStatus::Progress(DeliveryStage::Delivered),
            })
            .is_err()
    );
    assert_eq!(wakes.0.load(Ordering::SeqCst), before);
    // A later polling coordinator replaces the earlier one.
    let replacement = Arc::new(Counter::default());
    let replacement_waker = Waker::from(replacement.clone());
    let mut replacement_cx = Context::from_waker(&replacement_waker);
    drop(driver.drive_registered(&registry, &mut replacement_cx));
    driver.close();
    driver
        .apply_completion(&DestinationCompletion {
            destination: DestinationId(1),
            through: batch().last_cursor().clone(),
            status: DeliveryStatus::Progress(DeliveryStage::Delivered),
        })
        .unwrap();
    assert_eq!(replacement.0.load(Ordering::SeqCst), 1);
    assert_eq!(wakes.0.load(Ordering::SeqCst), before);
    let turn = driver.drive_registered(&registry, &mut replacement_cx);
    assert!(matches!(
        turn.delivery.delivery,
        Poll::Ready(SupervisionStep::Finalized(_))
    ));
    assert_eq!(replacement.0.load(Ordering::SeqCst), 2);
    assert_eq!(wakes.0.load(Ordering::SeqCst), before);
    // The returned terminal record still owns payload capacity until released.
    assert!(!driver.snapshot().workers.workers[0].1.is_drained());
    drop(turn);
    assert!(driver.snapshot().workers.workers[0].1.is_drained());
    assert_eq!(driver.snapshot().tracked_obligations, 0);
    assert_eq!(driver.snapshot().journal_records, 1);
    // Reporting through the authoritative journal wakes the latest coordinator,
    // even after close; rejected acknowledgements preserve the notification.
    let clock = ManualClock::new(Instant::now(), 0);
    let claim = driver
        .journal_mut()
        .claim_next(&clock, std::time::Duration::from_secs(1))
        .unwrap()
        .unwrap();
    let before = replacement.0.load(Ordering::SeqCst);
    assert!(
        driver
            .journal_mut()
            .acknowledge(
                marketforge_engine::live::routing::acknowledgement::DeliveryObligationId(u64::MAX),
                claim.token,
                &clock
            )
            .is_err()
    );
    assert_eq!(replacement.0.load(Ordering::SeqCst), before);
    driver
        .journal_mut()
        .acknowledge(claim.record.obligation.id, claim.token, &clock)
        .unwrap();
    assert_eq!(replacement.0.load(Ordering::SeqCst), before + 1);
    assert_eq!(driver.snapshot().journal_records, 0);
}

#[test]
fn registered_driver_retry_timer_is_bounded_and_drains_external_payload_charge() {
    use marketforge_engine::live::{
        foundation::{clock::ManualClock, deadline::ManualDeadlineTimer},
        model::error::ErrorCode,
        routing::{
            acknowledgement::{AcknowledgementTracker, FinalizationJournal},
            admission::DeadlineTimer,
            destination::DestinationCapacity,
            driver::DeliveryDriver,
            queue::DestinationQueue,
            supervisor::DeliverySupervisor,
            tracking_recovery::TrackingRecovery,
            worker::DestinationWorkers,
        },
    };
    use std::{
        num::NonZeroUsize,
        sync::atomic::{AtomicUsize, Ordering},
        task::{Context, Wake, Waker},
        time::{Duration, Instant},
    };
    #[derive(Default)]
    struct Counter(AtomicUsize);
    impl Wake for Counter {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let now = Instant::now();
    let interval = Duration::from_secs(1);
    let clock = ManualClock::new(now, 0);
    let timer = ManualDeadlineTimer::new(now, 1).unwrap();
    let registry = RouterRegistry::new();
    let adapter = destination(1, DestinationKind::Publication, DeliveryPolicy::Required);
    let queue =
        DestinationQueue::new(DestinationCapacity::new(adapter.descriptor().capacity, 1).unwrap());
    queue.try_enqueue(batch()).unwrap();
    let held = queue.try_dequeue().unwrap();
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
    assert_eq!(
        driver
            .drive_registered_with_retry(&registry, &mut cx, &clock, &timer, Duration::ZERO)
            .1,
        Err(ErrorCode::InvalidInput)
    );
    let blocker = timer.try_sleep_until(now + interval).unwrap();
    let before = wakes.0.load(Ordering::SeqCst);
    assert_eq!(
        driver
            .drive_registered_with_retry(&registry, &mut cx, &clock, &timer, interval)
            .1,
        Err(ErrorCode::Capacity)
    );
    assert_eq!(wakes.0.load(Ordering::SeqCst), before);
    drop(blocker);
    assert!(
        driver
            .drive_registered_with_retry(&registry, &mut cx, &clock, &timer, interval)
            .1
            .is_ok()
    );
    assert!(timer.try_sleep_until(now + interval).is_err());
    clock.advance(Duration::from_millis(500)).unwrap();
    timer.advance_to(now + Duration::from_millis(500)).unwrap();
    let before = wakes.0.load(Ordering::SeqCst);
    for _ in 0..3 {
        assert!(
            driver
                .drive_registered_with_retry(&registry, &mut cx, &clock, &timer, interval)
                .1
                .is_ok()
        );
    }
    assert_eq!(wakes.0.load(Ordering::SeqCst), before);
    registry.shutdown();
    driver.close();
    assert!(!driver.snapshot().is_drained());
    clock.advance(Duration::from_millis(500)).unwrap();
    timer.advance_to(now + interval).unwrap();
    assert!(wakes.0.load(Ordering::SeqCst) > before);
    assert!(
        driver
            .drive_registered_with_retry(&registry, &mut cx, &clock, &timer, interval)
            .1
            .is_ok()
    );
    assert!(timer.try_sleep_until(now + interval * 2).is_err());
    let before = wakes.0.load(Ordering::SeqCst);
    drop(held);
    clock.advance(interval).unwrap();
    timer.advance_to(now + interval * 2).unwrap();
    assert!(wakes.0.load(Ordering::SeqCst) > before);
    let (turn, scheduled) =
        driver.drive_registered_with_retry(&registry, &mut cx, &clock, &timer, interval);
    assert_eq!(scheduled, Ok(()));
    assert_eq!(turn.registry.workers_reaped, 1);
    assert!(driver.snapshot().is_drained());
    assert!(timer.try_sleep_until(now + interval * 3).is_ok());
}

#[test]
fn registered_driver_retry_uses_clock_and_rejects_early_timers_without_busy_wakes() {
    use marketforge_engine::live::{
        foundation::{
            clock::{ManualClock, MonotonicClock},
            deadline::ManualDeadlineTimer,
        },
        model::error::ErrorCode,
        routing::{
            acknowledgement::{AcknowledgementTracker, FinalizationJournal},
            admission::{DeadlineFuture, DeadlineTimer},
            driver::DeliveryDriver,
            supervisor::DeliverySupervisor,
            tracking_recovery::TrackingRecovery,
            worker::DestinationWorkers,
        },
    };
    use std::{
        num::NonZeroUsize,
        sync::atomic::{AtomicUsize, Ordering},
        task::{Context, Wake, Waker},
        time::{Duration, Instant},
    };
    struct Counter(AtomicUsize);
    impl Wake for Counter {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    struct Early;
    impl DeadlineTimer for Early {
        fn sleep_until(&self, _: Instant) -> DeadlineFuture {
            Box::pin(async {})
        }
    }
    struct Crossing(Arc<ManualClock>);
    impl DeadlineTimer for Crossing {
        fn sleep_until(&self, _: Instant) -> DeadlineFuture {
            let clock = self.0.clone();
            Box::pin(async move {
                clock.advance(Duration::from_secs(1)).unwrap();
            })
        }
    }
    let now = Instant::now();
    let interval = Duration::from_secs(1);
    let clock = Arc::new(ManualClock::new(now, 0));
    let timer = ManualDeadlineTimer::new(now, 1).unwrap();
    let registry = RouterRegistry::new();
    let limit = NonZeroUsize::new(1).unwrap();
    let mut driver = DeliveryDriver::new(
        DeliverySupervisor::new(DestinationWorkers::new(limit), limit),
        AcknowledgementTracker::new(2).unwrap(),
        TrackingRecovery::new(2).unwrap(),
        FinalizationJournal::new(2).unwrap(),
    );
    let wakes = Arc::new(Counter(AtomicUsize::new(0)));
    let waker = Waker::from(wakes.clone());
    let mut cx = Context::from_waker(&waker);
    for _ in 0..3 {
        assert_eq!(
            driver
                .drive_registered_with_retry(&registry, &mut cx, clock.as_ref(), &Early, interval)
                .1,
            Err(ErrorCode::InvalidInput)
        );
    }
    assert_eq!(wakes.0.load(Ordering::SeqCst), 0);
    assert_eq!(
        driver
            .drive_registered_with_retry(&registry, &mut cx, clock.as_ref(), &timer, interval)
            .1,
        Ok(())
    );
    // An already retained timer may also report early. Its charge is released.
    timer.advance_to(now + interval).unwrap();
    let before = wakes.0.load(Ordering::SeqCst);
    assert_eq!(
        driver
            .drive_registered_with_retry(&registry, &mut cx, clock.as_ref(), &timer, interval)
            .1,
        Err(ErrorCode::InvalidInput)
    );
    assert_eq!(wakes.0.load(Ordering::SeqCst), before);
    // The authoritative clock reaching the boundary retires a pending future,
    // even when the timer adapter's timeline has stopped advancing.
    clock.advance(interval).unwrap();
    assert_eq!(
        driver
            .drive_registered_with_retry(&registry, &mut cx, clock.as_ref(), &timer, interval)
            .1,
        Ok(())
    );
    clock.advance(interval).unwrap();
    assert_eq!(
        driver
            .drive_registered_with_retry(&registry, &mut cx, clock.as_ref(), &timer, interval)
            .1,
        Ok(())
    );
    assert!(timer.try_sleep_until(clock.now() + interval).is_err());
    assert_eq!(wakes.0.load(Ordering::SeqCst), before);
    // Expiry during a fresh poll schedules exactly one further bounded turn.
    clock.advance(interval).unwrap();
    assert_eq!(
        driver
            .drive_registered_with_retry(
                &registry,
                &mut cx,
                clock.as_ref(),
                &Crossing(clock.clone()),
                interval
            )
            .1,
        Ok(())
    );
    assert_eq!(wakes.0.load(Ordering::SeqCst), before + 1);
    assert!(timer.try_sleep_until(clock.now() + interval).is_ok());
}

#[test]
fn closed_driver_returns_ordered_preparation_without_commitment() {
    use marketforge_engine::live::{
        foundation::{cancellation::CancellationToken, clock::ManualClock},
        routing::{
            acknowledgement::{AcknowledgementTracker, FinalizationJournal},
            admission::AdmissionContext,
            cursor::StreamAdmissionOrder,
            destination::DestinationCapacity,
            driver::DeliveryDriver,
            ordered::OrderedFanout,
            queue::DestinationQueue,
            supervisor::DeliverySupervisor,
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
    driver.close();
    let Err((reason, prepared)) = driver.commit_ordered(prepared) else {
        panic!("closed owner must reject")
    };
    assert_eq!(
        reason,
        marketforge_engine::live::routing::driver::DeliveryAdmissionError::Closed
    );
    assert_eq!(driver.snapshot().tracked_obligations, 0);
    assert_eq!(order.next(), Some(batch().first_cursor().clone()));
    assert!(order.snapshot().pending_last.is_some());
    drop(prepared);
    assert!(order.snapshot().pending_last.is_none());
    assert_eq!(order.next(), Some(batch().first_cursor().clone()));
}

#[test]
fn router_observation_requires_closed_admission_and_has_no_runtime_effects() {
    use marketforge_engine::live::{
        foundation::resources::ResourceUsage,
        routing::{
            acknowledgement::{AcknowledgementTracker, FinalizationJournal},
            driver::DeliveryDriver,
            namespaces::AdmissionNamespaces,
            ordered::OrderedAdmissionBudget,
            shutdown::RouterDrainSnapshot,
            supervisor::DeliverySupervisor,
            tracking_recovery::TrackingRecovery,
            worker::DestinationWorkers,
        },
    };
    use std::num::NonZeroUsize;
    let registry = RouterRegistry::new();
    let namespaces = AdmissionNamespaces::new(1).unwrap();
    let limit = ResourceUsage {
        items: 2,
        bytes: 100_000,
    };
    let budget = OrderedAdmissionBudget::new(limit).unwrap();
    let one = NonZeroUsize::new(1).unwrap();
    let mut delivery = DeliveryDriver::new(
        DeliverySupervisor::new(DestinationWorkers::new(one), one),
        AcknowledgementTracker::new(1).unwrap(),
        TrackingRecovery::new(1).unwrap(),
        FinalizationJournal::new(1).unwrap(),
    );
    let before = RouterDrainSnapshot::observe(&namespaces, &budget, &registry, &delivery);
    assert_eq!(before.admission_limit, limit);
    assert_eq!(before.admission_usage, ResourceUsage::default());
    assert!(!before.admission_closed);
    assert!(!before.is_drained());
    assert_eq!(
        before,
        RouterDrainSnapshot::observe(&namespaces, &budget, &registry, &delivery)
    );
    // The production projection uses the real prebound native collector. It
    // cannot mutate any router owner, including while measurements are disabled.
    use marketforge_engine::live::{
        foundation::clock::ManualClock,
        metrics::registry::NativeMetrics,
        model::{
            error::ErrorCode,
            observation::{
                Measurement, MetricBinding, MetricValue, MissingMeasurement, ResetBoundary,
            },
            operation::{ResourceId, ResourceVersion},
        },
        ports::observability::ObservationAdmission,
        routing::metrics::ROUTER_METRICS,
    };
    use std::time::{Duration, Instant};
    let clock = Arc::new(ManualClock::new(Instant::now(), 0));
    let metrics = NativeMetrics::new(clock.clone(), 3, 18, true).unwrap();
    let binding = |generation, catalog: Vec<_>| MetricBinding {
        owner: ResourceVersion {
            resource: ResourceId::Runtime,
            generation,
        },
        reset_epoch: 0,
        reset: ResetBoundary::OwnerGeneration,
        metrics: catalog,
    };
    let handle = metrics
        .register(binding(1, ROUTER_METRICS.to_vec()))
        .unwrap();
    for (metric, result) in before.record_metrics(&handle, 123) {
        assert!(ROUTER_METRICS.contains(&metric));
        assert_eq!(result, Ok(ObservationAdmission::Accepted));
    }
    // Sampling cadence is monotonic and never builds snapshots while disabled
    // or before its deadline. Delayed scheduling emits only one current sample.
    use marketforge_engine::live::routing::metrics::RouterMetricSampler;
    let start = marketforge_engine::live::foundation::MonotonicClock::now(clock.as_ref());
    let interval = Duration::from_secs(2);
    assert!(RouterMetricSampler::new(Duration::ZERO, Some(start)).is_err());
    let mut sampler = RouterMetricSampler::new(interval, None).unwrap();
    assert!(
        sampler
            .sample_due(start, clock.as_ref(), &handle, || panic!("disabled"))
            .unwrap()
            .is_none()
    );
    sampler.schedule(Some(start + interval));
    assert!(
        sampler
            .sample_due(start, clock.as_ref(), &handle, || panic!("early"))
            .unwrap()
            .is_none()
    );
    clock.set_wall_ns(123);
    let sampled = sampler
        .sample_due(start + interval, clock.as_ref(), &handle, || before.clone())
        .unwrap()
        .unwrap();
    assert!(
        sampled
            .iter()
            .all(|(_, result)| *result == Ok(ObservationAdmission::Accepted))
    );
    let late = start + Duration::from_secs(100);
    assert!(
        sampler
            .sample_due(late, clock.as_ref(), &handle, || before.clone())
            .unwrap()
            .is_some()
    );
    assert_eq!(sampler.next_deadline(), Some(late + interval));
    assert!(
        sampler
            .sample_due(late, clock.as_ref(), &handle, || panic!("catch-up burst"))
            .unwrap()
            .is_none()
    );
    struct FailedWall;
    impl marketforge_engine::live::foundation::clock::WallClock for FailedWall {
        fn unix_time_ns(&self) -> Result<i64, ErrorCode> {
            Err(ErrorCode::Unavailable)
        }
    }
    sampler.schedule(Some(start));
    assert_eq!(
        sampler.sample_due(start, &FailedWall, &handle, || panic!("wall failure")),
        Err(ErrorCode::Unavailable)
    );
    assert_eq!(sampler.next_deadline(), Some(start + interval));
    let mut overflow = RouterMetricSampler::new(Duration::MAX, Some(start)).unwrap();
    assert_eq!(
        overflow.sample_due(start, clock.as_ref(), &handle, || panic!("overflow")),
        Err(ErrorCode::Limit)
    );
    assert_eq!(overflow.next_deadline(), None);
    sampler.schedule(None);
    assert_eq!(sampler.next_deadline(), None);
    use marketforge_engine::live::routing::admission::{DeadlineFuture, DeadlineTimer};
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        task::{Context, Poll, Wake, Waker},
    };
    struct Timer {
        calls: AtomicUsize,
        mode: u8,
    }
    impl DeadlineTimer for Timer {
        fn sleep_until(&self, _: Instant) -> DeadlineFuture {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.mode == 1 {
                Box::pin(async {})
            } else {
                Box::pin(std::future::pending())
            }
        }
        fn try_sleep_until(&self, at: Instant) -> Result<DeadlineFuture, ErrorCode> {
            if self.mode == 2 {
                Err(ErrorCode::Capacity)
            } else {
                Ok(self.sleep_until(at))
            }
        }
    }
    struct NoWake;
    impl Wake for NoWake {
        fn wake(self: Arc<Self>) {
            panic!("no busy wake")
        }
    }
    let waker = Waker::from(Arc::new(NoWake));
    let mut cx = Context::from_waker(&waker);
    let timer = Timer {
        calls: AtomicUsize::new(0),
        mode: 0,
    };
    sampler.schedule(Some(start + interval));
    for _ in 0..2 {
        assert!(
            sampler
                .poll_sample(
                    &mut cx,
                    clock.as_ref(),
                    &timer,
                    clock.as_ref(),
                    &handle,
                    || panic!("early snapshot")
                )
                .is_pending()
        );
    }
    assert_eq!(timer.calls.load(Ordering::SeqCst), 1);
    sampler.schedule(None);
    assert!(matches!(
        sampler.poll_sample(
            &mut cx,
            clock.as_ref(),
            &timer,
            clock.as_ref(),
            &handle,
            || panic!("disabled snapshot")
        ),
        Poll::Ready(Ok(None))
    ));
    sampler.schedule(Some(start + interval));
    let failed = Timer {
        calls: AtomicUsize::new(0),
        mode: 2,
    };
    assert_eq!(
        sampler.poll_sample(
            &mut cx,
            clock.as_ref(),
            &failed,
            clock.as_ref(),
            &handle,
            || panic!("timer failure")
        ),
        Poll::Ready(Err(ErrorCode::Capacity))
    );
    assert_eq!(sampler.next_deadline(), Some(start + interval));
    let early = Timer {
        calls: AtomicUsize::new(0),
        mode: 1,
    };
    assert_eq!(
        sampler.poll_sample(
            &mut cx,
            clock.as_ref(),
            &early,
            clock.as_ref(),
            &handle,
            || panic!("early timer")
        ),
        Poll::Ready(Err(ErrorCode::InvalidInput))
    );
    assert_eq!(early.calls.load(Ordering::SeqCst), 2);
    sampler.schedule(Some(start));
    assert!(matches!(
        sampler.poll_sample(
            &mut cx,
            clock.as_ref(),
            &timer,
            clock.as_ref(),
            &handle,
            || before.clone()
        ),
        Poll::Ready(Ok(Some(_)))
    ));
    assert_eq!(timer.calls.load(Ordering::SeqCst), 1);
    // A timer may become ready after the initial clock check, including
    // while polling the single replacement for an earlier wake.
    struct AdvancingTimer {
        clock: Arc<marketforge_engine::live::foundation::clock::ManualClock>,
        calls: AtomicUsize,
        advance_on: usize,
        pending: bool,
        interval: Duration,
    }
    impl DeadlineTimer for AdvancingTimer {
        fn sleep_until(&self, _: Instant) -> DeadlineFuture {
            let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            let clock = self.clock.clone();
            let interval = self.interval;
            let advance = call == self.advance_on;
            let pending = self.pending;
            Box::pin(async move {
                if advance {
                    clock.advance(interval).unwrap();
                    if pending {
                        std::future::pending::<()>().await;
                    }
                }
            })
        }
    }
    for (advance_on, pending) in [(1, false), (2, false), (1, true), (2, true)] {
        use marketforge_engine::live::foundation::clock::MonotonicClock;
        let deadline = clock.now() + interval;
        sampler.schedule(Some(deadline));
        let advancing = AdvancingTimer {
            clock: clock.clone(),
            calls: AtomicUsize::new(0),
            advance_on,
            pending,
            interval,
        };
        assert!(matches!(
            sampler.poll_sample(
                &mut cx,
                clock.as_ref(),
                &advancing,
                clock.as_ref(),
                &handle,
                || before.clone()
            ),
            Poll::Ready(Ok(Some(_)))
        ));
        assert_eq!(advancing.calls.load(Ordering::SeqCst), advance_on);
        assert_eq!(sampler.next_deadline(), Some(deadline + interval));
    }
    let samples = metrics.snapshot(Duration::from_secs(1));
    assert_eq!(samples.len(), 6);
    for (i, sample) in samples.iter().enumerate() {
        assert_eq!(sample.metric, ROUTER_METRICS[i]);
        assert_eq!(
            sample.measurement,
            Measurement::Available {
                value: if i == 1 {
                    MetricValue::Bytes(0)
                } else {
                    MetricValue::UnsignedGauge(0)
                },
                observed_at_ns: 123,
            }
        );
    }
    // Distinct fixture values catch accidental field swaps and unit confusion.
    let mut projection = before.clone();
    projection.admission_usage = ResourceUsage {
        items: 7,
        bytes: 800,
    };
    projection.delivery.retained_deliveries = 3;
    projection.delivery.tracked_obligations = 4;
    projection.delivery.journal_records = 5;
    projection.delivery.escalated_records = 2;
    assert!(
        projection
            .record_metrics(&handle, 456)
            .iter()
            .all(|(_, r)| *r == Ok(ObservationAdmission::Accepted))
    );
    let samples = metrics.snapshot(Duration::from_secs(1));
    for (sample, value) in samples.iter().zip([7, 800, 3, 4, 5, 2]) {
        assert_eq!(
            sample.measurement,
            Measurement::Available {
                value: if sample.metric == ROUTER_METRICS[1] {
                    MetricValue::Bytes(value)
                } else {
                    MetricValue::UnsignedGauge(value)
                },
                observed_at_ns: 456,
            }
        );
    }
    clock.advance(Duration::from_secs(1)).unwrap();
    assert!(
        metrics
            .snapshot(Duration::from_secs(1))
            .iter()
            .all(|s| matches!(
                s.measurement,
                Measurement::Stale {
                    observed_at_ns: 456,
                    ..
                }
            ))
    );
    metrics.set_enabled(false);
    assert!(
        before
            .record_metrics(&handle, 789)
            .iter()
            .all(|(_, r)| *r == Ok(ObservationAdmission::Disabled))
    );
    assert!(
        metrics
            .snapshot(Duration::from_secs(1))
            .iter()
            .all(|s| s.measurement
                == Measurement::Unavailable {
                    reason: MissingMeasurement::Disabled
                })
    );
    metrics.set_enabled(true);
    // Replacement invalidates the old generation; missing series remain errors
    // while the one registered gauge continues to be sampled independently.
    let replacement = metrics
        .register(binding(2, vec![ROUTER_METRICS[0]]))
        .unwrap();
    assert!(
        before
            .record_metrics(&handle, 0)
            .iter()
            .all(|(_, r)| *r == Err(ErrorCode::StaleGeneration))
    );
    let partial = before.record_metrics(&replacement, 0);
    assert_eq!(partial[0].1, Ok(ObservationAdmission::Accepted));
    assert!(partial[1..].iter().all(|(_, r)| r.is_err()));
    assert_eq!(
        before,
        RouterDrainSnapshot::observe(&namespaces, &budget, &registry, &delivery)
    );
    assert!(!delivery.is_closed());
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(namespaces.poll_shutdown(&mut cx).is_ready());
    registry.shutdown();
    delivery.close();
    // Every other owner is closed and empty. Empty admission is still open.
    let open = RouterDrainSnapshot::observe(&namespaces, &budget, &registry, &delivery);
    assert!(open.delivery.is_drained());
    assert!(!open.is_drained());
    budget.close();
    let closed = RouterDrainSnapshot::observe(&namespaces, &budget, &registry, &delivery);
    assert!(closed.admission_closed);
    assert!(closed.is_drained());
    assert!(!open.admission_closed); // Earlier observations are owned and stable.
}

#[test]
fn registered_router_shutdown_reconciles_capacity_before_closing_delivery() {
    use marketforge_engine::live::{
        foundation::resources::ResourceUsage,
        routing::{
            acknowledgement::{AcknowledgementTracker, FinalizationJournal},
            admission::DeadlineFuture,
            destination::DestinationCapacity,
            driver::{DeliveryDriver, RegistryWorkerSyncError},
            namespaces::AdmissionNamespaces,
            ordered::OrderedAdmissionBudget,
            queue::DestinationQueue,
            shutdown::{RegisteredRouterShutdownStep, RouterShutdownOwners},
            supervisor::DeliverySupervisor,
            tracking_recovery::TrackingRecovery,
            worker::{DestinationWorkers, WorkerRegistrationError},
        },
    };
    use std::{
        num::NonZeroUsize,
        sync::atomic::{AtomicUsize, Ordering},
        task::{Context, Wake, Waker},
    };
    #[derive(Default)]
    struct Counter(AtomicUsize);
    impl Wake for Counter {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let registry = RouterRegistry::new();
    let mut queues = Vec::new();
    for id in [1, 2] {
        let adapter = destination(id, DestinationKind::Publication, DeliveryPolicy::Required);
        let queue = DestinationQueue::new(
            DestinationCapacity::new(adapter.descriptor().capacity, 1).unwrap(),
        );
        registry.register_queue(adapter, queue.clone()).unwrap();
        queues.push(queue);
    }
    queues[0].try_enqueue(batch()).unwrap();
    let held = queues[0].try_dequeue().unwrap();
    queues[1].try_enqueue(batch()).unwrap();
    let held_second = queues[1].try_dequeue().unwrap();
    // Exercise retired queues before the active registry, with only one slot.
    registry.unregister(DestinationId(1)).unwrap();
    let namespaces = AdmissionNamespaces::new(1).unwrap();
    namespaces.register(batch().first_cursor().clone()).unwrap();
    let budget = OrderedAdmissionBudget::new(ResourceUsage {
        items: 10,
        bytes: 100_000,
    })
    .unwrap();
    let limit = NonZeroUsize::new(1).unwrap();
    let mut delivery = DeliveryDriver::new(
        DeliverySupervisor::new(DestinationWorkers::new(limit), limit),
        AcknowledgementTracker::new(2).unwrap(),
        TrackingRecovery::new(2).unwrap(),
        FinalizationJournal::new(2).unwrap(),
    );
    let mut owners = RouterShutdownOwners {
        namespaces: &namespaces,
        admission: &budget,
        destinations: &registry,
        delivery: &mut delivery,
    };
    let counter = Arc::new(Counter::default());
    let waker = Waker::from(counter.clone());
    let mut cx = Context::from_waker(&waker);
    let mut expired: DeadlineFuture = Box::pin(std::future::ready(()));
    assert!(matches!(
        owners.drive_registered(&mut cx, &mut expired),
        RegisteredRouterShutdownStep::DeadlineReached
    ));
    let snapshot = owners.snapshot();
    assert!(
        snapshot.admission_closed
            && snapshot.namespaces.shutting_down
            && snapshot.destinations.shutting_down
    );
    assert!(snapshot.delivery.workers.workers.is_empty());
    assert!(!owners.delivery.is_closed());
    assert!(!snapshot.is_drained());
    let mut later: DeadlineFuture = Box::pin(std::future::pending());
    let RegisteredRouterShutdownStep::Step(turn) = owners.drive_registered(&mut cx, &mut later)
    else {
        panic!("attach retired queue")
    };
    assert_eq!(turn.registry.attachment, Ok(true));
    assert_eq!(
        owners.snapshot().delivery.workers.workers[0].0,
        DestinationId(1)
    );
    drop(turn);
    let before = counter.0.load(Ordering::SeqCst);
    for _ in 0..2 {
        let RegisteredRouterShutdownStep::Step(turn) = owners.drive_registered(&mut cx, &mut later)
        else {
            panic!("retain capacity responsibility")
        };
        assert_eq!(
            turn.registry.attachment,
            Err(RegistryWorkerSyncError::Scheduler(
                WorkerRegistrationError::Full
            ))
        );
        assert!(turn.delivery.delivery.is_pending());
        assert!(!owners.delivery.is_closed());
    }
    assert_eq!(counter.0.load(Ordering::SeqCst), before);
    drop(held);
    let RegisteredRouterShutdownStep::Step(turn) = owners.drive_registered(&mut cx, &mut later)
    else {
        panic!("reconcile after charge release")
    };
    assert_eq!(turn.registry.workers_reaped, 1);
    assert_eq!(turn.registry.retired_reaped, 1);
    assert_eq!(turn.registry.attachment, Ok(true));
    assert_eq!(
        owners.snapshot().delivery.workers.workers[0].0,
        DestinationId(2)
    );
    assert!(!owners.delivery.is_closed());
    drop(turn);
    drop(held_second);
    let before = counter.0.load(Ordering::SeqCst);
    let RegisteredRouterShutdownStep::Step(turn) = owners.drive_registered(&mut cx, &mut later)
    else {
        panic!("finish reconciliation")
    };
    assert_eq!(turn.registry.attachment, Ok(false));
    assert!(owners.delivery.is_closed());
    assert!(counter.0.load(Ordering::SeqCst) > before);
    drop(turn);
    assert!(matches!(
        owners.drive_registered(&mut cx, &mut later),
        RegisteredRouterShutdownStep::Drained
    ));
    assert!(matches!(
        owners.drive_registered(&mut cx, &mut expired),
        RegisteredRouterShutdownStep::Drained
    ));
    assert!(
        queues
            .iter()
            .all(|queue| queue.observe().capacity.occupied_events == 0)
    );
}

#[test]
fn registered_router_shutdown_delivers_committed_queues_through_completion_and_reporting() {
    use marketforge_engine::live::{
        foundation::{CancellationToken, clock::ManualClock, resources::ResourceUsage},
        model::delivery::{DeliveryStage, DeliveryStatus},
        ports::routing::DestinationCompletion,
        routing::{
            acknowledgement::{AcknowledgementTracker, FinalizationJournal},
            admission::{AdmissionContext, DeadlineFuture},
            destination::DestinationCapacity,
            driver::DeliveryDriver,
            namespaces::AdmissionNamespaces,
            ordered::{OrderedAdmissionBudget, OrderedCommitOutcome},
            queue::DestinationQueue,
            shutdown::{RegisteredRouterShutdownStep, RouterShutdownOwners},
            supervisor::{DeliverySupervisor, SupervisionStep},
            tracking_recovery::TrackingRecovery,
            worker::DestinationWorkers,
        },
    };
    use std::{
        future::Future,
        num::NonZeroUsize,
        task::{Context, Poll, Waker},
        time::{Duration, Instant},
    };
    let registry = RouterRegistry::new();
    for id in [1, 2] {
        let adapter = destination(id, DestinationKind::Publication, DeliveryPolicy::Required);
        let queue = DestinationQueue::new(
            DestinationCapacity::new(adapter.descriptor().capacity, 1).unwrap(),
        );
        registry.register_queue(adapter, queue).unwrap();
    }
    let namespaces = AdmissionNamespaces::new(1).unwrap();
    namespaces.register(batch().first_cursor().clone()).unwrap();
    let budget = OrderedAdmissionBudget::new(ResourceUsage {
        items: 10,
        bytes: 100_000,
    })
    .unwrap();
    let clock = Arc::new(ManualClock::new(Instant::now(), 0));
    let mut cx = Context::from_waker(Waker::noop());
    let mut future = Box::pin(budget.prepare(
        &registry,
        &namespaces,
        batch(),
        AdmissionContext::without_deadline(CancellationToken::new()),
        clock.clone(),
        None,
    ));
    let Poll::Ready(Ok(prepared)) = future.as_mut().poll(&mut cx) else {
        panic!("available capacity")
    };
    drop(future);
    let limit = NonZeroUsize::new(1).unwrap();
    let mut delivery = DeliveryDriver::new(
        DeliverySupervisor::new(DestinationWorkers::new(limit), limit),
        AcknowledgementTracker::new(2).unwrap(),
        TrackingRecovery::new(2).unwrap(),
        FinalizationJournal::new(2).unwrap(),
    );
    let Ok((OrderedCommitOutcome::Tracked(_), _)) = delivery.commit_ordered(prepared) else {
        panic!("commit required delivery")
    };
    assert_eq!(delivery.snapshot().tracked_obligations, 2);
    assert!(delivery.snapshot().workers.workers.is_empty());
    let mut owners = RouterShutdownOwners {
        namespaces: &namespaces,
        admission: &budget,
        destinations: &registry,
        delivery: &mut delivery,
    };
    let mut deadline: DeadlineFuture = Box::pin(std::future::pending());
    let mut completions = Vec::new();
    let mut reported = 0;
    let mut drained = false;
    // Finite deterministic scheduler: two queues exceed the one-worker bound.
    for _ in 0..12 {
        match owners.drive_registered(&mut cx, &mut deadline) {
            RegisteredRouterShutdownStep::Drained => {
                drained = true;
                break;
            }
            RegisteredRouterShutdownStep::DeadlineReached => panic!("pending deadline"),
            RegisteredRouterShutdownStep::Step(turn) => {
                if let Poll::Ready(SupervisionStep::Retained(_)) = &turn.delivery.delivery {
                    let id = owners.snapshot().delivery.workers.workers[0].0;
                    assert!(!completions.contains(&id));
                    owners
                        .delivery
                        .apply_completion(&DestinationCompletion {
                            destination: id,
                            through: batch().last_cursor().clone(),
                            status: DeliveryStatus::Progress(DeliveryStage::Delivered),
                        })
                        .unwrap();
                    completions.push(id);
                }
                // Terminal output owns its payload charge until dropped.
                drop(turn);
                if let Some(claim) = owners
                    .delivery
                    .journal_mut()
                    .claim_next(clock.as_ref(), Duration::from_secs(1))
                    .unwrap()
                {
                    owners
                        .delivery
                        .journal_mut()
                        .acknowledge(claim.record.obligation.id, claim.token, clock.as_ref())
                        .unwrap();
                    reported += 1;
                }
            }
        }
    }
    assert!(drained);
    assert_eq!(completions, vec![DestinationId(1), DestinationId(2)]);
    assert_eq!(reported, 2);
    assert!(owners.snapshot().is_drained());
    assert_eq!(owners.snapshot().delivery.tracked_obligations, 0);
}

#[test]
fn reporting_router_shutdown_preserves_retry_and_timer_failure_until_drain() {
    use marketforge_engine::live::{
        foundation::{CancellationToken, clock::ManualClock, resources::ResourceUsage},
        model::delivery::{DeliveryStage, DeliveryStatus},
        ports::routing::DestinationCompletion,
        routing::{
            acknowledgement::{
                AcknowledgementTracker, FinalizationJournal, FinalizationRecord,
                FinalizationRetryPolicy,
            },
            admission::{AdmissionContext, DeadlineFuture, DeadlineTimer},
            destination::DestinationCapacity,
            driver::DeliveryDriver,
            namespaces::AdmissionNamespaces,
            ordered::{OrderedAdmissionBudget, OrderedCommitOutcome},
            queue::DestinationQueue,
            reporting::{
                FinalizationReporter, FinalizationReportingConsumer, ReportingError, ReportingStep,
            },
            shutdown::{ReportingRouterShutdownStep, RouterShutdownOwners},
            supervisor::{DeliverySupervisor, SupervisionStep},
            tracking_recovery::TrackingRecovery,
            worker::DestinationWorkers,
        },
    };
    use std::{
        future::Future,
        num::NonZeroUsize,
        task::{Context, Poll, Waker},
        time::{Duration, Instant},
    };
    struct Reporter(Arc<std::sync::atomic::AtomicUsize>);
    impl FinalizationReporter for Reporter {
        fn report(&self, _: &FinalizationRecord) -> Result<(), ReportingError> {
            if self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                Err(ReportingError::TemporarilyUnavailable)
            } else {
                Ok(())
            }
        }
    }
    struct FullTimer;
    impl DeadlineTimer for FullTimer {
        fn sleep_until(&self, _: Instant) -> DeadlineFuture {
            panic!("must use bounded admission")
        }
        fn try_sleep_until(
            &self,
            _: Instant,
        ) -> Result<DeadlineFuture, marketforge_engine::live::model::error::ErrorCode> {
            Err(marketforge_engine::live::model::error::ErrorCode::Capacity)
        }
    }
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let consumer = FinalizationReportingConsumer::new(
        Reporter(calls.clone()),
        Duration::from_secs(1),
        FinalizationRetryPolicy::new(Duration::from_secs(1), Duration::from_secs(1), 3).unwrap(),
    )
    .unwrap();
    let registry = RouterRegistry::new();
    for id in [1, 2] {
        let adapter = destination(id, DestinationKind::Publication, DeliveryPolicy::Required);
        let queue = DestinationQueue::new(
            DestinationCapacity::new(adapter.descriptor().capacity, 1).unwrap(),
        );
        registry.register_queue(adapter, queue).unwrap();
    }
    let namespaces = AdmissionNamespaces::new(1).unwrap();
    namespaces.register(batch().first_cursor().clone()).unwrap();
    let budget = OrderedAdmissionBudget::new(ResourceUsage {
        items: 10,
        bytes: 100_000,
    })
    .unwrap();
    let clock = Arc::new(ManualClock::new(Instant::now(), 0));
    let mut cx = Context::from_waker(Waker::noop());
    let mut future = Box::pin(budget.prepare(
        &registry,
        &namespaces,
        batch(),
        AdmissionContext::without_deadline(CancellationToken::new()),
        clock.clone(),
        None,
    ));
    let Poll::Ready(Ok(prepared)) = future.as_mut().poll(&mut cx) else {
        panic!("available capacity")
    };
    drop(future);
    let limit = NonZeroUsize::new(1).unwrap();
    let mut delivery = DeliveryDriver::new(
        DeliverySupervisor::new(DestinationWorkers::new(limit), limit),
        AcknowledgementTracker::new(2).unwrap(),
        TrackingRecovery::new(2).unwrap(),
        FinalizationJournal::new(2).unwrap(),
    );
    let Ok((OrderedCommitOutcome::Tracked(_), _)) = delivery.commit_ordered(prepared) else {
        panic!("commit required delivery")
    };
    assert_eq!(delivery.snapshot().tracked_obligations, 2);
    assert!(delivery.snapshot().workers.workers.is_empty());
    let mut owners = RouterShutdownOwners {
        namespaces: &namespaces,
        admission: &budget,
        destinations: &registry,
        delivery: &mut delivery,
    };
    let mut deadline: DeadlineFuture = Box::pin(std::future::pending());
    let mut expired: DeadlineFuture = Box::pin(std::future::ready(()));
    assert!(matches!(
        owners.drive_registered_reporting(
            &mut cx,
            &mut expired,
            clock.as_ref(),
            &FullTimer,
            Duration::from_secs(1),
            &consumer
        ),
        ReportingRouterShutdownStep::DeadlineReached
    ));
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert!(owners.snapshot().delivery.workers.workers.is_empty());
    let mut retries = 0;
    let mut scheduling_failures = 0;
    let mut completions = Vec::new();
    let mut reported = 0;
    let mut drained = false;
    // Finite deterministic scheduler: two queues exceed the one-worker bound.
    for _ in 0..16 {
        clock.advance(Duration::from_secs(1)).unwrap();
        match owners.drive_registered_reporting(
            &mut cx,
            &mut deadline,
            clock.as_ref(),
            &FullTimer,
            Duration::from_secs(1),
            &consumer,
        ) {
            ReportingRouterShutdownStep::Drained => {
                drained = true;
                break;
            }
            ReportingRouterShutdownStep::DeadlineReached => panic!("pending deadline"),
            ReportingRouterShutdownStep::Step {
                delivery: turn,
                scheduling,
                reporting,
            } => {
                if scheduling.is_err() {
                    scheduling_failures += 1;
                }
                match reporting.unwrap() {
                    ReportingStep::Reported => reported += 1,
                    ReportingStep::RetryScheduled => retries += 1,
                    ReportingStep::Idle => {}
                    ReportingStep::PermanentlyFailed => panic!("retry should succeed"),
                }
                if let Poll::Ready(SupervisionStep::Retained(_)) = &turn.delivery.delivery {
                    let id = owners.snapshot().delivery.workers.workers[0].0;
                    assert!(!completions.contains(&id));
                    owners
                        .delivery
                        .apply_completion(&DestinationCompletion {
                            destination: id,
                            through: batch().last_cursor().clone(),
                            status: DeliveryStatus::Progress(DeliveryStage::Delivered),
                        })
                        .unwrap();
                    completions.push(id);
                }
                // Terminal output owns its payload charge until dropped.
                drop(turn);
            }
        }
    }
    assert!(drained);
    assert_eq!(completions, vec![DestinationId(1), DestinationId(2)]);
    assert_eq!(reported, 2);
    assert_eq!(retries, 1);
    assert!(scheduling_failures > 0);
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 3);
    assert!(owners.snapshot().is_drained());
    assert_eq!(owners.snapshot().delivery.tracked_obligations, 0);
}

#[test]
fn reporting_router_shutdown_releases_timer_when_empty_delivery_closes() {
    use marketforge_engine::live::{
        foundation::{clock::ManualClock, resources::ResourceUsage},
        routing::{
            acknowledgement::{
                AcknowledgementTracker, FinalizationJournal, FinalizationRecord,
                FinalizationRetryPolicy,
            },
            admission::{DeadlineFuture, DeadlineTimer},
            driver::DeliveryDriver,
            namespaces::AdmissionNamespaces,
            ordered::OrderedAdmissionBudget,
            reporting::{FinalizationReporter, FinalizationReportingConsumer, ReportingError},
            shutdown::{ReportingRouterShutdownStep, RouterShutdownOwners},
            supervisor::DeliverySupervisor,
            tracking_recovery::TrackingRecovery,
            worker::DestinationWorkers,
        },
    };
    use std::{
        future::Future,
        num::NonZeroUsize,
        pin::Pin,
        sync::atomic::{AtomicUsize, Ordering},
        task::{Context, Poll, Waker},
        time::{Duration, Instant},
    };
    struct Sleep(Arc<AtomicUsize>);
    impl Future for Sleep {
        type Output = ();
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
            Poll::Pending
        }
    }
    impl Drop for Sleep {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }
    struct Timer(Arc<AtomicUsize>);
    impl DeadlineTimer for Timer {
        fn sleep_until(&self, _: Instant) -> DeadlineFuture {
            self.0.fetch_add(1, Ordering::SeqCst);
            Box::pin(Sleep(self.0.clone()))
        }
    }
    struct Reporter;
    impl FinalizationReporter for Reporter {
        fn report(&self, _: &FinalizationRecord) -> Result<(), ReportingError> {
            panic!("no obligations")
        }
    }
    let slots = Arc::new(AtomicUsize::new(0));
    let registry = RouterRegistry::new();
    let namespaces = AdmissionNamespaces::new(1).unwrap();
    let budget = OrderedAdmissionBudget::new(ResourceUsage {
        items: 1,
        bytes: 1000,
    })
    .unwrap();
    let limit = NonZeroUsize::new(1).unwrap();
    let mut delivery = DeliveryDriver::new(
        DeliverySupervisor::new(DestinationWorkers::new(limit), limit),
        AcknowledgementTracker::new(1).unwrap(),
        TrackingRecovery::new(1).unwrap(),
        FinalizationJournal::new(1).unwrap(),
    );
    let consumer = FinalizationReportingConsumer::new(
        Reporter,
        Duration::from_secs(1),
        FinalizationRetryPolicy::new(Duration::from_secs(1), Duration::from_secs(1), 1).unwrap(),
    )
    .unwrap();
    let mut owners = RouterShutdownOwners {
        namespaces: &namespaces,
        admission: &budget,
        destinations: &registry,
        delivery: &mut delivery,
    };
    let clock = ManualClock::new(Instant::now(), 0);
    let mut cx = Context::from_waker(Waker::noop());
    let mut deadline: DeadlineFuture = Box::pin(std::future::pending());
    let turn = owners.drive_registered_reporting(
        &mut cx,
        &mut deadline,
        &clock,
        &Timer(slots.clone()),
        Duration::from_secs(1),
        &consumer,
    );
    assert!(matches!(
        turn,
        ReportingRouterShutdownStep::Step {
            scheduling: Ok(()),
            ..
        }
    ));
    assert!(owners.snapshot().is_drained());
    assert_eq!(slots.load(Ordering::SeqCst), 0);
    assert!(matches!(
        owners.drive_registered_reporting(
            &mut cx,
            &mut deadline,
            &clock,
            &Timer(slots.clone()),
            Duration::from_secs(1),
            &consumer
        ),
        ReportingRouterShutdownStep::Drained
    ));
    assert_eq!(slots.load(Ordering::SeqCst), 0);
}

mod ingress_witnesses {
    use super::*;
    use marketforge_engine::live::{
        foundation::{
            cancellation::CancellationToken, clock::ManualClock, resources::ResourceUsage,
        },
        routing::{
            acknowledgement::{AcknowledgementTracker, FinalizationJournal},
            admission::AdmissionContext,
            destination::DestinationCapacity,
            driver::DeliveryDriver,
            ingress::{IngressError, IngressOutcome, RouterIngress},
            namespaces::AdmissionNamespaces,
            ordered::{OptionalOrderedOutcome, OrderedAdmissionBudget, OrderedCommitOutcome},
            queue::DestinationQueue,
            router::TrackedFanoutResult,
            supervisor::DeliverySupervisor,
            tracking_recovery::TrackingRecovery,
            worker::DestinationWorkers,
        },
    };
    use std::{
        future::Future,
        num::NonZeroUsize,
        sync::Mutex,
        task::{Context, Poll, Waker},
        time::Instant,
    };

    struct Fixture {
        registry: RouterRegistry,
        namespaces: AdmissionNamespaces,
        budget: OrderedAdmissionBudget,
        driver: Mutex<DeliveryDriver>,
        clock: Arc<ManualClock>,
    }
    impl Fixture {
        fn new() -> Self {
            let limit = NonZeroUsize::new(2).unwrap();
            let namespaces = AdmissionNamespaces::new(1).unwrap();
            namespaces.register(batch().first_cursor().clone()).unwrap();
            Self {
                registry: RouterRegistry::new(),
                namespaces,
                budget: OrderedAdmissionBudget::new(ResourceUsage {
                    items: 2,
                    bytes: 100_000,
                })
                .unwrap(),
                driver: Mutex::new(DeliveryDriver::new(
                    DeliverySupervisor::new(DestinationWorkers::new(limit), limit),
                    AcknowledgementTracker::new(4).unwrap(),
                    TrackingRecovery::new(4).unwrap(),
                    FinalizationJournal::new(4).unwrap(),
                )),
                clock: Arc::new(ManualClock::new(Instant::now(), 0)),
            }
        }
        fn queue(&self, id: u64, policy: DeliveryPolicy) -> DestinationQueue {
            let adapter = Arc::new(TestDestination {
                descriptor: DestinationDescriptor {
                    id: DestinationId(id),
                    kind: DestinationKind::Publication,
                    policy,
                    capacity: DeliveryCapacity::new(2, 100_000).unwrap(),
                },
            });
            let queue = DestinationQueue::new(
                DestinationCapacity::new(adapter.descriptor().capacity, 2).unwrap(),
            );
            self.registry
                .register_queue(adapter, queue.clone())
                .unwrap();
            queue
        }
        fn ingress(&self) -> RouterIngress<'_> {
            RouterIngress {
                registry: &self.registry,
                namespaces: &self.namespaces,
                budget: &self.budget,
                delivery: &self.driver,
                clock: self.clock.clone(),
                timer: None,
            }
        }
        fn next(&self) -> u64 {
            self.namespaces.snapshot().namespaces[0]
                .order
                .next
                .as_ref()
                .unwrap()
                .offset
        }
    }
    fn context() -> AdmissionContext {
        AdmissionContext::without_deadline(CancellationToken::new())
    }
    fn ready<F: Future>(future: F) -> F::Output {
        let mut future = std::pin::pin!(future);
        match future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        {
            Poll::Ready(value) => value,
            Poll::Pending => panic!("expected immediate admission result"),
        }
    }

    #[test]
    fn ingress_composes_required_and_optional_without_claiming_completion() {
        let fixture = Fixture::new();
        let required = fixture.queue(1, DeliveryPolicy::Required);
        let optional = fixture.queue(2, DeliveryPolicy::BestEffort);
        // Optional overload must not prevent required admission.
        optional.try_enqueue(batch()).unwrap();
        let ingress = fixture.ingress();
        let original = batch();
        let outcome = ready(ingress.admit(original.clone(), context())).unwrap();
        let IngressOutcome::Required {
            required: result,
            optional,
        } = outcome
        else {
            panic!("required destination participates")
        };
        assert!(matches!(
            result,
            OrderedCommitOutcome::Tracked(TrackedFanoutResult::Complete { .. })
        ));
        let optional = optional.unwrap().unwrap();
        assert!(optional.queued.is_empty());
        assert_eq!(optional.failed.len(), 1);
        assert_eq!(fixture.next(), 3);
        assert_eq!(fixture.budget.usage(), ResourceUsage::default());
        assert_eq!(
            fixture
                .driver
                .lock()
                .unwrap()
                .snapshot()
                .tracked_obligations,
            1
        );
        let delivery = required.try_dequeue().unwrap();
        assert!(Arc::ptr_eq(
            &delivery.batch().events()[0],
            &original.events()[0]
        ));
        assert_eq!(delivery.batch().last_cursor(), batch().last_cursor());
        // No destination completion was fabricated by ingress.
        assert_eq!(
            fixture
                .driver
                .lock()
                .unwrap()
                .snapshot()
                .retained_deliveries,
            0
        );
    }

    #[test]
    fn ingress_wait_releases_driver_and_rolls_back_on_drop_or_owner_closure() {
        let fixture = Fixture::new();
        let queue = fixture.queue(1, DeliveryPolicy::Required);
        queue.try_enqueue(batch()).unwrap();
        let held = queue.try_dequeue().unwrap();
        let ingress = fixture.ingress();
        let mut future = Box::pin(ingress.admit(batch(), context()));
        fn assert_send<T: Send>(_: &T) {}
        assert_send(&future);
        let mut cx = Context::from_waker(Waker::noop());
        assert!(future.as_mut().poll(&mut cx).is_pending());
        assert_eq!(fixture.budget.usage().items, 2);
        assert!(fixture.driver.try_lock().is_ok());
        assert!(
            fixture.namespaces.snapshot().namespaces[0]
                .order
                .pending_last
                .is_some()
        );
        drop(future);
        assert_eq!(fixture.budget.usage(), ResourceUsage::default());
        assert!(
            fixture.namespaces.snapshot().namespaces[0]
                .order
                .pending_last
                .is_none()
        );
        assert_eq!(fixture.next(), 1);
        let mut future = Box::pin(ingress.admit(batch(), context()));
        assert!(future.as_mut().poll(&mut cx).is_pending());
        fixture.driver.lock().unwrap().close();
        drop(held);
        let Poll::Ready(Err(rejected)) = future.as_mut().poll(&mut cx) else {
            panic!("closed driver rejects after capacity resumes")
        };
        assert_eq!(rejected.reason, IngressError::DriverClosed);
        assert_eq!(rejected.batch, batch());
        assert_eq!(fixture.next(), 1);
        assert_eq!(fixture.budget.usage(), ResourceUsage::default());
        assert!(queue.try_dequeue().is_none());
        assert_eq!(
            fixture
                .driver
                .lock()
                .unwrap()
                .snapshot()
                .tracked_obligations,
            0
        );
    }

    #[test]
    fn ingress_wait_cancellation_and_budget_exhaustion_keep_frontier_retryable() {
        let fixture = Fixture::new();
        let queue = fixture.queue(1, DeliveryPolicy::Required);
        queue.try_enqueue(batch()).unwrap();
        let held = queue.try_dequeue().unwrap();
        let ingress = fixture.ingress();
        let cancellation = CancellationToken::new();
        let mut waiting = Box::pin(ingress.admit(
            batch(),
            AdmissionContext::without_deadline(cancellation.clone()),
        ));
        let mut cx = Context::from_waker(Waker::noop());
        assert!(waiting.as_mut().poll(&mut cx).is_pending());
        let rejected = ready(ingress.admit(batch(), context())).unwrap_err();
        assert!(matches!(
            rejected.reason,
            IngressError::Preparation(
                marketforge_engine::live::routing::ordered::OrderedPreparationError::Budget(_)
            )
        ));
        assert_eq!(fixture.budget.usage().items, 2);
        cancellation.cancel();
        let Poll::Ready(Err(rejected)) = waiting.as_mut().poll(&mut cx) else {
            panic!("cancellation finishes without freeing destination capacity")
        };
        assert!(matches!(rejected.reason, IngressError::Preparation(_)));
        assert_eq!(fixture.budget.usage(), ResourceUsage::default());
        assert_eq!(fixture.next(), 1);
        drop(waiting);
        drop(held);
        // The same batch remains admissible after the cancelled attempt.
        assert!(matches!(
            ready(ingress.admit(rejected.batch, context())).unwrap(),
            IngressOutcome::Required {
                required: OrderedCommitOutcome::Tracked(TrackedFanoutResult::Complete { .. }),
                ..
            }
        ));
        assert_eq!(fixture.next(), 3);
        assert_eq!(fixture.budget.usage(), ResourceUsage::default());
    }

    #[test]
    fn ingress_optional_and_early_failures_preserve_explicit_outcomes() {
        let fixture = Fixture::new();
        let ingress = fixture.ingress();
        let no_destinations = ready(ingress.admit(batch(), context())).unwrap_err();
        assert!(matches!(
            no_destinations.reason,
            IngressError::Preparation(_)
        ));
        let optional = fixture.queue(1, DeliveryPolicy::BestEffort);
        let guard = fixture.driver.lock().unwrap();
        assert_eq!(
            ready(ingress.admit(batch(), context())).unwrap_err().reason,
            IngressError::DriverBusy
        );
        drop(guard);
        assert_eq!(fixture.next(), 1);
        let cancelled = CancellationToken::new();
        cancelled.cancel();
        let outcome =
            ready(ingress.admit(batch(), AdmissionContext::without_deadline(cancelled))).unwrap();
        assert!(matches!(
            outcome,
            IngressOutcome::Optional(OptionalOrderedOutcome::Rejected { .. })
        ));
        assert_eq!(fixture.next(), 1);
        assert!(optional.try_dequeue().is_none());
        optional.try_enqueue(batch()).unwrap();
        let outcome = ready(ingress.admit(batch(), context())).unwrap();
        let IngressOutcome::Optional(OptionalOrderedOutcome::Attempted(attempt)) = outcome else {
            panic!("optional-only overload is an explicit attempted outcome")
        };
        assert!(attempt.queued.is_empty());
        assert_eq!(attempt.failed.len(), 1);
        assert_eq!(fixture.next(), 3);
        assert_eq!(
            fixture
                .driver
                .lock()
                .unwrap()
                .snapshot()
                .tracked_obligations,
            0
        );
    }
}
