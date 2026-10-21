use crate::live::support::live_events::l2_event;
use marketforge_engine::live::{
    model::delivery::{DeliveryCapacity, DeliveryFailure, DeliveryPolicy, DestinationKind},
    ports::routing::{
        DestinationAdmission, DestinationDescriptor, DestinationId, EventBatch,
        EventDestinationPort, RoutingFuture,
    },
    routing::{
        destination::DestinationCapacity,
        queue::DestinationQueue,
        worker::{DestinationWorker, WorkerAdmissionResult, WorkerCreationError, WorkerStep},
    },
};
use std::{
    future::poll_fn,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
};
#[derive(Default)]
struct WakeCount(AtomicUsize);
impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
struct Adapter {
    descriptor: DestinationDescriptor,
    calls: AtomicUsize,
    ready: AtomicBool,
    waiter: Mutex<Option<Waker>>,
    mode: u8,
}
impl Adapter {
    fn release(&self) {
        self.ready.store(true, Ordering::SeqCst);
        if let Some(w) = self.waiter.lock().unwrap().take() {
            w.wake();
        }
    }
}
impl EventDestinationPort for Adapter {
    fn descriptor(&self) -> DestinationDescriptor {
        self.descriptor
    }
    fn admit<'a>(&'a self, batch: EventBatch) -> RoutingFuture<'a, DestinationAdmission> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            poll_fn(|cx| {
                let mut waiter = self.waiter.lock().unwrap();
                if self.ready.load(Ordering::SeqCst) {
                    Poll::Ready(())
                } else {
                    *waiter = Some(cx.waker().clone());
                    Poll::Pending
                }
            })
            .await;
            match self.mode {
                1 => DestinationAdmission::Rejected {
                    destination: self.descriptor.id,
                    reason: DeliveryFailure::Unavailable,
                },
                2 => DestinationAdmission::Accepted {
                    destination: DestinationId(999),
                    through: batch.last_cursor().clone(),
                },
                3 => DestinationAdmission::Accepted {
                    destination: self.descriptor.id,
                    through: batch.first_cursor().clone(),
                },
                4 => DestinationAdmission::Rejected {
                    destination: DestinationId(999),
                    reason: DeliveryFailure::Unavailable,
                },
                _ => DestinationAdmission::Accepted {
                    destination: self.descriptor.id,
                    through: batch.last_cursor().clone(),
                },
            }
        })
    }
}
fn fixture(mode: u8) -> (DestinationQueue, Arc<Adapter>, DestinationWorker) {
    fixture_id(mode, 7)
}
fn fixture_id(mode: u8, id: u64) -> (DestinationQueue, Arc<Adapter>, DestinationWorker) {
    let capacity = DeliveryCapacity::new(4, 100_000).unwrap();
    let queue = DestinationQueue::new(DestinationCapacity::new(capacity, 1).unwrap());
    let adapter = Arc::new(Adapter {
        descriptor: DestinationDescriptor {
            id: DestinationId(id),
            kind: DestinationKind::Recording,
            policy: DeliveryPolicy::Durable,
            capacity,
        },
        calls: AtomicUsize::new(0),
        ready: AtomicBool::new(false),
        waiter: Mutex::new(None),
        mode,
    });
    let worker = DestinationWorker::new(queue.clone(), adapter.clone()).unwrap();
    (queue, adapter, worker)
}
fn batch(offset: u64) -> EventBatch {
    EventBatch::new(vec![
        l2_event("worker", 1, offset),
        l2_event("worker", 1, offset + 1),
    ])
    .unwrap()
}

#[test]
fn destination_worker_retains_call_payload_and_charge_until_outcome_handoff() {
    let (queue, adapter, mut worker) = fixture(0);
    let first = batch(10);
    let second = batch(12);
    queue.try_enqueue(first.clone()).unwrap();
    queue.try_enqueue(second.clone()).unwrap();
    assert!(queue.try_dequeue().is_none()); // exclusive consumer fence
    assert!(matches!(
        DestinationWorker::new(queue.clone(), adapter.clone()),
        Err(WorkerCreationError::AlreadyClaimed)
    ));
    let wakes = Arc::new(WakeCount::default());
    let waker = Waker::from(wakes.clone());
    let mut cx = Context::from_waker(&waker);
    assert_eq!(worker.poll_admission(&mut cx), Poll::Pending);
    assert_eq!(worker.poll_admission(&mut cx), Poll::Pending);
    assert_eq!(adapter.calls.load(Ordering::SeqCst), 1);
    assert!(worker.take_outcome().is_none());
    assert_eq!(queue.observe().capacity.occupied_events, 4);
    assert_eq!(queue.snapshot().queued_events, 2);
    // A caller can stop polling for arbitrary time, close at its shutdown deadline,
    // then resume the same call without replaying an ambiguous admission.
    worker.close();
    let before = worker.snapshot();
    assert!(before.closed && before.in_flight && !before.is_drained());
    assert!(queue.try_enqueue(batch(14)).is_err());
    adapter.release();
    assert_eq!(wakes.0.load(Ordering::SeqCst), 1);
    assert_eq!(
        worker.poll_admission(&mut cx),
        Poll::Ready(WorkerStep::OutcomeReady)
    );
    assert_eq!(
        worker.poll_admission(&mut cx),
        Poll::Ready(WorkerStep::OutcomeReady)
    );
    assert_eq!(adapter.calls.load(Ordering::SeqCst), 1);
    let outcome = worker.take_outcome().unwrap();
    assert_eq!(outcome.payload.batch(), &first);
    assert!(Arc::ptr_eq(
        &outcome.payload.batch().events()[0],
        &first.events()[0]
    ));
    assert!(matches!(
        outcome.admission,
        WorkerAdmissionResult::Valid(DestinationAdmission::Accepted { .. })
    ));
    assert_eq!(outcome.descriptor.policy, DeliveryPolicy::Durable); // acceptance is still no durable receipt
    assert_eq!(queue.observe().capacity.occupied_events, 4);
    drop(outcome);
    assert_eq!(queue.observe().capacity.occupied_events, 2);
    assert_eq!(
        worker.poll_admission(&mut cx),
        Poll::Ready(WorkerStep::OutcomeReady)
    );
    let outcome = worker.take_outcome().unwrap();
    assert_eq!(outcome.payload.batch(), &second);
    assert_eq!(adapter.calls.load(Ordering::SeqCst), 2);
    assert!(!worker.snapshot().is_drained()); // transferred payload still charged
    assert_eq!(worker.snapshot().occupied_events, 2);
    drop(outcome);
    assert_eq!(
        worker.poll_admission(&mut cx),
        Poll::Ready(WorkerStep::Idle)
    );
    assert!(worker.snapshot().is_drained());
    assert!(!before.is_drained());
    assert_eq!(queue.observe().capacity.occupied_events, 0);
}

#[test]
fn destination_worker_validates_responses_and_retains_failed_payloads() {
    let waker = Waker::from(Arc::new(WakeCount::default()));
    let mut cx = Context::from_waker(&waker);
    for mode in 1..=4 {
        let (queue, adapter, mut worker) = fixture(mode);
        let original = batch(10);
        queue.try_enqueue(original.clone()).unwrap();
        adapter.release();
        assert_eq!(
            worker.poll_admission(&mut cx),
            Poll::Ready(WorkerStep::OutcomeReady)
        );
        let outcome = worker.take_outcome().unwrap();
        assert_eq!(outcome.payload.batch(), &original);
        assert_eq!(queue.observe().capacity.occupied_events, 2);
        if mode == 1 {
            assert!(matches!(
                outcome.admission,
                WorkerAdmissionResult::Valid(DestinationAdmission::Rejected {
                    reason: DeliveryFailure::Unavailable,
                    ..
                })
            ));
        } else {
            assert!(matches!(
                outcome.admission,
                WorkerAdmissionResult::InvalidResponse(_)
            ));
        }
        drop(outcome);
        assert_eq!(queue.observe().capacity.occupied_events, 0);
    }
}

#[test]
fn destination_worker_constructor_rejection_and_idle_lease_release() {
    let (queue, adapter, worker) = fixture(0);
    let other = DestinationQueue::new(
        DestinationCapacity::new(DeliveryCapacity::new(1, 100).unwrap(), 1).unwrap(),
    );
    assert!(matches!(
        DestinationWorker::new(other.clone(), adapter.clone()),
        Err(WorkerCreationError::CapacityMismatch)
    ));
    other.try_enqueue(batch(10)).unwrap_err(); // bounded actual memory charge
    drop(worker);
    let mut replacement = DestinationWorker::new(queue.clone(), adapter).unwrap();
    let waker = Waker::from(Arc::new(WakeCount::default()));
    assert_eq!(
        replacement.poll_admission(&mut Context::from_waker(&waker)),
        Poll::Ready(WorkerStep::Idle)
    );
    drop(replacement);
    queue.try_enqueue(batch(10)).unwrap();
    assert!(queue.try_dequeue().is_some());
}

#[test]
fn destination_worker_slow_adapter_does_not_block_another_destination() {
    let (slow_queue, slow_adapter, mut slow) = fixture(0);
    let (fast_queue, fast_adapter, mut fast) = fixture(0);
    let original = batch(10);
    slow_queue.try_enqueue(original.clone()).unwrap();
    fast_queue.try_enqueue(original).unwrap();
    fast_adapter.release();
    let waker = Waker::from(Arc::new(WakeCount::default()));
    let mut cx = Context::from_waker(&waker);
    assert_eq!(slow.poll_admission(&mut cx), Poll::Pending);
    assert_eq!(
        fast.poll_admission(&mut cx),
        Poll::Ready(WorkerStep::OutcomeReady)
    );
    drop(fast.take_outcome().unwrap());
    assert!(fast.snapshot().is_drained());
    assert!(!slow.snapshot().is_drained());
    assert_eq!(slow_adapter.calls.load(Ordering::SeqCst), 1);
    assert_eq!(slow_queue.observe().capacity.occupied_events, 2);
    slow_adapter.release();
    assert_eq!(
        slow.poll_admission(&mut cx),
        Poll::Ready(WorkerStep::OutcomeReady)
    );
    drop(slow.take_outcome().unwrap());
}

#[test]
fn destination_worker_registered_ordered_fanout_preserves_obligation_stages() {
    use marketforge_engine::live::{
        foundation::{CancellationToken, SystemMonotonicClock, resources::ResourceUsage},
        model::delivery::{DeliveryStage, DeliveryStatus},
        routing::{
            acknowledgement::{AcknowledgementTracker, DeliveryObligationId},
            admission::AdmissionContext,
            namespaces::AdmissionNamespaces,
            ordered::{OrderedAdmissionBudget, OrderedCommitOutcome},
            router::{RegistryWorkerError, RouterRegistry, TrackedFanoutResult},
            tracking_recovery::TrackingRecovery,
        },
    };
    let (queue, adapter, initial_worker) = fixture(0);
    drop(initial_worker);
    let registry = RouterRegistry::new();
    registry
        .register_queue(adapter.clone(), queue.clone())
        .unwrap();
    assert!(matches!(
        registry.claim_worker(DestinationId(99)),
        Err(RegistryWorkerError::UnknownDestination)
    ));
    let mut worker = registry.claim_worker(DestinationId(7)).unwrap();
    let original = batch(10);
    let namespaces = AdmissionNamespaces::new(1).unwrap();
    namespaces
        .register(original.first_cursor().clone())
        .unwrap();
    let budget = OrderedAdmissionBudget::new(ResourceUsage {
        items: 4,
        bytes: 100_000,
    })
    .unwrap();
    let mut tracker = AcknowledgementTracker::new(2).unwrap();
    let mut recovery = TrackingRecovery::new(2).unwrap();
    let waker = Waker::from(Arc::new(WakeCount::default()));
    let mut cx = Context::from_waker(&waker);
    let mut preparing = Box::pin(budget.prepare(
        &registry,
        &namespaces,
        original.clone(),
        AdmissionContext::without_deadline(CancellationToken::new()),
        Arc::new(SystemMonotonicClock),
        None,
    ));
    use std::future::Future;
    let prepared = match preparing.as_mut().poll(&mut cx) {
        Poll::Ready(Ok(prepared)) => prepared,
        _ => panic!("local capacity must be immediately ready"),
    };
    drop(preparing);
    assert!(matches!(
        prepared.commit(&mut tracker, &mut recovery),
        OrderedCommitOutcome::Tracked(TrackedFanoutResult::Complete { .. })
    ));
    assert_eq!(tracker.len(), 1);
    assert_eq!(
        tracker.status(DeliveryObligationId(1)),
        Some(DeliveryStatus::Progress(DeliveryStage::Queued))
    );
    assert_eq!(worker.poll_admission(&mut cx), Poll::Pending);
    registry.shutdown();
    assert!(!worker.snapshot().is_drained());
    adapter.release();
    assert_eq!(
        worker.poll_admission(&mut cx),
        Poll::Ready(WorkerStep::OutcomeReady)
    );
    let outcome = worker.take_outcome().unwrap();
    assert!(Arc::ptr_eq(
        &outcome.payload.batch().events()[0],
        &original.events()[0]
    ));
    assert_eq!(
        outcome.payload.batch().last_cursor(),
        original.last_cursor()
    );
    // No invented delivered/committed/durable observation from adapter admission.
    assert_eq!(
        tracker.status(DeliveryObligationId(1)),
        Some(DeliveryStatus::Progress(DeliveryStage::Queued))
    );
    use marketforge_engine::live::{
        ports::routing::DestinationCompletion,
        routing::{acknowledgement::FinalizationJournal, completion::PendingDeliveries},
    };
    let mut deliveries = PendingDeliveries::new(std::num::NonZeroUsize::new(1).unwrap());
    let id = deliveries.ingest(outcome, &tracker).unwrap();
    assert_eq!(id, DeliveryObligationId(1));
    deliveries.apply_admission(id, &mut tracker).unwrap();
    assert_eq!(
        tracker.status(DeliveryObligationId(1)),
        Some(DeliveryStatus::Progress(DeliveryStage::Queued))
    );
    let mut journal = FinalizationJournal::new(1).unwrap();
    assert!(
        deliveries
            .finalize_into(id, &mut tracker, &mut journal)
            .is_err()
    );
    assert_eq!(deliveries.len(), 1);
    assert!(journal.is_empty());
    assert!(!worker.snapshot().is_drained());
    deliveries
        .apply_completion(
            &mut tracker,
            &DestinationCompletion {
                destination: DestinationId(7),
                through: original.last_cursor().clone(),
                status: DeliveryStatus::Progress(DeliveryStage::Committed),
            },
        )
        .unwrap();
    assert!(
        deliveries
            .finalize_into(id, &mut tracker, &mut journal)
            .is_err()
    );
    assert_eq!(deliveries.len(), 1);
    assert!(journal.is_empty());
    assert!(!worker.snapshot().is_drained());
    deliveries
        .apply_completion(
            &mut tracker,
            &DestinationCompletion {
                destination: DestinationId(7),
                through: original.last_cursor().clone(),
                status: DeliveryStatus::Progress(DeliveryStage::Durable),
            },
        )
        .unwrap();
    let finalized = deliveries
        .finalize_into(id, &mut tracker, &mut journal)
        .unwrap();
    assert!(deliveries.is_empty());
    assert_eq!(journal.len(), 1);
    assert!(tracker.is_empty());
    assert!(!worker.snapshot().is_drained());
    drop(finalized);
    assert!(worker.snapshot().is_drained());
    assert!(recovery.snapshot().is_drained());
}

#[test]
fn destination_worker_registration_replacement_fences_old_queue_ownership() {
    use marketforge_engine::live::routing::router::{RegistryWorkerError, RouterRegistry};
    let (queue, old_adapter, initial_worker) = fixture(0);
    drop(initial_worker);
    let registry = RouterRegistry::new();
    registry
        .register_queue(old_adapter.clone(), queue.clone())
        .unwrap();
    let mut old_worker = registry.claim_worker(DestinationId(7)).unwrap();
    queue.try_enqueue(batch(10)).unwrap();
    let waker = Waker::from(Arc::new(WakeCount::default()));
    let mut cx = Context::from_waker(&waker);
    assert_eq!(old_worker.poll_admission(&mut cx), Poll::Pending);
    registry.unregister(DestinationId(7)).unwrap();
    let (new_queue, new_adapter, initial_worker) = fixture(0);
    drop(initial_worker);
    registry
        .register_queue(new_adapter.clone(), new_queue.clone())
        .unwrap();
    let mut new_worker = registry.claim_worker(DestinationId(7)).unwrap();
    new_queue.try_enqueue(batch(12)).unwrap();
    old_worker.close(); // only the retired instance is closed
    assert!(!new_queue.snapshot().closed);
    new_adapter.release();
    assert_eq!(
        new_worker.poll_admission(&mut cx),
        Poll::Ready(WorkerStep::OutcomeReady)
    );
    let new_outcome = new_worker.take_outcome().unwrap();
    assert_eq!(new_outcome.payload.batch().first_cursor().offset, 12);
    assert_eq!(old_adapter.calls.load(Ordering::SeqCst), 1);
    old_adapter.release();
    assert_eq!(
        old_worker.poll_admission(&mut cx),
        Poll::Ready(WorkerStep::OutcomeReady)
    );
    let old_outcome = old_worker.take_outcome().unwrap();
    assert_eq!(old_outcome.payload.batch().first_cursor().offset, 10);
    drop(old_outcome);
    drop(new_outcome);
    assert!(old_worker.snapshot().is_drained());
    assert!(new_worker.snapshot().is_drained());
    registry.shutdown();
    drop(new_worker);
    assert!(registry.claim_worker(DestinationId(7)).is_ok()); // closed-queue drain is permitted
    let no_queue = RouterRegistry::new();
    no_queue.register(new_adapter).unwrap();
    assert!(matches!(
        no_queue.claim_worker(DestinationId(7)),
        Err(RegistryWorkerError::MissingQueue)
    ));
}

#[test]
fn destination_worker_concurrent_registry_claim_has_one_owner() {
    use marketforge_engine::live::routing::router::{RegistryWorkerError, RouterRegistry};
    let (queue, adapter, initial_worker) = fixture(0);
    drop(initial_worker);
    let registry = RouterRegistry::new();
    registry.register_queue(adapter, queue).unwrap();
    let barrier = std::sync::Barrier::new(2);
    let results = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..2)
            .map(|_| {
                scope.spawn(|| {
                    barrier.wait();
                    registry.claim_worker(DestinationId(7))
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(
                r,
                Err(RegistryWorkerError::Worker(
                    WorkerCreationError::AlreadyClaimed
                ))
            ))
            .count(),
        1
    );
    drop(results);
    assert!(registry.claim_worker(DestinationId(7)).is_ok());
}

#[test]
fn destination_worker_drops_adapter_before_charge_and_consumer_lease() {
    use std::{future::Future, pin::Pin};
    struct PendingCall {
        queue: DestinationQueue,
        dropped: Arc<AtomicBool>,
    }
    impl Future for PendingCall {
        type Output = DestinationAdmission;
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Pending
        }
    }
    impl Drop for PendingCall {
        fn drop(&mut self) {
            assert_eq!(self.queue.observe().capacity.occupied_events, 4);
            assert_eq!(self.queue.snapshot().queued_events, 2);
            assert!(self.queue.try_dequeue().is_none());
            self.dropped.store(true, Ordering::SeqCst);
        }
    }
    struct DropAdapter {
        descriptor: DestinationDescriptor,
        queue: DestinationQueue,
        dropped: Arc<AtomicBool>,
    }
    impl EventDestinationPort for DropAdapter {
        fn descriptor(&self) -> DestinationDescriptor {
            self.descriptor
        }
        fn admit<'a>(&'a self, _: EventBatch) -> RoutingFuture<'a, DestinationAdmission> {
            Box::pin(PendingCall {
                queue: self.queue.clone(),
                dropped: self.dropped.clone(),
            })
        }
    }
    let (queue, adapter, worker) = fixture(0);
    drop(worker);
    let dropped = Arc::new(AtomicBool::new(false));
    let mut worker = DestinationWorker::new(
        queue.clone(),
        Arc::new(DropAdapter {
            descriptor: adapter.descriptor,
            queue: queue.clone(),
            dropped: dropped.clone(),
        }),
    )
    .unwrap();
    queue.try_enqueue(batch(10)).unwrap();
    queue.try_enqueue(batch(12)).unwrap();
    let waker = Waker::from(Arc::new(WakeCount::default()));
    assert_eq!(
        worker.poll_admission(&mut Context::from_waker(&waker)),
        Poll::Pending
    );
    drop(worker); // abandonment remains explicit, never reported as successful drain
    assert!(dropped.load(Ordering::SeqCst));
    assert_eq!(queue.observe().capacity.occupied_events, 2);
    let remainder = queue.try_dequeue().unwrap();
    assert_eq!(remainder.batch().first_cursor().offset, 12);
}

#[test]
fn destination_worker_waits_for_queue_activity_and_closed_drain() {
    let (queue, adapter, mut worker) = fixture(0);
    let wakes = Arc::new(WakeCount::default());
    let waker = Waker::from(wakes.clone());
    let mut cx = Context::from_waker(&waker);
    assert_eq!(worker.poll_next(&mut cx), Poll::Pending);
    assert_eq!(worker.poll_next(&mut cx), Poll::Pending);
    queue.try_enqueue(batch(10)).unwrap();
    assert_eq!(wakes.0.load(Ordering::SeqCst), 1);
    assert_eq!(worker.poll_next(&mut cx), Poll::Pending);
    worker.close();
    assert_eq!(adapter.calls.load(Ordering::SeqCst), 1);
    adapter.release();
    assert_eq!(
        worker.poll_next(&mut cx),
        Poll::Ready(WorkerStep::OutcomeReady)
    );
    let outcome = worker.take_outcome().unwrap();
    assert_eq!(worker.poll_next(&mut cx), Poll::Ready(WorkerStep::Idle));
    assert!(!worker.snapshot().is_drained());
    drop(outcome);
    assert!(worker.snapshot().is_drained());
}

#[test]
fn destination_worker_queue_waiter_replacement_reserved_insert_and_release() {
    use marketforge_engine::live::model::memory::RetainedSize;
    let (queue, adapter, mut worker) = fixture(0);
    let old = Arc::new(WakeCount::default());
    let new = Arc::new(WakeCount::default());
    let old_waker = Waker::from(old.clone());
    let new_waker = Waker::from(new.clone());
    assert_eq!(
        worker.poll_next(&mut Context::from_waker(&old_waker)),
        Poll::Pending
    );
    assert_eq!(
        worker.poll_next(&mut Context::from_waker(&new_waker)),
        Poll::Pending
    );
    let payload = batch(10);
    let reservation = queue
        .capacity()
        .try_reserve(payload.len(), payload.retained_size_bytes().unwrap())
        .unwrap();
    queue.enqueue_reserved(payload, reservation).unwrap();
    assert_eq!(old.0.load(Ordering::SeqCst), 0);
    assert_eq!(new.0.load(Ordering::SeqCst), 1);
    drop(worker);
    // A new lease can drain the original queue without inheriting its old waiter.
    let mut worker = DestinationWorker::new(queue.clone(), adapter).unwrap();
    assert_eq!(
        worker.poll_next(&mut Context::from_waker(&new_waker)),
        Poll::Pending
    );
    drop(worker);
    let mut worker = DestinationWorker::new(queue.clone(), fixture(0).1).unwrap();
    assert_eq!(
        worker.poll_next(&mut Context::from_waker(&new_waker)),
        Poll::Pending
    );
    worker.close();
    assert_eq!(new.0.load(Ordering::SeqCst), 2);
    assert_eq!(
        worker.poll_next(&mut Context::from_waker(&new_waker)),
        Poll::Ready(WorkerStep::Idle)
    );
}

#[test]
fn destination_worker_scheduler_is_fair_bounded_and_retains_slow_calls() {
    use marketforge_engine::live::routing::worker::{DestinationWorkers, WorkerRegistrationError};
    let mut workers = DestinationWorkers::new(std::num::NonZeroUsize::new(2).unwrap());
    let (slow_queue, slow, slow_worker) = fixture_id(0, 1);
    let (fast_queue, fast, fast_worker) = fixture_id(0, 2);
    slow_queue.try_enqueue(batch(10)).unwrap();
    fast_queue.try_enqueue(batch(10)).unwrap();
    fast_queue.try_enqueue(batch(12)).unwrap();
    assert!(workers.insert(slow_worker).is_ok());
    assert!(workers.insert(fast_worker).is_ok());
    let (_, _, duplicate) = fixture_id(0, 1);
    let (reason, duplicate) = workers.insert(duplicate).err().unwrap();
    assert_eq!(reason, WorkerRegistrationError::DuplicateDestination);
    drop(duplicate);
    let (_, _, overflow) = fixture_id(0, 3);
    let (reason, overflow) = workers.insert(overflow).err().unwrap();
    assert_eq!(reason, WorkerRegistrationError::Full);
    drop(overflow);
    let waker = Waker::from(Arc::new(WakeCount::default()));
    let mut cx = Context::from_waker(&waker);
    fast.release();
    let Poll::Ready(first) = workers.poll_outcome(&mut cx) else {
        panic!("fast adapter blocked");
    };
    assert_eq!(first.descriptor.id, DestinationId(2));
    assert_eq!(slow.calls.load(Ordering::SeqCst), 1);
    slow.release();
    let Poll::Ready(second) = workers.poll_outcome(&mut cx) else {
        panic!("slow adapter starved");
    };
    assert_eq!(second.descriptor.id, DestinationId(1));
    assert_eq!(slow.calls.load(Ordering::SeqCst), 1);
    let Poll::Ready(third) = workers.poll_outcome(&mut cx) else {
        panic!("successor missing");
    };
    assert_eq!(third.descriptor.id, DestinationId(2));
    assert_eq!(third.payload.batch(), &batch(12));
    workers.close();
    assert!(workers.remove_drained(DestinationId(1)).is_none());
    assert!(!workers.snapshot().is_drained());
    drop((first, second, third));
    assert!(workers.snapshot().is_drained());
    assert!(workers.remove_drained(DestinationId(1)).is_some());
    assert!(workers.remove_drained(DestinationId(2)).is_some());
    assert!(workers.poll_outcome(&mut cx).is_pending());
    let (_, _, late) = fixture_id(0, 4);
    let (reason, _) = workers.insert(late).err().unwrap();
    assert_eq!(reason, WorkerRegistrationError::Closed);
}

#[test]
fn destination_worker_scheduler_wakes_and_preserves_pending_shutdown_ownership() {
    use marketforge_engine::live::routing::worker::DestinationWorkers;
    let mut workers = DestinationWorkers::new(std::num::NonZeroUsize::new(1).unwrap());
    let (queue, adapter, worker) = fixture(2);
    assert!(workers.insert(worker).is_ok());
    let wakes = Arc::new(WakeCount::default());
    let waker = Waker::from(wakes.clone());
    let mut cx = Context::from_waker(&waker);
    assert!(workers.poll_outcome(&mut cx).is_pending());
    queue.try_enqueue(batch(10)).unwrap();
    assert_eq!(wakes.0.load(Ordering::SeqCst), 1);
    assert!(workers.poll_outcome(&mut cx).is_pending());
    workers.close();
    assert!(!workers.snapshot().is_drained());
    assert!(workers.remove_drained(DestinationId(7)).is_none());
    assert!(queue.try_enqueue(batch(12)).is_err());
    // A supervisor can retain the whole scheduler at a deadline, then resume.
    let mut retained = workers;
    adapter.release();
    let Poll::Ready(outcome) = retained.poll_outcome(&mut cx) else {
        panic!("lost pending call");
    };
    assert_eq!(adapter.calls.load(Ordering::SeqCst), 1);
    assert!(matches!(
        outcome.admission,
        WorkerAdmissionResult::InvalidResponse(_)
    ));
    assert!(!retained.snapshot().is_drained());
    drop(outcome);
    assert!(retained.snapshot().is_drained());
}

#[test]
fn destination_worker_shutdown_deadline_retains_pending_and_queued_work() {
    use marketforge_engine::live::{
        foundation::deadline::ManualDeadlineTimer,
        routing::{
            admission::DeadlineTimer,
            worker::{DestinationWorkers, WorkerShutdownStep},
        },
    };
    let now = std::time::Instant::now();
    let expiry = now + std::time::Duration::from_secs(1);
    let timer = ManualDeadlineTimer::new(now, 1).unwrap();
    let mut deadline = timer.try_sleep_until(expiry).unwrap();
    let (queue, adapter, worker) = fixture(0);
    queue.try_enqueue(batch(10)).unwrap();
    queue.try_enqueue(batch(12)).unwrap();
    let mut workers = DestinationWorkers::new(std::num::NonZeroUsize::new(1).unwrap());
    assert!(workers.insert(worker).is_ok());
    let wakes = Arc::new(WakeCount::default());
    let waker = Waker::from(wakes.clone());
    let mut cx = Context::from_waker(&waker);
    assert!(workers.poll_shutdown(&mut cx, &mut deadline).is_pending());
    assert!(queue.try_enqueue(batch(14)).is_err());
    assert_eq!(adapter.calls.load(Ordering::SeqCst), 1);
    timer.advance_to(expiry).unwrap();
    assert!(wakes.0.load(Ordering::SeqCst) > 0);
    assert!(matches!(
        workers.poll_shutdown(&mut cx, &mut deadline),
        Poll::Ready(WorkerShutdownStep::DeadlineReached)
    ));
    assert_eq!(workers.snapshot().workers[0].1.queued_batches, 1);
    assert!(!workers.snapshot().is_drained());
    adapter.release();
    assert!(matches!(
        workers.poll_shutdown(&mut cx, &mut deadline),
        Poll::Ready(WorkerShutdownStep::DeadlineReached)
    ));
    drop(deadline);
    let mut resumed = timer
        .try_sleep_until(expiry + std::time::Duration::from_secs(1))
        .unwrap();
    let Poll::Ready(WorkerShutdownStep::Outcome(first)) =
        workers.poll_shutdown(&mut cx, &mut resumed)
    else {
        panic!("lost pending call")
    };
    assert_eq!(adapter.calls.load(Ordering::SeqCst), 1);
    let Poll::Ready(WorkerShutdownStep::Outcome(second)) =
        workers.poll_shutdown(&mut cx, &mut resumed)
    else {
        panic!("lost queued successor")
    };
    assert_eq!(second.payload.batch(), &batch(12));
    assert!(workers.poll_shutdown(&mut cx, &mut resumed).is_pending());
    drop((first, second));
    assert!(matches!(
        workers.poll_shutdown(&mut cx, &mut resumed),
        Poll::Ready(WorkerShutdownStep::Drained)
    ));
}

#[test]
fn destination_worker_shutdown_expiry_fences_new_adapter_calls() {
    use marketforge_engine::live::{
        foundation::deadline::ManualDeadlineTimer,
        routing::{
            admission::DeadlineTimer,
            worker::{DestinationWorkers, WorkerShutdownStep},
        },
    };
    let now = std::time::Instant::now();
    let timer = ManualDeadlineTimer::new(now, 1).unwrap();
    let mut deadline = timer.try_sleep_until(now).unwrap();
    let (queue, adapter, worker) = fixture(0);
    queue.try_enqueue(batch(10)).unwrap();
    let mut workers = DestinationWorkers::new(std::num::NonZeroUsize::new(1).unwrap());
    assert!(workers.insert(worker).is_ok());
    let waker = Waker::from(Arc::new(WakeCount::default()));
    let mut cx = Context::from_waker(&waker);
    assert!(matches!(
        workers.poll_shutdown(&mut cx, &mut deadline),
        Poll::Ready(WorkerShutdownStep::DeadlineReached)
    ));
    assert_eq!(adapter.calls.load(Ordering::SeqCst), 0);
    assert_eq!(workers.snapshot().workers[0].1.queued_batches, 1);
    let mut empty = DestinationWorkers::new(std::num::NonZeroUsize::new(1).unwrap());
    assert!(matches!(
        empty.poll_shutdown(&mut cx, &mut deadline),
        Poll::Ready(WorkerShutdownStep::Drained)
    ));
    assert!(empty.snapshot().is_drained());
}

fn retained_fixture(
    mode: u8,
) -> (
    DestinationWorker,
    marketforge_engine::live::routing::worker::WorkerOutcome,
    marketforge_engine::live::routing::acknowledgement::AcknowledgementTracker,
) {
    use marketforge_engine::live::{
        model::delivery::{DeliveryStage, DeliveryStatus},
        routing::acknowledgement::{
            AcknowledgementTracker, DeliveryAcknowledgement, DeliveryCursorRange,
            DeliveryObligation, DeliveryObligationId,
        },
    };
    let (queue, adapter, mut worker) = fixture(mode);
    let batch = batch(10);
    let obligation = DeliveryObligation {
        id: DeliveryObligationId(1),
        destination: DestinationId(7),
        range: DeliveryCursorRange::new(batch.first_cursor().clone(), batch.last_cursor().clone())
            .unwrap(),
        kind: DestinationKind::Recording,
        policy: DeliveryPolicy::Durable,
    };
    let mut tracker = AcknowledgementTracker::new(2).unwrap();
    tracker.register(obligation.clone()).unwrap();
    tracker
        .acknowledge(DeliveryAcknowledgement {
            obligation: obligation.id,
            destination: obligation.destination,
            range: obligation.range,
            status: DeliveryStatus::Progress(DeliveryStage::Queued),
        })
        .unwrap();
    queue.try_enqueue(batch).unwrap();
    adapter.release();
    let waker = Waker::from(Arc::new(WakeCount::default()));
    assert_eq!(
        worker.poll_next(&mut Context::from_waker(&waker)),
        Poll::Ready(WorkerStep::OutcomeReady)
    );
    let outcome = worker.take_outcome().unwrap();
    worker.close();
    (worker, outcome, tracker)
}

#[test]
fn retained_delivery_rejects_bad_correlation_and_preserves_capacity() {
    use marketforge_engine::live::{
        model::delivery::{DeliveryStage, DeliveryStatus},
        ports::routing::DestinationCompletion,
        routing::{
            acknowledgement::{AcknowledgementError, DeliveryObligationId},
            completion::{CompletionError, RetainedDelivery},
        },
    };
    let (worker, outcome, mut tracker) = retained_fixture(0);
    let (error, outcome) =
        RetainedDelivery::bind(outcome, DeliveryObligationId(999), &tracker).unwrap_err();
    assert_eq!(
        error,
        CompletionError::Tracking(AcknowledgementError::UnknownObligation)
    );
    assert!(!worker.snapshot().is_drained());
    let mut wrong = outcome.descriptor;
    wrong.policy = DeliveryPolicy::Required;
    let mut outcome = outcome;
    outcome.descriptor = wrong;
    let (error, mut outcome) =
        RetainedDelivery::bind(outcome, DeliveryObligationId(1), &tracker).unwrap_err();
    assert_eq!(error, CompletionError::DescriptorMismatch);
    outcome.descriptor.policy = DeliveryPolicy::Durable;
    let delivery = RetainedDelivery::bind(outcome, DeliveryObligationId(1), &tracker).unwrap();
    let mut completion = DestinationCompletion {
        destination: DestinationId(999),
        through: delivery.payload().batch().last_cursor().clone(),
        status: DeliveryStatus::Progress(DeliveryStage::Durable),
    };
    assert_eq!(
        delivery.apply_completion(&mut tracker, &completion),
        Err(CompletionError::DestinationMismatch)
    );
    completion.destination = DestinationId(7);
    completion.through.offset -= 1;
    assert_eq!(
        delivery.apply_completion(&mut tracker, &completion),
        Err(CompletionError::CursorMismatch)
    );
    completion.through.offset += 1;
    completion.status = DeliveryStatus::Pending;
    assert!(
        delivery
            .apply_completion(&mut tracker, &completion)
            .is_err()
    );
    assert_eq!(
        tracker.status(DeliveryObligationId(1)),
        Some(DeliveryStatus::Progress(DeliveryStage::Queued))
    );
    completion.status = DeliveryStatus::Progress(DeliveryStage::Committed);
    delivery
        .apply_completion(&mut tracker, &completion)
        .unwrap();
    completion.status = DeliveryStatus::Progress(DeliveryStage::Delivered);
    assert!(
        delivery
            .apply_completion(&mut tracker, &completion)
            .is_err()
    );
    assert!(!worker.snapshot().is_drained());
    // Ownership can move to a supervisor without changing any tracker state.
    let moved = delivery;
    assert_eq!(moved.obligation().id, DeliveryObligationId(1));
    assert_eq!(
        tracker.status(DeliveryObligationId(1)),
        Some(DeliveryStatus::Progress(DeliveryStage::Committed))
    );
}

#[test]
fn retained_delivery_rejection_and_full_journal_keep_recovery_payload() {
    use marketforge_engine::live::{
        model::delivery::DeliveryStatus,
        ports::routing::DestinationCompletion,
        routing::{
            acknowledgement::{DeliveryObligationId, FinalizationJournal, ObligationFinalization},
            completion::{CompletionError, RetainedDelivery},
        },
    };
    let (worker, outcome, mut tracker) = retained_fixture(1);
    let delivery = RetainedDelivery::bind(outcome, DeliveryObligationId(1), &tracker).unwrap();
    delivery.apply_admission(&mut tracker).unwrap();
    assert_eq!(
        tracker.status(DeliveryObligationId(1)),
        Some(DeliveryStatus::Failed(DeliveryFailure::Unavailable))
    );
    assert_eq!(
        delivery.apply_completion(
            &mut tracker,
            &DestinationCompletion {
                destination: DestinationId(7),
                through: delivery.payload().batch().last_cursor().clone(),
                status: DeliveryStatus::Failed(DeliveryFailure::Unavailable),
            }
        ),
        Err(CompletionError::AdmissionRejected)
    );
    let mut journal = FinalizationJournal::new(1).unwrap();
    // Fill the real journal using a different obligation, not a fake capacity flag.
    let mut other = delivery.obligation().clone();
    other.id = DeliveryObligationId(2);
    tracker.register(other.clone()).unwrap();
    tracker
        .acknowledge(
            marketforge_engine::live::routing::acknowledgement::DeliveryAcknowledgement {
                obligation: other.id,
                destination: other.destination,
                range: other.range,
                status: DeliveryStatus::Failed(DeliveryFailure::Rejected),
            },
        )
        .unwrap();
    tracker.finalize_into(other.id, &mut journal).unwrap();
    let (_, delivery) = delivery
        .finalize_into(&mut tracker, &mut journal)
        .unwrap_err();
    assert_eq!(tracker.len(), 1);
    assert!(!worker.snapshot().is_drained());
    // Supervisor can hand off to another bounded terminal journal without resubmission.
    let mut available = FinalizationJournal::new(1).unwrap();
    let finalized = delivery
        .finalize_into(&mut tracker, &mut available)
        .unwrap();
    assert!(matches!(
        finalized.outcome,
        ObligationFinalization::Failed {
            reason: DeliveryFailure::Unavailable,
            ..
        }
    ));
    assert!(tracker.is_empty());
    assert!(!worker.snapshot().is_drained());
    assert_eq!(finalized.payload.batch().first_cursor().offset, 10);
    drop(finalized);
    assert!(worker.snapshot().is_drained());
}

#[test]
fn retained_delivery_does_not_trust_forged_valid_admission_label() {
    use marketforge_engine::live::routing::{
        acknowledgement::DeliveryObligationId,
        completion::{CompletionError, RetainedDelivery},
    };
    for mode in [2, 3, 4] {
        let (worker, mut outcome, tracker) = retained_fixture(mode);
        let response = match outcome.admission {
            WorkerAdmissionResult::InvalidResponse(response) => response,
            _ => panic!("fixture must produce invalid correlation"),
        };
        outcome.admission = WorkerAdmissionResult::Valid(response);
        let (error, outcome) =
            RetainedDelivery::bind(outcome, DeliveryObligationId(1), &tracker).unwrap_err();
        assert_eq!(error, CompletionError::InvalidAdmission);
        assert!(!worker.snapshot().is_drained());
        drop(outcome);
        assert!(worker.snapshot().is_drained());
        assert_eq!(tracker.len(), 1);
    }
}

#[test]
fn pending_deliveries_correlate_complete_and_retain_failed_handoffs() {
    use marketforge_engine::live::{
        model::delivery::{DeliveryStage, DeliveryStatus},
        ports::routing::DestinationCompletion,
        routing::{
            acknowledgement::{DeliveryObligationId, FinalizationJournal},
            completion::{PendingDeliveries, PendingDeliveryError},
        },
    };
    let (worker, outcome, mut tracker) = retained_fixture(0);
    let mut pending = PendingDeliveries::new(std::num::NonZeroUsize::new(1).unwrap());
    let id = pending.ingest(outcome, &tracker).unwrap();
    assert_eq!(id, DeliveryObligationId(1));
    pending.apply_admission(id, &mut tracker).unwrap();
    assert_eq!(
        tracker.status(id),
        Some(DeliveryStatus::Progress(DeliveryStage::Queued))
    );
    let mut journal = FinalizationJournal::new(1).unwrap();
    assert!(
        pending
            .finalize_into(id, &mut tracker, &mut journal)
            .is_err()
    );
    assert_eq!(pending.len(), 1);
    assert!(!worker.snapshot().is_drained());
    let (other_worker, other_outcome, _) = retained_fixture(0);
    let (error, other_outcome) = pending.ingest(other_outcome, &tracker).unwrap_err();
    assert_eq!(error, PendingDeliveryError::Full);
    assert!(!other_worker.snapshot().is_drained());
    let mut completion = DestinationCompletion {
        destination: DestinationId(999),
        through: pending
            .get(id)
            .unwrap()
            .payload()
            .batch()
            .last_cursor()
            .clone(),
        status: DeliveryStatus::Progress(DeliveryStage::Durable),
    };
    assert_eq!(
        pending.apply_completion(&mut tracker, &completion),
        Err(PendingDeliveryError::UnknownObligation)
    );
    completion.destination = DestinationId(7);
    assert_eq!(pending.apply_completion(&mut tracker, &completion), Ok(id));
    completion.status = DeliveryStatus::Progress(DeliveryStage::Queued);
    assert!(pending.apply_completion(&mut tracker, &completion).is_err());
    assert_eq!(
        tracker.status(id),
        Some(DeliveryStatus::Progress(DeliveryStage::Durable))
    );
    let finalized = pending
        .finalize_into(id, &mut tracker, &mut journal)
        .unwrap();
    assert!(pending.is_empty());
    assert!(tracker.is_empty());
    assert_eq!(journal.len(), 1);
    assert!(!worker.snapshot().is_drained());
    drop(finalized);
    assert!(worker.snapshot().is_drained());
    drop(other_outcome);
    assert!(other_worker.snapshot().is_drained());
}

#[test]
fn pending_deliveries_reject_ambiguous_unknown_and_duplicate_obligations() {
    use marketforge_engine::live::routing::{
        acknowledgement::{AcknowledgementTracker, DeliveryObligationId},
        completion::{PendingDeliveries, PendingDeliveryError},
    };
    let (worker, outcome, mut tracker) = retained_fixture(0);
    let mut pending = PendingDeliveries::new(std::num::NonZeroUsize::new(2).unwrap());
    let empty = AcknowledgementTracker::new(1).unwrap();
    let (error, outcome) = pending.ingest(outcome, &empty).unwrap_err();
    assert_eq!(error, PendingDeliveryError::UnknownObligation);
    assert!(pending.is_empty());
    assert!(!worker.snapshot().is_drained());
    let mut duplicate = tracker.obligation(DeliveryObligationId(1)).unwrap().clone();
    duplicate.id = DeliveryObligationId(2);
    tracker.register(duplicate).unwrap();
    let (error, outcome) = pending.ingest(outcome, &tracker).unwrap_err();
    assert_eq!(error, PendingDeliveryError::AmbiguousObligation);
    let (_, second, clean_tracker) = retained_fixture(0);
    let id = pending.ingest(outcome, &clean_tracker).unwrap();
    let (error, second) = pending.ingest(second, &clean_tracker).unwrap_err();
    assert_eq!(error, PendingDeliveryError::DuplicateObligation);
    assert_eq!(pending.len(), 1);
    assert!(
        pending
            .take_for_recovery(DeliveryObligationId(999))
            .is_none()
    );
    let retained = pending.take_for_recovery(id).unwrap();
    assert!(pending.is_empty());
    assert_eq!(clean_tracker.len(), 1);
    assert!(!worker.snapshot().is_drained());
    drop(retained);
    assert!(worker.snapshot().is_drained());
    drop(second);
}

#[test]
fn pending_deliveries_rejection_and_full_journal_preserve_recovery_ownership() {
    use marketforge_engine::live::{
        model::delivery::DeliveryStatus,
        routing::{
            acknowledgement::{DeliveryAcknowledgement, DeliveryObligationId, FinalizationJournal},
            completion::PendingDeliveries,
        },
    };
    let (worker, outcome, mut tracker) = retained_fixture(1);
    let mut pending = PendingDeliveries::new(std::num::NonZeroUsize::new(1).unwrap());
    let id = pending.ingest(outcome, &tracker).unwrap();
    pending.apply_admission(id, &mut tracker).unwrap();
    let mut other = tracker.obligation(id).unwrap().clone();
    other.id = DeliveryObligationId(2);
    tracker.register(other.clone()).unwrap();
    tracker
        .acknowledge(DeliveryAcknowledgement {
            obligation: other.id,
            destination: other.destination,
            range: other.range,
            status: DeliveryStatus::Failed(DeliveryFailure::Rejected),
        })
        .unwrap();
    let mut full = FinalizationJournal::new(1).unwrap();
    tracker.finalize_into(other.id, &mut full).unwrap();
    assert!(pending.finalize_next_into(&mut tracker, &mut full).is_err());
    assert_eq!(pending.len(), 1);
    assert_eq!(
        tracker.status(id),
        Some(DeliveryStatus::Failed(DeliveryFailure::Unavailable))
    );
    assert!(!worker.snapshot().is_drained());
    let mut available = FinalizationJournal::new(1).unwrap();
    let finalized = pending
        .finalize_next_into(&mut tracker, &mut available)
        .unwrap()
        .unwrap();
    assert!(pending.is_empty());
    assert!(tracker.is_empty());
    assert!(!worker.snapshot().is_drained());
    drop(finalized);
    assert!(worker.snapshot().is_drained());
}

#[test]
fn pending_deliveries_scan_skips_incomplete_and_transfers_terminal_payload() {
    use marketforge_engine::live::{
        model::delivery::DeliveryStatus,
        routing::{
            acknowledgement::{DeliveryAcknowledgement, DeliveryObligationId, FinalizationJournal},
            completion::PendingDeliveries,
        },
    };
    let (worker, first, mut tracker) = retained_fixture(0);
    let (other_worker, mut second, _) = retained_fixture(0);
    let mut obligation = tracker.obligation(DeliveryObligationId(1)).unwrap().clone();
    obligation.id = DeliveryObligationId(2);
    obligation.destination = DestinationId(8);
    second.descriptor.id = obligation.destination;
    if let WorkerAdmissionResult::Valid(DestinationAdmission::Accepted { destination, .. }) =
        &mut second.admission
    {
        *destination = obligation.destination;
    } else {
        panic!("fixture must accept admission");
    }
    tracker.register(obligation.clone()).unwrap();
    let mut pending = PendingDeliveries::new(std::num::NonZeroUsize::new(2).unwrap());
    let first_id = pending.ingest(first, &tracker).unwrap();
    let second_id = pending.ingest(second, &tracker).unwrap();
    let mut journal = FinalizationJournal::new(1).unwrap();
    assert!(
        pending
            .finalize_next_into(&mut tracker, &mut journal)
            .unwrap()
            .is_none()
    );
    tracker
        .acknowledge(DeliveryAcknowledgement {
            obligation: second_id,
            destination: obligation.destination,
            range: obligation.range,
            status: DeliveryStatus::Failed(DeliveryFailure::Unavailable),
        })
        .unwrap();
    let terminal = pending
        .finalize_next_into(&mut tracker, &mut journal)
        .unwrap()
        .unwrap();
    assert_eq!(pending.len(), 1);
    assert!(pending.get(first_id).is_some());
    assert!(pending.get(second_id).is_none());
    assert_eq!(tracker.len(), 1);
    assert!(!other_worker.snapshot().is_drained());
    drop(terminal);
    assert!(other_worker.snapshot().is_drained());
    assert!(!worker.snapshot().is_drained());
    assert!(
        pending
            .finalize_next_into(&mut tracker, &mut journal)
            .unwrap()
            .is_none()
    );
}
