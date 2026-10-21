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
    let capacity = DeliveryCapacity::new(4, 100_000).unwrap();
    let queue = DestinationQueue::new(DestinationCapacity::new(capacity, 1).unwrap());
    let adapter = Arc::new(Adapter {
        descriptor: DestinationDescriptor {
            id: DestinationId(7),
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
    drop(outcome);
    assert!(worker.snapshot().is_drained());
    assert_eq!(tracker.len(), 1); // runtime must still own completion responsibility
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
