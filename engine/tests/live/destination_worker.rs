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
