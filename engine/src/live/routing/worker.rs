//! Single-consumer destination admission driver. Scheduling belongs to the runtime.
//!
//! A pending adapter future and its charged payload remain owned here when the
//! caller stops polling (including cancellation/deadline selection). Resuming
//! polling continues the same operation; it never resubmits an ambiguous call.
use super::queue::{DestinationQueue, QueueWorkerLease, QueuedBatch};
use crate::live::ports::routing::{
    DestinationAdmission, DestinationDescriptor, EventDestinationPort, RoutingFuture,
};
use std::{
    sync::Arc,
    task::{Context, Poll},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerCreationError {
    CapacityMismatch,
    AlreadyClaimed,
}

/// Admission responses are retained verbatim; invalid correlation is not success.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkerAdmissionResult {
    Valid(DestinationAdmission),
    InvalidResponse(DestinationAdmission),
}

/// Transfers payload ownership and its capacity charge to the outcome consumer.
/// Adapter acceptance is not delivery, persistence, durability or finalization.
#[derive(Debug)]
#[must_use = "retain payload and correlate the outcome before releasing responsibility"]
pub struct WorkerOutcome {
    pub descriptor: DestinationDescriptor,
    pub payload: QueuedBatch,
    pub admission: WorkerAdmissionResult,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerStep {
    Idle,
    OutcomeReady,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkerSnapshot {
    pub in_flight: bool,
    pub outcome_pending: bool,
    pub queued_batches: usize,
    pub closed: bool,
    /// Includes queued, reserved, in-flight and transferred outcome payloads.
    pub occupied_events: usize,
    pub occupied_bytes: u64,
}
impl WorkerSnapshot {
    pub fn is_drained(&self) -> bool {
        !self.in_flight
            && !self.outcome_pending
            && self.queued_batches == 0
            && self.occupied_events == 0
            && self.occupied_bytes == 0
    }
}

/// Owns at most one adapter call and one payload. No tasks, retries or hidden queues.
/// Keep this owner alive until drain or explicit supervisor handoff; dropping it
/// abandons its in-process operation and is not a successful shutdown.
pub struct DestinationWorker {
    destination: Arc<dyn EventDestinationPort>,
    descriptor: DestinationDescriptor,
    queue: DestinationQueue,
    // Drop the adapter future before releasing its payload's capacity charge.
    operation: Option<RoutingFuture<'static, DestinationAdmission>>,
    payload: Option<QueuedBatch>,
    outcome: Option<WorkerAdmissionResult>,
    // Release dequeue authority only after the old operation has been dropped.
    lease: QueueWorkerLease,
}
impl DestinationWorker {
    pub fn new(
        queue: DestinationQueue,
        destination: Arc<dyn EventDestinationPort>,
    ) -> Result<Self, WorkerCreationError> {
        let descriptor = destination.descriptor();
        Self::from_registered(queue, destination, descriptor)
    }

    /// Uses the descriptor frozen by registration, without invoking adapter code
    /// while the registry's membership fence is held.
    pub(crate) fn from_registered(
        queue: DestinationQueue,
        destination: Arc<dyn EventDestinationPort>,
        descriptor: DestinationDescriptor,
    ) -> Result<Self, WorkerCreationError> {
        if queue.capacity().limits() != descriptor.capacity {
            return Err(WorkerCreationError::CapacityMismatch);
        }
        let lease = queue
            .claim_worker()
            .ok_or(WorkerCreationError::AlreadyClaimed)?;
        Ok(Self {
            destination,
            descriptor,
            queue,
            lease,
            payload: None,
            operation: None,
            outcome: None,
        })
    }

    /// Poll one admission. The completed outcome blocks further dequeue until
    /// explicitly transferred, so a slow outcome consumer applies bounded pressure.
    /// Polling never waits for a new queue item; Idle requires runtime rescheduling.
    pub fn poll_admission(&mut self, cx: &mut Context<'_>) -> Poll<WorkerStep> {
        self.poll_admission_inner(cx, false)
    }

    /// Await queue activity and adapter progress. Empty closed queues return Idle;
    /// transferred payload charges still prevent full drain.
    pub fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<WorkerStep> {
        self.poll_admission_inner(cx, true)
    }

    fn poll_admission_inner(&mut self, cx: &mut Context<'_>, wait: bool) -> Poll<WorkerStep> {
        if self.outcome.is_some() {
            return Poll::Ready(WorkerStep::OutcomeReady);
        }
        if self.payload.is_none() {
            let next = if wait {
                match self.lease.poll_dequeue(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(next) => next,
                }
            } else {
                self.lease.dequeue()
            };
            let Some(payload) = next else {
                return Poll::Ready(WorkerStep::Idle);
            };
            let destination = self.destination.clone();
            let batch = payload.batch().clone();
            // Store before polling: caller cancellation cannot destroy this call.
            self.payload = Some(payload);
            self.operation = Some(Box::pin(async move { destination.admit(batch).await }));
        }
        let response = match self
            .operation
            .as_mut()
            .expect("active payload has operation")
            .as_mut()
            .poll(cx)
        {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(response) => response,
        };
        self.operation = None;
        let valid = match &response {
            DestinationAdmission::Accepted {
                destination,
                through,
            } => {
                *destination == self.descriptor.id
                    && through == self.payload.as_ref().unwrap().batch().last_cursor()
            }
            DestinationAdmission::Rejected { destination, .. } => {
                *destination == self.descriptor.id
            }
        };
        self.outcome = Some(if valid {
            WorkerAdmissionResult::Valid(response)
        } else {
            WorkerAdmissionResult::InvalidResponse(response)
        });
        Poll::Ready(WorkerStep::OutcomeReady)
    }

    pub fn take_outcome(&mut self) -> Option<WorkerOutcome> {
        let admission = self.outcome.take()?;
        Some(WorkerOutcome {
            descriptor: self.descriptor,
            payload: self.payload.take().expect("outcome owns payload"),
            admission,
        })
    }

    /// Stop new queue admission, retaining queued and in-flight obligations.
    pub fn close(&self) {
        self.queue.shutdown();
    }

    pub fn snapshot(&self) -> WorkerSnapshot {
        let observation = self.queue.observe();
        let queue = observation.queue;
        WorkerSnapshot {
            in_flight: self.operation.is_some(),
            outcome_pending: self.outcome.is_some(),
            queued_batches: queue.queued_batches,
            closed: queue.closed,
            occupied_events: observation.capacity.occupied_events,
            occupied_bytes: observation.capacity.occupied_bytes,
        }
    }
}

/// Bounded runtime scheduling owner for exclusive destination workers.
/// Each poll visits at most `limit` workers and transfers at most one outcome.
/// Slow adapters retain their operation while unrelated workers make progress.
/// The supervisor must correlate and retain returned outcomes; this owner does
/// not turn destination admission into completion or durability evidence.
pub struct DestinationWorkers {
    workers: Vec<DestinationWorker>,
    limit: usize,
    next: usize,
    closed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerRegistrationError {
    Closed,
    Full,
    DuplicateDestination,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkersSnapshot {
    pub closed: bool,
    pub limit: usize,
    pub workers: Vec<(crate::live::ports::routing::DestinationId, WorkerSnapshot)>,
}
impl WorkersSnapshot {
    /// Includes charges transferred to outcome consumers. Does not establish
    /// that independent delivery obligations or tracking recovery are drained.
    pub fn is_drained(&self) -> bool {
        self.closed && self.workers.iter().all(|(_, worker)| worker.is_drained())
    }
}
/// A shutdown step never discards workers or transferred payload responsibility.
/// Drained covers worker resources only, not independent delivery obligations.
#[derive(Debug)]
#[must_use]
pub enum WorkerShutdownStep {
    Outcome(WorkerOutcome),
    Drained,
    DeadlineReached,
}

impl DestinationWorkers {
    pub fn new(limit: std::num::NonZeroUsize) -> Self {
        Self {
            workers: Vec::new(),
            limit: limit.get(),
            next: 0,
            closed: false,
        }
    }

    /// Failure returns the complete owner, including any pending adapter call.
    /// Closed retired instances may coexist with an independent same-ID replacement.
    /// Two open instances with the same ID remain a registration error; queue
    /// leases independently enforce one consumer per actual queue.
    #[allow(clippy::result_large_err)]
    pub fn insert(
        &mut self,
        worker: DestinationWorker,
    ) -> Result<(), (WorkerRegistrationError, DestinationWorker)> {
        let error = if self.closed {
            Some(WorkerRegistrationError::Closed)
        } else if self.workers.iter().any(|existing| {
            existing.descriptor.id == worker.descriptor.id
                && (existing.queue.same_owner(&worker.queue)
                    || (!existing.snapshot().closed && !worker.snapshot().closed))
        }) {
            Some(WorkerRegistrationError::DuplicateDestination)
        } else if self.workers.len() == self.limit {
            Some(WorkerRegistrationError::Full)
        } else {
            None
        };
        if let Some(error) = error {
            return Err((error, worker));
        }
        self.workers.push(worker);
        Ok(())
    }

    /// Round-robin polling prevents a continuously ready destination starving
    /// another. Pending calls and queue waiters receive the supervisor's waker.
    /// Cancelling a polling future leaves every operation owned by this set.
    pub fn poll_outcome(&mut self, cx: &mut Context<'_>) -> Poll<WorkerOutcome> {
        let count = self.workers.len();
        for _ in 0..count {
            let index = self.next;
            self.next = (index + 1) % count;
            if self.workers[index].poll_next(cx) == Poll::Ready(WorkerStep::OutcomeReady) {
                return Poll::Ready(self.workers[index].take_outcome().expect("ready outcome"));
            }
        }
        Poll::Pending
    }

    /// Fence admission and drive bounded drain until the supplied deadline fires.
    /// Keep the same deadline future across polls. Its waker must fire at expiry;
    /// use the bounded DeadlineTimer port to register it before entering shutdown.
    /// At expiry no new adapter call is started. The entire scheduler remains
    /// owned by the caller for observation, recovery or later resumed draining.
    /// Returned outcomes retain capacity and must be correlated by the supervisor.
    /// External payload release need not wake this call; poll again after handling
    /// an outcome. The deadline guarantees wakeup even if external charges remain.
    pub fn poll_shutdown(
        &mut self,
        cx: &mut Context<'_>,
        deadline: &mut super::admission::DeadlineFuture,
    ) -> Poll<WorkerShutdownStep> {
        if !self.closed {
            self.close();
        }
        if self
            .workers
            .iter()
            .all(|worker| worker.snapshot().is_drained())
        {
            return Poll::Ready(WorkerShutdownStep::Drained);
        }
        if deadline.as_mut().poll(cx).is_ready() {
            return Poll::Ready(WorkerShutdownStep::DeadlineReached);
        }
        match self.poll_outcome(cx) {
            Poll::Ready(outcome) => Poll::Ready(WorkerShutdownStep::Outcome(outcome)),
            Poll::Pending => Poll::Pending,
        }
    }

    /// Establish the admission fence for all owned queues. Continue polling to
    /// drain; a deadline must return this owner to its supervisor, not drop it.
    pub fn close(&mut self) {
        self.closed = true;
        for worker in &self.workers {
            worker.close();
        }
    }

    /// Retire only a closed, fully drained worker. In-flight operations, queued
    /// batches and externally retained payload charges prevent removal.
    pub fn remove_drained(
        &mut self,
        id: crate::live::ports::routing::DestinationId,
    ) -> Option<DestinationWorker> {
        let index = self.workers.iter().position(|worker| {
            let snapshot = worker.snapshot();
            worker.descriptor.id == id && snapshot.closed && snapshot.is_drained()
        })?;
        let worker = self.workers.remove(index);
        // Preserve the next worker's identity when an earlier slot is removed.
        if index < self.next {
            self.next -= 1;
        }
        if self.next >= self.workers.len() {
            self.next = 0;
        }
        Some(worker)
    }

    pub fn snapshot(&self) -> WorkersSnapshot {
        WorkersSnapshot {
            closed: self.closed,
            limit: self.limit,
            workers: self
                .workers
                .iter()
                .map(|worker| (worker.descriptor.id, worker.snapshot()))
                .collect(),
        }
    }
}
