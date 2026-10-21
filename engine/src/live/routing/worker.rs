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
        if self.outcome.is_some() {
            return Poll::Ready(WorkerStep::OutcomeReady);
        }
        if self.payload.is_none() {
            let Some(payload) = self.lease.dequeue() else {
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
