//! Bounded FIFO destination queue.
//!
//! Each queued batch owns its capacity permit.
//!
//! Queue insertion is the admission commitment boundary.
//! Cancellation after successful insertion cannot revoke admission.
//!
//! This module does not establish transport delivery, persistence
//! commitment, or crash durability.
use crate::live::model::memory::RetainedSize;
use crate::live::routing::admission::{
    AdmissionContext, AdmissionWait, AdmissionWaitError, AdmissionWaitResult, DeadlineTimer,
};
use crate::live::routing::destination::CapacityReservation;
use crate::live::{
    ports::routing::{EventBatch, RoutingAdmissionFailure},
    routing::destination::{CapacityError, DestinationCapacity, OccupiedCapacity},
};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueError {
    Closed,
    Capacity(CapacityError),
    MemoryAccountingOverflow,
    ReservationMismatch,
}
/// Result of waiting for destination capacity.
///
/// This operation never takes ownership of an EventBatch.
#[derive(Debug)]
pub enum AsyncCapacityAdmission {
    Reserved(CapacityReservation),
    Rejected(AsyncQueueAdmissionError),
}

/// Rejected admission retains ownership of the original batch.
#[derive(Debug)]
pub struct QueueRejection {
    pub batch: EventBatch,
    pub reason: QueueError,
}
/// Failed reserved admission retains both owned resources.
#[derive(Debug)]
pub struct ReservedQueueRejection {
    pub batch: EventBatch,
    pub reservation: CapacityReservation,
    pub reason: QueueError,
}

/// A batch and its capacity permit.
///
/// Dropping this object releases the occupied capacity.
#[derive(Debug)]
pub struct QueuedBatch {
    batch: EventBatch,
    permit: OccupiedCapacity,
}

impl QueuedBatch {
    pub fn batch(&self) -> &EventBatch {
        &self.batch
    }

    pub fn events(&self) -> usize {
        self.permit.events()
    }

    pub fn bytes(&self) -> u64 {
        self.permit.bytes()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueueSnapshot {
    pub queued_batches: usize,
    pub queued_events: usize,
    pub queued_bytes: u64,
    pub closed: bool,
}

/// One queue observation taken with insertion/dequeue excluded. Capacity includes
/// reservations and consumer-held batches, so it must not be called queue depth.
/// Independent capacity releases may occur immediately after this observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueueObservation {
    pub queue: QueueSnapshot,
    pub capacity: super::destination::CapacityUsage,
}

#[derive(Debug, Default)]
struct QueueState {
    batches: VecDeque<QueuedBatch>,
    closed: bool,
    worker_claimed: bool,
}

#[derive(Debug)]
struct QueueInner {
    state: Mutex<QueueState>,
    capacity: DestinationCapacity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AsyncQueueAdmissionError {
    Cancelled,
    DeadlineExceeded,
    Capacity(CapacityError),
    Queue(QueueError),
    MissingTimer,
}

impl From<RoutingAdmissionFailure> for AsyncQueueAdmissionError {
    fn from(value: RoutingAdmissionFailure) -> Self {
        match value {
            RoutingAdmissionFailure::Cancelled => Self::Cancelled,
            RoutingAdmissionFailure::DeadlineExceeded => Self::DeadlineExceeded,
            RoutingAdmissionFailure::ShuttingDown => Self::Capacity(CapacityError::ShuttingDown),
            RoutingAdmissionFailure::CapacityExceeded
            | RoutingAdmissionFailure::NoRequiredCapacity => Self::Capacity(CapacityError::Full),
            RoutingAdmissionFailure::InvalidCursor => Self::Queue(QueueError::ReservationMismatch),
        }
    }
}

impl From<AdmissionWaitError> for AsyncQueueAdmissionError {
    fn from(value: AdmissionWaitError) -> Self {
        match value {
            AdmissionWaitError::MissingTimer => Self::MissingTimer,
            AdmissionWaitError::TimerCapacity | AdmissionWaitError::CancellationCapacity => {
                Self::Capacity(CapacityError::Full)
            }
        }
    }
}

/// Shared bounded destination queue.
///
/// Clones reference the same queue and capacity manager.
#[derive(Debug, Clone)]
pub struct DestinationQueue {
    inner: Arc<QueueInner>,
}

/// Exclusive dequeue authority held by one destination worker.
pub(crate) struct QueueWorkerLease {
    queue: DestinationQueue,
}

impl QueueWorkerLease {
    pub(crate) fn dequeue(&self) -> Option<QueuedBatch> {
        self.queue
            .inner
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .batches
            .pop_front()
    }
}

impl Drop for QueueWorkerLease {
    fn drop(&mut self) {
        self.queue
            .inner
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .worker_claimed = false;
    }
}

impl DestinationQueue {
    pub(crate) fn claim_worker(&self) -> Option<QueueWorkerLease> {
        let mut state = self.inner.state.lock().unwrap_or_else(|p| p.into_inner());
        if state.worker_claimed {
            return None;
        }
        state.worker_claimed = true;
        Some(QueueWorkerLease {
            queue: self.clone(),
        })
    }
    /// Queue ownership identity, independent of equal capacity settings.
    pub(crate) fn same_owner(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }

    pub fn new(capacity: DestinationCapacity) -> Self {
        Self {
            inner: Arc::new(QueueInner {
                state: Mutex::new(QueueState::default()),
                capacity,
            }),
        }
    }
    /// Wait for enough capacity to admit a batch.
    ///
    /// The caller retains ownership of the batch throughout this operation.
    ///
    /// Dropping the future releases its waiter registration and any
    /// acquired reservation, but cannot destroy the caller's event batch.
    pub async fn reserve_async(
        &self,
        batch: &EventBatch,
        context: AdmissionContext,
        timer: Option<&dyn DeadlineTimer>,
    ) -> AsyncCapacityAdmission {
        let events = batch.len();

        let bytes = match batch.retained_size_bytes() {
            Ok(bytes) => bytes,
            Err(_) => {
                return AsyncCapacityAdmission::Rejected(AsyncQueueAdmissionError::Queue(
                    QueueError::MemoryAccountingOverflow,
                ));
            }
        };

        let capacity_future = self.inner.capacity.reserve(events, bytes);

        let waiting = match AdmissionWait::new(capacity_future, context.clone(), timer) {
            Ok(waiting) => waiting,
            Err(error) => {
                return AsyncCapacityAdmission::Rejected(error.into());
            }
        };

        let reservation = match waiting.await {
            AdmissionWaitResult::Ready(Ok(reservation)) => reservation,

            AdmissionWaitResult::Ready(Err(error)) => {
                return AsyncCapacityAdmission::Rejected(AsyncQueueAdmissionError::Capacity(error));
            }

            AdmissionWaitResult::Rejected(reason) => {
                return AsyncCapacityAdmission::Rejected(reason.into());
            }
        };

        // Capacity is reserved, but the event has not been admitted.
        // Cancellation remains valid until synchronous commitment.
        if context.cancellation.is_cancelled() {
            return AsyncCapacityAdmission::Rejected(AsyncQueueAdmissionError::Cancelled);
        }

        if let Some(deadline) = context.deadline {
            if std::time::Instant::now() >= deadline {
                return AsyncCapacityAdmission::Rejected(
                    AsyncQueueAdmissionError::DeadlineExceeded,
                );
            }
        }

        AsyncCapacityAdmission::Reserved(reservation)
    }
    /// Commit a previously acquired capacity reservation.
    ///
    /// The reservation must belong to this queue's capacity manager.
    /// On rejection, ownership of both batch and reservation is returned.
    ///
    /// This method does not wait or perform cancellation checks.
    pub fn enqueue_reserved(
        &self,
        batch: EventBatch,
        reservation: CapacityReservation,
    ) -> Result<(), ReservedQueueRejection> {
        // Verify that the reservation belongs to this queue.
        if !reservation.belongs_to(&self.inner.capacity) {
            return Err(ReservedQueueRejection {
                batch,
                reservation,
                reason: QueueError::ReservationMismatch,
            });
        }

        let expected_events = batch.len();

        let expected_bytes = match batch.retained_size_bytes() {
            Ok(bytes) => bytes,
            Err(_) => {
                return Err(ReservedQueueRejection {
                    batch,
                    reservation,
                    reason: QueueError::MemoryAccountingOverflow,
                });
            }
        };

        if reservation.events() != expected_events || reservation.bytes() != expected_bytes {
            return Err(ReservedQueueRejection {
                batch,
                reservation,
                reason: QueueError::ReservationMismatch,
            });
        }

        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        if state.closed || self.inner.capacity.usage().shutting_down {
            return Err(ReservedQueueRejection {
                batch,
                reservation,
                reason: QueueError::Closed,
            });
        }

        // The queue now owns the batch and occupied permit.
        // Cancellation is not consulted after this boundary.
        state.batches.push_back(QueuedBatch {
            batch,
            permit: reservation.commit(),
        });

        Ok(())
    }
    /// Attempt immediate admission.
    ///
    /// The caller must supply the retained-size estimate for the
    /// batch. Byte accounting is based on this supplied value.
    ///
    /// A successful return means the queue owns the batch.
    /// Attempt immediate admission.
    ///
    /// The queue calculates the retained-memory charge itself.
    ///
    /// Successful insertion transfers ownership of both the batch
    /// and its capacity permit.
    pub fn try_enqueue(&self, batch: EventBatch) -> Result<(), QueueRejection> {
        let events = batch.len();

        let bytes = match batch.retained_size_bytes() {
            Ok(bytes) => bytes,

            Err(_) => {
                return Err(QueueRejection {
                    batch,
                    reason: QueueError::MemoryAccountingOverflow,
                });
            }
        };

        // Lock ordering: queue -> capacity.
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        if state.closed {
            return Err(QueueRejection {
                batch,
                reason: QueueError::Closed,
            });
        }

        let reservation = match self.inner.capacity.try_reserve(events, bytes) {
            Ok(reservation) => reservation,

            Err(error) => {
                return Err(QueueRejection {
                    batch,
                    reason: QueueError::Capacity(error),
                });
            }
        };

        let permit = reservation.commit();

        // Queue insertion is the admission commitment boundary.
        state.batches.push_back(QueuedBatch { batch, permit });

        Ok(())
    }

    /// Remove the oldest queued batch.
    /// Returns None while an exclusive destination worker owns dequeue authority.
    ///
    /// Ownership transfers to the consumer.
    ///
    /// Capacity remains occupied until the returned batch
    /// is dropped, including while the consumer processes it.
    pub fn try_dequeue(&self) -> Option<QueuedBatch> {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        if state.worker_claimed {
            return None;
        }
        state.batches.pop_front()
    }

    /// Stop accepting new batches.
    ///
    /// Already queued batches remain available for draining.
    pub fn close(&self) {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        state.closed = true;
    }

    /// Shut down the queue and its capacity manager.
    ///
    /// Outstanding batches remain owned by the queue or consumers.
    /// The caller must explicitly drain or discard them.
    pub fn shutdown(&self) {
        self.close();
        self.inner.capacity.shutdown();
    }

    /// Discard queued batches and release their permits.
    ///
    /// This is a destructive operation. Required and durable
    /// destinations must account for discarded obligations before
    /// invoking it.
    ///
    /// Batches already dequeued by consumers are unaffected.
    pub fn discard_queued(&self) -> usize {
        let batches = {
            let mut state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());

            std::mem::take(&mut state.batches)
        };

        let discarded = batches.len();

        // Drop permits outside the queue mutex.
        drop(batches);

        discarded
    }

    pub fn snapshot(&self) -> QueueSnapshot {
        self.observe().queue
    }

    pub fn observe(&self) -> QueueObservation {
        let state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        let mut queued_events = 0;
        let mut queued_bytes = 0;

        for item in &state.batches {
            queued_events += item.events();
            queued_bytes += item.bytes();
        }

        QueueObservation {
            queue: QueueSnapshot {
                queued_batches: state.batches.len(),
                queued_events,
                queued_bytes,
                closed: state.closed,
            },
            // Preserve insertion's queue -> capacity lock order.
            capacity: self.inner.capacity.usage(),
        }
    }

    pub fn capacity(&self) -> &DestinationCapacity {
        &self.inner.capacity
    }
}
