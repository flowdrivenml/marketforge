//! Two-phase destination admission transaction.
//!
//! Waiting and capacity reservation are cancellable.
//! Queue commitment is synchronous and irreversible.
//!
//! The caller retains ownership of the event batch until commit.

use std::time::Instant;

use crate::live::{
    ports::routing::EventBatch,
    routing::{
        admission::{AdmissionContext, DeadlineTimer},
        destination::CapacityReservation,
        queue::{AsyncCapacityAdmission, AsyncQueueAdmissionError, DestinationQueue, QueueError},
    },
};

/// An admission transaction holding reserved destination capacity.
///
/// Dropping the transaction releases its reservation.
///
/// The transaction does not own the event batch.
#[derive(Debug)]
pub struct AdmissionTransaction {
    reservation: CapacityReservation,
    context: AdmissionContext,
}

/// Failure during synchronous transaction commitment.
///
/// The original event batch is always returned on rejection.
#[derive(Debug)]
pub struct TransactionRejection {
    pub batch: EventBatch,
    pub reason: TransactionError,
}

/// Transaction-level rejection classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionError {
    Cancelled,
    DeadlineExceeded,
    Admission(AsyncQueueAdmissionError),
    Queue(QueueError),
}

impl AdmissionTransaction {
    /// Prepare an admission transaction without consuming the batch.
    ///
    /// Cancellation or deadline expiration during waiting prevents
    /// the creation of a transaction.
    pub async fn prepare(
        queue: &DestinationQueue,
        batch: &EventBatch,
        context: AdmissionContext,
        timer: Option<&dyn DeadlineTimer>,
    ) -> Result<Self, TransactionError> {
        match queue.reserve_async(batch, context.clone(), timer).await {
            AsyncCapacityAdmission::Reserved(reservation) => Ok(Self {
                reservation,
                context,
            }),

            AsyncCapacityAdmission::Rejected(reason) => Err(TransactionError::Admission(reason)),
        }
    }

    /// Commit the event batch into its destination queue.
    ///
    /// No asynchronous suspension occurs during commitment.
    ///
    /// Once the queue accepts the batch, cancellation cannot
    /// retroactively change the outcome.
    pub fn commit(
        self,
        queue: &DestinationQueue,
        batch: EventBatch,
    ) -> Result<(), TransactionRejection> {
        if self.context.cancellation.is_cancelled() {
            return Err(TransactionRejection {
                batch,
                reason: TransactionError::Cancelled,
            });
        }

        if let Some(deadline) = self.context.deadline {
            if Instant::now() >= deadline {
                return Err(TransactionRejection {
                    batch,
                    reason: TransactionError::DeadlineExceeded,
                });
            }
        }

        match queue.enqueue_reserved(batch, self.reservation) {
            Ok(()) => Ok(()),

            Err(rejection) => Err(TransactionRejection {
                batch: rejection.batch,
                reason: TransactionError::Queue(rejection.reason),
            }),
        }
    }
}
