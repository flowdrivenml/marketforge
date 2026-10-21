//! Producer port over contextual ingress and mandatory bounded outcome ownership.
//!
//! Only complete required queue admission (or a completed optional-only attempt)
//! yields Accepted. Partial/repair outcomes remain in the journal and yield a
//! conservative rejection. Rejection is not rollback: producers must reconcile
//! retained outcomes before retrying an identity whose fan-out already started.
use std::time::Duration;

use super::{
    admission::AdmissionContext,
    cursor::CursorOrderError,
    ingress::{IngressError, IngressOutcome, RouterIngress},
    ingress_reporting::{IngressOutcomes, RecordedIngressError},
    namespaces::NamespaceError,
    ordered::{
        OptionalOrderedError, OptionalOrderedOutcome, OrderedCommitFailure, OrderedCommitOutcome,
        OrderedPreparationError,
    },
    queue::AsyncQueueAdmissionError,
    router::{
        BestEffortError, FanoutPreparationError, RouterRegistryError, TrackedFanoutError,
        TrackedFanoutResult,
    },
};
use crate::live::{
    foundation::cancellation::CancellationToken,
    model::error::ErrorCode,
    ports::routing::{
        EventBatch, EventRouterPort, RoutingAdmission, RoutingAdmissionFailure, RoutingFuture,
    },
};

/// Shares all mutable owners with the runtime. The cancellation token fences
/// producer waits; runtime delivery/shutdown remains separately owned. Timeout
/// is measured anew for each call using the authoritative ingress clock.
pub struct RouterPort<'a> {
    pub ingress: RouterIngress<'a>,
    pub outcomes: &'a IngressOutcomes,
    pub cancellation: CancellationToken,
    pub timeout: Option<Duration>,
}

impl EventRouterPort for RouterPort<'_> {
    fn admit<'a>(&'a self, batch: EventBatch) -> RoutingFuture<'a, RoutingAdmission> {
        Box::pin(async move {
            let deadline = match self.timeout {
                Some(timeout) => match self.ingress.clock.now().checked_add(timeout) {
                    Some(deadline) => Some(deadline),
                    None => return rejected(RoutingAdmissionFailure::DeadlineExceeded),
                },
                None => None,
            };
            match self
                .outcomes
                .admit_receipt(
                    &self.ingress,
                    batch,
                    AdmissionContext::new(self.cancellation.clone(), deadline),
                )
                .await
            {
                Ok(receipt) => receipt,
                Err(RecordedIngressError::Capacity { reason, .. }) => rejected(match reason {
                    ErrorCode::Unavailable => RoutingAdmissionFailure::ShuttingDown,
                    _ => RoutingAdmissionFailure::CapacityExceeded,
                }),
                Err(RecordedIngressError::Admission(error)) => rejected(match error.reason {
                    IngressError::Preparation(error) => preparation(error),
                    IngressError::DriverClosed => RoutingAdmissionFailure::ShuttingDown,
                    IngressError::DriverBusy | IngressError::DriverPoisoned => {
                        RoutingAdmissionFailure::CapacityExceeded
                    }
                }),
            }
        })
    }
}

fn rejected(reason: RoutingAdmissionFailure) -> RoutingAdmission {
    RoutingAdmission::Rejected { reason }
}

pub(super) fn receipt(batch: &EventBatch, outcome: &IngressOutcome) -> RoutingAdmission {
    let failure = match outcome {
        IngressOutcome::Required {
            required: OrderedCommitOutcome::Tracked(TrackedFanoutResult::Complete { through, .. }),
            ..
        } => {
            return RoutingAdmission::Accepted {
                through: through.clone(),
            };
        }
        IngressOutcome::Optional(OptionalOrderedOutcome::Attempted(_)) => {
            return RoutingAdmission::Accepted {
                through: batch.last_cursor().clone(),
            };
        }
        IngressOutcome::Required {
            required:
                OrderedCommitOutcome::Rejected {
                    reason: OrderedCommitFailure::Context(reason),
                    ..
                },
            ..
        } => *reason,
        IngressOutcome::Required {
            required:
                OrderedCommitOutcome::Tracked(TrackedFanoutResult::Rejected {
                    reason: TrackedFanoutError::Context(reason),
                    ..
                }),
            ..
        } => *reason,
        IngressOutcome::Optional(OptionalOrderedOutcome::Rejected { reason, .. }) => match reason {
            OptionalOrderedError::Preparation(error) => preparation(*error),
            OptionalOrderedError::Commitment(BestEffortError::Context(reason)) => *reason,
            _ => RoutingAdmissionFailure::NoRequiredCapacity,
        },
        // Never promote required delivery failure or uncertain tracking to success.
        _ => RoutingAdmissionFailure::NoRequiredCapacity,
    };
    rejected(failure)
}

fn preparation(error: OrderedPreparationError) -> RoutingAdmissionFailure {
    use RoutingAdmissionFailure as F;
    match error {
        OrderedPreparationError::Context(reason) => reason,
        OrderedPreparationError::Order(CursorOrderError::Closed)
        | OrderedPreparationError::Namespace(NamespaceError::Shutdown | NamespaceError::Retired)
        | OrderedPreparationError::Registry(RouterRegistryError::ShuttingDown)
        | OrderedPreparationError::Budget(ErrorCode::Unavailable) => F::ShuttingDown,
        OrderedPreparationError::Order(CursorOrderError::Busy) => F::CapacityExceeded,
        OrderedPreparationError::Order(_)
        | OrderedPreparationError::Namespace(
            NamespaceError::Unknown | NamespaceError::InvalidCursor(_),
        ) => F::InvalidCursor,
        OrderedPreparationError::Capacity(FanoutPreparationError::Admission { reason, .. }) => {
            match reason {
                AsyncQueueAdmissionError::Cancelled => F::Cancelled,
                AsyncQueueAdmissionError::DeadlineExceeded => F::DeadlineExceeded,
                _ => F::NoRequiredCapacity,
            }
        }
        OrderedPreparationError::Registry(RouterRegistryError::NoDestinations) => {
            F::NoRequiredCapacity
        }
        _ => F::CapacityExceeded,
    }
}
