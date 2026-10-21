//! Contextual producer admission composed from the existing router owners.
//!
//! No delivery lock is held while waiting on queue capacity. Commitment uses
//! nonwaiting access to the one delivery driver; contention returns responsibility
//! to the producer rather than creating an unbounded mutex waiter queue.
use std::sync::{Arc, Mutex, MutexGuard, TryLockError};

use super::{
    admission::{AdmissionContext, DeadlineTimer},
    driver::DeliveryDriver,
    namespaces::AdmissionNamespaces,
    ordered::{
        OptionalOrderedOutcome, OrderedAdmissionBudget, OrderedCommitOutcome,
        OrderedPreparationError,
    },
    router::{BestEffortError, BestEffortFanout, RouterRegistry},
};
use crate::live::{foundation::MonotonicClock, ports::routing::EventBatch};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngressError {
    Preparation(OrderedPreparationError),
    DriverBusy,
    DriverPoisoned,
    DriverClosed,
}

#[derive(Debug)]
#[must_use = "retain the rejected batch for producer retry or recovery"]
pub struct IngressRejection {
    pub batch: EventBatch,
    pub reason: IngressError,
}

/// Detailed results deliberately preserve partial fan-out and tracking repair
/// responsibility. They cannot be discarded into a boolean admission receipt.
#[derive(Debug)]
#[must_use = "inspect required, optional and recovery outcomes independently"]
pub enum IngressOutcome {
    Required {
        required: OrderedCommitOutcome,
        optional: Option<Result<BestEffortFanout, BestEffortError>>,
    },
    Optional(OptionalOrderedOutcome),
}

impl IngressOutcome {
    /// Whether the producer outcome still owns tracking-repair reservations.
    /// A reporter must transfer these to a recovery owner before acknowledging
    /// the record. Already transferred recovery work remains independently owned.
    pub fn retains_tracking_recovery(&self) -> bool {
        use super::router::{RecoveryTrackingFailure, TrackedFanoutResult};
        match self {
            Self::Required {
                required:
                    super::ordered::OrderedCommitOutcome::Tracked(
                        TrackedFanoutResult::RecoveryPending { tracking, .. },
                    ),
                ..
            } => match tracking {
                Ok(result) => !result.unresolved.is_empty(),
                Err(RecoveryTrackingFailure::ReservationCountMismatch { reservations, .. }) => {
                    !reservations.is_empty()
                }
            },
            _ => false,
        }
    }
}

/// Construction-time binding to the same owners used by runtime delivery,
/// shutdown and observations. The caller retains their lifecycle and scheduling.
/// This is the detailed admission surface, not the lossy EventRouterPort adapter.
pub struct RouterIngress<'a> {
    pub registry: &'a RouterRegistry,
    pub namespaces: &'a AdmissionNamespaces,
    pub budget: &'a OrderedAdmissionBudget,
    pub delivery: &'a Mutex<DeliveryDriver>,
    pub clock: Arc<dyn MonotonicClock>,
    pub timer: Option<&'a dyn DeadlineTimer>,
}

impl RouterIngress<'_> {
    fn driver(&self) -> Result<MutexGuard<'_, DeliveryDriver>, IngressError> {
        let driver = self.delivery.try_lock().map_err(|error| match error {
            TryLockError::WouldBlock => IngressError::DriverBusy,
            TryLockError::Poisoned(_) => IngressError::DriverPoisoned,
        })?;
        if driver.is_closed() {
            return Err(IngressError::DriverClosed);
        }
        Ok(driver)
    }

    /// Dropping this future rolls back uncommitted reservations and order permits.
    /// Once commitment starts, no await point can hide irreversible outcomes.
    pub async fn admit(
        &self,
        batch: EventBatch,
        context: AdmissionContext,
    ) -> Result<IngressOutcome, IngressRejection> {
        let reject = |reason| IngressRejection {
            batch: batch.clone(),
            reason,
        };
        // Reject an unavailable owner before allocating timers or reservations.
        drop(self.driver().map_err(&reject)?);
        let plan = self.registry.plan(&batch).map_err(|error| {
            reject(IngressError::Preparation(
                OrderedPreparationError::Registry(error),
            ))
        })?;
        if plan.required_count() == 0 {
            // Keep closure and optional commitment serialized with delivery.
            // Registry revision checks in the actual admission path fence races
            // between this classification and the final optional-only plan.
            let _driver = self.driver().map_err(&reject)?;
            return Ok(IngressOutcome::Optional(self.budget.try_optional_only(
                self.registry,
                self.namespaces,
                batch,
                context,
                self.clock.as_ref(),
            )));
        }
        let prepared = self
            .budget
            .prepare(
                self.registry,
                self.namespaces,
                batch.clone(),
                context,
                self.clock.clone(),
                self.timer,
            )
            .await
            .map_err(|error| reject(IngressError::Preparation(error)))?;
        let mut driver = self.driver().map_err(&reject)?;
        match driver.commit_ordered(prepared) {
            Ok((required, optional)) => Ok(IngressOutcome::Required { required, optional }),
            Err((_, prepared)) => {
                drop(prepared);
                Err(reject(IngressError::DriverClosed))
            }
        }
    }
}
