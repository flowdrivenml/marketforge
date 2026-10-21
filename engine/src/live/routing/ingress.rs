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
    /// Return attached repairs to the existing delivery owners. Each update is
    /// attempted once; rejected ownership stays in this outcome in original order.
    /// Count mismatches lack a trustworthy reservation mapping and are untouched.
    /// Successful transfer is not proof that the repair or delivery completed.
    pub(crate) fn reconcile_tracking(
        &mut self,
        tracker: &mut super::acknowledgement::AcknowledgementTracker,
        recovery: &mut super::tracking_recovery::TrackingRecovery,
    ) -> TrackingHandoff {
        use super::router::{TrackedFanoutResult, UnresolvedTrackingUpdate};
        let Self::Required {
            required:
                super::ordered::OrderedCommitOutcome::Tracked(TrackedFanoutResult::RecoveryPending {
                    batch,
                    tracking,
                    ..
                }),
            ..
        } = self
        else {
            return TrackingHandoff::default();
        };
        let Ok(result) = tracking else {
            return TrackingHandoff {
                mapping_blocked: true,
                ..TrackingHandoff::default()
            };
        };
        let range = super::acknowledgement::DeliveryCursorRange::new(
            batch.first_cursor().clone(),
            batch.last_cursor().clone(),
        )
        .expect("EventBatch validates its cursor range");
        let mut report = TrackingHandoff::default();
        let mut index = 0;
        while index < result.unresolved.len() {
            let update = result.unresolved.remove(index);
            let rejected = match update {
                UnresolvedTrackingUpdate::ApplyFailure {
                    destination,
                    failure,
                } => {
                    match recovery.apply_or_retain(
                        tracker,
                        failure.acknowledgement,
                        failure.reservation,
                    ) {
                        Ok(_) => None,
                        Err(failure) => Some(UnresolvedTrackingUpdate::ApplyFailure {
                            destination,
                            failure,
                        }),
                    }
                }
                UnresolvedTrackingUpdate::MissingObligation {
                    destination,
                    status,
                    reservation,
                } => {
                    match recovery.retain_unresolved(
                        destination,
                        range.clone(),
                        status,
                        reservation,
                    ) {
                        Ok(()) => None,
                        Err(failure) => Some(UnresolvedTrackingUpdate::MissingObligation {
                            destination: failure.destination,
                            status: failure.status,
                            reservation: failure.reservation,
                        }),
                    }
                }
            };
            if let Some(update) = rejected {
                result.unresolved.insert(index, update);
                index += 1;
                report.retained += 1;
            } else {
                report.transferred += 1;
            }
        }
        report
    }

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

/// Ownership handoff only; transferred repairs may still require intervention.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TrackingHandoff {
    pub transferred: usize,
    pub retained: usize,
    pub mapping_blocked: bool,
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
