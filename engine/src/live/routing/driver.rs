//! Single mutable owner for the router's in-process delivery responsibilities.
//! Admission and reporting borrow these same owners; this is not a second
//! tracker, an executor, or a crash-durable recovery service.
use super::{
    acknowledgement::{
        AcknowledgementTracker, DeliveryObligationId, FinalizationJournal, PreparedObligations,
        PreparedObligationsAbortError,
    },
    admission::DeadlineFuture,
    completion::{PendingDeliveryError, RetainedDelivery},
    supervisor::{DeliverySupervisionSnapshot, DeliverySupervisor, SupervisionStep},
    tracking_recovery::{TrackingRecovery, TrackingRecoveryPass},
    worker::{DestinationWorker, WorkerOutcome, WorkerRegistrationError},
};
use crate::live::ports::routing::DestinationCompletion;
use std::task::{Context, Poll};

/// One bounded recovery pass followed by at most one supervision step.
/// Recovery errors remain observable even when delivery work is pending.
#[derive(Debug)]
#[must_use]
pub struct DeliveryDrive {
    pub recovery: TrackingRecoveryPass,
    pub delivery: Poll<SupervisionStep>,
}

/// One bounded runtime turn using the authoritative registry and delivery owners.
/// Membership errors are observations, not proof that delivery has drained.
#[derive(Debug)]
#[must_use]
pub struct RegisteredDeliveryDrive {
    pub registry: RegistryWorkerSync,
    pub delivery: DeliveryDrive,
    pub membership: Poll<Result<u64, super::router::RouterRegistryError>>,
}

#[derive(Debug)]
#[must_use]
pub enum DeliveryShutdown {
    Step(DeliveryDrive),
    Drained,
    DeadlineReached,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryAdmissionError {
    Closed,
}

/// One bounded membership maintenance turn; no adapter operation is polled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub struct RegistryWorkerSync {
    pub workers_reaped: usize,
    pub retired_reaped: usize,
    pub attachment: Result<bool, RegistryWorkerSyncError>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegistryWorkerSyncError {
    Closed,
    Registry(super::router::RegistryWorkerError),
    Scheduler(WorkerRegistrationError),
}

pub struct DeliveryDriver {
    closed: bool,
    supervisor: DeliverySupervisor,
    tracker: AcknowledgementTracker,
    recovery: TrackingRecovery,
    journal: FinalizationJournal,
}

impl DeliveryDriver {
    /// Transfer the authoritative owners, including any existing responsibilities.
    pub fn new(
        supervisor: DeliverySupervisor,
        tracker: AcknowledgementTracker,
        recovery: TrackingRecovery,
        journal: FinalizationJournal,
    ) -> Self {
        Self {
            closed: false,
            supervisor,
            tracker,
            recovery,
            journal,
        }
    }

    /// Exclusive admission access to the same tracker and recovery capacity
    /// consumed by worker supervision. Never hold this borrow across external IO.
    pub fn admission_owners(
        &mut self,
    ) -> Result<(&mut AcknowledgementTracker, &mut TrackingRecovery), DeliveryAdmissionError> {
        if self.closed {
            return Err(DeliveryAdmissionError::Closed);
        }
        Ok((&mut self.tracker, &mut self.recovery))
    }

    /// Attach a registry worker to the existing authoritative delivery owners.
    /// Shutdown fencing is enforced by the owned scheduler. Errors return the
    /// worker intact, including queue lease and any pending operation.
    #[allow(clippy::result_large_err)]
    pub fn insert_worker(
        &mut self,
        worker: DestinationWorker,
    ) -> Result<(), (WorkerRegistrationError, DestinationWorker)> {
        self.supervisor.insert_worker(worker)
    }

    /// Release only closed, drained scheduler instances. Call registry reaping
    /// separately; neither operation establishes delivery completion.
    pub fn reap_drained_workers(&mut self) -> usize {
        self.supervisor.reap_drained_workers()
    }

    /// Reap drained ownership and attach at most one unclaimed registry queue.
    /// Repeat on membership change or after a charge is released, not in a busy
    /// loop after capacity failure. Retired queues take precedence over new ones.
    /// A rejected fresh worker has never dequeued or polled an adapter: releasing
    /// its lease leaves every payload and charge in the original registry queue.
    /// Existing workers and their operations are never replaced or dropped here.
    /// Shutdown permits reaping but fences new attachment.
    pub fn sync_registry(
        &mut self,
        registry: &super::router::RouterRegistry,
    ) -> RegistryWorkerSync {
        let workers_reaped = self.reap_drained_workers();
        let retired_reaped = registry.reap_retired();
        let attachment =
            if self.closed {
                Err(RegistryWorkerSyncError::Closed)
            } else {
                match registry.claim_next_worker() {
                    Ok(Some(worker)) => self.insert_worker(worker).map(|()| true).map_err(
                        |(reason, fresh_worker)| {
                            drop(fresh_worker);
                            RegistryWorkerSyncError::Scheduler(reason)
                        },
                    ),
                    Ok(None) => Ok(false),
                    Err(reason) => Err(RegistryWorkerSyncError::Registry(reason)),
                }
            };
        RegistryWorkerSync {
            workers_reaped,
            retired_reaped,
            attachment,
        }
    }

    pub fn is_closed(&self) -> bool {
        self.closed
    }

    /// Roll back an existing preparation even after admission closes. This
    /// deliberately grants no access for creating or committing new obligations.
    pub fn abort_prepared(
        &mut self,
        prepared: PreparedObligations,
    ) -> Result<(), PreparedObligationsAbortError> {
        self.tracker.abort_prepared(prepared)
    }

    /// Reporting must explicitly acknowledge or escalate actual terminal records.
    pub fn journal_mut(&mut self) -> &mut FinalizationJournal {
        &mut self.journal
    }

    pub fn snapshot(&self) -> DeliverySupervisionSnapshot {
        self.supervisor
            .snapshot(&self.tracker, &self.recovery, &self.journal)
    }

    /// Does not sleep or loop until success. The enclosing runtime schedules a
    /// turn on worker wakeup, completion, reporting, or a bounded retry timer.
    /// A failed recovery pass alone must not trigger immediate repeated turns.
    /// Recovery precedes finalization so successfully repaired tracking can
    /// release a terminal payload in this same turn. Independent worker work
    /// continues even when some recovery updates cannot yet be applied.
    pub fn drive(&mut self, cx: &mut Context<'_>) -> DeliveryDrive {
        let recovery = self.recovery.retry_pass(&mut self.tracker);
        let delivery = self
            .supervisor
            .poll_step(cx, &mut self.tracker, &mut self.journal);
        DeliveryDrive { recovery, delivery }
    }

    /// Reconcile at most one queue, run one delivery turn, then arm membership
    /// observation against the revision captured before either operation. This
    /// closes the race with membership changes during adapter polling.
    ///
    /// Successful attachment, retention and finalization schedule another bounded
    /// turn. Capacity/correlation/reporting failures and failed tracking retries
    /// never self-wake: the runtime must arrange completion/reporting notifications
    /// and a bounded retry timer. External payload release also needs a runtime
    /// notification or timer. Worker futures and idle queues register this waker.
    /// This is a cooperative scheduling primitive, not a deadline shutdown path;
    /// close admission and synchronize all queues before closing the driver.
    pub fn drive_registered(
        &mut self,
        registry: &super::router::RouterRegistry,
        cx: &mut Context<'_>,
    ) -> RegisteredDeliveryDrive {
        let revision = registry.revision();
        let maintenance = self.sync_registry(registry);
        let delivery = self.drive(cx);
        let membership = registry.poll_membership_change(revision, cx);
        let progressed = matches!(
            delivery.delivery,
            Poll::Ready(SupervisionStep::Retained(_) | SupervisionStep::Finalized(_))
        );
        if maintenance.attachment == Ok(true)
            || progressed
            || matches!(membership, Poll::Ready(Ok(_)))
        {
            cx.waker().wake_by_ref();
        }
        RegisteredDeliveryDrive {
            registry: maintenance,
            delivery,
            membership,
        }
    }

    pub fn apply_completion(
        &mut self,
        completion: &DestinationCompletion,
    ) -> Result<DeliveryObligationId, PendingDeliveryError> {
        self.supervisor
            .apply_completion(&mut self.tracker, completion)
    }

    pub fn close(&mut self) {
        self.closed = true;
        self.supervisor.close();
    }

    /// Fence worker queues and check all owned delivery responsibilities, not
    /// just worker resources. Expiry performs no recovery update or adapter call.
    /// Keep this owner after expiry; a later deadline can resume draining.
    /// Admission namespace closure remains the enclosing router's responsibility.
    pub fn drive_shutdown(
        &mut self,
        cx: &mut Context<'_>,
        deadline: &mut DeadlineFuture,
    ) -> DeliveryShutdown {
        self.close();
        if self.snapshot().is_drained() {
            return DeliveryShutdown::Drained;
        }
        if deadline.as_mut().poll(cx).is_ready() {
            return DeliveryShutdown::DeadlineReached;
        }
        DeliveryShutdown::Step(self.drive(cx))
    }

    /// Recovery transfer preserves the original worker payload and its charge.
    pub fn take_blocked_for_recovery(&mut self) -> Option<(PendingDeliveryError, WorkerOutcome)> {
        self.supervisor.take_blocked_for_recovery()
    }

    pub fn take_for_recovery(&mut self, id: DeliveryObligationId) -> Option<RetainedDelivery> {
        self.supervisor.take_for_recovery(id)
    }
}
