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
    worker::WorkerOutcome,
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
