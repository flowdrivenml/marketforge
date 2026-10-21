//! Bounded composition of worker polling and retained delivery ownership.
//! The caller supplies the authoritative tracker and terminal journal used by
//! admission. No task, completion evidence, or retry is manufactured here.
use super::{
    acknowledgement::{
        AcknowledgementTracker, DeliveryObligationId, FinalizationHandoffError, FinalizationJournal,
    },
    completion::{FinalizedDelivery, PendingDeliveries, PendingDeliveryError, RetainedDelivery},
    tracking_recovery::{TrackingRecovery, TrackingRecoverySnapshot},
    worker::{
        DestinationWorker, DestinationWorkers, WorkerOutcome, WorkerRegistrationError,
        WorkersSnapshot,
    },
};
use crate::live::ports::routing::DestinationCompletion;
use std::task::{Context, Poll};

#[derive(Debug)]
#[must_use]
pub enum SupervisionStep {
    Retained(DeliveryObligationId),
    /// Optional queue attempts have no required-delivery obligation. Preserve
    /// the exact outcome for reporting without poisoning required correlation.
    BestEffort(WorkerOutcome),
    Finalized(FinalizedDelivery),
    CapacityBlocked,
    OutcomeBlocked(PendingDeliveryError),
    AdmissionBlocked(PendingDeliveryError),
    FinalizationBlocked(FinalizationHandoffError),
}

#[derive(Debug)]
#[must_use]
pub enum SupervisionShutdownStep {
    Step(SupervisionStep),
    ResourcesDrained,
    DeadlineReached,
}

/// Owned observations from the real delivery owners. This does not establish
/// admission-namespace closure, external durability or complete router shutdown.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliverySupervisionSnapshot {
    pub workers: WorkersSnapshot,
    pub retained_deliveries: usize,
    pub retained_limit: usize,
    pub blocked_outcome: Option<PendingDeliveryError>,
    pub tracked_obligations: usize,
    pub obligation_limit: usize,
    pub prepared_transactions: usize,
    pub recovery: TrackingRecoverySnapshot,
    pub journal_records: usize,
    pub journal_remaining: usize,
    pub escalated_records: usize,
}

impl DeliverySupervisionSnapshot {
    /// Worker closure and every delivery responsibility must be resolved.
    /// In particular, an empty obligation map cannot hide prepared empty
    /// transactions, and transferred payload charges remain visible in workers.
    pub fn is_drained(&self) -> bool {
        self.workers.is_drained()
            && self.retained_deliveries == 0
            && self.blocked_outcome.is_none()
            && self.tracked_obligations == 0
            && self.prepared_transactions == 0
            && self.recovery.is_drained()
            && self.journal_records == 0
    }
}

/// Owns worker calls, retained deliveries and at most one uncorrelated outcome.
/// Invalid adapter responses remain charged until explicit recovery transfer.
/// Dropping this owner is abandonment, never successful shutdown.
pub struct DeliverySupervisor {
    workers: DestinationWorkers,
    pending: PendingDeliveries,
    blocked: Option<(PendingDeliveryError, WorkerOutcome)>,
}
impl DeliverySupervisor {
    pub(super) fn registered_workers_in(&self, registry: &super::router::RouterRegistry) -> bool {
        self.workers.registered_in(registry)
    }

    pub fn new(workers: DestinationWorkers, limit: std::num::NonZeroUsize) -> Self {
        Self {
            workers,
            pending: PendingDeliveries::new(limit),
            blocked: None,
        }
    }

    /// Add a newly claimed registry instance without replacing the delivery owner.
    /// Failure preserves the complete worker for retry or explicit handoff.
    #[allow(clippy::result_large_err)]
    pub fn insert_worker(
        &mut self,
        worker: DestinationWorker,
    ) -> Result<(), (WorkerRegistrationError, DestinationWorker)> {
        self.workers.insert(worker)
    }

    /// Runtime maintenance after payload release; does not finalize obligations.
    pub fn reap_drained_workers(&mut self) -> usize {
        self.workers.reap_drained()
    }

    /// One bounded step: terminal handoff, or at most one worker outcome.
    /// Capacity and correlation faults exert explicit backpressure. Call again
    /// after completion, recovery transfer or journal capacity becomes available;
    /// blocked steps do not arrange an automatic wakeup or spin internally.
    pub fn poll_step(
        &mut self,
        cx: &mut Context<'_>,
        tracker: &mut AcknowledgementTracker,
        journal: &mut FinalizationJournal,
    ) -> Poll<SupervisionStep> {
        match self.pending.finalize_next_into(tracker, journal) {
            Ok(Some(delivery)) => return Poll::Ready(SupervisionStep::Finalized(delivery)),
            Err(error) => return Poll::Ready(SupervisionStep::FinalizationBlocked(error)),
            Ok(None) => {}
        }
        if let Some((error, _)) = &self.blocked {
            return Poll::Ready(SupervisionStep::OutcomeBlocked(*error));
        }
        if self.pending.len() == self.pending.capacity() {
            return Poll::Ready(SupervisionStep::CapacityBlocked);
        }
        let outcome = match self.workers.poll_outcome(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(outcome) => outcome,
        };
        if outcome.descriptor.policy == crate::live::model::delivery::DeliveryPolicy::BestEffort {
            return Poll::Ready(SupervisionStep::BestEffort(outcome));
        }
        match self.pending.ingest(outcome, tracker) {
            Ok(id) => match self.pending.apply_admission(id, tracker) {
                Ok(()) => Poll::Ready(SupervisionStep::Retained(id)),
                Err(error) => Poll::Ready(SupervisionStep::AdmissionBlocked(error)),
            },
            Err((error, outcome)) => {
                self.blocked = Some((error, outcome));
                Poll::Ready(SupervisionStep::OutcomeBlocked(error))
            }
        }
    }

    /// Deadline expiry fences new adapter calls and returns without dropping
    /// pending operations or payloads. After expiry, resume with this same owner
    /// and a newly registered deadline future. Resource
    /// drain does not imply independent tracker/recovery/journal completion.
    pub fn poll_shutdown(
        &mut self,
        cx: &mut Context<'_>,
        deadline: &mut super::admission::DeadlineFuture,
        tracker: &mut AcknowledgementTracker,
        journal: &mut FinalizationJournal,
    ) -> Poll<SupervisionShutdownStep> {
        self.close();
        if self.resources_drained() {
            return Poll::Ready(SupervisionShutdownStep::ResourcesDrained);
        }
        if deadline.as_mut().poll(cx).is_ready() {
            return Poll::Ready(SupervisionShutdownStep::DeadlineReached);
        }
        self.poll_step(cx, tracker, journal)
            .map(SupervisionShutdownStep::Step)
    }

    pub fn apply_completion(
        &self,
        tracker: &mut AcknowledgementTracker,
        completion: &DestinationCompletion,
    ) -> Result<DeliveryObligationId, PendingDeliveryError> {
        self.pending.apply_completion(tracker, completion)
    }

    /// Explicitly transfer the original charge and recovery responsibility.
    pub fn take_blocked_for_recovery(&mut self) -> Option<(PendingDeliveryError, WorkerOutcome)> {
        self.blocked.take()
    }

    pub fn take_for_recovery(&mut self, id: DeliveryObligationId) -> Option<RetainedDelivery> {
        self.pending.take_for_recovery(id)
    }

    #[allow(clippy::result_large_err)]
    pub fn restore_from_recovery(
        &mut self,
        delivery: RetainedDelivery,
        tracker: &AcknowledgementTracker,
    ) -> Result<(), (PendingDeliveryError, RetainedDelivery)> {
        self.pending.restore(delivery, tracker)
    }

    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }
    pub fn workers_snapshot(&self) -> WorkersSnapshot {
        self.workers.snapshot()
    }

    /// Call while holding the enclosing owner's tracker/recovery/journal fence.
    /// Queue observations remain individually synchronized; this is observational
    /// and must not replace the authoritative admission and lifecycle fences.
    pub fn snapshot(
        &self,
        tracker: &AcknowledgementTracker,
        recovery: &TrackingRecovery,
        journal: &FinalizationJournal,
    ) -> DeliverySupervisionSnapshot {
        DeliverySupervisionSnapshot {
            workers: self.workers.snapshot(),
            retained_deliveries: self.pending.len(),
            retained_limit: self.pending.capacity(),
            blocked_outcome: self.blocked.as_ref().map(|(error, _)| *error),
            tracked_obligations: tracker.len(),
            obligation_limit: tracker.capacity(),
            prepared_transactions: tracker.prepared_transactions(),
            recovery: recovery.snapshot(),
            journal_records: journal.len(),
            journal_remaining: journal.remaining_capacity(),
            escalated_records: journal.escalated_count(),
        }
    }

    /// Fence admission while preserving every in-flight call and retained charge.
    /// Continue bounded polling or transfer responsibility explicitly at deadline.
    pub fn close(&mut self) {
        self.workers.close();
    }

    /// Resource drain only. Independent tracker/recovery/journal obligations
    /// must be checked by the enclosing router before claiming full shutdown.
    pub fn resources_drained(&self) -> bool {
        self.blocked.is_none() && self.pending.is_empty() && self.workers.snapshot().is_drained()
    }
}
