//! Bounded composition of worker polling and retained delivery ownership.
//! The caller supplies the authoritative tracker and terminal journal used by
//! admission. No task, completion evidence, or retry is manufactured here.
use super::{
    acknowledgement::{
        AcknowledgementTracker, DeliveryObligationId, FinalizationHandoffError, FinalizationJournal,
    },
    completion::{FinalizedDelivery, PendingDeliveries, PendingDeliveryError, RetainedDelivery},
    worker::{DestinationWorkers, WorkerOutcome, WorkersSnapshot},
};
use crate::live::ports::routing::DestinationCompletion;
use std::task::{Context, Poll};

#[derive(Debug)]
#[must_use]
pub enum SupervisionStep {
    Retained(DeliveryObligationId),
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

/// Owns worker calls, retained deliveries and at most one uncorrelated outcome.
/// Invalid adapter responses remain charged until explicit recovery transfer.
/// Dropping this owner is abandonment, never successful shutdown.
pub struct DeliverySupervisor {
    workers: DestinationWorkers,
    pending: PendingDeliveries,
    blocked: Option<(PendingDeliveryError, WorkerOutcome)>,
}
impl DeliverySupervisor {
    pub fn new(workers: DestinationWorkers, limit: std::num::NonZeroUsize) -> Self {
        Self {
            workers,
            pending: PendingDeliveries::new(limit),
            blocked: None,
        }
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

    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }
    pub fn workers_snapshot(&self) -> WorkersSnapshot {
        self.workers.snapshot()
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
