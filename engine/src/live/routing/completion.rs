//! Payload ownership across destination admission, completion and finalization.
//! No retry or task is spawned here. A supervisor retains this owner on every
//! error and transfers terminal payloads explicitly to release or recovery.
use super::{
    acknowledgement::{
        AcknowledgementError, AcknowledgementTracker, DeliveryAcknowledgement, DeliveryCursorRange,
        DeliveryObligation, DeliveryObligationId, FinalizationHandoffError, FinalizationJournal,
        ObligationFinalization,
    },
    queue::QueuedBatch,
    worker::{WorkerAdmissionResult, WorkerOutcome},
};
use crate::live::{
    model::delivery::DeliveryStatus,
    ports::routing::{DestinationAdmission, DestinationCompletion},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionError {
    ForeignTracker,
    Tracking(AcknowledgementError),
    DescriptorMismatch,
    InvalidAdmission,
    AdmissionRejected,
    DestinationMismatch,
    CursorMismatch,
}

/// One charged payload, correlated to one existing delivery obligation.
/// Dropping this owner abandons payload recovery; it is not successful drain.
#[derive(Debug)]
#[must_use = "retain until terminal journal handoff or explicit recovery transfer"]
pub struct RetainedDelivery {
    outcome: WorkerOutcome,
    obligation: DeliveryObligation,
    tracker_identity: std::sync::Arc<()>,
}

/// Terminal reporting ownership is in the journal. Payload capacity remains
/// charged until the supervisor releases this value or transfers it to recovery.
#[derive(Debug)]
#[must_use = "failed delivery may require payload recovery before releasing capacity"]
pub struct FinalizedDelivery {
    pub outcome: ObligationFinalization,
    pub payload: QueuedBatch,
}

impl RetainedDelivery {
    /// Failure returns the complete original outcome and its capacity charge.
    #[allow(clippy::result_large_err)]
    pub fn bind(
        outcome: WorkerOutcome,
        id: DeliveryObligationId,
        tracker: &AcknowledgementTracker,
    ) -> Result<Self, (CompletionError, WorkerOutcome)> {
        let validate = || {
            let obligation = tracker.obligation(id).ok_or(CompletionError::Tracking(
                AcknowledgementError::UnknownObligation,
            ))?;
            let descriptor = outcome.descriptor;
            if obligation.destination != descriptor.id
                || obligation.kind != descriptor.kind
                || obligation.policy != descriptor.policy
            {
                return Err(CompletionError::DescriptorMismatch);
            }
            if &obligation.range.first != outcome.payload.batch().first_cursor()
                || &obligation.range.last != outcome.payload.batch().last_cursor()
            {
                return Err(CompletionError::Tracking(
                    AcknowledgementError::RangeMismatch,
                ));
            }
            // Recheck the actual response, including callers constructing public
            // WorkerOutcome fields; the Valid label alone is not evidence.
            match &outcome.admission {
                WorkerAdmissionResult::Valid(DestinationAdmission::Accepted {
                    destination,
                    through,
                }) if *destination == descriptor.id
                    && through == outcome.payload.batch().last_cursor() => {}
                WorkerAdmissionResult::Valid(DestinationAdmission::Rejected {
                    destination,
                    ..
                }) if *destination == descriptor.id => {}
                _ => return Err(CompletionError::InvalidAdmission),
            }
            Ok(obligation.clone())
        };
        match validate() {
            Ok(obligation) => Ok(Self {
                outcome,
                obligation,
                tracker_identity: tracker.identity().clone(),
            }),
            Err(error) => Err((error, outcome)),
        }
    }

    pub fn payload(&self) -> &QueuedBatch {
        &self.outcome.payload
    }

    pub fn obligation(&self) -> &DeliveryObligation {
        &self.obligation
    }

    fn check_tracker(&self, tracker: &AcknowledgementTracker) -> Result<(), CompletionError> {
        if !std::sync::Arc::ptr_eq(&self.tracker_identity, tracker.identity()) {
            return Err(CompletionError::ForeignTracker);
        }
        match tracker.obligation(self.obligation.id) {
            Some(obligation) if obligation == &self.obligation => Ok(()),
            Some(_) => Err(CompletionError::DescriptorMismatch),
            None => Err(CompletionError::Tracking(
                AcknowledgementError::UnknownObligation,
            )),
        }
    }

    /// Acceptance adds no delivery stage. Rejection records the actual failure,
    /// while retaining the payload for recovery and terminal reporting.
    pub fn apply_admission(
        &self,
        tracker: &mut AcknowledgementTracker,
    ) -> Result<(), CompletionError> {
        self.check_tracker(tracker)?;
        if let WorkerAdmissionResult::Valid(DestinationAdmission::Rejected { reason, .. }) =
            self.outcome.admission
        {
            self.apply_status(tracker, DeliveryStatus::Failed(reason))?;
        }
        Ok(())
    }

    /// Only a fully correlated, separately observed completion advances progress.
    /// A partial-through report cannot fulfill this batch's obligation.
    pub fn apply_completion(
        &self,
        tracker: &mut AcknowledgementTracker,
        completion: &DestinationCompletion,
    ) -> Result<(), CompletionError> {
        self.check_tracker(tracker)?;
        if !matches!(
            self.outcome.admission,
            WorkerAdmissionResult::Valid(DestinationAdmission::Accepted { .. })
        ) {
            return Err(CompletionError::AdmissionRejected);
        }
        if completion.destination != self.obligation.destination {
            return Err(CompletionError::DestinationMismatch);
        }
        if completion.through != self.obligation.range.last {
            return Err(CompletionError::CursorMismatch);
        }
        self.apply_status(tracker, completion.status)
    }

    fn apply_status(
        &self,
        tracker: &mut AcknowledgementTracker,
        status: DeliveryStatus,
    ) -> Result<(), CompletionError> {
        tracker
            .acknowledge(DeliveryAcknowledgement {
                obligation: self.obligation.id,
                destination: self.obligation.destination,
                range: DeliveryCursorRange {
                    first: self.obligation.range.first.clone(),
                    last: self.obligation.range.last.clone(),
                },
                status,
            })
            .map_err(CompletionError::Tracking)
    }

    /// Uses the tracker's existing policy and atomic journal handoff. Incomplete
    /// guarantees and full journals return the entire charged owner for retry.
    #[allow(clippy::result_large_err)]
    pub fn finalize_into(
        self,
        tracker: &mut AcknowledgementTracker,
        journal: &mut FinalizationJournal,
    ) -> Result<FinalizedDelivery, (FinalizationHandoffError, Self)> {
        if self.check_tracker(tracker).is_err() {
            return Err((
                FinalizationHandoffError::Acknowledgement(AcknowledgementError::UnknownObligation),
                self,
            ));
        }
        match tracker.finalize_into(self.obligation.id, journal) {
            Ok(outcome) => Ok(FinalizedDelivery {
                outcome,
                payload: self.outcome.payload,
            }),
            Err(error) => Err((error, self)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PendingDeliveryError {
    Full,
    AllocationFailed,
    UnknownObligation,
    AmbiguousObligation,
    DuplicateObligation,
    Completion(CompletionError),
}

/// Bounded supervisor-owned payloads awaiting independent destination evidence.
/// Tracker metadata and progress remain authoritative. Scans are bounded by the
/// configured tracker/collection capacities; no secondary cursor index is kept.
#[derive(Debug)]
#[must_use = "retain pending payloads through completion or explicit recovery transfer"]
pub struct PendingDeliveries {
    limit: std::num::NonZeroUsize,
    deliveries: Vec<RetainedDelivery>,
}

impl PendingDeliveries {
    pub fn new(limit: std::num::NonZeroUsize) -> Self {
        Self {
            limit,
            deliveries: Vec::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.deliveries.len()
    }
    pub fn is_empty(&self) -> bool {
        self.deliveries.is_empty()
    }
    pub fn capacity(&self) -> usize {
        self.limit.get()
    }

    pub fn get(&self, id: DeliveryObligationId) -> Option<&RetainedDelivery> {
        self.deliveries
            .iter()
            .find(|delivery| delivery.obligation.id == id)
    }

    /// Return an explicitly transferred payload to its original owner. Neither
    /// progress nor admission is replayed; failures return the complete charge.
    #[allow(clippy::result_large_err)]
    pub fn restore(
        &mut self,
        delivery: RetainedDelivery,
        tracker: &AcknowledgementTracker,
    ) -> Result<(), (PendingDeliveryError, RetainedDelivery)> {
        let validate = || {
            delivery
                .check_tracker(tracker)
                .map_err(PendingDeliveryError::Completion)?;
            if self.get(delivery.obligation.id).is_some() {
                return Err(PendingDeliveryError::DuplicateObligation);
            }
            if self.len() == self.capacity() {
                return Err(PendingDeliveryError::Full);
            }
            Ok(())
        };
        if let Err(error) = validate() {
            return Err((error, delivery));
        }
        if self.deliveries.try_reserve(1).is_err() {
            return Err((PendingDeliveryError::AllocationFailed, delivery));
        }
        self.deliveries.push(delivery);
        Ok(())
    }

    /// Correlate against the real tracker. Ambiguity fails closed rather than
    /// selecting an arbitrary registration. Errors return the charged outcome.
    #[allow(clippy::result_large_err)]
    pub fn ingest(
        &mut self,
        outcome: WorkerOutcome,
        tracker: &AcknowledgementTracker,
    ) -> Result<DeliveryObligationId, (PendingDeliveryError, WorkerOutcome)> {
        let correlate = || {
            if self.len() == self.capacity() {
                return Err(PendingDeliveryError::Full);
            }
            let mut matches = tracker.obligations().filter(|obligation| {
                obligation.destination == outcome.descriptor.id
                    && obligation.kind == outcome.descriptor.kind
                    && obligation.policy == outcome.descriptor.policy
                    && &obligation.range.first == outcome.payload.batch().first_cursor()
                    && &obligation.range.last == outcome.payload.batch().last_cursor()
            });
            let id = matches
                .next()
                .ok_or(PendingDeliveryError::UnknownObligation)?
                .id;
            if matches.next().is_some() {
                return Err(PendingDeliveryError::AmbiguousObligation);
            }
            if self.get(id).is_some() {
                return Err(PendingDeliveryError::DuplicateObligation);
            }
            Ok(id)
        };
        let id = match correlate() {
            Ok(id) => id,
            Err(error) => return Err((error, outcome)),
        };
        if self.deliveries.try_reserve(1).is_err() {
            return Err((PendingDeliveryError::AllocationFailed, outcome));
        }
        let delivery = RetainedDelivery::bind(outcome, id, tracker)
            .map_err(|(error, outcome)| (PendingDeliveryError::Completion(error), outcome))?;
        self.deliveries.push(delivery);
        Ok(id)
    }

    /// Ingestion does not advance progress; applying rejection is explicit.
    pub fn apply_admission(
        &self,
        id: DeliveryObligationId,
        tracker: &mut AcknowledgementTracker,
    ) -> Result<(), PendingDeliveryError> {
        self.get(id)
            .ok_or(PendingDeliveryError::UnknownObligation)?
            .apply_admission(tracker)
            .map_err(PendingDeliveryError::Completion)
    }

    pub fn apply_completion(
        &self,
        tracker: &mut AcknowledgementTracker,
        completion: &DestinationCompletion,
    ) -> Result<DeliveryObligationId, PendingDeliveryError> {
        let mut matches = self.deliveries.iter().filter(|delivery| {
            delivery.obligation.destination == completion.destination
                && delivery.obligation.range.last == completion.through
        });
        let delivery = matches
            .next()
            .ok_or(PendingDeliveryError::UnknownObligation)?;
        if matches.next().is_some() {
            return Err(PendingDeliveryError::AmbiguousObligation);
        }
        delivery
            .apply_completion(tracker, completion)
            .map_err(PendingDeliveryError::Completion)?;
        Ok(delivery.obligation.id)
    }

    /// Failed handoff restores the charged owner. Successful handoff transfers
    /// payload release/recovery responsibility to the caller, independently of
    /// the journal's terminal reporting responsibility.
    pub fn finalize_into(
        &mut self,
        id: DeliveryObligationId,
        tracker: &mut AcknowledgementTracker,
        journal: &mut FinalizationJournal,
    ) -> Result<FinalizedDelivery, FinalizationHandoffError> {
        let index = self
            .deliveries
            .iter()
            .position(|delivery| delivery.obligation.id == id)
            .ok_or(FinalizationHandoffError::Acknowledgement(
                AcknowledgementError::UnknownObligation,
            ))?;
        let delivery = self.deliveries.swap_remove(index);
        match delivery.finalize_into(tracker, journal) {
            Ok(finalized) => Ok(finalized),
            Err((error, delivery)) => {
                self.deliveries.push(delivery);
                Err(error)
            }
        }
    }

    /// Transfer at most one terminal payload, scanning the bounded collection.
    /// Incomplete obligations do not block unrelated terminal deliveries. Any
    /// tracking or journal error leaves all payloads owned here for retry.
    /// `None` means no retained obligation is terminal, not successful drain.
    pub fn finalize_next_into(
        &mut self,
        tracker: &mut AcknowledgementTracker,
        journal: &mut FinalizationJournal,
    ) -> Result<Option<FinalizedDelivery>, FinalizationHandoffError> {
        for index in 0..self.deliveries.len() {
            let delivery = &self.deliveries[index];
            if tracker.obligation(delivery.obligation.id) != Some(&delivery.obligation) {
                return Err(FinalizationHandoffError::Acknowledgement(
                    AcknowledgementError::UnknownObligation,
                ));
            }
            match tracker.finalize_into(delivery.obligation.id, journal) {
                Ok(outcome) => {
                    let delivery = self.deliveries.remove(index);
                    return Ok(Some(FinalizedDelivery {
                        outcome,
                        payload: delivery.outcome.payload,
                    }));
                }
                Err(FinalizationHandoffError::Acknowledgement(
                    AcknowledgementError::IncompleteObligation,
                )) => continue,
                Err(error) => return Err(error),
            }
        }
        Ok(None)
    }

    /// Explicit recovery transfer leaves the authoritative obligation tracked.
    pub fn take_for_recovery(&mut self, id: DeliveryObligationId) -> Option<RetainedDelivery> {
        let index = self
            .deliveries
            .iter()
            .position(|delivery| delivery.obligation.id == id)?;
        Some(self.deliveries.swap_remove(index))
    }
}
