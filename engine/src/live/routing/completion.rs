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
        if tracker.obligation(self.obligation.id) != Some(&self.obligation) {
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
