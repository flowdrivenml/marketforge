//! Terminal delivery-outcome reporting.
//!
//! Processes leased finalization records through a reporting handler.
//!
//! Successful handling acknowledges the journal claim.
//! Recoverable handling failures release the claim for retry.
//!
//! The journal remains the owner of each record until acknowledged.

use std::time::Duration;

use crate::live::routing::acknowledgement::{FinalizationEscalation, FinalizationRetryPolicy};
use crate::live::{
    foundation::clock::MonotonicClock,
    routing::acknowledgement::{
        FinalizationClaim, FinalizationJournal, FinalizationJournalError, FinalizationRecord,
    },
};

/// Result of processing a terminal delivery outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReportingError {
    TemporarilyUnavailable,
    PermanentFailure,
}

/// Reporting handler for terminal delivery outcomes.
///
/// Implementations must not treat successful queue admission as
/// proof of external persistence or consumer acknowledgement.
///
/// Handlers should be idempotent because claims may be retried.
pub trait FinalizationReporter: Send + Sync {
    fn report(&self, record: &FinalizationRecord) -> Result<(), ReportingError>;
}

/// Result of one reporting iteration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReportingStep {
    Idle,
    Reported,
    RetryScheduled,
    PermanentlyFailed,
}

/// Errors produced by the reporting coordinator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReportingCoordinatorError {
    Journal(FinalizationJournalError),
}

impl From<FinalizationJournalError> for ReportingCoordinatorError {
    fn from(error: FinalizationJournalError) -> Self {
        Self::Journal(error)
    }
}

/// Coordinates bounded journal claims with a reporting handler.
///
/// This is a synchronous, runtime-independent coordinator.
/// Scheduling and task supervision belong to the runtime layer.
pub struct FinalizationReportingConsumer<R> {
    reporter: R,
    lease_duration: Duration,
    retry_policy: FinalizationRetryPolicy,
}

impl<R: FinalizationReporter> FinalizationReportingConsumer<R> {
    pub fn new(
        reporter: R,
        lease_duration: Duration,
        retry_policy: FinalizationRetryPolicy,
    ) -> Result<Self, FinalizationJournalError> {
        if lease_duration.is_zero() {
            return Err(FinalizationJournalError::InvalidLeaseDuration);
        }

        Ok(Self {
            reporter,
            lease_duration,
            retry_policy,
        })
    }

    /// Process at most one terminal outcome.
    ///
    /// A successful handler invocation acknowledges the claim.
    /// A temporary failure releases it for retry.
    ///
    /// Permanent failures remain in the journal until an explicit
    /// recovery or escalation policy handles them.
    pub fn process_one(
        &self,
        journal: &mut FinalizationJournal,
        clock: &dyn MonotonicClock,
    ) -> Result<ReportingStep, ReportingCoordinatorError> {
        let Some(claim) = journal.claim_next(clock, self.lease_duration)? else {
            return Ok(ReportingStep::Idle);
        };

        self.process_claim(journal, clock, claim)
    }

    fn process_claim(
        &self,
        journal: &mut FinalizationJournal,
        clock: &dyn MonotonicClock,
        claim: FinalizationClaim,
    ) -> Result<ReportingStep, ReportingCoordinatorError> {
        let id = claim.record.obligation.id;

        match self.reporter.report(&claim.record) {
            Ok(()) => {
                journal.acknowledge(id, claim.token, clock)?;

                Ok(ReportingStep::Reported)
            }

            Err(ReportingError::TemporarilyUnavailable) => {
                journal.schedule_retry(id, claim.token, clock, self.retry_policy)?;

                Ok(ReportingStep::RetryScheduled)
            }

            Err(ReportingError::PermanentFailure) => {
                journal.escalate(
                    id,
                    claim.token,
                    clock,
                    FinalizationEscalation::PermanentReportingFailure,
                )?;

                Ok(ReportingStep::PermanentlyFailed)
            }
        }
    }
}
