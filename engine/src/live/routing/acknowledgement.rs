//! Destination acknowledgement tracking.
//!
//! Correlates asynchronous destination progress with the exact
//! routing obligation that produced it.
//!
//! This module does not perform IO, allocate stream cursors,
//! or establish persistence guarantees.
use crate::live::foundation::clock::MonotonicClock;
use crate::live::model::delivery::DeliveryPolicy;
use crate::live::model::delivery::DestinationKind;
use crate::live::routing::resolution::{BoundedResolutionSink, ResolutionAcceptanceReceipt};
use crate::live::{
    model::{
        cursor::StreamCursor,
        delivery::{DeliveryFailure, DeliveryStage, DeliveryStatus},
    },
    ports::routing::DestinationId,
};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

static NEXT_JOURNAL_ID: AtomicU64 = AtomicU64::new(1);
/// Unique identity of one destination delivery obligation.
///
/// IDs must not be reused while an acknowledgement from an older
/// obligation could still arrive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DeliveryObligationId(pub u64);
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObligationFinalization {
    Succeeded {
        id: DeliveryObligationId,
        destination: DestinationId,
    },

    Failed {
        id: DeliveryObligationId,
        destination: DestinationId,
        reason: DeliveryFailure,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FinalizationHandoffError {
    Acknowledgement(AcknowledgementError),
    Journal(FinalizationJournalError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FinalizationJournalId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EscalationResolutionToken {
    pub journal: FinalizationJournalId,
    pub sequence: u64,
}

/// A finalized delivery record that has been escalated and resolved.
///
/// Bridges the finalization journal and the escalation-resolution
/// acceptance sink. Must remain stable while the sink retains
/// the corresponding `ResolutionAcceptanceReceipt`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EscalationResolutionRecord {
    pub record: FinalizationRecord,
    pub escalation: FinalizationEscalation,
    pub resolution: EscalationResolution,
}

/// A prepared resolution that has not yet removed its journal record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedEscalationResolution {
    pub record: EscalationResolutionRecord,
    pub token: EscalationResolutionToken,
}

/// One terminal delivery outcome awaiting reporting or recovery.
///
/// The journal owns this record until explicitly acknowledged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalizationRecord {
    pub obligation: DeliveryObligation,
    pub outcome: ObligationFinalization,
}

/// A record's state within the finalization journal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FinalizationRecordState {
    Pending,
    Claimed,
}
#[derive(Debug, Clone, Copy)]
pub struct FinalizationRetryPolicy {
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub max_attempts: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FinalizationRetryPolicyError {
    ZeroInitialBackoff,
    InvalidMaximumBackoff,
    ZeroMaximumAttempts,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EscalatedFinalizationRecord {
    pub record: FinalizationRecord,
    pub reason: FinalizationEscalation,
    pub attempts: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EscalationResolution {
    /// Reporting has been verified through an external mechanism.
    ReportingVerified,

    /// The failure has been explicitly accepted by an authorized
    /// recovery owner. This does not mean delivery succeeded.
    FailureAcknowledged,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PreparedObligationsAbortError {
    pub transaction: ObligationsTransactionId,
}

/// Error returned when a prepared-transaction handle
/// cannot be committed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreparedObligationsHandleError {
    /// The associated transaction has already been
    /// committed and no longer owns the obligations.
    AlreadyCommitted,
}

/// Unique identity of one obligation batch.
///
/// Transactions are the only entity that allocates
/// delivery obligation IDs. The ID is never re-used
/// after a transaction is rolled back.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ObligationsTransactionId(pub u64);

/// Prepared delivery obligations awaiting commitment.
///
/// This type does not own the obligations.
/// The caller may inspect them via `obligations()`
/// but must not manually insert them into a tracker.
///
/// Dropping without `begin_commitment` is a no-op.
#[derive(Debug)]
pub struct PreparedObligations {
    transaction_id: ObligationsTransactionId,
    obligations: Vec<DeliveryObligation>,
}

/// Handle returned when the caller is ready to commit
/// prepared obligations.
///
/// Dropping a handle without calling `begin_commitment`
/// is a no-op. The transaction is automatically aborted
/// when only the inner `PreparedObligations` is dropped.
#[derive(Debug)]
pub struct PreparedObligationsHandle {
    inner: Option<PreparedObligations>,
    committed: bool,
}

impl PreparedObligationsHandle {
    /// Inspect the prepared obligations without
    /// altering tracker state.
    pub fn obligations(&self) -> &[DeliveryObligation] {
        self.inner
            .as_ref()
            .expect("precondition: handle not yet consumed")
            .obligations()
    }

    /// Commit the prepared obligations to the tracker.
    ///
    /// Returns the committed obligations for the caller's
    /// reference. After `begin_commitment` returns
    /// successfully the obligations are active and
    /// tracked independently of this handle.
    pub fn begin_commitment(
        mut self,
        tracker: &mut AcknowledgementTracker,
    ) -> Result<PreparedObligationsHandle, PreparedObligationsHandleError> {
        let prepared = self
            .inner
            .take()
            .ok_or(PreparedObligationsHandleError::AlreadyCommitted)?;
        tracker.begin_commitment(prepared).map(|mut handle| {
            handle.committed = true;
            handle
        })
    }

    /// Attempt to abort this handle through the tracker.
    ///
    /// Returns `AlreadyCommitted` when the associated
    /// transaction has already begun commitment.
    ///
    /// After a successful abort this handle is consumed
    /// and cannot be used again.
    pub fn abort_prepared(
        mut self,
        tracker: &mut AcknowledgementTracker,
    ) -> Result<(), PreparedObligationsHandleError> {
        if self.committed {
            return Err(PreparedObligationsHandleError::AlreadyCommitted);
        }

        let prepared = self.inner.take().expect("precondition: not yet committed");

        tracker
            .abort_prepared(prepared)
            .map_err(|_| PreparedObligationsHandleError::AlreadyCommitted)
    }
}

impl PreparedObligations {
    pub fn transaction_id(&self) -> ObligationsTransactionId {
        self.transaction_id
    }

    pub fn obligations(&self) -> &[DeliveryObligation] {
        &self.obligations
    }

    pub fn len(&self) -> usize {
        self.obligations.len()
    }

    pub fn is_empty(&self) -> bool {
        self.obligations.is_empty()
    }
}

impl FinalizationRetryPolicy {
    pub fn new(
        initial_backoff: Duration,
        max_backoff: Duration,
        max_attempts: u32,
    ) -> Result<Self, FinalizationRetryPolicyError> {
        if initial_backoff.is_zero() {
            return Err(FinalizationRetryPolicyError::ZeroInitialBackoff);
        }

        if max_backoff < initial_backoff {
            return Err(FinalizationRetryPolicyError::InvalidMaximumBackoff);
        }

        if max_attempts == 0 {
            return Err(FinalizationRetryPolicyError::ZeroMaximumAttempts);
        }

        Ok(Self {
            initial_backoff,
            max_backoff,
            max_attempts,
        })
    }

    pub fn backoff(&self, attempts: u32) -> Duration {
        let exponent = attempts.saturating_sub(1).min(31);
        let multiplier = 1_u32 << exponent;

        self.initial_backoff
            .checked_mul(multiplier)
            .unwrap_or(self.max_backoff)
            .min(self.max_backoff)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FinalizationEscalation {
    PermanentReportingFailure,
    RetryLimitExceeded,
}

#[derive(Debug, Clone, Copy)]
struct RetryState {
    attempts: u32,
    next_eligible_at: Option<Instant>,
    escalation: Option<FinalizationEscalation>,
}
/// Fencing token for one claim attempt.
///
/// Tokens are monotonically allocated and never reused by a journal
/// instance, including when an older claim expires.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FinalizationClaimToken(pub u64);

/// One leased claim returned to a reporting consumer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalizationClaim {
    pub record: FinalizationRecord,
    pub token: FinalizationClaimToken,
    pub expires_at: Instant,
}

/// Active claim metadata retained by the journal.
#[derive(Debug, Clone, Copy)]
struct ClaimLease {
    token: FinalizationClaimToken,
    expires_at: Instant,
}

#[derive(Debug, Clone)]
struct JournalEntry {
    record: FinalizationRecord,
    lease: Option<ClaimLease>,
    retry: RetryState,
    prepared_resolution: Option<(EscalationResolutionToken, EscalationResolution)>,
}

/// Bounded in-memory ownership of terminal delivery outcomes.
///
/// Records remain present after claiming and are removed only
/// after explicit acknowledgement by the reporting/recovery owner.
///
/// This journal is not crash-durable.
#[derive(Debug)]
pub struct FinalizationJournal {
    id: FinalizationJournalId,
    entries: VecDeque<JournalEntry>,
    capacity: usize,
    next_claim_token: Option<u64>,
    next_resolution_token: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FinalizationJournalError {
    InvalidCapacity,
    Full,
    DuplicateRecord,
    UnknownRecord,
    NotClaimed,
    InvalidLeaseDuration,
    ClaimTokenExhausted,
    StaleClaim,
    LeaseExpired,
    NotEscalated,
    AlreadyEscalated,
    ResolutionAlreadyPrepared,
    ResolutionNotPrepared,
    StaleResolutionToken,
    ResolutionTokenExhausted,
    JournalIdentityExhausted,
    InvalidResolutionReceipt,
    ResolutionNotAccepted,
}

impl FinalizationJournal {
    pub fn new(capacity: usize) -> Result<Self, FinalizationJournalError> {
        if capacity == 0 {
            return Err(FinalizationJournalError::InvalidCapacity);
        }
        let id = NEXT_JOURNAL_ID
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(1)
            })
            .map_err(|_| FinalizationJournalError::JournalIdentityExhausted)?;
        Ok(Self {
            entries: VecDeque::new(),
            capacity,
            next_claim_token: Some(1),
            next_resolution_token: Some(1),
            id: FinalizationJournalId(id),
        })
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }
    pub fn id(&self) -> FinalizationJournalId {
        self.id
    }

    pub fn schedule_retry(
        &mut self,
        id: DeliveryObligationId,
        token: FinalizationClaimToken,
        clock: &dyn MonotonicClock,
        policy: FinalizationRetryPolicy,
    ) -> Result<(), FinalizationJournalError> {
        let now = clock.now();

        let entry = self
            .entries
            .iter_mut()
            .find(|entry| entry.record.obligation.id == id)
            .ok_or(FinalizationJournalError::UnknownRecord)?;

        let lease = entry.lease.ok_or(FinalizationJournalError::NotClaimed)?;

        if lease.token != token {
            return Err(FinalizationJournalError::StaleClaim);
        }

        if now >= lease.expires_at {
            return Err(FinalizationJournalError::LeaseExpired);
        }

        entry.retry.attempts = entry.retry.attempts.saturating_add(1);

        if entry.retry.attempts >= policy.max_attempts {
            entry.retry.escalation = Some(FinalizationEscalation::RetryLimitExceeded);

            entry.retry.next_eligible_at = None;
        } else {
            let delay = policy.backoff(entry.retry.attempts);

            entry.retry.next_eligible_at = Some(
                now.checked_add(delay)
                    .ok_or(FinalizationJournalError::InvalidLeaseDuration)?,
            );
        }

        entry.lease = None;

        Ok(())
    }
    /// Prepare an escalation resolution without removing its record.
    ///
    /// The caller must arrange for the local operational-event owner
    /// to accept the resolution before committing removal.
    pub fn prepare_resolution(
        &mut self,
        id: DeliveryObligationId,
        resolution: EscalationResolution,
    ) -> Result<PreparedEscalationResolution, FinalizationJournalError> {
        let position = self
            .entries
            .iter()
            .position(|entry| entry.record.obligation.id == id)
            .ok_or(FinalizationJournalError::UnknownRecord)?;

        let escalated = self
            .entries
            .get(position)
            .ok_or(FinalizationJournalError::UnknownRecord)?
            .retry
            .escalation
            .ok_or(FinalizationJournalError::NotEscalated)?;

        if self
            .entries
            .get(position)
            .ok_or(FinalizationJournalError::UnknownRecord)?
            .prepared_resolution
            .is_some()
        {
            return Err(FinalizationJournalError::ResolutionAlreadyPrepared);
        }

        let next = self
            .next_resolution_token
            .ok_or(FinalizationJournalError::ResolutionTokenExhausted)?;

        self.next_resolution_token = next.checked_add(1);
        let token = EscalationResolutionToken {
            journal: self.id,
            sequence: next,
        };

        self.entries[position].prepared_resolution = Some((token, resolution));

        let entry = self
            .entries
            .get(position)
            .ok_or(FinalizationJournalError::UnknownRecord)?;

        Ok(PreparedEscalationResolution {
            record: EscalationResolutionRecord {
                record: entry.record.clone(),
                escalation: escalated,
                resolution,
            },
            token,
        })
    }
    /// Remove an escalated record after its resolution has been
    /// accepted by the local operational-event owner.
    ///
    /// The caller is responsible for proving that acceptance.
    pub fn commit_resolution(
        &mut self,
        id: DeliveryObligationId,
        receipt: &ResolutionAcceptanceReceipt,
        sink: &BoundedResolutionSink,
    ) -> Result<EscalationResolutionRecord, FinalizationJournalError> {
        let position = self
            .entries
            .iter()
            .position(|entry| entry.record.obligation.id == id)
            .ok_or(FinalizationJournalError::UnknownRecord)?;

        let entry = &self.entries[position];

        let (token, resolution) = entry
            .prepared_resolution
            .ok_or(FinalizationJournalError::ResolutionNotPrepared)?;

        if receipt.token() != token {
            return Err(FinalizationJournalError::InvalidResolutionReceipt);
        }

        let expected = EscalationResolutionRecord {
            record: entry.record.clone(),
            escalation: entry
                .retry
                .escalation
                .ok_or(FinalizationJournalError::NotEscalated)?,
            resolution,
        };

        if receipt.record() != &expected {
            return Err(FinalizationJournalError::InvalidResolutionReceipt);
        }

        let mut guard = sink
            .guard_commitment(receipt)
            .map_err(|_| FinalizationJournalError::ResolutionNotAccepted)?;

        if !guard.can_commit() {
            return Err(FinalizationJournalError::ResolutionNotAccepted);
        }

        // All fallible validation must be complete.

        let removed = self
            .entries
            .remove(position)
            .expect("verified journal position");

        guard
            .mark_journal_committed()
            .expect("validated sink commitment state");

        debug_assert_eq!(removed.record, expected.record);

        Ok(expected)
    }
    /// Return a bounded snapshot of escalated records.
    ///
    /// The returned vector cannot exceed journal capacity.
    pub fn escalated_records(&self) -> Vec<EscalatedFinalizationRecord> {
        self.entries
            .iter()
            .filter_map(|entry| {
                let reason = entry.retry.escalation?;

                Some(EscalatedFinalizationRecord {
                    record: entry.record.clone(),
                    reason,
                    attempts: entry.retry.attempts,
                })
            })
            .collect()
    }
    /// Reauthorize reporting for an escalated record.
    ///
    /// This does not change the original delivery outcome.
    ///
    /// The caller must enforce management authorization and record
    /// the recovery action in the local operational-event system.
    pub fn authorize_retry(
        &mut self,
        id: DeliveryObligationId,
    ) -> Result<(), FinalizationJournalError> {
        let entry = self
            .entries
            .iter_mut()
            .find(|entry| entry.record.obligation.id == id)
            .ok_or(FinalizationJournalError::UnknownRecord)?;

        if entry.prepared_resolution.is_some() {
            return Err(FinalizationJournalError::ResolutionAlreadyPrepared);
        }

        if entry.retry.escalation.is_none() {
            return Err(FinalizationJournalError::NotEscalated);
        }

        entry.retry.escalation = None;
        entry.retry.next_eligible_at = None;
        entry.retry.attempts = 0;
        entry.lease = None;

        Ok(())
    }
    pub fn escalated_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.retry.escalation.is_some())
            .count()
    }
    pub fn escalate(
        &mut self,
        id: DeliveryObligationId,
        token: FinalizationClaimToken,
        clock: &dyn MonotonicClock,
        reason: FinalizationEscalation,
    ) -> Result<(), FinalizationJournalError> {
        let now = clock.now();

        let entry = self
            .entries
            .iter_mut()
            .find(|entry| entry.record.obligation.id == id)
            .ok_or(FinalizationJournalError::UnknownRecord)?;

        let lease = entry.lease.ok_or(FinalizationJournalError::NotClaimed)?;

        if lease.token != token {
            return Err(FinalizationJournalError::StaleClaim);
        }

        if now >= lease.expires_at {
            return Err(FinalizationJournalError::LeaseExpired);
        }

        entry.retry.escalation = Some(reason);
        entry.retry.next_eligible_at = None;
        entry.lease = None;

        Ok(())
    }
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn remaining_capacity(&self) -> usize {
        self.capacity - self.entries.len()
    }

    pub fn contains(&self, id: DeliveryObligationId) -> bool {
        self.entries
            .iter()
            .any(|entry| entry.record.obligation.id == id)
    }

    /// Claim the oldest unclaimed terminal outcome.
    ///
    /// Claiming does not remove the record.
    /// Claim the oldest available terminal outcome.
    ///
    /// Expired claims are eligible for reassignment.
    ///
    /// The caller supplies the clock so tests can control time.
    pub fn claim_next(
        &mut self,
        clock: &dyn MonotonicClock,
        lease_duration: Duration,
    ) -> Result<Option<FinalizationClaim>, FinalizationJournalError> {
        if lease_duration.is_zero() {
            return Err(FinalizationJournalError::InvalidLeaseDuration);
        }

        let now = clock.now();

        let Some(position) = self.entries.iter().position(|entry| {
            if entry.retry.escalation.is_some() {
                return false;
            }

            if let Some(next_eligible_at) = entry.retry.next_eligible_at {
                if now < next_eligible_at {
                    return false;
                }
            }

            match entry.lease {
                None => true,
                Some(lease) => now >= lease.expires_at,
            }
        }) else {
            return Ok(None);
        };

        let expires_at = now
            .checked_add(lease_duration)
            .ok_or(FinalizationJournalError::InvalidLeaseDuration)?;

        let next = self
            .next_claim_token
            .ok_or(FinalizationJournalError::ClaimTokenExhausted)?;

        self.next_claim_token = next.checked_add(1);

        let token = FinalizationClaimToken(next);

        let entry = &mut self.entries[position];

        entry.lease = Some(ClaimLease { token, expires_at });

        Ok(Some(FinalizationClaim {
            record: entry.record.clone(),
            token,
            expires_at,
        }))
    }

    /// Acknowledge successful processing of one leased outcome.
    ///
    /// Only the current, unexpired claim token may acknowledge it.
    pub fn acknowledge(
        &mut self,
        id: DeliveryObligationId,
        token: FinalizationClaimToken,
        clock: &dyn MonotonicClock,
    ) -> Result<FinalizationRecord, FinalizationJournalError> {
        let position = self
            .entries
            .iter()
            .position(|entry| entry.record.obligation.id == id)
            .ok_or(FinalizationJournalError::UnknownRecord)?;

        let lease = self.entries[position]
            .lease
            .ok_or(FinalizationJournalError::NotClaimed)?;

        if lease.token != token {
            return Err(FinalizationJournalError::StaleClaim);
        }

        if clock.now() >= lease.expires_at {
            return Err(FinalizationJournalError::LeaseExpired);
        }

        let entry = self
            .entries
            .remove(position)
            .expect("verified journal position");

        Ok(entry.record)
    }

    /// Release a claim so another consumer can retry immediately.
    ///
    /// Stale claim tokens cannot release newer claims.
    pub fn release_claim(
        &mut self,
        id: DeliveryObligationId,
        token: FinalizationClaimToken,
        clock: &dyn MonotonicClock,
    ) -> Result<(), FinalizationJournalError> {
        let entry = self
            .entries
            .iter_mut()
            .find(|entry| entry.record.obligation.id == id)
            .ok_or(FinalizationJournalError::UnknownRecord)?;

        let lease = entry.lease.ok_or(FinalizationJournalError::NotClaimed)?;

        if lease.token != token {
            return Err(FinalizationJournalError::StaleClaim);
        }

        if clock.now() >= lease.expires_at {
            return Err(FinalizationJournalError::LeaseExpired);
        }

        entry.lease = None;

        Ok(())
    }
}
/// The cursor interval covered by one delivery obligation.
///
/// Both endpoints are inclusive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryCursorRange {
    pub first: StreamCursor,
    pub last: StreamCursor,
}

impl DeliveryCursorRange {
    pub fn new(first: StreamCursor, last: StreamCursor) -> Result<Self, AcknowledgementError> {
        if first.stream_id != last.stream_id
            || first.log_epoch != last.log_epoch
            || first.offset > last.offset
        {
            return Err(AcknowledgementError::InvalidRange);
        }

        Ok(Self { first, last })
    }
}

/// Specification for a delivery obligation to be prepared.
///
/// Allows the caller to describe the obligation before
/// the tracker allocates its identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryObligationSpec {
    pub destination: DestinationId,
    pub range: DeliveryCursorRange,
    pub kind: DestinationKind,
    pub policy: DeliveryPolicy,
}

/// Identity and scope of a registered delivery obligation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryObligation {
    pub id: DeliveryObligationId,
    pub destination: DestinationId,
    pub range: DeliveryCursorRange,
    pub kind: DestinationKind,
    pub policy: DeliveryPolicy,
}

/// A destination's progress report.
///
/// Acknowledgements are observations of destination-owned work.
/// They do not create the reported delivery guarantee.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryAcknowledgement {
    pub obligation: DeliveryObligationId,
    pub destination: DestinationId,
    pub range: DeliveryCursorRange,
    pub status: DeliveryStatus,
}

/// Errors produced while validating acknowledgement correlation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcknowledgementError {
    InvalidRange,
    DuplicateObligation,
    UnknownObligation,
    DestinationMismatch,
    RangeMismatch,
    ProgressRegression,
    AlreadyFailed,
    InvalidAcknowledgement,
    InvalidStageForDestination,
    InvalidStageTransition,
    InvalidCapacity,
    CapacityExceeded,
    ObligationIdExhausted,
    ObligationStillPending,
    IncompleteObligation,
    InvalidCompletionPolicy,
}

/// Tracks progress for registered delivery obligations.
///
/// This is a deterministic state machine. The production router
/// owns synchronization and resource accounting around it.
#[derive(Debug)]
pub struct AcknowledgementTracker {
    obligations: HashMap<DeliveryObligationId, TrackedObligation>,
    max_obligations: usize,
    next_id: Option<u64>,
    transaction_state: HashMap<ObligationsTransactionId, Vec<DeliveryObligationId>>,
}

#[derive(Debug, Clone)]
struct TrackedObligation {
    obligation: DeliveryObligation,
    status: DeliveryStatus,
    destination_kind: DestinationKind,
}

impl AcknowledgementTracker {
    pub fn new(max_obligations: usize) -> Result<Self, AcknowledgementError> {
        if max_obligations == 0 {
            return Err(AcknowledgementError::InvalidCapacity);
        }

        Ok(Self {
            obligations: HashMap::new(),
            max_obligations,
            next_id: Some(1),
            transaction_state: HashMap::new(),
        })
    }
    /// Transfer a terminal obligation into the bounded journal.
    ///
    /// If the journal cannot accept the outcome, the obligation
    /// remains in active tracking.
    ///
    /// The caller must provide exclusive mutable access to both
    /// the tracker and journal for this operation.
    pub fn finalize_into(
        &mut self,
        id: DeliveryObligationId,
        journal: &mut FinalizationJournal,
    ) -> Result<ObligationFinalization, FinalizationHandoffError> {
        let tracked =
            self.obligations
                .get(&id)
                .ok_or(FinalizationHandoffError::Acknowledgement(
                    AcknowledgementError::UnknownObligation,
                ))?;

        // Validate completion before changing either collection.
        let outcome = match tracked.status {
            DeliveryStatus::Failed(reason) => ObligationFinalization::Failed {
                id,
                destination: tracked.obligation.destination,
                reason,
            },

            DeliveryStatus::Progress(stage) => {
                let required =
                    required_completion_stage(tracked.obligation.kind, tracked.obligation.policy)
                        .map_err(FinalizationHandoffError::Acknowledgement)?;

                if stage_rank(stage) < stage_rank(required) {
                    return Err(FinalizationHandoffError::Acknowledgement(
                        AcknowledgementError::IncompleteObligation,
                    ));
                }

                ObligationFinalization::Succeeded {
                    id,
                    destination: tracked.obligation.destination,
                }
            }

            DeliveryStatus::Pending => {
                return Err(FinalizationHandoffError::Acknowledgement(
                    AcknowledgementError::IncompleteObligation,
                ));
            }
        };

        if journal.entries.len() >= journal.capacity {
            return Err(FinalizationHandoffError::Journal(
                FinalizationJournalError::Full,
            ));
        }

        if journal.contains(id) {
            return Err(FinalizationHandoffError::Journal(
                FinalizationJournalError::DuplicateRecord,
            ));
        }

        let record = FinalizationRecord {
            obligation: tracked.obligation.clone(),
            outcome,
        };

        // The journal takes ownership before the tracker releases it.
        journal.entries.push_back(JournalEntry {
            record,
            lease: None,
            retry: RetryState {
                attempts: 0,
                next_eligible_at: None,
                escalation: None,
            },
            prepared_resolution: None,
        });

        self.obligations.remove(&id);

        Ok(outcome)
    }

    pub fn register_batch(
        &mut self,
        obligations: Vec<DeliveryObligation>,
    ) -> Result<(), AcknowledgementError> {
        if obligations.len() > self.remaining_capacity() {
            return Err(AcknowledgementError::CapacityExceeded);
        }

        let mut seen = std::collections::HashSet::new();

        for obligation in &obligations {
            required_completion_stage(obligation.kind, obligation.policy)?;

            if self.obligations.contains_key(&obligation.id) || !seen.insert(obligation.id) {
                return Err(AcknowledgementError::DuplicateObligation);
            }
        }

        for obligation in obligations {
            self.register(obligation)
                .expect("prevalidated obligation registration");
        }

        Ok(())
    }
    pub fn allocate_id(&mut self) -> Result<DeliveryObligationId, AcknowledgementError> {
        let current = self
            .next_id
            .ok_or(AcknowledgementError::ObligationIdExhausted)?;

        self.next_id = current.checked_add(1);

        Ok(DeliveryObligationId(current))
    }

    pub fn capacity(&self) -> usize {
        self.max_obligations
    }

    pub fn remaining_capacity(&self) -> usize {
        self.max_obligations.saturating_sub(self.obligations.len())
    }

    pub fn register(&mut self, obligation: DeliveryObligation) -> Result<(), AcknowledgementError> {
        // Reject unsupported destination/policy combinations before
        // allocating tracking capacity.
        required_completion_stage(obligation.kind, obligation.policy)?;
        if self.obligations.contains_key(&obligation.id) {
            return Err(AcknowledgementError::DuplicateObligation);
        }
        if self.obligations.len() >= self.max_obligations {
            return Err(AcknowledgementError::CapacityExceeded);
        }

        self.obligations.insert(
            obligation.id,
            TrackedObligation {
                destination_kind: obligation.kind,
                obligation,
                status: DeliveryStatus::Pending,
            },
        );

        Ok(())
    }

    pub fn acknowledge(
        &mut self,
        acknowledgement: DeliveryAcknowledgement,
    ) -> Result<(), AcknowledgementError> {
        let tracked = self
            .obligations
            .get_mut(&acknowledgement.obligation)
            .ok_or(AcknowledgementError::UnknownObligation)?;

        if tracked.obligation.destination != acknowledgement.destination {
            return Err(AcknowledgementError::DestinationMismatch);
        }

        if tracked.obligation.range != acknowledgement.range {
            return Err(AcknowledgementError::RangeMismatch);
        }

        validate_transition(
            tracked.destination_kind,
            tracked.status,
            acknowledgement.status,
        )?;

        tracked.status = acknowledgement.status;

        Ok(())
    }

    pub fn status(&self, obligation: DeliveryObligationId) -> Option<DeliveryStatus> {
        self.obligations
            .get(&obligation)
            .map(|tracked| tracked.status)
    }
    pub fn len(&self) -> usize {
        self.obligations.len()
    }

    pub fn is_empty(&self) -> bool {
        self.obligations.is_empty()
    }

    /// Validate obligations and register them under a
    /// new opaque transaction.
    ///
    /// All obligations are registered before any error is
    /// returned so that the caller never partially sees
    /// a failed batch.
    pub fn prepare_obligations(
        &mut self,
        obligations: Vec<DeliveryObligation>,
    ) -> Result<PreparedObligations, AcknowledgementError> {
        if obligations.len() > self.remaining_capacity() {
            return Err(AcknowledgementError::CapacityExceeded);
        }

        for obligation in &obligations {
            required_completion_stage(obligation.kind, obligation.policy)?;

            if self.obligations.contains_key(&obligation.id) {
                return Err(AcknowledgementError::DuplicateObligation);
            }
        }

        let transaction_id = ObligationsTransactionId(
            self.next_id
                .ok_or(AcknowledgementError::ObligationIdExhausted)?,
        );

        self.next_id = Some(
            transaction_id
                .0
                .checked_add(1)
                .ok_or(AcknowledgementError::ObligationIdExhausted)?,
        );

        for obligation in &obligations {
            self.register(obligation.clone())
                .expect("obligation passed validation");

            self.transaction_state
                .entry(transaction_id)
                .or_insert_with(Vec::new)
                .push(obligation.id);
        }

        Ok(PreparedObligations {
            transaction_id,
            obligations,
        })
    }

    /// Validate the specified obligation specs and prepare
    /// them into a single tracked transaction.
    ///
    /// All specifications are validated before any
    /// mutation so that validation failure never
    /// consumes obligation IDs.
    pub fn prepare_obligation_specs(
        &mut self,
        specs: Vec<DeliveryObligationSpec>,
    ) -> Result<PreparedObligations, AcknowledgementError> {
        // Capacity must be checked before any mutation.
        if specs.len() > self.remaining_capacity() {
            return Err(AcknowledgementError::CapacityExceeded);
        }

        // Validate all specs before allocating any IDs.
        for spec in &specs {
            required_completion_stage(spec.kind, spec.policy)?;
        }

        // All specs validated — now allocate and build.
        let mut prepared = Vec::with_capacity(specs.len());

        for spec in specs {
            let id = self.allocate_id()?;
            prepared.push(DeliveryObligation {
                id,
                destination: spec.destination,
                range: spec.range,
                kind: spec.kind,
                policy: spec.policy,
            });
        }

        self.prepare_obligations(prepared)
    }

    /// Abort a prepared transaction and remove every
    /// obligation it registered.
    ///
    /// Returns `AlreadyAborted` when the transaction has
    /// either been committed or successfully aborted
    /// already.
    pub fn abort_prepared(
        &mut self,
        prepared: PreparedObligations,
    ) -> Result<(), PreparedObligationsAbortError> {
        let transaction_id = prepared.transaction_id();

        let pending = self.transaction_state.remove(&transaction_id).ok_or(
            PreparedObligationsAbortError {
                transaction: transaction_id,
            },
        )?;

        for id in pending {
            self.obligations.remove(&id);
        }

        Ok(())
    }

    /// Transition obligations from a transaction into
    /// the active tracker.
    ///
    /// Returns a handle the caller must commit or drop.
    /// After this call the transaction is no longer
    /// abortable (its obligations are active).
    pub fn begin_commitment(
        &mut self,
        prepared: PreparedObligations,
    ) -> Result<PreparedObligationsHandle, PreparedObligationsHandleError> {
        let transaction_id = prepared.transaction_id();

        let pending = self
            .transaction_state
            .remove(&transaction_id)
            .ok_or(PreparedObligationsHandleError::AlreadyCommitted)?;

        for id in &pending {
            if !self.obligations.contains_key(id) {
                return Err(PreparedObligationsHandleError::AlreadyCommitted);
            }
        }

        Ok(PreparedObligationsHandle {
            inner: Some(prepared),
            committed: true,
        })
    }
}
///
fn required_completion_stage(
    kind: DestinationKind,
    policy: DeliveryPolicy,
) -> Result<DeliveryStage, AcknowledgementError> {
    match (kind, policy) {
        (
            DestinationKind::Publication | DestinationKind::Derived,
            DeliveryPolicy::BestEffort | DeliveryPolicy::Required,
        ) => Ok(DeliveryStage::Delivered),

        (
            DestinationKind::Recording | DestinationKind::Retention,
            DeliveryPolicy::BestEffort | DeliveryPolicy::Required,
        ) => Ok(DeliveryStage::Committed),

        (DestinationKind::Recording | DestinationKind::Retention, DeliveryPolicy::Durable) => {
            Ok(DeliveryStage::Durable)
        }

        (DestinationKind::Publication | DestinationKind::Derived, DeliveryPolicy::Durable) => {
            Err(AcknowledgementError::InvalidCompletionPolicy)
        }
    }
}

fn stage_allowed(destination: DestinationKind, stage: DeliveryStage) -> bool {
    match destination {
        DestinationKind::Publication => matches!(
            stage,
            DeliveryStage::Accepted | DeliveryStage::Queued | DeliveryStage::Delivered
        ),

        DestinationKind::Recording | DestinationKind::Retention => {
            matches!(
                stage,
                DeliveryStage::Accepted
                    | DeliveryStage::Queued
                    | DeliveryStage::Committed
                    | DeliveryStage::Durable
            )
        }

        DestinationKind::Derived => matches!(
            stage,
            DeliveryStage::Accepted | DeliveryStage::Queued | DeliveryStage::Delivered
        ),
    }
}

fn stage_rank(stage: DeliveryStage) -> u8 {
    match stage {
        DeliveryStage::Accepted => 0,
        DeliveryStage::Queued => 1,
        DeliveryStage::Delivered => 2,
        DeliveryStage::Committed => 3,
        DeliveryStage::Durable => 4,
    }
}

fn validate_transition(
    destination: DestinationKind,
    previous: DeliveryStatus,
    next: DeliveryStatus,
) -> Result<(), AcknowledgementError> {
    if let DeliveryStatus::Progress(stage) = next {
        if !stage_allowed(destination, stage) {
            return Err(AcknowledgementError::InvalidStageForDestination);
        }
    }

    match (previous, next) {
        (DeliveryStatus::Failed(_), _) => Err(AcknowledgementError::AlreadyFailed),

        (DeliveryStatus::Pending, DeliveryStatus::Pending) => Ok(()),

        (DeliveryStatus::Pending, DeliveryStatus::Progress(_)) => Ok(()),

        (DeliveryStatus::Pending, DeliveryStatus::Failed(_)) => Ok(()),

        (DeliveryStatus::Progress(previous_stage), DeliveryStatus::Progress(next_stage)) => {
            if stage_rank(next_stage) < stage_rank(previous_stage) {
                Err(AcknowledgementError::ProgressRegression)
            } else {
                Ok(())
            }
        }

        (DeliveryStatus::Progress(_), DeliveryStatus::Failed(_)) => Ok(()),

        (DeliveryStatus::Progress(_), DeliveryStatus::Pending) => {
            Err(AcknowledgementError::ProgressRegression)
        }
    }
}

/// A failure reported for an outstanding delivery obligation.
///
/// This alias keeps failure classification aligned with the
/// shared delivery domain model.
pub type DestinationFailure = DeliveryFailure;
