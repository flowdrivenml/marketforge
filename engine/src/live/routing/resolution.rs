//! Bounded local acceptance of escalation-resolution records.
//!
//! Acceptance transfers responsibility to an in-process sink.
//! It does not establish crash durability.
//!
//! Receipts identify the sink instance and exact accepted resolution.
//! They cannot be constructed directly by external callers.
use crate::live::foundation::clock::MonotonicClock;
use crate::live::routing::acknowledgement::{
    EscalationResolutionRecord, EscalationResolutionToken,
};
use std::sync::MutexGuard;
use std::time::{Duration, Instant};
use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

static NEXT_SINK_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolutionSinkState {
    Accepted,
    JournalCommitted,
    Claimed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolutionAcceptanceError {
    Unavailable,
    CapacityExceeded,
    Rejected,
    IdentityExhausted,
    ConflictingAcceptance,
    InvalidLeaseDuration,
    ClaimTokenExhausted,
    NotClaimed,
    StaleClaim,
    LeaseExpired,
}

/// Holds an accepted resolution stable during journal commitment.
///
/// The sink mutex remains locked for the lifetime of this guard.
pub struct ResolutionCommitGuard<'a> {
    state: MutexGuard<'a, SinkState>,
    receipt: ResolutionAcceptanceReceipt,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ResolutionClaimToken(pub u64);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolutionClaim {
    pub receipt: ResolutionAcceptanceReceipt,
    pub token: ResolutionClaimToken,
    pub expires_at: Instant,
}

#[derive(Debug, Clone, Copy)]
struct ResolutionClaimLease {
    token: ResolutionClaimToken,
    expires_at: Instant,
}

impl ResolutionCommitGuard<'_> {
    pub fn receipt(&self) -> &ResolutionAcceptanceReceipt {
        &self.receipt
    }
    /// Mark the accepted resolution as committed by its journal.
    ///
    /// Must be called only after the journal has completed its
    /// ownership-transfer operation.
    pub fn mark_journal_committed(&mut self) -> Result<(), ResolutionAcceptanceError> {
        let entry = self
            .state
            .entries
            .iter_mut()
            .find(|entry| entry.receipt == self.receipt)
            .ok_or(ResolutionAcceptanceError::Rejected)?;

        if entry.state != ResolutionSinkState::Accepted {
            return Err(ResolutionAcceptanceError::Rejected);
        }

        entry.state = ResolutionSinkState::JournalCommitted;

        Ok(())
    }
    pub fn can_commit(&self) -> bool {
        self.state.entries.iter().any(|entry| {
            entry.receipt == self.receipt && entry.state == ResolutionSinkState::Accepted
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolutionAcceptanceReceipt {
    sink_id: u64,
    sequence: u64,
    token: EscalationResolutionToken,
    record: EscalationResolutionRecord,
}

impl ResolutionAcceptanceReceipt {
    pub fn token(&self) -> EscalationResolutionToken {
        self.token
    }

    pub fn record(&self) -> &EscalationResolutionRecord {
        &self.record
    }
}

/// Accepts responsibility for an escalation-resolution record.
pub trait ResolutionAcceptanceSink: Send + Sync {
    fn accept(
        &self,
        token: EscalationResolutionToken,
        resolution: &EscalationResolutionRecord,
    ) -> Result<ResolutionAcceptanceReceipt, ResolutionAcceptanceError>;
}

#[derive(Debug)]
struct AcceptedResolution {
    receipt: ResolutionAcceptanceReceipt,
    state: ResolutionSinkState,
    lease: Option<ResolutionClaimLease>,
}

#[derive(Debug)]
struct SinkState {
    entries: VecDeque<AcceptedResolution>,
    next_sequence: Option<u64>,
    next_claim_token: Option<u64>,
    closed: bool,
}

#[derive(Debug)]
struct SinkInner {
    id: u64,
    capacity: usize,
    state: Mutex<SinkState>,
}

/// Bounded in-process resolution sink.
///
/// Each accepted record remains owned by the sink until drained.
#[derive(Debug, Clone)]
pub struct BoundedResolutionSink {
    inner: Arc<SinkInner>,
}

impl BoundedResolutionSink {
    pub fn new(capacity: usize) -> Result<Self, ResolutionAcceptanceError> {
        if capacity == 0 {
            return Err(ResolutionAcceptanceError::CapacityExceeded);
        }

        let id = NEXT_SINK_ID.fetch_add(1, Ordering::Relaxed);

        if id == u64::MAX {
            return Err(ResolutionAcceptanceError::IdentityExhausted);
        }

        Ok(Self {
            inner: Arc::new(SinkInner {
                id,
                capacity,
                state: Mutex::new(SinkState {
                    entries: VecDeque::new(),
                    next_sequence: Some(1),
                    closed: false,
                    next_claim_token: Some(1),
                }),
            }),
        })
    }
    /// Acquire a commitment guard for an accepted resolution.
    ///
    /// While this guard exists, the sink cannot acknowledge or remove
    /// the accepted record.
    pub fn guard_commitment(
        &self,
        receipt: &ResolutionAcceptanceReceipt,
    ) -> Result<ResolutionCommitGuard<'_>, ResolutionAcceptanceError> {
        if receipt.sink_id != self.inner.id {
            return Err(ResolutionAcceptanceError::Rejected);
        }

        let state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        if !state.entries.iter().any(|entry| entry.receipt == *receipt) {
            return Err(ResolutionAcceptanceError::Rejected);
        }

        Ok(ResolutionCommitGuard {
            state,
            receipt: receipt.clone(),
        })
    }
    pub fn len(&self) -> usize {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .entries
            .len()
    }
    /// Release an active claim after a recoverable processing failure.
    ///
    /// The resolution becomes eligible for another consumer immediately.
    pub fn release_claim(
        &self,
        receipt: &ResolutionAcceptanceReceipt,
        token: ResolutionClaimToken,
        clock: &dyn MonotonicClock,
    ) -> Result<(), ResolutionAcceptanceError> {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        let entry = state
            .entries
            .iter_mut()
            .find(|entry| entry.receipt == *receipt)
            .ok_or(ResolutionAcceptanceError::Rejected)?;

        if entry.state != ResolutionSinkState::Claimed {
            return Err(ResolutionAcceptanceError::NotClaimed);
        }

        let lease = entry.lease.ok_or(ResolutionAcceptanceError::NotClaimed)?;

        if lease.token != token {
            return Err(ResolutionAcceptanceError::StaleClaim);
        }

        if clock.now() >= lease.expires_at {
            return Err(ResolutionAcceptanceError::LeaseExpired);
        }

        entry.state = ResolutionSinkState::JournalCommitted;
        entry.lease = None;

        Ok(())
    }
    pub fn close(&self) {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .closed = true;
    }

    /// Claim an accepted resolution that has been journal-committed.
    ///
    /// Only records in the `JournalCommitted` state may be claimed.
    /// Claiming transitions the record to `Claimed` state.
    /// Claim the oldest eligible resolution.
    ///
    /// Only journal-committed records can be claimed.
    /// Expired claims may be reclaimed with a new fencing token.
    ///
    /// Claiming does not remove the accepted record or release capacity.
    pub fn try_take(
        &self,
        clock: &dyn MonotonicClock,
        lease_duration: Duration,
    ) -> Result<Option<ResolutionClaim>, ResolutionAcceptanceError> {
        if lease_duration.is_zero() {
            return Err(ResolutionAcceptanceError::InvalidLeaseDuration);
        }

        let now = clock.now();

        let expires_at = now
            .checked_add(lease_duration)
            .ok_or(ResolutionAcceptanceError::InvalidLeaseDuration)?;

        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        let Some(position) = state.entries.iter().position(|entry| match entry.state {
            ResolutionSinkState::Accepted => false,

            ResolutionSinkState::JournalCommitted => true,

            ResolutionSinkState::Claimed => {
                entry.lease.is_some_and(|lease| now >= lease.expires_at)
            }
        }) else {
            return Ok(None);
        };

        let next = state
            .next_claim_token
            .ok_or(ResolutionAcceptanceError::ClaimTokenExhausted)?;

        state.next_claim_token = next.checked_add(1);

        let token = ResolutionClaimToken(next);

        let entry = &mut state.entries[position];

        entry.state = ResolutionSinkState::Claimed;
        entry.lease = Some(ResolutionClaimLease { token, expires_at });

        Ok(Some(ResolutionClaim {
            receipt: entry.receipt.clone(),
            token,
            expires_at,
        }))
    }
    /// Acknowledge successful processing of a leased resolution.
    ///
    /// Only the current, unexpired claim token can remove the record.
    pub fn acknowledge(
        &self,
        receipt: &ResolutionAcceptanceReceipt,
        token: ResolutionClaimToken,
        clock: &dyn MonotonicClock,
    ) -> Result<(), ResolutionAcceptanceError> {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        let position = state
            .entries
            .iter()
            .position(|entry| entry.receipt == *receipt)
            .ok_or(ResolutionAcceptanceError::Rejected)?;

        let entry = &state.entries[position];

        if entry.state != ResolutionSinkState::Claimed {
            return Err(ResolutionAcceptanceError::NotClaimed);
        }

        let lease = entry.lease.ok_or(ResolutionAcceptanceError::NotClaimed)?;

        if lease.token != token {
            return Err(ResolutionAcceptanceError::StaleClaim);
        }

        if clock.now() >= lease.expires_at {
            return Err(ResolutionAcceptanceError::LeaseExpired);
        }

        state.entries.remove(position);

        Ok(())
    }
    /// Check that a receipt belongs to this sink and corresponds
    /// to an accepted record still retained by the sink.
    pub fn verify(&self, receipt: &ResolutionAcceptanceReceipt) -> bool {
        if receipt.sink_id != self.inner.id {
            return false;
        }

        let state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        state.entries.iter().any(|entry| entry.receipt == *receipt)
    }
}

impl ResolutionAcceptanceSink for BoundedResolutionSink {
    fn accept(
        &self,
        token: EscalationResolutionToken,
        resolution: &EscalationResolutionRecord,
    ) -> Result<ResolutionAcceptanceReceipt, ResolutionAcceptanceError> {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        if state.closed {
            return Err(ResolutionAcceptanceError::Unavailable);
        }

        // Idempotent acceptance for the same token and record.
        for entry in &state.entries {
            if entry.receipt.token == token {
                if entry.receipt.record == *resolution {
                    return Ok(entry.receipt.clone());
                }

                return Err(ResolutionAcceptanceError::ConflictingAcceptance);
            }
        }

        if state.entries.len() >= self.inner.capacity {
            return Err(ResolutionAcceptanceError::CapacityExceeded);
        }

        let sequence = state
            .next_sequence
            .ok_or(ResolutionAcceptanceError::IdentityExhausted)?;

        state.next_sequence = sequence.checked_add(1);

        let receipt = ResolutionAcceptanceReceipt {
            sink_id: self.inner.id,
            sequence,
            token,
            record: resolution.clone(),
        };

        state.entries.push_back(AcceptedResolution {
            receipt: receipt.clone(),
            state: ResolutionSinkState::Accepted,
            lease: None,
        });

        Ok(receipt)
    }
}
