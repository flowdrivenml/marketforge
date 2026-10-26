use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use serde_json::Value;
use thiserror::Error;
use tokio::time::Instant;

use super::{
    CompiledSubscriptionAck, ConnectionFence, ConnectionStamp, ExpectedSubscriptionAck,
    SubscriptionAckClassification, SubscriptionAckIdentity,
};

// ============================================================
// TRANSACTION IDENTITY
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SubscriptionTransactionId(u64);

impl SubscriptionTransactionId {
    pub fn get(self) -> u64 {
        self.0
    }
}

// ============================================================
// TRANSACTION STATE
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscriptionTransactionPhase {
    Reserved,
    AwaitingAck,
}

#[derive(Debug, Clone)]
struct PendingSubscription {
    stamp: ConnectionStamp,
    stream_id: String,
    expected: ExpectedSubscriptionAck,
    phase: SubscriptionTransactionPhase,
    deadline: Option<Instant>,
}

// ============================================================
// COMPLETION
// ============================================================

#[derive(Debug, Clone)]
pub enum SubscriptionCompletionKind {
    Confirmed,

    Rejected {
        code: Option<String>,
        message: Option<String>,
    },

    Ambiguous,

    TimedOut,

    Cancelled,
}

#[derive(Debug, Clone)]
pub struct SubscriptionCompletion {
    pub transaction_id: SubscriptionTransactionId,
    pub stamp: ConnectionStamp,
    pub stream_id: String,
    pub kind: SubscriptionCompletionKind,
}

// ============================================================
// INCOMING RESPONSE CLASSIFICATION
// ============================================================

#[derive(Debug, Clone)]
pub enum SubscriptionDispatch {
    /// No pending operation consumed this message.
    ///
    /// The caller must pass it to the normal message router.
    Unrelated,

    /// An ACK transaction completed.
    ///
    /// The caller must not route this as market data.
    Completed(SubscriptionCompletion),
}

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, Error)]
pub enum SubscriptionPendingError {
    #[error("maximum pending subscriptions must be positive")]
    InvalidCapacity,

    #[error("subscription ACK timeout must be positive")]
    InvalidTimeout,

    #[error("subscription transaction identifier exhausted")]
    TransactionIdExhausted,

    #[error("subscription pending capacity exhausted")]
    CapacityExceeded,

    #[error("another subscription without a request ID is pending")]
    SingleFlightRequired,

    #[error("duplicate subscription request ID")]
    DuplicateRequestId,

    #[error("subscription already pending for this stream")]
    StreamBusy,

    #[error("connection generation is not current")]
    StaleConnection,

    #[error("subscription transaction not found")]
    UnknownTransaction,

    #[error("subscription transaction belongs to another generation")]
    GenerationMismatch,

    #[error("subscription transaction already marked sent")]
    AlreadySent,
    #[error("subscription request ID was already used on this connection")]
    RetiredRequestId,

    #[error("subscription request-ID retirement capacity exhausted")]
    RetirementCapacityExceeded,

    #[error("subscription requires a fresh physical connection")]
    ReconnectRequired,

    #[error("ID-less subscription identity was previously used")]
    RetiredIdlessIdentity,

    #[error("incoming ACK matches multiple pending transactions")]
    AmbiguousCorrelation,
}

// ============================================================
// TRANSACTION MANAGER
// ============================================================

/// Owns pending subscription operations for one connection slot.
///
/// This type does not own the WebSocket, subscriptions, or
/// StreamRecoveryCoordinator.
///
/// All state changes are synchronous and must be serialized
/// by the connection/session owner.
///
/// The caller must:
/// - Reserve before enqueueing a subscription.
/// - Mark sent only after successful socket transmission.
/// - Cancel reservations when transmission fails.
/// - Invalidate outstanding operations on generation change.
/// - Apply confirmed completions to the recovery coordinator
///   only after checking their current recovery token.
#[derive(Debug)]
pub struct SubscriptionPendingManager {
    next_id: u64,

    maximum_pending: usize,
    acknowledgment_timeout: Duration,

    pending: BTreeMap<SubscriptionTransactionId, PendingSubscription>,

    /// Request IDs are retired immediately upon reservation.
    ///
    /// A cancelled or timed-out request can therefore never
    /// have its identifier reused within this manager.
    retired_request_ids: BTreeSet<String>,

    /// Native identities previously reserved without a
    /// request ID.
    ///
    /// None means the historical request had no reliable
    /// native identity and cannot be disambiguated from
    /// later ID-less operations.
    idless_history: Vec<Option<SubscriptionAckIdentity>>,

    /// An uncertain ID-less transaction requires recycling
    /// the physical connection before further subscription
    /// operations are admitted.
    reconnect_required: bool,

    /// Hard upper bound on retained correlation identities.
    maximum_retired: usize,
}

impl SubscriptionPendingManager {
    pub fn new(
        maximum_pending: usize,
        acknowledgment_timeout: Duration,
    ) -> Result<Self, SubscriptionPendingError> {
        if maximum_pending == 0 {
            return Err(SubscriptionPendingError::InvalidCapacity);
        }

        if acknowledgment_timeout.is_zero() {
            return Err(SubscriptionPendingError::InvalidTimeout);
        }

        Ok(Self {
            next_id: 0,
            maximum_pending,
            acknowledgment_timeout,

            pending: BTreeMap::new(),

            retired_request_ids: BTreeSet::new(),
            idless_history: Vec::new(),
            reconnect_required: false,

            // Operational memory bound.
            //
            // Exhaustion requires a fresh physical connection.
            // This is not an exchange-imposed quota.
            maximum_retired: 65_536,
        })
    }
    pub fn reconnect_required(&self) -> bool {
        self.reconnect_required
    }

    pub fn retired_request_id_count(&self) -> usize {
        self.retired_request_ids.len()
    }

    pub fn retired_idless_identity_count(&self) -> usize {
        self.idless_history.len()
    }
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    pub fn phase(&self, id: SubscriptionTransactionId) -> Option<SubscriptionTransactionPhase> {
        self.pending.get(&id).map(|entry| entry.phase)
    }

    // ========================================================
    // RESERVE BEFORE TRANSMISSION
    // ========================================================

    pub fn reserve(
        &mut self,
        fence: &ConnectionFence,
        stamp: &ConnectionStamp,
        stream_id: impl Into<String>,
        expected: ExpectedSubscriptionAck,
    ) -> Result<SubscriptionTransactionId, SubscriptionPendingError> {
        let stream_id = stream_id.into();

        if !self.current_connection(fence, stamp) {
            return Err(SubscriptionPendingError::StaleConnection);
        }

        // An uncertain ID-less subscription outcome makes
        // the connection unsuitable for further subscription
        // transactions.
        if self.reconnect_required {
            return Err(SubscriptionPendingError::ReconnectRequired);
        }

        if stream_id.trim().is_empty() {
            return Err(SubscriptionPendingError::StreamBusy);
        }

        if self.pending.len() >= self.maximum_pending {
            return Err(SubscriptionPendingError::CapacityExceeded);
        }

        // ----------------------------------------------------
        // SINGLE-FLIGHT ADMISSION
        // ----------------------------------------------------

        let idless = expected.request_id.is_none();

        if self
            .pending
            .values()
            .any(|entry| entry.expected.request_id.is_none())
            || (idless && !self.pending.is_empty())
        {
            return Err(SubscriptionPendingError::SingleFlightRequired);
        }

        // ----------------------------------------------------
        // STREAM OWNERSHIP
        // ----------------------------------------------------

        if self
            .pending
            .values()
            .any(|entry| entry.stamp == *stamp && entry.stream_id == stream_id)
        {
            return Err(SubscriptionPendingError::StreamBusy);
        }

        // ----------------------------------------------------
        // REQUEST-ID RETIREMENT
        // ----------------------------------------------------

        let request_id_key = expected
            .request_id
            .as_ref()
            .map(|id| serde_json::to_string(id).expect("serde_json::Value serializes"));

        if let Some(key) = &request_id_key {
            if self.retired_request_ids.contains(key) {
                return Err(SubscriptionPendingError::RetiredRequestId);
            }
        }

        // ----------------------------------------------------
        // ID-LESS IDENTITY RETIREMENT
        // ----------------------------------------------------

        if idless {
            for previous in &self.idless_history {
                match (previous.as_ref(), expected.identity.as_ref()) {
                    // An earlier ID-less request without an
                    // identity could produce a late generic
                    // ACK indistinguishable from this request.
                    (None, _) | (_, None) => {
                        return Err(SubscriptionPendingError::ReconnectRequired);
                    }

                    (Some(previous), Some(current)) if previous == current => {
                        return Err(SubscriptionPendingError::RetiredIdlessIdentity);
                    }

                    // Different complete native identities
                    // remain distinguishable, provided the
                    // ACK protocol actually echoes those fields.
                    (Some(_), Some(_)) => {}
                }
            }
        }

        // ----------------------------------------------------
        // BOUNDED HISTORY
        // ----------------------------------------------------

        let retained = self
            .retired_request_ids
            .len()
            .saturating_add(self.idless_history.len());

        if retained >= self.maximum_retired {
            return Err(SubscriptionPendingError::RetirementCapacityExceeded);
        }

        // ----------------------------------------------------
        // ALLOCATE TRANSACTION ID
        // ----------------------------------------------------

        let next = self
            .next_id
            .checked_add(1)
            .ok_or(SubscriptionPendingError::TransactionIdExhausted)?;

        let id = SubscriptionTransactionId(next);

        // ----------------------------------------------------
        // COMMIT RETIREMENT AND RESERVATION
        // ----------------------------------------------------

        let mut expected = expected;

        // Single-flight is enforced by this manager.
        expected.single_flight = idless;

        if let Some(key) = request_id_key {
            self.retired_request_ids.insert(key);
        } else {
            self.idless_history.push(expected.identity.clone());
        }

        self.next_id = next;

        self.pending.insert(
            id,
            PendingSubscription {
                stamp: stamp.clone(),
                stream_id,
                expected,
                phase: SubscriptionTransactionPhase::Reserved,
                deadline: None,
            },
        );

        Ok(id)
    }

    // ========================================================
    // ACTUAL SOCKET TRANSMISSION COMPLETED
    // ========================================================

    pub fn mark_sent(
        &mut self,
        fence: &ConnectionFence,
        stamp: &ConnectionStamp,
        id: SubscriptionTransactionId,
        sent_at: Instant,
    ) -> Result<(), SubscriptionPendingError> {
        if !self.current_connection(fence, stamp) {
            return Err(SubscriptionPendingError::StaleConnection);
        }

        let entry = self
            .pending
            .get_mut(&id)
            .ok_or(SubscriptionPendingError::UnknownTransaction)?;

        if entry.stamp != *stamp {
            return Err(SubscriptionPendingError::GenerationMismatch);
        }

        if entry.phase != SubscriptionTransactionPhase::Reserved {
            return Err(SubscriptionPendingError::AlreadySent);
        }

        entry.phase = SubscriptionTransactionPhase::AwaitingAck;
        entry.deadline = Some(sent_at + self.acknowledgment_timeout);

        Ok(())
    }

    // ========================================================
    // CLASSIFY ONE INCOMING MESSAGE
    // ========================================================

    pub fn classify(
        &mut self,
        fence: &ConnectionFence,
        stamp: &ConnectionStamp,
        protocol: &CompiledSubscriptionAck,
        message: &Value,
        now: Instant,
    ) -> Result<SubscriptionDispatch, SubscriptionPendingError> {
        if !self.current_connection(fence, stamp) {
            return Err(SubscriptionPendingError::StaleConnection);
        }

        // ----------------------------------------------------
        // COLLECT EVERY CANDIDATE
        // ----------------------------------------------------

        let mut candidates = Vec::new();

        for (&id, pending) in &self.pending {
            if pending.stamp != *stamp {
                continue;
            }

            // Only actually transmitted requests can
            // receive an authoritative exchange ACK.
            if pending.phase != SubscriptionTransactionPhase::AwaitingAck {
                continue;
            }

            // An expired transaction may never be confirmed,
            // even if the expiration timer has not yet
            // removed it from the pending map.
            if pending.deadline.is_some_and(|deadline| now >= deadline) {
                continue;
            }

            let classification = protocol.classify(message, &pending.expected);

            if classification != SubscriptionAckClassification::Unrelated {
                candidates.push((id, classification));
            }
        }

        // ----------------------------------------------------
        // NO MATCH
        // ----------------------------------------------------

        if candidates.is_empty() {
            return Ok(SubscriptionDispatch::Unrelated);
        }

        // ----------------------------------------------------
        // MULTIPLE MATCHES
        // ----------------------------------------------------

        if candidates.len() > 1 {
            // Do not select a transaction based on map order.
            //
            // The connection owner must treat this error as
            // fatal protocol uncertainty and recycle the
            // physical connection.
            self.reconnect_required = true;

            return Err(SubscriptionPendingError::AmbiguousCorrelation);
        }

        // ----------------------------------------------------
        // EXACTLY ONE CANDIDATE
        // ----------------------------------------------------

        let (id, classification) = candidates.pop().expect("one candidate");

        let pending = self
            .pending
            .remove(&id)
            .expect("selected transaction exists");

        let kind = match classification {
            SubscriptionAckClassification::Confirmed => SubscriptionCompletionKind::Confirmed,

            SubscriptionAckClassification::Rejected { code, message } => {
                SubscriptionCompletionKind::Rejected { code, message }
            }

            SubscriptionAckClassification::Ambiguous => {
                // A response matching one transaction but
                // contradicting required ACK semantics is
                // an uncertain outcome.
                //
                // We must not retry ID-less operations on
                // this physical connection.
                if pending.expected.request_id.is_none() {
                    self.reconnect_required = true;
                }

                SubscriptionCompletionKind::Ambiguous
            }

            SubscriptionAckClassification::Unrelated => {
                unreachable!()
            }
        };

        Ok(SubscriptionDispatch::Completed(SubscriptionCompletion {
            transaction_id: id,
            stamp: pending.stamp,
            stream_id: pending.stream_id,
            kind,
        }))
    }
    // ========================================================
    // TIMEOUTS
    // ========================================================

    /// Return the earliest active ACK deadline.
    ///
    /// Reserved requests have no ACK deadline because
    /// they have not yet been transmitted.
    pub fn next_deadline(&self) -> Option<Instant> {
        self.pending
            .values()
            .filter_map(|entry| entry.deadline)
            .min()
    }

    /// Expire all requests whose ACK deadline has passed.
    ///
    /// The returned completions must be delivered to the
    /// subscription owner; they are not acknowledgments.
    pub fn expire(&mut self, stamp: &ConnectionStamp, now: Instant) -> Vec<SubscriptionCompletion> {
        let expired: Vec<_> = self
            .pending
            .iter()
            .filter_map(|(&id, entry)| {
                (entry.stamp == *stamp && entry.deadline.is_some_and(|deadline| now >= deadline))
                    .then_some(id)
            })
            .collect();

        expired
            .into_iter()
            .filter_map(|id| self.complete(id, SubscriptionCompletionKind::TimedOut))
            .collect()
    }

    // ========================================================
    // EXPLICIT CANCEL / RECONNECT INVALIDATION
    // ========================================================

    pub fn cancel(
        &mut self,
        id: SubscriptionTransactionId,
    ) -> Result<SubscriptionCompletion, SubscriptionPendingError> {
        self.complete(id, SubscriptionCompletionKind::Cancelled)
            .ok_or(SubscriptionPendingError::UnknownTransaction)
    }

    /// Clear all transactions associated with a revoked
    /// physical connection generation.
    pub fn invalidate_generation(
        &mut self,
        stamp: &ConnectionStamp,
    ) -> Vec<SubscriptionCompletion> {
        let ids: Vec<_> = self
            .pending
            .iter()
            .filter_map(|(&id, entry)| (&entry.stamp == stamp).then_some(id))
            .collect();

        ids.into_iter()
            .filter_map(|id| self.complete(id, SubscriptionCompletionKind::Cancelled))
            .collect()
    }

    // ========================================================
    // INTERNAL HELPERS
    // ========================================================

    fn complete(
        &mut self,
        id: SubscriptionTransactionId,
        kind: SubscriptionCompletionKind,
    ) -> Option<SubscriptionCompletion> {
        let entry = self.pending.remove(&id)?;

        // For an ID-less subscription, an uncertain outcome
        // can leave a late ACK in flight.
        //
        // Recycling the physical connection is the safe
        // recovery boundary.
        if entry.expected.request_id.is_none()
            && matches!(
                kind,
                SubscriptionCompletionKind::TimedOut
                    | SubscriptionCompletionKind::Cancelled
                    | SubscriptionCompletionKind::Ambiguous
            )
        {
            self.reconnect_required = true;
        }

        Some(SubscriptionCompletion {
            transaction_id: id,
            stamp: entry.stamp,
            stream_id: entry.stream_id,
            kind,
        })
    }

    fn current_connection(&self, fence: &ConnectionFence, stamp: &ConnectionStamp) -> bool {
        fence.current_stamp().as_ref() == Some(stamp)
            && matches!(
                fence.state(),
                super::ConnectionState::Connected | super::ConnectionState::Ready
            )
    }
}
