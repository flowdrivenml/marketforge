use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use serde_json::Value;
use thiserror::Error;
use tokio::time::Instant;

use super::ack::{CompiledSubscriptionAck, ExpectedSubscriptionAck, SubscriptionAckClassification};
use super::identity::SubscriptionAckIdentity;
use crate::live::infrastructure::websocket::lifecycle::fence::{ConnectionFence, ConnectionStamp};

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
                crate::live::infrastructure::websocket::lifecycle::state::ConnectionState::Connected | crate::live::infrastructure::websocket::lifecycle::state::ConnectionState::Ready
            )
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use serde_json::json;
    use tokio::time::Instant;

    use crate::live::{
        config::exchange::ExchangeConfig,
        infrastructure::websocket::{
            ConnectionFence, ExpectedSubscriptionAck, SubscriptionAckCompiler,
            SubscriptionCompletionKind, SubscriptionDispatch, SubscriptionPendingError,
            SubscriptionPendingManager, SubscriptionTransactionPhase,
        },
    };

    fn ack_protocol() -> crate::live::infrastructure::websocket::CompiledSubscriptionAck {
        let config: ExchangeConfig = toml::from_str(
            r#"
schema_version = 1

[exchange]
id = "binance"
market = "spot"
profile = "binance_spot"

[websocket]
url = "wss://example.invalid"

[websocket.subscription]
strategy = "json_rpc"
request_id_field = "id"
success_field = "result"
success_is_null = true
error_field = "error"
error_code_field = "error.code"
error_message_field = "error.msg"

[channels.trades]
enabled = true
format = "trades/binance_spot"

[rest]
base_url = "https://example.invalid"
"#,
        )
        .unwrap();

        SubscriptionAckCompiler::compile(&config).unwrap()
    }

    fn manager() -> SubscriptionPendingManager {
        SubscriptionPendingManager::new(8, Duration::from_secs(2)).unwrap()
    }

    fn connected_fence() -> (
        ConnectionFence,
        crate::live::infrastructure::websocket::ConnectionStamp,
    ) {
        let fence = ConnectionFence::new("runtime-1", "connection-1").unwrap();

        let stamp = fence.begin_connect().unwrap();

        fence.connected(&stamp).unwrap();

        (fence, stamp)
    }

    #[test]
    fn ack_deadline_starts_after_transmission() {
        let (fence, stamp) = connected_fence();
        let mut pending = manager();

        let id = pending
            .reserve(
                &fence,
                &stamp,
                "BTCUSDT:trades",
                ExpectedSubscriptionAck::subscribe(Some(json!(1))),
            )
            .unwrap();

        assert_eq!(
            pending.phase(id),
            Some(SubscriptionTransactionPhase::Reserved),
        );

        assert_eq!(pending.next_deadline(), None);

        let sent_at = Instant::now();

        pending.mark_sent(&fence, &stamp, id, sent_at).unwrap();

        assert_eq!(
            pending.phase(id),
            Some(SubscriptionTransactionPhase::AwaitingAck),
        );

        assert_eq!(
            pending.next_deadline(),
            Some(sent_at + Duration::from_secs(2)),
        );
    }

    #[test]
    fn matching_ack_completes_correct_transaction() {
        let (fence, stamp) = connected_fence();
        let mut pending = manager();

        let first = pending
            .reserve(
                &fence,
                &stamp,
                "BTCUSDT:trades",
                ExpectedSubscriptionAck::subscribe(Some(json!(10))),
            )
            .unwrap();

        let second = pending
            .reserve(
                &fence,
                &stamp,
                "ETHUSDT:trades",
                ExpectedSubscriptionAck::subscribe(Some(json!(11))),
            )
            .unwrap();

        let now = Instant::now();

        pending.mark_sent(&fence, &stamp, first, now).unwrap();
        pending.mark_sent(&fence, &stamp, second, now).unwrap();

        let dispatch = pending
            .classify(
                &fence,
                &stamp,
                &ack_protocol(),
                &json!({"id": 11, "result": null}),
                now,
            )
            .unwrap();

        let SubscriptionDispatch::Completed(completion) = dispatch else {
            panic!("expected completed subscription");
        };

        assert_eq!(completion.transaction_id, second);
        assert_eq!(completion.stream_id, "ETHUSDT:trades");

        assert!(matches!(
            completion.kind,
            SubscriptionCompletionKind::Confirmed
        ));

        assert_eq!(pending.pending_count(), 1);

        assert_eq!(
            pending.phase(first),
            Some(SubscriptionTransactionPhase::AwaitingAck),
        );
    }

    #[test]
    fn late_ack_cannot_confirm_expired_transaction() {
        let (fence, stamp) = connected_fence();
        let mut pending = manager();

        let id = pending
            .reserve(
                &fence,
                &stamp,
                "BTCUSDT:trades",
                ExpectedSubscriptionAck::subscribe(Some(json!(1))),
            )
            .unwrap();

        let sent_at = Instant::now();

        pending.mark_sent(&fence, &stamp, id, sent_at).unwrap();

        let expired_at = sent_at + Duration::from_secs(2);

        let completions = pending.expire(&stamp, expired_at);

        assert_eq!(completions.len(), 1);

        assert!(matches!(
            completions[0].kind,
            SubscriptionCompletionKind::TimedOut
        ));

        let dispatch = pending
            .classify(
                &fence,
                &stamp,
                &ack_protocol(),
                &json!({"id": 1, "result": null}),
                expired_at,
            )
            .unwrap();

        assert!(matches!(dispatch, SubscriptionDispatch::Unrelated));

        assert_eq!(pending.pending_count(), 0);
    }

    #[test]
    fn cannot_confirm_ack_received_after_deadline() {
        let (fence, stamp) = connected_fence();
        let mut pending = manager();

        let id = pending
            .reserve(
                &fence,
                &stamp,
                "BTCUSDT:trades",
                ExpectedSubscriptionAck::subscribe(Some(json!(1))),
            )
            .unwrap();

        let sent_at = Instant::now();

        pending.mark_sent(&fence, &stamp, id, sent_at).unwrap();

        // Even if the caller hasn't collected the timeout
        // completion, classification must not accept a late ACK.
        let dispatch = pending
            .classify(
                &fence,
                &stamp,
                &ack_protocol(),
                &json!({"id": 1, "result": null}),
                sent_at + Duration::from_secs(3),
            )
            .unwrap();

        assert!(matches!(dispatch, SubscriptionDispatch::Unrelated));

        let completions = pending.expire(&stamp, sent_at + Duration::from_secs(3));

        assert_eq!(completions.len(), 1);
        assert!(matches!(
            completions[0].kind,
            SubscriptionCompletionKind::TimedOut
        ));
    }

    #[test]
    fn rejects_reused_request_ids() {
        let (fence, stamp) = connected_fence();
        let mut pending = manager();

        pending
            .reserve(
                &fence,
                &stamp,
                "BTCUSDT:trades",
                ExpectedSubscriptionAck::subscribe(Some(json!(7))),
            )
            .unwrap();

        assert!(matches!(
            pending.reserve(
                &fence,
                &stamp,
                "ETHUSDT:trades",
                ExpectedSubscriptionAck::subscribe(Some(json!(7))),
            ),
            Err(SubscriptionPendingError::RetiredRequestId)
        ));
    }

    #[test]
    fn idless_subscription_requires_exclusive_flight() {
        let (fence, stamp) = connected_fence();
        let mut pending = manager();

        pending
            .reserve(
                &fence,
                &stamp,
                "BTCUSDT:trades",
                ExpectedSubscriptionAck::subscribe(None),
            )
            .unwrap();

        assert!(matches!(
            pending.reserve(
                &fence,
                &stamp,
                "ETHUSDT:trades",
                ExpectedSubscriptionAck::subscribe(Some(json!(2))),
            ),
            Err(SubscriptionPendingError::SingleFlightRequired)
        ));
    }

    #[test]
    fn reconnect_invalidates_all_old_pending_requests() {
        let (fence, first_stamp) = connected_fence();
        let mut pending = manager();

        let id = pending
            .reserve(
                &fence,
                &first_stamp,
                "BTCUSDT:trades",
                ExpectedSubscriptionAck::subscribe(Some(json!(1))),
            )
            .unwrap();

        pending
            .mark_sent(&fence, &first_stamp, id, Instant::now())
            .unwrap();

        fence.failed(&first_stamp).unwrap();

        let completions = pending.invalidate_generation(&first_stamp);

        assert_eq!(completions.len(), 1);
        assert_eq!(pending.pending_count(), 0);

        let second_stamp = fence.begin_connect().unwrap();
        fence.connected(&second_stamp).unwrap();

        assert!(matches!(
            pending.reserve(
                &fence,
                &first_stamp,
                "BTCUSDT:trades",
                ExpectedSubscriptionAck::subscribe(Some(json!(2))),
            ),
            Err(SubscriptionPendingError::StaleConnection)
        ));

        assert!(
            pending
                .reserve(
                    &fence,
                    &second_stamp,
                    "BTCUSDT:trades",
                    ExpectedSubscriptionAck::subscribe(Some(json!(2))),
                )
                .is_ok()
        );
    }

    #[test]
    fn transmission_failure_releases_reservation() {
        let (fence, stamp) = connected_fence();
        let mut pending = manager();

        let id = pending
            .reserve(
                &fence,
                &stamp,
                "BTCUSDT:trades",
                ExpectedSubscriptionAck::subscribe(Some(json!(1))),
            )
            .unwrap();

        let completion = pending.cancel(id).unwrap();

        assert!(matches!(
            completion.kind,
            SubscriptionCompletionKind::Cancelled
        ));

        assert_eq!(pending.pending_count(), 0);
    }

    mod retirement {
        use std::time::Duration;

        use serde_json::json;
        use tokio::time::Instant;

        use crate::live::{
            config::exchange::ExchangeConfig,
            infrastructure::websocket::{
                CompiledSubscriptionAck, ConnectionFence, ConnectionStamp, ExpectedSubscriptionAck,
                SubscriptionAckCompiler, SubscriptionAckIdentity, SubscriptionCompletionKind,
                SubscriptionDispatch, SubscriptionPendingError, SubscriptionPendingManager,
            },
        };

        // ============================================================
        // HELPERS
        // ============================================================

        fn fence() -> (ConnectionFence, ConnectionStamp) {
            let fence = ConnectionFence::new("runtime-1", "slot-1").unwrap();

            let stamp = fence.begin_connect().unwrap();

            fence.connected(&stamp).unwrap();

            (fence, stamp)
        }

        fn pending() -> SubscriptionPendingManager {
            SubscriptionPendingManager::new(16, Duration::from_secs(2)).unwrap()
        }

        fn rpc_protocol() -> CompiledSubscriptionAck {
            let config: ExchangeConfig = toml::from_str(
                r#"
schema_version = 1

[exchange]
id = "binance"
market = "spot"
profile = "binance_spot"

[websocket]
url = "wss://example.invalid"

[websocket.subscription]
strategy = "json_rpc"
request_id_field = "id"
success_field = "result"
success_is_null = true

[channels.trades]
enabled = true
format = "trades/binance_spot"

[rest]
base_url = "https://example.invalid"
"#,
            )
            .unwrap();

            SubscriptionAckCompiler::compile(&config).unwrap()
        }

        fn native_protocol() -> CompiledSubscriptionAck {
            let config: ExchangeConfig = toml::from_str(
                r#"
schema_version = 1

[exchange]
id = "okx"
market = "spot"
profile = "okx_spot"

[websocket]
url = "wss://example.invalid"

[websocket.subscription]
strategy = "json_operation"
operation_field = "op"
arguments_field = "args"

request_id_field = "id"
request_id_type = "string"
request_id_optional = true

acknowledgement_field = "event"
subscribe_acknowledgement = "subscribe"
unsubscribe_acknowledgement = "unsubscribe"

[channels.trades]
enabled = true
format = "trades/okx_spot"

[rest]
base_url = "https://example.invalid"
"#,
            )
            .unwrap();

            SubscriptionAckCompiler::compile(&config).unwrap()
        }

        fn identity(instrument: &str) -> SubscriptionAckIdentity {
            SubscriptionAckIdentity::builder()
                .field("arg.channel", json!("trades-all"))
                .unwrap()
                .field("arg.instId", json!(instrument))
                .unwrap()
                .build()
                .unwrap()
        }

        // ============================================================
        // REQUEST-ID RETIREMENT
        // ============================================================

        #[test]
        fn completed_request_id_cannot_be_reused() {
            let (fence, stamp) = fence();

            let mut pending = pending();

            let first = pending
                .reserve(
                    &fence,
                    &stamp,
                    "BTC:trades",
                    ExpectedSubscriptionAck::subscribe(Some(json!(42))),
                )
                .unwrap();

            pending
                .mark_sent(&fence, &stamp, first, Instant::now())
                .unwrap();

            let result = pending
                .classify(
                    &fence,
                    &stamp,
                    &rpc_protocol(),
                    &json!({
                        "id": 42,
                        "result": null
                    }),
                    Instant::now(),
                )
                .unwrap();

            assert!(matches!(result, SubscriptionDispatch::Completed(_)));

            assert_eq!(pending.pending_count(), 0);

            assert!(matches!(
                pending.reserve(
                    &fence,
                    &stamp,
                    "ETH:trades",
                    ExpectedSubscriptionAck::subscribe(Some(json!(42)),),
                ),
                Err(SubscriptionPendingError::RetiredRequestId)
            ));

            assert_eq!(pending.retired_request_id_count(), 1);
        }

        #[test]
        fn timed_out_request_id_remains_retired() {
            let (fence, stamp) = fence();

            let mut pending = pending();

            let id = pending
                .reserve(
                    &fence,
                    &stamp,
                    "BTC:trades",
                    ExpectedSubscriptionAck::subscribe(Some(json!(7))),
                )
                .unwrap();

            let sent_at = Instant::now();

            pending.mark_sent(&fence, &stamp, id, sent_at).unwrap();

            let completions = pending.expire(&stamp, sent_at + Duration::from_secs(2));

            assert_eq!(completions.len(), 1);

            assert!(matches!(
                completions[0].kind,
                SubscriptionCompletionKind::TimedOut
            ));

            assert!(matches!(
                pending.reserve(
                    &fence,
                    &stamp,
                    "ETH:trades",
                    ExpectedSubscriptionAck::subscribe(Some(json!(7)),),
                ),
                Err(SubscriptionPendingError::RetiredRequestId)
            ));

            // A new request ID remains admissible.
            assert!(
                pending
                    .reserve(
                        &fence,
                        &stamp,
                        "ETH:trades",
                        ExpectedSubscriptionAck::subscribe(Some(json!(8)),),
                    )
                    .is_ok()
            );
        }

        #[test]
        fn cancelled_request_id_remains_retired() {
            let (fence, stamp) = fence();

            let mut pending = pending();

            let id = pending
                .reserve(
                    &fence,
                    &stamp,
                    "BTC:trades",
                    ExpectedSubscriptionAck::subscribe(Some(json!("req-1"))),
                )
                .unwrap();

            pending.cancel(id).unwrap();

            assert!(matches!(
                pending.reserve(
                    &fence,
                    &stamp,
                    "ETH:trades",
                    ExpectedSubscriptionAck::subscribe(Some(json!("req-1")),),
                ),
                Err(SubscriptionPendingError::RetiredRequestId)
            ));
        }

        // ============================================================
        // ID-LESS SUBSCRIPTIONS
        // ============================================================

        #[test]
        fn idless_timeout_requires_reconnect() {
            let (fence, stamp) = fence();

            let mut pending = pending();

            let id = pending
                .reserve(
                    &fence,
                    &stamp,
                    "BTC:trades",
                    ExpectedSubscriptionAck::subscribe(None).with_identity(identity("BTC-USDT")),
                )
                .unwrap();

            let sent_at = Instant::now();

            pending.mark_sent(&fence, &stamp, id, sent_at).unwrap();

            pending.expire(&stamp, sent_at + Duration::from_secs(2));

            assert!(pending.reconnect_required());

            assert!(matches!(
                pending.reserve(
                    &fence,
                    &stamp,
                    "BTC:trades",
                    ExpectedSubscriptionAck::subscribe(None).with_identity(identity("BTC-USDT")),
                ),
                Err(SubscriptionPendingError::ReconnectRequired)
            ));
        }

        #[test]
        fn idless_identity_cannot_be_reused() {
            let (fence, stamp) = fence();

            let mut pending = pending();

            let id = pending
                .reserve(
                    &fence,
                    &stamp,
                    "BTC:trades",
                    ExpectedSubscriptionAck::subscribe(None).with_identity(identity("BTC-USDT")),
                )
                .unwrap();

            pending
                .mark_sent(&fence, &stamp, id, Instant::now())
                .unwrap();

            let result = pending
                .classify(
                    &fence,
                    &stamp,
                    &native_protocol(),
                    &json!({
                        "event": "subscribe",
                        "arg": {
                            "channel": "trades-all",
                            "instId": "BTC-USDT"
                        }
                    }),
                    Instant::now(),
                )
                .unwrap();

            assert!(matches!(result, SubscriptionDispatch::Completed(_)));

            assert!(matches!(
                pending.reserve(
                    &fence,
                    &stamp,
                    "BTC:trades",
                    ExpectedSubscriptionAck::subscribe(None).with_identity(identity("BTC-USDT")),
                ),
                Err(SubscriptionPendingError::RetiredIdlessIdentity)
            ));

            // Another native identity is independently
            // correlatable.
            assert!(
                pending
                    .reserve(
                        &fence,
                        &stamp,
                        "ETH:trades",
                        ExpectedSubscriptionAck::subscribe(None)
                            .with_identity(identity("ETH-USDT")),
                    )
                    .is_ok()
            );
        }

        #[test]
        fn idless_without_identity_cannot_be_followed_by_another() {
            let (fence, stamp) = fence();

            let mut pending = pending();

            let first = pending
                .reserve(
                    &fence,
                    &stamp,
                    "BTC:trades",
                    ExpectedSubscriptionAck::subscribe(None),
                )
                .unwrap();

            // Even after cancellation, the previous request
            // might have reached the exchange.
            pending.cancel(first).unwrap();

            assert!(matches!(
                pending.reserve(
                    &fence,
                    &stamp,
                    "ETH:trades",
                    ExpectedSubscriptionAck::subscribe(None).with_identity(identity("ETH-USDT")),
                ),
                Err(SubscriptionPendingError::ReconnectRequired)
            ));
        }

        // ============================================================
        // AMBIGUOUS CORRELATION
        // ============================================================

        #[test]
        fn ambiguous_ack_cannot_select_first_pending_entry() {
            let (fence, stamp) = fence();

            let mut pending = pending();

            let first = pending
                .reserve(
                    &fence,
                    &stamp,
                    "BTC:trades",
                    ExpectedSubscriptionAck::subscribe(Some(json!(10))),
                )
                .unwrap();

            let second = pending
                .reserve(
                    &fence,
                    &stamp,
                    "ETH:trades",
                    ExpectedSubscriptionAck::subscribe(Some(json!(11))),
                )
                .unwrap();

            let sent_at = Instant::now();

            pending.mark_sent(&fence, &stamp, first, sent_at).unwrap();

            pending.mark_sent(&fence, &stamp, second, sent_at).unwrap();

            // An event-only response with no request ID can
            // resemble multiple outstanding subscriptions.
            //
            // It must never complete the first transaction
            // merely because its map key sorts earlier.
            let result = pending.classify(
                &fence,
                &stamp,
                &native_protocol(),
                &json!({
                    "event": "subscribe"
                }),
                sent_at + Duration::from_millis(1),
            );

            assert!(matches!(
                result,
                Err(SubscriptionPendingError::AmbiguousCorrelation)
            ));

            assert_eq!(pending.pending_count(), 2);

            assert!(pending.reconnect_required());
        }

        #[test]
        fn wrong_request_id_does_not_consume_pending_transaction() {
            let (fence, stamp) = fence();

            let mut pending = pending();

            let id = pending
                .reserve(
                    &fence,
                    &stamp,
                    "BTC:trades",
                    ExpectedSubscriptionAck::subscribe(Some(json!(42))),
                )
                .unwrap();

            pending
                .mark_sent(&fence, &stamp, id, Instant::now())
                .unwrap();

            let dispatch = pending
                .classify(
                    &fence,
                    &stamp,
                    &rpc_protocol(),
                    &json!({
                        "id": 43,
                        "result": null
                    }),
                    Instant::now(),
                )
                .unwrap();

            assert!(matches!(dispatch, SubscriptionDispatch::Unrelated));

            assert_eq!(pending.pending_count(), 1);
        }
    }
}
