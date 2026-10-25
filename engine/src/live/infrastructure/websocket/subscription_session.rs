use std::{collections::BTreeMap, time::Duration};

use serde_json::Value;
use thiserror::Error;
use tokio::time::Instant;

use super::{
    CompiledSubscriptionAck, ConnectionFence, ConnectionStamp, ExpectedSubscriptionAck,
    StreamRecoveryCoordinator, StreamRecoveryError, StreamRecoveryPhase, StreamRecoveryToken,
    SubscriptionCompletion, SubscriptionCompletionKind, SubscriptionDispatch,
    SubscriptionPendingError, SubscriptionPendingManager, SubscriptionSendReceipt,
    SubscriptionTransactionId,
};

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, Error)]
pub enum SubscriptionSessionError {
    #[error("session connection generation is no longer current")]
    StaleConnection,

    #[error("subscription recovery token is no longer current")]
    StaleRecoveryToken,

    #[error("subscription is not awaiting confirmation")]
    InvalidRecoveryPhase,

    #[error("transmission receipt has an unexpected identity")]
    InvalidReceipt,

    #[error(transparent)]
    Pending(#[from] SubscriptionPendingError),

    #[error(transparent)]
    Recovery(#[from] StreamRecoveryError),
}

// ============================================================
// EVENTS
// ============================================================

#[derive(Debug)]
pub enum SubscriptionSessionEvent {
    Unrelated,

    /// The subscription was acknowledged, and its
    /// recovery token advanced to AwaitingRecovery.
    Confirmed {
        transaction_id: SubscriptionTransactionId,
        token: StreamRecoveryToken,
    },

    /// Rejected, ambiguous, timed out, or cancelled.
    ///
    /// No recovery confirmation is granted.
    Failed {
        completion: SubscriptionCompletion,
    },
}

// ============================================================
// SESSION
// ============================================================

/// Subscription transaction authority for one physical
/// WebSocket generation.
///
/// The owner must serialize access to this object.
///
/// Does not own the socket or normalize market data.
///
/// Recovery transitions are delegated to the existing
/// StreamRecoveryCoordinator.
#[derive(Debug)]
pub struct SubscriptionSession {
    fence: ConnectionFence,
    stamp: ConnectionStamp,

    pending: SubscriptionPendingManager,

    /// Associates transactions with exact stream epochs.
    tokens: BTreeMap<SubscriptionTransactionId, StreamRecoveryToken>,
}

impl SubscriptionSession {
    pub fn new(
        fence: ConnectionFence,
        stamp: ConnectionStamp,
        maximum_pending: usize,
        ack_timeout: Duration,
    ) -> Result<Self, SubscriptionSessionError> {
        if fence.current_stamp().as_ref() != Some(&stamp) {
            return Err(SubscriptionSessionError::StaleConnection);
        }

        Ok(Self {
            fence,
            stamp,
            pending: SubscriptionPendingManager::new(maximum_pending, ack_timeout)?,
            tokens: BTreeMap::new(),
        })
    }
    pub fn pending_token(&self, id: SubscriptionTransactionId) -> Option<StreamRecoveryToken> {
        self.tokens.get(&id).cloned()
    }
    pub fn pending_count(&self) -> usize {
        self.pending.pending_count()
    }

    pub fn next_deadline(&self) -> Option<Instant> {
        self.pending.next_deadline()
    }

    // ========================================================
    // RESERVATION
    // ========================================================

    /// Reserve a subscription transaction bound to one
    /// exact recovery token.
    ///
    /// The caller must serialize and enqueue the native
    /// request after this succeeds.
    pub fn reserve(
        &mut self,
        recovery: &StreamRecoveryCoordinator,
        token: &StreamRecoveryToken,
        expected: ExpectedSubscriptionAck,
    ) -> Result<SubscriptionTransactionId, SubscriptionSessionError> {
        self.require_connection()?;

        if token.connection() != &self.stamp {
            return Err(SubscriptionSessionError::StaleRecoveryToken);
        }

        if recovery.token(token.stream_id()) != Some(token) {
            return Err(SubscriptionSessionError::StaleRecoveryToken);
        }

        if recovery.phase(token.stream_id()) != Some(StreamRecoveryPhase::AwaitingSubscription) {
            return Err(SubscriptionSessionError::InvalidRecoveryPhase);
        }

        let id = self
            .pending
            .reserve(&self.fence, &self.stamp, token.stream_id(), expected)?;

        self.tokens.insert(id, token.clone());

        Ok(id)
    }

    // ========================================================
    // TRANSMISSION RECEIPT
    // ========================================================

    /// Invoked after the actual socket writer reports
    /// successful transmission.
    ///
    /// This does not confirm the subscription.
    pub fn transmitted(
        &mut self,
        receipt: SubscriptionSendReceipt,
    ) -> Result<(), SubscriptionSessionError> {
        self.require_connection()?;

        if receipt.stamp != self.stamp || !self.tokens.contains_key(&receipt.transaction_id) {
            return Err(SubscriptionSessionError::InvalidReceipt);
        }

        self.pending.mark_sent(
            &self.fence,
            &self.stamp,
            receipt.transaction_id,
            receipt.sent_at,
        )?;

        Ok(())
    }

    // ========================================================
    // EXCHANGE RESPONSE
    // ========================================================

    /// Classify one native exchange message and apply
    /// confirmed subscription transitions.
    ///
    /// The caller must deliver transmission receipts
    /// before classifying the associated ACK.
    pub fn on_message(
        &mut self,
        recovery: &mut StreamRecoveryCoordinator,
        protocol: &CompiledSubscriptionAck,
        message: &Value,
        now: Instant,
    ) -> Result<SubscriptionSessionEvent, SubscriptionSessionError> {
        self.require_connection()?;

        let dispatch = self
            .pending
            .classify(&self.fence, &self.stamp, protocol, message, now)?;

        let SubscriptionDispatch::Completed(completion) = dispatch else {
            return Ok(SubscriptionSessionEvent::Unrelated);
        };

        let token = self.tokens.remove(&completion.transaction_id);

        match completion.kind {
            SubscriptionCompletionKind::Confirmed => {
                let token = token.ok_or(SubscriptionSessionError::StaleRecoveryToken)?;

                // Fail closed if stream rebinding occurred
                // while the subscription was pending.
                if recovery.token(token.stream_id()) != Some(&token) {
                    return Err(SubscriptionSessionError::StaleRecoveryToken);
                }

                recovery.subscription_confirmed(&self.fence, &token)?;

                Ok(SubscriptionSessionEvent::Confirmed {
                    transaction_id: completion.transaction_id,
                    token,
                })
            }

            _ => Ok(SubscriptionSessionEvent::Failed { completion }),
        }
    }

    // ========================================================
    // DEADLINES
    // ========================================================

    /// Expire transactions and release recovery-token
    /// associations.
    ///
    /// Call this when the next ACK deadline is reached.
    pub fn expire(&mut self, now: Instant) -> Vec<SubscriptionCompletion> {
        let completions = self.pending.expire(&self.stamp, now);

        for completion in &completions {
            self.tokens.remove(&completion.transaction_id);
        }

        completions
    }

    // ========================================================
    // TRANSMISSION FAILURE
    // ========================================================

    /// Cancel a transaction whose transmission failed.
    pub fn cancel(
        &mut self,
        id: SubscriptionTransactionId,
    ) -> Result<SubscriptionCompletion, SubscriptionSessionError> {
        let completion = self.pending.cancel(id)?;

        self.tokens.remove(&id);

        Ok(completion)
    }

    // ========================================================
    // GENERATION INVALIDATION
    // ========================================================

    /// Revoke all pending operations after the physical
    /// connection is invalidated.
    pub fn invalidate(&mut self) -> Vec<SubscriptionCompletion> {
        let completions = self.pending.invalidate_generation(&self.stamp);

        self.tokens.clear();

        completions
    }

    // ========================================================
    // INTERNAL VALIDATION
    // ========================================================

    fn require_connection(&self) -> Result<(), SubscriptionSessionError> {
        if self.fence.current_stamp().as_ref() != Some(&self.stamp) {
            return Err(SubscriptionSessionError::StaleConnection);
        }

        if !matches!(
            self.fence.state(),
            super::ConnectionState::Connected | super::ConnectionState::Ready
        ) {
            return Err(SubscriptionSessionError::StaleConnection);
        }

        Ok(())
    }
}
