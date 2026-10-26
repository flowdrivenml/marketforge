use crate::live::infrastructure::websocket::lifecycle::fence::{ConnectionFence, ConnectionStamp};
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;

// ============================================================
// IDENTITIES
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryStreamKind {
    Trades,
    Depth,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamRecoveryPhase {
    AwaitingSubscription,
    AwaitingRecovery,
    Ready,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamRecoveryToken {
    stream_id: String,
    connection: ConnectionStamp,
    stream_generation: u64,
    subscription_generation: u64,
}

impl StreamRecoveryToken {
    pub fn stream_id(&self) -> &str {
        &self.stream_id
    }

    pub fn connection(&self) -> &ConnectionStamp {
        &self.connection
    }

    pub fn stream_generation(&self) -> u64 {
        self.stream_generation
    }

    pub fn subscription_generation(&self) -> u64 {
        self.subscription_generation
    }

    pub fn connection_generation(&self) -> u64 {
        self.connection.generation().get()
    }
}

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, Error, PartialEq, Eq)]
pub enum StreamRecoveryError {
    #[error("invalid stream identifier")]
    InvalidStreamId,

    #[error("stream already registered: {0}")]
    DuplicateStream(String),

    #[error("unknown stream: {0}")]
    UnknownStream(String),

    #[error("connection generation is stale or unavailable")]
    StaleConnection,

    #[error("stream recovery token is stale")]
    StaleToken,

    #[error("stream generation exhausted")]
    GenerationExhausted,

    #[error("invalid recovery transition")]
    InvalidTransition,
    #[error("stream '{stream_id}' has a conflicting recovery kind")]
    ConflictingStreamKind { stream_id: String },
}

// ============================================================
// STATE
// ============================================================

#[derive(Debug)]
struct RecoveryEntry {
    kind: RecoveryStreamKind,
    phase: StreamRecoveryPhase,
    token: Option<StreamRecoveryToken>,
}

/// Recovery coordinator for one logical connection slot.
///
/// Calls are serialized by the owner.
///
/// All stream generations are local to the coordinator.
/// A production runtime must retain this authority for
/// the lifetime of the logical connection slot.
///
/// A new runtime incarnation requires a new ConnectionFence.
#[derive(Debug)]
pub struct StreamRecoveryCoordinator {
    entries: BTreeMap<String, RecoveryEntry>,
    last_stream_generation: u64,
    last_subscription_generation: u64,
}

impl Default for StreamRecoveryCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

impl StreamRecoveryCoordinator {
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
            last_stream_generation: 0,
            last_subscription_generation: 0,
        }
    }

    // ========================================================
    // REGISTER STREAM
    // ========================================================

    pub fn register(
        &mut self,
        stream_id: impl Into<String>,
        kind: RecoveryStreamKind,
    ) -> Result<(), StreamRecoveryError> {
        let stream_id = stream_id.into();

        if stream_id.trim().is_empty() || stream_id.trim() != stream_id {
            return Err(StreamRecoveryError::InvalidStreamId);
        }

        if self.entries.contains_key(&stream_id) {
            return Err(StreamRecoveryError::DuplicateStream(stream_id));
        }

        self.entries.insert(
            stream_id,
            RecoveryEntry {
                kind,
                phase: StreamRecoveryPhase::AwaitingSubscription,
                token: None,
            },
        );

        Ok(())
    }
    /// Atomically reconcile the complete desired stream set
    /// for one logical connection slot and allocate fresh
    /// recovery tokens for the current physical generation.
    ///
    /// The caller must supply the COMPLETE desired stream
    /// set for this slot, not an arbitrary subset.
    ///
    /// All validation and generation-overflow checks happen
    /// before any coordinator state is changed.
    ///
    /// Removed streams lose their registrations, but global
    /// stream/subscription generation counters are preserved.
    ///
    /// This method performs no asynchronous operations.
    pub fn reconcile_and_bind(
        &mut self,
        fence: &ConnectionFence,
        stamp: &ConnectionStamp,
        desired: &[(String, RecoveryStreamKind)],
    ) -> Result<BTreeMap<String, StreamRecoveryToken>, StreamRecoveryError> {
        // --------------------------------------------------------
        // CONNECTION AUTHORITY
        // --------------------------------------------------------

        if fence.current_stamp().as_ref() != Some(stamp) {
            return Err(StreamRecoveryError::StaleConnection);
        }

        if !matches!(
            fence.state(),
            crate::live::infrastructure::websocket::lifecycle::state::ConnectionState::Connected
                | crate::live::infrastructure::websocket::lifecycle::state::ConnectionState::Ready
        ) {
            return Err(StreamRecoveryError::StaleConnection);
        }

        // --------------------------------------------------------
        // VALIDATE DESIRED REGISTRATIONS
        // --------------------------------------------------------

        let mut seen = BTreeSet::new();

        for (stream_id, kind) in desired {
            if stream_id.is_empty()
                || stream_id.trim() != stream_id
                || stream_id.chars().any(char::is_whitespace)
            {
                return Err(StreamRecoveryError::InvalidStreamId);
            }

            if !seen.insert(stream_id.as_str()) {
                return Err(StreamRecoveryError::DuplicateStream(stream_id.clone()));
            }

            if let Some(existing) = self.entries.get(stream_id) {
                if existing.kind != *kind {
                    return Err(StreamRecoveryError::ConflictingStreamKind {
                        stream_id: stream_id.clone(),
                    });
                }
            }
        }

        // --------------------------------------------------------
        // PREFLIGHT EPOCH CAPACITY
        // --------------------------------------------------------

        let count =
            u64::try_from(desired.len()).map_err(|_| StreamRecoveryError::GenerationExhausted)?;

        let next_stream_generation = self
            .last_stream_generation
            .checked_add(count)
            .ok_or(StreamRecoveryError::GenerationExhausted)?;

        let next_subscription_generation = self
            .last_subscription_generation
            .checked_add(count)
            .ok_or(StreamRecoveryError::GenerationExhausted)?;

        // --------------------------------------------------------
        // BUILD THE REPLACEMENT STATE
        // --------------------------------------------------------

        // Construct the complete replacement before mutating
        // authoritative state.
        //
        // Existing stream identities are preserved, while
        // obsolete registrations disappear from the new map.

        let mut replacement = BTreeMap::new();
        let mut tokens = BTreeMap::new();

        let mut stream_generation = self.last_stream_generation;

        let mut subscription_generation = self.last_subscription_generation;

        for (stream_id, kind) in desired {
            stream_generation += 1;
            subscription_generation += 1;

            let token = StreamRecoveryToken {
                stream_id: stream_id.clone(),
                connection: stamp.clone(),
                stream_generation,
                subscription_generation,
            };

            replacement.insert(
                stream_id.clone(),
                RecoveryEntry {
                    kind: *kind,
                    phase: StreamRecoveryPhase::AwaitingSubscription,
                    token: Some(token.clone()),
                },
            );

            tokens.insert(stream_id.clone(), token);
        }

        // --------------------------------------------------------
        // COMMIT
        // --------------------------------------------------------

        // No fallible validation remains after this point.
        self.entries = replacement;

        self.last_stream_generation = next_stream_generation;

        self.last_subscription_generation = next_subscription_generation;

        Ok(tokens)
    }
    // ========================================================
    // BEGIN NEW PHYSICAL BINDING
    // ========================================================

    /// Bind one registered stream to a new physical
    /// connection generation.
    ///
    /// The physical connection must be current and at
    /// least Connected. It need not yet be Ready.
    ///
    /// A fresh stream epoch and subscription epoch are
    /// allocated together.
    ///
    /// No publication is authorized at this point.
    pub fn begin_binding(
        &mut self,
        fence: &ConnectionFence,
        stamp: &ConnectionStamp,
        stream_id: &str,
    ) -> Result<StreamRecoveryToken, StreamRecoveryError> {
        if fence.current_stamp().as_ref() != Some(stamp) {
            return Err(StreamRecoveryError::StaleConnection);
        }

        if !matches!(
            fence.state(),
            crate::live::infrastructure::websocket::lifecycle::state::ConnectionState::Connected
                | crate::live::infrastructure::websocket::lifecycle::state::ConnectionState::Ready
        ) {
            return Err(StreamRecoveryError::StaleConnection);
        }

        if !self.entries.contains_key(stream_id) {
            return Err(StreamRecoveryError::UnknownStream(stream_id.to_owned()));
        }

        let next_stream = self
            .last_stream_generation
            .checked_add(1)
            .ok_or(StreamRecoveryError::GenerationExhausted)?;

        let next_subscription = self
            .last_subscription_generation
            .checked_add(1)
            .ok_or(StreamRecoveryError::GenerationExhausted)?;

        let token = StreamRecoveryToken {
            stream_id: stream_id.to_owned(),
            connection: stamp.clone(),
            stream_generation: next_stream,
            subscription_generation: next_subscription,
        };

        let entry = self
            .entries
            .get_mut(stream_id)
            .expect("stream existence checked above");

        entry.token = Some(token.clone());
        entry.phase = StreamRecoveryPhase::AwaitingSubscription;

        self.last_stream_generation = next_stream;
        self.last_subscription_generation = next_subscription;

        Ok(token)
    }

    // ========================================================
    // SUBSCRIPTION ACKNOWLEDGED
    // ========================================================

    /// Only the subscription protocol authority should
    /// invoke this after validating the native ACK.
    ///
    /// Sending a subscription request is not sufficient.
    pub fn subscription_confirmed(
        &mut self,
        fence: &ConnectionFence,
        token: &StreamRecoveryToken,
    ) -> Result<(), StreamRecoveryError> {
        self.require_current(fence, token)?;

        let entry = self.entry_mut(token)?;

        if entry.phase != StreamRecoveryPhase::AwaitingSubscription {
            return Err(StreamRecoveryError::InvalidTransition);
        }

        entry.phase = StreamRecoveryPhase::AwaitingRecovery;

        Ok(())
    }

    // ========================================================
    // INTEGRITY RECOVERY COMPLETED
    // ========================================================

    /// Only the source-stream integrity authority should
    /// invoke this transition.
    ///
    /// For depth, this must follow an authoritative
    /// synchronization outcome.
    ///
    /// For trades, this must follow the required source
    /// binding and continuity/coverage initialization.
    pub fn recovery_confirmed(
        &mut self,
        fence: &ConnectionFence,
        token: &StreamRecoveryToken,
    ) -> Result<(), StreamRecoveryError> {
        self.require_current(fence, token)?;

        let entry = self.entry_mut(token)?;

        if entry.phase != StreamRecoveryPhase::AwaitingRecovery {
            return Err(StreamRecoveryError::InvalidTransition);
        }

        entry.phase = StreamRecoveryPhase::Ready;

        Ok(())
    }

    // ========================================================
    // VALIDATE PUBLICATION ELIGIBILITY
    // ========================================================

    /// A preliminary stream-level eligibility check.
    ///
    /// It deliberately requires both:
    /// - A Ready physical connection generation.
    /// - A Ready stream recovery phase.
    ///
    /// Publication must still be protected by the
    /// authoritative source-integrity/publication fence.
    pub fn may_publish(&self, fence: &ConnectionFence, token: &StreamRecoveryToken) -> bool {
        if !fence.accepts(token.connection()) {
            return false;
        }

        self.entries.get(token.stream_id()).is_some_and(|entry| {
            entry.phase == StreamRecoveryPhase::Ready && entry.token.as_ref() == Some(token)
        })
    }

    // ========================================================
    // OBSERVABILITY
    // ========================================================

    pub fn phase(&self, stream_id: &str) -> Option<StreamRecoveryPhase> {
        self.entries.get(stream_id).map(|entry| entry.phase)
    }

    pub fn token(&self, stream_id: &str) -> Option<&StreamRecoveryToken> {
        self.entries.get(stream_id)?.token.as_ref()
    }

    pub fn kind(&self, stream_id: &str) -> Option<RecoveryStreamKind> {
        self.entries.get(stream_id).map(|entry| entry.kind)
    }

    // ========================================================
    // INTERNAL VALIDATION
    // ========================================================

    fn require_current(
        &self,
        fence: &ConnectionFence,
        token: &StreamRecoveryToken,
    ) -> Result<(), StreamRecoveryError> {
        if fence.current_stamp().as_ref() != Some(token.connection()) {
            return Err(StreamRecoveryError::StaleConnection);
        }

        let entry = self
            .entries
            .get(token.stream_id())
            .ok_or_else(|| StreamRecoveryError::UnknownStream(token.stream_id().to_owned()))?;

        if entry.token.as_ref() != Some(token) {
            return Err(StreamRecoveryError::StaleToken);
        }

        Ok(())
    }

    fn entry_mut(
        &mut self,
        token: &StreamRecoveryToken,
    ) -> Result<&mut RecoveryEntry, StreamRecoveryError> {
        let entry = self
            .entries
            .get_mut(token.stream_id())
            .ok_or_else(|| StreamRecoveryError::UnknownStream(token.stream_id().to_owned()))?;

        if entry.token.as_ref() != Some(token) {
            return Err(StreamRecoveryError::StaleToken);
        }

        Ok(entry)
    }
}
