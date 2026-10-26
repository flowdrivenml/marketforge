use thiserror::Error;

// ============================================================
// CONNECTION GENERATION
// ============================================================

/// Monotonically increasing identity of one physical
/// WebSocket connection attempt.
///
/// Generation zero is reserved and never issued.
///
/// A generation must not be reused within the lifetime
/// of its authoritative connection lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConnectionGeneration(u64);

impl ConnectionGeneration {
    pub fn get(self) -> u64 {
        self.0
    }
}

// ============================================================
// CONNECTION STATE
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    Idle,
    Connecting,
    Connected,
    Ready,
    Backoff,
    Stopped,
}

// ============================================================
// LIFECYCLE ERRORS
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ConnectionLifecycleError {
    #[error("invalid connection transition from {state:?}: {operation}")]
    InvalidTransition {
        state: ConnectionState,
        operation: &'static str,
    },

    #[error("connection generation exhausted")]
    GenerationExhausted,

    #[error("stale connection generation: expected {expected}, received {received}")]
    StaleGeneration { expected: u64, received: u64 },

    #[error("connection lifecycle is stopped")]
    Stopped,
}

// ============================================================
// AUTHORITATIVE LIFECYCLE
// ============================================================

/// Synchronous lifecycle authority for one logical
/// WebSocket connection slot.
///
/// A slot may have many sequential physical connections,
/// but only one generation can be current at any instant.
///
/// This object does not perform network I/O.
/// It is intended to be owned by the future connection
/// manager's single supervisory task.
#[derive(Debug)]
pub struct ConnectionLifecycle {
    state: ConnectionState,

    /// Last generation allocated, including failed attempts.
    last_generation: u64,

    /// Generation currently authorized to transition state.
    ///
    /// None while idle, backing off, or stopped.
    current_generation: Option<ConnectionGeneration>,
}

impl Default for ConnectionLifecycle {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectionLifecycle {
    pub fn new() -> Self {
        Self {
            state: ConnectionState::Idle,
            last_generation: 0,
            current_generation: None,
        }
    }

    // ========================================================
    // ACCESSORS
    // ========================================================

    pub fn state(&self) -> ConnectionState {
        self.state
    }

    pub fn current_generation(&self) -> Option<ConnectionGeneration> {
        self.current_generation
    }

    pub fn last_generation(&self) -> u64 {
        self.last_generation
    }

    // ========================================================
    // BEGIN CONNECTION ATTEMPT
    // ========================================================

    /// Allocate a new generation before attempting a
    /// physical WebSocket connection.
    ///
    /// Only Idle and Backoff may begin connecting.
    pub fn begin_connect(&mut self) -> Result<ConnectionGeneration, ConnectionLifecycleError> {
        match self.state {
            ConnectionState::Idle | ConnectionState::Backoff => {}

            ConnectionState::Stopped => {
                return Err(ConnectionLifecycleError::Stopped);
            }

            state => {
                return Err(ConnectionLifecycleError::InvalidTransition {
                    state,
                    operation: "begin_connect",
                });
            }
        }

        let next = self
            .last_generation
            .checked_add(1)
            .ok_or(ConnectionLifecycleError::GenerationExhausted)?;

        let generation = ConnectionGeneration(next);

        self.last_generation = next;
        self.current_generation = Some(generation);
        self.state = ConnectionState::Connecting;

        Ok(generation)
    }

    // ========================================================
    // CONNECTION ESTABLISHED
    // ========================================================

    /// The WebSocket handshake has completed.
    ///
    /// Subscriptions and stream initialization are not
    /// necessarily complete.
    pub fn connected(
        &mut self,
        generation: ConnectionGeneration,
    ) -> Result<(), ConnectionLifecycleError> {
        self.require_current(generation)?;

        self.require_state(ConnectionState::Connecting, "connected")?;

        self.state = ConnectionState::Connected;

        Ok(())
    }

    // ========================================================
    // CONNECTION READY
    // ========================================================

    /// Declare the generation ready only after all
    /// required subscription and initialization contracts
    /// have been satisfied.
    ///
    /// The future subscription manager must be the
    /// authority for this transition.
    pub fn ready(
        &mut self,
        generation: ConnectionGeneration,
    ) -> Result<(), ConnectionLifecycleError> {
        self.require_current(generation)?;

        self.require_state(ConnectionState::Connected, "ready")?;

        self.state = ConnectionState::Ready;

        Ok(())
    }

    // ========================================================
    // FAILURE / INVALIDATION
    // ========================================================

    /// Invalidate the current physical connection.
    ///
    /// The generation is revoked BEFORE retry backoff.
    ///
    /// Any delayed event referencing this generation
    /// is rejected from this point onward.
    pub fn failed(
        &mut self,
        generation: ConnectionGeneration,
    ) -> Result<(), ConnectionLifecycleError> {
        self.require_current(generation)?;

        match self.state {
            ConnectionState::Connecting | ConnectionState::Connected | ConnectionState::Ready => {
                self.current_generation = None;
                self.state = ConnectionState::Backoff;
                Ok(())
            }

            state => Err(ConnectionLifecycleError::InvalidTransition {
                state,
                operation: "failed",
            }),
        }
    }

    // ========================================================
    // SHUTDOWN
    // ========================================================

    /// Permanently stop this lifecycle instance.
    ///
    /// A fresh instance must be created to restart the
    /// logical connection slot.
    ///
    /// Shutdown is idempotent.
    pub fn stop(&mut self) {
        self.current_generation = None;
        self.state = ConnectionState::Stopped;
    }

    // ========================================================
    // FENCING
    // ========================================================

    /// Whether a message belongs to the current generation
    /// and the connection has completed initialization.
    ///
    /// This is a necessary transport-level condition,
    /// not sufficient authorization to apply a depth update.
    ///
    /// Depth synchronization and source-stream epoch
    /// validation remain separate responsibilities.
    pub fn accepts_data(&self, generation: ConnectionGeneration) -> bool {
        self.state == ConnectionState::Ready && self.current_generation == Some(generation)
    }

    /// Whether a callback belongs to the current physical
    /// connection, irrespective of its readiness.
    pub fn is_current(&self, generation: ConnectionGeneration) -> bool {
        self.current_generation == Some(generation)
            && matches!(
                self.state,
                ConnectionState::Connecting | ConnectionState::Connected | ConnectionState::Ready
            )
    }

    // ========================================================
    // INTERNAL VALIDATION
    // ========================================================

    fn require_current(
        &self,
        generation: ConnectionGeneration,
    ) -> Result<(), ConnectionLifecycleError> {
        if self.state == ConnectionState::Stopped {
            return Err(ConnectionLifecycleError::Stopped);
        }

        let expected = self
            .current_generation
            .map(ConnectionGeneration::get)
            .unwrap_or(0);

        if self.current_generation != Some(generation) {
            return Err(ConnectionLifecycleError::StaleGeneration {
                expected,
                received: generation.get(),
            });
        }

        Ok(())
    }

    fn require_state(
        &self,
        required: ConnectionState,
        operation: &'static str,
    ) -> Result<(), ConnectionLifecycleError> {
        if self.state != required {
            return Err(ConnectionLifecycleError::InvalidTransition {
                state: self.state,
                operation,
            });
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::live::infrastructure::websocket::{
        ConnectionLifecycle, ConnectionLifecycleError, ConnectionState,
    };

    // ============================================================
    // INITIAL STATE
    // ============================================================

    #[test]
    fn new_lifecycle_starts_idle() {
        let lifecycle = ConnectionLifecycle::new();

        assert_eq!(lifecycle.state(), ConnectionState::Idle,);

        assert_eq!(lifecycle.current_generation(), None);
        assert_eq!(lifecycle.last_generation(), 0);
    }

    // ============================================================
    // NORMAL CONNECTION ESTABLISHMENT
    // ============================================================

    #[test]
    fn connection_requires_explicit_readiness() {
        let mut lifecycle = ConnectionLifecycle::new();

        let generation = lifecycle.begin_connect().unwrap();

        assert_eq!(generation.get(), 1);

        assert_eq!(lifecycle.state(), ConnectionState::Connecting,);

        assert!(!lifecycle.accepts_data(generation));

        lifecycle.connected(generation).unwrap();

        assert_eq!(lifecycle.state(), ConnectionState::Connected,);

        // A successful handshake does not authorize publication.
        assert!(!lifecycle.accepts_data(generation));

        lifecycle.ready(generation).unwrap();

        assert_eq!(lifecycle.state(), ConnectionState::Ready,);

        assert!(lifecycle.accepts_data(generation));
    }

    // ============================================================
    // RECONNECT GENERATION FENCING
    // ============================================================

    #[test]
    fn reconnection_revokes_old_generation() {
        let mut lifecycle = ConnectionLifecycle::new();

        let first = lifecycle.begin_connect().unwrap();

        lifecycle.connected(first).unwrap();
        lifecycle.ready(first).unwrap();

        assert!(lifecycle.accepts_data(first));

        // Transport failure invalidates the current generation
        // immediately, before any retry attempt.
        lifecycle.failed(first).unwrap();

        assert_eq!(lifecycle.state(), ConnectionState::Backoff,);

        assert_eq!(lifecycle.current_generation(), None);

        assert!(!lifecycle.accepts_data(first));
        assert!(!lifecycle.is_current(first));

        // Allocate a new physical connection generation.
        let second = lifecycle.begin_connect().unwrap();

        assert_eq!(second.get(), first.get() + 1);

        lifecycle.connected(second).unwrap();
        lifecycle.ready(second).unwrap();

        assert!(lifecycle.accepts_data(second));

        // A buffered event from the previous connection
        // must never become valid again.
        assert!(!lifecycle.accepts_data(first));
    }

    // ============================================================
    // STALE CALLBACKS
    // ============================================================

    #[test]
    fn stale_connection_callback_cannot_change_state() {
        let mut lifecycle = ConnectionLifecycle::new();

        let first = lifecycle.begin_connect().unwrap();

        lifecycle.failed(first).unwrap();

        let second = lifecycle.begin_connect().unwrap();

        lifecycle.connected(second).unwrap();

        let result = lifecycle.connected(first);

        assert!(matches!(
            result,
            Err(ConnectionLifecycleError::StaleGeneration { .. }),
        ));

        // The stale callback must not modify the current state.
        assert_eq!(lifecycle.state(), ConnectionState::Connected,);

        assert_eq!(lifecycle.current_generation(), Some(second),);
    }

    // ============================================================
    // INVALID TRANSITIONS
    // ============================================================

    #[test]
    fn rejects_ready_before_handshake() {
        let mut lifecycle = ConnectionLifecycle::new();

        let generation = lifecycle.begin_connect().unwrap();

        let result = lifecycle.ready(generation);

        assert!(matches!(
            result,
            Err(ConnectionLifecycleError::InvalidTransition { .. }),
        ));

        assert_eq!(lifecycle.state(), ConnectionState::Connecting,);

        assert!(!lifecycle.accepts_data(generation));
    }

    #[test]
    fn rejects_duplicate_connect_attempt() {
        let mut lifecycle = ConnectionLifecycle::new();

        let first = lifecycle.begin_connect().unwrap();

        assert!(matches!(
            lifecycle.begin_connect(),
            Err(ConnectionLifecycleError::InvalidTransition { .. }),
        ));

        // A rejected transition does not allocate an ID.
        assert_eq!(lifecycle.last_generation(), 1);
        assert_eq!(lifecycle.current_generation(), Some(first));
    }

    #[test]
    fn rejects_duplicate_ready_transition() {
        let mut lifecycle = ConnectionLifecycle::new();

        let generation = lifecycle.begin_connect().unwrap();

        lifecycle.connected(generation).unwrap();
        lifecycle.ready(generation).unwrap();

        assert!(matches!(
            lifecycle.ready(generation),
            Err(ConnectionLifecycleError::InvalidTransition { .. }),
        ));

        assert!(lifecycle.accepts_data(generation));
    }

    // ============================================================
    // FAILURE DURING CONNECT
    // ============================================================

    #[test]
    fn handshake_failure_invalidates_generation() {
        let mut lifecycle = ConnectionLifecycle::new();

        let failed = lifecycle.begin_connect().unwrap();

        lifecycle.failed(failed).unwrap();

        assert_eq!(lifecycle.state(), ConnectionState::Backoff,);

        assert!(!lifecycle.accepts_data(failed));

        let retry = lifecycle.begin_connect().unwrap();

        assert_eq!(retry.get(), failed.get() + 1);
        assert!(!lifecycle.accepts_data(retry));
    }

    // ============================================================
    // TERMINAL SHUTDOWN
    // ============================================================

    #[test]
    fn shutdown_revokes_generation() {
        let mut lifecycle = ConnectionLifecycle::new();

        let generation = lifecycle.begin_connect().unwrap();

        lifecycle.connected(generation).unwrap();
        lifecycle.ready(generation).unwrap();

        lifecycle.stop();

        assert_eq!(lifecycle.state(), ConnectionState::Stopped,);

        assert_eq!(lifecycle.current_generation(), None);

        assert!(!lifecycle.accepts_data(generation));

        // A stopped lifecycle cannot be restarted implicitly.
        assert!(matches!(
            lifecycle.begin_connect(),
            Err(ConnectionLifecycleError::Stopped),
        ));

        // Late callbacks cannot reactivate the slot.
        assert!(matches!(
            lifecycle.ready(generation),
            Err(ConnectionLifecycleError::Stopped),
        ));
    }

    #[test]
    fn shutdown_is_idempotent() {
        let mut lifecycle = ConnectionLifecycle::new();

        lifecycle.stop();
        lifecycle.stop();

        assert_eq!(lifecycle.state(), ConnectionState::Stopped,);
    }

    // ============================================================
    // MANY RECONNECTIONS
    // ============================================================

    #[test]
    fn generations_never_repeat_during_reconnects() {
        let mut lifecycle = ConnectionLifecycle::new();

        let mut previous = 0_u64;

        for _ in 0..1000 {
            let generation = lifecycle.begin_connect().unwrap();

            assert!(generation.get() > previous);

            assert_eq!(generation.get(), previous + 1,);

            lifecycle.connected(generation).unwrap();
            lifecycle.ready(generation).unwrap();

            assert!(lifecycle.accepts_data(generation));

            lifecycle.failed(generation).unwrap();

            assert!(!lifecycle.accepts_data(generation));

            previous = generation.get();
        }

        assert_eq!(lifecycle.last_generation(), 1000);
    }
}
