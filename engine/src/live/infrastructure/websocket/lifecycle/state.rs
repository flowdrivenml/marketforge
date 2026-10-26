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
mod tests;
