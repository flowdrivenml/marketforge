use std::sync::{Arc, RwLock};

use thiserror::Error;

use super::{ConnectionGeneration, ConnectionLifecycle, ConnectionLifecycleError, ConnectionState};

// ============================================================
// CONNECTION STAMP
// ============================================================

/// Globally scoped identity of one physical connection
/// generation within a MarketForge runtime.
///
/// A generation number alone is insufficient because
/// another connection slot may allocate the same number.
///
/// Runtime incarnation identity also prevents a generation
/// from a previous process from matching a new process.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ConnectionStamp {
    runtime_id: Arc<str>,
    slot_id: Arc<str>,
    generation: ConnectionGeneration,
}

impl ConnectionStamp {
    pub fn runtime_id(&self) -> &str {
        &self.runtime_id
    }

    pub fn slot_id(&self) -> &str {
        &self.slot_id
    }

    pub fn generation(&self) -> ConnectionGeneration {
        self.generation
    }
}

// ============================================================
// FENCED MESSAGE
// ============================================================

/// One value originating from a particular physical
/// connection generation.
///
/// The stamp is transport provenance, not authorization
/// to publish its contents.
#[derive(Debug)]
pub struct FencedMessage<T> {
    stamp: ConnectionStamp,
    payload: T,
}

impl<T> FencedMessage<T> {
    pub fn new(stamp: ConnectionStamp, payload: T) -> Self {
        Self { stamp, payload }
    }

    pub fn stamp(&self) -> &ConnectionStamp {
        &self.stamp
    }

    pub fn payload(&self) -> &T {
        &self.payload
    }

    pub fn into_parts(self) -> (ConnectionStamp, T) {
        (self.stamp, self.payload)
    }
}

// ============================================================
// FENCE ERRORS
// ============================================================

#[derive(Debug, Error)]
pub enum ConnectionFenceError {
    #[error("invalid connection-fence identity: {0}")]
    InvalidIdentity(&'static str),

    #[error(transparent)]
    Lifecycle(#[from] ConnectionLifecycleError),
}

// ============================================================
// SHARED AUTHORITY
// ============================================================

#[derive(Debug)]
struct FenceInner {
    runtime_id: Arc<str>,
    slot_id: Arc<str>,
    lifecycle: RwLock<ConnectionLifecycle>,
}

/// Shareable authoritative lifecycle for one logical
/// WebSocket connection slot.
///
/// Every clone references the same lifecycle.
///
/// A transport generation is accepted only while Ready.
///
/// No network I/O is performed by this type.
#[derive(Debug, Clone)]
pub struct ConnectionFence {
    inner: Arc<FenceInner>,
}

impl ConnectionFence {
    pub fn new(
        runtime_id: impl Into<String>,
        slot_id: impl Into<String>,
    ) -> Result<Self, ConnectionFenceError> {
        let runtime_id = runtime_id.into();
        let slot_id = slot_id.into();

        if runtime_id.trim().is_empty() {
            return Err(ConnectionFenceError::InvalidIdentity("runtime_id"));
        }

        if slot_id.trim().is_empty() {
            return Err(ConnectionFenceError::InvalidIdentity("slot_id"));
        }

        Ok(Self {
            inner: Arc::new(FenceInner {
                runtime_id: Arc::from(runtime_id),
                slot_id: Arc::from(slot_id),
                lifecycle: RwLock::new(ConnectionLifecycle::new()),
            }),
        })
    }

    // ========================================================
    // ACCESSORS
    // ========================================================

    pub fn state(&self) -> ConnectionState {
        self.read().state()
    }

    pub fn current_stamp(&self) -> Option<ConnectionStamp> {
        let lifecycle = self.read();

        lifecycle
            .current_generation()
            .map(|generation| self.stamp(generation))
    }

    // ========================================================
    // CONNECTION ATTEMPT
    // ========================================================

    pub fn begin_connect(&self) -> Result<ConnectionStamp, ConnectionFenceError> {
        let generation = self.write().begin_connect()?;

        Ok(self.stamp(generation))
    }

    // ========================================================
    // HANDSHAKE COMPLETED
    // ========================================================

    pub fn connected(&self, stamp: &ConnectionStamp) -> Result<(), ConnectionFenceError> {
        self.require_identity(stamp)?;

        self.write().connected(stamp.generation)?;

        Ok(())
    }

    // ========================================================
    // SUBSCRIPTION READINESS
    // ========================================================

    /// This transition must be invoked only by the
    /// authoritative subscription/session initializer.
    ///
    /// A successful WebSocket handshake is not sufficient.
    ///
    /// Stream-specific recovery readiness remains separate.
    pub fn ready(&self, stamp: &ConnectionStamp) -> Result<(), ConnectionFenceError> {
        self.require_identity(stamp)?;

        self.write().ready(stamp.generation)?;

        Ok(())
    }

    // ========================================================
    // FAILURE / REVOCATION
    // ========================================================

    /// Revoke the generation before retrying.
    ///
    /// Once this method returns, subsequent fenced
    /// processing cannot begin for the old generation.
    pub fn failed(&self, stamp: &ConnectionStamp) -> Result<(), ConnectionFenceError> {
        self.require_identity(stamp)?;

        self.write().failed(stamp.generation)?;

        Ok(())
    }

    // ========================================================
    // STOP
    // ========================================================

    pub fn stop(&self) {
        self.write().stop();
    }

    // ========================================================
    // FENCE CHECK
    // ========================================================

    /// Point-in-time validation.
    ///
    /// Suitable for diagnostics and preliminary rejection,
    /// but not by itself sufficient for asynchronous
    /// processing followed by publication.
    pub fn accepts(&self, stamp: &ConnectionStamp) -> bool {
        self.same_identity(stamp) && self.read().accepts_data(stamp.generation)
    }
    /// Execute a synchronous transport-delivery operation
    /// while the specified physical connection is current
    /// and its WebSocket handshake has completed.
    ///
    /// Unlike with_ready(), this does not require stream
    /// recovery or canonical publication readiness.
    ///
    /// This permits native market data to reach the
    /// normalization and integrity pipeline.
    ///
    /// The lifecycle read lock is held throughout the closure,
    /// preventing generation revocation from completing while
    /// the operation executes.
    ///
    /// Do not perform asynchronous operations inside the closure.
    /// Do not treat admission to a downstream queue as canonical
    /// publication authorization.
    pub fn with_connected<R>(
        &self,
        stamp: &ConnectionStamp,
        operation: impl FnOnce() -> R,
    ) -> Option<R> {
        if !self.same_identity(stamp) {
            return None;
        }

        let lifecycle = self.read();

        if lifecycle.current_generation() != Some(stamp.generation()) {
            return None;
        }

        if !matches!(
            lifecycle.state(),
            ConnectionState::Connected | ConnectionState::Ready
        ) {
            return None;
        }

        Some(operation())
    }
    /// Atomically validate generation readiness and
    /// execute a synchronous processing operation.
    ///
    /// The lifecycle read lock remains held throughout
    /// the closure.
    ///
    /// Failure revocation requires a write lock, so
    /// revocation cannot complete while this operation
    /// is executing.
    ///
    /// Do not perform blocking network or disk I/O
    /// inside the closure.
    ///
    /// Do not enqueue unvalidated work and assume this
    /// lock continues protecting it after the closure.
    pub fn with_ready<R>(
        &self,
        stamp: &ConnectionStamp,
        operation: impl FnOnce() -> R,
    ) -> Option<R> {
        if !self.same_identity(stamp) {
            return None;
        }

        let lifecycle = self.read();

        if !lifecycle.accepts_data(stamp.generation) {
            return None;
        }

        Some(operation())
    }

    // ========================================================
    // PRIVATE HELPERS
    // ========================================================

    fn stamp(&self, generation: ConnectionGeneration) -> ConnectionStamp {
        ConnectionStamp {
            runtime_id: self.inner.runtime_id.clone(),
            slot_id: self.inner.slot_id.clone(),
            generation,
        }
    }

    fn same_identity(&self, stamp: &ConnectionStamp) -> bool {
        self.inner.runtime_id == stamp.runtime_id && self.inner.slot_id == stamp.slot_id
    }

    fn require_identity(&self, stamp: &ConnectionStamp) -> Result<(), ConnectionFenceError> {
        if !self.same_identity(stamp) {
            return Err(ConnectionFenceError::InvalidIdentity(
                "stamp belongs to another runtime or slot",
            ));
        }

        Ok(())
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, ConnectionLifecycle> {
        self.inner
            .lifecycle
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, ConnectionLifecycle> {
        self.inner
            .lifecycle
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}
