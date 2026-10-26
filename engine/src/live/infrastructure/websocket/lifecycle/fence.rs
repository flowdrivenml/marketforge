use std::sync::{Arc, RwLock};

use thiserror::Error;

use super::state::{
    ConnectionGeneration, ConnectionLifecycle, ConnectionLifecycleError, ConnectionState,
};

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

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc, Barrier,
        atomic::{AtomicBool, Ordering},
    };

    use crate::live::infrastructure::websocket::{ConnectionFence, ConnectionState, FencedMessage};

    // ============================================================
    // SHARED AUTHORITY
    // ============================================================

    #[test]
    fn fence_clones_share_lifecycle() {
        let owner = ConnectionFence::new("runtime-1", "slot-A").unwrap();

        let consumer = owner.clone();

        let generation = owner.begin_connect().unwrap();

        assert_eq!(consumer.state(), ConnectionState::Connecting);

        owner.connected(&generation).unwrap();

        assert!(!consumer.accepts(&generation));

        owner.ready(&generation).unwrap();

        assert!(consumer.accepts(&generation));

        owner.failed(&generation).unwrap();

        assert!(!consumer.accepts(&generation));
    }

    // ============================================================
    // STALE BUFFERED DATA
    // ============================================================

    #[test]
    fn old_generation_messages_are_rejected() {
        let fence = ConnectionFence::new("runtime-1", "slot-A").unwrap();

        let first = fence.begin_connect().unwrap();

        fence.connected(&first).unwrap();
        fence.ready(&first).unwrap();

        let buffered = FencedMessage::new(first.clone(), "depth-update-500");

        assert!(fence.accepts(buffered.stamp()));

        // Old connection fails.
        fence.failed(&first).unwrap();

        // Replacement establishes a new generation.
        let second = fence.begin_connect().unwrap();

        fence.connected(&second).unwrap();
        fence.ready(&second).unwrap();

        assert!(fence.accepts(&second));

        // Previously buffered data cannot be accepted.
        assert!(!fence.accepts(buffered.stamp()));

        assert_eq!(
            fence.with_ready(buffered.stamp(), || buffered.payload(),),
            None,
        );
    }

    // ============================================================
    // SLOT IDENTITY
    // ============================================================

    #[test]
    fn separate_connection_slots_do_not_share_generations() {
        let first = ConnectionFence::new("runtime-1", "slot-A").unwrap();

        let second = ConnectionFence::new("runtime-1", "slot-B").unwrap();

        let first_stamp = first.begin_connect().unwrap();
        let second_stamp = second.begin_connect().unwrap();

        assert_eq!(
            first_stamp.generation().get(),
            second_stamp.generation().get(),
        );

        first.connected(&first_stamp).unwrap();
        first.ready(&first_stamp).unwrap();

        second.connected(&second_stamp).unwrap();
        second.ready(&second_stamp).unwrap();

        assert!(first.accepts(&first_stamp));
        assert!(second.accepts(&second_stamp));

        assert!(!first.accepts(&second_stamp));
        assert!(!second.accepts(&first_stamp));
    }

    // ============================================================
    // RUNTIME INCARNATION
    // ============================================================

    #[test]
    fn previous_runtime_generation_is_rejected() {
        let old = ConnectionFence::new("runtime-old", "slot-A").unwrap();

        let current = ConnectionFence::new("runtime-new", "slot-A").unwrap();

        let old_stamp = old.begin_connect().unwrap();

        let current_stamp = current.begin_connect().unwrap();

        current.connected(&current_stamp).unwrap();
        current.ready(&current_stamp).unwrap();

        // Generation numbers happen to be equal.
        assert_eq!(
            old_stamp.generation().get(),
            current_stamp.generation().get(),
        );

        // Runtime incarnation prevents accidental equality.
        assert!(!current.accepts(&old_stamp));
        assert!(current.accepts(&current_stamp));
    }

    // ============================================================
    // NO PUBLICATION BEFORE READY
    // ============================================================

    #[test]
    fn connected_socket_cannot_accept_market_data() {
        let fence = ConnectionFence::new("runtime-1", "slot-A").unwrap();

        let stamp = fence.begin_connect().unwrap();

        fence.connected(&stamp).unwrap();

        let accepted = fence.with_ready(&stamp, || {
            panic!("unready connection accepted market data");
        });

        assert!(accepted.is_none());

        fence.ready(&stamp).unwrap();

        assert_eq!(fence.with_ready(&stamp, || 42), Some(42),);
    }

    // ============================================================
    // REVOCATION VS SYNCHRONOUS PROCESSING
    // ============================================================

    #[test]
    fn revocation_waits_for_inflight_fenced_operation() {
        let fence = ConnectionFence::new("runtime-1", "slot-A").unwrap();

        let stamp = fence.begin_connect().unwrap();

        fence.connected(&stamp).unwrap();
        fence.ready(&stamp).unwrap();

        let started = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));

        let revoked = Arc::new(AtomicBool::new(false));

        let worker_fence = fence.clone();
        let worker_stamp = stamp.clone();

        let worker_started = started.clone();
        let worker_release = release.clone();

        let worker = std::thread::spawn(move || {
            worker_fence.with_ready(&worker_stamp, || {
                worker_started.wait();

                // Keep the read lock held until the test
                // explicitly permits processing to finish.
                worker_release.wait();

                "accepted"
            })
        });

        // Wait until the processing closure has entered.
        started.wait();

        let revoke_fence = fence.clone();
        let revoke_stamp = stamp.clone();
        let revoke_complete = revoked.clone();

        let revoker = std::thread::spawn(move || {
            revoke_fence.failed(&revoke_stamp).unwrap();

            revoke_complete.store(true, Ordering::SeqCst);
        });

        // The processing closure is still holding the
        // lifecycle read lock at this point.
        assert!(!revoked.load(Ordering::SeqCst));

        release.wait();

        assert_eq!(worker.join().unwrap(), Some("accepted"));

        revoker.join().unwrap();

        assert!(revoked.load(Ordering::SeqCst));

        assert!(!fence.accepts(&stamp));
    }

    // ============================================================
    // TERMINAL SHUTDOWN
    // ============================================================

    #[test]
    fn shutdown_invalidates_all_clones() {
        let fence = ConnectionFence::new("runtime-1", "slot-A").unwrap();

        let consumer = fence.clone();

        let stamp = fence.begin_connect().unwrap();

        fence.connected(&stamp).unwrap();
        fence.ready(&stamp).unwrap();

        fence.stop();

        assert_eq!(consumer.state(), ConnectionState::Stopped);

        assert!(!consumer.accepts(&stamp));
        assert!(consumer.current_stamp().is_none());
    }

    // ============================================================
    // VALIDATE IDENTITIES
    // ============================================================

    #[test]
    fn rejects_empty_runtime_and_slot_identity() {
        assert!(ConnectionFence::new("", "slot-A").is_err());

        assert!(ConnectionFence::new("runtime-1", "").is_err());
    }
}
