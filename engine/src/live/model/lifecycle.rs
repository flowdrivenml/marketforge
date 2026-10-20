//! Stream lifecycle models required by MarketForge Live.
//!
//! Authoritative reference: 02 — Domain Models and Contracts.md, Sections 7.1–7.7.
//!
//! MarketForge Live represents connection, subscription, source-stream,
//! and data-integrity lifecycles independently. A stream may be connected
//! and subscribed while its order book remains invalid or its coverage
//! incomplete.

use serde::{Deserialize, Serialize};

/// Independent operational state of a logical source stream.
///
/// Each field represents an autonomous dimension owned by a different
/// subsystem (Stream Registry, Connection Manager, Subscription Manager,
/// Integrity Processor, Recovery Coordinator, Retention subsystem).
/// No field's state implies the others.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceStreamState {
    pub lifecycle: StreamLifecycle,
    pub connection: ConnectionState,
    pub subscription: SubscriptionState,
    pub integrity: IntegrityState,
    pub recovery: RecoveryState,
}

impl SourceStreamState {
    /// Initial state for a freshly registered source stream.
    pub fn initial() -> Self {
        Self {
            lifecycle: StreamLifecycle::Registered,
            connection: ConnectionState::Created,
            subscription: SubscriptionState::Pending,
            integrity: IntegrityState::Uninitialized,
            recovery: RecoveryState::Idle,
        }
    }
}

/// Lifecycle of a logical source-stream runtime.
///
/// `StreamLifecycle` represents execution and resource-ownership state
/// of the logical runtime. It does **not** represent exchange connectivity,
/// subscription readiness, data integrity, historical coverage, or health.
///
/// The Stream Registry is authoritative for this lifecycle.
///
/// # Variant independence
///
/// `Reconnecting`, `Degraded`, and `Synchronizing` are **not** lifecycle
/// variants. They belong to `ConnectionState`, health assessment, and
/// `IntegrityState` respectively, owned by their respective subsystems.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamLifecycle {
    /// The logical stream has been registered, but its runtime has
    /// not yet started.
    Registered,
    /// The logical runtime is initializing its required resources
    /// and coordinating activation.
    Starting,
    /// The logical runtime is running and can coordinate acquisition,
    /// integrity processing, and recovery.
    Active,
    /// The logical runtime remains allocated but is not currently
    /// acquiring data because no acquisition demand requires it.
    Idle,
    /// Runtime shutdown or resource release is in progress.
    Stopping,
    /// The logical runtime is stopped, but its registration may remain
    /// available for later activation.
    Stopped,
    /// The logical runtime cannot continue without intervention or a
    /// separately authorized restart.
    Failed,
}

/// Authoritative physical connection lifecycle.
///
/// The Connection Manager owns this state. It is independent of logical
/// source-stream lifecycle, native subscription state, market-data
/// integrity, and health assessment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionState {
    /// The logical connection descriptor exists, but connection
    /// establishment has not begun.
    Created,
    /// Connection establishment is waiting for applicable resource
    /// capacity or rate-limit admission.
    PendingAdmission,
    /// Endpoint resolution is in progress.
    Resolving,
    /// Physical connection establishment is in progress.
    Connecting,
    /// Required TLS and WebSocket negotiation is in progress.
    Handshaking,
    /// The physical transport has satisfied its connection-
    /// establishment requirements.
    Connected,
    /// A recoverable connection interruption has occurred and
    /// restoration is pending or underway.
    Reconnecting,
    /// An intentional connection shutdown or replacement is in
    /// progress.
    Closing,
    /// The physical connection has terminated intentionally or
    /// through completed shutdown.
    Closed,
    /// Connection establishment or operation cannot continue
    /// without configuration correction or explicit intervention.
    Failed,
}

/// Native subscription activation lifecycle.
///
/// The Subscription Manager owns admission, acknowledgement, and
/// re-submission semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubscriptionState {
    /// A subscription request has not yet been issued.
    Pending,
    /// A subscription request is active on the server and awaiting
    /// acknowledgement.
    Subscribing,
    /// The exchange has confirmed the subscription.
    Active,
    /// An unsubscribe request is active and awaiting acknowledgement.
    Unsubscribing,
    /// The exchange rejected the subscription request.
    Rejected,
    /// The subscription activation process cannot continue without
    /// intervention.
    Failed,
}

/// Data-integrity state for the stream's event sequence.
///
/// Integrity state does not derive from connection status alone.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum IntegrityState {
    /// No authoritative state established.
    Uninitialized,
    /// Snapshot/update synchronization underway.
    Synchronizing,
    /// Book state satisfies required continuity rules.
    Valid,
    /// Required integrity conditions have been violated.
    /// The reason is owned by Document 04 (Integrity model).
    Invalid { reason: IntegrityFailure },
}

/// Reason for an integrity failure.
///
/// Defined by Document 04 (Integrity model). Future variants will
/// enumerate specific failure modes (gap detected, sequence mismatch,
/// snapshot mismatch, etc.).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum IntegrityFailure {
    /// A specific failure reason will be added when Document 04 is applied.
    ReasonNotDefined,
}

/// Current recovery workflow state.
///
/// Distinguishes recoverable temporary failures from terminal conditions
/// requiring intervention. Recovery completion does not automatically
/// imply complete historical coverage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryState {
    /// No recovery is active or required.
    Idle,
    /// A recovery operation has been scheduled but not yet started.
    Pending,
    /// Recovery is underway (e.g., historical fetch, re-sync).
    Recovering,
    /// Waiting for an external retry delay (e.g., cooldown, backoff).
    WaitingForRetry,
    /// Maximum recovery attempts exhausted; manual intervention required.
    Exhausted,
}

/// Historical coverage status for a defined source and interval.
///
/// Independent of current stream validity. A stream may report
/// connection: ACTIVE, integrity: VALID, coverage: INCOMPLETE
/// while current acquisition is healthy and an earlier interval
/// remains unresolved.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CoverageState {
    /// Coverage status has not yet been determined.
    Unknown,
    /// All required historical data is present.
    Complete,
    /// Some required historical data is missing.
    Incomplete,
    /// Historical recovery is underway.
    Recovering,
    /// Historical recovery has been attempted but cannot complete.
    Unrecoverable,
}
