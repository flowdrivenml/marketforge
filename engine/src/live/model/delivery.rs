//! Delivery contracts for MarketForge Live.
//!
//! Defines the semantics shared by the event router, publication,
//! recording, retention, and derived-stream destinations.
//!
//! Acceptance, transport delivery, persistence commitment, and
//! crash durability are distinct guarantees.
//!
//! This module contains domain types only. It does not implement
//! queues, cursor allocation, storage, or routing.

use serde::{Deserialize, Serialize};

use super::cursor::StreamCursor;

/// The delivery guarantee requested from a destination.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryPolicy {
    /// Failure is observable but does not block unrelated destinations.
    BestEffort,

    /// The destination must accept the event or report a violation.
    ///
    /// Acceptance does not imply persistence or consumer processing.
    Required,

    /// The destination must satisfy its explicitly configured
    /// persistence commitment before the obligation is fulfilled.
    ///
    /// The exact commitment boundary belongs to the destination.
    Durable,
}

/// Identifies the kind of delivery destination.
///
/// A concrete destination instance is identified separately by
/// its registration identity in the routing subsystem.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DestinationKind {
    Publication,
    Recording,
    Retention,
    Derived,
}

/// The observable progress of one destination obligation.
///
/// These stages describe distinct facts. They must not be treated
/// as interchangeable success conditions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryStage {
    /// The router has accepted responsibility for the event.
    Accepted,

    /// The destination has accepted the event into its bounded queue.
    Queued,

    /// The destination has completed its declared delivery operation.
    ///
    /// For publication this may mean a successful socket write.
    /// It does not imply processing by the remote consumer.
    Delivered,

    /// A persistence backend has committed the event.
    ///
    /// This does not independently establish crash durability.
    Committed,

    /// The configured crash-durability requirement has been met.
    Durable,
}

/// Classification of a failed destination obligation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryFailure {
    /// The destination cannot accept additional work.
    CapacityExceeded,

    /// The destination is temporarily unavailable.
    Unavailable,

    /// The destination rejected the event or operation.
    Rejected,

    /// The operation exceeded its deadline.
    Timeout,

    /// The operation was cancelled before completion.
    Cancelled,

    /// The destination disconnected.
    Disconnected,

    /// The destination encountered a persistence failure.
    PersistenceFailed,

    /// The requested durability guarantee could not be satisfied.
    DurabilityUnavailable,

    /// The destination cannot determine whether an operation committed.
    CommitOutcomeUnknown,

    /// The destination encountered an internal failure.
    Internal,
}

/// The current result of one destination obligation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryStatus {
    /// Work remains outstanding.
    Pending,

    /// The destination has reached the reported stage.
    Progress(DeliveryStage),

    /// The destination failed to satisfy its obligation.
    Failed(DeliveryFailure),
}

/// Identifies the backpressure response selected for a destination.
///
/// The actual behavior is implemented by the destination owner
/// and coordinated through the router's admission contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackpressureAction {
    /// Wait for capacity, subject to cancellation and deadline.
    Wait,

    /// Disconnect an overloaded consumer.
    DisconnectConsumer,

    /// Reject admission and report the failed guarantee.
    FailDelivery,

    /// Terminate an affected retention session.
    FailRetentionSession,
}

/// Declares the bounded admission capacity of a destination.
///
/// Both limits are mandatory. A destination must enforce both
/// independently rather than treating either as advisory.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryCapacity {
    pub max_pending_events: usize,
    pub max_pending_bytes: u64,
}

impl DeliveryCapacity {
    pub fn new(
        max_pending_events: usize,
        max_pending_bytes: u64,
    ) -> Result<Self, DeliveryCapacityError> {
        if max_pending_events == 0 {
            return Err(DeliveryCapacityError::ZeroEventLimit);
        }

        if max_pending_bytes == 0 {
            return Err(DeliveryCapacityError::ZeroByteLimit);
        }

        Ok(Self {
            max_pending_events,
            max_pending_bytes,
        })
    }
}

/// Validation errors for delivery capacity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryCapacityError {
    ZeroEventLimit,
    ZeroByteLimit,
}

impl std::fmt::Display for DeliveryCapacityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroEventLimit => {
                write!(f, "delivery event capacity must be positive")
            }
            Self::ZeroByteLimit => {
                write!(f, "delivery byte capacity must be positive")
            }
        }
    }
}

impl std::error::Error for DeliveryCapacityError {}

/// The result reported for one event at one destination.
///
/// The cursor is the original accepted-event identity.
/// Destination processing must not allocate a replacement cursor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DestinationDeliveryResult {
    pub cursor: StreamCursor,
    pub destination: DestinationKind,
    pub policy: DeliveryPolicy,
    pub status: DeliveryStatus,
}

impl DestinationDeliveryResult {
    pub fn pending(
        cursor: StreamCursor,
        destination: DestinationKind,
        policy: DeliveryPolicy,
    ) -> Self {
        Self {
            cursor,
            destination,
            policy,
            status: DeliveryStatus::Pending,
        }
    }

    pub fn progress(
        cursor: StreamCursor,
        destination: DestinationKind,
        policy: DeliveryPolicy,
        stage: DeliveryStage,
    ) -> Self {
        Self {
            cursor,
            destination,
            policy,
            status: DeliveryStatus::Progress(stage),
        }
    }

    pub fn failed(
        cursor: StreamCursor,
        destination: DestinationKind,
        policy: DeliveryPolicy,
        failure: DeliveryFailure,
    ) -> Self {
        Self {
            cursor,
            destination,
            policy,
            status: DeliveryStatus::Failed(failure),
        }
    }

    /// Whether this result reports an explicit failure of a
    /// required or durable destination obligation.
    pub fn is_required_violation(&self) -> bool {
        matches!(
            (self.policy, self.status),
            (
                DeliveryPolicy::Required | DeliveryPolicy::Durable,
                DeliveryStatus::Failed(_)
            )
        )
    }
}
