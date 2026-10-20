//! Stream demand and ownership types required by MarketForge Live.
//!
//! Authoritative reference: 02 — Domain Models and Contracts.md, Sections 8.1–8.4.
//!
//! Source acquisition is shared across all consumers requiring
//! equivalent exchange data. The Stream Registry is the authoritative
//! owner of source identity, demand registration, and acquisition lifecycle.

use serde::{Deserialize, Serialize};

use crate::live::model::identity::{DerivedStreamId, StreamId};

/// Opaque identifier for one demand registration.
///
/// Each call to the Stream Registry's acquire method produces a unique
/// `DemandId` even if the underlying `DemandOwner` category is the same.
/// The allocation strategy is the Stream Registry's responsibility —
/// it **must not** rely solely on manually incremented counters combined
/// with owner category.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DemandId(pub u64);

/// Opaque identifier for a connected subscriber session.
///
/// Owned by the Subscriber subsystem (Document 10). Defined here
/// because `DemandOwner::Subscriber` references it.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SubscriberId(pub String);

/// Opaque identifier for a retention session.
///
/// Owned by the Retention subsystem (Document 13). Defined here
/// because `DemandOwner::RetentionSession` references it.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RetentionSessionId(pub String);

/// Category that originated a demand registration.
///
/// Multiple distinct `DemandId` registrations may share the same
/// `DemandOwner` category.
///
/// Demand ownership must not rely solely on manually incremented
/// counters for uniqueness — `DemandId` itself is the unique
/// registration identifier.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DemandOwner {
    /// A configured, persistent acquisition requirement.
    /// Survives runtime restarts.
    PersistentConfiguration,
    /// A recording subsystem requirement.
    /// Survives runtime restarts while the recording is enabled.
    Recording,
    /// A connected subscriber session.
    /// Existed only while the session remains active.
    Subscriber(SubscriberId),
    /// A derived stream requiring this source's data.
    /// Existed only while the derived stream remains registered.
    DerivedStream(DerivedStreamId),
    /// A temporary retention reservation.
    /// Existed only for the duration of the retention session.
    RetentionSession(RetentionSessionId),
}

/// An active claim on a source stream, returned by the registry.
///
/// Identifies one specific demand registration, not merely a stream.
/// The registry guarantees that concurrent equivalent requests do not
/// produce duplicate logical acquisitions; each successful acquire
/// call returns the handle to the authoritative demand registration.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StreamHandle {
    pub stream_id: StreamId,
    pub demand_id: DemandId,
}

impl StreamHandle {
    /// Construct a handle — opaque to the caller.
    pub fn new(stream_id: StreamId, demand_id: DemandId) -> Self {
        Self {
            stream_id,
            demand_id,
        }
    }
}
