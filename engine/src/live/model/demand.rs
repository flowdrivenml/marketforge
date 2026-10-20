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
/// This enum classifies **who** initiated the demand, not **whether**
/// the demand persists across restarts. Persistence lifetime is a
/// property of the subsystem that owns the owner — not an intrinsic
/// property of the `DemandOwner` variant.
///
/// # Persistent-configuration demands
///
/// `PersistentConfiguration` requirements are restored through the
/// Configuration Manager (Document 13) and the Runtime Reconciler
/// after restart. The Runtime Reconciler re-applies configuration to
/// the Stream Registry, which creates corresponding demand
/// registrations on behalf of `PersistentConfiguration`.
///
/// # Recording demands
///
/// `Recording` demands are created when a recording subsystem begins
/// acquisition and released when recording ends. Whether a recording
/// survives a restart depends on external configuration, not on this
/// enum variant.
///
/// # Derived-stream demands
///
/// `DerivedStream` demands represent demands originated by another
/// registered stream that depends on this source. Derived-stream
/// demands are **neither** inherently persistent **nor** inherently
/// transient. Their lifetime depends on whether the derived stream's
/// own registration is persistent or transient — determined by its
/// own `DemandOwner` and the configuration that established it.
///
/// # Demand registration
///
/// Demand registrations are managed independently by the Stream
/// Registry (Document 05). The registry creates, tracks, and
/// garbage-collects `DemandId` registrations. Multiple distinct
/// `DemandId` registrations may share the same `DemandOwner`
/// category — shared acquisition is the norm, not the exception.
///
/// The registry does **not** use manual reference counting. When
/// demand registrations are released, the registry determines whether
/// the underlying source stream must continue acquiring, pause to
/// idle, or be deregistered.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DemandOwner {
    /// A configured, persistent acquisition requirement (ownership category).
    PersistentConfiguration,
    /// A recording subsystem requirement (ownership category).
    Recording,
    /// A connected subscriber session (ownership category).
    Subscriber(SubscriberId),
    /// A derived stream requiring this source's data (ownership category).
    DerivedStream(DerivedStreamId),
    /// A temporary retention reservation (ownership category).
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
