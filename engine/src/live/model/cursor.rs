//! Stream cursor and positioning types.
//!
//! Authoritative reference: 02 — Domain Models and Contracts.md, Sections 4–5.

use serde::{Deserialize, Serialize};

use crate::live::model::identity::StreamId;

/// Namespace in which MarketForge cursor offsets are unique and ordered.
///
// Log epochs should not automatically change after every WebSocket reconnection.
// A reconnect may interrupt exchange continuity while the retained event log
// remains structurally continuous.
// Log epochs change when the runtime cannot safely continue the previous
// cursor namespace or when an explicitly defined log lifecycle transition
// requires it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LogEpoch(pub u64);

/// Position within a MarketForge-managed event sequence.
///
// The tuple (stream_id, log_epoch, offset) must uniquely identify a retained
// event position.
//
// Cursor invariants:
// - Offsets increase monotonically within a log epoch.
// - An offset is never reused for a different event within the same epoch.
// - Cursor ordering is defined only within a compatible stream and log epoch.
// - Cursor assignment must preserve accepted source-event order.
// - Moving an event between RAM and Parquet must not change its cursor.
// - Retention deletion must not permit cursor reuse.
// - A cursor must not imply historical completeness or exchange sequence continuity.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StreamCursor {
    pub stream_id: StreamId,
    pub log_epoch: LogEpoch,
    pub offset: u64,
}

impl StreamCursor {
    /// Compare positions only within the same retained log namespace.
    pub fn compare_position(&self, other: &Self) -> Option<std::cmp::Ordering> {
        (self.stream_id == other.stream_id && self.log_epoch == other.log_epoch)
            .then(|| self.offset.cmp(&other.offset))
    }

    pub fn new(stream_id: StreamId, log_epoch: LogEpoch, offset: u64) -> Self {
        Self {
            stream_id,
            log_epoch,
            offset,
        }
    }
}

/// Identifies one physical connection attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ConnectionGeneration(pub u64);

/// Identifies one subscription activation lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SubscriptionGeneration(pub u64);

/// Identifies one synchronized order-book state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct BookGeneration(pub u64);

/// Identifies one derived-stream semantic generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DerivedGeneration(pub u64);
