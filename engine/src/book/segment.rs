use crate::{
    error::{MarketForgeError, Result},
    job::IntegrityCategory,
};
use serde::{Deserialize, Serialize};

// -----------------------------------------------------------------------------
// Segment metadata
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BookSegment {
    pub segment_id: u64,

    pub start_timestamp_ns: i64,

    /// Global canonical event offset where initialization begins.
    pub initial_event_offset: u64,

    /// Number of canonical levels establishing the initial book.
    pub initial_level_count: u64,
}

// -----------------------------------------------------------------------------
// Segment tracker
// -----------------------------------------------------------------------------

#[derive(Debug, Default, Clone)]
pub struct SegmentTracker {
    segments: Vec<BookSegment>,
    next_event_offset: u64,
}

impl SegmentTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn segments(&self) -> &[BookSegment] {
        &self.segments
    }

    pub fn next_event_offset(&self) -> u64 {
        self.next_event_offset
    }

    /// Register a complete authoritative initialization snapshot.
    pub fn begin_segment(&mut self, timestamp_ns: i64, initial_level_count: usize) -> Result<u64> {
        let segment_id =
            u64::try_from(self.segments.len()).map_err(|_| invalid_error("segment ID overflow"))?;

        let initial_level_count = u64::try_from(initial_level_count)
            .map_err(|_| invalid_error("initial level count overflow"))?;

        let next_offset = self
            .next_event_offset
            .checked_add(initial_level_count)
            .ok_or_else(|| invalid_error("segment event offset overflow"))?;

        self.segments.push(BookSegment {
            segment_id,
            start_timestamp_ns: timestamp_ns,
            initial_event_offset: self.next_event_offset,
            initial_level_count,
        });

        self.next_event_offset = next_offset;

        Ok(segment_id)
    }

    /// Register canonical changes emitted after initialization.
    pub fn record_changes(&mut self, count: usize) -> Result<()> {
        let count = u64::try_from(count).map_err(|_| invalid_error("event count overflow"))?;

        self.next_event_offset = self
            .next_event_offset
            .checked_add(count)
            .ok_or_else(|| invalid_error("segment event offset overflow"))?;

        Ok(())
    }

    /// Reset all tracking information for a new processing task.
    pub fn clear(&mut self) {
        self.segments.clear();
        self.next_event_offset = 0;
    }
}

// -----------------------------------------------------------------------------
// Errors
// -----------------------------------------------------------------------------

fn invalid_error(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::RecordIntegrity {
        category: IntegrityCategory::InvalidRecord,
        message: message.into(),
    }
}
