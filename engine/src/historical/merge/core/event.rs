use super::{depth_stream::DepthEventRef, ordering::MergeKey, trade_stream::TradeEventRef};

// -----------------------------------------------------------------------------
// Event kind
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MergeEventKind {
    Trade = 0,
    Depth = 1,
}

// -----------------------------------------------------------------------------
// Unified event reference
// -----------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum MergeEventRef {
    Trade(TradeEventRef),
    Depth(DepthEventRef),
}

impl MergeEventRef {
    pub fn kind(&self) -> MergeEventKind {
        match self {
            Self::Trade(_) => MergeEventKind::Trade,
            Self::Depth(_) => MergeEventKind::Depth,
        }
    }

    pub fn timestamp_ns(&self) -> i64 {
        match self {
            Self::Trade(event) => event.event_timestamp_ns,
            Self::Depth(event) => event.event.event_timestamp_ns,
        }
    }

    pub fn dataset_id(&self) -> u64 {
        match self {
            Self::Trade(event) => event.dataset_id,
            Self::Depth(event) => event.dataset_id,
        }
    }

    pub fn task_id(&self) -> u64 {
        match self {
            Self::Trade(event) => event.task_id,
            Self::Depth(event) => event.task_id,
        }
    }

    pub fn event_ordinal(&self) -> u64 {
        match self {
            Self::Trade(event) => event.event_ordinal,
            Self::Depth(event) => event.event.event_ordinal,
        }
    }

    pub fn canonical_row_offset(&self) -> u64 {
        match self {
            Self::Trade(event) => event.canonical_row_offset,
            Self::Depth(event) => event.event.canonical_row_offset,
        }
    }

    pub fn canonical_row_count(&self) -> u64 {
        match self {
            Self::Trade(_) => 1,
            Self::Depth(event) => event.event.canonical_row_count,
        }
    }

    pub fn merge_key(&self, stream_rank: u32) -> MergeKey {
        MergeKey {
            timestamp_ns: self.timestamp_ns(),
            stream_rank,
            task_rank: self.task_id(),
            event_ordinal: self.event_ordinal(),
        }
    }
}

// -----------------------------------------------------------------------------
// Merged event
// -----------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct MergedEvent {
    pub stream_rank: u32,
    pub event: MergeEventRef,
}
