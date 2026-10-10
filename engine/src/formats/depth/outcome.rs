use crate::canonical::L2LevelUpdate;

/// Metadata describing how canonical levels should be interpreted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DepthEventBoundary {
    /// Complete initialization of an independently reconstructable book.
    Initialization,

    /// Ordinary changes within the current reconstruction segment.
    Changes,
}

/// Source metadata associated with one depth message.
///
/// Native sequence identifiers are optional because not every exchange
/// provides them.
///
/// The ordinal identifies the source record's position within its task.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DepthSourceEventMetadata {
    pub source_event_ordinal: Option<u64>,

    pub event_timestamp_ns: i64,
    pub system_timestamp_ns: Option<i64>,

    pub sequence_start: Option<u64>,
    pub sequence_end: Option<u64>,
}

/// Operation represented by the original exchange message.
///
/// Independent of reconstruction-segment boundaries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DepthSourceOperation {
    Snapshot,
    AbsoluteUpdate,
    RelativeUpdate,
}

/// Result of processing one source depth message.
#[derive(Debug)]
pub struct DepthProcessingOutcome {
    pub boundary: DepthEventBoundary,
    pub operation: DepthSourceOperation,
    pub events: Vec<L2LevelUpdate>,
    pub source: DepthSourceEventMetadata,
}

impl DepthProcessingOutcome {
    pub fn initialization(events: Vec<L2LevelUpdate>) -> Self {
        Self {
            boundary: DepthEventBoundary::Initialization,
            operation: DepthSourceOperation::Snapshot,
            events,
            source: DepthSourceEventMetadata::default(),
        }
    }

    pub fn changes(events: Vec<L2LevelUpdate>) -> Self {
        Self {
            boundary: DepthEventBoundary::Changes,
            operation: DepthSourceOperation::AbsoluteUpdate,
            events,
            source: DepthSourceEventMetadata::default(),
        }
    }

    pub fn with_source(mut self, source: DepthSourceEventMetadata) -> Self {
        self.source = source;
        self
    }
}
