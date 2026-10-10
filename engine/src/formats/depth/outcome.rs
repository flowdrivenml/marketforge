use crate::canonical::L2LevelUpdate;

/// Metadata describing how canonical levels should be interpreted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DepthEventBoundary {
    /// Complete initialization of an independently reconstructable book.
    Initialization,

    /// Ordinary changes within the current reconstruction segment.
    Changes,
}

/// Result of processing one source depth message.
#[derive(Debug)]
pub struct DepthProcessingOutcome {
    pub boundary: DepthEventBoundary,
    pub events: Vec<L2LevelUpdate>,
}

impl DepthProcessingOutcome {
    pub fn initialization(events: Vec<L2LevelUpdate>) -> Self {
        Self {
            boundary: DepthEventBoundary::Initialization,
            events,
        }
    }

    pub fn changes(events: Vec<L2LevelUpdate>) -> Self {
        Self {
            boundary: DepthEventBoundary::Changes,
            events,
        }
    }
}
