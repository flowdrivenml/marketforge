//! Lexicographic nanosecond ordering; field order is part of the contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MergeKey {
    pub timestamp_ns: i64,
    pub stream_rank: u32,
    pub task_rank: u64,
    pub event_ordinal: u64,
}

impl MergeKey {
    pub fn new(timestamp_ns: i64, stream_rank: u32, task_rank: u64, event_ordinal: u64) -> Self {
        Self {
            timestamp_ns,
            stream_rank,
            task_rank,
            event_ordinal,
        }
    }
}
