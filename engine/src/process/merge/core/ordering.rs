use super::depth_stream::DepthEventRef;

// -----------------------------------------------------------------------------
// Deterministic chronological ordering
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct MergeKey {
    pub timestamp_ns: i64,
    pub stream_rank: u32,
    pub task_rank: u64,
    pub event_ordinal: u64,
}

impl MergeKey {
    pub fn from_depth_event(event: &DepthEventRef, stream_rank: u32) -> Self {
        Self {
            timestamp_ns: event.event.event_timestamp_ns,
            stream_rank,
            task_rank: event.task_id,
            event_ordinal: event.event.event_ordinal,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn orders_events_chronologically() {
        let earlier = MergeKey {
            timestamp_ns: 100,
            stream_rank: 3,
            task_rank: 0,
            event_ordinal: 1,
        };

        let later = MergeKey {
            timestamp_ns: 200,
            stream_rank: 0,
            task_rank: 0,
            event_ordinal: 1,
        };

        assert!(earlier < later);
    }

    #[test]
    fn resolves_equal_timestamps_by_stream_rank() {
        let first = MergeKey {
            timestamp_ns: 100,
            stream_rank: 0,
            task_rank: 0,
            event_ordinal: 1,
        };

        let second = MergeKey {
            timestamp_ns: 100,
            stream_rank: 1,
            task_rank: 0,
            event_ordinal: 1,
        };

        assert!(first < second);
    }

    #[test]
    fn resolves_equal_timestamps_within_stream() {
        let first = MergeKey {
            timestamp_ns: 100,
            stream_rank: 0,
            task_rank: 0,
            event_ordinal: 10,
        };

        let second = MergeKey {
            timestamp_ns: 100,
            stream_rank: 0,
            task_rank: 1,
            event_ordinal: 1,
        };

        assert!(first < second);
    }
}
