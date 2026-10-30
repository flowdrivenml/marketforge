use super::MergeKey;
use std::{cmp::Reverse, collections::BinaryHeap};

/// One complete event is an opaque payload; depth events are never split.
#[derive(Debug)]
pub struct SelectedHead<E> {
    pub source_index: usize,
    pub key: MergeKey,
    pub event: E,
}

/// Pending means at least one active source has no known head yet. It is not EOF.
#[derive(Debug)]
pub enum MergePoll<E> {
    Ready(SelectedHead<E>),
    Pending,
    Exhausted,
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum MergeError {
    #[error("K-way merger requires at least one source")]
    EmptySources,
    #[error("too many merge sources")]
    TooManySources,
    #[error("invalid merge source index {0}")]
    InvalidSource(usize),
    #[error("merge source {0} already has a head or is exhausted")]
    SourceNotAwaiting(usize),
    #[error("merge key rank does not identify source {0}")]
    RankMismatch(usize),
    #[error("missing pending merge event for source {0}")]
    MissingHead(usize),
    #[error(
        "merge source {source_index} produced an ordering regression: previous={previous:?}, current={current:?}"
    )]
    SourceRegression {
        source_index: usize,
        previous: MergeKey,
        current: MergeKey,
    },
    #[error("K-way merge ordering regression: previous={previous:?}, current={current:?}")]
    GlobalRegression {
        previous: MergeKey,
        current: MergeKey,
    },
    #[error("merged event count overflow")]
    EventCountOverflow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct HeapEntry {
    key: MergeKey,
    stream_index: usize,
}

enum Slot<E> {
    Awaiting,
    Ready(E),
    Exhausted,
}

/// One pending event per source, O(sources) memory and O(log sources) selection.
///
/// Source indices assign stable unique stream ranks. Every active source must
/// supply a head before selection is safe. After Ready, advance only that source
/// and call `push` or `finish_source`. A temporarily empty source stays awaiting.
/// No arrival-order, watermark, lateness or asynchronous policy is supplied here.
pub struct HeadQueue<E> {
    pending: Vec<Slot<E>>,
    heap: BinaryHeap<Reverse<HeapEntry>>,
    awaiting: usize,
    previous_key: Option<MergeKey>,
    events_emitted: u64,
    validate_source_order: bool,
}

impl<E> HeadQueue<E> {
    /// Historical unified merging checks source order on replacement; its legacy
    /// depth coordinator checks global order on selection instead. The flag
    /// preserves this distinction without changing acceptance or error timing.
    pub fn new(source_count: usize, validate_source_order: bool) -> Result<Self, MergeError> {
        if source_count == 0 {
            return Err(MergeError::EmptySources);
        }
        if source_count > u32::MAX as usize {
            return Err(MergeError::TooManySources);
        }
        Ok(Self {
            pending: (0..source_count).map(|_| Slot::Awaiting).collect(),
            heap: BinaryHeap::with_capacity(source_count),
            awaiting: source_count,
            previous_key: None,
            events_emitted: 0,
            validate_source_order,
        })
    }
    pub fn push(&mut self, source_index: usize, key: MergeKey, event: E) -> Result<(), MergeError> {
        let slot = self
            .pending
            .get_mut(source_index)
            .ok_or(MergeError::InvalidSource(source_index))?;
        if !matches!(slot, Slot::Awaiting) {
            return Err(MergeError::SourceNotAwaiting(source_index));
        }
        if key.stream_rank as usize != source_index {
            return Err(MergeError::RankMismatch(source_index));
        }
        if self.validate_source_order {
            if let Some(previous) = self
                .previous_key
                .filter(|previous| previous.stream_rank as usize == source_index)
            {
                if key < previous {
                    return Err(MergeError::SourceRegression {
                        source_index,
                        previous,
                        current: key,
                    });
                }
            }
        }
        *slot = Slot::Ready(event);
        self.awaiting -= 1;
        self.heap.push(Reverse(HeapEntry {
            key,
            stream_index: source_index,
        }));
        Ok(())
    }
    pub fn finish_source(&mut self, source_index: usize) -> Result<(), MergeError> {
        let slot = self
            .pending
            .get_mut(source_index)
            .ok_or(MergeError::InvalidSource(source_index))?;
        if !matches!(slot, Slot::Awaiting) {
            return Err(MergeError::SourceNotAwaiting(source_index));
        }
        *slot = Slot::Exhausted;
        self.awaiting -= 1;
        Ok(())
    }
    pub fn next_head(&mut self) -> Result<MergePoll<E>, MergeError> {
        if self.awaiting != 0 {
            return Ok(MergePoll::Pending);
        }
        let Some(Reverse(entry)) = self.heap.pop() else {
            return Ok(MergePoll::Exhausted);
        };
        let index = entry.stream_index;
        let Slot::Ready(event) = std::mem::replace(&mut self.pending[index], Slot::Awaiting) else {
            return Err(MergeError::MissingHead(index));
        };
        self.awaiting += 1;
        if let Some(previous) = self.previous_key {
            if entry.key < previous {
                return Err(MergeError::GlobalRegression {
                    previous,
                    current: entry.key,
                });
            }
        }
        self.previous_key = Some(entry.key);
        self.events_emitted = self
            .events_emitted
            .checked_add(1)
            .ok_or(MergeError::EventCountOverflow)?;
        Ok(MergePoll::Ready(SelectedHead {
            source_index: index,
            key: entry.key,
            event,
        }))
    }
    pub fn events_emitted(&self) -> u64 {
        self.events_emitted
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn event_count_overflow_is_an_error() {
        let mut queue = HeadQueue::new(1, true).unwrap();
        queue.events_emitted = u64::MAX;
        queue.push(0, MergeKey::new(0, 0, 0, 0), ()).unwrap();
        assert!(matches!(
            queue.next_head(),
            Err(MergeError::EventCountOverflow)
        ));
    }
}
