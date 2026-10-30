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
        let Some(&Reverse(entry)) = self.heap.peek() else {
            return Ok(MergePoll::Exhausted);
        };
        let index = entry.stream_index;
        if !matches!(self.pending.get(index), Some(Slot::Ready(_))) {
            return Err(MergeError::MissingHead(index));
        }
        if let Some(previous) = self.previous_key {
            if entry.key < previous {
                return Err(MergeError::GlobalRegression {
                    previous,
                    current: entry.key,
                });
            }
        }
        let events_emitted = self
            .events_emitted
            .checked_add(1)
            .ok_or(MergeError::EventCountOverflow)?;

        // All fallible validation precedes removal. Move the payload only once;
        // selection errors leave the entire queue unchanged.
        let Slot::Ready(event) = std::mem::replace(&mut self.pending[index], Slot::Awaiting) else {
            unreachable!("pending head was validated before selection");
        };
        self.heap.pop();
        self.awaiting += 1;
        self.previous_key = Some(entry.key);
        self.events_emitted = events_emitted;
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

    #[derive(Debug, PartialEq, Eq)]
    struct QueueState {
        heap: Vec<Reverse<HeapEntry>>,
        slots: Vec<(&'static str, Option<&'static str>)>,
        awaiting: usize,
        previous_key: Option<MergeKey>,
        events_emitted: u64,
    }

    fn state(queue: &HeadQueue<&'static str>) -> QueueState {
        QueueState {
            heap: queue.heap.as_slice().to_vec(),
            slots: queue
                .pending
                .iter()
                .map(|slot| match slot {
                    Slot::Awaiting => ("awaiting", None),
                    Slot::Ready(event) => ("ready", Some(*event)),
                    Slot::Exhausted => ("exhausted", None),
                })
                .collect(),
            awaiting: queue.awaiting,
            previous_key: queue.previous_key,
            events_emitted: queue.events_emitted,
        }
    }

    fn assert_invariants<E>(queue: &HeadQueue<E>) {
        assert_eq!(
            queue.awaiting,
            queue
                .pending
                .iter()
                .filter(|slot| matches!(slot, Slot::Awaiting))
                .count()
        );
        assert_eq!(
            queue.heap.len(),
            queue
                .pending
                .iter()
                .filter(|slot| matches!(slot, Slot::Ready(_)))
                .count()
        );
        for (index, slot) in queue.pending.iter().enumerate() {
            let heads = queue
                .heap
                .iter()
                .filter(|entry| entry.0.stream_index == index)
                .count();
            assert_eq!(heads, usize::from(matches!(slot, Slot::Ready(_))));
        }
    }

    fn selected<E>(queue: &mut HeadQueue<E>) -> SelectedHead<E> {
        let MergePoll::Ready(head) = queue.next_head().unwrap() else {
            panic!("expected a ready head")
        };
        assert_invariants(queue);
        head
    }

    #[test]
    fn global_regression_preserves_pending_event_and_queue_state() {
        let mut queue = HeadQueue::new(2, false).unwrap();
        let previous = MergeKey::new(10, 0, 0, 0);
        let current = MergeKey::new(9, 0, 0, 1);
        queue.push(0, previous, "first").unwrap();
        queue
            .push(1, MergeKey::new(20, 1, 0, 0), "other source")
            .unwrap();
        assert_eq!(selected(&mut queue).event, "first");
        queue.push(0, current, "regressing").unwrap();
        let before = state(&queue);
        for _ in 0..2 {
            assert!(
                matches!(queue.next_head(), Err(MergeError::GlobalRegression { previous: p, current: c }) if p == previous && c == current)
            );
            assert_eq!(state(&queue), before);
            assert_invariants(&queue);
        }
        // Relax the injected ordering condition to prove the original payload
        // and the other source's head are both still selectable.
        queue.previous_key = None;
        assert_eq!(selected(&mut queue).event, "regressing");
        queue.finish_source(0).unwrap();
        assert_eq!(selected(&mut queue).event, "other source");
        assert_eq!(queue.events_emitted(), 3);
    }

    #[test]
    fn event_count_overflow_preserves_pending_event_and_queue_state() {
        let mut queue = HeadQueue::new(1, true).unwrap();
        let previous = MergeKey::new(10, 0, 0, 0);
        queue.push(0, previous, "first").unwrap();
        selected(&mut queue);
        let current = MergeKey::new(11, 0, 0, 1);
        queue.push(0, current, "retained").unwrap();
        queue.events_emitted = u64::MAX;
        let before = state(&queue);
        for _ in 0..2 {
            assert!(matches!(
                queue.next_head(),
                Err(MergeError::EventCountOverflow)
            ));
            assert_eq!(state(&queue), before);
            assert_invariants(&queue);
        }
        queue.events_emitted = 1;
        let head = selected(&mut queue);
        assert_eq!(head.event, "retained");
        assert_eq!(head.key, current);
        assert_eq!(queue.previous_key, Some(current));
        assert_eq!(queue.events_emitted(), 2);
    }

    #[test]
    fn missing_head_validation_is_non_destructive() {
        let mut queue = HeadQueue::new(1, true).unwrap();
        queue.push(0, MergeKey::new(0, 0, 0, 0), "event").unwrap();
        // An inconsistent slot is not reachable through the public API, but
        // its defensive validation must not destroy the heap entry either.
        queue.pending[0] = Slot::Exhausted;
        let before = state(&queue);
        assert!(matches!(queue.next_head(), Err(MergeError::MissingHead(0))));
        assert_eq!(state(&queue), before);
    }

    #[test]
    fn normal_selection_pending_and_exhaustion_preserve_invariants() {
        let mut queue = HeadQueue::new(2, true).unwrap();
        queue.push(0, MergeKey::new(20, 0, 0, 0), "later").unwrap();
        let before = state(&queue);
        assert!(matches!(queue.next_head().unwrap(), MergePoll::Pending));
        assert_eq!(state(&queue), before);
        queue
            .push(1, MergeKey::new(10, 1, 0, 0), "earlier")
            .unwrap();
        let head = selected(&mut queue);
        assert_eq!((head.source_index, head.event), (1, "earlier"));
        assert_eq!(queue.previous_key, Some(head.key));
        assert_eq!(queue.events_emitted(), 1);
        queue.finish_source(1).unwrap();
        assert_eq!(selected(&mut queue).event, "later");
        queue.finish_source(0).unwrap();
        assert_invariants(&queue);
        let before = state(&queue);
        for _ in 0..2 {
            assert!(matches!(queue.next_head().unwrap(), MergePoll::Exhausted));
            assert_eq!(state(&queue), before);
        }
    }
}
