use std::{cmp::Reverse, collections::BinaryHeap};

use crate::historical::merge::output::manifests::manifest::MergeSkippedInitialization;
use crate::shared::error::{MarketForgeError, Result};

use {
    crate::historical::merge::core::event::MergeEventRef,
    crate::historical::merge::core::event::MergedEvent,
    crate::historical::merge::core::event_source::MergeEventSource,
    crate::historical::merge::core::ordering::MergeKey,
};

// -----------------------------------------------------------------------------
// Heap entry
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct HeapEntry {
    key: MergeKey,
    stream_index: usize,
}

// -----------------------------------------------------------------------------
// Generic K-way chronological merger
// -----------------------------------------------------------------------------

pub struct KWayEventMerger {
    streams: Vec<Box<dyn MergeEventSource>>,
    pending: Vec<Option<MergeEventRef>>,
    heap: BinaryHeap<Reverse<HeapEntry>>,

    events_emitted: u64,
    previous_key: Option<MergeKey>,
}

impl KWayEventMerger {
    // -------------------------------------------------------------------------
    // Construction
    // -------------------------------------------------------------------------

    pub fn new(mut streams: Vec<Box<dyn MergeEventSource>>) -> Result<Self> {
        if streams.is_empty() {
            return invalid("K-way event merger requires at least one stream");
        }

        if streams.len() > u32::MAX as usize {
            return invalid("too many merge event streams");
        }

        let mut pending = Vec::with_capacity(streams.len());
        let mut heap = BinaryHeap::with_capacity(streams.len());

        for (index, stream) in streams.iter_mut().enumerate() {
            let event = stream.next_event()?;

            if let Some(ref event) = event {
                let stream_rank = u32::try_from(index)
                    .map_err(|_| invalid_error("merge stream rank overflow"))?;

                let key = event.merge_key(stream_rank);

                heap.push(Reverse(HeapEntry {
                    key,
                    stream_index: index,
                }));
            }

            pending.push(event);
        }

        Ok(Self {
            streams,
            pending,
            heap,

            events_emitted: 0,
            previous_key: None,
        })
    }

    // -------------------------------------------------------------------------
    // Next chronological event
    // -------------------------------------------------------------------------

    pub fn next_event(&mut self) -> Result<Option<MergedEvent>> {
        let Some(Reverse(entry)) = self.heap.pop() else {
            return Ok(None);
        };

        let index = entry.stream_index;

        let event = self.pending[index].take().ok_or_else(|| {
            invalid_error(format!("missing pending event for merge stream {index}"))
        })?;

        // ---------------------------------------------------------------------
        // Verify global ordering
        // ---------------------------------------------------------------------

        if let Some(previous) = self.previous_key {
            if entry.key < previous {
                return invalid(format!(
                    "K-way event ordering regression: \
                     previous={previous:?}, current={:?}",
                    entry.key,
                ));
            }
        }

        self.previous_key = Some(entry.key);

        self.events_emitted = self
            .events_emitted
            .checked_add(1)
            .ok_or_else(|| invalid_error("merged event count overflow"))?;

        // ---------------------------------------------------------------------
        // Advance only the contributing stream
        // ---------------------------------------------------------------------

        let next = self.streams[index].next_event()?;

        if let Some(ref next_event) = next {
            let stream_rank =
                u32::try_from(index).map_err(|_| invalid_error("merge stream rank overflow"))?;

            let key = next_event.merge_key(stream_rank);

            // Each individual source must preserve chronological ordering.
            if key < entry.key {
                return invalid(format!(
                    "merge source {index} produced an ordering regression: \
                     previous={:?}, current={key:?}",
                    entry.key,
                ));
            }

            self.heap.push(Reverse(HeapEntry {
                key,
                stream_index: index,
            }));
        }

        self.pending[index] = next;

        Ok(Some(MergedEvent {
            stream_rank: entry.key.stream_rank,
            event,
        }))
    }
    pub fn skipped_initializations(&self) -> Vec<MergeSkippedInitialization> {
        self.streams
            .iter()
            .flat_map(|stream| stream.skipped_initializations().iter().cloned())
            .collect()
    }
    pub fn events_emitted(&self) -> u64 {
        self.events_emitted
    }
}

// -----------------------------------------------------------------------------
// Errors
// -----------------------------------------------------------------------------

fn invalid_error(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(message.into())
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(invalid_error(message))
}
