use std::{cmp::Reverse, collections::BinaryHeap};

use crate::error::{MarketForgeError, Result};

use super::manifest::MergeSkippedInitialization;
use super::{depth_stream::DepthEventRef, ordering::MergeKey, source::DepthEventSource};
// -----------------------------------------------------------------------------
// Merged event
// -----------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct MergedDepthEvent {
    pub stream_rank: u32,
    pub event: DepthEventRef,
}

// -----------------------------------------------------------------------------
// Heap entry
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct HeapEntry {
    key: MergeKey,
    stream_index: usize,
}

// -----------------------------------------------------------------------------
// K-way chronological merger
// -----------------------------------------------------------------------------

pub struct KWayDepthMerger {
    streams: Vec<Box<dyn DepthEventSource>>,
    pending: Vec<Option<DepthEventRef>>,
    heap: BinaryHeap<Reverse<HeapEntry>>,

    events_emitted: u64,
    previous_key: Option<MergeKey>,
}

impl KWayDepthMerger {
    pub fn new(mut streams: Vec<Box<dyn DepthEventSource>>) -> Result<Self> {
        if streams.is_empty() {
            return invalid("K-way merger requires at least one stream");
        }

        if streams.len() > u32::MAX as usize {
            return invalid("too many merge streams");
        }

        // Each stream contributes one pending event.
        let mut pending = Vec::with_capacity(streams.len());
        let mut heap = BinaryHeap::with_capacity(streams.len());

        for (index, stream) in streams.iter_mut().enumerate() {
            let event = stream.next_event()?;

            if let Some(ref event) = event {
                let stream_rank = u32::try_from(index).map_err(|_| {
                    MarketForgeError::InvalidConfiguration("merge stream rank overflow".to_owned())
                })?;

                let key = MergeKey::from_depth_event(event, stream_rank);

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
    // Metrics
    // -------------------------------------------------------------------------

    pub fn events_emitted(&self) -> u64 {
        self.events_emitted
    }

    pub fn timestamp_regressions_dropped(&self) -> Result<u64> {
        self.streams.iter().try_fold(0_u64, |total, stream| {
            total
                .checked_add(stream.timestamp_regressions_dropped())
                .ok_or_else(|| {
                    MarketForgeError::InvalidConfiguration(
                        "timestamp regression counter overflow".to_owned(),
                    )
                })
        })
    }

    // -------------------------------------------------------------------------
    // Next chronological event
    // -------------------------------------------------------------------------

    pub fn next_event(&mut self) -> Result<Option<MergedDepthEvent>> {
        let Some(Reverse(entry)) = self.heap.pop() else {
            return Ok(None);
        };

        let index = entry.stream_index;

        let event = self.pending[index].take().ok_or_else(|| {
            MarketForgeError::InvalidConfiguration(format!(
                "missing pending merge event for stream {index}"
            ))
        })?;

        // Verify global ordering.
        if let Some(previous) = self.previous_key {
            if entry.key < previous {
                return invalid(format!(
                    "K-way merge ordering regression: previous={previous:?}, current={:?}",
                    entry.key,
                ));
            }
        }

        self.previous_key = Some(entry.key);

        self.events_emitted = self.events_emitted.checked_add(1).ok_or_else(|| {
            MarketForgeError::InvalidConfiguration("merged event count overflow".to_owned())
        })?;

        // Advance only the stream that supplied the emitted event.
        let next = self.streams[index].next_event()?;

        if let Some(ref next_event) = next {
            let stream_rank = u32::try_from(index).map_err(|_| {
                MarketForgeError::InvalidConfiguration("merge stream rank overflow".to_owned())
            })?;

            let key = MergeKey::from_depth_event(next_event, stream_rank);

            self.heap.push(Reverse(HeapEntry {
                key,
                stream_index: index,
            }));
        }

        self.pending[index] = next;

        Ok(Some(MergedDepthEvent {
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
}

// -----------------------------------------------------------------------------
// Errors
// -----------------------------------------------------------------------------

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(MarketForgeError::InvalidConfiguration(message.into()))
}
