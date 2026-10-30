use crate::shared::merge::{HeadQueue, MergeError, MergePoll};

use crate::shared::error::{MarketForgeError, Result};

use crate::historical::merge::output::manifests::manifest::MergeSkippedInitialization;
use {
    crate::historical::merge::core::ordering::MergeKey,
    crate::historical::merge::core::source::DepthEventSource,
    crate::historical::merge::depth::stream::DepthEventRef,
};
// -----------------------------------------------------------------------------
// Merged event
// -----------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct MergedDepthEvent {
    pub stream_rank: u32,
    pub event: DepthEventRef,
}

pub struct KWayDepthMerger {
    streams: Vec<Box<dyn DepthEventSource>>,
    queue: HeadQueue<DepthEventRef>,
}

impl KWayDepthMerger {
    pub fn new(mut streams: Vec<Box<dyn DepthEventSource>>) -> Result<Self> {
        let mut queue = HeadQueue::new(streams.len(), false).map_err(merge_error)?;
        for (index, stream) in streams.iter_mut().enumerate() {
            match stream.next_event()? {
                Some(event) => {
                    let stream_rank = u32::try_from(index)
                        .map_err(|_| merge_error(MergeError::TooManySources))?;
                    let key = MergeKey::from_depth_event(&event, stream_rank);
                    queue.push(index, key, event).map_err(merge_error)?;
                }
                None => queue.finish_source(index).map_err(merge_error)?,
            }
        }
        Ok(Self { streams, queue })
    }

    // -------------------------------------------------------------------------
    // Metrics
    // -------------------------------------------------------------------------

    pub fn events_emitted(&self) -> u64 {
        self.queue.events_emitted()
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
        let selected = match self.queue.next_head().map_err(merge_error)? {
            MergePoll::Ready(selected) => selected,
            MergePoll::Exhausted => return Ok(None),
            MergePoll::Pending => return invalid("Historical merge source has no pending head"),
        };
        let index = selected.source_index;
        // Preserve pull-ahead and error timing: advance before returning the event.
        match self.streams[index].next_event()? {
            Some(event) => {
                let stream_rank = selected.key.stream_rank;
                let key = MergeKey::from_depth_event(&event, stream_rank);
                self.queue.push(index, key, event).map_err(merge_error)?;
            }
            None => self.queue.finish_source(index).map_err(merge_error)?,
        }
        Ok(Some(MergedDepthEvent {
            stream_rank: selected.key.stream_rank,
            event: selected.event,
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

fn merge_error(error: MergeError) -> MarketForgeError {
    let message = match error {
        MergeError::EmptySources => "K-way merger requires at least one stream".to_owned(),
        MergeError::TooManySources => "too many merge streams".to_owned(),
        MergeError::MissingHead(index) => format!("missing pending merge event for stream {index}"),
        other => other.to_string(),
    };
    MarketForgeError::InvalidConfiguration(message)
}
