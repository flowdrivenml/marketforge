use crate::shared::merge::{HeadQueue, MergeError, MergePoll};

use crate::historical::merge::output::manifests::manifest::MergeSkippedInitialization;
use crate::shared::error::{MarketForgeError, Result};

use {
    crate::historical::merge::core::event::MergeEventRef,
    crate::historical::merge::core::event::MergedEvent,
    crate::historical::merge::core::event_source::MergeEventSource,
};

pub struct KWayEventMerger {
    streams: Vec<Box<dyn MergeEventSource>>,
    queue: HeadQueue<MergeEventRef>,
}

impl KWayEventMerger {
    // -------------------------------------------------------------------------
    // Construction
    // -------------------------------------------------------------------------

    pub fn new(mut streams: Vec<Box<dyn MergeEventSource>>) -> Result<Self> {
        let mut queue = HeadQueue::new(streams.len(), true).map_err(merge_error)?;
        for (index, stream) in streams.iter_mut().enumerate() {
            match stream.next_event()? {
                Some(event) => {
                    let stream_rank = u32::try_from(index)
                        .map_err(|_| merge_error(MergeError::TooManySources))?;
                    let key = event.merge_key(stream_rank);
                    queue.push(index, key, event).map_err(merge_error)?;
                }
                None => queue.finish_source(index).map_err(merge_error)?,
            }
        }
        Ok(Self { streams, queue })
    }

    // -------------------------------------------------------------------------
    // Next chronological event
    // -------------------------------------------------------------------------

    pub fn next_event(&mut self) -> Result<Option<MergedEvent>> {
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
                let key = event.merge_key(stream_rank);
                self.queue.push(index, key, event).map_err(merge_error)?;
            }
            None => self.queue.finish_source(index).map_err(merge_error)?,
        }
        Ok(Some(MergedEvent {
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
    pub fn events_emitted(&self) -> u64 {
        self.queue.events_emitted()
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

fn merge_error(error: MergeError) -> MarketForgeError {
    let message = match error {
        MergeError::EmptySources => "K-way event merger requires at least one stream".to_owned(),
        MergeError::TooManySources => "too many merge event streams".to_owned(),
        MergeError::MissingHead(index) => format!("missing pending event for merge stream {index}"),
        MergeError::GlobalRegression { previous, current } => {
            format!("K-way event ordering regression: previous={previous:?}, current={current:?}")
        }
        other => other.to_string(),
    };
    MarketForgeError::InvalidConfiguration(message)
}
