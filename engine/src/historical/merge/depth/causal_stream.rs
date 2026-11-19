//! Replay a committed stream in event-index order. Select whole dependency
//! prefixes of reconstruction segments, including zero-row observations.
use std::{ops::Range, vec::IntoIter};

use super::{
    cursor::DepthEventCursor, event_validation::visit_depth_events, inventory::DepthTaskInventory,
    stream::DepthEventRef,
};
use crate::{
    historical::{
        job::MergeTimeRange,
        merge::core::{event::MergeEventRef, event_source::MergeEventSource},
    },
    shared::{
        error::{MarketForgeError, Result},
        formats::depth::DepthEventBoundary,
    },
};

pub(crate) struct CausalDepthStream {
    tasks: IntoIter<DepthTaskInventory>,
    time_range: MergeTimeRange,
    current: Option<(u64, u64, DepthEventCursor)>,
}

impl CausalDepthStream {
    pub(crate) fn new(tasks: Vec<DepthTaskInventory>, time_range: MergeTimeRange) -> Self {
        Self {
            tasks: tasks.into_iter(),
            time_range,
            current: None,
        }
    }
}

impl MergeEventSource for CausalDepthStream {
    fn next_event(&mut self) -> Result<Option<MergeEventRef>> {
        loop {
            if let Some((dataset_id, task_id, cursor)) = &mut self.current {
                if let Some(event) = cursor.next_event()? {
                    return Ok(Some(MergeEventRef::Depth(DepthEventRef {
                        dataset_id: *dataset_id,
                        task_id: *task_id,
                        event,
                    })));
                }
                self.current = None;
            }

            let Some(task) = self.tasks.next() else {
                return Ok(None);
            };
            let (start, _, base) = task.source_range.unwrap_or((0, 0, 0));
            let ranges = selected_event_ranges(&task, self.time_range, start)?;
            if !ranges.is_empty() {
                let cursor =
                    DepthEventCursor::open_source_ranges(&task.events_path, &ranges, base, None)?;
                self.current = Some((task.dataset_id, task.task_id, cursor));
            }
        }
    }
}

/// Keep the segment initialization and all dependencies through the last
/// requested event. A later regression can make a prior out-of-range update
/// necessary. Plan from index positions, independent of timestamp monotonicity.
fn selected_event_ranges(
    task: &DepthTaskInventory,
    time: MergeTimeRange,
    base: u64,
) -> Result<Vec<Range<u64>>> {
    let mut selected = Vec::new();
    let mut initialization = None;
    let mut eligible_end = None;
    visit_depth_events(task, |position, event| {
        let position = base.checked_add(position).ok_or_else(|| {
            MarketForgeError::InvalidConfiguration("depth event index position overflow".into())
        })?;
        if event.event_boundary == DepthEventBoundary::Initialization {
            if let (Some(start), Some(end)) = (initialization, eligible_end.take()) {
                selected.push(start..end);
            }
            initialization = Some(position);
        }
        if (time.start_ns..time.end_ns).contains(&event.event_timestamp_ns) {
            if initialization.is_none() {
                return Err(MarketForgeError::InvalidConfiguration(format!(
                    "depth publication {} task {} needs a committed initialization before event {}",
                    task.dataset_id, task.task_id, event.event_ordinal
                )));
            }
            eligible_end = Some(position.checked_add(1).ok_or_else(|| {
                MarketForgeError::InvalidConfiguration("depth event index position overflow".into())
            })?);
        }
        Ok(())
    })?;
    if let (Some(start), Some(end)) = (initialization, eligible_end) {
        selected.push(start..end);
    }
    Ok(selected)
}
