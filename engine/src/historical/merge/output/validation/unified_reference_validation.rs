use std::collections::HashMap;

use crate::shared::error::{MarketForgeError, Result};

use {
    crate::historical::merge::depth::inventory::DepthTaskInventory,
    crate::historical::merge::output::writers::unified_timeline::UnifiedTimelineRecord,
    crate::historical::merge::trades::inventory::TradeTaskInventory,
};

// -----------------------------------------------------------------------------
// Canonical task bounds
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
pub struct CanonicalTaskBounds {
    pub dataset_id: u64,
    pub task_id: u64,
    pub canonical_rows: u64,
    pub is_trade: bool,
}

// -----------------------------------------------------------------------------
// Per-stream validation state
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
struct StreamState {
    task_id: u64,
    event_ordinal: u64,
    canonical_row_offset: u64,
    canonical_row_count: u64,
}

// -----------------------------------------------------------------------------
// Reference validator
// -----------------------------------------------------------------------------

pub struct UnifiedReferenceValidator {
    tasks: HashMap<(u64, u64), CanonicalTaskBounds>,
    streams: HashMap<u32, StreamState>,
}

impl UnifiedReferenceValidator {
    pub fn new(trades: &[TradeTaskInventory], depth: &[DepthTaskInventory]) -> Result<Self> {
        let mut tasks = HashMap::new();

        for task in trades {
            let bounds = CanonicalTaskBounds {
                dataset_id: task.dataset_id,
                task_id: task.task_id,
                canonical_rows: task.canonical_rows,
                is_trade: true,
            };

            if tasks
                .insert((task.dataset_id, task.task_id), bounds)
                .is_some()
            {
                return invalid("duplicate canonical task identity");
            }
        }

        for task in depth {
            let bounds = CanonicalTaskBounds {
                dataset_id: task.dataset_id,
                task_id: task.task_id,
                canonical_rows: task.canonical_rows,
                is_trade: false,
            };

            if tasks
                .insert((task.dataset_id, task.task_id), bounds)
                .is_some()
            {
                return invalid("duplicate canonical task identity");
            }
        }

        Ok(Self {
            tasks,
            streams: HashMap::new(),
        })
    }

    pub fn validate(&mut self, record: &UnifiedTimelineRecord) -> Result<()> {
        // ---------------------------------------------------------------------
        // Resolve source task
        // ---------------------------------------------------------------------

        let bounds = self
            .tasks
            .get(&(record.dataset_id, record.task_id))
            .ok_or_else(|| {
                invalid_error(format!(
                    "unknown canonical task: dataset={} task={}",
                    record.dataset_id, record.task_id
                ))
            })?;

        // ---------------------------------------------------------------------
        // Validate event kind
        // ---------------------------------------------------------------------

        let is_trade = match record.event_kind {
            0 => true,
            1 => false,
            kind => {
                return invalid(format!("invalid canonical event kind {kind}"));
            }
        };

        if bounds.is_trade != is_trade {
            return invalid(format!(
                "canonical task/event kind mismatch: dataset={} task={}",
                record.dataset_id, record.task_id
            ));
        }

        // ---------------------------------------------------------------------
        // Validate canonical row range
        // ---------------------------------------------------------------------

        // Trades always reference exactly one canonical row.
        // Depth events may represent updates without emitted book-level changes.

        if is_trade && record.canonical_row_count != 1 {
            return invalid("trade event must reference exactly one row");
        }

        let end = record
            .canonical_row_offset
            .checked_add(record.canonical_row_count)
            .ok_or_else(|| invalid_error("canonical row range overflow"))?;

        if end > bounds.canonical_rows {
            return invalid(format!(
                "canonical reference exceeds task bounds: \
                 dataset={} task={} end={} rows={}",
                record.dataset_id, record.task_id, end, bounds.canonical_rows
            ));
        }

        // ---------------------------------------------------------------------
        // Validate source progression
        // ---------------------------------------------------------------------

        if let Some(previous) = self.streams.get(&record.stream_rank) {
            if record.task_id < previous.task_id {
                return invalid("canonical task ordering regression");
            }

            if record.task_id == previous.task_id {
                if record.event_ordinal <= previous.event_ordinal {
                    return invalid("duplicate or regressing canonical event ordinal");
                }

                if record.canonical_row_offset < previous.canonical_row_offset {
                    return invalid("canonical row offset regression");
                }

                // Overlapping canonical row ranges are not permitted.
                let previous_end = previous
                    .canonical_row_offset
                    .checked_add(previous.canonical_row_count)
                    .ok_or_else(|| invalid_error("previous canonical range overflow"))?;

                if record.canonical_row_offset < previous_end {
                    return invalid("overlapping canonical row references");
                }
            }
        }

        self.streams.insert(
            record.stream_rank,
            StreamState {
                task_id: record.task_id,
                event_ordinal: record.event_ordinal,
                canonical_row_offset: record.canonical_row_offset,
                canonical_row_count: record.canonical_row_count,
            },
        );

        Ok(())
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
