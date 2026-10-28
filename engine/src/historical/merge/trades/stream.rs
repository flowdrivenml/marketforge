use crate::shared::error::{MarketForgeError, Result};

use {
    crate::historical::merge::trades::cursor::TradeCursor,
    crate::historical::merge::trades::inventory::TradeTaskInventory,
};

// -----------------------------------------------------------------------------
// Trade event reference
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TradeEventRef {
    pub dataset_id: u64,
    pub task_id: u64,

    /// Zero-based task-local trade ordinal.
    pub event_ordinal: u64,

    /// Task-global canonical row offset.
    pub canonical_row_offset: u64,

    pub event_timestamp_ns: i64,
}

// -----------------------------------------------------------------------------
// Trade stream
// -----------------------------------------------------------------------------

pub struct TradeStream {
    dataset_id: u64,
    tasks: Vec<TradeTaskInventory>,

    current_task: usize,
    current_file: usize,
    current_cursor: Option<TradeCursor>,

    range: Option<(i64, i64)>,

    previous_timestamp_ns: Option<i64>,
    events_emitted: u64,
}

// -----------------------------------------------------------------------------
// Construction
// -----------------------------------------------------------------------------

impl TradeStream {
    pub fn new(dataset_id: u64, tasks: Vec<TradeTaskInventory>) -> Result<Self> {
        Self::build(dataset_id, tasks, None)
    }

    pub fn new_range(
        dataset_id: u64,
        tasks: Vec<TradeTaskInventory>,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Self> {
        if start_ns >= end_ns {
            return invalid("invalid trade stream timestamp range");
        }

        Self::build(dataset_id, tasks, Some((start_ns, end_ns)))
    }

    fn build(
        dataset_id: u64,
        mut tasks: Vec<TradeTaskInventory>,
        range: Option<(i64, i64)>,
    ) -> Result<Self> {
        if tasks.iter().any(|task| task.dataset_id != dataset_id) {
            return invalid(format!(
                "dataset {dataset_id}: trade stream contains foreign tasks"
            ));
        }

        // Preserve deterministic task ordering.
        tasks.sort_by_key(|task| (task.start_timestamp_ns, task.task_id));

        for pair in tasks.windows(2) {
            if pair[0].task_id == pair[1].task_id {
                return invalid(format!(
                    "dataset {dataset_id}: duplicate trade task {}",
                    pair[0].task_id
                ));
            }

            if pair[1].start_timestamp_ns < pair[0].end_timestamp_ns {
                return invalid(format!(
                    "dataset {dataset_id}: overlapping trade tasks {} and {}",
                    pair[0].task_id, pair[1].task_id
                ));
            }
        }

        Ok(Self {
            dataset_id,
            tasks,

            current_task: 0,
            current_file: 0,
            current_cursor: None,

            range,

            previous_timestamp_ns: None,
            events_emitted: 0,
        })
    }

    // -------------------------------------------------------------------------
    // Metrics
    // -------------------------------------------------------------------------

    pub fn dataset_id(&self) -> u64 {
        self.dataset_id
    }

    pub fn events_emitted(&self) -> u64 {
        self.events_emitted
    }

    // -------------------------------------------------------------------------
    // Read next trade event
    // -------------------------------------------------------------------------

    pub fn next_event(&mut self) -> Result<Option<TradeEventRef>> {
        loop {
            if self.current_task >= self.tasks.len() {
                return Ok(None);
            }

            let task = &self.tasks[self.current_task];

            if self.current_file >= task.files.len() {
                self.current_task += 1;
                self.current_file = 0;
                self.current_cursor = None;
                continue;
            }

            let file = &task.files[self.current_file];

            // -------------------------------------------------------------
            // Skip files outside the requested interval
            // -------------------------------------------------------------

            if let Some((start_ns, end_ns)) = self.range {
                if file.end_timestamp_ns < start_ns || file.start_timestamp_ns >= end_ns {
                    self.current_file += 1;
                    self.current_cursor = None;
                    continue;
                }
            }

            // -------------------------------------------------------------
            // Open current Parquet file
            // -------------------------------------------------------------

            if self.current_cursor.is_none() {
                let cursor = match self.range {
                    Some((start_ns, end_ns)) => {
                        TradeCursor::open_indexed_range(&file.path, start_ns, end_ns)?
                    }

                    None => TradeCursor::open(&file.path)?,
                };

                self.current_cursor = Some(cursor);
            }

            // -------------------------------------------------------------
            // Read next canonical trade
            // -------------------------------------------------------------

            let trade = self
                .current_cursor
                .as_mut()
                .expect("trade cursor initialized")
                .next_trade()?;

            let Some(trade) = trade else {
                self.current_file += 1;
                self.current_cursor = None;
                continue;
            };

            // -------------------------------------------------------------
            // Resolve task-global canonical row offset
            // -------------------------------------------------------------

            let canonical_row_offset = file
                .row_offset
                .checked_add(trade.file_row_index)
                .ok_or_else(|| invalid_error("trade canonical row offset overflow"))?;

            if canonical_row_offset >= task.canonical_rows {
                return invalid(format!(
                    "dataset {} task {}: canonical row offset exceeds task",
                    self.dataset_id, task.task_id
                ));
            }

            // -------------------------------------------------------------
            // Validate chronological ordering
            // -------------------------------------------------------------

            if let Some(previous) = self.previous_timestamp_ns {
                if trade.event_timestamp_ns < previous {
                    return invalid(format!(
                        "dataset {}: trade timestamp regression: \
                         previous={}, current={}",
                        self.dataset_id, previous, trade.event_timestamp_ns
                    ));
                }
            }

            self.previous_timestamp_ns = Some(trade.event_timestamp_ns);

            self.events_emitted = self
                .events_emitted
                .checked_add(1)
                .ok_or_else(|| invalid_error("trade event counter overflow"))?;

            return Ok(Some(TradeEventRef {
                dataset_id: self.dataset_id,
                task_id: task.task_id,

                event_ordinal: canonical_row_offset,
                canonical_row_offset,

                event_timestamp_ns: trade.event_timestamp_ns,
            }));
        }
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
