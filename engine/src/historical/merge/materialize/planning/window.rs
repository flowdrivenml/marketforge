use crate::shared::error::{MarketForgeError, Result};

use crate::historical::merge::output::writers::unified_timeline::UnifiedTimelineRecord;

// -----------------------------------------------------------------------------
// Window limits
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaterializationBatchLimits {
    pub max_events: usize,
    pub max_canonical_rows: u64,
}

impl MaterializationBatchLimits {
    pub fn new(max_events: usize, max_canonical_rows: u64) -> Result<Self> {
        if max_events == 0 {
            return invalid("materialization max_events must be positive");
        }

        if max_canonical_rows == 0 {
            return invalid("materialization max_canonical_rows must be positive");
        }

        Ok(Self {
            max_events,
            max_canonical_rows,
        })
    }
}

// -----------------------------------------------------------------------------
// Materialization window
// -----------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct MaterializationWindow {
    pub references: Vec<UnifiedTimelineRecord>,
    pub canonical_rows: u64,
}

impl MaterializationWindow {
    pub fn event_count(&self) -> usize {
        self.references.len()
    }

    pub fn is_empty(&self) -> bool {
        self.references.is_empty()
    }
}

// -----------------------------------------------------------------------------
// Window builder
// -----------------------------------------------------------------------------

pub struct MaterializationWindowBuilder {
    limits: MaterializationBatchLimits,
    references: Vec<UnifiedTimelineRecord>,
    canonical_rows: u64,
}

impl MaterializationWindowBuilder {
    pub fn new(limits: MaterializationBatchLimits) -> Self {
        Self {
            limits,
            references: Vec::with_capacity(limits.max_events),
            canonical_rows: 0,
        }
    }

    /// Push one complete logical event.
    ///
    /// Returns a completed window when adding the event would exceed
    /// either configured limit.
    ///
    /// The incoming event is retained in the next window.
    pub fn push(
        &mut self,
        reference: UnifiedTimelineRecord,
    ) -> Result<Option<MaterializationWindow>> {
        let next_rows = self
            .canonical_rows
            .checked_add(reference.canonical_row_count)
            .ok_or_else(|| invalid_error("materialization row-count overflow"))?;

        let exceeds_events = self.references.len() >= self.limits.max_events;

        let exceeds_rows = next_rows > self.limits.max_canonical_rows;

        if !self.references.is_empty() && (exceeds_events || exceeds_rows) {
            let completed = self.take_window();

            self.references.push(reference);
            self.canonical_rows = reference.canonical_row_count;

            return Ok(Some(completed));
        }

        self.references.push(reference);
        self.canonical_rows = next_rows;

        Ok(None)
    }

    /// Finish the remaining window.
    pub fn finish(&mut self) -> Option<MaterializationWindow> {
        if self.references.is_empty() {
            return None;
        }

        Some(self.take_window())
    }

    fn take_window(&mut self) -> MaterializationWindow {
        MaterializationWindow {
            references: std::mem::take(&mut self.references),
            canonical_rows: std::mem::replace(&mut self.canonical_rows, 0),
        }
    }
}

fn invalid_error(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(message.into())
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(invalid_error(message))
}
