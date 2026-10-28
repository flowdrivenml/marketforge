use std::{collections::HashMap, path::Path};

use {
    crate::shared::error::MarketForgeError, crate::shared::error::Result,
    crate::shared::formats::depth::DepthEventBoundary,
    crate::shared::formats::depth::DepthSourceOperation,
    crate::shared::persistence::parquet::IndexedDepthEvent,
};

use {
    crate::historical::merge::depth::cursor::DepthEventCursor,
    crate::historical::merge::output::writers::unified_timeline::UnifiedTimelineRecord,
};

// -----------------------------------------------------------------------------
// Resolved depth metadata
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedDepthMetadata {
    pub system_timestamp_ns: Option<i64>,
    pub sequence_start: Option<u64>,
    pub sequence_end: Option<u64>,
}

// -----------------------------------------------------------------------------
// Task-local metadata cursor
// -----------------------------------------------------------------------------

struct DepthMetadataCursor {
    cursor: DepthEventCursor,
    last_requested_ordinal: Option<u64>,
    pending: Option<IndexedDepthEvent>,
}

impl DepthMetadataCursor {
    fn open(path: &Path) -> Result<Self> {
        Ok(Self {
            cursor: DepthEventCursor::open(path)?,
            last_requested_ordinal: None,
            pending: None,
        })
    }

    fn resolve(&mut self, reference: &UnifiedTimelineRecord) -> Result<ResolvedDepthMetadata> {
        if let Some(previous) = self.last_requested_ordinal {
            if reference.event_ordinal <= previous {
                return invalid(format!(
                    "depth metadata ordinal is not increasing: \
                     previous={}, requested={}",
                    previous, reference.event_ordinal
                ));
            }
        }

        loop {
            let event = match self.pending.take() {
                Some(event) => event,
                None => self.cursor.next_event()?.ok_or_else(|| {
                    invalid_error(format!(
                        "depth event ordinal {} not found",
                        reference.event_ordinal
                    ))
                })?,
            };

            if event.event_ordinal < reference.event_ordinal {
                continue;
            }

            if event.event_ordinal > reference.event_ordinal {
                self.pending = Some(event);

                return invalid(format!(
                    "depth event ordinal {} missing from source index",
                    reference.event_ordinal
                ));
            }

            validate_event(reference, &event)?;

            self.last_requested_ordinal = Some(reference.event_ordinal);

            return Ok(ResolvedDepthMetadata {
                system_timestamp_ns: event.system_timestamp_ns,
                sequence_start: event.sequence_start,
                sequence_end: event.sequence_end,
            });
        }
    }
    fn open_range(path: &Path, start_ns: i64, end_ns: i64) -> Result<Self> {
        if start_ns >= end_ns {
            return invalid(format!(
                "invalid depth metadata range: [{start_ns}, {end_ns})"
            ));
        }

        Ok(Self {
            cursor: DepthEventCursor::open_indexed_range(path, start_ns, end_ns)?,
            last_requested_ordinal: None,
            pending: None,
        })
    }
}

// -----------------------------------------------------------------------------
// Worker-local metadata resolver
// -----------------------------------------------------------------------------

pub struct DepthMetadataResolver {
    tasks: HashMap<(u64, u64), DepthMetadataCursor>,
}

impl Default for DepthMetadataResolver {
    fn default() -> Self {
        Self::new()
    }
}

impl DepthMetadataResolver {
    pub fn new() -> Self {
        Self {
            tasks: HashMap::new(),
        }
    }

    /// Register a canonical depth event index.
    ///
    /// One cursor is maintained per (dataset_id, task_id).
    pub fn register_task(
        &mut self,
        dataset_id: u64,
        task_id: u64,
        events_path: &Path,
    ) -> Result<()> {
        let key = (dataset_id, task_id);

        if self.tasks.contains_key(&key) {
            return invalid(format!(
                "duplicate depth metadata task: dataset={}, task={}",
                dataset_id, task_id
            ));
        }

        let cursor = DepthMetadataCursor::open(events_path)?;

        self.tasks.insert(key, cursor);

        Ok(())
    }
    /// Register a depth task for a specific chronological partition.
    ///
    /// Uses indexed Parquet reading when available.
    /// Source event ordinals may contain gaps.
    pub fn register_task_range(
        &mut self,
        dataset_id: u64,
        task_id: u64,
        events_path: &Path,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<()> {
        let key = (dataset_id, task_id);

        if self.tasks.contains_key(&key) {
            return invalid(format!(
                "duplicate depth metadata task: dataset={}, task={}",
                dataset_id, task_id
            ));
        }

        let cursor = DepthMetadataCursor::open_range(events_path, start_ns, end_ns)?;

        self.tasks.insert(key, cursor);

        Ok(())
    }

    /// Resolve metadata for one retained logical depth event.
    pub fn resolve(&mut self, reference: &UnifiedTimelineRecord) -> Result<ResolvedDepthMetadata> {
        if reference.event_kind != 1 {
            return invalid("depth metadata resolver received non-depth event");
        }

        let key = (reference.dataset_id, reference.task_id);

        let task = self.tasks.get_mut(&key).ok_or_else(|| {
            invalid_error(format!(
                "unregistered depth metadata task: \
                 dataset={}, task={}",
                reference.dataset_id, reference.task_id
            ))
        })?;

        task.resolve(reference)
    }

    /// Resolve a chronological batch.
    ///
    /// References may alternate between tasks, but ordinals
    /// must increase within each individual task.
    pub fn resolve_batch(
        &mut self,
        references: &[UnifiedTimelineRecord],
    ) -> Result<Vec<ResolvedDepthMetadata>> {
        let mut output = Vec::with_capacity(references.len());

        for reference in references {
            output.push(self.resolve(reference)?);
        }

        Ok(output)
    }

    pub fn registered_tasks(&self) -> usize {
        self.tasks.len()
    }
}

// -----------------------------------------------------------------------------
// Source event validation
// -----------------------------------------------------------------------------

fn validate_event(reference: &UnifiedTimelineRecord, event: &IndexedDepthEvent) -> Result<()> {
    if event.event_ordinal != reference.event_ordinal {
        return invalid("depth event ordinal mismatch");
    }

    if event.event_timestamp_ns != reference.event_timestamp_ns {
        return invalid(format!(
            "depth event timestamp mismatch: ordinal={}",
            reference.event_ordinal
        ));
    }

    if event.canonical_row_offset != reference.canonical_row_offset {
        return invalid(format!(
            "depth canonical row offset mismatch: ordinal={}",
            reference.event_ordinal
        ));
    }

    if event.canonical_row_count != reference.canonical_row_count {
        return invalid(format!(
            "depth canonical row count mismatch: ordinal={}",
            reference.event_ordinal
        ));
    }

    let boundary = match event.event_boundary {
        DepthEventBoundary::Changes => 0,
        DepthEventBoundary::Initialization => 1,
    };

    if reference.event_boundary != Some(boundary) {
        return invalid(format!(
            "depth event boundary mismatch: ordinal={}",
            reference.event_ordinal
        ));
    }

    let operation = match event.source_operation {
        DepthSourceOperation::Snapshot => 0,
        DepthSourceOperation::AbsoluteUpdate => 1,
        DepthSourceOperation::RelativeUpdate => 2,
    };

    if reference.source_operation != Some(operation) {
        return invalid(format!(
            "depth source operation mismatch: ordinal={}",
            reference.event_ordinal
        ));
    }

    Ok(())
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
