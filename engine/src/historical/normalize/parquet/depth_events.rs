use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, UInt8Array, UInt64Array};

use crate::{
    error::{MarketForgeError, Result},
    formats::depth::{DepthEventBoundary, DepthProcessingOutcome, DepthSourceOperation},
};

use super::schema::depth_event_schema;

// -----------------------------------------------------------------------------
// Indexed source event
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexedDepthEvent {
    pub event_ordinal: u64,
    pub canonical_row_offset: u64,
    pub canonical_row_count: u64,

    pub event_timestamp_ns: i64,
    pub system_timestamp_ns: Option<i64>,

    pub sequence_start: Option<u64>,
    pub sequence_end: Option<u64>,

    pub event_boundary: DepthEventBoundary,
    pub source_operation: DepthSourceOperation,
}

impl IndexedDepthEvent {
    pub fn from_outcome(outcome: &DepthProcessingOutcome, row_offset: u64) -> Result<Self> {
        let event_ordinal = outcome.source.source_event_ordinal.ok_or_else(|| {
            MarketForgeError::InvalidConfiguration(
                "depth event index requires source event ordinal".to_owned(),
            )
        })?;

        let canonical_row_count = u64::try_from(outcome.events.len()).map_err(|_| {
            MarketForgeError::InvalidConfiguration("depth event row count overflow".to_owned())
        })?;

        row_offset.checked_add(canonical_row_count).ok_or_else(|| {
            MarketForgeError::InvalidConfiguration("depth event row offset overflow".to_owned())
        })?;

        Ok(Self {
            event_ordinal,
            canonical_row_offset: row_offset,
            canonical_row_count,

            event_timestamp_ns: outcome.source.event_timestamp_ns,
            system_timestamp_ns: outcome.source.system_timestamp_ns,

            sequence_start: outcome.source.sequence_start,
            sequence_end: outcome.source.sequence_end,

            event_boundary: outcome.boundary,
            source_operation: outcome.operation,
        })
    }
}

// -----------------------------------------------------------------------------
// Arrow event-index builder
// -----------------------------------------------------------------------------

#[derive(Debug, Default)]
pub struct DepthEventBatchBuilder {
    events: Vec<IndexedDepthEvent>,
}

impl DepthEventBatchBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            events: Vec::with_capacity(capacity),
        }
    }

    pub fn push(&mut self, event: IndexedDepthEvent) {
        self.events.push(event);
    }

    pub fn len(&self) -> usize {
        self.events.len()
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    pub fn finish(&mut self) -> Result<Option<RecordBatch>> {
        if self.events.is_empty() {
            return Ok(None);
        }

        let batch = self.build_batch()?;

        self.events.clear();

        Ok(Some(batch))
    }

    fn build_batch(&self) -> Result<RecordBatch> {
        let events = &self.events;

        let columns: Vec<ArrayRef> = vec![
            Arc::new(UInt64Array::from(
                events.iter().map(|e| e.event_ordinal).collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                events
                    .iter()
                    .map(|e| e.canonical_row_offset)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                events
                    .iter()
                    .map(|e| e.canonical_row_count)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                events
                    .iter()
                    .map(|e| e.event_timestamp_ns)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                events
                    .iter()
                    .map(|e| e.system_timestamp_ns)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                events.iter().map(|e| e.sequence_start).collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                events.iter().map(|e| e.sequence_end).collect::<Vec<_>>(),
            )),
            Arc::new(UInt8Array::from(
                events
                    .iter()
                    .map(|e| match e.event_boundary {
                        DepthEventBoundary::Changes => 0,
                        DepthEventBoundary::Initialization => 1,
                    })
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt8Array::from(
                events
                    .iter()
                    .map(|e| match e.source_operation {
                        DepthSourceOperation::Snapshot => 0,
                        DepthSourceOperation::AbsoluteUpdate => 1,
                        DepthSourceOperation::RelativeUpdate => 2,
                    })
                    .collect::<Vec<_>>(),
            )),
        ];

        RecordBatch::try_new(depth_event_schema(), columns).map_err(|error| {
            MarketForgeError::InvalidCanonical(format!(
                "failed to construct depth event-index batch: {error}"
            ))
        })
    }
}
