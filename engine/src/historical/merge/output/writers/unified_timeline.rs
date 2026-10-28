use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, UInt8Array, UInt32Array, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::{
    shared::error::{MarketForgeError, Result},
    shared::formats::depth::{DepthEventBoundary, DepthSourceOperation},
};

use {
    crate::historical::merge::core::event::MergeEventKind,
    crate::historical::merge::core::event::MergeEventRef,
    crate::historical::merge::core::event::MergedEvent,
};

// -----------------------------------------------------------------------------
// Unified timeline schema
// -----------------------------------------------------------------------------

pub const UNIFIED_TIMELINE_VERSION: u32 = 1;

pub fn unified_timeline_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("event_timestamp_ns", DataType::Int64, false),
        Field::new("stream_rank", DataType::UInt32, false),
        Field::new("event_kind", DataType::UInt8, false),
        Field::new("dataset_id", DataType::UInt64, false),
        Field::new("task_id", DataType::UInt64, false),
        Field::new("event_ordinal", DataType::UInt64, false),
        Field::new("canonical_row_offset", DataType::UInt64, false),
        Field::new("canonical_row_count", DataType::UInt64, false),
        Field::new("event_boundary", DataType::UInt8, true),
        Field::new("source_operation", DataType::UInt8, true),
    ]))
}

// -----------------------------------------------------------------------------
// Unified timeline record
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnifiedTimelineRecord {
    pub event_timestamp_ns: i64,
    pub stream_rank: u32,
    pub event_kind: u8,

    pub dataset_id: u64,
    pub task_id: u64,
    pub event_ordinal: u64,

    pub canonical_row_offset: u64,
    pub canonical_row_count: u64,

    pub event_boundary: Option<u8>,
    pub source_operation: Option<u8>,
}

impl From<&MergedEvent> for UnifiedTimelineRecord {
    fn from(value: &MergedEvent) -> Self {
        let (event_boundary, source_operation) = match &value.event {
            MergeEventRef::Trade(_) => (None, None),

            MergeEventRef::Depth(depth) => {
                let boundary = match depth.event.event_boundary {
                    DepthEventBoundary::Changes => 0,
                    DepthEventBoundary::Initialization => 1,
                };

                let operation = match depth.event.source_operation {
                    DepthSourceOperation::Snapshot => 0,
                    DepthSourceOperation::AbsoluteUpdate => 1,
                    DepthSourceOperation::RelativeUpdate => 2,
                };

                (Some(boundary), Some(operation))
            }
        };

        Self {
            event_timestamp_ns: value.event.timestamp_ns(),
            stream_rank: value.stream_rank,
            event_kind: match value.event.kind() {
                MergeEventKind::Trade => 0,
                MergeEventKind::Depth => 1,
            },

            dataset_id: value.event.dataset_id(),
            task_id: value.event.task_id(),
            event_ordinal: value.event.event_ordinal(),

            canonical_row_offset: value.event.canonical_row_offset(),
            canonical_row_count: value.event.canonical_row_count(),

            event_boundary,
            source_operation,
        }
    }
}

// -----------------------------------------------------------------------------
// Arrow batch builder
// -----------------------------------------------------------------------------

#[derive(Debug, Default)]
pub struct UnifiedTimelineBatchBuilder {
    records: Vec<UnifiedTimelineRecord>,
}

impl UnifiedTimelineBatchBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            records: Vec::with_capacity(capacity),
        }
    }

    pub fn push(&mut self, record: UnifiedTimelineRecord) {
        self.records.push(record);
    }

    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    pub fn clear(&mut self) {
        self.records.clear();
    }

    pub fn finish(&mut self) -> Result<Option<RecordBatch>> {
        if self.records.is_empty() {
            return Ok(None);
        }

        let batch = self.build_batch()?;
        self.records.clear();

        Ok(Some(batch))
    }

    fn build_batch(&self) -> Result<RecordBatch> {
        let records = &self.records;

        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(
                records
                    .iter()
                    .map(|record| record.event_timestamp_ns)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt32Array::from(
                records
                    .iter()
                    .map(|record| record.stream_rank)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt8Array::from(
                records
                    .iter()
                    .map(|record| record.event_kind)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                records
                    .iter()
                    .map(|record| record.dataset_id)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                records
                    .iter()
                    .map(|record| record.task_id)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                records
                    .iter()
                    .map(|record| record.event_ordinal)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                records
                    .iter()
                    .map(|record| record.canonical_row_offset)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                records
                    .iter()
                    .map(|record| record.canonical_row_count)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt8Array::from(
                records
                    .iter()
                    .map(|record| record.event_boundary)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt8Array::from(
                records
                    .iter()
                    .map(|record| record.source_operation)
                    .collect::<Vec<_>>(),
            )),
        ];

        RecordBatch::try_new(unified_timeline_schema(), columns).map_err(|error| {
            MarketForgeError::InvalidCanonical(format!(
                "failed to construct unified timeline RecordBatch: {error}"
            ))
        })
    }
}
