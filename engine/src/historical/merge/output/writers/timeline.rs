use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, UInt8Array, UInt32Array, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::{
    error::{MarketForgeError, Result},
    formats::depth::{DepthEventBoundary, DepthSourceOperation},
};

use super::kway::MergedDepthEvent;

// -----------------------------------------------------------------------------
// Timeline schema
// -----------------------------------------------------------------------------

pub fn timeline_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("event_timestamp_ns", DataType::Int64, false),
        Field::new("stream_rank", DataType::UInt32, false),
        Field::new("dataset_id", DataType::UInt64, false),
        Field::new("task_id", DataType::UInt64, false),
        Field::new("event_ordinal", DataType::UInt64, false),
        Field::new("canonical_row_offset", DataType::UInt64, false),
        Field::new("canonical_row_count", DataType::UInt64, false),
        Field::new("event_boundary", DataType::UInt8, false),
        Field::new("source_operation", DataType::UInt8, false),
    ]))
}

// -----------------------------------------------------------------------------
// Timeline record
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
pub struct TimelineRecord {
    pub event_timestamp_ns: i64,
    pub stream_rank: u32,

    pub dataset_id: u64,
    pub task_id: u64,
    pub event_ordinal: u64,

    pub canonical_row_offset: u64,
    pub canonical_row_count: u64,

    pub event_boundary: u8,
    pub source_operation: u8,
}

impl From<&MergedDepthEvent> for TimelineRecord {
    fn from(value: &MergedDepthEvent) -> Self {
        let event = &value.event.event;

        let event_boundary = match event.event_boundary {
            DepthEventBoundary::Changes => 0,
            DepthEventBoundary::Initialization => 1,
        };

        let source_operation = match event.source_operation {
            DepthSourceOperation::Snapshot => 0,
            DepthSourceOperation::AbsoluteUpdate => 1,
            DepthSourceOperation::RelativeUpdate => 2,
        };

        Self {
            event_timestamp_ns: event.event_timestamp_ns,
            stream_rank: value.stream_rank,

            dataset_id: value.event.dataset_id,
            task_id: value.event.task_id,
            event_ordinal: event.event_ordinal,

            canonical_row_offset: event.canonical_row_offset,
            canonical_row_count: event.canonical_row_count,

            event_boundary,
            source_operation,
        }
    }
}

// -----------------------------------------------------------------------------
// Timeline batch builder
// -----------------------------------------------------------------------------

#[derive(Debug, Default)]
pub struct TimelineBatchBuilder {
    records: Vec<TimelineRecord>,
}

impl TimelineBatchBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            records: Vec::with_capacity(capacity),
        }
    }

    pub fn push(&mut self, record: TimelineRecord) {
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

        // Clear only after successful conversion.
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

        RecordBatch::try_new(timeline_schema(), columns).map_err(|error| {
            MarketForgeError::InvalidCanonical(format!(
                "failed to construct merged timeline RecordBatch: {error}"
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timeline_schema_contains_expected_fields() {
        let schema = timeline_schema();

        let expected = [
            "event_timestamp_ns",
            "stream_rank",
            "dataset_id",
            "task_id",
            "event_ordinal",
            "canonical_row_offset",
            "canonical_row_count",
            "event_boundary",
            "source_operation",
        ];

        assert_eq!(schema.fields().len(), expected.len());

        for (field, name) in schema.fields().iter().zip(expected) {
            assert_eq!(field.name(), name);
        }
    }

    #[test]
    fn timeline_batch_preserves_event_references() {
        let mut builder = TimelineBatchBuilder::new();

        builder.push(TimelineRecord {
            event_timestamp_ns: 100,
            stream_rank: 2,
            dataset_id: 112,
            task_id: 5,
            event_ordinal: 42,
            canonical_row_offset: 1000,
            canonical_row_count: 3,
            event_boundary: 0,
            source_operation: 2,
        });

        let batch = builder.finish().unwrap().unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 9);
        assert!(builder.is_empty());

        let dataset_ids = batch
            .column_by_name("dataset_id")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        assert_eq!(dataset_ids.value(0), 112);

        let offsets = batch
            .column_by_name("canonical_row_offset")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        assert_eq!(offsets.value(0), 1000);
    }

    #[test]
    fn empty_builder_produces_no_batch() {
        let mut builder = TimelineBatchBuilder::new();

        assert!(builder.finish().unwrap().is_none());
    }
}
