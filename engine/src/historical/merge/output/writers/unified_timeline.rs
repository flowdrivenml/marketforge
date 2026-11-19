use std::sync::Arc;

use arrow_array::{
    ArrayRef, BooleanArray, Int64Array, RecordBatch, UInt8Array, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::{
    shared::error::{MarketForgeError, Result},
    shared::formats::depth::DepthEventBoundary,
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
    pub causal: Option<CausalPosition>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CausalPosition {
    pub sequence: u64,
    pub reconstruction_only: bool,
    pub system_timestamp_ns: Option<i64>,
    pub sequence_start: Option<u64>,
    pub sequence_end: Option<u64>,
}

pub(crate) fn causal_schema(base: &SchemaRef, logical: bool) -> SchemaRef {
    let mut fields = base.fields().to_vec();
    fields.extend([
        Arc::new(Field::new("timeline_sequence", DataType::UInt64, false)),
        Arc::new(Field::new("reconstruction_only", DataType::Boolean, false)),
    ]);
    if logical {
        fields.extend([
            Arc::new(Field::new(
                "indexed_system_timestamp_ns",
                DataType::Int64,
                true,
            )),
            Arc::new(Field::new("indexed_sequence_start", DataType::UInt64, true)),
            Arc::new(Field::new("indexed_sequence_end", DataType::UInt64, true)),
        ]);
    }
    Arc::new(Schema::new(fields))
}

pub(crate) fn add_causal_columns(
    batch: RecordBatch,
    records: &[UnifiedTimelineRecord],
    logical: bool,
) -> Result<RecordBatch> {
    let positions = records
        .iter()
        .map(|record| {
            record.causal.ok_or_else(|| {
                MarketForgeError::InvalidCanonical("causal timeline row has no position".into())
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(UInt64Array::from_iter_values(
        positions.iter().map(|position| position.sequence),
    )));
    columns.push(Arc::new(BooleanArray::from_iter(
        positions
            .iter()
            .map(|position| Some(position.reconstruction_only)),
    )));
    if logical {
        columns.push(Arc::new(Int64Array::from_iter(
            positions
                .iter()
                .map(|position| position.system_timestamp_ns),
        )));
        columns.push(Arc::new(UInt64Array::from_iter(
            positions.iter().map(|position| position.sequence_start),
        )));
        columns.push(Arc::new(UInt64Array::from_iter(
            positions.iter().map(|position| position.sequence_end),
        )));
    }
    RecordBatch::try_new(causal_schema(&batch.schema(), logical), columns)
        .map_err(|error| MarketForgeError::InvalidCanonical(error.to_string()))
}

pub(crate) fn read_causal_position(
    batch: &RecordBatch,
    row: usize,
    logical: bool,
) -> Result<CausalPosition> {
    use arrow_array::Array;
    fn column<'a, T: Array + 'static>(batch: &'a RecordBatch, name: &str) -> Result<&'a T> {
        batch
            .column_by_name(name)
            .and_then(|column| column.as_any().downcast_ref::<T>())
            .ok_or_else(|| {
                MarketForgeError::InvalidCanonical(format!(
                    "missing or invalid causal timeline column {name}"
                ))
            })
    }
    let sequences = column::<UInt64Array>(batch, "timeline_sequence")?;
    let reconstruction = column::<BooleanArray>(batch, "reconstruction_only")?;
    if sequences.is_null(row) || reconstruction.is_null(row) {
        return Err(MarketForgeError::InvalidCanonical(
            "null causal sequence or reconstruction marker".into(),
        ));
    }
    let mut position = CausalPosition {
        sequence: sequences.value(row),
        reconstruction_only: reconstruction.value(row),
        system_timestamp_ns: None,
        sequence_start: None,
        sequence_end: None,
    };
    if logical {
        let system = column::<Int64Array>(batch, "indexed_system_timestamp_ns")?;
        let start = column::<UInt64Array>(batch, "indexed_sequence_start")?;
        let end = column::<UInt64Array>(batch, "indexed_sequence_end")?;
        position.system_timestamp_ns = (!system.is_null(row)).then(|| system.value(row));
        position.sequence_start = (!start.is_null(row)).then(|| start.value(row));
        position.sequence_end = (!end.is_null(row)).then(|| end.value(row));
    }
    Ok(position)
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

                let operation = depth.event.source_operation.code();

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
            causal: None,
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

        let batch = RecordBatch::try_new(unified_timeline_schema(), columns).map_err(|error| {
            MarketForgeError::InvalidCanonical(format!(
                "failed to construct unified timeline RecordBatch: {error}"
            ))
        })?;
        if records
            .first()
            .is_some_and(|record| record.causal.is_some())
        {
            add_causal_columns(batch, records, true)
        } else {
            Ok(batch)
        }
    }
}
