use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, Int64Array, ListArray, RecordBatch, StringArray, StructArray, UInt8Array,
    UInt32Array, UInt64Array, new_empty_array,
};
use arrow_buffer::OffsetBuffer;
use arrow_schema::DataType;
use arrow_select::concat::concat;

use crate::{
    error::{MarketForgeError, Result},
    process::parquet::depth_schema,
};

use super::{
    materialize_depth_metadata::ResolvedDepthMetadata, materialize_resolver::ResolvedMarketEvent,
    materialize_schema::physical_depth_schema, unified_manifest::UnifiedMergeStream,
};

// -----------------------------------------------------------------------------
// Materialization input
// -----------------------------------------------------------------------------

pub struct PhysicalDepthEvent<'a> {
    pub event: &'a ResolvedMarketEvent,
    pub metadata: ResolvedDepthMetadata,
    pub stream: &'a UnifiedMergeStream,
}

// -----------------------------------------------------------------------------
// Physical depth builder
// -----------------------------------------------------------------------------

#[derive(Debug, Default)]
pub struct PhysicalDepthBatchBuilder;

impl PhysicalDepthBatchBuilder {
    pub fn new() -> Self {
        Self
    }

    pub fn build(&self, events: &[PhysicalDepthEvent<'_>]) -> Result<Option<RecordBatch>> {
        if events.is_empty() {
            return Ok(None);
        }

        let canonical_schema = depth_schema();
        let physical_schema = physical_depth_schema();

        let mut offsets = Vec::with_capacity(events.len() + 1);
        offsets.push(0_i32);

        let mut total_levels = 0_u64;

        // ---------------------------------------------------------------------
        // Validate canonical events and construct list offsets
        // ---------------------------------------------------------------------

        for input in events {
            let reference = &input.event.reference;
            let stream = input.stream;

            if reference.event_kind != 1 {
                return invalid("physical depth builder received non-depth event");
            }

            if reference.dataset_id != stream.dataset_id
                || reference.stream_rank != stream.stream_rank
            {
                return invalid("depth stream identity mismatch");
            }

            if reference.event_boundary.is_none() || reference.source_operation.is_none() {
                return invalid("missing depth event metadata");
            }

            if reference.event_boundary.unwrap() > 1 || reference.source_operation.unwrap() > 2 {
                return invalid("invalid depth boundary or source operation");
            }

            let resolved_rows = u64::try_from(input.event.canonical_rows())
                .map_err(|_| invalid_error("canonical row-count overflow"))?;

            if resolved_rows != reference.canonical_row_count {
                return invalid(format!(
                    "depth canonical row mismatch: expected={}, actual={}",
                    reference.canonical_row_count, resolved_rows
                ));
            }

            for batch in &input.event.canonical_batches {
                if batch.schema().as_ref() != canonical_schema.as_ref() {
                    return invalid("canonical depth schema mismatch");
                }

                validate_canonical_identity(batch, input)?;
            }

            total_levels = total_levels
                .checked_add(resolved_rows)
                .ok_or_else(|| invalid_error("depth level-count overflow"))?;

            let offset = i32::try_from(total_levels)
                .map_err(|_| invalid_error("Arrow List offset exceeds i32 range"))?;

            offsets.push(offset);
        }

        // ---------------------------------------------------------------------
        // Build nested canonical level columns
        // ---------------------------------------------------------------------

        let level_fields = match physical_schema
            .field_with_name("changes")
            .map_err(|error| invalid_error(error.to_string()))?
            .data_type()
        {
            DataType::List(item) => match item.data_type() {
                DataType::Struct(fields) => fields.clone(),
                _ => return invalid("depth changes must contain structs"),
            },
            _ => return invalid("depth changes must be an Arrow List"),
        };

        let mut level_columns = Vec::<ArrayRef>::with_capacity(6);

        for column_index in 6..canonical_schema.fields().len() {
            let arrays: Vec<&dyn Array> = events
                .iter()
                .flat_map(|input| input.event.canonical_batches.iter())
                .filter(|batch| batch.num_rows() > 0)
                .map(|batch| batch.column(column_index).as_ref())
                .collect();

            let column = if arrays.is_empty() {
                new_empty_array(canonical_schema.field(column_index).data_type())
            } else {
                concat(&arrays).map_err(|error| {
                    invalid_error(format!(
                        "failed to concatenate depth column {}: {error}",
                        canonical_schema.field(column_index).name()
                    ))
                })?
            };

            level_columns.push(column);
        }

        let levels = StructArray::try_new(level_fields, level_columns, None).map_err(|error| {
            invalid_error(format!("failed to construct depth level struct: {error}"))
        })?;

        let changes_field = physical_schema
            .field_with_name("changes")
            .map_err(|error| invalid_error(error.to_string()))?;

        let DataType::List(item_field) = changes_field.data_type() else {
            return invalid("invalid depth changes schema");
        };

        let changes = ListArray::try_new(
            item_field.clone(),
            OffsetBuffer::new(offsets.into()),
            Arc::new(levels),
            None,
        )
        .map_err(|error| {
            invalid_error(format!("failed to construct nested depth changes: {error}"))
        })?;

        // ---------------------------------------------------------------------
        // Common event fields
        // ---------------------------------------------------------------------

        let timestamps = Int64Array::from(
            events
                .iter()
                .map(|e| e.event.reference.event_timestamp_ns)
                .collect::<Vec<_>>(),
        );

        let system_timestamps = Int64Array::from(
            events
                .iter()
                .map(|e| e.metadata.system_timestamp_ns)
                .collect::<Vec<_>>(),
        );

        let exchanges = StringArray::from(
            events
                .iter()
                .map(|e| e.stream.exchange.as_str())
                .collect::<Vec<_>>(),
        );

        let instrument_ids = Int64Array::from(
            events
                .iter()
                .map(|e| e.stream.instrument_id)
                .collect::<Vec<_>>(),
        );

        let symbols = StringArray::from(
            events
                .iter()
                .map(|e| e.stream.symbol.as_str())
                .collect::<Vec<_>>(),
        );

        let stream_ids = StringArray::from(
            events
                .iter()
                .map(|e| e.stream.stream_id.as_str())
                .collect::<Vec<_>>(),
        );

        // ---------------------------------------------------------------------
        // Merge provenance
        // ---------------------------------------------------------------------

        let stream_ranks = UInt32Array::from(
            events
                .iter()
                .map(|e| e.event.reference.stream_rank)
                .collect::<Vec<_>>(),
        );

        let dataset_ids = UInt64Array::from(
            events
                .iter()
                .map(|e| e.event.reference.dataset_id)
                .collect::<Vec<_>>(),
        );

        let task_ids = UInt64Array::from(
            events
                .iter()
                .map(|e| e.event.reference.task_id)
                .collect::<Vec<_>>(),
        );

        let event_ordinals = UInt64Array::from(
            events
                .iter()
                .map(|e| e.event.reference.event_ordinal)
                .collect::<Vec<_>>(),
        );

        let row_offsets = UInt64Array::from(
            events
                .iter()
                .map(|e| e.event.reference.canonical_row_offset)
                .collect::<Vec<_>>(),
        );

        let row_counts = UInt64Array::from(
            events
                .iter()
                .map(|e| e.event.reference.canonical_row_count)
                .collect::<Vec<_>>(),
        );

        // ---------------------------------------------------------------------
        // Depth event metadata
        // ---------------------------------------------------------------------

        let boundaries = UInt8Array::from(
            events
                .iter()
                .map(|e| e.event.reference.event_boundary.unwrap())
                .collect::<Vec<_>>(),
        );

        let operations = UInt8Array::from(
            events
                .iter()
                .map(|e| e.event.reference.source_operation.unwrap())
                .collect::<Vec<_>>(),
        );

        let sequence_starts = UInt64Array::from(
            events
                .iter()
                .map(|e| e.metadata.sequence_start)
                .collect::<Vec<_>>(),
        );

        let sequence_ends = UInt64Array::from(
            events
                .iter()
                .map(|e| e.metadata.sequence_end)
                .collect::<Vec<_>>(),
        );

        // ---------------------------------------------------------------------
        // Construct physical RecordBatch
        // ---------------------------------------------------------------------

        let columns: Vec<ArrayRef> = vec![
            Arc::new(timestamps),
            Arc::new(system_timestamps),
            Arc::new(exchanges),
            Arc::new(instrument_ids),
            Arc::new(symbols),
            Arc::new(stream_ids),
            Arc::new(stream_ranks),
            Arc::new(dataset_ids),
            Arc::new(task_ids),
            Arc::new(event_ordinals),
            Arc::new(row_offsets),
            Arc::new(row_counts),
            Arc::new(boundaries),
            Arc::new(operations),
            Arc::new(sequence_starts),
            Arc::new(sequence_ends),
            Arc::new(changes),
        ];

        let batch = RecordBatch::try_new(physical_schema, columns).map_err(|error| {
            invalid_error(format!("failed to construct physical depth batch: {error}"))
        })?;

        if batch.num_rows() != events.len() {
            return invalid("physical depth event-count mismatch");
        }

        Ok(Some(batch))
    }
}

// -----------------------------------------------------------------------------
// Canonical identity validation
// -----------------------------------------------------------------------------

fn validate_canonical_identity(batch: &RecordBatch, input: &PhysicalDepthEvent<'_>) -> Result<()> {
    let reference = &input.event.reference;
    let stream = input.stream;

    let timestamps = batch
        .column_by_name("event_timestamp_ns")
        .and_then(|array| array.as_any().downcast_ref::<Int64Array>())
        .ok_or_else(|| invalid_error("missing canonical event timestamp"))?;

    let exchanges = batch
        .column_by_name("exchange")
        .and_then(|array| array.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| invalid_error("missing canonical exchange"))?;

    let instruments = batch
        .column_by_name("instrument_id")
        .and_then(|array| array.as_any().downcast_ref::<Int64Array>())
        .ok_or_else(|| invalid_error("missing canonical instrument ID"))?;

    let symbols = batch
        .column_by_name("symbol")
        .and_then(|array| array.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| invalid_error("missing canonical symbol"))?;

    let streams = batch
        .column_by_name("stream_id")
        .and_then(|array| array.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| invalid_error("missing canonical stream ID"))?;

    for row in 0..batch.num_rows() {
        if timestamps.is_null(row)
            || timestamps.value(row) != reference.event_timestamp_ns
            || exchanges.is_null(row)
            || exchanges.value(row) != stream.exchange
            || instruments.is_null(row)
            || instruments.value(row) != stream.instrument_id
            || symbols.is_null(row)
            || symbols.value(row) != stream.symbol
            || streams.is_null(row)
            || streams.value(row) != stream.stream_id
        {
            return invalid(format!(
                "canonical depth identity mismatch: dataset={}, task={}, ordinal={}",
                reference.dataset_id, reference.task_id, reference.event_ordinal
            ));
        }
    }

    Ok(())
}

fn invalid_error(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(message.into())
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(invalid_error(message))
}
