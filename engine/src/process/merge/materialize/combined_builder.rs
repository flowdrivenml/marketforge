use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, RecordBatch, StructArray, UInt8Array, UInt32Array, new_null_array,
};
use arrow_schema::{DataType, Fields};
use arrow_select::{concat::concat, take::take};

use crate::error::{MarketForgeError, Result};

use super::{
    materialize_depth_builder::{PhysicalDepthBatchBuilder, PhysicalDepthEvent},
    materialize_depth_metadata::ResolvedDepthMetadata,
    materialize_resolver::ResolvedMarketEvent,
    materialize_schema::physical_combined_schema,
    materialize_trade_builder::PhysicalTradeBatchBuilder,
    unified_manifest::UnifiedMergeStream,
};

// -----------------------------------------------------------------------------
// Input
// -----------------------------------------------------------------------------

pub struct PhysicalCombinedEvent<'a> {
    pub event: &'a ResolvedMarketEvent,
    pub stream: &'a UnifiedMergeStream,
    pub depth_metadata: Option<ResolvedDepthMetadata>,
}

// -----------------------------------------------------------------------------
// Combined builder
// -----------------------------------------------------------------------------

#[derive(Debug, Default)]
pub struct PhysicalCombinedBatchBuilder;

impl PhysicalCombinedBatchBuilder {
    pub fn new() -> Self {
        Self
    }

    pub fn build(&self, events: &[PhysicalCombinedEvent<'_>]) -> Result<Option<RecordBatch>> {
        if events.is_empty() {
            return Ok(None);
        }

        let schema = physical_combined_schema();

        // ---------------------------------------------------------------------
        // Separate event types while recording original positions
        // ---------------------------------------------------------------------

        let mut trade_events = Vec::new();
        let mut depth_events = Vec::new();

        let mut trade_indices = Vec::with_capacity(events.len());
        let mut depth_indices = Vec::with_capacity(events.len());

        let mut source_indices = Vec::with_capacity(events.len());
        let mut event_kinds = Vec::with_capacity(events.len());

        for input in events {
            let reference = &input.event.reference;

            if reference.dataset_id != input.stream.dataset_id
                || reference.stream_rank != input.stream.stream_rank
            {
                return invalid("combined event stream identity mismatch");
            }

            match reference.event_kind {
                0 => {
                    if input.depth_metadata.is_some() {
                        return invalid("trade event contains depth metadata");
                    }

                    let index = checked_index(trade_events.len())?;

                    trade_indices.push(Some(index));
                    depth_indices.push(None);

                    source_indices.push(index);
                    event_kinds.push(0);

                    trade_events.push(input.event.clone());
                }

                1 => {
                    let metadata = input
                        .depth_metadata
                        .ok_or_else(|| invalid_error("depth event missing metadata"))?;

                    let index = checked_index(depth_events.len())?;

                    trade_indices.push(None);
                    depth_indices.push(Some(index));

                    event_kinds.push(1);

                    depth_events.push(PhysicalDepthEvent {
                        event: input.event,
                        metadata,
                        stream: input.stream,
                    });

                    // Corrected after trade count is known.
                    source_indices.push(index);
                }

                _ => return invalid("unsupported combined event kind"),
            }
        }

        let trade_count = trade_events.len();

        // Common columns are concatenated in trade-then-depth order.
        for (input, index) in events.iter().zip(source_indices.iter_mut()) {
            if input.event.reference.event_kind == 1 {
                let position =
                    usize::try_from(*index).map_err(|_| invalid_error("source index overflow"))?;

                *index = checked_index(
                    trade_count
                        .checked_add(position)
                        .ok_or_else(|| invalid_error("source index overflow"))?,
                )?;
            }
        }

        // ---------------------------------------------------------------------
        // Build specialized physical batches
        // ---------------------------------------------------------------------

        let trade_batch = PhysicalTradeBatchBuilder::new().build(&trade_events)?;

        let depth_batch = PhysicalDepthBatchBuilder::new().build(&depth_events)?;

        // ---------------------------------------------------------------------
        // Shared event columns
        // ---------------------------------------------------------------------

        let common_names = [
            "event_timestamp_ns",
            "system_timestamp_ns",
            "exchange",
            "instrument_id",
            "symbol",
            "stream_id",
            "stream_rank",
            "dataset_id",
            "task_id",
            "event_ordinal",
            "canonical_row_offset",
            "canonical_row_count",
        ];

        let source_indices = UInt32Array::from(source_indices);

        let mut common_columns = Vec::with_capacity(common_names.len());

        for name in common_names {
            let mut sources: Vec<&dyn Array> = Vec::new();

            if let Some(batch) = &trade_batch {
                let column = batch
                    .column_by_name(name)
                    .ok_or_else(|| invalid_error(format!("missing trade column: {name}")))?;

                sources.push(column.as_ref());
            }

            if let Some(batch) = &depth_batch {
                let column = batch
                    .column_by_name(name)
                    .ok_or_else(|| invalid_error(format!("missing depth column: {name}")))?;

                sources.push(column.as_ref());
            }

            let concatenated = concat(&sources).map_err(|error| {
                invalid_error(format!(
                    "combined column concatenation failed: {name}: {error}"
                ))
            })?;

            let reordered =
                take(concatenated.as_ref(), &source_indices, None).map_err(|error| {
                    invalid_error(format!(
                        "combined column interleaving failed: {name}: {error}"
                    ))
                })?;

            common_columns.push(reordered);
        }

        // ---------------------------------------------------------------------
        // Nullable trade payload
        // ---------------------------------------------------------------------

        let trade_field = schema
            .field_with_name("trade")
            .map_err(|error| invalid_error(error.to_string()))?;

        let trade_payload = build_payload(
            trade_batch.as_ref(),
            &trade_payload_fields(trade_field.data_type())?,
            &UInt32Array::from(trade_indices),
            6,
            18,
            events.len(),
        )?;

        // ---------------------------------------------------------------------
        // Nullable depth payload
        // ---------------------------------------------------------------------

        let depth_field = schema
            .field_with_name("depth")
            .map_err(|error| invalid_error(error.to_string()))?;

        let depth_payload = build_payload(
            depth_batch.as_ref(),
            &trade_payload_fields(depth_field.data_type())?,
            &UInt32Array::from(depth_indices),
            12,
            17,
            events.len(),
        )?;

        // ---------------------------------------------------------------------
        // Assemble combined schema
        // ---------------------------------------------------------------------

        let mut columns: Vec<ArrayRef> = Vec::with_capacity(15);

        // First six shared identity fields.
        columns.extend(common_columns[..6].iter().cloned());

        columns.push(Arc::new(UInt8Array::from(event_kinds)));

        // Six provenance fields.
        columns.extend(common_columns[6..].iter().cloned());

        columns.push(trade_payload);
        columns.push(depth_payload);

        let batch = RecordBatch::try_new(schema, columns).map_err(|error| {
            invalid_error(format!("failed to construct combined batch: {error}"))
        })?;

        if batch.num_rows() != events.len() {
            return invalid("combined event count mismatch");
        }

        Ok(Some(batch))
    }
}

// -----------------------------------------------------------------------------
// Payload construction
// -----------------------------------------------------------------------------

fn trade_payload_fields(data_type: &DataType) -> Result<Fields> {
    match data_type {
        DataType::Struct(fields) => Ok(fields.clone()),
        _ => invalid("combined payload must be Struct"),
    }
}

fn build_payload(
    batch: Option<&RecordBatch>,
    fields: &Fields,
    indices: &UInt32Array,
    column_start: usize,
    column_end: usize,
    output_rows: usize,
) -> Result<ArrayRef> {
    let data_type = DataType::Struct(fields.clone());

    let Some(batch) = batch else {
        return Ok(new_null_array(&data_type, output_rows));
    };

    if column_end > batch.num_columns() || column_end - column_start != fields.len() {
        return invalid("payload column range does not match schema");
    }

    let columns = batch.columns()[column_start..column_end].to_vec();

    let source = StructArray::try_new(fields.clone(), columns, None)
        .map_err(|error| invalid_error(format!("failed to construct source payload: {error}")))?;

    // Nullable indices generate null parent structs.
    // Child values remain canonical Arrow values.
    let payload = take(&source, indices, None)
        .map_err(|error| invalid_error(format!("failed to interleave payload: {error}")))?;

    Ok(payload)
}

fn checked_index(index: usize) -> Result<u32> {
    u32::try_from(index).map_err(|_| invalid_error("combined Arrow index exceeds UInt32"))
}

fn invalid_error(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(message.into())
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(invalid_error(message))
}
