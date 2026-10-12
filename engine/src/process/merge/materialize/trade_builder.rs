use std::sync::Arc;

use arrow_array::{Array, ArrayRef, RecordBatch, UInt32Array, UInt64Array};
use arrow_select::concat::concat;

use crate::{
    error::{MarketForgeError, Result},
    process::parquet::trade_schema,
};

use super::{materialize_resolver::ResolvedMarketEvent, materialize_schema::physical_trade_schema};

// -----------------------------------------------------------------------------
// Physical trade batch builder
// -----------------------------------------------------------------------------

#[derive(Debug, Default)]
pub struct PhysicalTradeBatchBuilder;

impl PhysicalTradeBatchBuilder {
    pub fn new() -> Self {
        Self
    }

    /// Construct a physical trade RecordBatch.
    ///
    /// Input events must already be chronologically ordered.
    /// Canonical Arrow values are preserved without numerical conversion.
    pub fn build(&self, events: &[ResolvedMarketEvent]) -> Result<Option<RecordBatch>> {
        if events.is_empty() {
            return Ok(None);
        }

        let canonical_schema = trade_schema();
        let physical_schema = physical_trade_schema();

        let mut canonical_columns = Vec::with_capacity(canonical_schema.fields().len());

        // ---------------------------------------------------------------------
        // Validate event references and canonical batches
        // ---------------------------------------------------------------------

        for event in events {
            let reference = &event.reference;

            if reference.event_kind != 0 {
                return invalid("physical trade builder received non-trade event");
            }

            if reference.canonical_row_count != 1 {
                return invalid("physical trade event must reference exactly one row");
            }

            if reference.event_boundary.is_some() || reference.source_operation.is_some() {
                return invalid("trade event contains depth-specific metadata");
            }

            if event.canonical_rows() != 1 {
                return invalid("resolved trade must contain exactly one canonical row");
            }

            for batch in &event.canonical_batches {
                if batch.schema().as_ref() != canonical_schema.as_ref() {
                    return invalid("resolved trade canonical schema mismatch");
                }
            }
        }

        // ---------------------------------------------------------------------
        // Assemble canonical columns
        // ---------------------------------------------------------------------

        for column_index in 0..canonical_schema.fields().len() {
            let arrays: Vec<&dyn Array> = events
                .iter()
                .flat_map(|event| event.canonical_batches.iter())
                .filter(|batch| batch.num_rows() > 0)
                .map(|batch| batch.column(column_index).as_ref())
                .collect();

            let combined = concat(&arrays).map_err(|error| {
                invalid_error(format!(
                    "failed to concatenate canonical trade column \
                     {}: {error}",
                    canonical_schema.field(column_index).name()
                ))
            })?;

            canonical_columns.push(combined);
        }

        // ---------------------------------------------------------------------
        // Append provenance columns
        // ---------------------------------------------------------------------

        let stream_rank = UInt32Array::from(
            events
                .iter()
                .map(|event| event.reference.stream_rank)
                .collect::<Vec<_>>(),
        );

        let dataset_id = UInt64Array::from(
            events
                .iter()
                .map(|event| event.reference.dataset_id)
                .collect::<Vec<_>>(),
        );

        let task_id = UInt64Array::from(
            events
                .iter()
                .map(|event| event.reference.task_id)
                .collect::<Vec<_>>(),
        );

        let event_ordinal = UInt64Array::from(
            events
                .iter()
                .map(|event| event.reference.event_ordinal)
                .collect::<Vec<_>>(),
        );

        let canonical_row_offset = UInt64Array::from(
            events
                .iter()
                .map(|event| event.reference.canonical_row_offset)
                .collect::<Vec<_>>(),
        );

        let canonical_row_count = UInt64Array::from(
            events
                .iter()
                .map(|event| event.reference.canonical_row_count)
                .collect::<Vec<_>>(),
        );

        let mut columns: Vec<ArrayRef> = canonical_columns;

        columns.extend([
            Arc::new(stream_rank) as ArrayRef,
            Arc::new(dataset_id),
            Arc::new(task_id),
            Arc::new(event_ordinal),
            Arc::new(canonical_row_offset),
            Arc::new(canonical_row_count),
        ]);

        // ---------------------------------------------------------------------
        // Construct physical Arrow batch
        // ---------------------------------------------------------------------

        let batch = RecordBatch::try_new(physical_schema, columns).map_err(|error| {
            invalid_error(format!("failed to construct physical trade batch: {error}"))
        })?;

        if batch.num_rows() != events.len() {
            return invalid(format!(
                "physical trade event count mismatch: \
                 expected={}, actual={}",
                events.len(),
                batch.num_rows()
            ));
        }

        Ok(Some(batch))
    }
}

fn invalid_error(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(message.into())
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(invalid_error(message))
}
