use arrow_array::{Array, Int64Array, RecordBatch};

use crate::{
    error::{MarketForgeError, Result},
    process::convert::{
        output::{ConvertedDepthBatchBuilder, DepthConversionRow},
        reference::{ExternalReferenceCursor, ReferenceState},
    },
};

use super::{DepthConversionKernel, DepthConversionMetrics};

// -----------------------------------------------------------------------------
// Depth conversion alignment
// -----------------------------------------------------------------------------

pub struct DepthConversionAlignment<'a> {
    kernel: DepthConversionKernel<'a>,
    cursor: ExternalReferenceCursor,
    state: ReferenceState,
    builder: Option<ConvertedDepthBatchBuilder>,

    metrics: DepthConversionMetrics,
    reference_updates: u64,
}

impl<'a> DepthConversionAlignment<'a> {
    pub fn new(kernel: DepthConversionKernel<'a>, cursor: ExternalReferenceCursor) -> Self {
        Self::from_checkpoint(kernel, cursor, ReferenceState::new())
    }

    // -------------------------------------------------------------------------
    // Convert one physical depth batch
    // -------------------------------------------------------------------------

    pub fn convert_batch(&mut self, batch: &RecordBatch) -> Result<RecordBatch> {
        let timestamps = batch
            .column_by_name("event_timestamp_ns")
            .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| invalid_error("missing depth event timestamps"))?;

        if self.builder.is_none() {
            self.builder = Some(ConvertedDepthBatchBuilder::new(batch.schema())?);
        }

        let mut rows = Vec::<DepthConversionRow>::with_capacity(batch.num_rows());

        // -------------------------------------------------------------
        // Align references independently for each depth event
        // -------------------------------------------------------------

        for row in 0..batch.num_rows() {
            if timestamps.is_null(row) {
                return invalid("null depth event timestamp");
            }

            let timestamp_ns = timestamps.value(row);

            let updates = self.cursor.advance_before(timestamp_ns, &mut self.state)?;

            self.reference_updates = self
                .reference_updates
                .checked_add(updates)
                .ok_or_else(|| invalid_error("depth reference update overflow"))?;

            // ---------------------------------------------------------
            // Convert exactly one event using its historical state
            // ---------------------------------------------------------

            let converted =
                self.kernel
                    .convert_event(batch, row, &self.state, &mut self.metrics)?;

            rows.push(converted);
        }

        // -------------------------------------------------------------
        // Build converted Arrow batch
        // -------------------------------------------------------------

        self.builder
            .as_ref()
            .expect("depth builder initialized")
            .build(batch, &rows)
    }

    // -------------------------------------------------------------------------
    // Metrics
    // -------------------------------------------------------------------------

    pub fn metrics(&self) -> &DepthConversionMetrics {
        &self.metrics
    }

    pub fn reference_updates(&self) -> u64 {
        self.reference_updates
    }

    pub fn reference_state(&self) -> &ReferenceState {
        &self.state
    }

    pub fn observations_read(&self) -> u64 {
        self.cursor.observations_read()
    }
    pub fn from_checkpoint(
        kernel: DepthConversionKernel<'a>,
        cursor: ExternalReferenceCursor,
        state: ReferenceState,
    ) -> Self {
        Self {
            kernel,
            cursor,
            state,
            builder: None,
            metrics: DepthConversionMetrics::default(),
            reference_updates: 0,
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
