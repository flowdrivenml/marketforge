use crate::historical::convert::execution::depth::conversion::DepthBatchView;
use crate::historical::convert::execution::{DepthConversionKernel, DepthConversionMetrics};
use arrow_array::{Array, RecordBatch};
use {
    crate::historical::convert::output::ConvertedDepthBatchBuilder,
    crate::historical::convert::output::DepthConversionBuffers,
    crate::historical::convert::output::TradeConversionRow,
    crate::historical::convert::reference::ExternalReferenceCursor,
    crate::historical::convert::reference::ReferenceState, crate::shared::error::MarketForgeError,
    crate::shared::error::Result,
};

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
        let view = DepthBatchView::new(batch)?;

        if self.builder.is_none() {
            self.builder = Some(ConvertedDepthBatchBuilder::new(batch.schema())?);
        }

        // ---------------------------------------------------------------------
        // Allocate direct conversion buffers
        // ---------------------------------------------------------------------

        let offsets = view.changes.value_offsets();

        let first =
            usize::try_from(offsets[0]).map_err(|_| invalid_error("invalid depth offset"))?;

        let last = usize::try_from(offsets[offsets.len() - 1])
            .map_err(|_| invalid_error("invalid depth offset"))?;

        let total_levels = last
            .checked_sub(first)
            .ok_or_else(|| invalid_error("invalid depth offset range"))?;

        let mut buffers = DepthConversionBuffers::with_capacity(batch.num_rows(), total_levels);

        let mut metadata = Vec::<TradeConversionRow>::with_capacity(batch.num_rows());

        // ---------------------------------------------------------------------
        // Align references and convert each event
        // ---------------------------------------------------------------------

        for row in 0..batch.num_rows() {
            if view.timestamps.is_null(row) {
                return invalid("null depth event timestamp");
            }

            let timestamp_ns = view.timestamps.value(row);

            let updates = self.cursor.advance_before(timestamp_ns, &mut self.state)?;

            self.reference_updates = self
                .reference_updates
                .checked_add(updates)
                .ok_or_else(|| invalid_error("depth reference update overflow"))?;

            let converted = self.kernel.convert_event_into_buffers(
                &view,
                row,
                &self.state,
                &mut self.metrics,
                &mut buffers,
            )?;

            metadata.push(converted);
        }

        // ---------------------------------------------------------------------
        // Construct Arrow output directly from buffers
        // ---------------------------------------------------------------------

        self.builder
            .as_ref()
            .expect("depth builder initialized")
            .build_from_buffers(batch, &metadata, buffers)
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
