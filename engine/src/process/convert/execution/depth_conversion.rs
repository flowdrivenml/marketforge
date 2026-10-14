use std::collections::HashMap;

use arrow_array::{
    Array, Decimal256Array, Int64Array, ListArray, RecordBatch, StructArray, UInt32Array,
    UInt64Array,
};

use crate::{
    error::{MarketForgeError, Result},
    job::ConvertJob,
    process::convert::{
        ConversionDecimal, ConversionPlan, ConvertedDepthLevel, convert_depth_level,
        output::{
            CONVERSION_FAILURE_ARITHMETIC, CONVERSION_FAILURE_FUTURE,
            CONVERSION_FAILURE_IDENTITY_MISMATCH, CONVERSION_FAILURE_MISSING,
            CONVERSION_FAILURE_NO_PATH, CONVERSION_FAILURE_STALE, CONVERSION_STATUS_CONVERTED,
            CONVERSION_STATUS_IDENTITY, CONVERSION_STATUS_UNAVAILABLE, DepthConversionBuffers,
            DepthConversionRow, TradeConversionRow,
        },
        reference::{
            ConversionUnavailable, ReferenceKey, ReferenceState, evaluate_conversion_path,
        },
    },
};

// -----------------------------------------------------------------------------
// Depth conversion metrics
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct DepthConversionMetrics {
    pub events_read: u64,
    pub identity_events: u64,
    pub converted_events: u64,
    pub unavailable_events: u64,

    pub levels_read: u64,
    pub levels_converted: u64,
    pub levels_unavailable: u64,
}
pub(super) struct DepthBatchView<'a> {
    pub(super) timestamps: &'a Int64Array,
    ranks: &'a UInt32Array,
    datasets: &'a UInt64Array,
    tasks: &'a UInt64Array,
    ordinals: &'a UInt64Array,

    pub(super) changes: &'a ListArray,
    prices: &'a Decimal256Array,
    quantities: &'a Decimal256Array,
}

impl<'a> DepthBatchView<'a> {
    pub(super) fn new(batch: &'a RecordBatch) -> Result<Self> {
        let changes = column::<ListArray>(batch, "changes")?;

        let levels = changes
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| invalid_error("depth changes must contain structs"))?;

        let prices = levels
            .column_by_name("price")
            .and_then(|array| array.as_any().downcast_ref::<Decimal256Array>())
            .ok_or_else(|| invalid_error("missing depth level prices"))?;

        let quantities = levels
            .column_by_name("quantity_quote")
            .and_then(|array| array.as_any().downcast_ref::<Decimal256Array>())
            .ok_or_else(|| invalid_error("missing depth quote quantities"))?;

        Ok(Self {
            timestamps: column::<Int64Array>(batch, "event_timestamp_ns")?,
            ranks: column::<UInt32Array>(batch, "stream_rank")?,
            datasets: column::<UInt64Array>(batch, "dataset_id")?,
            tasks: column::<UInt64Array>(batch, "task_id")?,
            ordinals: column::<UInt64Array>(batch, "event_ordinal")?,
            changes,
            prices,
            quantities,
        })
    }
}
// -----------------------------------------------------------------------------
// Depth conversion kernel
// -----------------------------------------------------------------------------

pub struct DepthConversionKernel<'a> {
    job: &'a ConvertJob,
    plan: &'a ConversionPlan,
    streams: HashMap<u32, usize>,
}

impl<'a> DepthConversionKernel<'a> {
    pub fn new(job: &'a ConvertJob, plan: &'a ConversionPlan) -> Result<Self> {
        let streams = plan
            .streams
            .iter()
            .enumerate()
            .map(|(index, stream)| (stream.source.stream_rank, index))
            .collect::<HashMap<_, _>>();

        if streams.len() != plan.streams.len() {
            return invalid("duplicate depth conversion stream rank");
        }

        Ok(Self { job, plan, streams })
    }

    // -------------------------------------------------------------------------
    // Convert one Arrow batch
    // -------------------------------------------------------------------------

    // -----------------------------------------------------------------------------
    // Convert one Arrow batch
    // -----------------------------------------------------------------------------

    pub fn convert_batch(
        &self,
        batch: &RecordBatch,
        state: &ReferenceState,
        metrics: &mut DepthConversionMetrics,
    ) -> Result<Vec<DepthConversionRow>> {
        let view = DepthBatchView::new(batch)?;

        let mut results = Vec::with_capacity(batch.num_rows());

        for row in 0..batch.num_rows() {
            results.push(self.convert_event_view(&view, row, state, metrics)?);
        }

        Ok(results)
    }

    // -----------------------------------------------------------------------------
    // Convert one depth event without slicing the Arrow batch
    // -----------------------------------------------------------------------------
    pub fn convert_event(
        &self,
        batch: &RecordBatch,
        row: usize,
        state: &ReferenceState,
        metrics: &mut DepthConversionMetrics,
    ) -> Result<DepthConversionRow> {
        if row >= batch.num_rows() {
            return invalid("depth conversion row index out of bounds");
        }

        let view = DepthBatchView::new(batch)?;

        self.convert_event_view(&view, row, state, metrics)
    }

    pub(super) fn convert_event_view(
        &self,
        view: &DepthBatchView<'_>,
        row: usize,
        state: &ReferenceState,
        metrics: &mut DepthConversionMetrics,
    ) -> Result<DepthConversionRow> {
        if row >= view.changes.len() {
            return invalid("depth conversion row index out of bounds");
        }

        let timestamps = view.timestamps;
        let ranks = view.ranks;
        let datasets = view.datasets;
        let tasks = view.tasks;
        let ordinals = view.ordinals;

        let changes = view.changes;
        let prices = view.prices;
        let quantities = view.quantities;

        // -------------------------------------------------------------------------
        // Validate event identity
        // -------------------------------------------------------------------------

        if timestamps.is_null(row)
            || ranks.is_null(row)
            || datasets.is_null(row)
            || tasks.is_null(row)
            || ordinals.is_null(row)
            || changes.is_null(row)
        {
            return invalid("null required depth event field");
        }

        let rank = ranks.value(row);
        let dataset_id = datasets.value(row);

        let stream_index = self
            .streams
            .get(&rank)
            .ok_or_else(|| invalid_error(format!("unknown depth conversion stream rank {rank}")))?;

        let stream = &self.plan.streams[*stream_index];

        if stream.source.dataset_id != dataset_id {
            return invalid("depth conversion dataset identity mismatch");
        }

        let key = ReferenceKey::new(
            timestamps.value(row),
            rank,
            tasks.value(row),
            ordinals.value(row),
        );

        // -------------------------------------------------------------------------
        // Resolve nested depth levels
        // -------------------------------------------------------------------------

        let offsets = changes.value_offsets();

        let start = usize::try_from(offsets[row])
            .map_err(|_| invalid_error("negative depth list offset"))?;

        let end = usize::try_from(offsets[row + 1])
            .map_err(|_| invalid_error("negative depth list offset"))?;

        if end < start || end > prices.len() || end > quantities.len() {
            return invalid("invalid depth level offsets");
        }

        let level_count = end - start;

        // -------------------------------------------------------------------------
        // Validate level prices
        // -------------------------------------------------------------------------

        for index in start..end {
            if prices.is_null(index) {
                return invalid(format!(
                    "null depth price at event {row}, level {}",
                    index - start,
                ));
            }
        }

        let target_quote = self.plan.target_quote.as_str().to_owned();

        // -------------------------------------------------------------------------
        // Identity conversion
        // -------------------------------------------------------------------------

        let result = if stream.identity_conversion {
            let converted_levels = copy_identity_levels(prices, quantities, start, end)?;

            DepthConversionRow {
                metadata: TradeConversionRow {
                    target_quote,
                    status: CONVERSION_STATUS_IDENTITY,
                    failure: None,
                    rate: Some(ConversionDecimal::one()),
                    price_target: None,
                    quantity_quote_target: None,
                    path: Some(vec![self.plan.target_quote.as_str().to_owned()]),
                    references: Vec::new(),
                },
                levels: Some(converted_levels),
            }
        } else if stream.paths.is_empty() {
            unavailable_row(target_quote, CONVERSION_FAILURE_NO_PATH)
        } else {
            let mut converted = None;
            let mut failure = CONVERSION_FAILURE_MISSING;

            for path in &stream.paths {
                match evaluate_conversion_path(
                    path,
                    state,
                    key,
                    self.job.conversion.max_staleness_ns,
                ) {
                    Ok(evaluation) => {
                        match convert_levels(prices, quantities, start, end, &evaluation) {
                            Ok(converted_levels) => {
                                converted = Some(DepthConversionRow {
                                    metadata: TradeConversionRow {
                                        target_quote: target_quote.clone(),
                                        status: CONVERSION_STATUS_CONVERTED,
                                        failure: None,
                                        rate: Some(evaluation.rate),
                                        price_target: None,
                                        quantity_quote_target: None,
                                        path: Some(
                                            path.currencies
                                                .iter()
                                                .map(|currency| currency.as_str().to_owned())
                                                .collect(),
                                        ),
                                        references: evaluation.references,
                                    },
                                    levels: Some(converted_levels),
                                });

                                break;
                            }

                            Err(_) => {
                                failure = CONVERSION_FAILURE_ARITHMETIC;
                            }
                        }
                    }

                    Err(reason) => {
                        failure = failure_code(&reason);
                    }
                }
            }

            converted.unwrap_or_else(|| unavailable_row(target_quote, failure))
        };

        // -------------------------------------------------------------------------
        // Update metrics
        // -------------------------------------------------------------------------

        metrics.levels_read = metrics
            .levels_read
            .checked_add(level_count as u64)
            .ok_or_else(|| invalid_error("depth level counter overflow"))?;

        match result.metadata.status {
            CONVERSION_STATUS_IDENTITY => {
                metrics.identity_events += 1;
                metrics.levels_converted += level_count as u64;
            }

            CONVERSION_STATUS_CONVERTED => {
                metrics.converted_events += 1;
                metrics.levels_converted += level_count as u64;
            }

            CONVERSION_STATUS_UNAVAILABLE => {
                metrics.unavailable_events += 1;
                metrics.levels_unavailable += level_count as u64;
            }

            _ => return invalid("invalid depth conversion status"),
        }

        metrics.events_read += 1;

        Ok(result)
    }
    pub fn convert_combined_event(
        &self,
        batch: &RecordBatch,
        row: usize,
        state: &ReferenceState,
        metrics: &mut DepthConversionMetrics,
    ) -> Result<DepthConversionRow> {
        use arrow_array::UInt8Array;
        use arrow_schema::{Field, Schema};
        use std::sync::Arc;

        // Validate row bounds.
        if row >= batch.num_rows() {
            return invalid("combined depth row index out of bounds");
        }

        // Validate event discriminator.
        let kinds = column::<UInt8Array>(batch, "event_kind")?;

        if kinds.is_null(row) || kinds.value(row) != 1 {
            return invalid("combined depth adapter requires a depth event");
        }

        // Extract nested depth payload.
        let depths = column::<StructArray>(batch, "depth")?;

        if depths.is_null(row) {
            return invalid("combined depth event has null depth payload");
        }

        // Extract nested changes list.
        let changes = depths
            .column_by_name("changes")
            .ok_or_else(|| invalid_error("missing combined depth changes"))?;

        if changes.as_any().downcast_ref::<ListArray>().is_none() {
            return invalid("combined depth changes must be a ListArray");
        }

        // Preserve all original source fields.
        let mut fields = batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>();

        let mut columns = batch.columns().to_vec();

        // Expose depth.changes as a top-level column.
        fields.push(Field::new("changes", changes.data_type().clone(), true));

        columns.push(changes.clone());

        // Construct a lightweight Arrow view.
        let schema = Arc::new(Schema::new(fields));

        let adapted = RecordBatch::try_new(schema, columns).map_err(|error| {
            invalid_error(format!("cannot construct combined depth adapter: {error}"))
        })?;

        // Reuse the existing depth conversion kernel.
        self.convert_event(&adapted, row, state, metrics)
    }
    pub fn adapt_combined_depth_batch(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        use arrow_schema::{Field, Schema};
        use std::sync::Arc;

        let depths = column::<StructArray>(batch, "depth")?;

        let changes = depths
            .column_by_name("changes")
            .ok_or_else(|| invalid_error("missing combined depth changes"))?;

        if changes.as_any().downcast_ref::<ListArray>().is_none() {
            return invalid("combined depth changes must be a ListArray");
        }

        let mut fields = batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>();

        let mut columns = batch.columns().to_vec();

        fields.push(Field::new("changes", changes.data_type().clone(), true));

        columns.push(changes.clone());

        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(|error| {
            invalid_error(format!("cannot construct combined depth adapter: {error}"))
        })
    }
    pub(super) fn convert_event_into_buffers(
        &self,
        view: &DepthBatchView<'_>,
        row: usize,
        state: &ReferenceState,
        metrics: &mut DepthConversionMetrics,
        buffers: &mut DepthConversionBuffers,
    ) -> Result<TradeConversionRow> {
        if row >= view.changes.len() {
            return invalid("depth conversion row index out of bounds");
        }

        let timestamps = view.timestamps;
        let ranks = view.ranks;
        let datasets = view.datasets;
        let tasks = view.tasks;
        let ordinals = view.ordinals;
        let changes = view.changes;
        let prices = view.prices;
        let quantities = view.quantities;

        // ---------------------------------------------------------------------
        // Validate event identity
        // ---------------------------------------------------------------------

        if timestamps.is_null(row)
            || ranks.is_null(row)
            || datasets.is_null(row)
            || tasks.is_null(row)
            || ordinals.is_null(row)
            || changes.is_null(row)
        {
            return invalid("null required depth event field");
        }

        let rank = ranks.value(row);
        let dataset_id = datasets.value(row);

        let stream_index = self
            .streams
            .get(&rank)
            .ok_or_else(|| invalid_error(format!("unknown depth conversion stream rank {rank}")))?;

        let stream = &self.plan.streams[*stream_index];

        if stream.source.dataset_id != dataset_id {
            return invalid("depth conversion dataset identity mismatch");
        }

        let key = ReferenceKey::new(
            timestamps.value(row),
            rank,
            tasks.value(row),
            ordinals.value(row),
        );

        // ---------------------------------------------------------------------
        // Resolve depth-level boundaries
        // ---------------------------------------------------------------------

        let offsets = changes.value_offsets();

        let start = usize::try_from(offsets[row])
            .map_err(|_| invalid_error("negative depth list offset"))?;

        let end = usize::try_from(offsets[row + 1])
            .map_err(|_| invalid_error("negative depth list offset"))?;

        if end < start || end > prices.len() || end > quantities.len() {
            return invalid("invalid depth level offsets");
        }

        let level_count = end - start;

        for index in start..end {
            if prices.is_null(index) {
                return invalid(format!(
                    "null depth price at event {row}, level {}",
                    index - start,
                ));
            }
        }

        // ---------------------------------------------------------------------
        // Preserve buffer boundaries
        // ---------------------------------------------------------------------

        let checkpoint = buffers.prices.len();

        if checkpoint != buffers.quantities.len() {
            return invalid("depth conversion buffer length mismatch");
        }

        let target_quote = self.plan.target_quote.as_str().to_owned();

        // ---------------------------------------------------------------------
        // Identity conversion
        // ---------------------------------------------------------------------

        let result = if stream.identity_conversion {
            buffers.append_identity(prices, quantities, start, end)?;

            TradeConversionRow {
                target_quote,
                status: CONVERSION_STATUS_IDENTITY,
                failure: None,
                rate: Some(ConversionDecimal::one()),
                price_target: None,
                quantity_quote_target: None,
                path: Some(vec![self.plan.target_quote.as_str().to_owned()]),
                references: Vec::new(),
            }
        } else if stream.paths.is_empty() {
            buffers.append_unavailable(level_count);

            unavailable_row(target_quote, CONVERSION_FAILURE_NO_PATH).metadata
        } else {
            let mut converted = None;
            let mut failure = CONVERSION_FAILURE_MISSING;

            for path in &stream.paths {
                match evaluate_conversion_path(
                    path,
                    state,
                    key,
                    self.job.conversion.max_staleness_ns,
                ) {
                    Ok(evaluation) => {
                        match convert_levels_into_buffers(
                            prices,
                            quantities,
                            start,
                            end,
                            &evaluation,
                            buffers,
                        ) {
                            Ok(()) => {
                                converted = Some(TradeConversionRow {
                                    target_quote: target_quote.clone(),
                                    status: CONVERSION_STATUS_CONVERTED,
                                    failure: None,
                                    rate: Some(evaluation.rate),
                                    price_target: None,
                                    quantity_quote_target: None,
                                    path: Some(
                                        path.currencies
                                            .iter()
                                            .map(|currency| currency.as_str().to_owned())
                                            .collect(),
                                    ),
                                    references: evaluation.references,
                                });

                                break;
                            }

                            Err(_) => {
                                failure = CONVERSION_FAILURE_ARITHMETIC;
                            }
                        }
                    }

                    Err(reason) => {
                        failure = failure_code(&reason);
                    }
                }
            }

            match converted {
                Some(metadata) => metadata,

                None => {
                    buffers.rollback(checkpoint);
                    buffers.append_unavailable(level_count);

                    unavailable_row(target_quote, failure).metadata
                }
            }
        };

        // ---------------------------------------------------------------------
        // Verify event-level buffer accounting
        // ---------------------------------------------------------------------

        if buffers.prices.len() != checkpoint + level_count
            || buffers.quantities.len() != checkpoint + level_count
        {
            return invalid("depth conversion event buffer mismatch");
        }

        buffers.finish_event()?;

        // ---------------------------------------------------------------------
        // Update metrics
        // ---------------------------------------------------------------------

        metrics.levels_read = metrics
            .levels_read
            .checked_add(level_count as u64)
            .ok_or_else(|| invalid_error("depth level counter overflow"))?;

        match result.status {
            CONVERSION_STATUS_IDENTITY => {
                metrics.identity_events += 1;
                metrics.levels_converted += level_count as u64;
            }

            CONVERSION_STATUS_CONVERTED => {
                metrics.converted_events += 1;
                metrics.levels_converted += level_count as u64;
            }

            CONVERSION_STATUS_UNAVAILABLE => {
                metrics.unavailable_events += 1;
                metrics.levels_unavailable += level_count as u64;
            }

            _ => return invalid("invalid depth conversion status"),
        }

        metrics.events_read += 1;

        Ok(result)
    }
}
// -----------------------------------------------------------------------------
// Identity conversion
// -----------------------------------------------------------------------------

fn copy_identity_levels(
    prices: &Decimal256Array,
    quantities: &Decimal256Array,
    start: usize,
    end: usize,
) -> Result<Vec<ConvertedDepthLevel>> {
    let mut levels = Vec::with_capacity(end - start);

    for index in start..end {
        let price = ConversionDecimal::new(prices.value(index))?;

        let quantity_quote = if quantities.is_null(index) {
            None
        } else {
            Some(ConversionDecimal::new(quantities.value(index))?)
        };

        levels.push(ConvertedDepthLevel {
            price_target: price,
            quantity_quote_target: quantity_quote,
        });
    }

    Ok(levels)
}

// -----------------------------------------------------------------------------
// Convert all levels atomically
// -----------------------------------------------------------------------------

fn convert_levels(
    prices: &Decimal256Array,
    quantities: &Decimal256Array,
    start: usize,
    end: usize,
    evaluation: &crate::process::convert::reference::EvaluatedConversion,
) -> Result<Vec<ConvertedDepthLevel>> {
    let mut levels = Vec::with_capacity(end - start);

    for index in start..end {
        let price = ConversionDecimal::new(prices.value(index))?;

        let quantity_quote = if quantities.is_null(index) {
            None
        } else {
            Some(ConversionDecimal::new(quantities.value(index))?)
        };

        levels.push(convert_depth_level(price, quantity_quote, evaluation)?);
    }

    Ok(levels)
}

// -----------------------------------------------------------------------------
// Unavailable conversion
// -----------------------------------------------------------------------------

fn unavailable_row(target_quote: String, failure: u8) -> DepthConversionRow {
    DepthConversionRow {
        metadata: TradeConversionRow {
            target_quote,
            status: CONVERSION_STATUS_UNAVAILABLE,
            failure: Some(failure),
            rate: None,
            price_target: None,
            quantity_quote_target: None,
            path: None,
            references: Vec::new(),
        },
        levels: None,
    }
}

fn failure_code(reason: &ConversionUnavailable) -> u8 {
    match reason {
        ConversionUnavailable::MissingReference(_) => CONVERSION_FAILURE_MISSING,

        ConversionUnavailable::StaleReference(_) => CONVERSION_FAILURE_STALE,

        ConversionUnavailable::FutureReference(_) => CONVERSION_FAILURE_FUTURE,

        ConversionUnavailable::ReferenceIdentityMismatch(_) => CONVERSION_FAILURE_IDENTITY_MISMATCH,

        ConversionUnavailable::ArithmeticFailure(_) => CONVERSION_FAILURE_ARITHMETIC,
    }
}

// -----------------------------------------------------------------------------
// Arrow column access
// -----------------------------------------------------------------------------

fn column<'a, T: Array + 'static>(batch: &'a RecordBatch, name: &str) -> Result<&'a T> {
    batch
        .column_by_name(name)
        .and_then(|array| array.as_any().downcast_ref::<T>())
        .ok_or_else(|| invalid_error(format!("missing or invalid depth column: {name}")))
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

// -----------------------------------------------------------------------------
// Convert depth levels directly into batch buffers
// -----------------------------------------------------------------------------

fn convert_levels_into_buffers(
    prices: &Decimal256Array,
    quantities: &Decimal256Array,
    start: usize,
    end: usize,
    evaluation: &crate::process::convert::reference::EvaluatedConversion,
    buffers: &mut DepthConversionBuffers,
) -> Result<()> {
    let checkpoint = buffers.prices.len();

    for index in start..end {
        let result = (|| {
            let price = ConversionDecimal::new(prices.value(index))?;

            let quantity = if quantities.is_null(index) {
                None
            } else {
                Some(ConversionDecimal::new(quantities.value(index))?)
            };

            convert_depth_level(price, quantity, evaluation)
        })();

        match result {
            Ok(level) => {
                buffers.append_level(
                    Some(level.price_target.mantissa()),
                    level.quantity_quote_target.map(|value| value.mantissa()),
                );
            }

            Err(error) => {
                // Preserve event-level atomicity.
                buffers.rollback(checkpoint);
                return Err(error);
            }
        }
    }

    Ok(())
}
