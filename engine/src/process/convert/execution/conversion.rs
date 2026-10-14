use std::collections::{HashMap, HashSet};

use arrow_array::{Array, Decimal256Array, Int64Array, RecordBatch, UInt32Array, UInt64Array};

use crate::{
    error::{MarketForgeError, Result},
    job::ConvertJob,
    process::convert::{
        ConversionDecimal, ConversionPlan, convert_trade_values,
        output::{
            CONVERSION_FAILURE_ARITHMETIC, CONVERSION_FAILURE_FUTURE,
            CONVERSION_FAILURE_IDENTITY_MISMATCH, CONVERSION_FAILURE_MISSING,
            CONVERSION_FAILURE_NO_PATH, CONVERSION_FAILURE_STALE, CONVERSION_STATUS_CONVERTED,
            CONVERSION_STATUS_IDENTITY, CONVERSION_STATUS_UNAVAILABLE, TradeConversionRow,
        },
        reference::{
            ConversionUnavailable, ReferenceKey, ReferenceObservation, ReferenceState,
            evaluate_conversion_path,
        },
    },
};

use super::materialize::PhysicalConversionMetrics;

// -----------------------------------------------------------------------------
// Shared conversion kernel
// -----------------------------------------------------------------------------

pub struct TradeConversionKernel<'a> {
    job: &'a ConvertJob,
    plan: &'a ConversionPlan,

    streams: HashMap<u32, usize>,
    reference_streams: HashSet<u32>,
}

impl<'a> TradeConversionKernel<'a> {
    pub fn new(job: &'a ConvertJob, plan: &'a ConversionPlan) -> Result<Self> {
        let streams = plan
            .streams
            .iter()
            .enumerate()
            .map(|(index, stream)| (stream.source.stream_rank, index))
            .collect::<HashMap<_, _>>();

        if streams.len() != plan.streams.len() {
            return invalid("duplicate conversion stream rank");
        }

        let reference_streams = plan
            .streams
            .iter()
            .filter(|stream| job.conversion.references.contains(&stream.source.identity))
            .map(|stream| stream.source.stream_rank)
            .collect::<HashSet<_>>();

        Ok(Self {
            job,
            plan,
            streams,
            reference_streams,
        })
    }

    // -------------------------------------------------------------------------
    // Convert one Arrow batch
    // -------------------------------------------------------------------------

    pub fn convert_batch(
        &self,
        batch: &RecordBatch,
        state: &mut ReferenceState,
        metrics: &mut PhysicalConversionMetrics,
    ) -> Result<Vec<TradeConversionRow>> {
        let mut rows = Vec::with_capacity(batch.num_rows());

        for row in 0..batch.num_rows() {
            rows.push(self.convert_event(batch, row, state, metrics)?);
        }

        Ok(rows)
    }
    fn convert_event_impl(
        &self,
        batch: &RecordBatch,
        row: usize,
        state: &mut ReferenceState,
        metrics: &mut PhysicalConversionMetrics,
        update_reference: bool,
    ) -> Result<TradeConversionRow> {
        if row >= batch.num_rows() {
            return invalid("trade conversion row index out of bounds");
        }

        let timestamps = column::<Int64Array>(batch, "event_timestamp_ns")?;
        let ranks = column::<UInt32Array>(batch, "stream_rank")?;
        let dataset_ids = column::<UInt64Array>(batch, "dataset_id")?;
        let tasks = column::<UInt64Array>(batch, "task_id")?;
        let ordinals = column::<UInt64Array>(batch, "event_ordinal")?;
        let prices = column::<Decimal256Array>(batch, "price")?;
        let quantities = column::<Decimal256Array>(batch, "quantity_quote")?;

        let rank = ranks.value(row);
        let dataset_id = dataset_ids.value(row);

        let stream_index = self
            .streams
            .get(&rank)
            .ok_or_else(|| invalid_error(format!("unknown conversion stream rank {rank}")))?;

        let stream = &self.plan.streams[*stream_index];

        if stream.source.dataset_id != dataset_id {
            return invalid("conversion source dataset mismatch");
        }

        let key = ReferenceKey::new(
            timestamps.value(row),
            rank,
            tasks.value(row),
            ordinals.value(row),
        );

        let price = ConversionDecimal::new(prices.value(row))?;

        let quantity_quote = if quantities.is_null(row) {
            None
        } else {
            Some(ConversionDecimal::new(quantities.value(row))?)
        };

        let target_quote = self.plan.target_quote.as_str().to_owned();

        // Evaluate conversion before updating reference state.
        let result = if stream.identity_conversion {
            TradeConversionRow {
                target_quote,
                status: CONVERSION_STATUS_IDENTITY,
                failure: None,
                rate: Some(ConversionDecimal::one()),
                price_target: Some(price),
                quantity_quote_target: quantity_quote,
                path: Some(vec![self.plan.target_quote.as_str().to_owned()]),
                references: Vec::new(),
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
                        match convert_trade_values(price, quantity_quote, &evaluation) {
                            Ok(values) => {
                                converted = Some(TradeConversionRow {
                                    target_quote: target_quote.clone(),
                                    status: CONVERSION_STATUS_CONVERTED,
                                    failure: None,
                                    rate: Some(evaluation.rate),
                                    price_target: Some(values.price_target),
                                    quantity_quote_target: values.quantity_quote_target,
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

            converted.unwrap_or_else(|| unavailable_row(target_quote, failure))
        };

        match result.status {
            CONVERSION_STATUS_IDENTITY => metrics.identity_events += 1,
            CONVERSION_STATUS_CONVERTED => metrics.converted_events += 1,
            CONVERSION_STATUS_UNAVAILABLE => metrics.unavailable_events += 1,
            _ => return invalid("invalid conversion status"),
        }

        // Update references only after conversion.
        if update_reference && self.reference_streams.contains(&rank) {
            state.update(ReferenceObservation {
                dataset_id,
                stream_id: stream.source.stream_id.clone(),
                key,
                price,
            })?;

            metrics.reference_updates += 1;
        }

        metrics.events_read += 1;

        Ok(result)
    }
    pub fn convert_combined_event(
        &self,
        batch: &RecordBatch,
        row: usize,
        state: &mut ReferenceState,
        metrics: &mut PhysicalConversionMetrics,
    ) -> Result<TradeConversionRow> {
        self.validate_combined_trade_row(batch, row)?;

        let adapted = self.adapt_combined_trade_batch(batch)?;

        self.convert_event(&adapted, row, state, metrics)
    }
    fn validate_combined_trade_row(&self, batch: &RecordBatch, row: usize) -> Result<()> {
        use arrow_array::{StructArray, UInt8Array};

        if row >= batch.num_rows() {
            return invalid("combined trade row index out of bounds");
        }

        let kinds = column::<UInt8Array>(batch, "event_kind")?;

        if kinds.is_null(row) || kinds.value(row) != 0 {
            return invalid("combined trade adapter requires a trade event");
        }

        let trades = column::<StructArray>(batch, "trade")?;

        if trades.is_null(row) {
            return invalid("combined trade event has null trade payload");
        }

        Ok(())
    }
    pub fn convert_event(
        &self,
        batch: &RecordBatch,
        row: usize,
        state: &mut ReferenceState,
        metrics: &mut PhysicalConversionMetrics,
    ) -> Result<TradeConversionRow> {
        self.convert_event_impl(batch, row, state, metrics, true)
    }
    pub fn evaluate_combined_event(
        &self,
        batch: &RecordBatch,
        row: usize,
        state: &mut ReferenceState,
        metrics: &mut PhysicalConversionMetrics,
    ) -> Result<TradeConversionRow> {
        let adapted = self.adapt_combined_trade_batch(batch)?;

        self.convert_event_impl(&adapted, row, state, metrics, false)
    }
    pub fn adapt_combined_trade_batch(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        use arrow_array::StructArray;

        let trades = column::<StructArray>(batch, "trade")?;

        let prices = trades
            .column_by_name("price")
            .ok_or_else(|| invalid_error("missing combined trade price"))?;

        let quantities = trades
            .column_by_name("quantity_quote")
            .ok_or_else(|| invalid_error("missing combined trade quote quantity"))?;

        let mut fields = batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>();

        let mut columns = batch.columns().to_vec();

        fields.push(arrow_schema::Field::new(
            "price",
            prices.data_type().clone(),
            true,
        ));

        columns.push(prices.clone());

        fields.push(arrow_schema::Field::new(
            "quantity_quote",
            quantities.data_type().clone(),
            true,
        ));

        columns.push(quantities.clone());

        let schema = std::sync::Arc::new(arrow_schema::Schema::new(fields));

        RecordBatch::try_new(schema, columns).map_err(|error| {
            invalid_error(format!("cannot construct combined trade adapter: {error}"))
        })
    }
    pub fn evaluate_adapted_event(
        &self,
        adapted: &RecordBatch,
        row: usize,
        state: &mut ReferenceState,
        metrics: &mut PhysicalConversionMetrics,
    ) -> Result<TradeConversionRow> {
        self.convert_event_impl(adapted, row, state, metrics, false)
    }
}

// -----------------------------------------------------------------------------
// Unavailable conversion
// -----------------------------------------------------------------------------

fn unavailable_row(target_quote: String, failure: u8) -> TradeConversionRow {
    TradeConversionRow {
        target_quote,
        status: CONVERSION_STATUS_UNAVAILABLE,
        failure: Some(failure),
        rate: None,
        price_target: None,
        quantity_quote_target: None,
        path: None,
        references: Vec::new(),
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
    let index = batch
        .schema()
        .index_of(name)
        .map_err(|_| invalid_error(format!("missing conversion column: {name}")))?;

    batch
        .column(index)
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| invalid_error(format!("invalid conversion column type: {name}")))
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
