use super::{
    CombinedReferenceAlignment, DepthConversionKernel, DepthConversionMetrics,
    PhysicalConversionMetrics, TradeConversionKernel,
};
use crate::{
    error::{MarketForgeError, Result},
    job::ConvertJob,
    process::convert::{
        ConversionDecimal, ConversionPlan,
        output::{DepthConversionRow, TradeConversionRow},
        reference::{ReferenceKey, ReferenceObservation},
    },
};
use arrow_array::{
    Array, Decimal256Array, Int64Array, RecordBatch, StructArray, UInt8Array, UInt32Array,
    UInt64Array,
};
use std::collections::HashMap;

// -----------------------------------------------------------------------------
// Combined conversion result
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub enum CombinedConversionRow {
    Trade(TradeConversionRow),
    Depth(DepthConversionRow),
}

// -----------------------------------------------------------------------------
// Combined conversion metrics
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct CombinedConversionMetrics {
    pub trade: PhysicalConversionMetrics,
    pub depth: DepthConversionMetrics,

    pub events_read: u64,
    pub trade_events: u64,
    pub depth_events: u64,

    pub skipped_trade_events: u64,
    pub skipped_depth_events: u64,
    pub skipped_depth_levels: u64,
}

// -----------------------------------------------------------------------------
// Combined conversion kernel
// -----------------------------------------------------------------------------

pub struct CombinedConversionKernel<'a> {
    trade: TradeConversionKernel<'a>,
    depth: DepthConversionKernel<'a>,

    target_quote: String,
    selected_streams: HashMap<u32, bool>,
}

impl<'a> CombinedConversionKernel<'a> {
    pub fn new(job: &'a ConvertJob, plan: &'a ConversionPlan) -> Result<Self> {
        let selected_streams = plan
            .streams
            .iter()
            .map(|stream| (stream.source.stream_rank, stream.selected_for_conversion))
            .collect::<HashMap<_, _>>();

        if selected_streams.len() != plan.streams.len() {
            return invalid("duplicate combined conversion stream rank");
        }

        Ok(Self {
            trade: TradeConversionKernel::new(job, plan)?,
            depth: DepthConversionKernel::new(job, plan)?,

            target_quote: job.conversion.target_quote.clone(),
            selected_streams,
        })
    }
    fn selected_for_conversion(&self, batch: &RecordBatch, row: usize) -> Result<bool> {
        let ranks = column::<UInt32Array>(batch, "stream_rank")?;

        if row >= ranks.len() || ranks.is_null(row) {
            return invalid("invalid combined conversion stream rank");
        }

        self.selected_streams
            .get(&ranks.value(row))
            .copied()
            .ok_or_else(|| {
                invalid_error(format!(
                    "combined conversion stream rank not planned: {}",
                    ranks.value(row)
                ))
            })
    }
    pub fn convert_event(
        &self,
        batch: &RecordBatch,
        row: usize,
        alignment: &mut CombinedReferenceAlignment,
        metrics: &mut CombinedConversionMetrics,
    ) -> Result<CombinedConversionRow> {
        if row >= batch.num_rows() {
            return invalid("combined conversion row index out of bounds");
        }

        let timestamps = column::<Int64Array>(batch, "event_timestamp_ns")?;

        let kinds = column::<UInt8Array>(batch, "event_kind")?;

        if timestamps.is_null(row) || kinds.is_null(row) {
            return invalid("null combined event identity");
        }

        let timestamp_ns = timestamps.value(row);
        let selected = self.selected_for_conversion(batch, row)?;

        // -------------------------------------------------------------
        // Advance external references strictly before current event.
        // -------------------------------------------------------------

        alignment.advance_before(timestamp_ns)?;

        // -------------------------------------------------------------
        // Dispatch event by physical kind.
        // -------------------------------------------------------------

        let result = match kinds.value(row) {
            0 => {
                let converted = if selected {
                    self.trade.evaluate_combined_event(
                        batch,
                        row,
                        alignment.state_mut(),
                        &mut metrics.trade,
                    )?
                } else {
                    metrics.skipped_trade_events = metrics
                        .skipped_trade_events
                        .checked_add(1)
                        .ok_or_else(|| invalid_error("skipped trade counter overflow"))?;

                    TradeConversionRow::skipped(&self.target_quote)
                };

                // Incorporate selected internal reference only after
                // evaluating the current trade.
                let streams = column::<arrow_array::StringArray>(batch, "stream_id")?;

                let stream_id = streams.value(row);

                if alignment.is_internal_reference(stream_id) {
                    let observation = trade_observation(batch, row)?;
                    alignment.observe_internal(observation)?;
                }

                metrics.trade_events += 1;

                CombinedConversionRow::Trade(converted)
            }

            1 => {
                let converted = if selected {
                    self.depth.convert_combined_event(
                        batch,
                        row,
                        alignment.state(),
                        &mut metrics.depth,
                    )?
                } else {
                    DepthConversionRow::skipped(&self.target_quote)
                };

                metrics.depth_events += 1;

                CombinedConversionRow::Depth(converted)
            }

            kind => {
                return invalid(format!("unsupported combined event kind: {kind}"));
            }
        };

        metrics.events_read += 1;

        Ok(result)
    }

    pub fn convert_batch(
        &self,
        batch: &RecordBatch,
        alignment: &mut CombinedReferenceAlignment,
        metrics: &mut CombinedConversionMetrics,
    ) -> Result<Vec<CombinedConversionRow>> {
        let kinds = column::<UInt8Array>(batch, "event_kind")?;

        let trade_batch = self.trade.adapt_combined_trade_batch(batch)?;
        let depth_batch = self.depth.adapt_combined_depth_batch(batch)?;

        let mut results = Vec::with_capacity(batch.num_rows());

        for row in 0..batch.num_rows() {
            let selected = self.selected_for_conversion(batch, row)?;
            let timestamps = column::<Int64Array>(batch, "event_timestamp_ns")?;

            if timestamps.is_null(row) || kinds.is_null(row) {
                return invalid("null combined event identity");
            }

            alignment.advance_before(timestamps.value(row))?;

            let result = match kinds.value(row) {
                0 => {
                    let converted = if selected {
                        self.trade.evaluate_adapted_event(
                            &trade_batch,
                            row,
                            alignment.state_mut(),
                            &mut metrics.trade,
                        )?
                    } else {
                        metrics.skipped_trade_events = metrics
                            .skipped_trade_events
                            .checked_add(1)
                            .ok_or_else(|| invalid_error("skipped trade counter overflow"))?;

                        TradeConversionRow::skipped(&self.target_quote)
                    };

                    let streams = column::<arrow_array::StringArray>(batch, "stream_id")?;

                    if alignment.is_internal_reference(streams.value(row)) {
                        alignment.observe_internal(trade_observation(batch, row)?)?;
                    }

                    metrics.trade_events += 1;

                    CombinedConversionRow::Trade(converted)
                }
                1 => {
                    let converted = if selected {
                        self.depth.convert_event(
                            &depth_batch,
                            row,
                            alignment.state(),
                            &mut metrics.depth,
                        )?
                    } else {
                        let levels = combined_depth_level_count(batch, row)?;

                        metrics.skipped_depth_events = metrics
                            .skipped_depth_events
                            .checked_add(1)
                            .ok_or_else(|| invalid_error("skipped depth counter overflow"))?;

                        metrics.skipped_depth_levels = metrics
                            .skipped_depth_levels
                            .checked_add(levels)
                            .ok_or_else(|| invalid_error("skipped depth level counter overflow"))?;

                        DepthConversionRow::skipped(&self.target_quote)
                    };

                    metrics.depth_events += 1;

                    CombinedConversionRow::Depth(converted)
                }

                kind => {
                    return invalid(format!("unsupported combined event kind: {kind}"));
                }
            };

            metrics.events_read += 1;
            results.push(result);
        }

        Ok(results)
    }
}
fn combined_depth_level_count(batch: &RecordBatch, row: usize) -> Result<u64> {
    let depths = column::<StructArray>(batch, "depth")?;

    if row >= batch.num_rows() || depths.is_null(row) {
        return invalid("invalid skipped combined depth payload");
    }

    let changes = depths
        .column_by_name("changes")
        .and_then(|array| array.as_any().downcast_ref::<arrow_array::ListArray>())
        .ok_or_else(|| invalid_error("missing combined depth changes"))?;

    if changes.is_null(row) {
        return invalid("null skipped combined depth changes");
    }

    Ok(changes.value_length(row) as u64)
}
// -----------------------------------------------------------------------------
// Extract internal trade-reference observation
// -----------------------------------------------------------------------------

fn trade_observation(batch: &RecordBatch, row: usize) -> Result<ReferenceObservation> {
    let timestamps = column::<Int64Array>(batch, "event_timestamp_ns")?;

    let ranks = column::<UInt32Array>(batch, "stream_rank")?;

    let datasets = column::<UInt64Array>(batch, "dataset_id")?;

    let tasks = column::<UInt64Array>(batch, "task_id")?;

    let ordinals = column::<UInt64Array>(batch, "event_ordinal")?;

    let streams = column::<arrow_array::StringArray>(batch, "stream_id")?;

    let trades = column::<StructArray>(batch, "trade")?;

    if trades.is_null(row) {
        return invalid("combined reference trade has null payload");
    }

    let prices = trades
        .column_by_name("price")
        .and_then(|array| array.as_any().downcast_ref::<Decimal256Array>())
        .ok_or_else(|| invalid_error("missing combined trade price"))?;

    if prices.is_null(row) {
        return invalid("combined reference trade has null price");
    }

    Ok(ReferenceObservation {
        dataset_id: datasets.value(row),
        stream_id: streams.value(row).to_owned(),

        key: ReferenceKey::new(
            timestamps.value(row),
            ranks.value(row),
            tasks.value(row),
            ordinals.value(row),
        ),

        price: ConversionDecimal::new(prices.value(row))?,
    })
}

// -----------------------------------------------------------------------------
// Arrow access
// -----------------------------------------------------------------------------

fn column<'a, T: Array + 'static>(batch: &'a RecordBatch, name: &str) -> Result<&'a T> {
    batch
        .column_by_name(name)
        .and_then(|array| array.as_any().downcast_ref::<T>())
        .ok_or_else(|| invalid_error(format!("missing or invalid combined column: {name}")))
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
