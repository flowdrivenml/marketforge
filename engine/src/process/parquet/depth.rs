use std::sync::Arc;

use arrow_array::{ArrayRef, Decimal256Array, Int64Array, RecordBatch, StringArray, UInt64Array};
use arrow_buffer::i256;

use crate::{
    canonical::{BookSide, Exchange, L2LevelUpdate},
    error::{MarketForgeError, Result},
};

use super::{
    decimal::decimal_to_i256,
    schema::{DECIMAL_PRECISION, DECIMAL_SCALE, depth_schema},
};

// -----------------------------------------------------------------------------
// Depth batch builder
// -----------------------------------------------------------------------------

#[derive(Debug, Default)]
pub struct DepthBatchBuilder {
    levels: Vec<L2LevelUpdate>,
}

impl DepthBatchBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            levels: Vec::with_capacity(capacity),
        }
    }

    pub fn push(&mut self, level: L2LevelUpdate) {
        self.levels.push(level);
    }

    pub fn extend<I>(&mut self, levels: I)
    where
        I: IntoIterator<Item = L2LevelUpdate>,
    {
        self.levels.extend(levels);
    }

    pub fn len(&self) -> usize {
        self.levels.len()
    }

    pub fn is_empty(&self) -> bool {
        self.levels.is_empty()
    }

    pub fn clear(&mut self) {
        self.levels.clear();
    }

    // -------------------------------------------------------------------------
    // Build Arrow batch
    // -------------------------------------------------------------------------

    pub fn finish(&mut self) -> Result<Option<RecordBatch>> {
        if self.levels.is_empty() {
            return Ok(None);
        }

        // Build before clearing so conversion errors preserve buffered levels.
        let batch = self.build_batch()?;

        self.levels.clear();

        Ok(Some(batch))
    }

    fn build_batch(&self) -> Result<RecordBatch> {
        let levels = &self.levels;

        let event_timestamp_ns = Int64Array::from(
            levels
                .iter()
                .map(|level| level.envelope.event_timestamp_ns)
                .collect::<Vec<_>>(),
        );

        let system_timestamp_ns = Int64Array::from(
            levels
                .iter()
                .map(|level| level.envelope.system_timestamp_ns)
                .collect::<Vec<_>>(),
        );

        let exchange = StringArray::from(
            levels
                .iter()
                .map(|level| exchange_name(level.envelope.exchange))
                .collect::<Vec<_>>(),
        );

        let instrument_id = Int64Array::from(
            levels
                .iter()
                .map(|level| level.envelope.instrument_id)
                .collect::<Vec<_>>(),
        );

        let symbol = StringArray::from(
            levels
                .iter()
                .map(|level| level.envelope.symbol.as_str())
                .collect::<Vec<_>>(),
        );

        let stream_id = StringArray::from(
            levels
                .iter()
                .map(|level| level.envelope.stream_id.as_str())
                .collect::<Vec<_>>(),
        );

        let side = StringArray::from(
            levels
                .iter()
                .map(|level| side_name(level.side))
                .collect::<Vec<_>>(),
        );

        let price = decimal_array(levels.iter().map(|level| Some(level.price)))?;

        let quantity_base = decimal_array(levels.iter().map(|level| level.quantity_base))?;

        let quantity_quote = decimal_array(levels.iter().map(|level| level.quantity_quote))?;

        let quantity_contracts =
            decimal_array(levels.iter().map(|level| level.quantity_contracts))?;

        let order_count = UInt64Array::from(
            levels
                .iter()
                .map(|level| level.order_count)
                .collect::<Vec<_>>(),
        );

        // ---------------------------------------------------------------------
        // Construct RecordBatch
        // ---------------------------------------------------------------------

        let columns: Vec<ArrayRef> = vec![
            Arc::new(event_timestamp_ns),
            Arc::new(system_timestamp_ns),
            Arc::new(exchange),
            Arc::new(instrument_id),
            Arc::new(symbol),
            Arc::new(stream_id),
            Arc::new(side),
            Arc::new(price),
            Arc::new(quantity_base),
            Arc::new(quantity_quote),
            Arc::new(quantity_contracts),
            Arc::new(order_count),
        ];

        RecordBatch::try_new(depth_schema(), columns).map_err(|error| {
            MarketForgeError::InvalidCanonical(format!(
                "failed to construct depth Arrow batch: {error}"
            ))
        })
    }
}

// -----------------------------------------------------------------------------
// Decimal256 conversion
// -----------------------------------------------------------------------------

fn decimal_array<I>(values: I) -> Result<Decimal256Array>
where
    I: IntoIterator<Item = Option<rust_decimal::Decimal>>,
{
    let values = values
        .into_iter()
        .map(|value| value.map(decimal_to_i256).transpose())
        .collect::<Result<Vec<Option<i256>>>>()?;

    Decimal256Array::from(values)
        .with_precision_and_scale(DECIMAL_PRECISION, DECIMAL_SCALE)
        .map_err(|error| {
            MarketForgeError::InvalidCanonical(format!(
                "failed to construct depth Decimal256 array: {error}"
            ))
        })
}

// -----------------------------------------------------------------------------
// Enum serialization
// -----------------------------------------------------------------------------

fn exchange_name(exchange: Exchange) -> &'static str {
    match exchange {
        Exchange::Bybit => "bybit",
        Exchange::Binance => "binance",
        Exchange::Okx => "okx",
        Exchange::Bitget => "bitget",
        Exchange::GateIo => "gateio",
    }
}

fn side_name(side: BookSide) -> &'static str {
    match side {
        BookSide::Bid => "bid",
        BookSide::Ask => "ask",
    }
}
