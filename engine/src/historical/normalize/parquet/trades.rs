use std::sync::Arc;

use arrow_array::{
    ArrayRef, BooleanArray, Decimal256Array, Int64Array, RecordBatch, StringArray, UInt64Array,
};
use arrow_buffer::i256;

use crate::{
    canonical::{Exchange, Trade, TradeSide},
    error::{MarketForgeError, Result},
};

use super::{
    decimal::decimal_to_i256,
    schema::{DECIMAL_PRECISION, DECIMAL_SCALE, trade_schema},
};

#[derive(Debug, Default)]
pub struct TradeBatchBuilder {
    trades: Vec<Trade>,
}

impl TradeBatchBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            trades: Vec::with_capacity(capacity),
        }
    }

    pub fn push(&mut self, trade: Trade) {
        self.trades.push(trade);
    }

    pub fn len(&self) -> usize {
        self.trades.len()
    }

    pub fn is_empty(&self) -> bool {
        self.trades.is_empty()
    }

    pub fn clear(&mut self) {
        self.trades.clear();
    }

    pub fn finish(&mut self) -> Result<Option<RecordBatch>> {
        if self.trades.is_empty() {
            return Ok(None);
        }

        // Build the batch before clearing the source records.
        // Failed conversions must not silently discard trades.
        let batch = self.build_batch()?;

        self.trades.clear();

        Ok(Some(batch))
    }

    fn build_batch(&self) -> Result<RecordBatch> {
        let trades = &self.trades;

        let event_timestamp_ns = Int64Array::from(
            trades
                .iter()
                .map(|trade| trade.envelope.event_timestamp_ns)
                .collect::<Vec<_>>(),
        );

        let system_timestamp_ns = Int64Array::from(
            trades
                .iter()
                .map(|trade| trade.envelope.system_timestamp_ns)
                .collect::<Vec<_>>(),
        );

        let exchange = StringArray::from(
            trades
                .iter()
                .map(|trade| exchange_name(trade.envelope.exchange))
                .collect::<Vec<_>>(),
        );

        let instrument_id = Int64Array::from(
            trades
                .iter()
                .map(|trade| trade.envelope.instrument_id)
                .collect::<Vec<_>>(),
        );

        let symbol = StringArray::from(
            trades
                .iter()
                .map(|trade| trade.envelope.symbol.as_str())
                .collect::<Vec<_>>(),
        );

        let stream_id = StringArray::from(
            trades
                .iter()
                .map(|trade| trade.envelope.stream_id.as_str())
                .collect::<Vec<_>>(),
        );

        let trade_id = StringArray::from(
            trades
                .iter()
                .map(|trade| trade.trade_id.as_deref())
                .collect::<Vec<_>>(),
        );

        let sequence = UInt64Array::from(
            trades
                .iter()
                .map(|trade| trade.sequence)
                .collect::<Vec<_>>(),
        );

        let side = StringArray::from(
            trades
                .iter()
                .map(|trade| side_name(trade.side))
                .collect::<Vec<_>>(),
        );

        let price = decimal_array(trades.iter().map(|trade| Some(trade.price)))?;

        let quantity_base = decimal_array(trades.iter().map(|trade| trade.quantity_base))?;

        let quantity_quote = decimal_array(trades.iter().map(|trade| trade.quantity_quote))?;

        let quantity_contracts =
            decimal_array(trades.iter().map(|trade| trade.quantity_contracts))?;

        let is_rpi =
            BooleanArray::from(trades.iter().map(|trade| trade.is_rpi).collect::<Vec<_>>());

        let trade_iv = decimal_array(trades.iter().map(|trade| trade.trade_iv))?;

        let mark_iv = decimal_array(trades.iter().map(|trade| trade.mark_iv))?;

        let index_price = decimal_array(trades.iter().map(|trade| trade.index_price))?;

        let mark_price = decimal_array(trades.iter().map(|trade| trade.mark_price))?;

        let columns: Vec<ArrayRef> = vec![
            Arc::new(event_timestamp_ns),
            Arc::new(system_timestamp_ns),
            Arc::new(exchange),
            Arc::new(instrument_id),
            Arc::new(symbol),
            Arc::new(stream_id),
            Arc::new(trade_id),
            Arc::new(sequence),
            Arc::new(side),
            Arc::new(price),
            Arc::new(quantity_base),
            Arc::new(quantity_quote),
            Arc::new(quantity_contracts),
            Arc::new(is_rpi),
            Arc::new(trade_iv),
            Arc::new(mark_iv),
            Arc::new(index_price),
            Arc::new(mark_price),
        ];

        RecordBatch::try_new(trade_schema(), columns).map_err(|error| {
            MarketForgeError::InvalidCanonical(format!(
                "failed to construct canonical trade RecordBatch: {error}"
            ))
        })
    }
}

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
                "failed to construct Decimal256 array: {error}"
            ))
        })
}

fn exchange_name(exchange: Exchange) -> &'static str {
    match exchange {
        Exchange::Bybit => "bybit",
        Exchange::Binance => "binance",
        Exchange::Okx => "okx",
        Exchange::Bitget => "bitget",
        Exchange::GateIo => "gateio",
    }
}

fn side_name(side: TradeSide) -> &'static str {
    match side {
        TradeSide::Buy => "buy",
        TradeSide::Sell => "sell",
    }
}
