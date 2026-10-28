use serde_json::Value;
use thiserror::Error;

use crate::{
    live::{config::exchange::ExchangeId, stream::request::SourceChannel},
    shared::domain::canonical::{EventEnvelope, Exchange, Trade},
};

use crate::live::infrastructure::normalization::context::NormalizationContext;
use crate::live::infrastructure::normalization::extraction::JsonExtractor;
use crate::live::infrastructure::normalization::message::{
    NormalizationSource, NormalizedTrades, TradeRecordSemantics,
};
use crate::live::infrastructure::normalization::quantity::QuantityNormalizer;
use crate::live::infrastructure::normalization::trades::plan::CompiledTradeNormalizationPlan;

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, Error)]
pub enum TradeDecodeError {
    #[error("unsupported trade plan: {0}")]
    UnsupportedPlan(String),

    #[error("invalid normalization context: {0}")]
    InvalidContext(String),

    #[error("trade field extraction failed: {0}")]
    Fields(String),

    #[error("aggressor-side normalization failed: {0}")]
    Side(String),

    #[error("quantity normalization failed: {0}")]
    Quantity(String),

    #[error("trade batch construction failed: {0}")]
    Batch(String),
}

// ============================================================
// GENERIC TRADE DECODER
// ============================================================

/// Stateless decoder of individual exchange-native trades.
///
/// The compiled plan controls:
/// - Native record extraction.
/// - Native field mappings.
/// - Timestamp representation.
/// - Aggressor-side interpretation.
/// - Quantity precision.
///
/// NormalizationContext controls:
/// - Instrument identity.
/// - Exchange identity.
/// - Native quantity units.
/// - Contract value.
/// - Contract denomination.
///
/// The decoder does not maintain order-book state,
/// sequence continuity, deduplication, or recovery state.
#[derive(Debug, Clone)]
pub struct TradeDecoder {
    pub(super) plan: CompiledTradeNormalizationPlan,
}

impl TradeDecoder {
    // ========================================================
    // CONSTRUCTION
    // ========================================================

    pub fn new(plan: CompiledTradeNormalizationPlan) -> Result<Self, TradeDecodeError> {
        if !matches!(&plan.semantics, TradeRecordSemantics::Individual) {
            return Err(TradeDecodeError::UnsupportedPlan(
                "aggregate trades are not supported by the \
                 individual-trade decoder"
                    .into(),
            ));
        }

        if plan.aggregate_fields.is_some() {
            return Err(TradeDecodeError::UnsupportedPlan(
                "individual trade plan contains aggregate metadata".into(),
            ));
        }

        Ok(Self { plan })
    }
    /// Decode a batch after its transport-specific message
    /// validation and record extraction have succeeded.
    ///
    /// Both REST and WebSocket use this canonical conversion path.
    pub(super) fn decode_records(
        &self,
        records: crate::live::infrastructure::normalization::trades::plan::TradeRecordView<'_>,
        context: &NormalizationContext,
        source: NormalizationSource,
    ) -> Result<NormalizedTrades, TradeDecodeError> {
        self.validate_context(context)?;

        let mut trades = Vec::with_capacity(records.len());

        for record in records.iter() {
            let trade = self.decode_record(record, context)?;
            trades.push(trade);
        }

        NormalizedTrades::new(
            context.stream_id().to_owned(),
            source,
            TradeRecordSemantics::Individual,
            trades,
        )
        .map_err(TradeDecodeError::Batch)
    }
    pub fn plan(&self) -> &CompiledTradeNormalizationPlan {
        &self.plan
    }

    // ========================================================
    // CONTEXT VALIDATION
    // ========================================================

    pub(super) fn validate_context(
        &self,
        context: &NormalizationContext,
    ) -> Result<(), TradeDecodeError> {
        if !matches!(context.channel(), SourceChannel::Trades) {
            return Err(TradeDecodeError::InvalidContext(
                "trade decoder requires a trades source stream".into(),
            ));
        }

        if let Some(declared) = &self.plan.declared_quantity_type {
            if context.quantity_type() != *declared {
                return Err(TradeDecodeError::InvalidContext(format!(
                    "native quantity unit mismatch: \
                         format declares {declared:?}, \
                         instrument declares {:?}",
                    context.quantity_type(),
                )));
            }
        }
        // ========================================================
        // FORMAT / EXCHANGE COMPATIBILITY
        // ========================================================

        if context.exchange() != self.plan.expected_exchange {
            return Err(TradeDecodeError::InvalidContext(format!(
                "exchange mismatch: format expects {:?}, \
             stream is {:?}",
                self.plan.expected_exchange,
                context.exchange(),
            )));
        }

        // ========================================================
        // FORMAT / MARKET COMPATIBILITY
        // ========================================================

        if context.market_category() != self.plan.expected_market {
            return Err(TradeDecodeError::InvalidContext(format!(
                "market mismatch: format expects {:?}, \
             stream is {:?}",
                self.plan.expected_market,
                context.market_category(),
            )));
        }
        Ok(())
    }
    // ========================================================
    // NATIVE RECORD INSTRUMENT VALIDATION
    // ========================================================

    fn validate_record_identity(
        &self,
        record: &Value,
        context: &NormalizationContext,
    ) -> Result<(), TradeDecodeError> {
        let Some(path) = self.plan.record_symbol_field.as_deref() else {
            // This format does not declare a record-level
            // instrument field.
            //
            // Such formats require message-envelope
            // validation before publication.
            return Ok(());
        };

        let native_value = JsonExtractor::required(record, path).map_err(|error| {
            TradeDecodeError::InvalidContext(format!(
                "cannot extract native instrument at \
                 '{path}': {error}"
            ))
        })?;

        let native_symbol = native_value.as_str().ok_or_else(|| {
            TradeDecodeError::InvalidContext(format!(
                "native instrument at '{path}' \
                     must be a string"
            ))
        })?;

        if native_symbol.is_empty() {
            return Err(TradeDecodeError::InvalidContext(
                "native instrument symbol is empty".into(),
            ));
        }

        // Symbols must match the resolved native instrument
        // identity exactly.
        //
        // Do not strip suffixes, reinterpret market types,
        // or construct aliases inside the trade decoder.
        //
        // Any symbol aliasing must be resolved explicitly
        // by the instrument catalog or subscription layer.
        if native_symbol != context.symbol() {
            return Err(TradeDecodeError::InvalidContext(format!(
                "native instrument mismatch: \
                     received '{native_symbol}', \
                     expected '{}'",
                context.symbol(),
            )));
        }

        Ok(())
    }
    // ========================================================
    // DECODE ONE ALREADY-EXTRACTED RECORD
    // ========================================================

    pub fn decode_record(
        &self,
        record: &Value,
        context: &NormalizationContext,
    ) -> Result<Trade, TradeDecodeError> {
        self.validate_context(context)?;
        self.validate_record_identity(record, context)?;
        // ========================================================
        // NATIVE INSTRUMENT DISCRIMINATOR
        // ========================================================

        if let Some(validator) = &self.plan.symbol_type {
            validator.validate(record).map_err(|error| {
                TradeDecodeError::InvalidContext(format!(
                    "native instrument discriminator: {error}"
                ))
            })?;
        }
        // ====================================================
        // NATIVE FIELDS
        // ====================================================

        let fields = self
            .plan
            .fields
            .read(record)
            .map_err(|error| TradeDecodeError::Fields(error.to_string()))?;

        // ====================================================
        // AGGRESSOR SIDE
        // ====================================================

        let resolved = self
            .plan
            .side
            .resolve(fields.native_side, fields.native_quantity)
            .map_err(|error| TradeDecodeError::Side(error.to_string()))?;

        // ====================================================
        // CONTRACT / ASSET QUANTITY CONVERSION
        // ====================================================

        let quantities = QuantityNormalizer::normalize_trade(
            resolved.quantity,
            fields.price,
            context,
            self.plan.precision,
        )
        .map_err(|error| TradeDecodeError::Quantity(error.to_string()))?;

        // ====================================================
        // EXCHANGE IDENTITY
        // ====================================================

        let exchange = canonical_exchange(&context.exchange());

        // ====================================================
        // CANONICAL ENVELOPE
        // ====================================================

        let envelope = EventEnvelope {
            event_timestamp_ns: fields.timestamp_ns,

            // Receive/system timestamps belong to the
            // transport ingestion boundary, not the
            // exchange's event timestamp.
            system_timestamp_ns: None,

            exchange,

            instrument_id: context.instrument_id(),

            symbol: context.symbol().to_owned(),

            stream_id: context.stream_id().to_owned(),
        };

        // ====================================================
        // CANONICAL TRADE
        // ====================================================

        Ok(Trade {
            envelope,

            trade_id: fields.trade_id,

            sequence: fields.sequence,

            side: resolved.side,

            price: fields.price,

            quantity_base: quantities.quantity_base,

            quantity_quote: quantities.quantity_quote,

            quantity_contracts: quantities.quantity_contracts,

            // Supplementary native attributes require
            // explicit configured decoding contracts.
            is_rpi: None,
            trade_iv: None,
            mark_iv: None,
            index_price: None,
            mark_price: None,
        })
    }
}

// ============================================================
// LIVE -> CANONICAL EXCHANGE MAPPING
// ============================================================

/// Central mapping between the Live exchange registry
/// and the historical/canonical exchange enumeration.
///
/// No string conversions or fallback exchange identity.
fn canonical_exchange(exchange: &ExchangeId) -> Exchange {
    match exchange {
        ExchangeId::Binance => Exchange::Binance,
        ExchangeId::Bybit => Exchange::Bybit,
        ExchangeId::Okx => Exchange::Okx,
        ExchangeId::Bitget => Exchange::Bitget,
        ExchangeId::Gateio => Exchange::GateIo,
    }
}
