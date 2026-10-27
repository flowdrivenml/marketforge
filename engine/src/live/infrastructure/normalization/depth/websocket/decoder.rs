use crate::live::infrastructure::normalization::context::NormalizationContext;
use crate::live::infrastructure::normalization::depth::compiler::DepthNormalizationCompiler;
use crate::live::infrastructure::normalization::depth::websocket::fields::DepthMetadataPlan;
use crate::live::infrastructure::normalization::extraction::JsonExtractor;
use crate::live::infrastructure::normalization::message::DepthTimestampProvenance;
use crate::live::infrastructure::normalization::message::{
    DepthMessageKind, NormalizationSource, NormalizedDepth,
};
use crate::live::infrastructure::normalization::quantity::{
    QuantityNormalizer, QuantityPrecisionPolicy,
};
use crate::live::stream::request::SourceChannel;
use crate::{
    canonical::{BookSide, EventEnvelope, Exchange, L2LevelUpdate},
    live::config::{exchange::ExchangeId, format::DepthFormatConfig},
};
use serde_json::Value;
use thiserror::Error;

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, Error)]
pub enum DepthDecodeError {
    #[error("invalid decoder configuration: {0}")]
    Configuration(String),

    #[error("invalid normalization context: {0}")]
    Context(String),

    #[error("native depth envelope rejected: {0}")]
    Envelope(String),

    #[error("depth message classification failed: {0}")]
    Classification(String),

    #[error("native depth records rejected: {0}")]
    Records(String),

    #[error("depth metadata decoding failed: {0}")]
    Metadata(String),

    #[error("depth level decoding failed: {0}")]
    Levels(String),

    #[error("depth quantity normalization failed: {0}")]
    Quantity(String),

    #[error("canonical depth construction failed: {0}")]
    Canonical(String),
}

// ============================================================
// GENERIC WEBSOCKET DEPTH DECODER
// ============================================================

/// Converts one complete exchange-native WebSocket depth
/// message into canonical NormalizedDepth.
///
/// This decoder:
///
/// - Validates the expected native message identity.
/// - Classifies snapshots and deltas.
/// - Extracts configured native sequence metadata.
/// - Converts timestamps to canonical nanoseconds.
/// - Decodes all bid/ask levels atomically.
/// - Normalizes native quantities using instrument metadata.
/// - Preserves available order counts.
///
/// It does not:
///
/// - Maintain an order book.
/// - Check continuity against preceding messages.
/// - Schedule REST recovery.
/// - Publish market data.
/// - Assign trusted source epochs.
///
/// Those responsibilities belong to other Live components.
#[derive(Debug, Clone)]
pub struct DepthDecoder {
    config: DepthFormatConfig,

    levels: crate::live::infrastructure::normalization::depth::compiler::CompiledDepthLevelPlan,

    metadata: DepthMetadataPlan,

    precision: QuantityPrecisionPolicy,

    subscription_depth: Option<u64>,
}

impl DepthDecoder {
    // ========================================================
    // CONSTRUCTION
    // ========================================================

    pub fn new_websocket(
        config: &DepthFormatConfig,
        max_levels: usize,
        precision: QuantityPrecisionPolicy,
        subscription_depth: Option<u64>,
    ) -> Result<Self, DepthDecodeError> {
        config.validate().map_err(DepthDecodeError::Configuration)?;

        precision
            .validate()
            .map_err(|error| DepthDecodeError::Configuration(error.to_string()))?;

        if subscription_depth == Some(0) {
            return Err(DepthDecodeError::Configuration(
                "subscription depth must be positive".into(),
            ));
        }

        // Bybit topic patterns can contain {depth}.
        //
        // The expected depth must originate from the
        // authorized subscription configuration.
        if config
            .message
            .topic_pattern
            .as_deref()
            .is_some_and(|pattern| pattern.contains("{depth}"))
            && subscription_depth.is_none()
        {
            return Err(DepthDecodeError::Configuration(
                "topic pattern requires subscription depth".into(),
            ));
        }

        let levels = DepthNormalizationCompiler::compile_levels(config, max_levels)
            .map_err(|error| DepthDecodeError::Configuration(error.to_string()))?;

        let metadata = DepthMetadataPlan::compile(config)
            .map_err(|error| DepthDecodeError::Configuration(error.to_string()))?;

        // Validate supported message classification rules.
        validate_classification_configuration(config)?;

        Ok(Self {
            config: config.clone(),
            levels,
            metadata,
            precision,
            subscription_depth,
        })
    }

    // ========================================================
    // DECODE ONE COMPLETE WEBSOCKET MESSAGE
    // ========================================================

    pub fn decode_websocket(
        &self,
        message: &Value,
        context: &NormalizationContext,
    ) -> Result<NormalizedDepth, DepthDecodeError> {
        // ====================================================
        // INSTRUMENT / FORMAT COMPATIBILITY
        // ====================================================

        self.validate_context(context)?;

        // ====================================================
        // NATIVE MESSAGE ENVELOPE
        // ====================================================

        self.validate_envelope(message, context)?;

        // ====================================================
        // EXTRACT EXACTLY ONE NATIVE BOOK RECORD
        // ====================================================
        // ====================================================
        // EXTRACT EXACTLY ONE NATIVE BOOK RECORD
        // ====================================================

        let record = self.extract_record(message)?;

        // Validate native instrument identity before decoding
        // any market data or constructing canonical records.
        self.validate_record_identity(record, context)?;

        // ====================================================
        // SNAPSHOT / DELTA CLASSIFICATION
        // ====================================================

        let kind = self.classify(message, record)?;

        // ====================================================
        // NATIVE SEQUENCE AND TIMESTAMP
        // ====================================================

        let metadata = self
            .metadata
            .decode(message, kind)
            .map_err(|error| DepthDecodeError::Metadata(error.to_string()))?;

        // ====================================================
        // ATOMIC LEVEL EXTRACTION
        // ====================================================

        let native_levels = self
            .levels
            .levels
            .extract(record)
            .map_err(|error| DepthDecodeError::Levels(error.to_string()))?;

        // ====================================================
        // CANONICAL LEVEL CONSTRUCTION
        // ====================================================

        let exchange = canonical_exchange(&context.exchange());

        let total_levels = native_levels
            .bids
            .len()
            .checked_add(native_levels.asks.len())
            .ok_or_else(|| DepthDecodeError::Levels("combined level count overflow".into()))?;

        let mut levels = Vec::with_capacity(total_levels);

        for level in native_levels.bids {
            levels.push(self.canonical_level(
                level,
                BookSide::Bid,
                context,
                exchange,
                metadata.event_timestamp_ns,
            )?);
        }

        for level in native_levels.asks {
            levels.push(self.canonical_level(
                level,
                BookSide::Ask,
                context,
                exchange,
                metadata.event_timestamp_ns,
            )?);
        }

        // ====================================================
        // ATOMIC CANONICAL MESSAGE
        // ====================================================

        // The declared depth is known for subscription
        // profiles that explicitly select a book depth.
        //
        // Do not infer full book coverage merely from
        // the number of levels received.
        NormalizedDepth::new(
            context.stream_id().to_owned(),
            NormalizationSource::WebSocket,
            kind,
            metadata.sequence,
            levels,
            Some(metadata.event_timestamp_ns),
            self.subscription_depth,
        )
        .and_then(|message| message.with_timestamp_provenance(DepthTimestampProvenance::Exchange))
        .map_err(DepthDecodeError::Canonical)
    }

    // ========================================================
    // CONTEXT VALIDATION
    // ========================================================

    fn validate_context(&self, context: &NormalizationContext) -> Result<(), DepthDecodeError> {
        if context.channel() != SourceChannel::Depth {
            return Err(DepthDecodeError::Context(
                "depth decoder requires a depth source stream".into(),
            ));
        }
        if context.exchange() != self.config.format.exchange {
            return Err(DepthDecodeError::Context(
                "exchange differs from compiled depth format".into(),
            ));
        }

        if context.market_category() != self.config.format.market {
            return Err(DepthDecodeError::Context(
                "market differs from compiled depth format".into(),
            ));
        }

        if context.symbol().is_empty() {
            return Err(DepthDecodeError::Context(
                "native instrument symbol is empty".into(),
            ));
        }

        Ok(())
    }

    // ========================================================
    // NATIVE ENVELOPE VALIDATION
    // ========================================================

    fn validate_envelope(
        &self,
        message: &Value,
        context: &NormalizationContext,
    ) -> Result<(), DepthDecodeError> {
        let cfg = &self.config.message;

        if !message.is_object() {
            return Err(DepthDecodeError::Envelope(
                "native WebSocket message must be an object".into(),
            ));
        }

        let equals =
            |field: Option<&str>, expected: Option<&str>| -> Result<(), DepthDecodeError> {
                match (field, expected) {
                    (None, None) => Ok(()),

                    (Some(path), Some(expected)) => {
                        let actual = required_string(message, path)?;

                        if actual != expected {
                            return Err(DepthDecodeError::Envelope(format!(
                                "{path}: expected {expected:?}, got {actual:?}"
                            )));
                        }

                        Ok(())
                    }

                    _ => Err(DepthDecodeError::Configuration(
                        "incomplete native envelope rule".into(),
                    )),
                }
            };

        equals(
            cfg.event_type_field.as_deref(),
            cfg.event_type_value.as_deref(),
        )?;

        equals(cfg.channel_field.as_deref(), cfg.channel_value.as_deref())?;

        equals(cfg.event_field.as_deref(), cfg.event_value.as_deref())?;

        // These fields identify an instrument directly.
        for path in [cfg.symbol_field.as_deref(), cfg.instrument_field.as_deref()]
            .into_iter()
            .flatten()
        {
            let actual = required_string(message, path)?;

            if actual != context.symbol() {
                return Err(DepthDecodeError::Envelope(format!(
                    "{path}: native symbol {actual:?} \
                         differs from {:?}",
                    context.symbol(),
                )));
            }
        }

        // ====================================================
        // TOPIC IDENTITY
        // ====================================================

        match (
            cfg.topic_field.as_deref(),
            cfg.topic_value.as_deref(),
            cfg.topic_pattern.as_deref(),
        ) {
            (Some(path), Some(expected), None) => {
                equals(Some(path), Some(expected))?;
            }

            (Some(path), None, Some(pattern)) => {
                let actual = required_string(message, path)?;

                let expected = render_topic(pattern, context.symbol(), self.subscription_depth)?;

                if actual != expected {
                    return Err(DepthDecodeError::Envelope(format!(
                        "{path}: expected topic {expected:?}, \
                             got {actual:?}"
                    )));
                }
            }

            (None, None, None) => {}

            _ => {
                return Err(DepthDecodeError::Configuration(
                    "invalid topic validation configuration".into(),
                ));
            }
        }

        // ====================================================
        // MARKET CATEGORY
        // ====================================================

        if let Some(path) = cfg.instrument_type_field.as_deref() {
            let actual = required_string(message, path)?;

            if let Some(expected) = cfg.instrument_type_value.as_deref() {
                if actual != expected {
                    return Err(DepthDecodeError::Envelope(format!(
                        "{path}: expected {expected:?}, got {actual:?}"
                    )));
                }
            } else if !cfg.supported_instrument_types.is_empty() {
                if !cfg
                    .supported_instrument_types
                    .iter()
                    .any(|value| value == actual)
                {
                    return Err(DepthDecodeError::Envelope(format!(
                        "{path}: unsupported native market {actual:?}"
                    )));
                }
            } else {
                return Err(DepthDecodeError::Configuration(
                    "instrument type field has no validation rule".into(),
                ));
            }
        }

        // ====================================================
        // RECORD-LEVEL INSTRUMENT IDENTITY
        // ====================================================

        if let Some(field) = self
            .config
            .fields
            .get("symbol")
            .or_else(|| self.config.fields.get("instrument"))
        {
            // For formats that declare a native instrument
            // inside the record, validate it after extracting
            // the record rather than trusting only its envelope.
            //
            // This happens below in extract_record_identity.
            let _ = field;
        }

        Ok(())
    }

    // ========================================================
    // NATIVE RECORD EXTRACTION
    // ========================================================

    fn extract_record<'a>(&self, message: &'a Value) -> Result<&'a Value, DepthDecodeError> {
        let Some(records) = &self.config.records else {
            // Formats without [records] declare root-relative
            // level paths, e.g. Binance and Bybit.
            return Ok(message);
        };

        match records.representation.as_str() {
            "object" => {
                let record = JsonExtractor::required(message, &records.path)
                    .map_err(|error| DepthDecodeError::Records(error.to_string()))?;

                if !record.is_object() {
                    return Err(DepthDecodeError::Records(
                        "depth record must be an object".into(),
                    ));
                }

                Ok(record)
            }

            "array" => {
                let array = JsonExtractor::array(message, &records.path)
                    .map_err(|error| DepthDecodeError::Records(error.to_string()))?;

                // DepthMetadataPlan reads record zero.
                // Therefore enforce a single record before
                // metadata decoding or book construction.
                if array.len() != 1 {
                    return Err(DepthDecodeError::Records(format!(
                        "expected exactly one depth record, got {}",
                        array.len(),
                    )));
                }

                let record = &array[0];

                if !record.is_object() {
                    return Err(DepthDecodeError::Records(
                        "depth record must be an object".into(),
                    ));
                }

                Ok(record)
            }

            other => Err(DepthDecodeError::Configuration(format!(
                "unsupported depth record representation {other:?}"
            ))),
        }
    }

    fn validate_record_identity(
        &self,
        record: &Value,
        context: &NormalizationContext,
    ) -> Result<(), DepthDecodeError> {
        if !record.is_object() {
            return Err(DepthDecodeError::Records(
                "native depth record must be an object".into(),
            ));
        }

        // Native field mappings are supplied by the depth TOML.
        //
        // They are relative to the extracted depth record when
        // [records] is configured, or to the message root
        // otherwise.
        //
        // Validate every declared native instrument identity,
        // not merely the first one.

        for logical_name in ["symbol", "instrument"] {
            let Some(path) = self.config.fields.get(logical_name) else {
                continue;
            };

            let native = JsonExtractor::string(record, path).map_err(|error| {
                DepthDecodeError::Records(format!("native {logical_name} at '{path}': {error}"))
            })?;

            if native != context.symbol() {
                return Err(DepthDecodeError::Records(format!(
                    "native {logical_name} mismatch: \
                 received '{native}', expected '{}'",
                    context.symbol(),
                )));
            }
        }

        Ok(())
    }

    // ========================================================
    // SNAPSHOT / DELTA CLASSIFICATION
    // ========================================================

    fn classify(
        &self,
        message: &Value,
        record: &Value,
    ) -> Result<DepthMessageKind, DepthDecodeError> {
        let cfg = &self.config.message;

        // ====================================================
        // MESSAGE TYPE (BYBIT-STYLE)
        // ====================================================

        if let Some(path) = cfg.message_type_field.as_deref() {
            let native = required_string(message, path)?;

            if cfg.snapshot_type.as_deref() == Some(native) {
                return Ok(DepthMessageKind::Snapshot);
            }

            if cfg.delta_type.as_deref() == Some(native) {
                return Ok(DepthMessageKind::Delta);
            }

            return Err(DepthDecodeError::Classification(format!(
                "unknown depth message type {native:?}"
            )));
        }

        // ====================================================
        // ACTION FIELD (BITGET / OKX-STYLE)
        // ====================================================

        if let Some(path) = cfg.action_field.as_deref() {
            let native = required_string(message, path)?;

            if cfg.snapshot_action.as_deref() == Some(native) {
                return Ok(DepthMessageKind::Snapshot);
            }

            if cfg.update_action.as_deref() == Some(native) {
                return Ok(DepthMessageKind::Delta);
            }

            return Err(DepthDecodeError::Classification(format!(
                "unknown depth action {native:?}"
            )));
        }

        // ====================================================
        // OPTIONAL FULL-SNAPSHOT DISCRIMINATOR
        // ====================================================

        if let Some(classification) = &self.config.classification {
            let logical = &classification.full_snapshot_field;

            let field = self.config.fields.get(logical).ok_or_else(|| {
                DepthDecodeError::Configuration(format!(
                    "missing snapshot discriminator mapping {logical:?}"
                ))
            })?;

            let value = JsonExtractor::optional(record, field)
                .map_err(|error| DepthDecodeError::Classification(error.to_string()))?;

            match value {
                Some(value) => {
                    let native = value.as_bool().ok_or_else(|| {
                        DepthDecodeError::Classification(
                            "full-snapshot discriminator must be boolean".into(),
                        )
                    })?;

                    if native == classification.full_snapshot_value {
                        return Ok(DepthMessageKind::Snapshot);
                    }

                    return Ok(DepthMessageKind::Delta);
                }

                None if classification.missing_full_means_delta => {
                    return Ok(DepthMessageKind::Delta);
                }

                None => {
                    return Err(DepthDecodeError::Classification(
                        "required full-snapshot discriminator is missing".into(),
                    ));
                }
            }
        }

        // Formats declaring only delta messages must
        // explicitly use a REST snapshot synchronization
        // strategy. Never infer snapshot authority from
        // an arbitrary native payload.
        if self.config.synchronization.snapshot_source == "rest" {
            return Ok(DepthMessageKind::Delta);
        }

        Err(DepthDecodeError::Classification(
            "no native snapshot/delta classification rule".into(),
        ))
    }

    // ========================================================
    // CANONICAL LEVEL
    // ========================================================

    fn canonical_level(
        &self,
        level: crate::live::infrastructure::normalization::depth::plan::NativeDepthLevel,
        side: BookSide,
        context: &NormalizationContext,
        exchange: Exchange,
        event_timestamp_ns: i64,
    ) -> Result<L2LevelUpdate, DepthDecodeError> {
        let quantities = QuantityNormalizer::normalize_level(
            level.quantity,
            level.price,
            context,
            self.precision,
        )
        .map_err(|error| DepthDecodeError::Quantity(error.to_string()))?;

        // Preserve the native quantity denomination and
        // any derived representations separately.
        //
        // Do not invent order counts when the exchange
        // does not provide them.
        Ok(L2LevelUpdate {
            envelope: EventEnvelope {
                event_timestamp_ns,

                // Receive time belongs to the transport
                // ingestion boundary, not the exchange.
                system_timestamp_ns: None,

                exchange,

                instrument_id: context.instrument_id(),

                symbol: context.symbol().to_owned(),

                stream_id: context.stream_id().to_owned(),
            },

            side,

            price: level.price,

            quantity_base: quantities.quantity_base,

            quantity_quote: quantities.quantity_quote,

            quantity_contracts: quantities.quantity_contracts,

            order_count: level.order_count,
        })
    }
}

// ============================================================
// CONFIGURATION VALIDATION
// ============================================================

fn validate_classification_configuration(
    config: &DepthFormatConfig,
) -> Result<(), DepthDecodeError> {
    let message = &config.message;

    if message.message_type_field.is_some()
        && (message.snapshot_type.is_none() || message.delta_type.is_none())
    {
        return Err(DepthDecodeError::Configuration(
            "message type classification requires both snapshot and delta values".into(),
        ));
    }

    if message.action_field.is_some()
        && (message.snapshot_action.is_none() || message.update_action.is_none())
    {
        return Err(DepthDecodeError::Configuration(
            "action classification requires both snapshot and update values".into(),
        ));
    }

    Ok(())
}

// ============================================================
// NATIVE FIELD HELPERS
// ============================================================

fn required_string<'a>(message: &'a Value, path: &str) -> Result<&'a str, DepthDecodeError> {
    JsonExtractor::string(message, path)
        .map_err(|error| DepthDecodeError::Envelope(error.to_string()))
}

fn render_topic(
    pattern: &str,
    symbol: &str,
    depth: Option<u64>,
) -> Result<String, DepthDecodeError> {
    let depth = match depth {
        Some(value) => value.to_string(),
        None if !pattern.contains("{depth}") => String::new(),

        None => {
            return Err(DepthDecodeError::Configuration(
                "topic requires subscription depth".into(),
            ));
        }
    };

    let rendered = pattern
        .replace("{symbol_upper}", &symbol.to_ascii_uppercase())
        .replace("{symbol_lower}", &symbol.to_ascii_lowercase())
        .replace("{symbol}", symbol)
        .replace("{depth}", &depth);

    if rendered.contains('{') || rendered.contains('}') {
        return Err(DepthDecodeError::Configuration(format!(
            "unsupported topic placeholder: {pattern:?}"
        )));
    }

    Ok(rendered)
}

// ============================================================
// LIVE EXCHANGE → CANONICAL EXCHANGE
// ============================================================

fn canonical_exchange(exchange: &ExchangeId) -> Exchange {
    match exchange {
        ExchangeId::Binance => Exchange::Binance,
        ExchangeId::Bybit => Exchange::Bybit,
        ExchangeId::Okx => Exchange::Okx,
        ExchangeId::Bitget => Exchange::Bitget,
        ExchangeId::Gateio => Exchange::GateIo,
    }
}
