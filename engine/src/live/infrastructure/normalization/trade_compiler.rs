use super::{
    AggressorSideNormalizer, NativeSideValue, NativeTradeFieldLayout, PositiveQuantitySide,
    QuantityPrecisionPolicy, SymbolTypeMissingPolicy, SymbolTypeValidator, TimestampUnit,
    TradeExtractionPlan, TradeFieldPlan, TradeRecordRepresentation, TradeRecordSemantics,
};
use super::{EnvelopeRule, EnvelopeValidator};
use crate::live::config::{
    exchange::{ExchangeId, MarketType},
    format::MessageConfig,
};
use crate::live::stream::request::QuantityType;
use crate::{canonical::TradeSide, live::config::format::TradeFormatConfig};
use serde_json::Value;
use std::collections::BTreeMap;
use thiserror::Error;
// ============================================================
// COMPILATION ERRORS
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum TradeCompileError {
    #[error("invalid trade format: {0}")]
    InvalidFormat(String),

    #[error("missing trade field mapping: {0}")]
    MissingField(String),

    #[error("unsupported trade configuration: {0}")]
    Unsupported(String),
}

// ============================================================
// AGGREGATE TRADE METADATA
// ============================================================

/// Native aggregate identifiers that must survive decoding.
///
/// These are field paths, not extracted values.
///
/// The eventual decoder must preserve all configured
/// identifiers rather than fabricate individual trades.
#[derive(Debug, Clone)]
pub struct AggregateTradeFieldPlan {
    pub aggregate_id: String,
    pub first_trade_id: String,
    pub last_trade_id: String,
}

// ============================================================
// COMPILED TRADE NORMALIZATION PLAN
// ============================================================

/// Immutable normalization instructions compiled from one
/// exchange-native trade format.
///
/// This structure contains no exchange-specific branching.
///
/// It is intentionally independent of instrument identity.
/// Economic quantity conversion uses NormalizationContext
/// at decoding time.
#[derive(Debug, Clone)]
pub struct CompiledTradeNormalizationPlan {
    pub format_id: String,

    pub extraction: TradeExtractionPlan,

    pub fields: TradeFieldPlan,

    pub side: AggressorSideNormalizer,

    pub semantics: TradeRecordSemantics,

    pub precision: QuantityPrecisionPolicy,

    pub aggregate_fields: Option<AggregateTradeFieldPlan>,

    pub declared_quantity_type: Option<QuantityType>,

    /// Native symbol path inside an individual trade record.
    ///
    /// When configured, the decoder must verify that this
    /// symbol matches the authoritative stream context.
    pub record_symbol_field: Option<String>,
    /// Exchange and market expected by this native format.
    pub expected_exchange: ExchangeId,
    pub expected_market: MarketType,

    /// Compiled native WebSocket message identity rules.
    pub envelope: EnvelopeValidator,
    /// Optional compiled native instrument discriminator.
    pub symbol_type: Option<SymbolTypeValidator>,
}

// ============================================================
// COMPILER
// ============================================================

pub struct TradeNormalizationCompiler;

impl TradeNormalizationCompiler {
    /// Compile an existing WebSocket TradeFormatConfig.
    ///
    /// REST configurations will use a separate extraction
    /// and field-layout adapter feeding the same runtime
    /// normalization components.
    pub fn compile_websocket(
        config: &TradeFormatConfig,
        max_records: usize,
        precision: QuantityPrecisionPolicy,
    ) -> Result<CompiledTradeNormalizationPlan, TradeCompileError> {
        config
            .validate()
            .map_err(TradeCompileError::InvalidFormat)?;

        precision
            .validate()
            .map_err(|error| TradeCompileError::InvalidFormat(error.to_string()))?;

        // ====================================================
        // TIMESTAMP
        // ====================================================

        let timestamp_unit =
            TimestampUnit::parse(&config.timestamps.source_unit).map_err(|_| {
                TradeCompileError::Unsupported(format!(
                    "timestamp unit '{}' requires \
                         a dedicated exact conversion strategy",
                    config.timestamps.source_unit,
                ))
            })?;

        // ====================================================
        // RECORD EXTRACTION
        // ====================================================

        let (path, representation) = if let Some(records) = &config.records {
            let representation = TradeRecordRepresentation::parse(&records.representation)
                .map_err(TradeCompileError::Unsupported)?;

            (records.path.clone(), representation)
        } else {
            // Existing single-record format declarations
            // use message.records = "single".
            match config.message.records.as_deref() {
                Some("single") => (
                    config.message.root.clone(),
                    TradeRecordRepresentation::Object,
                ),

                other => {
                    return Err(TradeCompileError::Unsupported(format!(
                        "missing records configuration; \
                                 message.records={other:?}"
                    )));
                }
            }
        };

        let extraction = TradeExtractionPlan::new(path, representation, max_records)
            .map_err(|error| TradeCompileError::InvalidFormat(error.to_string()))?;

        // ====================================================
        // NORMALIZED FIELD SOURCES
        // ====================================================

        let price_source = config
            .normalization
            .price
            .source
            .as_deref()
            .unwrap_or("price");

        let quantity_source = config
            .normalization
            .quantity
            .source
            .as_deref()
            .ok_or_else(|| {
                TradeCompileError::MissingField("normalization.quantity.source".into())
            })?;

        let timestamp_source = &config.timestamps.event_source;

        // ====================================================
        // NATIVE FIELD PATHS
        // ====================================================

        let price = field(config, price_source)?;

        let quantity = field(config, quantity_source)?;

        let timestamp = field(config, timestamp_source)?;

        let trade_id = Some(field(config, &config.identity.source)?);

        let sequence = config
            .sequence
            .as_ref()
            .map(|sequence| field(config, &sequence.source))
            .transpose()?;

        // ====================================================
        // SIDE STRATEGY
        // ====================================================

        let side_config = config
            .normalization
            .side
            .as_ref()
            .ok_or_else(|| TradeCompileError::MissingField("normalization.side".into()))?;

        let (side, side_field) = if side_config.strategy.as_deref() == Some("signed_quantity") {
            // Positive/negative mappings must be explicit.
            let positive = side_config.mappings.get("positive").ok_or_else(|| {
                TradeCompileError::MissingField("normalization.side.positive".into())
            })?;

            let negative = side_config.mappings.get("negative").ok_or_else(|| {
                TradeCompileError::MissingField("normalization.side.negative".into())
            })?;

            let positive_side = parse_canonical_side(positive)?;

            let negative_side = parse_canonical_side(negative)?;

            if positive_side == negative_side {
                return Err(TradeCompileError::InvalidFormat(
                    "positive and negative quantity \
                             mappings must represent opposite sides"
                        .into(),
                ));
            }

            let convention = match positive_side {
                TradeSide::Buy => PositiveQuantitySide::Buy,
                TradeSide::Sell => PositiveQuantitySide::Sell,
            };

            (AggressorSideNormalizer::signed_quantity(convention), None)
        } else {
            if side_config.strategy.is_some() && side_config.strategy.as_deref() != Some("mapped") {
                return Err(TradeCompileError::Unsupported(format!(
                    "side strategy {:?}",
                    side_config.strategy,
                )));
            }

            let source = side_config.source.as_deref().ok_or_else(|| {
                TradeCompileError::MissingField("normalization.side.source".into())
            })?;

            let source_path = field(config, source)?;

            let source_type = config
                .types
                .get(source)
                .ok_or_else(|| TradeCompileError::MissingField(format!("types.{source}")))?;

            let mut mappings = BTreeMap::new();

            for (native, canonical) in &side_config.mappings {
                if native == "unknown_policy" {
                    continue;
                }

                let native_value = match source_type.as_str() {
                    "boolean" => {
                        let value = match native.as_str() {
                            "true" => true,
                            "false" => false,
                            other => {
                                return Err(TradeCompileError::Unsupported(format!(
                                    "boolean side value '{other}'"
                                )));
                            }
                        };

                        NativeSideValue::Boolean(value)
                    }

                    "string" => NativeSideValue::String(native.clone()),

                    other => {
                        return Err(TradeCompileError::Unsupported(format!(
                            "mapped side field type '{other}'"
                        )));
                    }
                };

                mappings.insert(native_value, parse_canonical_side(canonical)?);
            }

            let normalizer = AggressorSideNormalizer::mapped(mappings)
                .map_err(|error| TradeCompileError::InvalidFormat(error.to_string()))?;

            (normalizer, Some(source_path))
        };

        // ====================================================
        // TYPED FIELD PLAN
        // ====================================================

        let layout = NativeTradeFieldLayout {
            trade_id,
            price,
            quantity,
            timestamp,
            side: side_field,
            sequence,
        };

        let fields = TradeFieldPlan::new(layout, timestamp_unit)
            .map_err(|error| TradeCompileError::InvalidFormat(error.to_string()))?;

        // ====================================================
        // TRADE SEMANTICS
        // ====================================================

        let (semantics, aggregate_fields) = if let Some(aggregation) = &config.aggregation {
            if aggregation.kind != "exchange_aggregated" {
                return Err(TradeCompileError::Unsupported(format!(
                    "aggregation type '{}'",
                    aggregation.kind,
                )));
            }

            if aggregation.expand_to_individual_trades == Some(true) {
                return Err(TradeCompileError::InvalidFormat(
                    "aggregate trades cannot be expanded \
                             into fabricated executions"
                        .into(),
                ));
            }

            let aggregate_fields = AggregateTradeFieldPlan {
                aggregate_id: field(config, "aggregate_trade_id")?,
                first_trade_id: field(config, "first_trade_id")?,
                last_trade_id: field(config, "last_trade_id")?,
            };

            (TradeRecordSemantics::Aggregated, Some(aggregate_fields))
        } else {
            (TradeRecordSemantics::Individual, None)
        };

        // ====================================================
        // RESULT
        // ====================================================
        //
        let declared_quantity_type = match config.normalization.quantity.raw_unit.as_deref() {
            Some("base_asset") => Some(QuantityType::Base),
            Some("quote_asset") => Some(QuantityType::Quote),
            Some("contracts") => Some(QuantityType::Contracts),

            None => None,

            Some(other) => {
                return Err(TradeCompileError::Unsupported(format!(
                    "native quantity unit '{other}'"
                )));
            }
        };
        // ========================================================
        // RECORD-LEVEL INSTRUMENT IDENTITY
        // ========================================================

        // Different formats name their native instrument fields
        // differently.
        //
        // All mappings originate from the existing format TOML.
        //
        // The message-level symbol field is used when the
        // individual trade record is also the message root,
        // as with Binance individual trades.

        let record_symbol_field = config
            .fields
            .get("symbol")
            .or_else(|| config.fields.get("instrument"))
            .or_else(|| config.fields.get("currency_pair"))
            .cloned()
            .or_else(|| {
                if config.message.records.as_deref() == Some("single") && config.message.root == "$"
                {
                    config.message.symbol_field.clone()
                } else {
                    None
                }
            });

        let envelope = compile_trade_envelope(&config.message)?;
        let symbol_type = config
            .symbol_type
            .as_ref()
            .map(|spec| {
                let path = field(config, &spec.source)?;

                let missing_policy = match spec.missing_policy.as_str() {
                    "allow" => SymbolTypeMissingPolicy::Allow,
                    "reject" => SymbolTypeMissingPolicy::Reject,

                    other => {
                        return Err(TradeCompileError::Unsupported(format!(
                            "symbol_type.missing_policy '{other}'"
                        )));
                    }
                };

                if spec.mismatch_policy != "reject" {
                    return Err(TradeCompileError::Unsupported(format!(
                        "symbol_type.mismatch_policy '{}'",
                        spec.mismatch_policy,
                    )));
                }

                SymbolTypeValidator::new(path, spec.expected_values.clone(), missing_policy)
                    .map_err(|error| TradeCompileError::InvalidFormat(error.to_string()))
            })
            .transpose()?;
        Ok(CompiledTradeNormalizationPlan {
            format_id: config.format.id.clone(),
            extraction,
            fields,
            side,
            semantics,
            precision,
            aggregate_fields,
            declared_quantity_type,
            record_symbol_field,
            expected_exchange: config.format.exchange,
            expected_market: config.format.market,
            envelope,
            symbol_type,
        })
    }
    // ============================================================
    // GENERIC WEBSOCKET ENVELOPE COMPILATION
    // ============================================================
}

// ============================================================
// FIELD MAPPING RESOLUTION
// ============================================================

fn field(config: &TradeFormatConfig, logical_name: &str) -> Result<String, TradeCompileError> {
    config
        .fields
        .get(logical_name)
        .cloned()
        .ok_or_else(|| TradeCompileError::MissingField(format!("fields.{logical_name}")))
}

// ============================================================
// CANONICAL SIDE PARSING
// ============================================================

fn parse_canonical_side(value: &str) -> Result<TradeSide, TradeCompileError> {
    match value {
        "buy" => Ok(TradeSide::Buy),
        "sell" => Ok(TradeSide::Sell),

        other => Err(TradeCompileError::Unsupported(format!(
            "canonical side '{other}'"
        ))),
    }
}

fn compile_trade_envelope(message: &MessageConfig) -> Result<EnvelopeValidator, TradeCompileError> {
    let mut rules = Vec::new();

    // ========================================================
    // EVENT TYPE
    // ========================================================

    push_exact(
        &mut rules,
        "event_type",
        message.event_type_field.as_deref(),
        message.event_type_value.as_deref(),
    )?;

    // ========================================================
    // CHANNEL
    // ========================================================

    push_exact(
        &mut rules,
        "channel",
        message.channel_field.as_deref(),
        message.channel_value.as_deref(),
    )?;

    // ========================================================
    // EVENT / ACTION
    // ========================================================

    push_exact(
        &mut rules,
        "event",
        message.event_field.as_deref(),
        message.event_value.as_deref(),
    )?;

    // ========================================================
    // MESSAGE TYPE
    // ========================================================

    push_exact(
        &mut rules,
        "message_type",
        message.message_type_field.as_deref(),
        message.message_type_value.as_deref(),
    )?;

    // ========================================================
    // TOPIC
    // ========================================================

    if message.topic_value.is_some() && message.topic_pattern.is_some() {
        return Err(TradeCompileError::InvalidFormat(
            "topic_value and topic_pattern cannot both \
             be configured"
                .into(),
        ));
    }

    if let Some(expected) = message.topic_value.as_deref() {
        let path = required_path("message.topic_field", message.topic_field.as_deref())?;

        rules.push(EnvelopeRule::Equals {
            path,
            expected: Value::String(expected.to_owned()),
        });
    }

    if let Some(template) = message.topic_pattern.as_deref() {
        let path = required_path("message.topic_field", message.topic_field.as_deref())?;

        rules.push(EnvelopeRule::TopicTemplate {
            path,
            template: template.to_owned(),
        });
    }

    // ========================================================
    // INSTRUMENT SYMBOL
    // ========================================================

    // Some formats declare more than one instrument source.
    // Validate every explicitly declared message-level
    // symbol path.
    //
    // Exact native-symbol matching is intentional.
    // Symbol aliases must be resolved by the instrument
    // catalog or subscription layer.

    if let Some(path) = message.symbol_field.as_deref() {
        rules.push(EnvelopeRule::Symbol {
            path: path.to_owned(),
        });
    }

    if let Some(path) = message.instrument_field.as_deref() {
        if Some(path) != message.symbol_field.as_deref() {
            rules.push(EnvelopeRule::Symbol {
                path: path.to_owned(),
            });
        }
    }

    // ========================================================
    // NATIVE INSTRUMENT CATEGORY
    // ========================================================

    if message.instrument_type_value.is_some() && !message.supported_instrument_types.is_empty() {
        return Err(TradeCompileError::InvalidFormat(
            "instrument_type_value conflicts with \
             supported_instrument_types"
                .into(),
        ));
    }

    if let Some(expected) = message.instrument_type_value.as_deref() {
        let path = required_path(
            "message.instrument_type_field",
            message.instrument_type_field.as_deref(),
        )?;

        rules.push(EnvelopeRule::Equals {
            path,
            expected: Value::String(expected.to_owned()),
        });
    }

    if !message.supported_instrument_types.is_empty() {
        let path = required_path(
            "message.instrument_type_field",
            message.instrument_type_field.as_deref(),
        )?;

        let accepted = message
            .supported_instrument_types
            .iter()
            .cloned()
            .map(Value::String)
            .collect();

        rules.push(EnvelopeRule::OneOf { path, accepted });
    }

    // ========================================================
    // REJECT DECLARATIONS WE DID NOT COMPILE
    // ========================================================

    // Fail closed if a declaration expects a field but
    // supplies no rule value, or vice versa.

    if message.topic_field.is_some()
        && message.topic_value.is_none()
        && message.topic_pattern.is_none()
    {
        return Err(TradeCompileError::InvalidFormat(
            "topic_field has no corresponding validation rule".into(),
        ));
    }

    if message.instrument_type_field.is_some()
        && message.instrument_type_value.is_none()
        && message.supported_instrument_types.is_empty()
    {
        return Err(TradeCompileError::InvalidFormat(
            "instrument_type_field has no allowed values".into(),
        ));
    }

    // The first version of the envelope compiler does
    // not interpret symbol_source = "topic" as a separate
    // field: the topic template itself already validates
    // the subscription symbol.
    //
    // Unsupported symbol-source strategies are rejected.

    if let Some(source) = message.symbol_source.as_deref() {
        if source != "topic" {
            return Err(TradeCompileError::Unsupported(format!(
                "message.symbol_source '{source}'"
            )));
        }

        if message.topic_pattern.is_none() {
            return Err(TradeCompileError::InvalidFormat(
                "symbol_source=topic requires topic_pattern".into(),
            ));
        }
    }

    // ========================================================
    // COMPILE IMMUTABLE VALIDATOR
    // ========================================================

    EnvelopeValidator::new(rules)
        .map_err(|error| TradeCompileError::InvalidFormat(format!("message envelope: {error}")))
}

// ============================================================
// EXACT FIELD RULE
// ============================================================

fn push_exact(
    rules: &mut Vec<EnvelopeRule>,
    name: &str,
    field: Option<&str>,
    expected: Option<&str>,
) -> Result<(), TradeCompileError> {
    match (field, expected) {
        (Some(path), Some(value)) => {
            rules.push(EnvelopeRule::Equals {
                path: path.to_owned(),
                expected: Value::String(value.to_owned()),
            });

            Ok(())
        }

        (None, None) => Ok(()),

        _ => Err(TradeCompileError::InvalidFormat(format!(
            "message {name} requires both \
                 field and expected value"
        ))),
    }
}

// ============================================================
// REQUIRED FIELD PATH
// ============================================================

fn required_path(name: &str, path: Option<&str>) -> Result<String, TradeCompileError> {
    let path = path.ok_or_else(|| TradeCompileError::MissingField(name.to_owned()))?;

    if path.trim().is_empty() {
        return Err(TradeCompileError::InvalidFormat(format!(
            "{name} cannot be empty"
        )));
    }

    Ok(path.to_owned())
}
