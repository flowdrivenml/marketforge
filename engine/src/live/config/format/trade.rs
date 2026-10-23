use std::{collections::BTreeMap, path::Path};

use serde::{Deserialize, Serialize};

use crate::live::config::{error::ConfigError, loader::load_toml};

use super::{
    common::{
        EnvelopeMappings, FieldMappings, FieldTypes, InstrumentConfig, MessageConfig,
        NormalizationConfig, RecordsConfig, TimestampConfig,
    },
    identity::{FormatIdentity, FormatKind},
};

// ============================================================
// TRADE FORMAT CONFIGURATION
// ============================================================

/// Complete configuration for an exchange-native trade format.
///
/// Describes how exchange WebSocket trade messages should
/// eventually be interpreted and normalized.
///
/// This structure does not perform event decoding.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TradeFormatConfig {
    pub schema_version: u32,

    pub format: FormatIdentity,

    pub message: MessageConfig,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub records: Option<RecordsConfig>,

    pub fields: FieldMappings,

    pub types: FieldTypes,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub envelope: Option<EnvelopeMappings>,

    pub timestamps: TimestampConfig,

    pub normalization: NormalizationConfig,

    pub instrument: InstrumentConfig,

    pub identity: TradeIdentityConfig,

    pub integrity: TradeIntegrityConfig,

    pub output: TradeOutputConfig,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aggregation: Option<TradeAggregationConfig>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attributes: Option<BTreeMap<String, TradeAttributeConfig>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sequence: Option<TradeSequenceConfig>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub symbol_type: Option<SymbolTypeConfig>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_initialization: Option<TradeStreamInitializationConfig>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unified_trade: Option<UnifiedTradeConfig>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protocol: Option<TradeProtocolConfig>,
}

// ============================================================
// AGGREGATION
// ============================================================

/// Exchange-native trade aggregation semantics.
///
/// Aggregated executions must not be expanded into
/// fabricated individual trades.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TradeAggregationConfig {
    #[serde(rename = "type")]
    pub kind: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aggregation_window_ms: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preserve_aggregate_id: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preserve_trade_id_range: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expand_to_individual_trades: Option<bool>,
}

// ============================================================
// NATIVE TRADE IDENTITY
// ============================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TradeIdentityConfig {
    pub strategy: String,

    pub source: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preserve_native_id: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preserve_exchange_trade_id: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preserve_market_trade_id: Option<bool>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub deduplication_key: Vec<String>,
}

// ============================================================
// TRADE ATTRIBUTES
// ============================================================

/// Optional exchange-native trade attribute.
///
/// Examples:
/// - Bybit block_trade
/// - Bybit rpi_trade
/// - Bitget execution_link_id
/// - Gate.io is_internal
/// - OKX trade_source
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TradeAttributeConfig {
    pub source: String,

    pub preserve: bool,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub missing_policy: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub true_value: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub false_value: Option<String>,
}

// ============================================================
// TRADE SEQUENCE METADATA
// ============================================================

/// Optional native sequencing information.
///
/// Trade sequence metadata must not automatically be
/// interpreted as consecutive trade IDs.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TradeSequenceConfig {
    pub strategy: String,

    pub source: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preserve_native_sequence: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub require_consecutive: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allow_equal_values: Option<bool>,
}

// ============================================================
// SYMBOL TYPE
// ============================================================

/// Optional Binance symbol-type validation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SymbolTypeConfig {
    pub source: String,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub expected_values: Vec<u64>,

    pub missing_policy: String,

    pub mismatch_policy: String,
}

// ============================================================
// INITIAL STREAM HANDLING
// ============================================================

/// Controls how initial exchange trade snapshots are
/// distinguished from subsequent live updates.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TradeStreamInitializationConfig {
    pub strategy: String,

    pub action_source: String,

    pub snapshot_action: String,

    pub update_action: String,

    pub snapshot_policy: String,
}

// ============================================================
// UNIFIED TRADE SEMANTICS
// ============================================================

/// Gate.io Spot unified trading-pair metadata.
///
/// The actual base and quote assets may differ from
/// the nominal subscribed currency pair.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UnifiedTradeConfig {
    pub mode_source: String,

    pub actual_base_source: String,

    pub actual_quote_source: String,

    pub regular_mode: u64,

    pub preserve_trade_mode: bool,

    pub preserve_actual_assets: bool,

    pub nonregular_mode_policy: String,
}

// ============================================================
// PROTOCOL REQUIREMENTS
// ============================================================

/// Transport-level requirements declared by a trade format.
///
/// These must eventually be enforced when the physical
/// WebSocket connection is established.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TradeProtocolConfig {
    pub required_header: String,

    pub required_header_value: String,

    pub require_decimal_size_support: bool,
}

// ============================================================
// TRADE INTEGRITY
// ============================================================

/// Declarative integrity checks for trade processing.
///
/// These settings describe required behavior but do not
/// implement the checks themselves.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TradeIntegrityConfig {
    pub reject_malformed_json: bool,

    pub reject_missing_required_fields: bool,

    pub reject_invalid_numeric_values: bool,

    pub reject_nonpositive_price: bool,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reject_nonpositive_quantity: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reject_zero_quantity: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reject_precision_loss: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detect_duplicate_trade_ids: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detect_trade_id_regression: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detect_trade_id_gaps: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detect_duplicate_aggregate_ids: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detect_aggregate_id_regression: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detect_aggregate_id_gaps: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validate_trade_id_range: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detect_cross_sequence_regression: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detect_market_trade_id_regression: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detect_market_trade_id_gaps: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validate_timestamp_consistency: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validate_unified_trade_assets: Option<bool>,

    pub gap_action: String,
}

// ============================================================
// CANONICAL TRADE OUTPUT
// ============================================================

/// Declares which native trade metadata should be
/// preserved in canonical output.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TradeOutputConfig {
    pub event_type: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trade_semantics: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preserve_native_trade_id: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preserve_native_aggregate_id: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preserve_native_trade_id_range: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preserve_native_sequence: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preserve_market_trade_id: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preserve_execution_link_id: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preserve_rpi_flag: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preserve_price_direction: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preserve_block_trade_flag: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preserve_rpi_trade_flag: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preserve_internal_trade_flag: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preserve_trade_source: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preserve_trade_range: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preserve_trade_mode: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preserve_actual_assets: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preserve_event_timestamp: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preserve_exchange_event_timestamp: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preserve_source_provenance: Option<bool>,
}

// ============================================================
// LOADING
// ============================================================

impl TradeFormatConfig {
    /// Load a trade format from a TOML file.
    ///
    /// Uses the existing generic configuration loader.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();

        let config: Self = load_toml(path)?;

        config
            .validate()
            .map_err(|reason| ConfigError::Validation {
                path: path.to_path_buf(),
                reason,
            })?;

        Ok(config)
    }

    /// Fully qualified format identifier.
    ///
    /// Example: trades/binance_spot
    pub fn qualified_id(&self) -> String {
        self.format.qualified_id(FormatKind::Trades)
    }

    /// Validate trade format identity and basic structure.
    ///
    /// Detailed cross-field validation will be implemented
    /// in format/validation.rs.
    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != 1 {
            return Err(format!(
                "unsupported trade format schema version {}",
                self.schema_version
            ));
        }

        self.format.validate(FormatKind::Trades)?;

        if self.fields.is_empty() {
            return Err("trade format requires field mappings".into());
        }

        if self.types.is_empty() {
            return Err("trade format requires field types".into());
        }

        if !self.fields.contains_key("price") {
            return Err("trade format requires a price field".into());
        }

        if !self.types.contains_key("price") {
            return Err("trade format requires a price type".into());
        }

        if self.timestamps.canonical_unit != "nanoseconds" {
            return Err("trade format canonical timestamp unit must be nanoseconds".into());
        }

        if self.instrument.exchange != self.format.exchange {
            return Err("trade format instrument exchange does not match format identity".into());
        }

        if self.instrument.market != self.format.market {
            return Err("trade format instrument market does not match format identity".into());
        }

        if self.output.event_type != "trade" {
            return Err(format!(
                "trade format output event_type must be 'trade', got '{}'",
                self.output.event_type
            ));
        }

        Ok(())
    }
}
