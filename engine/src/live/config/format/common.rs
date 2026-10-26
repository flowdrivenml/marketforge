use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::live::config::exchange::{ExchangeId, MarketType};

// ============================================================
// MESSAGE IDENTIFICATION
// ============================================================

/// Identifies exchange messages and locates their payloads.
///
/// Supports Binance event types, Bybit topics, Bitget topics,
/// Gate.io channels, and OKX channels.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MessageConfig {
    pub root: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_type_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_type_value: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub symbol_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub symbol_source: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_value: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_pattern: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel_value: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_value: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_type_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_type_value: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_type: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delta_type: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instrument_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instrument_type_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instrument_type_value: Option<String>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub supported_instrument_types: Vec<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_action: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub update_action: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload_path: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub records: Option<String>,
}

// ============================================================
// RECORD EXTRACTION
// ============================================================

/// Describes how individual records are extracted from
/// an exchange message.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordsConfig {
    pub path: String,

    pub representation: String,

    pub record_type: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_records_per_message: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_records_per_message: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preserve_array_order: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preserve_source_order: Option<bool>,
}

// ============================================================
// FIELD MAPPINGS
// ============================================================

/// Maps canonical field names to exchange-native JSON paths.
///
/// Example:
///
/// [fields]
/// trade_id = "t"
/// price = "p"
/// trade_timestamp = "T"
///
/// The keys are intentionally dynamic because each exchange
/// exposes a different set of native fields.
pub type FieldMappings = BTreeMap<String, String>;

/// Maps canonical field names to native value representations.
///
/// Example:
///
/// [types]
/// trade_id = "u64"
/// price = "decimal_string"
/// trade_timestamp = "i64"
pub type FieldTypes = BTreeMap<String, String>;

/// Additional exchange-envelope field mappings.
pub type EnvelopeMappings = BTreeMap<String, String>;

// ============================================================
// TIMESTAMP NORMALIZATION
// ============================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimestampConfig {
    pub event_source: String,

    pub source_unit: String,

    pub canonical_unit: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parse_integer_string: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parse_strategy: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preserve_exchange_event_timestamp: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preserve_message_timestamp: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preserve_secondary_timestamp: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preserve_submillisecond_precision: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exchange_event_timestamp_source: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secondary_timestamp_source: Option<String>,
}

// ============================================================
// PRICE NORMALIZATION
// ============================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PriceNormalization {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,

    pub representation: String,

    pub allow_zero: bool,

    pub allow_negative: bool,
}

// ============================================================
// QUANTITY NORMALIZATION
// ============================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuantityNormalization {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_unit: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw_unit: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quantity_semantics: Option<String>,

    pub representation: String,

    pub allow_zero: bool,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allow_negative: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signed: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub absolute_value: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub derive_base_quantity: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub derive_quote_quantity: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rpi_excluded_quantity_source: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub update_mode: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub zero_quantity_action: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reject_precision_loss: Option<bool>,
}

// ============================================================
// AGGRESSOR SIDE
// ============================================================

/// Maps native side indicators to canonical buy/sell values.
///
/// Uses a flattened mapping because exchanges encode side
/// information differently:
///
/// Binance: true/false
/// Bybit: Buy/Sell
/// Gate.io Futures: positive/negative
///
/// The mapping values are preserved exactly.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SideNormalization {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub strategy: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unknown_policy: Option<String>,

    #[serde(flatten)]
    pub mappings: BTreeMap<String, String>,
}

// ============================================================
// INSTRUMENT RESOLUTION
// ============================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstrumentConfig {
    pub source: String,

    pub resolver: String,

    pub exchange: ExchangeId,

    pub market: MarketType,

    pub require_known_instrument: bool,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validate_subscription_symbol: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_category_source: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validate_native_category: Option<bool>,
}

// ============================================================
// SHARED NORMALIZATION CONTAINER
// ============================================================

/// Shared normalization sections.
///
/// Trade-specific side normalization and depth-specific
/// order-count/contract normalization are optional.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NormalizationConfig {
    pub price: PriceNormalization,

    pub quantity: QuantityNormalization,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub side: Option<SideNormalization>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub order_count: Option<OrderCountNormalization>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contract: Option<ContractNormalization>,
}

// ============================================================
// ORDER COUNT NORMALIZATION
// ============================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrderCountNormalization {
    pub source: String,

    pub preserve: bool,

    pub allow_zero: bool,

    pub allow_negative: bool,
}

// ============================================================
// CONTRACT NORMALIZATION
// ============================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContractNormalization {
    pub resolver: String,

    pub require_contract_value: bool,

    pub require_contract_value_asset: bool,

    pub derive_base_quantity: bool,

    pub derive_quote_quantity: bool,
}

#[cfg(test)]
mod tests;
