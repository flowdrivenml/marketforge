use std::collections::BTreeMap;

use crate::live::config::format::{
    ContractNormalization, EnvelopeMappings, FieldMappings, FieldTypes, InstrumentConfig,
    MessageConfig, NormalizationConfig, OrderCountNormalization, PriceNormalization,
    QuantityNormalization, RecordsConfig, SideNormalization, TimestampConfig,
};

use crate::live::config::exchange::{ExchangeId, MarketType};

// ============================================================
// MESSAGE CONFIGURATION
// ============================================================

#[test]
fn deserialize_minimal_message() {
    let input = r#"
root = "data"
"#;

    let config: MessageConfig = toml::from_str(input).unwrap();

    assert_eq!(config.root, "data");
    assert!(config.event_type_field.is_none());
    assert!(config.topic_field.is_none());
}

#[test]
fn deserialize_message_with_optional_fields() {
    let input = r#"
root = "data"
event_type_field = "e"
event_type_value = "trade"
symbol_field = "s"
topic_field = "topic"
topic_pattern = "publicTrade.*"
payload_path = "data"
"#;

    let config: MessageConfig = toml::from_str(input).unwrap();

    assert_eq!(config.event_type_field.as_deref(), Some("e"));
    assert_eq!(config.event_type_value.as_deref(), Some("trade"));
    assert_eq!(config.symbol_field.as_deref(), Some("s"));
    assert_eq!(config.topic_field.as_deref(), Some("topic"));
    assert_eq!(config.topic_pattern.as_deref(), Some("publicTrade.*"));
    assert_eq!(config.payload_path.as_deref(), Some("data"));
}

#[test]
fn reject_unknown_message_field() {
    let input = r#"
root = "data"
unknown_property = true
"#;

    assert!(toml::from_str::<MessageConfig>(input).is_err());
}

// ============================================================
// RECORD EXTRACTION
// ============================================================

#[test]
fn deserialize_records_config() {
    let input = r#"
path = "data"
representation = "array"
record_type = "object"
expected_records_per_message = 1
preserve_array_order = true
"#;

    let config: RecordsConfig = toml::from_str(input).unwrap();

    assert_eq!(config.path, "data");
    assert_eq!(config.representation, "array");
    assert_eq!(config.record_type, "object");
    assert_eq!(config.expected_records_per_message, Some(1));
    assert_eq!(config.preserve_array_order, Some(true));
}

#[test]
fn reject_missing_records_path() {
    let input = r#"
representation = "array"
record_type = "object"
"#;

    assert!(toml::from_str::<RecordsConfig>(input).is_err());
}

#[test]
fn reject_unknown_records_field() {
    let input = r#"
path = "data"
representation = "array"
record_type = "object"
unknown = 123
"#;

    assert!(toml::from_str::<RecordsConfig>(input).is_err());
}

// ============================================================
// FIELD MAPPINGS
// ============================================================

#[test]
fn deserialize_field_mappings() {
    let input = r#"
price = "p"
quantity = "q"
trade_id = "t"
"#;

    let fields: FieldMappings = toml::from_str(input).unwrap();

    assert_eq!(fields["price"], "p");
    assert_eq!(fields["quantity"], "q");
    assert_eq!(fields["trade_id"], "t");
}

#[test]
fn deserialize_field_types() {
    let input = r#"
price = "decimal_string"
quantity = "decimal_string"
trade_id = "u64"
"#;

    let types: FieldTypes = toml::from_str(input).unwrap();

    assert_eq!(types["price"], "decimal_string");
    assert_eq!(types["quantity"], "decimal_string");
    assert_eq!(types["trade_id"], "u64");
}

#[test]
fn reject_non_string_field_mapping() {
    let input = r#"
price = 123
"#;

    assert!(toml::from_str::<FieldMappings>(input).is_err());
}

#[test]
fn field_mapping_round_trip() {
    let mut fields = BTreeMap::new();

    fields.insert("price".to_string(), "p".to_string());
    fields.insert("quantity".to_string(), "q".to_string());

    let serialized = toml::to_string(&fields).unwrap();

    let restored: FieldMappings = toml::from_str(&serialized).unwrap();

    assert_eq!(fields, restored);
}

#[test]
fn deserialize_envelope_mappings() {
    let input = r#"
instrument = "arg.instId"
channel = "arg.channel"
"#;

    let envelope: EnvelopeMappings = toml::from_str(input).unwrap();

    assert_eq!(envelope["instrument"], "arg.instId");
    assert_eq!(envelope["channel"], "arg.channel");
}

// ============================================================
// TIMESTAMP CONFIGURATION
// ============================================================

#[test]
fn deserialize_timestamps() {
    let input = r#"
event_source = "trade_timestamp"
source_unit = "milliseconds"
canonical_unit = "nanoseconds"
parse_integer_string = true
preserve_exchange_event_timestamp = true
"#;

    let config: TimestampConfig = toml::from_str(input).unwrap();

    assert_eq!(config.event_source, "trade_timestamp");
    assert_eq!(config.source_unit, "milliseconds");
    assert_eq!(config.canonical_unit, "nanoseconds");
    assert_eq!(config.parse_integer_string, Some(true));
    assert_eq!(config.preserve_exchange_event_timestamp, Some(true));
}

#[test]
fn reject_missing_timestamp_source() {
    let input = r#"
source_unit = "milliseconds"
canonical_unit = "nanoseconds"
"#;

    assert!(toml::from_str::<TimestampConfig>(input).is_err());
}

#[test]
fn reject_unknown_timestamp_field() {
    let input = r#"
event_source = "timestamp"
source_unit = "milliseconds"
canonical_unit = "nanoseconds"
unknown = true
"#;

    assert!(toml::from_str::<TimestampConfig>(input).is_err());
}

// ============================================================
// PRICE NORMALIZATION
// ============================================================

#[test]
fn deserialize_price_normalization() {
    let input = r#"
source = "price"
representation = "decimal_string"
allow_zero = false
allow_negative = false
"#;

    let config: PriceNormalization = toml::from_str(input).unwrap();

    assert_eq!(config.source.as_deref(), Some("price"));
    assert_eq!(config.representation, "decimal_string");
    assert!(!config.allow_zero);
    assert!(!config.allow_negative);
}

#[test]
fn reject_missing_price_representation() {
    let input = r#"
allow_zero = false
allow_negative = false
"#;

    assert!(toml::from_str::<PriceNormalization>(input).is_err());
}

// ============================================================
// QUANTITY NORMALIZATION
// ============================================================

#[test]
fn deserialize_quantity_normalization() {
    let input = r#"
source = "quantity"
source_unit = "base"
representation = "decimal_string"
allow_zero = true
update_mode = "absolute"
zero_quantity_action = "delete"
"#;

    let config: QuantityNormalization = toml::from_str(input).unwrap();

    assert_eq!(config.source.as_deref(), Some("quantity"));
    assert_eq!(config.source_unit.as_deref(), Some("base"));
    assert_eq!(config.representation, "decimal_string");
    assert!(config.allow_zero);
    assert_eq!(config.update_mode.as_deref(), Some("absolute"));
    assert_eq!(config.zero_quantity_action.as_deref(), Some("delete"));
}

#[test]
fn reject_missing_quantity_representation() {
    let input = r#"
allow_zero = true
"#;

    assert!(toml::from_str::<QuantityNormalization>(input).is_err());
}

// ============================================================
// SIDE NORMALIZATION
// ============================================================

#[test]
fn deserialize_side_normalization() {
    let input = r#"
source = "buyer_is_maker"
strategy = "boolean_mapping"
unknown_policy = "reject"
true = "sell"
false = "buy"
"#;

    let config: SideNormalization = toml::from_str(input).unwrap();

    assert_eq!(config.source.as_deref(), Some("buyer_is_maker"));
    assert_eq!(config.strategy.as_deref(), Some("boolean_mapping"));
    assert_eq!(config.mappings["true"], "sell");
    assert_eq!(config.mappings["false"], "buy");
}

#[test]
fn side_normalization_preserves_dynamic_mappings() {
    let input = r#"
source = "side"
strategy = "value_mapping"
Buy = "buy"
Sell = "sell"
"#;

    let config: SideNormalization = toml::from_str(input).unwrap();

    let serialized = toml::to_string(&config).unwrap();

    let original: toml::Value = toml::from_str(input).unwrap();
    let reconstructed: toml::Value = toml::from_str(&serialized).unwrap();

    assert_eq!(original, reconstructed);
}

// ============================================================
// INSTRUMENT RESOLUTION
// ============================================================

#[test]
fn deserialize_instrument_config() {
    let input = r#"
source = "symbol"
resolver = "exchange_instrument_registry"
exchange = "binance"
market = "spot"
require_known_instrument = true
validate_subscription_symbol = true
"#;

    let config: InstrumentConfig = toml::from_str(input).unwrap();

    assert_eq!(config.source, "symbol");
    assert_eq!(config.resolver, "exchange_instrument_registry");
    assert_eq!(config.exchange, ExchangeId::Binance);
    assert_eq!(config.market, MarketType::Spot);
    assert!(config.require_known_instrument);
    assert_eq!(config.validate_subscription_symbol, Some(true));
}

#[test]
fn reject_unknown_instrument_exchange() {
    let input = r#"
source = "symbol"
resolver = "exchange_instrument_registry"
exchange = "unknown"
market = "spot"
require_known_instrument = true
"#;

    assert!(toml::from_str::<InstrumentConfig>(input).is_err());
}

#[test]
fn reject_unknown_instrument_field() {
    let input = r#"
source = "symbol"
resolver = "exchange_instrument_registry"
exchange = "binance"
market = "spot"
require_known_instrument = true
unexpected = true
"#;

    assert!(toml::from_str::<InstrumentConfig>(input).is_err());
}

// ============================================================
// OPTIONAL NORMALIZATION COMPONENTS
// ============================================================

#[test]
fn deserialize_order_count_normalization() {
    let input = r#"
source = "order_count"
preserve = true
allow_zero = true
allow_negative = false
"#;

    let config: OrderCountNormalization = toml::from_str(input).unwrap();

    assert_eq!(config.source, "order_count");
    assert!(config.preserve);
    assert!(config.allow_zero);
    assert!(!config.allow_negative);
}

#[test]
fn deserialize_contract_normalization() {
    let input = r#"
resolver = "instrument_metadata"
require_contract_value = true
require_contract_value_asset = true
derive_base_quantity = true
derive_quote_quantity = true
"#;

    let config: ContractNormalization = toml::from_str(input).unwrap();

    assert_eq!(config.resolver, "instrument_metadata");
    assert!(config.require_contract_value);
    assert!(config.require_contract_value_asset);
    assert!(config.derive_base_quantity);
    assert!(config.derive_quote_quantity);
}

#[test]
fn deserialize_complete_normalization() {
    let input = r#"
[price]
source = "price"
representation = "decimal_string"
allow_zero = false
allow_negative = false

[quantity]
source = "quantity"
representation = "decimal_string"
allow_zero = true

[side]
source = "side"
strategy = "value_mapping"
Buy = "buy"
Sell = "sell"
"#;

    let config: NormalizationConfig = toml::from_str(input).unwrap();

    assert_eq!(config.price.source.as_deref(), Some("price"));
    assert_eq!(config.quantity.source.as_deref(), Some("quantity"));
    assert!(config.side.is_some());
    assert!(config.order_count.is_none());
    assert!(config.contract.is_none());
}

#[test]
fn reject_unknown_normalization_section() {
    let input = r#"
[price]
representation = "decimal_string"
allow_zero = false
allow_negative = false

[quantity]
representation = "decimal_string"
allow_zero = true

[unknown]
enabled = true
"#;

    assert!(toml::from_str::<NormalizationConfig>(input).is_err());
}
