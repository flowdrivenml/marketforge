use std::{fs, path::Path};

use marketforge_engine::live::config::format::{
    SymbolTypeConfig, TradeAggregationConfig, TradeAttributeConfig, TradeFormatConfig,
    TradeIdentityConfig, TradeIntegrityConfig, TradeOutputConfig, TradeProtocolConfig,
    TradeSequenceConfig, TradeStreamInitializationConfig, UnifiedTradeConfig,
};

// ============================================================
// TEST HELPERS
// ============================================================

fn trade_directory() -> &'static Path {
    Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../data/.config/live/formats/trades"
    ))
}

fn load_trade(profile: &str) -> TradeFormatConfig {
    let path = trade_directory().join(format!("{profile}.toml"));

    TradeFormatConfig::load(&path)
        .unwrap_or_else(|error| panic!("Failed to load trade format '{}': {error}", path.display()))
}

fn parse<T: serde::de::DeserializeOwned>(input: &str) -> T {
    toml::from_str(input).expect("Failed to deserialize test TOML")
}

// ============================================================
// TRADE AGGREGATION
// ============================================================

#[test]
fn deserialize_trade_aggregation() {
    let input = r#"
type = "exchange_aggregated"
aggregation_window_ms = 100
preserve_aggregate_id = true
preserve_trade_id_range = true
expand_to_individual_trades = false
"#;

    let config: TradeAggregationConfig = parse(input);

    assert_eq!(config.kind, "exchange_aggregated");
    assert_eq!(config.aggregation_window_ms, Some(100));
    assert_eq!(config.preserve_aggregate_id, Some(true));
    assert_eq!(config.preserve_trade_id_range, Some(true));
    assert_eq!(config.expand_to_individual_trades, Some(false));
}

#[test]
fn deserialize_minimal_trade_aggregation() {
    let config: TradeAggregationConfig = parse(
        r#"
type = "none"
"#,
    );

    assert_eq!(config.kind, "none");
    assert!(config.aggregation_window_ms.is_none());
}

#[test]
fn reject_unknown_aggregation_field() {
    let input = r#"
type = "exchange_aggregated"
unknown = true
"#;

    assert!(toml::from_str::<TradeAggregationConfig>(input).is_err());
}

// ============================================================
// NATIVE TRADE IDENTITY
// ============================================================

#[test]
fn deserialize_trade_identity() {
    let input = r#"
strategy = "native_trade_id"
source = "trade_id"
preserve_native_id = true
deduplication_key = ["exchange", "market", "instrument", "trade_id"]
"#;

    let config: TradeIdentityConfig = parse(input);

    assert_eq!(config.strategy, "native_trade_id");
    assert_eq!(config.source, "trade_id");
    assert_eq!(config.preserve_native_id, Some(true));
    assert_eq!(config.deduplication_key.len(), 4);
}

#[test]
fn deserialize_trade_identity_without_optional_fields() {
    let config: TradeIdentityConfig = parse(
        r#"
strategy = "native_trade_id"
source = "trade_id"
"#,
    );

    assert_eq!(config.source, "trade_id");
    assert!(config.preserve_native_id.is_none());
    assert!(config.deduplication_key.is_empty());
}

#[test]
fn reject_missing_trade_identity_source() {
    let input = r#"
strategy = "native_trade_id"
"#;

    assert!(toml::from_str::<TradeIdentityConfig>(input).is_err());
}

#[test]
fn reject_unknown_trade_identity_field() {
    let input = r#"
strategy = "native_trade_id"
source = "trade_id"
unexpected = 123
"#;

    assert!(toml::from_str::<TradeIdentityConfig>(input).is_err());
}

// ============================================================
// TRADE ATTRIBUTES
// ============================================================

#[test]
fn deserialize_trade_attribute() {
    let input = r#"
source = "block_trade"
preserve = true
missing_policy = "false"
"#;

    let config: TradeAttributeConfig = parse(input);

    assert_eq!(config.source, "block_trade");
    assert!(config.preserve);
    assert_eq!(config.missing_policy.as_deref(), Some("false"));
}

#[test]
fn deserialize_trade_attribute_boolean_mapping() {
    let input = r#"
source = "rpi_trade"
preserve = true
true_value = "1"
false_value = "0"
"#;

    let config: TradeAttributeConfig = parse(input);

    assert_eq!(config.true_value.as_deref(), Some("1"));
    assert_eq!(config.false_value.as_deref(), Some("0"));
}

#[test]
fn reject_missing_attribute_source() {
    let input = r#"
preserve = true
"#;

    assert!(toml::from_str::<TradeAttributeConfig>(input).is_err());
}

// ============================================================
// TRADE SEQUENCE METADATA
// ============================================================

#[test]
fn deserialize_trade_sequence() {
    let input = r#"
strategy = "native_cross_sequence"
source = "cross_sequence"
preserve_native_sequence = true
require_consecutive = false
allow_equal_values = true
"#;

    let config: TradeSequenceConfig = parse(input);

    assert_eq!(config.strategy, "native_cross_sequence");
    assert_eq!(config.source, "cross_sequence");
    assert_eq!(config.preserve_native_sequence, Some(true));
    assert_eq!(config.require_consecutive, Some(false));
    assert_eq!(config.allow_equal_values, Some(true));
}

#[test]
fn reject_missing_sequence_strategy() {
    let input = r#"
source = "cross_sequence"
"#;

    assert!(toml::from_str::<TradeSequenceConfig>(input).is_err());
}

// ============================================================
// SYMBOL TYPE
// ============================================================

#[test]
fn deserialize_symbol_type() {
    let input = r#"
source = "symbol_type"
expected_values = [1, 2]
missing_policy = "reject"
mismatch_policy = "reject"
"#;

    let config: SymbolTypeConfig = parse(input);

    assert_eq!(config.source, "symbol_type");
    assert_eq!(config.expected_values, vec![1, 2]);
    assert_eq!(config.missing_policy, "reject");
    assert_eq!(config.mismatch_policy, "reject");
}

#[test]
fn deserialize_symbol_type_without_expected_values() {
    let input = r#"
source = "symbol_type"
missing_policy = "ignore"
mismatch_policy = "reject"
"#;

    let config: SymbolTypeConfig = parse(input);

    assert!(config.expected_values.is_empty());
}

// ============================================================
// STREAM INITIALIZATION
// ============================================================

#[test]
fn deserialize_stream_initialization() {
    let input = r#"
strategy = "snapshot_then_updates"
action_source = "action"
snapshot_action = "snapshot"
update_action = "update"
snapshot_policy = "ignore_initial_snapshot"
"#;

    let config: TradeStreamInitializationConfig = parse(input);

    assert_eq!(config.strategy, "snapshot_then_updates");
    assert_eq!(config.action_source, "action");
    assert_eq!(config.snapshot_action, "snapshot");
    assert_eq!(config.update_action, "update");
    assert_eq!(config.snapshot_policy, "ignore_initial_snapshot");
}

#[test]
fn reject_missing_stream_initialization_policy() {
    let input = r#"
strategy = "snapshot_then_updates"
action_source = "action"
snapshot_action = "snapshot"
update_action = "update"
"#;

    assert!(toml::from_str::<TradeStreamInitializationConfig>(input).is_err());
}

// ============================================================
// UNIFIED TRADE SEMANTICS
// ============================================================

#[test]
fn deserialize_unified_trade() {
    let input = r#"
mode_source = "trade_mode"
actual_base_source = "actual_base"
actual_quote_source = "actual_quote"
regular_mode = 0
preserve_trade_mode = true
preserve_actual_assets = true
nonregular_mode_policy = "preserve"
"#;

    let config: UnifiedTradeConfig = parse(input);

    assert_eq!(config.mode_source, "trade_mode");
    assert_eq!(config.actual_base_source, "actual_base");
    assert_eq!(config.actual_quote_source, "actual_quote");
    assert_eq!(config.regular_mode, 0);
    assert!(config.preserve_trade_mode);
    assert!(config.preserve_actual_assets);
    assert_eq!(config.nonregular_mode_policy, "preserve");
}

#[test]
fn reject_missing_unified_trade_mode() {
    let input = r#"
actual_base_source = "actual_base"
actual_quote_source = "actual_quote"
regular_mode = 0
preserve_trade_mode = true
preserve_actual_assets = true
nonregular_mode_policy = "preserve"
"#;

    assert!(toml::from_str::<UnifiedTradeConfig>(input).is_err());
}

// ============================================================
// PROTOCOL REQUIREMENTS
// ============================================================

#[test]
fn deserialize_trade_protocol() {
    let input = r#"
required_header = "X-Gate-Size-Decimal"
required_header_value = "1"
require_decimal_size_support = true
"#;

    let config: TradeProtocolConfig = parse(input);

    assert_eq!(config.required_header, "X-Gate-Size-Decimal");
    assert_eq!(config.required_header_value, "1");
    assert!(config.require_decimal_size_support);
}

#[test]
fn reject_unknown_trade_protocol_field() {
    let input = r#"
required_header = "X-Gate-Size-Decimal"
required_header_value = "1"
require_decimal_size_support = true
unexpected = true
"#;

    assert!(toml::from_str::<TradeProtocolConfig>(input).is_err());
}

// ============================================================
// TRADE INTEGRITY
// ============================================================

fn minimal_integrity_toml() -> &'static str {
    r#"
reject_malformed_json = true
reject_missing_required_fields = true
reject_invalid_numeric_values = true
reject_nonpositive_price = true
gap_action = "mark_gap"
"#
}

#[test]
fn deserialize_minimal_trade_integrity() {
    let config: TradeIntegrityConfig = parse(minimal_integrity_toml());

    assert!(config.reject_malformed_json);
    assert!(config.reject_missing_required_fields);
    assert!(config.reject_invalid_numeric_values);
    assert!(config.reject_nonpositive_price);
    assert_eq!(config.gap_action, "mark_gap");
}

#[test]
fn deserialize_extended_trade_integrity() {
    let input = r#"
reject_malformed_json = true
reject_missing_required_fields = true
reject_invalid_numeric_values = true
reject_nonpositive_price = true
reject_zero_quantity = true
reject_precision_loss = true
detect_duplicate_trade_ids = true
detect_trade_id_regression = true
detect_trade_id_gaps = false
validate_timestamp_consistency = true
gap_action = "mark_gap"
"#;

    let config: TradeIntegrityConfig = parse(input);

    assert_eq!(config.reject_zero_quantity, Some(true));
    assert_eq!(config.reject_precision_loss, Some(true));
    assert_eq!(config.detect_duplicate_trade_ids, Some(true));
    assert_eq!(config.detect_trade_id_regression, Some(true));
    assert_eq!(config.detect_trade_id_gaps, Some(false));
    assert_eq!(config.validate_timestamp_consistency, Some(true));
}

#[test]
fn reject_missing_trade_integrity_gap_action() {
    let input = r#"
reject_malformed_json = true
reject_missing_required_fields = true
reject_invalid_numeric_values = true
reject_nonpositive_price = true
"#;

    assert!(toml::from_str::<TradeIntegrityConfig>(input).is_err());
}

#[test]
fn reject_unknown_trade_integrity_field() {
    let input = format!("{}\nunknown = true\n", minimal_integrity_toml());

    assert!(toml::from_str::<TradeIntegrityConfig>(&input).is_err());
}

// ============================================================
// CANONICAL TRADE OUTPUT
// ============================================================

#[test]
fn deserialize_minimal_trade_output() {
    let input = r#"
event_type = "trade"
"#;

    let config: TradeOutputConfig = parse(input);

    assert_eq!(config.event_type, "trade");
    assert!(config.trade_semantics.is_none());
    assert!(config.preserve_native_trade_id.is_none());
}

#[test]
fn deserialize_extended_trade_output() {
    let input = r#"
event_type = "trade"
trade_semantics = "individual_execution"
preserve_native_trade_id = true
preserve_native_sequence = true
preserve_event_timestamp = true
preserve_source_provenance = true
"#;

    let config: TradeOutputConfig = parse(input);

    assert_eq!(
        config.trade_semantics.as_deref(),
        Some("individual_execution")
    );

    assert_eq!(config.preserve_native_trade_id, Some(true));
    assert_eq!(config.preserve_native_sequence, Some(true));
    assert_eq!(config.preserve_event_timestamp, Some(true));
    assert_eq!(config.preserve_source_provenance, Some(true));
}

#[test]
fn reject_missing_trade_output_event_type() {
    let input = r#"
preserve_native_trade_id = true
"#;

    assert!(toml::from_str::<TradeOutputConfig>(input).is_err());
}

// ============================================================
// REAL TRADE FORMAT CONFIGURATIONS
// ============================================================

#[test]
fn load_all_real_trade_formats() {
    let mut tested = 0;

    for entry in fs::read_dir(trade_directory()).expect("Cannot read trade format directory") {
        let entry = entry.expect("Cannot read directory entry");
        let path = entry.path();

        if !path.is_file() || path.extension().is_none_or(|extension| extension != "toml") {
            continue;
        }

        let config = TradeFormatConfig::load(&path)
            .unwrap_or_else(|error| panic!("Failed to load {}: {error}", path.display()));

        assert_eq!(config.schema_version, 1);
        assert_eq!(config.output.event_type, "trade");

        config.validate().unwrap();

        println!("PASS: {}", config.qualified_id());

        tested += 1;
    }

    assert_eq!(tested, 15, "Expected 15 trade formats");
}

#[test]
fn trade_format_qualified_identity() {
    let config = load_trade("binance_spot");

    assert_eq!(config.qualified_id(), "trades/binance_spot");
}

#[test]
fn trade_format_identity_matches_instrument() {
    let config = load_trade("bybit_linear");

    assert_eq!(config.format.exchange, config.instrument.exchange);

    assert_eq!(config.format.market, config.instrument.market);
}

#[test]
fn trade_format_requires_price_field() {
    let mut config = load_trade("binance_spot");

    config.fields.remove("price");

    let error = config.validate().unwrap_err();

    assert!(
        error.contains("price field"),
        "Unexpected validation error: {error}"
    );
}

#[test]
fn trade_format_requires_price_type() {
    let mut config = load_trade("binance_spot");

    config.types.remove("price");

    let error = config.validate().unwrap_err();

    assert!(
        error.contains("price type"),
        "Unexpected validation error: {error}"
    );
}

#[test]
fn trade_format_rejects_wrong_output_type() {
    let mut config = load_trade("binance_spot");

    config.output.event_type = "l2_update".into();

    let error = config.validate().unwrap_err();

    assert!(
        error.contains("event_type"),
        "Unexpected validation error: {error}"
    );
}

#[test]
fn trade_format_rejects_instrument_exchange_mismatch() {
    use marketforge_engine::live::config::exchange::ExchangeId;

    let mut config = load_trade("binance_spot");

    config.instrument.exchange = ExchangeId::Bybit;

    let error = config.validate().unwrap_err();

    assert!(
        error.contains("instrument exchange"),
        "Unexpected validation error: {error}"
    );
}

#[test]
fn trade_format_rejects_instrument_market_mismatch() {
    use marketforge_engine::live::config::exchange::MarketType;

    let mut config = load_trade("binance_spot");

    config.instrument.market = MarketType::Linear;

    let error = config.validate().unwrap_err();

    assert!(
        error.contains("instrument market"),
        "Unexpected validation error: {error}"
    );
}

#[test]
fn trade_format_rejects_unsupported_schema_version() {
    let mut config = load_trade("binance_spot");

    config.schema_version = 999;

    let error = config.validate().unwrap_err();

    assert!(
        error.contains("schema version"),
        "Unexpected validation error: {error}"
    );
}

#[test]
fn trade_format_rejects_wrong_canonical_timestamp_unit() {
    let mut config = load_trade("binance_spot");

    config.timestamps.canonical_unit = "milliseconds".into();

    let error = config.validate().unwrap_err();

    assert!(
        error.contains("canonical timestamp unit"),
        "Unexpected validation error: {error}"
    );
}
