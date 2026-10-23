use std::path::Path;

use marketforge_engine::live::config::format::{
    DepthFormatConfig, TradeFormatConfig,
    validation::{validate_depth, validate_trade},
};

// ============================================================
// TEST HELPERS
// ============================================================

fn format_directory() -> &'static Path {
    Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../data/.config/live/formats"
    ))
}

fn load_trade(profile: &str) -> TradeFormatConfig {
    let path = format_directory()
        .join("trades")
        .join(format!("{profile}.toml"));

    TradeFormatConfig::load(&path)
        .unwrap_or_else(|error| panic!("Failed to load {}: {error}", path.display()))
}

fn load_depth(profile: &str) -> DepthFormatConfig {
    let path = format_directory()
        .join("depth")
        .join(format!("{profile}.toml"));

    DepthFormatConfig::load(&path)
        .unwrap_or_else(|error| panic!("Failed to load {}: {error}", path.display()))
}

fn assert_trade_error(config: &TradeFormatConfig, expected: &str) {
    let error = validate_trade(config).expect_err("Expected trade validation to fail");

    assert!(
        error.contains(expected),
        "Expected error containing '{expected}', got: {error}"
    );
}

fn assert_depth_error(config: &DepthFormatConfig, expected: &str) {
    let error = validate_depth(config).expect_err("Expected depth validation to fail");

    assert!(
        error.contains(expected),
        "Expected error containing '{expected}', got: {error}"
    );
}

// ============================================================
// TRADE FIELD VALIDATION
// ============================================================

#[test]
fn reject_trade_field_without_type() {
    let mut config = load_trade("binance_spot");

    config
        .fields
        .insert("test_field".into(), "native_test_field".into());

    assert_trade_error(&config, "field 'test_field' has no corresponding type");
}

#[test]
fn reject_trade_type_without_field() {
    let mut config = load_trade("binance_spot");

    config.types.insert("test_type".into(), "u64".into());

    assert_trade_error(&config, "type 'test_type' has no corresponding field");
}

#[test]
fn reject_missing_trade_timestamp_source() {
    let mut config = load_trade("binance_spot");

    config.timestamps.event_source = "nonexistent_timestamp".into();

    assert_trade_error(&config, "timestamps.event_source references missing field");
}

#[test]
fn reject_missing_trade_price_source() {
    let mut config = load_trade("binance_spot");

    config.normalization.price.source = Some("nonexistent_price".into());

    assert_trade_error(
        &config,
        "normalization.price.source references missing field",
    );
}

#[test]
fn reject_missing_trade_quantity_source() {
    let mut config = load_trade("binance_spot");

    config.normalization.quantity.source = Some("nonexistent_quantity".into());

    assert_trade_error(
        &config,
        "normalization.quantity.source references missing field",
    );
}

#[test]
fn reject_missing_trade_identity_source() {
    let mut config = load_trade("binance_spot");

    config.identity.source = "nonexistent_trade_id".into();

    assert_trade_error(&config, "identity.source references missing field");
}

#[test]
fn reject_empty_trade_deduplication_key() {
    let mut config = load_trade("binance_spot");

    config.identity.deduplication_key.clear();

    assert_trade_error(&config, "identity.deduplication_key cannot be empty");
}

// ============================================================
// TRADE SIDE NORMALIZATION
// ============================================================

#[test]
fn reject_missing_trade_side_source() {
    let mut config = load_trade("binance_spot");

    let side = config
        .normalization
        .side
        .as_mut()
        .expect("Missing side normalization");

    side.source = Some("nonexistent_side".into());

    assert_trade_error(
        &config,
        "normalization.side.source references missing field",
    );
}

#[test]
fn reject_empty_trade_side_mappings() {
    let mut config = load_trade("binance_spot");

    let side = config
        .normalization
        .side
        .as_mut()
        .expect("Missing side normalization");

    side.mappings.clear();

    assert_trade_error(&config, "normalization.side requires native side mappings");
}

// ============================================================
// TRADE AGGREGATION
// ============================================================

#[test]
fn reject_expanding_aggregated_trades() {
    let mut config = load_trade("binance_linear");

    let aggregation = config
        .aggregation
        .as_mut()
        .expect("Missing aggregation configuration");

    aggregation.kind = "exchange_aggregated".into();
    aggregation.expand_to_individual_trades = Some(true);

    assert_trade_error(&config, "cannot be expanded");
}

// ============================================================
// TRADE ATTRIBUTES
// ============================================================

#[test]
fn reject_missing_trade_attribute_source() {
    let mut config = load_trade("bybit_linear");

    let attributes = config
        .attributes
        .as_mut()
        .expect("Missing trade attributes");

    let attribute = attributes
        .values_mut()
        .next()
        .expect("No trade attributes available");

    attribute.source = "nonexistent_attribute".into();

    assert_trade_error(&config, "references missing field");
}

// ============================================================
// TRADE SEQUENCE
// ============================================================

#[test]
fn reject_missing_trade_sequence_source() {
    let mut config = load_trade("bybit_linear");

    let sequence = config.sequence.as_mut().expect("Missing trade sequence");

    sequence.source = "nonexistent_sequence".into();

    assert_trade_error(&config, "sequence.source references missing field");
}

// ============================================================
// DEPTH FIELD VALIDATION
// ============================================================

#[test]
fn reject_depth_field_without_type() {
    let mut config = load_depth("binance_spot");

    config
        .fields
        .insert("test_field".into(), "native_test_field".into());

    assert_depth_error(&config, "field 'test_field' has no corresponding type");
}

#[test]
fn reject_depth_type_without_field() {
    let mut config = load_depth("binance_spot");

    config.types.insert("test_type".into(), "u64".into());

    assert_depth_error(&config, "type 'test_type' has no corresponding field");
}

#[test]
fn reject_missing_depth_timestamp_source() {
    let mut config = load_depth("binance_spot");

    config.timestamps.event_source = "nonexistent_timestamp".into();

    assert_depth_error(&config, "timestamps.event_source references missing field");
}

// ============================================================
// DEPTH LEVEL VALIDATION
// ============================================================

#[test]
fn reject_wrong_depth_bid_side() {
    let mut config = load_depth("binance_spot");

    config.levels.bids.side = "ask".into();

    assert_depth_error(&config, "levels.bids.side must be 'bid'");
}

#[test]
fn reject_wrong_depth_ask_side() {
    let mut config = load_depth("binance_spot");

    config.levels.asks.side = "bid".into();

    assert_depth_error(&config, "levels.asks.side must be 'ask'");
}

// ============================================================
// DEPTH SEQUENCE REFERENCES
// ============================================================

#[test]
fn reject_missing_depth_first_sequence_field() {
    let mut config = load_depth("binance_spot");

    config.sequence.first_field = Some("nonexistent_first".into());

    assert_depth_error(&config, "sequence.first_field references missing field");
}

#[test]
fn reject_missing_depth_last_sequence_field() {
    let mut config = load_depth("binance_spot");

    config.sequence.last_field = Some("nonexistent_last".into());

    assert_depth_error(&config, "sequence.last_field references missing field");
}

#[test]
fn reject_missing_depth_previous_sequence_field() {
    let mut config = load_depth("binance_spot");

    config.sequence.previous_field = Some("nonexistent_previous".into());

    assert_depth_error(&config, "sequence.previous_field references missing field");
}

// ============================================================
// DEPTH SYNCHRONIZATION
// ============================================================

#[test]
fn reject_empty_depth_synchronization_strategy() {
    let mut config = load_depth("binance_spot");

    config.synchronization.strategy.clear();

    assert_depth_error(&config, "synchronization.strategy cannot be empty");
}

#[test]
fn reject_rest_synchronization_without_endpoint() {
    let mut config = load_depth("binance_spot");

    config.synchronization.snapshot_source = "rest".into();
    config.synchronization.snapshot_endpoint = None;

    assert_depth_error(&config, "REST synchronization requires snapshot_endpoint");
}

#[test]
fn reject_websocket_synchronization_without_initial_snapshot() {
    let mut config = load_depth("bybit_linear");

    config.synchronization.snapshot_source = "websocket".into();
    config.synchronization.require_initial_snapshot = Some(false);

    assert_depth_error(
        &config,
        "WebSocket synchronization requires initial snapshot",
    );
}

#[test]
fn reject_unknown_snapshot_source() {
    let mut config = load_depth("binance_spot");

    config.synchronization.snapshot_source = "unknown".into();

    assert_depth_error(&config, "unsupported synchronization.snapshot_source");
}

// ============================================================
// DEPTH RECOVERY
// ============================================================

#[test]
fn reject_depth_recovery_without_generation_fencing() {
    let mut config = load_depth("binance_spot");

    config.recovery.require_generation_fencing = false;

    assert_depth_error(&config, "depth recovery requires generation fencing");
}

// ============================================================
// DEPTH OUTPUT
// ============================================================

#[test]
fn reject_nonatomic_depth_output() {
    let mut config = load_depth("binance_spot");

    config.output.atomic_updates = false;

    assert_depth_error(&config, "depth output requires atomic updates");
}
