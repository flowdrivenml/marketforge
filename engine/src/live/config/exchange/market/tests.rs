use crate::live::config::exchange::MarketMapping;

// ============================================================
// HELPERS
// ============================================================

fn parse_mapping(input: &str) -> MarketMapping {
    toml::from_str(input).unwrap_or_else(|error| panic!("Failed to parse market mapping: {error}"))
}

// ============================================================
// BASIC MARKET MAPPING
// ============================================================

#[test]
fn deserialize_single_native_category() {
    let input = r#"
resolver = "marketforge_instrument_catalog"
native_category = "COIN-FUTURES"
websocket_inst_type = "coin-futures"
rest_category = "COIN-FUTURES"
"#;

    let mapping = parse_mapping(input);

    assert_eq!(mapping.resolver, "marketforge_instrument_catalog");

    assert_eq!(mapping.native_category.as_deref(), Some("COIN-FUTURES"));

    assert_eq!(mapping.websocket_inst_type.as_deref(), Some("coin-futures"));

    assert_eq!(mapping.rest_category.as_deref(), Some("COIN-FUTURES"));

    mapping.validate().expect("Valid mapping rejected");
}

#[test]
fn deserialize_multiple_native_categories() {
    let input = r#"
resolver = "marketforge_instrument_catalog"

supported_native_categories = [
    "USDT-FUTURES",
    "USDC-FUTURES",
]

[native_types.USDT-FUTURES]
websocket_inst_type = "usdt-futures"
rest_category = "USDT-FUTURES"

[native_types.USDC-FUTURES]
websocket_inst_type = "usdc-futures"
rest_category = "USDC-FUTURES"
"#;

    let mapping = parse_mapping(input);

    assert_eq!(mapping.supported_native_categories.len(), 2);
    assert_eq!(mapping.native_types.len(), 2);

    assert!(mapping.supports_category("USDT-FUTURES"));
    assert!(mapping.supports_category("USDC-FUTURES"));
    assert!(!mapping.supports_category("COIN-FUTURES"));

    let usdt = mapping
        .native_type("USDT-FUTURES")
        .expect("Missing USDT-FUTURES mapping");

    assert_eq!(usdt.websocket_inst_type.as_deref(), Some("usdt-futures"));

    assert_eq!(usdt.rest_category.as_deref(), Some("USDT-FUTURES"));

    mapping.validate().expect("Valid mapping rejected");
}

// ============================================================
// SETTLEMENT MAPPINGS
// ============================================================

#[test]
fn deserialize_single_native_settlement() {
    let input = r#"
resolver = "marketforge_instrument_catalog"
native_settlement = "btc"
require_inverse_contract_classification = true
"#;

    let mapping = parse_mapping(input);

    assert_eq!(mapping.native_settlement.as_deref(), Some("btc"));

    assert_eq!(mapping.require_inverse_contract_classification, Some(true));

    assert!(mapping.supports_settlement("btc"));
    assert!(!mapping.supports_settlement("usdt"));

    mapping.validate().expect("Valid mapping rejected");
}

#[test]
fn deserialize_multiple_settlements() {
    let input = r#"
resolver = "marketforge_instrument_catalog"

supported_settlements = [
    "usdt",
    "usd1",
]

[native_types.usdt]
settlement = "usdt"
websocket_route = "usdt"

[native_types.usd1]
settlement = "usd1"
websocket_route = "usd1"
"#;

    let mapping = parse_mapping(input);

    assert_eq!(mapping.supported_settlements.len(), 2);
    assert_eq!(mapping.native_types.len(), 2);

    assert!(mapping.supports_settlement("usdt"));
    assert!(mapping.supports_settlement("usd1"));
    assert!(!mapping.supports_settlement("btc"));

    assert_eq!(
        mapping
            .native_type("usdt")
            .unwrap()
            .websocket_route
            .as_deref(),
        Some("usdt")
    );

    assert_eq!(
        mapping
            .native_type("usd1")
            .unwrap()
            .websocket_route
            .as_deref(),
        Some("usd1")
    );

    mapping.validate().expect("Valid mapping rejected");
}

// ============================================================
// NATIVE MARKET TYPE ACCESS
// ============================================================

#[test]
fn retrieve_native_market_type() {
    let mapping = parse_mapping(
        r#"
resolver = "marketforge_instrument_catalog"

[native_types.USDT-FUTURES]
websocket_inst_type = "usdt-futures"
rest_category = "USDT-FUTURES"
"#,
    );

    let native = mapping.native_type("USDT-FUTURES");

    assert!(native.is_some());
    assert!(mapping.native_type("UNKNOWN").is_none());
}

#[test]
fn iterate_native_market_types() {
    let mapping = parse_mapping(
        r#"
resolver = "marketforge_instrument_catalog"

[native_types.usdt]
settlement = "usdt"
websocket_route = "usdt"

[native_types.usd1]
settlement = "usd1"
websocket_route = "usd1"
"#,
    );

    let names: Vec<&str> = mapping.native_types().map(|(name, _)| name).collect();

    assert_eq!(names, vec!["usd1", "usdt"]);
}

// ============================================================
// CLASSIFICATION REQUIREMENTS
// ============================================================

#[test]
fn deserialize_inverse_classification_requirement() {
    let mapping = parse_mapping(
        r#"
resolver = "marketforge_instrument_catalog"
require_inverse_contract_classification = true
"#,
    );

    assert_eq!(mapping.require_inverse_contract_classification, Some(true));
}

#[test]
fn deserialize_linear_classification_requirement() {
    let mapping = parse_mapping(
        r#"
resolver = "marketforge_instrument_catalog"
require_linear_contract_classification = true
"#,
    );

    assert_eq!(mapping.require_linear_contract_classification, Some(true));
}

// ============================================================
// VALIDATION
// ============================================================

#[test]
fn reject_empty_resolver() {
    let mapping = parse_mapping(
        r#"
resolver = ""
"#,
    );

    assert!(mapping.validate().is_err(), "Empty resolver was accepted");
}

#[test]
fn reject_empty_native_category() {
    let mapping = parse_mapping(
        r#"
resolver = "marketforge_instrument_catalog"
native_category = ""
"#,
    );

    assert!(
        mapping.validate().is_err(),
        "Empty native category was accepted"
    );
}

#[test]
fn reject_empty_native_settlement() {
    let mapping = parse_mapping(
        r#"
resolver = "marketforge_instrument_catalog"
native_settlement = ""
"#,
    );

    assert!(
        mapping.validate().is_err(),
        "Empty native settlement was accepted"
    );
}

#[test]
fn reject_empty_supported_category() {
    let mapping = parse_mapping(
        r#"
resolver = "marketforge_instrument_catalog"
supported_native_categories = ["USDT-FUTURES", ""]
"#,
    );

    assert!(
        mapping.validate().is_err(),
        "Empty supported category was accepted"
    );
}

#[test]
fn reject_empty_supported_settlement() {
    let mapping = parse_mapping(
        r#"
resolver = "marketforge_instrument_catalog"
supported_settlements = ["usdt", ""]
"#,
    );

    assert!(
        mapping.validate().is_err(),
        "Empty supported settlement was accepted"
    );
}

#[test]
fn reject_empty_websocket_route() {
    let mapping = parse_mapping(
        r#"
resolver = "marketforge_instrument_catalog"

[native_types.usdt]
settlement = "usdt"
websocket_route = ""
"#,
    );

    assert!(
        mapping.validate().is_err(),
        "Empty WebSocket route was accepted"
    );
}

// ============================================================
// STRICT FIELD RECOGNITION
// ============================================================

#[test]
fn reject_unknown_market_mapping_field() {
    let input = r#"
resolver = "marketforge_instrument_catalog"
unknown_setting = true
"#;

    assert!(
        toml::from_str::<MarketMapping>(input).is_err(),
        "Unknown market mapping field was accepted"
    );
}

#[test]
fn reject_unknown_native_market_type_field() {
    let input = r#"
resolver = "marketforge_instrument_catalog"

[native_types.usdt]
settlement = "usdt"
unknown_setting = 123
"#;

    assert!(
        toml::from_str::<MarketMapping>(input).is_err(),
        "Unknown native market type field was accepted"
    );
}

#[test]
fn reject_missing_resolver() {
    let input = r#"
native_category = "COIN-FUTURES"
"#;

    assert!(
        toml::from_str::<MarketMapping>(input).is_err(),
        "Market mapping without resolver was accepted"
    );
}

#[test]
fn reject_invalid_classification_type() {
    let input = r#"
resolver = "marketforge_instrument_catalog"
require_inverse_contract_classification = "yes"
"#;

    assert!(
        toml::from_str::<MarketMapping>(input).is_err(),
        "Invalid classification field type was accepted"
    );
}

// ============================================================
// SERIALIZATION
// ============================================================

#[test]
fn market_mapping_round_trip() {
    let input = r#"
resolver = "marketforge_instrument_catalog"

supported_native_categories = [
    "USDT-FUTURES",
    "USDC-FUTURES",
]

[native_types.USDT-FUTURES]
websocket_inst_type = "usdt-futures"
rest_category = "USDT-FUTURES"

[native_types.USDC-FUTURES]
websocket_inst_type = "usdc-futures"
rest_category = "USDC-FUTURES"
"#;

    let original: toml::Value = toml::from_str(input).expect("Invalid original TOML");

    let mapping = parse_mapping(input);

    let serialized = toml::to_string(&mapping).expect("Failed to serialize mapping");

    let reconstructed: toml::Value =
        toml::from_str(&serialized).expect("Invalid reconstructed TOML");

    assert_eq!(
        original, reconstructed,
        "Market mapping changed during round trip"
    );
}

// ============================================================
// REAL EXCHANGE CONFIGURATIONS
// ============================================================

// ============================================================
// FULL CONFIGURATION INTEGRATION
// ============================================================
