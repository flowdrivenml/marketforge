use std::{fs, path::Path};

use marketforge_engine::live::config::ExchangeConfig;

use marketforge_engine::live::config::exchange::MarketMapping;

// ============================================================
// HELPERS
// ============================================================

fn parse_mapping(input: &str) -> MarketMapping {
    toml::from_str(input).unwrap_or_else(|error| panic!("Failed to parse market mapping: {error}"))
}

fn config_dir() -> &'static Path {
    Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../data/.config/live/exchanges"
    ))
}

fn real_profiles() -> Vec<std::path::PathBuf> {
    let mut paths: Vec<_> = fs::read_dir(config_dir())
        .expect("Cannot read exchange configuration directory")
        .map(|entry| entry.expect("Cannot read directory entry").path())
        .filter(|path| path.is_file() && path.extension().is_some_and(|ext| ext == "toml"))
        .collect();

    paths.sort();
    paths
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

#[test]
fn deserialize_all_real_market_mappings() {
    let paths = real_profiles();

    assert_eq!(paths.len(), 15, "Expected 15 exchange profiles");

    let mut mapped = 0;
    let mut unmapped = 0;

    for path in paths {
        let contents = fs::read_to_string(&path).expect("Cannot read exchange TOML");

        let original: toml::Value = toml::from_str(&contents).expect("Invalid exchange TOML");

        let Some(mapping_value) = original.get("market_mapping") else {
            println!(
                "SKIP: {} — no native market mapping",
                path.file_name().unwrap().to_string_lossy()
            );

            unmapped += 1;
            continue;
        };

        let mapping_toml =
            toml::to_string(mapping_value).expect("Cannot serialize market mapping section");

        let mapping: MarketMapping = toml::from_str(&mapping_toml).unwrap_or_else(|error| {
            panic!(
                "Failed to deserialize market mapping in {}: {error}",
                path.display()
            )
        });

        mapping.validate().unwrap_or_else(|error| {
            panic!("Invalid market mapping in {}: {error}", path.display())
        });

        let reconstructed: toml::Value = toml::from_str(
            &toml::to_string(&mapping).expect("Cannot serialize typed market mapping"),
        )
        .expect("Invalid reconstructed market mapping");

        assert_eq!(
            mapping_value,
            &reconstructed,
            "Market mapping fields changed in {}",
            path.display()
        );

        println!(
            "PASS: {} — native market mapping recognized and preserved",
            path.file_name().unwrap().to_string_lossy()
        );

        mapped += 1;
    }

    assert!(mapped > 0, "No market mappings were tested");

    println!("\nMarket mapping results: {mapped} mapped, {unmapped} without mappings.");
}

// ============================================================
// FULL CONFIGURATION INTEGRATION
// ============================================================

#[test]
fn market_mappings_integrate_with_exchange_config() {
    let paths = real_profiles();

    assert_eq!(paths.len(), 15);

    for path in paths {
        let config = ExchangeConfig::load(&path).unwrap_or_else(|error| {
            panic!(
                "Failed to load exchange configuration {}: {error}",
                path.display()
            )
        });

        if let Some(mapping) = &config.market_mapping {
            mapping.validate().unwrap_or_else(|error| {
                panic!(
                    "Invalid market mapping in '{}': {error}",
                    config.exchange.profile
                )
            });

            for (name, native_type) in mapping.native_types() {
                if let Some(route) = &native_type.websocket_route {
                    assert!(
                        config.websocket.routes.contains_key(route),
                        "Profile '{}' native type '{}' references missing route '{}'",
                        config.exchange.profile,
                        name,
                        route
                    );
                }
            }

            println!(
                "PASS: {} — native market mapping integrated",
                config.exchange.profile
            );
        } else {
            println!(
                "PASS: {} — no native mapping required",
                config.exchange.profile
            );
        }
    }
}
