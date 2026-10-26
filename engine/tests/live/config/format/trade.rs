use std::{fs, path::Path};

use marketforge_engine::live::config::format::TradeFormatConfig;

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

// ============================================================
// TRADE AGGREGATION
// ============================================================

// ============================================================
// NATIVE TRADE IDENTITY
// ============================================================

// ============================================================
// TRADE ATTRIBUTES
// ============================================================

// ============================================================
// TRADE SEQUENCE METADATA
// ============================================================

// ============================================================
// SYMBOL TYPE
// ============================================================

// ============================================================
// STREAM INITIALIZATION
// ============================================================

// ============================================================
// UNIFIED TRADE SEMANTICS
// ============================================================

// ============================================================
// PROTOCOL REQUIREMENTS
// ============================================================

// ============================================================
// TRADE INTEGRITY
// ============================================================

// ============================================================
// CANONICAL TRADE OUTPUT
// ============================================================

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
