use std::{fs, path::Path};

use marketforge_engine::live::config::{
    DepthFormatConfig, FormatConfigStore, FormatKind, TradeFormatConfig,
};

const EXPECTED_PROFILES: &[&str] = &[
    "binance_spot",
    "binance_linear",
    "binance_inverse",
    "bybit_spot",
    "bybit_linear",
    "bybit_inverse",
    "okx_spot",
    "okx_linear",
    "okx_inverse",
    "bitget_spot",
    "bitget_linear",
    "bitget_inverse",
    "gateio_spot",
    "gateio_linear",
    "gateio_inverse",
];

fn format_directory() -> &'static Path {
    Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../data/.config/live/formats"
    ))
}

// ============================================================
// LOAD ALL FORMATS
// ============================================================

#[test]
fn load_all_format_configs() {
    let directory = format_directory();

    println!(
        "\nLoading format configurations from {}",
        directory.display()
    );

    let store = FormatConfigStore::load_dir(directory)
        .unwrap_or_else(|error| panic!("Failed to load formats: {error:#?}"));

    assert_eq!(store.trade_count(), EXPECTED_PROFILES.len());
    assert_eq!(store.depth_count(), EXPECTED_PROFILES.len());
    assert_eq!(store.len(), EXPECTED_PROFILES.len() * 2);

    for profile in EXPECTED_PROFILES {
        assert!(
            store.contains(FormatKind::Trades, profile),
            "Missing trade format: {profile}"
        );

        assert!(
            store.contains(FormatKind::Depth, profile),
            "Missing depth format: {profile}"
        );
    }

    for (name, config) in store.trades() {
        assert_eq!(name, config.format.id);
        config.validate().unwrap();

        println!(
            "TRADE: {} | exchange={} | market={} | channel={}",
            name,
            config.format.exchange.as_str(),
            config.format.market.as_str(),
            config.format.channel
        );
    }

    for (name, config) in store.depths() {
        assert_eq!(name, config.format.id);
        config.validate().unwrap();

        println!(
            "DEPTH: {} | exchange={} | market={} | channel={}",
            name,
            config.format.exchange.as_str(),
            config.format.market.as_str(),
            config.format.channel
        );
    }

    store.validate_all().expect("Store validation failed");

    println!(
        "\nSuccessfully loaded {} trade formats and {} depth formats.",
        store.trade_count(),
        store.depth_count()
    );
}

// ============================================================
// TRADE FORMAT ROUND-TRIP
// ============================================================

#[test]
fn trade_formats_preserve_all_fields() {
    let directory = format_directory().join("trades");

    let mut tested = 0;

    for entry in fs::read_dir(&directory).expect("Cannot read trade directory") {
        let entry = entry.expect("Cannot read directory entry");
        let path = entry.path();

        if path.extension().is_none_or(|ext| ext != "toml") {
            continue;
        }

        let original = fs::read_to_string(&path).expect("Cannot read TOML file");

        let original_value: toml::Value = toml::from_str(&original).expect("Invalid original TOML");

        let config = TradeFormatConfig::load(&path)
            .unwrap_or_else(|error| panic!("Failed to load {}: {error}", path.display()));

        let serialized = toml::to_string(&config)
            .unwrap_or_else(|error| panic!("Failed to serialize {}: {error}", path.display()));

        let reconstructed_value: toml::Value =
            toml::from_str(&serialized).expect("Invalid reconstructed TOML");

        assert_eq!(
            original_value,
            reconstructed_value,
            "Trade format fields changed or disappeared in {}",
            path.display()
        );

        println!(
            "PASS: trades/{} — all fields preserved",
            path.file_name().unwrap().to_string_lossy()
        );

        tested += 1;
    }

    assert_eq!(tested, EXPECTED_PROFILES.len());

    println!("\nAll {tested} trade formats preserved their fields.");
}

// ============================================================
// DEPTH FORMAT ROUND-TRIP
// ============================================================

#[test]
fn depth_formats_preserve_all_fields() {
    let directory = format_directory().join("depth");

    let mut tested = 0;

    for entry in fs::read_dir(&directory).expect("Cannot read depth directory") {
        let entry = entry.expect("Cannot read directory entry");
        let path = entry.path();

        if path.extension().is_none_or(|ext| ext != "toml") {
            continue;
        }

        let original = fs::read_to_string(&path).expect("Cannot read TOML file");

        let original_value: toml::Value = toml::from_str(&original).expect("Invalid original TOML");

        let config = DepthFormatConfig::load(&path)
            .unwrap_or_else(|error| panic!("Failed to load {}: {error}", path.display()));

        let serialized = toml::to_string(&config)
            .unwrap_or_else(|error| panic!("Failed to serialize {}: {error}", path.display()));

        let reconstructed_value: toml::Value =
            toml::from_str(&serialized).expect("Invalid reconstructed TOML");

        assert_eq!(
            original_value,
            reconstructed_value,
            "Depth format fields changed or disappeared in {}",
            path.display()
        );

        println!(
            "PASS: depth/{} — all fields preserved",
            path.file_name().unwrap().to_string_lossy()
        );

        tested += 1;
    }

    assert_eq!(tested, EXPECTED_PROFILES.len());

    println!("\nAll {tested} depth formats preserved their fields.");
}
