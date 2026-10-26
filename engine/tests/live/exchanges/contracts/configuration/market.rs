use std::{fs, path::Path};

use marketforge_engine::live::config::ExchangeConfig;

use marketforge_engine::live::config::exchange::MarketMapping;

// ============================================================
// HELPERS
// ============================================================

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

// ============================================================
// SETTLEMENT MAPPINGS
// ============================================================

// ============================================================
// NATIVE MARKET TYPE ACCESS
// ============================================================

// ============================================================
// CLASSIFICATION REQUIREMENTS
// ============================================================

// ============================================================
// VALIDATION
// ============================================================

// ============================================================
// STRICT FIELD RECOGNITION
// ============================================================

// ============================================================
// SERIALIZATION
// ============================================================

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
