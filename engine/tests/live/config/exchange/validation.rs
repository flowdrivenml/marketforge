use std::{fs, path::Path};

use marketforge_engine::live::config::ExchangeConfig;

// ============================================================
// HELPERS
// ============================================================

fn config_dir() -> &'static Path {
    Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../data/.config/live/exchanges"
    ))
}

/// Return all exchange configuration paths in deterministic order.
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
// SCHEMA AND IDENTITY
// ============================================================

// ============================================================
// WEBSOCKET CONNECTIVITY
// ============================================================

// ============================================================
// HEARTBEAT VALIDATION
// ============================================================

// ============================================================
// SUBSCRIPTION VALIDATION
// ============================================================

// ============================================================
// REST CONNECTIVITY
// ============================================================

// ============================================================
// REST ENDPOINTS
// ============================================================

// ============================================================
// RATE-LIMIT POLICY REFERENCES
// ============================================================

// ============================================================
// RATE-LIMIT VALUES
// ============================================================

// ============================================================
// REQUEST-WEIGHT POLICIES
// ============================================================

// ============================================================
// CHANNEL VALIDATION
// ============================================================

// ============================================================
// RECOVERY ENDPOINT REFERENCES
// ============================================================

// ============================================================
// DEPTH CAPABILITIES
// ============================================================

// ============================================================
// MARKET MAPPING ROUTES
// ============================================================

// ============================================================
// VALID PROFILES
// ============================================================

#[test]
fn accept_all_real_exchange_profiles() {
    let paths = real_profiles();

    assert_eq!(paths.len(), 15, "Expected 15 exchange profiles");

    for path in paths {
        let config = ExchangeConfig::load(&path).unwrap_or_else(|error| {
            panic!(
                "Failed to load exchange profile {}: {error}",
                path.display()
            )
        });

        config.validate().unwrap_or_else(|error| {
            panic!(
                "Valid exchange profile '{}' rejected: {error}",
                config.exchange.profile
            )
        });

        println!("PASS: {}", config.exchange.profile);
    }

    println!("\nAll 15 exchange profiles passed semantic validation.");
}
