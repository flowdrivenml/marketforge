use std::{fs, path::Path};

use marketforge_engine::live::config::{ExchangeConfig, WebSocketConfig};

// ============================================================
// HELPERS
// ============================================================

fn config_dir() -> &'static Path {
    Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../data/.config/live/exchanges"
    ))
}

// ============================================================
// CONNECTION ENDPOINTS
// ============================================================

// ============================================================
// CONNECTION LIMITS
// ============================================================

// ============================================================
// HEARTBEAT STRATEGIES
// ============================================================

// ============================================================
// SUBSCRIPTION PROTOCOLS
// ============================================================

// ============================================================
// MESSAGE ROUTING
// ============================================================

// ============================================================
// CONNECTION LIFECYCLE
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
fn deserialize_all_real_websocket_configs() {
    let mut tested = 0;

    for entry in fs::read_dir(config_dir()).expect("Cannot read exchange config directory") {
        let path = entry.expect("Cannot read directory entry").path();

        if !path.is_file() || path.extension().is_none_or(|ext| ext != "toml") {
            continue;
        }

        let contents = fs::read_to_string(&path).expect("Cannot read configuration file");

        let original: toml::Value = toml::from_str(&contents).expect("Invalid TOML");

        let websocket_value = original
            .get("websocket")
            .expect("Missing WebSocket section")
            .clone();

        let websocket_toml =
            toml::to_string(&websocket_value).expect("Cannot serialize WebSocket section");

        let websocket: WebSocketConfig = toml::from_str(&websocket_toml).unwrap_or_else(|error| {
            panic!(
                "Failed to deserialize WebSocket configuration in {}: {error}",
                path.display()
            )
        });

        let reconstructed: toml::Value = toml::from_str(
            &toml::to_string(&websocket).expect("Cannot serialize typed WebSocket configuration"),
        )
        .expect("Invalid reconstructed WebSocket TOML");

        assert_eq!(
            websocket_value,
            reconstructed,
            "WebSocket fields changed in {}",
            path.display()
        );

        println!(
            "PASS: {} — WebSocket fields recognized and preserved",
            path.file_name().unwrap().to_string_lossy()
        );

        tested += 1;
    }

    assert_eq!(tested, 15, "Expected 15 exchange configurations");
}

// ============================================================
// FULL CONFIGURATION COMPATIBILITY
// ============================================================

#[test]
fn websocket_configs_integrate_with_exchange_config() {
    let mut tested = 0;

    for entry in fs::read_dir(config_dir()).unwrap() {
        let path = entry.unwrap().path();

        if !path.is_file() || path.extension().is_none_or(|ext| ext != "toml") {
            continue;
        }

        let config = ExchangeConfig::load(&path).unwrap_or_else(|error| {
            panic!(
                "Failed to load exchange configuration {}: {error}",
                path.display()
            )
        });

        assert!(
            config.websocket.url.is_some()
                || config.websocket.base_url.is_some()
                || !config.websocket.routes.is_empty(),
            "Exchange {} has no WebSocket endpoint",
            config.exchange.profile
        );

        tested += 1;
    }

    assert_eq!(
        tested, 15,
        "Expected to validate WebSocket integration for all 15 exchange profiles"
    );

    println!("\nAll {tested} WebSocket configurations integrated successfully.");
}
