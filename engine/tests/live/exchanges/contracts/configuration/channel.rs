use std::{collections::BTreeMap, fs, path::Path};

use marketforge_engine::live::config::{ChannelConfig, ExchangeConfig};

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
// BASIC CHANNEL CONFIGURATION
// ============================================================

// ============================================================
// DEPTH CONFIGURATION
// ============================================================

// ============================================================
// SYNCHRONIZATION
// ============================================================

// ============================================================
// QUANTITY SEMANTICS
// ============================================================

// ============================================================
// OPTIONAL CHANNELS
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
fn deserialize_all_real_channel_configs() {
    let paths = real_profiles();

    assert_eq!(paths.len(), 15, "Expected 15 exchange profiles");

    let mut total_channels = 0;

    for path in paths {
        let contents = fs::read_to_string(&path).expect("Cannot read exchange configuration");

        let original: toml::Value = toml::from_str(&contents).expect("Invalid exchange TOML");

        let channels_value = original
            .get("channels")
            .expect("Missing channels section")
            .clone();

        let channels_toml =
            toml::to_string(&channels_value).expect("Cannot serialize channels section");

        // TOML root must be a table.
        let channels: BTreeMap<String, ChannelConfig> = toml::from_str(&channels_toml)
            .unwrap_or_else(|error| {
                panic!(
                    "Failed to deserialize channels in {}: {error}",
                    path.display()
                )
            });

        let reconstructed: toml::Value =
            toml::from_str(&toml::to_string(&channels).expect("Cannot serialize typed channels"))
                .expect("Invalid reconstructed channels");

        assert_eq!(
            channels_value,
            reconstructed,
            "Channel fields changed in {}",
            path.display()
        );

        println!(
            "PASS: {} — {} channels recognized and preserved",
            path.file_name().unwrap().to_string_lossy(),
            channels.len()
        );

        total_channels += channels.len();
    }

    println!("\nAll 15 exchange profiles validated; {total_channels} channels checked.");
}

// ============================================================
// FULL CONFIGURATION INTEGRATION
// ============================================================

#[test]
fn channel_configs_integrate_with_exchange_config() {
    let paths = real_profiles();

    assert_eq!(paths.len(), 15);

    for path in paths {
        let config = ExchangeConfig::load(&path).unwrap_or_else(|error| {
            panic!(
                "Failed to load exchange configuration {}: {error}",
                path.display()
            )
        });

        assert!(
            !config.channels.is_empty(),
            "Exchange '{}' has no channels",
            config.exchange.profile
        );

        for (name, channel) in &config.channels {
            if channel.enabled && matches!(name.as_str(), "trades" | "aggregate_trades" | "depth") {
                assert!(
                    channel.format.is_some(),
                    "Enabled channel '{}' in '{}' has no format",
                    name,
                    config.exchange.profile
                );
            }
        }

        println!(
            "PASS: {} — {} channels integrated successfully",
            config.exchange.profile,
            config.channels.len()
        );
    }

    println!("\nAll 15 exchange configurations passed channel integration.");
}
