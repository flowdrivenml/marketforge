use marketforge_engine::live::config::ExchangeConfigStore;

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

#[test]
fn load_all_exchange_configs() {
    let config_dir = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../data/.config/live/exchanges"
    );

    println!("\nLoading exchange configurations from: {config_dir}");

    let store = ExchangeConfigStore::load_dir(config_dir).unwrap_or_else(|error| {
        panic!("Failed to load exchange configurations:\n{error:#?}");
    });

    println!("\nLoaded {} exchange configurations", store.len());

    assert_eq!(
        store.len(),
        EXPECTED_PROFILES.len(),
        "Unexpected number of exchange profiles"
    );

    for profile in EXPECTED_PROFILES {
        assert!(
            store.contains(profile),
            "Missing exchange profile: {profile}"
        );
    }

    for (name, config) in store.iter() {
        println!("\n----------------------------------------");
        println!("Profile: {}", name);
        println!("Exchange: {}", config.exchange.id.as_str());
        println!("Market: {}", config.exchange.market.as_str());
        println!("Schema version: {}", config.schema_version);
        println!("REST base URL: {}", config.rest.base_url);

        println!("Channels:");

        for (channel_name, channel) in &config.channels {
            println!(
                "  - {} | enabled={} | format={:?}",
                channel_name, channel.enabled, channel.format
            );
        }

        // Verify that the store key matches the configuration identity.
        assert_eq!(
            name,
            config.exchange.profile.as_str(),
            "Profile identity mismatch"
        );

        // Validate again to make failures easy to identify.
        config.validate().unwrap_or_else(|error| {
            panic!("Invalid configuration '{name}': {error}");
        });
    }

    println!("\nAll exchange configurations loaded successfully.");
}

#[test]
fn exchange_configs_preserve_all_fields() {
    use std::{fs, path::Path};

    use marketforge_engine::live::config::ExchangeConfig;

    let config_dir = Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../data/.config/live/exchanges"
    ));

    let mut tested = 0;

    for entry in fs::read_dir(config_dir).expect("Cannot read config directory") {
        let entry = entry.expect("Cannot read directory entry");
        let path = entry.path();

        if !path.is_file() || path.extension().is_none_or(|ext| ext != "toml") {
            continue;
        }

        let original = fs::read_to_string(&path).expect("Cannot read TOML file");

        let original_value: toml::Value = toml::from_str(&original).expect("Invalid original TOML");

        let config = ExchangeConfig::load(&path)
            .unwrap_or_else(|error| panic!("Failed to load {}: {error}", path.display()));

        let serialized = toml::to_string(&config)
            .unwrap_or_else(|error| panic!("Failed to serialize {}: {error}", path.display()));

        let reconstructed_value: toml::Value =
            toml::from_str(&serialized).expect("Invalid reconstructed TOML");

        assert_eq!(
            original_value,
            reconstructed_value,
            "Configuration fields changed or disappeared in {}",
            path.display()
        );

        println!(
            "PASS: {} — all fields preserved",
            path.file_name().unwrap().to_string_lossy()
        );

        tested += 1;
    }

    assert_eq!(tested, 15, "Expected to test 15 exchange configurations");

    println!("\nAll {tested} exchange configurations preserved their fields.");
}

#[test]
fn configured_profiles_have_explicit_cases() {
    use std::collections::BTreeSet;
    let store =
        ExchangeConfigStore::load_dir(crate::support::data(".config/live/exchanges")).unwrap();
    let configured: BTreeSet<_> = store.iter().map(|(name, _)| name.to_owned()).collect();
    let cases: BTreeSet<_> = crate::cases::PROFILES
        .iter()
        .map(|name| (*name).to_owned())
        .collect();
    assert_eq!(
        configured, cases,
        "every configured profile must have explicit exchange cases"
    );
}
