use marketforge_engine::live::config::{ExchangeConfig, RestConfig};
use std::{fs, path::Path};

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
// BASIC REST CONFIGURATION
// ============================================================

// ============================================================
// REST ENDPOINTS
// ============================================================

// ============================================================
// PAGINATION
// ============================================================

// ============================================================
// RATE LIMITING
// ============================================================

// ============================================================
// RATE-LIMIT POLICIES
// ============================================================

// ============================================================
// REQUEST-WEIGHT POLICIES
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
fn deserialize_all_real_rest_configs() {
    let paths = real_profiles();

    assert_eq!(paths.len(), 15, "Expected 15 exchange profiles");

    for path in paths {
        let contents = fs::read_to_string(&path).expect("Cannot read exchange TOML");

        let original: toml::Value = toml::from_str(&contents).expect("Invalid exchange TOML");

        let rest_value = original.get("rest").expect("Missing REST section").clone();

        let rest_toml = toml::to_string(&rest_value).expect("Cannot serialize REST section");

        let rest: RestConfig = toml::from_str(&rest_toml).unwrap_or_else(|error| {
            panic!(
                "Failed to deserialize REST configuration in {}: {error}",
                path.display()
            )
        });

        let reconstructed: toml::Value = toml::from_str(
            &toml::to_string(&rest).expect("Cannot serialize typed REST configuration"),
        )
        .expect("Invalid reconstructed REST TOML");

        assert_eq!(
            rest_value,
            reconstructed,
            "REST fields changed in {}",
            path.display()
        );

        println!(
            "PASS: {} — REST fields recognized and preserved",
            path.file_name().unwrap().to_string_lossy()
        );
    }
}

// ============================================================
// FULL CONFIGURATION INTEGRATION
// ============================================================

#[test]
fn rest_configs_integrate_with_exchange_config() {
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
            !config.rest.base_url.trim().is_empty(),
            "Exchange '{}' has an empty REST base URL",
            config.exchange.profile
        );

        for (name, endpoint) in &config.rest.endpoints {
            assert!(
                !endpoint.method.trim().is_empty(),
                "REST endpoint '{}' in '{}' has no HTTP method",
                name,
                config.exchange.profile
            );

            assert!(
                !endpoint.path.trim().is_empty(),
                "REST endpoint '{}' in '{}' has no path",
                name,
                config.exchange.profile
            );
        }

        println!(
            "PASS: {} — {} REST endpoints integrated successfully",
            config.exchange.profile,
            config.rest.endpoints.len()
        );
    }

    println!("\nAll 15 exchange configurations passed REST integration.");
}

#[test]
fn loads_binance_spot_rest_trade_normalization() {
    use marketforge_engine::live::config::ExchangeConfig;

    let path = crate::support::data(".config/live/exchanges/binance_spot.toml");

    let config = ExchangeConfig::load(path).unwrap();

    let endpoint = config
        .rest
        .endpoint("recent_trades")
        .expect("missing recent_trades endpoint");

    let mapping = endpoint
        .trade_normalization
        .as_ref()
        .expect("missing REST trade normalization configuration");

    assert_eq!(mapping.trade_id, "id");
    assert_eq!(mapping.price, "price");
    assert_eq!(mapping.quantity, "qty");
    assert_eq!(mapping.timestamp, "time");
    assert_eq!(mapping.timestamp_unit, "milliseconds");

    assert_eq!(mapping.side.as_deref(), Some("isBuyerMaker"));
    assert_eq!(mapping.side_strategy, "mapped");

    assert_eq!(
        mapping.side_mappings.get("true").map(String::as_str),
        Some("sell"),
    );

    assert_eq!(
        mapping.side_mappings.get("false").map(String::as_str),
        Some("buy"),
    );

    assert!(mapping.sequence.is_none());
    assert!(mapping.symbol.is_none());
}

#[test]
fn resolves_all_15_rest_trade_instruments() {
    use marketforge_engine::live::config::ExchangeConfig;
    use marketforge_engine::live::infrastructure::rest::{RestRequest, RestRequestResolver};

    let root = crate::support::data(".config/live/exchanges");

    let cases = [
        ("binance_spot", "BTCUSDT"),
        ("binance_linear", "BTCUSDT"),
        ("binance_inverse", "BTCUSD_PERP"),
        ("bybit_spot", "BTCUSDT"),
        ("bybit_linear", "BTCUSDT"),
        ("bybit_inverse", "BTCUSD"),
        ("bitget_spot", "BTCUSDT"),
        ("bitget_linear", "BTCUSDT"),
        ("bitget_inverse", "BTCUSD_CM"),
        ("okx_spot", "BTC-USDT"),
        ("okx_linear", "BTC-USDT-SWAP"),
        ("okx_inverse", "BTC-USD-SWAP"),
        ("gateio_spot", "BTC_USDT"),
        ("gateio_linear", "BTC_USDT"),
        ("gateio_inverse", "BTC_USD"),
    ];

    for (profile, symbol) in cases {
        let exchange = profile.split('_').next().unwrap();

        let endpoint_name = if exchange == "gateio" {
            "historical_trades"
        } else {
            "recent_trades"
        };

        let config = ExchangeConfig::load(root.join(format!("{profile}.toml"))).unwrap();

        let endpoint = config.rest.endpoint(endpoint_name).unwrap();

        let mapping = endpoint.trade_normalization.as_ref().unwrap();

        let symbol_parameter = mapping.request_symbol_parameter.as_str();

        let mut request =
            RestRequest::new(profile, endpoint_name).with_parameter(symbol_parameter, symbol);

        request = match profile {
            "bitget_linear" => request.with_parameter("native_rest_category", "USDT-FUTURES"),

            "gateio_linear" => request.with_parameter("native_settlement", "usdt"),

            _ => request,
        };

        let resolved = RestRequestResolver::resolve_endpoint(&config, endpoint, &request)
            .unwrap_or_else(|error| panic!("{profile}: REST resolution failed: {error}"));
        if profile == "gateio_spot" {
            assert!(
                !request.parameters().contains_key("currency_pair"),
                "Gate.io Spot must derive currency_pair from symbol",
            );
        }
        assert_eq!(resolved.profile(), profile);
        assert_eq!(resolved.endpoint(), endpoint_name);
        match profile {
            "bitget_linear" => {
                assert_eq!(resolved.parameter("category"), Some("USDT-FUTURES"),);
            }

            "bitget_inverse" => {
                assert_eq!(resolved.parameter("category"), Some("COIN-FUTURES"),);
            }

            "gateio_spot" => {
                assert_eq!(resolved.parameter("currency_pair"), Some(symbol),);
            }

            "gateio_linear" => {
                assert_eq!(resolved.parameter("contract"), Some(symbol),);

                assert!(
                    resolved.url().contains("/futures/usdt/trades"),
                    "{profile}: incorrect settlement path: {}",
                    resolved.url(),
                );
            }

            "gateio_inverse" => {
                assert_eq!(resolved.parameter("contract"), Some(symbol),);

                assert!(
                    resolved.url().contains("/futures/btc/trades"),
                    "{profile}: incorrect settlement path: {}",
                    resolved.url(),
                );
            }

            _ => {}
        }
        // The actual outgoing query or path must contain
        // the requested instrument identity.
        //
        // URL-path matching is useful for exchanges whose
        // endpoint templates place an instrument in the path.
        let query_match = resolved.query().values().any(|value| value == symbol);

        let path_match = resolved
            .url()
            .split('?')
            .next()
            .unwrap()
            .split('/')
            .any(|segment| segment == symbol);

        assert!(
            query_match || path_match,
            "{profile}: symbol {symbol:?} not found in \
             outgoing HTTP query or URL path; \
             url={}, query={:?}",
            resolved.url(),
            resolved.query(),
        );

        println!(
            "PASS {profile:20} symbol={symbol:18} \
             query_match={query_match} path_match={path_match}"
        );
    }
}
