use marketforge_engine::live::config::exchange::RateLimitErrorCode;
use marketforge_engine::live::config::{ExchangeConfig, RestConfig};
use std::{fs, path::Path};

// ============================================================
// HELPERS
// ============================================================

fn parse_rest(input: &str) -> RestConfig {
    toml::from_str(input).unwrap_or_else(|error| panic!("Failed to parse REST config: {error}"))
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
// BASIC REST CONFIGURATION
// ============================================================

#[test]
fn deserialize_basic_rest_config() {
    let config = parse_rest(
        r#"
base_url = "https://api.example.com"
authentication = "none"
request_timeout_seconds = 10
"#,
    );

    assert_eq!(config.base_url, "https://api.example.com");
    assert_eq!(config.authentication.as_deref(), Some("none"));
    assert_eq!(config.request_timeout_seconds, Some(10));
}

#[test]
fn deserialize_rest_without_optional_fields() {
    let config = parse_rest(
        r#"
base_url = "https://api.example.com"
"#,
    );

    assert!(config.authentication.is_none());
    assert!(config.request_timeout_seconds.is_none());
    assert!(config.endpoints.is_empty());
    assert!(config.rate_limit_policies.is_empty());
    assert!(config.weight_policies.is_empty());
}

// ============================================================
// REST ENDPOINTS
// ============================================================

#[test]
fn deserialize_depth_snapshot_endpoint() {
    let config = parse_rest(
        r#"
base_url = "https://api.example.com"

[endpoints.depth_snapshot]
method = "GET"
path = "/api/v1/depth"
parameters = ["symbol", "limit"]
default_limit = 500
maximum_limit = 1000
response_sequence_field = "lastUpdateId"
authentication = "none"
"#,
    );

    let endpoint = config
        .endpoint("depth_snapshot")
        .expect("Missing depth snapshot endpoint");

    assert_eq!(endpoint.method, "GET");
    assert_eq!(endpoint.path, "/api/v1/depth");
    assert_eq!(endpoint.parameters, vec!["symbol", "limit"]);
    assert_eq!(endpoint.default_limit, Some(500));
    assert_eq!(endpoint.maximum_limit, Some(1000));
    assert_eq!(
        endpoint.response_sequence_field.as_deref(),
        Some("lastUpdateId")
    );
}

#[test]
fn deserialize_recent_trades_endpoint() {
    let config = parse_rest(
        r#"
base_url = "https://api.example.com"

[endpoints.recent_trades]
method = "GET"
path = "/api/v1/trades"
parameters = ["symbol", "limit"]
default_limit = 100
maximum_limit = 500
response_root = "data"
"#,
    );

    let endpoint = config.endpoint("recent_trades").unwrap();

    assert_eq!(endpoint.path, "/api/v1/trades");
    assert_eq!(endpoint.response_root.as_deref(), Some("data"));
}

#[test]
fn deserialize_historical_trades_endpoint() {
    let config = parse_rest(
        r#"
base_url = "https://api.example.com"

[endpoints.historical_trades]
method = "GET"
path = "/api/v1/historicalTrades"
parameters = ["symbol", "limit", "fromId"]
pagination_strategy = "trade_id"
pagination_parameter = "fromId"
max_history_days = 30
authentication = "api_key_header"
authentication_header = "X-MBX-APIKEY"
"#,
    );

    let endpoint = config.endpoint("historical_trades").unwrap();

    assert_eq!(endpoint.pagination_strategy.as_deref(), Some("trade_id"));
    assert_eq!(endpoint.pagination_parameter.as_deref(), Some("fromId"));
    assert_eq!(endpoint.max_history_days, Some(30));
    assert_eq!(
        endpoint.authentication_header.as_deref(),
        Some("X-MBX-APIKEY")
    );
}

#[test]
fn deserialize_settlement_specific_endpoint() {
    let config = parse_rest(
        r#"
base_url = "https://api.example.com"

[endpoints.depth_snapshot]
method = "GET"
path = "/futures/{settlement}/order_book"
parameters = ["contract", "limit", "with_id"]
settlement = "btc"
contract = "{symbol}"
with_id = true
interval = "0"
"#,
    );

    let endpoint = config.endpoint("depth_snapshot").unwrap();

    assert_eq!(endpoint.settlement.as_deref(), Some("btc"));
    assert_eq!(endpoint.contract.as_deref(), Some("{symbol}"));
    assert_eq!(endpoint.with_id, Some(true));
    assert_eq!(endpoint.interval.as_deref(), Some("0"));
}

#[test]
fn deserialize_response_field_mappings() {
    let config = parse_rest(
        r#"
base_url = "https://api.example.com"

[endpoints.depth_snapshot]
method = "GET"
path = "/depth"
response_root = "result"
response_symbol_field = "s"
response_sequence_field = "seqId"
response_update_id_field = "u"
response_cross_sequence_field = "seq"
response_timestamp_field = "ts"
response_bids_field = "b"
response_asks_field = "a"
response_level_type = "object"
response_price_field = "p"
response_quantity_field = "q"
"#,
    );

    let endpoint = config.endpoint("depth_snapshot").unwrap();

    assert_eq!(endpoint.response_root.as_deref(), Some("result"));
    assert_eq!(endpoint.response_symbol_field.as_deref(), Some("s"));
    assert_eq!(endpoint.response_sequence_field.as_deref(), Some("seqId"));
    assert_eq!(endpoint.response_update_id_field.as_deref(), Some("u"));
    assert_eq!(
        endpoint.response_cross_sequence_field.as_deref(),
        Some("seq")
    );
    assert_eq!(endpoint.response_timestamp_field.as_deref(), Some("ts"));
    assert_eq!(endpoint.response_bids_field.as_deref(), Some("b"));
    assert_eq!(endpoint.response_asks_field.as_deref(), Some("a"));
    assert_eq!(endpoint.response_level_type.as_deref(), Some("object"));
    assert_eq!(endpoint.response_price_field.as_deref(), Some("p"));
    assert_eq!(endpoint.response_quantity_field.as_deref(), Some("q"));
}

// ============================================================
// PAGINATION
// ============================================================

#[test]
fn deserialize_time_range_pagination() {
    let config = parse_rest(
        r#"
base_url = "https://api.example.com"

[endpoints.historical_trades]
method = "GET"
path = "/trades"
pagination_strategy = "gate_futures_trades"
preferred_pagination = "time_range"
legacy_id_pagination = true
"#,
    );

    let endpoint = config.endpoint("historical_trades").unwrap();

    assert_eq!(
        endpoint.pagination_strategy.as_deref(),
        Some("gate_futures_trades")
    );
    assert_eq!(endpoint.preferred_pagination.as_deref(), Some("time_range"));
    assert_eq!(endpoint.legacy_id_pagination, Some(true));
}

// ============================================================
// RATE LIMITING
// ============================================================

#[test]
fn deserialize_weighted_rate_limiting() {
    let config = parse_rest(
        r#"
base_url = "https://api.example.com"

[rate_limiting]
strategy = "weighted_shared_pool"
limits_source = "configured"
usage_header_prefix = "X-MBX-USED-WEIGHT"
http_throttled_status = 429
http_banned_status = 418
honor_retry_after = true
"#,
    );

    let limits = config.rate_limiting.unwrap();

    assert_eq!(limits.strategy, "weighted_shared_pool");
    assert_eq!(
        limits.usage_header_prefix.as_deref(),
        Some("X-MBX-USED-WEIGHT")
    );
    assert_eq!(limits.http_throttled_status, Some(429));
    assert_eq!(limits.http_banned_status, Some(418));
    assert_eq!(limits.honor_retry_after, Some(true));
}

#[test]
fn deserialize_shared_window_rate_limiting() {
    let config = parse_rest(
        r#"
base_url = "https://api.example.com"

[rate_limiting]
strategy = "shared_window"
ip_window_seconds = 5
ip_max_requests = 600
rate_limit_error_code = 10006
ip_limit_http_status = 403
"#,
    );

    let limits = config.rate_limiting.unwrap();

    assert_eq!(limits.strategy, "shared_window");
    assert_eq!(limits.ip_window_seconds, Some(5));
    assert_eq!(limits.ip_max_requests, Some(600));
    assert_eq!(
        limits.rate_limit_error_code,
        Some(RateLimitErrorCode::Integer(10006))
    );
    assert_eq!(limits.ip_limit_http_status, Some(403));
}

#[test]
fn deserialize_global_rate_limiting() {
    let config = parse_rest(
        r#"
base_url = "https://api.example.com"

[rate_limiting]
strategy = "endpoint_window"
rule = "ip"
global_max_requests = 6000
global_window_seconds = 60
http_throttled_status = 429
"#,
    );

    let limits = config.rate_limiting.unwrap();

    assert_eq!(limits.strategy, "endpoint_window");
    assert_eq!(limits.rule.as_deref(), Some("ip"));
    assert_eq!(limits.global_max_requests, Some(6000));
    assert_eq!(limits.global_window_seconds, Some(60));
}

// ============================================================
// RATE-LIMIT POLICIES
// ============================================================

#[test]
fn deserialize_endpoint_rate_limit_policy() {
    let config = parse_rest(
        r#"
base_url = "https://api.example.com"

[rate_limit_policies.depth_snapshot]
max_requests = 20
window_seconds = 1
"#,
    );

    let policy = config
        .rate_limit_policy("depth_snapshot")
        .expect("Missing rate-limit policy");

    assert_eq!(policy.max_requests, Some(20));
    assert_eq!(policy.window_seconds, Some(1));
}

#[test]
fn deserialize_unverified_rate_limit_policy() {
    let config = parse_rest(
        r#"
base_url = "https://api.example.com"

[rate_limit_policies.futures_public]
status = "requires_verification"
"#,
    );

    let policy = config.rate_limit_policy("futures_public").unwrap();

    assert_eq!(policy.status.as_deref(), Some("requires_verification"));
}

// ============================================================
// REQUEST-WEIGHT POLICIES
// ============================================================

#[test]
fn deserialize_fixed_weight_policy() {
    let config = parse_rest(
        r#"
base_url = "https://api.example.com"

[weight_policies.recent_trades]
type = "fixed"
weight = 5
"#,
    );

    let policy = config.weight_policy("recent_trades").unwrap();

    assert_eq!(policy.kind, "fixed");
    assert_eq!(policy.weight, Some(5));
    assert!(policy.tiers.is_empty());
}

#[test]
fn deserialize_discrete_weight_policy() {
    let config = parse_rest(
        r#"
base_url = "https://api.example.com"

[weight_policies.depth_snapshot]
type = "discrete"
parameter = "limit"

tiers = [
    { limit = 5, weight = 2 },
    { limit = 100, weight = 5 },
    { limit = 500, weight = 10 },
]
"#,
    );

    let policy = config.weight_policy("depth_snapshot").unwrap();

    assert_eq!(policy.kind, "discrete");
    assert_eq!(policy.parameter.as_deref(), Some("limit"));
    assert_eq!(policy.tiers.len(), 3);
    assert_eq!(policy.tiers[0].limit, Some(5));
    assert_eq!(policy.tiers[2].weight, 10);
}

#[test]
fn deserialize_tiered_weight_policy() {
    let config = parse_rest(
        r#"
base_url = "https://api.example.com"

[weight_policies.depth_snapshot]
type = "tiered"
parameter = "limit"

tiers = [
    { max = 100, weight = 5 },
    { max = 500, weight = 25 },
    { max = 1000, weight = 50 },
]
"#,
    );

    let policy = config.weight_policy("depth_snapshot").unwrap();

    assert_eq!(policy.kind, "tiered");
    assert_eq!(policy.tiers.len(), 3);
    assert_eq!(policy.tiers[0].max, Some(100));
    assert_eq!(policy.tiers[2].weight, 50);
}

// ============================================================
// STRICT FIELD RECOGNITION
// ============================================================

#[test]
fn reject_unknown_rest_field() {
    let input = r#"
base_url = "https://api.example.com"
unknown_setting = true
"#;

    assert!(
        toml::from_str::<RestConfig>(input).is_err(),
        "Unknown REST field was accepted"
    );
}

#[test]
fn reject_unknown_endpoint_field() {
    let input = r#"
base_url = "https://api.example.com"

[endpoints.depth_snapshot]
method = "GET"
path = "/depth"
unknown_parameter = 123
"#;

    assert!(
        toml::from_str::<RestConfig>(input).is_err(),
        "Unknown endpoint field was accepted"
    );
}

#[test]
fn reject_unknown_rate_limit_field() {
    let input = r#"
base_url = "https://api.example.com"

[rate_limiting]
strategy = "endpoint_window"
unknown_limit = 100
"#;

    assert!(
        toml::from_str::<RestConfig>(input).is_err(),
        "Unknown rate-limit field was accepted"
    );
}

#[test]
fn reject_unknown_weight_policy_field() {
    let input = r#"
base_url = "https://api.example.com"

[weight_policies.depth_snapshot]
type = "fixed"
weight = 5
unknown_weight = 10
"#;

    assert!(
        toml::from_str::<RestConfig>(input).is_err(),
        "Unknown weight-policy field was accepted"
    );
}

#[test]
fn reject_missing_rest_base_url() {
    assert!(
        toml::from_str::<RestConfig>("authentication = 'none'").is_err(),
        "REST configuration without base_url was accepted"
    );
}

#[test]
fn reject_missing_endpoint_method() {
    let input = r#"
base_url = "https://api.example.com"

[endpoints.depth_snapshot]
path = "/depth"
"#;

    assert!(
        toml::from_str::<RestConfig>(input).is_err(),
        "REST endpoint without method was accepted"
    );
}

#[test]
fn reject_invalid_endpoint_limit_type() {
    let input = r#"
base_url = "https://api.example.com"

[endpoints.depth_snapshot]
method = "GET"
path = "/depth"
default_limit = "five_hundred"
"#;

    assert!(
        toml::from_str::<RestConfig>(input).is_err(),
        "Invalid endpoint limit type was accepted"
    );
}

#[test]
fn reject_invalid_weight_tier() {
    let input = r#"
base_url = "https://api.example.com"

[weight_policies.depth_snapshot]
type = "tiered"

tiers = [
    { max = "large", weight = 5 },
]
"#;

    assert!(
        toml::from_str::<RestConfig>(input).is_err(),
        "Invalid weight tier was accepted"
    );
}

// ============================================================
// SERIALIZATION
// ============================================================

#[test]
fn rest_config_round_trip() {
    let input = r#"
base_url = "https://api.example.com"
authentication = "none"
request_timeout_seconds = 10

[endpoints.depth_snapshot]
method = "GET"
path = "/depth"
parameters = ["symbol", "limit"]
default_limit = 100

[rate_limiting]
strategy = "endpoint_window"
rule = "ip"

[rate_limit_policies.depth_snapshot]
max_requests = 20
window_seconds = 1
"#;

    let original: toml::Value = toml::from_str(input).expect("Invalid original TOML");

    let config = parse_rest(input);

    let serialized = toml::to_string(&config).expect("Failed to serialize REST config");

    let reconstructed: toml::Value =
        toml::from_str(&serialized).expect("Invalid reconstructed TOML");

    assert_eq!(
        original, reconstructed,
        "REST configuration changed during round trip"
    );
}

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
    use std::path::PathBuf;

    use marketforge_engine::live::config::ExchangeConfig;

    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../data/.config/live/exchanges/binance_spot.toml");

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
    use std::path::PathBuf;

    use marketforge_engine::live::config::ExchangeConfig;
    use marketforge_engine::live::infrastructure::rest::{RestRequest, RestRequestResolver};

    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../data/.config/live/exchanges");

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
