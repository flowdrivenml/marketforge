use std::path::Path;

use crate::live::config::ExchangeConfig;

// ============================================================
// HELPERS
// ============================================================

fn config_dir() -> &'static Path {
    Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../data/.config/live/exchanges"
    ))
}

/// Load a known-valid configuration as a starting point.
///
/// Individual tests modify one property and verify that
/// semantic validation rejects the resulting configuration.
fn valid_config() -> ExchangeConfig {
    let path = config_dir().join("binance_spot.toml");

    ExchangeConfig::load(&path).unwrap_or_else(|error| panic!("Cannot load test fixture: {error}"))
}

/// Assert that semantic validation fails and reports
/// the expected error context.
fn assert_invalid(config: &ExchangeConfig, expected: &str) {
    let error = config
        .validate()
        .expect_err("Invalid configuration was accepted");

    assert!(
        error.contains(expected),
        "Expected validation error containing '{expected}', got: {error}"
    );
}

// ============================================================
// SCHEMA AND IDENTITY
// ============================================================

#[test]
fn reject_unsupported_schema_version() {
    let mut config = valid_config();

    config.schema_version = 999;

    assert_invalid(&config, "unsupported exchange schema version");
}

#[test]
fn reject_mismatched_exchange_profile() {
    let mut config = valid_config();

    config.exchange.profile = "binance_inverse".into();

    assert_invalid(&config, "does not match expected");
}

// ============================================================
// WEBSOCKET CONNECTIVITY
// ============================================================

#[test]
fn reject_missing_websocket_endpoint() {
    let mut config = valid_config();

    config.websocket.url = None;
    config.websocket.base_url = None;
    config.websocket.routes.clear();

    assert_invalid(&config, "websocket requires");
}

#[test]
fn reject_empty_websocket_url() {
    let mut config = valid_config();

    config.websocket.url = Some(String::new());
    config.websocket.base_url = None;
    config.websocket.routes.clear();

    assert_invalid(&config, "websocket requires");
}

#[test]
fn reject_empty_named_websocket_route() {
    let mut config = valid_config();

    config.websocket.routes.insert(
        "invalid".into(),
        crate::live::config::exchange::WebSocketRoute {
            url: Some(String::new()),
            base_url: None,
            authentication: None,
            access_mode: None,
        },
    );

    assert_invalid(&config, "websocket.routes.invalid requires url or base_url");
}

// ============================================================
// HEARTBEAT VALIDATION
// ============================================================

#[test]
fn reject_zero_heartbeat_interval() {
    let mut config = valid_config();

    let heartbeat = config
        .websocket
        .heartbeat
        .as_mut()
        .expect("Fixture has no heartbeat");

    heartbeat.interval_seconds = Some(0);

    assert_invalid(&config, "heartbeat.interval_seconds");
}

#[test]
fn reject_zero_heartbeat_timeout() {
    let mut config = valid_config();

    let heartbeat = config
        .websocket
        .heartbeat
        .as_mut()
        .expect("Fixture has no heartbeat");

    heartbeat.response_timeout_seconds = Some(0);

    assert_invalid(&config, "heartbeat.response_timeout_seconds");
}

#[test]
fn reject_zero_heartbeat_general_timeout() {
    let mut config = valid_config();

    let heartbeat = config
        .websocket
        .heartbeat
        .as_mut()
        .expect("Fixture has no heartbeat");

    heartbeat.timeout_seconds = Some(0);

    assert_invalid(&config, "heartbeat.timeout_seconds");
}

// ============================================================
// SUBSCRIPTION VALIDATION
// ============================================================

#[test]
fn reject_empty_subscription_strategy() {
    let mut config = valid_config();

    config
        .websocket
        .subscription
        .as_mut()
        .expect("Fixture has no subscription")
        .strategy
        .clear();

    assert_invalid(&config, "subscription.strategy");
}

#[test]
fn reject_empty_unsubscribe_method() {
    let mut config = valid_config();

    let subscription = config
        .websocket
        .subscription
        .as_mut()
        .expect("Fixture has no subscription");

    subscription.supports_unsubscribe = Some(true);
    subscription.unsubscribe_method = Some(String::new());

    assert_invalid(&config, "subscription.unsubscribe_method");
}

// ============================================================
// REST CONNECTIVITY
// ============================================================

#[test]
fn reject_empty_rest_base_url() {
    let mut config = valid_config();

    config.rest.base_url.clear();

    assert_invalid(&config, "rest.base_url");
}

#[test]
fn reject_zero_rest_timeout() {
    let mut config = valid_config();

    config.rest.request_timeout_seconds = Some(0);

    assert_invalid(&config, "rest.request_timeout_seconds");
}

// ============================================================
// REST ENDPOINTS
// ============================================================

#[test]
fn reject_empty_rest_endpoint_method() {
    let mut config = valid_config();

    config
        .rest
        .endpoints
        .get_mut("depth_snapshot")
        .expect("Missing depth_snapshot endpoint")
        .method
        .clear();

    assert_invalid(&config, "method cannot be empty");
}

#[test]
fn reject_empty_rest_endpoint_path() {
    let mut config = valid_config();

    config
        .rest
        .endpoints
        .get_mut("depth_snapshot")
        .expect("Missing depth_snapshot endpoint")
        .path
        .clear();

    assert_invalid(&config, "path cannot be empty");
}

#[test]
fn reject_default_limit_exceeding_maximum() {
    let mut config = valid_config();

    let endpoint = config
        .rest
        .endpoints
        .get_mut("depth_snapshot")
        .expect("Missing depth_snapshot endpoint");

    endpoint.default_limit = Some(1000);
    endpoint.maximum_limit = Some(100);

    assert_invalid(&config, "exceeds maximum_limit");
}

// ============================================================
// RATE-LIMIT POLICY REFERENCES
// ============================================================

#[test]
fn reject_missing_rate_limit_policy() {
    let mut config = valid_config();

    config
        .rest
        .endpoints
        .get_mut("depth_snapshot")
        .expect("Missing depth_snapshot endpoint")
        .rate_limit_policy = Some("nonexistent_policy".into());

    assert_invalid(&config, "missing rate_limit_policy");
}

#[test]
fn reject_missing_weight_policy() {
    let mut config = valid_config();

    config
        .rest
        .endpoints
        .get_mut("depth_snapshot")
        .expect("Missing depth_snapshot endpoint")
        .weight_policy = Some("nonexistent_policy".into());

    assert_invalid(&config, "missing weight_policy");
}

// ============================================================
// RATE-LIMIT VALUES
// ============================================================

#[test]
fn reject_zero_rate_limit_requests() {
    let mut config = valid_config();

    config.rest.rate_limit_policies.insert(
        "invalid".into(),
        crate::live::config::exchange::RateLimitPolicy {
            status: None,
            max_requests: Some(0),
            window_seconds: Some(1),
        },
    );

    assert_invalid(&config, "max_requests must be positive");
}

#[test]
fn reject_zero_rate_limit_window() {
    let mut config = valid_config();

    config.rest.rate_limit_policies.insert(
        "invalid".into(),
        crate::live::config::exchange::RateLimitPolicy {
            status: None,
            max_requests: Some(10),
            window_seconds: Some(0),
        },
    );

    assert_invalid(&config, "window_seconds must be positive");
}

// ============================================================
// REQUEST-WEIGHT POLICIES
// ============================================================

#[test]
fn reject_unknown_weight_policy_type() {
    let mut config = valid_config();

    let policy = config
        .rest
        .weight_policies
        .get_mut("depth_snapshot")
        .expect("Missing depth_snapshot weight policy");

    policy.kind = "unknown".into();

    assert_invalid(&config, "unsupported weight policy type");
}

#[test]
fn reject_zero_fixed_request_weight() {
    let mut config = valid_config();

    let policy = config
        .rest
        .weight_policies
        .get_mut("recent_trades")
        .expect("Missing recent_trades weight policy");

    policy.kind = "fixed".into();
    policy.weight = Some(0);

    assert_invalid(&config, "requires positive weight");
}

#[test]
fn reject_empty_tiered_weight_policy() {
    let mut config = valid_config();

    let policy = config
        .rest
        .weight_policies
        .get_mut("depth_snapshot")
        .expect("Missing depth_snapshot weight policy");

    policy.kind = "tiered".into();
    policy.tiers.clear();

    assert_invalid(&config, "requires at least one tier");
}

#[test]
fn reject_zero_tier_weight() {
    let mut config = valid_config();

    let policy = config
        .rest
        .weight_policies
        .get_mut("depth_snapshot")
        .expect("Missing depth_snapshot weight policy");

    policy.tiers[0].weight = 0;

    assert_invalid(&config, "tier weight must be positive");
}

#[test]
fn reject_tier_without_threshold() {
    let mut config = valid_config();

    let policy = config
        .rest
        .weight_policies
        .get_mut("depth_snapshot")
        .expect("Missing depth_snapshot weight policy");

    policy.tiers[0].limit = None;
    policy.tiers[0].max = None;

    assert_invalid(&config, "tier requires limit or max");
}

// ============================================================
// CHANNEL VALIDATION
// ============================================================

#[test]
fn reject_missing_channels() {
    let mut config = valid_config();

    config.channels.clear();

    assert_invalid(&config, "at least one channel");
}

#[test]
fn reject_all_channels_disabled() {
    let mut config = valid_config();

    for channel in config.channels.values_mut() {
        channel.enabled = false;
    }

    assert_invalid(&config, "at least one enabled channel");
}

#[test]
fn reject_missing_enabled_trade_format() {
    let mut config = valid_config();

    config
        .channels
        .get_mut("trades")
        .expect("Missing trades channel")
        .format = None;

    assert_invalid(&config, "requires a normalization format");
}

#[test]
fn reject_missing_enabled_depth_format() {
    let mut config = valid_config();

    config
        .channels
        .get_mut("depth")
        .expect("Missing depth channel")
        .format = None;

    assert_invalid(&config, "requires a normalization format");
}

// ============================================================
// RECOVERY ENDPOINT REFERENCES
// ============================================================

#[test]
fn reject_missing_trade_recovery_endpoint() {
    let mut config = valid_config();

    config
        .channels
        .get_mut("trades")
        .expect("Missing trades channel")
        .recovery_endpoint = Some("missing_trades".into());

    assert_invalid(&config, "missing recovery endpoint");
}

#[test]
fn reject_missing_depth_snapshot_endpoint() {
    use crate::live::config::ExchangeConfig;

    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../data/.config/live/exchanges/gateio_spot.toml"
    );

    let mut config = ExchangeConfig::load(path).unwrap();

    let sync = config
        .channels
        .get_mut("depth")
        .unwrap()
        .synchronization
        .as_mut()
        .expect("Missing synchronization");

    sync.snapshot_endpoint = Some("nonexistent_snapshot".into());

    let error = config.validate().unwrap_err();

    assert!(
        error.contains("missing snapshot endpoint"),
        "Unexpected validation error: {error}"
    );
}

#[test]
fn reject_missing_rest_snapshot_endpoint() {
    use crate::live::config::ExchangeConfig;

    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../data/.config/live/exchanges/gateio_spot.toml"
    );

    let mut config = ExchangeConfig::load(path).unwrap();

    let sync = config
        .channels
        .get_mut("depth")
        .unwrap()
        .synchronization
        .as_mut()
        .expect("Missing synchronization");

    sync.rest_snapshot_endpoint = Some("nonexistent_rest_snapshot".into());

    let error = config.validate().unwrap_err();

    assert!(
        error.contains("missing REST snapshot endpoint"),
        "Unexpected validation error: {error}"
    );
}

// ============================================================
// DEPTH CAPABILITIES
// ============================================================

#[test]
fn reject_unsupported_default_depth() {
    let mut config = valid_config();

    let depth = config
        .channels
        .get_mut("depth")
        .expect("Missing depth channel");

    depth.supported_depths = vec![10, 20, 50];
    depth.default_depth = Some(100);

    assert_invalid(&config, "not listed in supported_depths");
}

#[test]
fn reject_unsupported_default_interval() {
    let mut config = valid_config();

    let depth = config
        .channels
        .get_mut("depth")
        .expect("Missing depth channel");

    depth.supported_intervals = vec!["20ms".into(), "100ms".into()];
    depth.default_interval = Some("500ms".into());

    assert_invalid(&config, "is not supported");
}

// ============================================================
// MARKET MAPPING ROUTES
// ============================================================

#[test]
fn reject_missing_native_websocket_route() {
    let mut config = ExchangeConfig::load(config_dir().join("gateio_linear.toml"))
        .expect("Cannot load Gate.io Linear fixture");

    let mapping = config
        .market_mapping
        .as_mut()
        .expect("Missing market mapping");

    let native = mapping
        .native_types
        .get_mut("usdt")
        .expect("Missing USDT native type");

    native.websocket_route = Some("nonexistent_route".into());

    assert_invalid(&config, "missing websocket route");
}

// ============================================================
// VALID PROFILES
// ============================================================
