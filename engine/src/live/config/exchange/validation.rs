use super::{ChannelConfig, ExchangeConfig, MarketMapping, RestConfig, WebSocketConfig};

use std::collections::BTreeSet;
impl ExchangeConfig {
    /// Validate the complete exchange configuration.
    ///
    /// Called after TOML deserialization and before the
    /// configuration becomes available to runtime components.
    pub fn validate(&self) -> Result<(), String> {
        self.validate_schema()?;
        self.exchange.validate()?;

        validate_websocket(&self.websocket)?;
        validate_rest(&self.rest)?;
        validate_channels(&self.channels, &self.rest, &self.websocket)?;

        if let Some(mapping) = &self.market_mapping {
            mapping.validate()?;
            validate_market_routes(mapping, &self.websocket)?;
        }

        Ok(())
    }

    fn validate_schema(&self) -> Result<(), String> {
        if self.schema_version != 1 {
            return Err(format!(
                "unsupported exchange schema version: {}",
                self.schema_version
            ));
        }

        Ok(())
    }
}

/// Validate WebSocket connectivity and protocol configuration.
fn validate_websocket(config: &WebSocketConfig) -> Result<(), String> {
    let has_url = config
        .url
        .as_ref()
        .is_some_and(|url| !url.trim().is_empty());

    let has_base_url = config
        .base_url
        .as_ref()
        .is_some_and(|url| !url.trim().is_empty());

    let has_routes = !config.routes.is_empty();

    if !has_url && !has_base_url && !has_routes {
        return Err("websocket requires url, base_url, or named routes".into());
    }

    for (name, route) in &config.routes {
        if name.trim().is_empty() {
            return Err("websocket route name cannot be empty".into());
        }

        let has_url = route.url.as_ref().is_some_and(|url| !url.trim().is_empty());

        let has_base_url = route
            .base_url
            .as_ref()
            .is_some_and(|url| !url.trim().is_empty());

        if !has_url && !has_base_url {
            return Err(format!("websocket.routes.{name} requires url or base_url"));
        }
    }

    // Heartbeat validation.
    if let Some(heartbeat) = &config.heartbeat {
        if heartbeat.interval_seconds == Some(0) {
            return Err("websocket.heartbeat.interval_seconds must be positive".into());
        }

        if heartbeat.response_timeout_seconds == Some(0) {
            return Err("websocket.heartbeat.response_timeout_seconds must be positive".into());
        }

        if heartbeat.timeout_seconds == Some(0) {
            return Err("websocket.heartbeat.timeout_seconds must be positive".into());
        }
    }

    // Subscription validation.
    if let Some(subscription) = &config.subscription {
        if subscription.strategy.trim().is_empty() {
            return Err("websocket.subscription.strategy cannot be empty".into());
        }

        if subscription.supports_unsubscribe == Some(true)
            && subscription
                .unsubscribe_method
                .as_ref()
                .is_some_and(|method| method.trim().is_empty())
        {
            return Err("websocket.subscription.unsubscribe_method cannot be empty".into());
        }
    }

    Ok(())
}

/// Validate REST endpoint definitions and policy references.
fn validate_rest(config: &RestConfig) -> Result<(), String> {
    if config.base_url.trim().is_empty() {
        return Err("rest.base_url cannot be empty".into());
    }

    if config.request_timeout_seconds == Some(0) {
        return Err("rest.request_timeout_seconds must be positive".into());
    }

    for (name, endpoint) in &config.endpoints {
        // ========================================================
        // APPLICATION RESPONSE CONFIGURATION
        // ========================================================

        if let Some(application) = &endpoint.application_response {
            application.validate().map_err(|reason| {
                format!("rest.endpoints.{name}.application_response: {reason}")
            })?;
        }
        let mut required = std::collections::HashSet::new();

        for parameter in &endpoint.required_parameters {
            if parameter.trim().is_empty() {
                return Err(format!(
                    "rest.endpoints.{name}: required parameter is empty"
                ));
            }

            if !required.insert(parameter) {
                return Err(format!(
                    "rest.endpoints.{name}: duplicate required parameter '{parameter}'"
                ));
            }

            if !endpoint.parameters.contains(parameter) {
                return Err(format!(
                    "rest.endpoints.{name}: required parameter '{parameter}' \
             is not declared in parameters"
                ));
            }
        }

        // ========================================================
        // NATIVE LIMIT PARAMETER
        // ========================================================

        if let Some(parameter) = &endpoint.limit_parameter {
            if parameter.trim().is_empty() {
                return Err(format!(
                    "rest.endpoints.{name}: limit_parameter cannot be empty"
                ));
            }

            if !endpoint.parameters.contains(parameter) {
                return Err(format!(
                    "rest.endpoints.{name}: limit_parameter '{parameter}' \
             is not declared in parameters"
                ));
            }
        }

        if name.trim().is_empty() {
            return Err("REST endpoint name cannot be empty".into());
        }

        if endpoint.method.trim().is_empty() {
            return Err(format!("rest.endpoints.{name}.method cannot be empty"));
        }

        if endpoint.path.trim().is_empty() {
            return Err(format!("rest.endpoints.{name}.path cannot be empty"));
        }

        if let (Some(default), Some(maximum)) = (endpoint.default_limit, endpoint.maximum_limit) {
            if default > maximum {
                return Err(format!(
                    "rest.endpoints.{name}: default_limit ({default}) \
                     exceeds maximum_limit ({maximum})"
                ));
            }
        }

        if let Some(policy) = &endpoint.rate_limit_policy {
            if !config.rate_limit_policies.contains_key(policy) {
                return Err(format!(
                    "rest.endpoints.{name} references missing \
                     rate_limit_policy '{policy}'"
                ));
            }
        }

        if let Some(policy) = &endpoint.weight_policy {
            if !config.weight_policies.contains_key(policy) {
                return Err(format!(
                    "rest.endpoints.{name} references missing \
                     weight_policy '{policy}'"
                ));
            }
        }
    }

    // Validate endpoint rate-limit policies.
    for (name, policy) in &config.rate_limit_policies {
        if policy.max_requests == Some(0) {
            return Err(format!(
                "rest.rate_limit_policies.{name}.max_requests must be positive"
            ));
        }

        if policy.window_seconds == Some(0) {
            return Err(format!(
                "rest.rate_limit_policies.{name}.window_seconds must be positive"
            ));
        }
    }

    // Validate request-weight policies.
    for (name, policy) in &config.weight_policies {
        match policy.kind.as_str() {
            "fixed" => {
                if policy.weight.is_none_or(|weight| weight == 0) {
                    return Err(format!(
                        "rest.weight_policies.{name}: \
                         fixed policy requires positive weight"
                    ));
                }
            }
            "request_count" => {
                if policy.cost.is_none_or(|cost| cost == 0) {
                    return Err(format!(
                        "rest.weight_policies.{name}: \
             request_count policy requires positive cost"
                    ));
                }
            }
            "discrete" | "tiered" => {
                if policy.tiers.is_empty() {
                    return Err(format!(
                        "rest.weight_policies.{name}: \
                         tiered policy requires at least one tier"
                    ));
                }

                for tier in &policy.tiers {
                    if tier.weight == 0 {
                        return Err(format!(
                            "rest.weight_policies.{name}: \
                             tier weight must be positive"
                        ));
                    }

                    if tier.limit.is_none() && tier.max.is_none() {
                        return Err(format!(
                            "rest.weight_policies.{name}: \
                             tier requires limit or max"
                        ));
                    }
                }
            }

            other => {
                return Err(format!(
                    "rest.weight_policies.{name}: \
                     unsupported weight policy type '{other}'"
                ));
            }
        }
    }

    Ok(())
}

/// Validate configured market-data channels.
fn validate_channels(
    channels: &std::collections::BTreeMap<String, ChannelConfig>,
    rest: &RestConfig,
    websocket: &WebSocketConfig,
) -> Result<(), String> {
    if channels.is_empty() {
        return Err("exchange must define at least one channel".into());
    }

    let mut enabled_count = 0;

    for (name, channel) in channels {
        if name.trim().is_empty() {
            return Err("channel name cannot be empty".into());
        }

        if !channel.enabled {
            continue;
        }

        enabled_count += 1;

        // Enabled canonical market-data channels require a format.
        if matches!(name.as_str(), "trades" | "aggregate_trades" | "depth") {
            if channel
                .format
                .as_ref()
                .is_none_or(|format| format.trim().is_empty())
            {
                return Err(format!(
                    "enabled channel '{name}' requires a normalization format"
                ));
            }
        }

        // Validate WebSocket route references.
        if let Some(route) = &channel.route {
            if !websocket.routes.is_empty() && !websocket.routes.contains_key(route) {
                return Err(format!(
                    "channel '{name}' references unknown websocket route '{route}'"
                ));
            }
        }

        // Validate trade recovery endpoint references.
        if let Some(endpoint) = &channel.recovery_endpoint {
            if !rest.endpoints.contains_key(endpoint) {
                return Err(format!(
                    "channel '{name}' references missing \
                     recovery endpoint '{endpoint}'"
                ));
            }
        }

        // Validate L2 synchronization.
        if let Some(sync) = &channel.synchronization {
            if let Some(endpoint) = &sync.snapshot_endpoint {
                if !rest.endpoints.contains_key(endpoint) {
                    return Err(format!(
                        "channel '{name}' references missing \
                         snapshot endpoint '{endpoint}'"
                    ));
                }
            }

            if let Some(endpoint) = &sync.rest_snapshot_endpoint {
                if !rest.endpoints.contains_key(endpoint) {
                    return Err(format!(
                        "channel '{name}' references missing \
                         REST snapshot endpoint '{endpoint}'"
                    ));
                }
            }
        }

        // Validate depth options.
        if let Some(default_depth) = channel.default_depth {
            if !channel.supported_depths.is_empty()
                && !channel.supported_depths.contains(&default_depth)
            {
                return Err(format!(
                    "channel '{name}': default_depth {default_depth} \
                     is not listed in supported_depths"
                ));
            }
        }

        if let Some(default_interval) = &channel.default_interval {
            if !channel.supported_intervals.is_empty()
                && !channel.supported_intervals.contains(default_interval)
            {
                return Err(format!(
                    "channel '{name}': default_interval '{default_interval}' \
                     is not supported"
                ));
            }
        }
    }

    if enabled_count == 0 {
        return Err("exchange must have at least one enabled channel".into());
    }

    Ok(())
}

/// Validate that native market mappings reference existing
/// WebSocket routes.
fn validate_market_routes(
    mapping: &MarketMapping,
    websocket: &WebSocketConfig,
) -> Result<(), String> {
    let available_routes: BTreeSet<&str> = websocket.routes.keys().map(String::as_str).collect();

    for (name, native_type) in mapping.native_types() {
        if let Some(route) = &native_type.websocket_route {
            if !available_routes.contains(route.as_str()) {
                return Err(format!(
                    "market_mapping.native_types.{name} references \
                     missing websocket route '{route}'"
                ));
            }
        }
    }

    Ok(())
}

// ============================================================
// SEMANTIC VALIDATION TESTS
// ============================================================
#[cfg(test)]
mod tests {
    use crate::live::config::exchange::RestApplicationResponseConfig;
    #[test]
    fn validates_raw_response() {
        let config: RestApplicationResponseConfig = toml::from_str(
            r#"
strategy = "raw"
"#,
        )
        .unwrap();

        assert!(config.validate().is_ok());
    }

    #[test]
    fn rejects_envelope_without_code_field() {
        let config: RestApplicationResponseConfig = toml::from_str(
            r#"
strategy = "envelope"
success_codes = ["0"]
"#,
        )
        .unwrap();

        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_envelope_without_success_codes() {
        let config: RestApplicationResponseConfig = toml::from_str(
            r#"
strategy = "envelope"
code_field = "code"
"#,
        )
        .unwrap();

        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_duplicate_success_codes() {
        let config: RestApplicationResponseConfig = toml::from_str(
            r#"
strategy = "envelope"
code_field = "code"
success_codes = ["0", "0"]
"#,
        )
        .unwrap();

        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_envelope_fields_for_raw_response() {
        let config: RestApplicationResponseConfig = toml::from_str(
            r#"
strategy = "raw"
code_field = "code"
success_codes = ["0"]
"#,
        )
        .unwrap();

        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_empty_optional_field_names() {
        let config: RestApplicationResponseConfig = toml::from_str(
            r#"
strategy = "envelope"
code_field = "code"
success_codes = ["0"]
message_field = ""
"#,
        )
        .unwrap();

        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_duplicate_envelope_field_names() {
        let config: RestApplicationResponseConfig = toml::from_str(
            r#"
strategy = "envelope"
code_field = "code"
success_codes = ["0"]
message_field = "code"
"#,
        )
        .unwrap();

        assert!(config.validate().is_err());
    }

    #[test]
    fn validates_complete_envelope() {
        let config: RestApplicationResponseConfig = toml::from_str(
            r#"
strategy = "envelope"
code_field = "retCode"
success_codes = ["0"]
message_field = "retMsg"
data_field = "result"
"#,
        )
        .unwrap();

        assert!(config.validate().is_ok());
    }

    mod behavior {
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

            ExchangeConfig::load(&path)
                .unwrap_or_else(|error| panic!("Cannot load test fixture: {error}"))
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
    }
}
