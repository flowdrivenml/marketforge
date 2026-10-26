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
mod tests;
