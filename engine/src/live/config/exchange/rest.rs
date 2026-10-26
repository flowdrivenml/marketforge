use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Complete REST API configuration for an exchange profile.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestConfig {
    /// Base REST API URL.
    pub base_url: String,

    /// Default authentication strategy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authentication: Option<String>,

    /// Request timeout in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_timeout_seconds: Option<u64>,

    /// Named REST endpoints.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub endpoints: BTreeMap<String, RestEndpoint>,

    /// Global REST rate-limiting configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rate_limiting: Option<RestRateLimiting>,

    /// Named endpoint rate-limit policies.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub rate_limit_policies: BTreeMap<String, RateLimitPolicy>,

    /// Named request-weight policies.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub weight_policies: BTreeMap<String, WeightPolicy>,
}
/// Native field mappings for one REST trade endpoint.
///
/// This describes the source format only.
/// Economic conversion remains the responsibility of
/// the existing normalization pipeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestTradeNormalizationConfig {
    pub trade_id: String,
    pub price: String,
    pub quantity: String,
    pub timestamp: String,

    pub timestamp_unit: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub side: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sequence: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub symbol: Option<String>,

    /// "mapped" or "signed_quantity".
    pub side_strategy: String,

    /// Required when side_strategy = "signed_quantity".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub positive_quantity_side: Option<String>,

    /// Native side values mapped to canonical buy/sell.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub side_mappings: BTreeMap<String, String>,
    /// Caller-supplied REST parameter identifying the native instrument.
    ///
    /// This is separate from `symbol`, which identifies a field
    /// inside an individual REST trade record.
    pub request_symbol_parameter: String,
}
/// Configuration for a single REST endpoint.
///
/// The endpoint name is the key in RestConfig.endpoints.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestEndpoint {
    /// HTTP method.
    pub method: String,
    /// Exchange-native currency-pair parameter template.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub currency_pair: Option<String>,
    /// Exchange-specific default pagination mode.
    ///
    /// OKX uses numeric types to distinguish trade-ID and
    /// timestamp-based historical pagination.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_pagination_type: Option<u64>,
    /// Endpoint path, potentially containing placeholders.
    pub path: String,

    /// Supported request parameters.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub parameters: Vec<String>,

    // ---------------------------------------------------------
    // REQUEST PARAMETERS
    // ---------------------------------------------------------
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub category: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub settlement: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contract: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub with_id: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_limit: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub maximum_limit: Option<u64>,

    // ---------------------------------------------------------
    // AUTHENTICATION
    // ---------------------------------------------------------
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authentication: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authentication_header: Option<String>,

    // ---------------------------------------------------------
    // RESPONSE STRUCTURE
    // ---------------------------------------------------------
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_root: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_symbol_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_sequence_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_update_id_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_cross_sequence_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_timestamp_field: Option<String>,
    /// Unit of response_timestamp_field.
    ///
    /// Valid values:
    /// - seconds
    /// - milliseconds
    /// - microseconds
    /// - nanoseconds
    ///
    /// Required when response_timestamp_field is configured.
    /// Forbidden when no timestamp field is configured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_timestamp_unit: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_bids_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_asks_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_level_type: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_price_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_quantity_field: Option<String>,

    // ---------------------------------------------------------
    // PAGINATION
    // ---------------------------------------------------------
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pagination_strategy: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pagination_parameter: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preferred_pagination: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub legacy_id_pagination: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_history_days: Option<u64>,

    // ---------------------------------------------------------
    // RATE-LIMIT POLICY REFERENCES
    // ---------------------------------------------------------
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rate_limit_policy: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub weight_policy: Option<String>,

    /// Maximum historical query horizon in hours.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_history_hours: Option<u64>,

    /// Maximum permitted query time window in milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_time_window_ms: Option<u64>,

    /// Parameters that must be resolvable before execution.
    ///
    /// Names refer to exchange-native request parameters.
    ///
    /// Requirements can be satisfied by caller values,
    /// configured fixed values, or configured defaults.
    ///
    /// Template variables used only for path substitution
    /// are validated separately by the resolver.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_parameters: Vec<String>,

    /// Exchange-native parameter controlled by default_limit
    /// and maximum_limit.
    ///
    /// Examples:
    /// - Binance: "limit"
    /// - Bybit:   "limit"
    /// - OKX:     "sz"
    ///
    /// When absent, the resolver may use "limit" only if
    /// that parameter is declared by this endpoint.
    ///
    /// An explicitly configured name takes precedence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit_parameter: Option<String>,

    /// Application-level response validation configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub application_response: Option<RestApplicationResponseConfig>,

    /// Shape of the native snapshot at response_root.
    ///
    /// Supported:
    /// - object
    /// - single_object_array
    ///
    /// Required whenever response_root is configured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_record_container: Option<String>,

    /// Optional trade normalization definition.
    ///
    /// Required by the REST trade normalization compiler,
    /// but not by unrelated REST endpoints.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trade_normalization: Option<RestTradeNormalizationConfig>,
}

/// Global REST rate-limiting configuration.
///
/// Different exchanges use different rate-limit strategies.
/// Optional fields represent their respective capabilities.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestRateLimiting {
    pub strategy: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rule: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limits_source: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage_header_prefix: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http_throttled_status: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http_banned_status: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub honor_retry_after: Option<bool>,

    // Shared request windows
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ip_window_seconds: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ip_max_requests: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub global_max_requests: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub global_window_seconds: Option<u64>,

    // Exchange-specific throttling signals
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rate_limit_error_code: Option<RateLimitErrorCode>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ip_limit_http_status: Option<u64>,
    /// Exchange response headers used for rate-limit tracking.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub headers: BTreeMap<String, toml::Value>,
}

/// Named REST rate-limit policy.
///
/// Some policies contain verified request quotas.
/// Others explicitly require verification before use.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RateLimitPolicy {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_requests: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window_seconds: Option<u64>,
}

/// REST request-weight calculation strategy.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WeightPolicy {
    /// Examples: fixed, discrete, tiered.
    #[serde(rename = "type")]
    pub kind: String,

    /// Fixed request weight.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub weight: Option<u64>,

    /// Parameter used to determine request weight.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parameter: Option<String>,

    /// Weight tiers.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tiers: Vec<WeightTier>,

    /// Request cost used by exchange-specific weight policies.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost: Option<u64>,
}

/// One request-weight tier.
///
/// Supports both exact-limit and maximum-limit thresholds.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WeightTier {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<u64>,

    pub weight: u64,
}

impl RestConfig {
    /// Retrieve a configured REST endpoint.
    pub fn endpoint(&self, name: &str) -> Option<&RestEndpoint> {
        self.endpoints.get(name)
    }

    /// Retrieve a named rate-limit policy.
    pub fn rate_limit_policy(&self, name: &str) -> Option<&RateLimitPolicy> {
        self.rate_limit_policies.get(name)
    }

    /// Retrieve a named request-weight policy.
    pub fn weight_policy(&self, name: &str) -> Option<&WeightPolicy> {
        self.weight_policies.get(name)
    }
}

/// Exchange-native rate-limit error code.
///
/// Some exchanges use integer codes, while others use strings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RateLimitErrorCode {
    Integer(i64),
    String(String),
}
/// - Gate.io: Raw
/// - Bybit: Envelope
/// - OKX: Envelope
/// - Bitget: Envelope
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RestApplicationResponseStrategy {
    Raw,
    Envelope,
}
/// Describes how an exchange REST endpoint reports
/// application-level success or failure.
///
/// This is independent of the HTTP status code.
///
/// Supported strategies:
///
/// - Raw: The endpoint returns JSON without a mandatory
///   application-level success code.
///
/// - Envelope: The endpoint wraps its response in an
///   application-status envelope containing a success code.
///
/// Examples:
///
/// - Binance: Raw
/// - Gate.io: Raw
/// - Bybit: Envelope (retCode == 0)
/// - OKX: Envelope (code == "0")
/// - Bitget: Envelope (code == "00000")
///
/// The configuration is validated during startup.
/// Actual response validation belongs to the REST runtime.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestApplicationResponseConfig {
    /// Strategy used to interpret the response.
    pub strategy: RestApplicationResponseStrategy,

    /// JSON field containing the application status code.
    ///
    /// Required for Envelope.
    /// Must be absent for Raw.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code_field: Option<String>,

    /// Application codes indicating successful execution.
    ///
    /// Required for Envelope.
    /// Must be empty for Raw.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub success_codes: Vec<String>,

    /// Optional JSON field containing the exchange's
    /// application-level message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_field: Option<String>,

    /// Optional JSON field containing the response payload.
    ///
    /// When configured, the runtime validator requires
    /// this field to exist and be non-null on success.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_field: Option<String>,
}
// ============================================================
// APPLICATION RESPONSE VALIDATION
// ============================================================

impl RestApplicationResponseConfig {
    /// Validate exchange application-response semantics.
    ///
    /// This is configuration validation only.
    ///
    /// Actual JSON response validation is performed by
    /// RestApplicationValidator at runtime.
    pub fn validate(&self) -> Result<(), String> {
        match self.strategy {
            // =================================================
            // RAW RESPONSE
            // =================================================
            RestApplicationResponseStrategy::Raw => {
                if self.code_field.is_some()
                    || !self.success_codes.is_empty()
                    || self.message_field.is_some()
                    || self.data_field.is_some()
                {
                    return Err("raw application responses must not \
                         declare envelope fields"
                        .into());
                }
            }

            // =================================================
            // APPLICATION ENVELOPE
            // =================================================
            RestApplicationResponseStrategy::Envelope => {
                let code_field = self
                    .code_field
                    .as_deref()
                    .ok_or("envelope response requires code_field")?;

                if code_field.trim().is_empty() {
                    return Err("envelope code_field cannot be empty".into());
                }

                if self.success_codes.is_empty() {
                    return Err("envelope response requires \
                         at least one success code"
                        .into());
                }

                let mut seen = std::collections::HashSet::new();

                for code in &self.success_codes {
                    if code.trim().is_empty() {
                        return Err("envelope success codes cannot \
                             be empty"
                            .into());
                    }

                    if !seen.insert(code) {
                        return Err(format!("duplicate envelope success code '{code}'"));
                    }
                }

                for (name, value) in [
                    ("message_field", &self.message_field),
                    ("data_field", &self.data_field),
                ] {
                    if value
                        .as_deref()
                        .is_some_and(|value| value.trim().is_empty())
                    {
                        return Err(format!("envelope {name} cannot be empty"));
                    }
                }

                // Avoid declaring two envelope components
                // that refer to the same JSON field.
                let mut fields = std::collections::HashSet::new();

                fields.insert(code_field);

                for (name, value) in [
                    ("message_field", &self.message_field),
                    ("data_field", &self.data_field),
                ] {
                    if let Some(value) = value.as_deref() {
                        if !fields.insert(value) {
                            return Err(format!(
                                "envelope {name} duplicates \
                                 another response field"
                            ));
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::live::config::RestConfig;
    use crate::live::config::exchange::RateLimitErrorCode;

    // ============================================================
    // HELPERS
    // ============================================================

    fn parse_rest(input: &str) -> RestConfig {
        toml::from_str(input).unwrap_or_else(|error| panic!("Failed to parse REST config: {error}"))
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

    // ============================================================
    // FULL CONFIGURATION INTEGRATION
    // ============================================================
}
