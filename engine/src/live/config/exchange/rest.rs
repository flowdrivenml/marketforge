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
