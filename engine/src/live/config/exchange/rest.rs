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
