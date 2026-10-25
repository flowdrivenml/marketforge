use std::{collections::BTreeMap, time::Duration};

use crate::live::infrastructure::limiter::RequestPriority;

// ============================================================
// HTTP METHOD
// ============================================================

/// HTTP methods supported by the public market-data REST client.
///
/// Authenticated trading operations are outside the scope
/// of MarketForge Live v1.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestMethod {
    Get,
    Post,
}

impl RestMethod {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Post => "POST",
        }
    }
}

// ============================================================
// REQUEST PARAMETERS
// ============================================================

/// Exchange-native REST request parameters.
///
/// Values remain strings because exchanges frequently accept
/// numeric identifiers, timestamps, and booleans as query
/// parameter strings.
///
/// The resolver is responsible for validating parameter names,
/// applying defaults, and checking endpoint constraints.
pub type RestParameters = BTreeMap<String, String>;

// ============================================================
// LOGICAL REQUEST
// ============================================================

/// One logical exchange REST operation.
///
/// The caller identifies:
/// - An exchange-market profile.
/// - A named REST endpoint.
/// - Exchange-native request parameters.
/// - The desired rate-limit admission priority.
///
/// The caller does not select arbitrary URLs, HTTP methods,
/// rate-limit budgets, or request weights.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestRequest {
    profile: String,
    endpoint: String,
    parameters: RestParameters,
    priority: RequestPriority,
}

impl RestRequest {
    // ========================================================
    // CONSTRUCTION
    // ========================================================

    pub fn new(profile: impl Into<String>, endpoint: impl Into<String>) -> Self {
        Self {
            profile: profile.into(),
            endpoint: endpoint.into(),
            parameters: RestParameters::new(),
            priority: RequestPriority::Normal,
        }
    }

    // ========================================================
    // BUILDER METHODS
    // ========================================================

    /// Add or replace an exchange-native parameter.
    ///
    /// Parameter validation is performed later by the resolver.
    pub fn with_parameter(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.parameters.insert(name.into(), value.into());
        self
    }

    /// Replace all caller-provided parameters.
    pub fn with_parameters(mut self, parameters: RestParameters) -> Self {
        self.parameters = parameters;
        self
    }

    /// Set admission priority for rate-limit scheduling.
    pub fn with_priority(mut self, priority: RequestPriority) -> Self {
        self.priority = priority;
        self
    }

    // ========================================================
    // ACCESSORS
    // ========================================================

    pub fn profile(&self) -> &str {
        &self.profile
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn parameters(&self) -> &RestParameters {
        &self.parameters
    }

    pub fn priority(&self) -> RequestPriority {
        self.priority
    }
}

// ============================================================
// RESOLVED REQUEST
// ============================================================

/// Immutable, fully resolved exchange REST request.
///
/// Produced only after endpoint lookup and request validation.
///
/// Contains the exact method, URL, parameters, and timeout
/// that the HTTP executor will use.
///
/// The same query parameters must be used when computing
/// endpoint-specific request weights.
///
/// The URL contains the resolved endpoint path, but query
/// parameters are stored separately for proper URL encoding
/// by the HTTP client.
#[derive(Debug, Clone)]
pub struct ResolvedRestRequest {
    profile: String,
    endpoint: String,

    method: RestMethod,

    url: String,

    query: RestParameters,

    timeout: Duration,

    priority: RequestPriority,
}

impl ResolvedRestRequest {
    // ========================================================
    // INTERNAL CONSTRUCTION
    // ========================================================

    /// Construct a resolved request.
    ///
    /// This constructor is restricted to the REST module.
    /// Callers must use the endpoint resolver rather than
    /// supplying arbitrary URLs and methods.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        profile: String,
        endpoint: String,
        method: RestMethod,
        url: String,
        query: RestParameters,
        timeout: Duration,
        priority: RequestPriority,
    ) -> Self {
        Self {
            profile,
            endpoint,
            method,
            url,
            query,
            timeout,
            priority,
        }
    }

    // ========================================================
    // ACCESSORS
    // ========================================================

    pub fn profile(&self) -> &str {
        &self.profile
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn method(&self) -> RestMethod {
        self.method
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn query(&self) -> &RestParameters {
        &self.query
    }

    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    pub fn priority(&self) -> RequestPriority {
        self.priority
    }

    /// Return the effective value of a resolved query parameter.
    pub fn parameter(&self, name: &str) -> Option<&str> {
        self.query.get(name).map(String::as_str)
    }

    /// Retrieve an unsigned numeric parameter.
    ///
    /// Used by request-weight policies that depend on values
    /// such as the requested depth snapshot limit.
    ///
    /// Returns None when the parameter is missing or is not
    /// a valid unsigned integer.
    ///
    /// A malformed parameter must be rejected by the resolver
    /// before this object reaches the executor.
    pub fn numeric_parameter(&self, name: &str) -> Option<u64> {
        self.parameter(name)?.parse::<u64>().ok()
    }
}

// ============================================================
// TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================
    // LOGICAL REQUEST
    // ========================================================

    #[test]
    fn creates_logical_request() {
        let request = RestRequest::new("binance_spot", "depth_snapshot");

        assert_eq!(request.profile(), "binance_spot");
        assert_eq!(request.endpoint(), "depth_snapshot");

        assert!(request.parameters().is_empty());

        assert_eq!(request.priority(), RequestPriority::Normal,);
    }

    #[test]
    fn supports_native_parameters() {
        let request = RestRequest::new("okx_spot", "depth_snapshot")
            .with_parameter("instId", "BTC-USDT")
            .with_parameter("sz", "400");

        assert_eq!(
            request.parameters().get("instId"),
            Some(&"BTC-USDT".to_string()),
        );

        assert_eq!(request.parameters().get("sz"), Some(&"400".to_string()),);
    }

    #[test]
    fn replacing_parameter_does_not_create_duplicates() {
        let request = RestRequest::new("binance_spot", "depth_snapshot")
            .with_parameter("limit", "100")
            .with_parameter("limit", "1000");

        assert_eq!(request.parameters().len(), 1);

        assert_eq!(request.parameters().get("limit"), Some(&"1000".to_string()),);
    }

    #[test]
    fn supports_request_priority() {
        let request =
            RestRequest::new("binance_spot", "depth_snapshot").with_priority(RequestPriority::High);

        assert_eq!(request.priority(), RequestPriority::High,);
    }

    // ========================================================
    // RESOLVED REQUEST
    // ========================================================

    fn resolved_request() -> ResolvedRestRequest {
        let mut query = RestParameters::new();

        query.insert("symbol".into(), "BTCUSDT".into());
        query.insert("limit".into(), "1000".into());

        ResolvedRestRequest::new(
            "binance_spot".into(),
            "depth_snapshot".into(),
            RestMethod::Get,
            "https://api.binance.com/api/v3/depth".into(),
            query,
            Duration::from_secs(10),
            RequestPriority::High,
        )
    }

    #[test]
    fn preserves_resolved_request_metadata() {
        let request = resolved_request();

        assert_eq!(request.profile(), "binance_spot");
        assert_eq!(request.endpoint(), "depth_snapshot");

        assert_eq!(request.method(), RestMethod::Get);

        assert_eq!(request.url(), "https://api.binance.com/api/v3/depth",);

        assert_eq!(request.timeout(), Duration::from_secs(10),);

        assert_eq!(request.priority(), RequestPriority::High,);
    }

    #[test]
    fn extracts_numeric_parameters() {
        let request = resolved_request();

        assert_eq!(request.numeric_parameter("limit"), Some(1000),);

        assert_eq!(request.numeric_parameter("symbol"), None,);

        assert_eq!(request.numeric_parameter("missing"), None,);
    }

    #[test]
    fn preserves_exact_parameter_values() {
        let request = resolved_request();

        assert_eq!(request.parameter("symbol"), Some("BTCUSDT"),);

        assert_eq!(request.parameter("limit"), Some("1000"),);
    }

    #[test]
    fn http_methods_have_stable_representations() {
        assert_eq!(RestMethod::Get.as_str(), "GET");
        assert_eq!(RestMethod::Post.as_str(), "POST");
    }
}
