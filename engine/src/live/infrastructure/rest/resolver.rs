use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

use url::Url;

use crate::live::{
    config::{
        ExchangeConfigStore,
        exchange::{ExchangeConfig, RestEndpoint},
    },
    infrastructure::rest::request::{ResolvedRestRequest, RestMethod, RestParameters, RestRequest},
};

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RestResolveError {
    #[error("unknown exchange profile '{0}'")]
    UnknownProfile(String),

    #[error("unknown REST endpoint '{endpoint}' in profile '{profile}'")]
    UnknownEndpoint { profile: String, endpoint: String },

    #[error("unsupported HTTP method '{0}'")]
    UnsupportedMethod(String),

    #[error("authenticated REST endpoint is not supported")]
    AuthenticationRequired,

    #[error("invalid REST base URL: {0}")]
    InvalidBaseUrl(String),

    #[error("invalid REST endpoint path: {0}")]
    InvalidPath(String),

    #[error("missing required parameter '{0}'")]
    MissingParameter(String),

    #[error("unsupported request parameter '{0}'")]
    UnsupportedParameter(String),

    #[error("caller cannot override fixed parameter '{0}'")]
    FixedParameterOverride(String),

    #[error("invalid parameter '{name}': {reason}")]
    InvalidParameter { name: String, reason: String },

    #[error("unresolved template variable '{0}'")]
    UnresolvedTemplate(String),

    #[error("invalid REST request timeout")]
    InvalidTimeout,
}

// ============================================================
// REST REQUEST RESOLVER
// ============================================================

/// Resolves named exchange REST operations into immutable,
/// validated HTTP requests.
///
/// Does not perform HTTP requests, retries, or rate limiting.
///
/// Exchange configurations are immutable after initialization.
pub struct RestRequestResolver<'a> {
    configs: &'a ExchangeConfigStore,
}

impl<'a> RestRequestResolver<'a> {
    pub fn new(configs: &'a ExchangeConfigStore) -> Self {
        Self { configs }
    }

    // ========================================================
    // PUBLIC API
    // ========================================================

    pub fn resolve(&self, request: &RestRequest) -> Result<ResolvedRestRequest, RestResolveError> {
        let config = self
            .configs
            .get(request.profile())
            .ok_or_else(|| RestResolveError::UnknownProfile(request.profile().to_owned()))?;

        let endpoint = config.endpoint(request.endpoint()).ok_or_else(|| {
            RestResolveError::UnknownEndpoint {
                profile: request.profile().to_owned(),
                endpoint: request.endpoint().to_owned(),
            }
        })?;

        Self::resolve_endpoint(config, endpoint, request)
    }

    /// Resolve an endpoint without requiring a config store.
    ///
    /// Useful for isolated unit tests and startup validation.
    pub fn resolve_endpoint(
        config: &ExchangeConfig,
        endpoint: &RestEndpoint,
        request: &RestRequest,
    ) -> Result<ResolvedRestRequest, RestResolveError> {
        if request.profile() != config.profile() {
            return Err(RestResolveError::UnknownProfile(
                request.profile().to_owned(),
            ));
        }

        let method = Self::resolve_method(&endpoint.method)?;

        Self::validate_authentication(config, endpoint)?;

        let timeout = Self::resolve_timeout(config)?;

        // Resolve effective parameters once.
        //
        // The first result contains outgoing HTTP query parameters.
        // The second contains all resolved values available for
        // URL path substitution.
        let (query, path_values) = Self::resolve_parameters(endpoint, request)?;

        let url = Self::resolve_url(&config.rest.base_url, &endpoint.path, &path_values)?;

        Ok(ResolvedRestRequest::new(
            request.profile().to_owned(),
            request.endpoint().to_owned(),
            method,
            url,
            query,
            timeout,
            request.priority(),
        ))
    }

    // ========================================================
    // HTTP METHOD
    // ========================================================

    fn resolve_method(method: &str) -> Result<RestMethod, RestResolveError> {
        match method {
            "GET" => Ok(RestMethod::Get),

            // POST bodies are not yet part of our public
            // market-data REST request contract.
            other => Err(RestResolveError::UnsupportedMethod(other.into())),
        }
    }

    // ========================================================
    // AUTHENTICATION
    // ========================================================

    fn validate_authentication(
        config: &ExchangeConfig,
        endpoint: &RestEndpoint,
    ) -> Result<(), RestResolveError> {
        // An endpoint-specific strategy overrides the
        // exchange-wide default.
        let authentication = endpoint
            .authentication
            .as_deref()
            .or(config.rest.authentication.as_deref());

        match authentication {
            None | Some("none") | Some("public") => Ok(()),

            Some(_) => Err(RestResolveError::AuthenticationRequired),
        }
    }

    // ========================================================
    // TIMEOUT
    // ========================================================

    fn resolve_timeout(config: &ExchangeConfig) -> Result<Duration, RestResolveError> {
        let seconds = config.rest.request_timeout_seconds.unwrap_or(10);

        if seconds == 0 {
            return Err(RestResolveError::InvalidTimeout);
        }

        Ok(Duration::from_secs(seconds))
    }

    // ========================================================
    // PARAMETERS
    // ========================================================

    fn resolve_parameters(
        endpoint: &RestEndpoint,
        request: &RestRequest,
    ) -> Result<(RestParameters, RestParameters), RestResolveError> {
        let caller = request.parameters();

        // ========================================================
        // DECLARED OUTGOING PARAMETERS
        // ========================================================

        let allowed: BTreeSet<&str> = endpoint.parameters.iter().map(String::as_str).collect();

        // ========================================================
        // FIXED CONFIGURATION VALUES
        // ========================================================

        let fixed = Self::fixed_parameters(endpoint);

        // ========================================================
        // DISCOVER TEMPLATE INPUTS
        // ========================================================

        let mut template_inputs = BTreeSet::new();

        for value in fixed.values() {
            if let Some(name) = Self::template_name(value) {
                template_inputs.insert(name.to_owned());
            }
        }

        for name in Self::path_templates(&endpoint.path)? {
            // A path variable may be resolved either from an
            // endpoint-defined fixed value or directly from
            // an explicitly supplied caller parameter.
            if !fixed.contains_key(&name) {
                template_inputs.insert(name);
            }
        }

        // ========================================================
        // VALIDATE CALLER PARAMETERS
        // ========================================================

        for (name, value) in caller {
            Self::validate_value(name, value)?;

            if fixed.contains_key(name) {
                return Err(RestResolveError::FixedParameterOverride(name.clone()));
            }

            if !allowed.contains(name.as_str()) && !template_inputs.contains(name) {
                return Err(RestResolveError::UnsupportedParameter(name.clone()));
            }
        }

        // ========================================================
        // RESOLVE FIXED VALUES
        // ========================================================

        // Begin with caller values because they may contain
        // template inputs such as:
        //
        // symbol = BTC_USDT
        // native_rest_category = USDT-FUTURES
        //
        // Configured fields are then resolved against those
        // inputs and made available for URL construction.
        let mut effective = caller.clone();

        for (name, template) in &fixed {
            let value = Self::resolve_value(template, caller)?;

            Self::validate_value(name, &value)?;

            effective.insert(name.clone(), value);
        }

        // ========================================================
        // BUILD OUTGOING QUERY PARAMETERS
        // ========================================================

        let mut query = RestParameters::new();

        for name in &endpoint.parameters {
            if let Some(value) = effective.get(name) {
                query.insert(name.clone(), value.clone());
            }
        }

        // ========================================================
        // RESOLVE NATIVE LIMIT PARAMETER
        // ========================================================

        let limit_parameter = match &endpoint.limit_parameter {
            Some(name) => {
                if !allowed.contains(name.as_str()) {
                    return Err(RestResolveError::InvalidParameter {
                        name: name.clone(),
                        reason: "limit parameter is not declared".into(),
                    });
                }

                Some(name.as_str())
            }

            None if allowed.contains("limit") => Some("limit"),

            None => None,
        };

        if let Some(name) = limit_parameter {
            if !query.contains_key(name) {
                if let Some(default) = endpoint.default_limit {
                    if default == 0 {
                        return Err(RestResolveError::InvalidParameter {
                            name: name.into(),
                            reason: "default limit must be positive".into(),
                        });
                    }

                    query.insert(name.to_owned(), default.to_string());
                }
            }

            // Validate the effective value, including caller
            // values, against the endpoint's maximum.
            if let Some(value) = query.get(name) {
                let parsed =
                    value
                        .parse::<u64>()
                        .map_err(|_| RestResolveError::InvalidParameter {
                            name: name.into(),
                            reason: "expected an unsigned integer".into(),
                        })?;

                if parsed == 0 {
                    return Err(RestResolveError::InvalidParameter {
                        name: name.into(),
                        reason: "limit must be positive".into(),
                    });
                }

                if let Some(maximum) = endpoint.maximum_limit {
                    if parsed > maximum {
                        return Err(RestResolveError::InvalidParameter {
                            name: name.into(),
                            reason: format!("must be between 1 and {maximum}"),
                        });
                    }
                }
            }
        } else if endpoint.default_limit.is_some() || endpoint.maximum_limit.is_some() {
            // Do not silently ignore configured limit rules
            // when the native limit parameter is unknown.
            return Err(RestResolveError::InvalidParameter {
                name: "limit_parameter".into(),
                reason: "endpoint defines limit constraints \
                     without a declared native limit parameter"
                    .into(),
            });
        }

        // ========================================================
        // VALIDATE REQUIRED OUTGOING PARAMETERS
        // ========================================================

        for name in &endpoint.required_parameters {
            if !allowed.contains(name.as_str()) {
                return Err(RestResolveError::InvalidParameter {
                    name: name.clone(),
                    reason: "required parameter is not declared".into(),
                });
            }

            if !query.contains_key(name) {
                return Err(RestResolveError::MissingParameter(name.clone()));
            }
        }

        // ========================================================
        // INCLUDE EFFECTIVE QUERY DEFAULTS IN PATH CONTEXT
        // ========================================================

        // The complete effective parameter map is used for
        // path substitution.
        //
        // This also includes any native limit defaults that
        // were resolved above.
        effective.extend(query.iter().map(|(k, v)| (k.clone(), v.clone())));

        // ========================================================
        // VALIDATE ALL PATH PLACEHOLDERS
        // ========================================================

        for name in Self::path_templates(&endpoint.path)? {
            if !effective.contains_key(&name) {
                return Err(RestResolveError::UnresolvedTemplate(name));
            }
        }

        Ok((query, effective))
    }
    fn fixed_parameters(endpoint: &RestEndpoint) -> BTreeMap<String, String> {
        let mut result = BTreeMap::new();

        if let Some(value) = &endpoint.category {
            result.insert("category".into(), value.clone());
        }

        if let Some(value) = &endpoint.settlement {
            result.insert("settlement".into(), value.clone());
        }

        if let Some(value) = &endpoint.contract {
            result.insert("contract".into(), value.clone());
        }

        if let Some(value) = &endpoint.interval {
            result.insert("interval".into(), value.clone());
        }

        if let Some(value) = endpoint.with_id {
            result.insert("with_id".into(), value.to_string());
        }

        if let Some(value) = endpoint.default_pagination_type {
            result.insert("type".into(), value.to_string());
        }

        if let Some(value) = &endpoint.currency_pair {
            result.insert("currency_pair".into(), value.clone());
        }

        result
    }

    // ========================================================
    // TEMPLATE RESOLUTION
    // ========================================================

    fn template_name(value: &str) -> Option<&str> {
        if value.starts_with('{') && value.ends_with('}') && value.len() > 2 {
            let name = &value[1..value.len() - 1];

            if !name.contains('{') && !name.contains('}') {
                return Some(name);
            }
        }

        None
    }

    fn resolve_value(template: &str, inputs: &RestParameters) -> Result<String, RestResolveError> {
        if let Some(name) = Self::template_name(template) {
            let value = inputs
                .get(name)
                .ok_or_else(|| RestResolveError::UnresolvedTemplate(name.to_owned()))?;

            return Ok(value.clone());
        }

        if template.contains('{') || template.contains('}') {
            return Err(RestResolveError::InvalidParameter {
                name: "template".into(),
                reason: format!("unsupported template '{template}'"),
            });
        }

        Ok(template.to_owned())
    }

    fn path_templates(path: &str) -> Result<BTreeSet<String>, RestResolveError> {
        let mut names = BTreeSet::new();

        for segment in path.split('/') {
            if let Some(name) = Self::template_name(segment) {
                names.insert(name.to_owned());
            } else if segment.contains('{') || segment.contains('}') {
                return Err(RestResolveError::InvalidPath(format!(
                    "unsupported path template '{segment}'"
                )));
            }
        }

        Ok(names)
    }

    // ========================================================
    // URL RESOLUTION
    // ========================================================

    fn resolve_url(
        base_url: &str,
        endpoint_path: &str,
        inputs: &RestParameters,
    ) -> Result<String, RestResolveError> {
        let base = Url::parse(base_url)
            .map_err(|error| RestResolveError::InvalidBaseUrl(error.to_string()))?;

        let is_https = base.scheme() == "https";

        let is_local_http = base.scheme() == "http"
            && matches!(base.host_str(), Some("localhost" | "127.0.0.1" | "::1"));

        if (!is_https && !is_local_http)
            || base.host_str().is_none()
            || !base.username().is_empty()
            || base.password().is_some()
            || base.query().is_some()
            || base.fragment().is_some()
        {
            return Err(RestResolveError::InvalidBaseUrl(
                "expected HTTPS URL without credentials, query, or fragment; \
                 HTTP is allowed only for local tests"
                    .into(),
            ));
        }

        if endpoint_path.is_empty()
            || !endpoint_path.starts_with('/')
            || endpoint_path.starts_with("//")
            || endpoint_path.contains('?')
            || endpoint_path.contains('#')
            || endpoint_path.contains('\\')
        {
            return Err(RestResolveError::InvalidPath(endpoint_path.into()));
        }

        let mut segments = Vec::new();

        for segment in endpoint_path.trim_start_matches('/').split('/') {
            let resolved = Self::resolve_value(segment, inputs)?;

            if resolved.is_empty()
                || resolved == "."
                || resolved == ".."
                || resolved.contains('/')
                || resolved.contains('\\')
                || resolved.contains('\0')
                || resolved.contains('%')
            {
                return Err(RestResolveError::InvalidPath(format!(
                    "unsafe path segment '{resolved}'"
                )));
            }

            segments.push(resolved);
        }

        // Preserve the configured base URL path.
        //
        // Gate.io, for example, may use a base containing
        // /api/v4 and a relative endpoint path.
        let base_path = base.path().trim_end_matches('/');

        let mut result = base.clone();

        let mut full_segments: Vec<String> = base_path
            .split('/')
            .filter(|segment| !segment.is_empty())
            .map(str::to_owned)
            .collect();

        full_segments.extend(segments);

        {
            let mut path = result.path_segments_mut().map_err(|_| {
                RestResolveError::InvalidBaseUrl("cannot modify base URL path".into())
            })?;

            path.clear();

            for segment in &full_segments {
                path.push(segment);
            }
        }

        // The URL library safely percent-encodes each
        // segment independently.
        Ok(result.into())
    }

    // ========================================================
    // PARAMETER VALIDATION
    // ========================================================

    fn validate_value(name: &str, value: &str) -> Result<(), RestResolveError> {
        if name.trim().is_empty() {
            return Err(RestResolveError::InvalidParameter {
                name: name.into(),
                reason: "parameter name is empty".into(),
            });
        }

        if value.is_empty() {
            return Err(RestResolveError::InvalidParameter {
                name: name.into(),
                reason: "parameter value is empty".into(),
            });
        }

        if value.contains('\0') {
            return Err(RestResolveError::InvalidParameter {
                name: name.into(),
                reason: "NUL bytes are forbidden".into(),
            });
        }

        Ok(())
    }
}

// ============================================================
// TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    use crate::live::infrastructure::limiter::RequestPriority;

    fn config() -> ExchangeConfig {
        toml::from_str(
            r#"
schema_version = 1

[exchange]
id = "binance"
market = "spot"
profile = "binance_spot"

[websocket]
url = "wss://stream.binance.com/ws"

[channels.trades]
enabled = true
format = "trades/binance_spot"

[rest]
base_url = "https://api.binance.com"
authentication = "none"
request_timeout_seconds = 7

[rest.endpoints.depth_snapshot]
method = "GET"
path = "/api/v3/depth"
parameters = ["symbol", "limit"]
default_limit = 100
maximum_limit = 1000
"#,
        )
        .unwrap()
    }

    fn endpoint() -> RestEndpoint {
        config()
            .rest
            .endpoints
            .get("depth_snapshot")
            .unwrap()
            .clone()
    }

    fn request() -> RestRequest {
        RestRequest::new("binance_spot", "depth_snapshot").with_parameter("symbol", "BTCUSDT")
    }

    // ========================================================
    // BASIC RESOLUTION
    // ========================================================

    #[test]
    fn resolves_get_request() {
        let config = config();

        let resolved =
            RestRequestResolver::resolve_endpoint(&config, &endpoint(), &request()).unwrap();

        assert_eq!(resolved.method(), RestMethod::Get);

        assert_eq!(resolved.url(), "https://api.binance.com/api/v3/depth",);

        assert_eq!(resolved.parameter("symbol"), Some("BTCUSDT"),);

        assert_eq!(resolved.parameter("limit"), Some("100"),);

        assert_eq!(resolved.timeout(), Duration::from_secs(7),);
    }

    #[test]
    fn preserves_caller_limit() {
        let config = config();

        let resolved = RestRequestResolver::resolve_endpoint(
            &config,
            &endpoint(),
            &request().with_parameter("limit", "500"),
        )
        .unwrap();

        assert_eq!(resolved.numeric_parameter("limit"), Some(500),);
    }

    #[test]
    fn rejects_excessive_limit() {
        let config = config();

        let result = RestRequestResolver::resolve_endpoint(
            &config,
            &endpoint(),
            &request().with_parameter("limit", "2000"),
        );

        assert!(matches!(
            result,
            Err(RestResolveError::InvalidParameter {
                name,
                ..
            }) if name == "limit"
        ));
    }

    #[test]
    fn rejects_unsupported_parameter() {
        let config = config();

        let result = RestRequestResolver::resolve_endpoint(
            &config,
            &endpoint(),
            &request().with_parameter("unexpected", "123"),
        );

        assert!(matches!(
            result,
            Err(RestResolveError::UnsupportedParameter(name))
                if name == "unexpected"
        ));
    }

    #[test]
    fn preserves_priority() {
        let config = config();

        let resolved = RestRequestResolver::resolve_endpoint(
            &config,
            &endpoint(),
            &request().with_priority(RequestPriority::High),
        )
        .unwrap();

        assert_eq!(resolved.priority(), RequestPriority::High,);
    }

    // ========================================================
    // TEMPLATES
    // ========================================================

    #[test]
    fn resolves_fixed_category_template() {
        let config = config();

        let mut endpoint = endpoint();

        endpoint.parameters.push("category".into());

        endpoint.category = Some("{native_rest_category}".into());

        let resolved = RestRequestResolver::resolve_endpoint(
            &config,
            &endpoint,
            &request().with_parameter("native_rest_category", "USDT-FUTURES"),
        )
        .unwrap();

        assert_eq!(resolved.parameter("category"), Some("USDT-FUTURES"),);

        assert!(resolved.parameter("native_rest_category").is_none());
    }

    #[test]
    fn rejects_missing_template_input() {
        let config = config();

        let mut endpoint = endpoint();

        endpoint.parameters.push("category".into());

        endpoint.category = Some("{native_rest_category}".into());

        let result = RestRequestResolver::resolve_endpoint(&config, &endpoint, &request());

        assert!(matches!(
            result,
            Err(RestResolveError::UnresolvedTemplate(name))
                if name == "native_rest_category"
        ));
    }

    #[test]
    fn substitutes_path_parameters() {
        let config = config();

        let mut endpoint = endpoint();

        endpoint.path = "/api/v4/futures/{settle}/order_book".into();

        let resolved = RestRequestResolver::resolve_endpoint(
            &config,
            &endpoint,
            &request().with_parameter("settle", "usdt"),
        )
        .unwrap();

        assert_eq!(
            resolved.url(),
            "https://api.binance.com/api/v4/futures/usdt/order_book",
        );

        assert!(resolved.parameter("settle").is_none());
    }

    #[test]
    fn rejects_path_traversal() {
        let config = config();

        let mut endpoint = endpoint();

        endpoint.path = "/api/v4/{settle}/order_book".into();

        let result = RestRequestResolver::resolve_endpoint(
            &config,
            &endpoint,
            &request().with_parameter("settle", ".."),
        );

        assert!(matches!(result, Err(RestResolveError::InvalidPath(_))));
    }

    // ========================================================
    // URL SAFETY
    // ========================================================

    #[test]
    fn preserves_base_api_path() {
        let mut config = config();

        config.rest.base_url = "https://api.gateio.ws/api/v4".into();

        let mut endpoint = endpoint();

        endpoint.path = "/spot/order_book".into();

        let resolved =
            RestRequestResolver::resolve_endpoint(&config, &endpoint, &request()).unwrap();

        assert_eq!(
            resolved.url(),
            "https://api.gateio.ws/api/v4/spot/order_book",
        );
    }

    #[test]
    fn rejects_external_http_base_url() {
        let mut config = config();

        config.rest.base_url = "http://example.com".into();

        let result = RestRequestResolver::resolve_endpoint(&config, &endpoint(), &request());

        assert!(matches!(result, Err(RestResolveError::InvalidBaseUrl(_))));
    }

    #[test]
    fn permits_local_test_server() {
        let mut config = config();

        config.rest.base_url = "http://127.0.0.1:8080".into();

        let resolved =
            RestRequestResolver::resolve_endpoint(&config, &endpoint(), &request()).unwrap();

        assert_eq!(resolved.url(), "http://127.0.0.1:8080/api/v3/depth",);
    }

    // ========================================================
    // AUTHENTICATION AND METHODS
    // ========================================================

    #[test]
    fn rejects_authenticated_endpoint() {
        let config = config();

        let mut endpoint = endpoint();

        endpoint.authentication = Some("api_key".into());

        let result = RestRequestResolver::resolve_endpoint(&config, &endpoint, &request());

        assert!(matches!(
            result,
            Err(RestResolveError::AuthenticationRequired)
        ));
    }

    #[test]
    fn rejects_unsupported_http_method() {
        let config = config();

        let mut endpoint = endpoint();

        endpoint.method = "DELETE".into();

        let result = RestRequestResolver::resolve_endpoint(&config, &endpoint, &request());

        assert!(matches!(
            result,
            Err(RestResolveError::UnsupportedMethod(_))
        ));
    }
    #[test]
    fn rejects_missing_required_symbol() {
        let config = config();

        let mut endpoint = endpoint();

        endpoint.required_parameters = vec!["symbol".into()];

        let request = RestRequest::new("binance_spot", "depth_snapshot");

        let result = RestRequestResolver::resolve_endpoint(&config, &endpoint, &request);

        assert!(matches!(
            result,
            Err(RestResolveError::MissingParameter(name))
                if name == "symbol"
        ));
    }
    #[test]
    fn applies_native_sz_limit() {
        let config = config();

        let mut endpoint = endpoint();

        endpoint.parameters = vec!["instId".into(), "sz".into()];

        endpoint.required_parameters = vec!["instId".into()];

        endpoint.limit_parameter = Some("sz".into());

        endpoint.default_limit = Some(400);
        endpoint.maximum_limit = Some(400);

        let request =
            RestRequest::new("binance_spot", "depth_snapshot").with_parameter("instId", "BTC-USDT");

        let resolved = RestRequestResolver::resolve_endpoint(&config, &endpoint, &request).unwrap();

        assert_eq!(resolved.parameter("sz"), Some("400"),);

        assert_eq!(resolved.parameter("instId"), Some("BTC-USDT"),);

        assert_eq!(resolved.parameter("limit"), None,);
    }
    #[test]
    fn resolves_fixed_settlement_in_url_path() {
        let config = config();

        let mut endpoint = endpoint();

        endpoint.path = "/futures/{settlement}/order_book".into();

        endpoint.parameters = vec!["contract".into(), "limit".into()];

        endpoint.required_parameters = vec!["contract".into()];

        endpoint.contract = Some("{symbol}".into());
        endpoint.settlement = Some("btc".into());

        let request =
            RestRequest::new("binance_spot", "depth_snapshot").with_parameter("symbol", "BTC_USD");

        let resolved = RestRequestResolver::resolve_endpoint(&config, &endpoint, &request).unwrap();

        assert_eq!(
            resolved.url(),
            "https://api.binance.com/futures/btc/order_book",
        );

        assert_eq!(resolved.parameter("contract"), Some("BTC_USD"),);

        // Settlement is part of the URL path.
        assert_eq!(resolved.parameter("settlement"), None,);
    }
    #[test]
    fn rejects_native_limit_above_maximum() {
        let config = config();

        let mut endpoint = endpoint();

        endpoint.parameters = vec!["instId".into(), "sz".into()];

        endpoint.limit_parameter = Some("sz".into());

        endpoint.default_limit = Some(400);
        endpoint.maximum_limit = Some(400);

        let request = RestRequest::new("binance_spot", "depth_snapshot")
            .with_parameter("instId", "BTC-USDT")
            .with_parameter("sz", "1000");

        let result = RestRequestResolver::resolve_endpoint(&config, &endpoint, &request);

        assert!(matches!(
            result,
            Err(RestResolveError::InvalidParameter {
                name,
                ..
            }) if name == "sz"
        ));
    }
}
