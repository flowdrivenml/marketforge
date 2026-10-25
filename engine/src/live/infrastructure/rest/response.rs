use std::{collections::BTreeMap, time::Duration};

use serde_json::Value;

// ============================================================
// RESPONSE HEADERS
// ============================================================

/// Selected HTTP response headers.
///
/// Header names are stored in lowercase.
///
/// The future HTTP executor should retain only headers
/// required for diagnostics, throttling, and monitoring.
///
/// Do not retain sensitive authentication or cookie headers.
pub type RestResponseHeaders = BTreeMap<String, String>;

// ============================================================
// EXCHANGE REST RESPONSE
// ============================================================

/// Successfully completed exchange REST operation.
///
/// This represents an HTTP response accepted by the REST
/// infrastructure, not necessarily market-data validity.
///
/// For example, an L2 synchronizer must still validate
/// snapshot sequence and continuity requirements.
///
/// Exchange JSON is preserved without normalization.
#[derive(Debug, Clone)]
pub struct ExchangeRestResponse {
    profile: String,
    endpoint: String,

    status: u16,
    body: Value,
    headers: RestResponseHeaders,

    elapsed: Duration,
    body_bytes: usize,
    attempts: u32,
}

impl ExchangeRestResponse {
    // ========================================================
    // CONSTRUCTION
    // ========================================================

    pub fn new(
        profile: impl Into<String>,
        endpoint: impl Into<String>,
        status: u16,
        body: Value,
        headers: RestResponseHeaders,
        elapsed: Duration,
        body_bytes: usize,
    ) -> Self {
        Self {
            profile: profile.into(),
            endpoint: endpoint.into(),
            status,
            body,
            headers,
            elapsed,
            body_bytes,
            attempts: 1,
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

    pub fn status(&self) -> u16 {
        self.status
    }

    pub fn body(&self) -> &Value {
        &self.body
    }

    pub fn into_body(self) -> Value {
        self.body
    }

    pub fn headers(&self) -> &RestResponseHeaders {
        &self.headers
    }

    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .get(&name.to_ascii_lowercase())
            .map(String::as_str)
    }

    pub fn elapsed(&self) -> Duration {
        self.elapsed
    }

    pub fn body_bytes(&self) -> usize {
        self.body_bytes
    }

    pub fn attempts(&self) -> u32 {
        self.attempts
    }

    // ========================================================
    // RETRY METADATA
    // ========================================================

    /// Record the number of HTTP attempts performed.
    ///
    /// Intended for the retry coordinator.
    ///
    /// The single-attempt executor always returns attempts=1.
    pub(crate) fn with_attempts(mut self, attempts: u32) -> Self {
        self.attempts = attempts.max(1);
        self
    }
}

// ============================================================
// REST ERROR CLASSIFICATION
// ============================================================

/// Stable classification of exchange REST failures.
///
/// Classification is separate from retry authorization.
///
/// A potentially transient error may still be non-retryable
/// because of the operation's semantics or caller policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RestErrorKind {
    // --------------------------------------------------------
    // TRANSPORT
    // --------------------------------------------------------
    /// DNS, connection, TLS, or other network failure.
    Network,

    /// HTTP request or response exceeded its deadline.
    Timeout,

    /// Operation was explicitly cancelled.
    Cancelled,

    // --------------------------------------------------------
    // EXCHANGE / HTTP
    // --------------------------------------------------------
    /// Exchange rejected the request due to rate limiting.
    RateLimited,

    /// Exchange reported a temporary server failure.
    ServerError,

    /// Exchange rejected request parameters or structure.
    InvalidRequest,

    /// Authentication was missing or invalid.
    Authentication,

    /// Credentials lack permission for the operation.
    PermissionDenied,

    /// Requested resource or endpoint was not found.
    NotFound,

    /// Exchange returned another unsuccessful HTTP status.
    HttpError,

    // --------------------------------------------------------
    // LOCAL VALIDATION / RESPONSE PROCESSING
    // --------------------------------------------------------
    /// Exchange profile or endpoint configuration is invalid.
    Configuration,

    /// Response could not be interpreted as expected.
    InvalidResponse,

    /// Response exceeded the configured maximum body size.
    ResponseTooLarge,

    /// Request could not acquire required local capacity.
    ResourceUnavailable,
}

impl RestErrorKind {
    // ========================================================
    // HTTP CLASSIFICATION
    // ========================================================

    /// Classify an unsuccessful HTTP status.
    ///
    /// Exchange-specific application error codes are handled
    /// separately by the REST feedback interpreter.
    ///
    /// HTTP 2xx is not classified here because an exchange
    /// can report application errors inside a 200 response.
    pub fn from_http_status(status: u16) -> Option<Self> {
        match status {
            200..=299 => None,

            400 | 405 | 409 | 410 | 411 | 413 | 414 | 415 | 416 | 422 => Some(Self::InvalidRequest),

            401 => Some(Self::Authentication),

            403 => Some(Self::PermissionDenied),

            404 => Some(Self::NotFound),

            408 | 425 | 500..=599 => Some(Self::ServerError),

            429 => Some(Self::RateLimited),

            _ => Some(Self::HttpError),
        }
    }

    // ========================================================
    // RETRY HINT
    // ========================================================

    /// Whether the failure could justify another attempt
    /// under an appropriate retry policy.
    ///
    /// This is a conservative hint, NOT a retry decision.
    ///
    /// The retry coordinator must still verify:
    /// - The operation is safe to repeat.
    /// - Retry attempts remain.
    /// - The deadline has not expired.
    /// - Cancellation has not been requested.
    ///
    /// InvalidResponse is deliberately excluded because
    /// schema failures may indicate permanent incompatibility.
    pub const fn is_transient_candidate(self) -> bool {
        matches!(
            self,
            Self::Network | Self::Timeout | Self::RateLimited | Self::ServerError
        )
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Network => "network",
            Self::Timeout => "timeout",
            Self::Cancelled => "cancelled",
            Self::RateLimited => "rate_limited",
            Self::ServerError => "server_error",
            Self::InvalidRequest => "invalid_request",
            Self::Authentication => "authentication",
            Self::PermissionDenied => "permission_denied",
            Self::NotFound => "not_found",
            Self::HttpError => "http_error",
            Self::Configuration => "configuration",
            Self::InvalidResponse => "invalid_response",
            Self::ResponseTooLarge => "response_too_large",
            Self::ResourceUnavailable => "resource_unavailable",
        }
    }
}

// ============================================================
// REST ERROR
// ============================================================

/// Classified error for one logical exchange REST operation.
///
/// Transport-specific errors are converted into this type
/// before being returned to stream consumers.
///
/// The error retains the original context needed for
/// diagnostics without exposing secrets or full URLs.
#[derive(Debug, Clone, thiserror::Error)]
#[error("REST {kind:?} for {profile}/{endpoint}: {message}")]
pub struct RestError {
    kind: RestErrorKind,

    profile: String,
    endpoint: String,

    message: String,

    http_status: Option<u16>,

    exchange_code: Option<String>,

    retry_after: Option<Duration>,

    attempts: u32,
}

impl RestError {
    // ========================================================
    // CONSTRUCTION
    // ========================================================

    pub fn new(
        kind: RestErrorKind,
        profile: impl Into<String>,
        endpoint: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            profile: profile.into(),
            endpoint: endpoint.into(),
            message: message.into(),
            http_status: None,
            exchange_code: None,
            retry_after: None,
            attempts: 1,
        }
    }

    /// Construct an error from an unsuccessful HTTP status.
    ///
    /// HTTP 2xx statuses must not use this constructor.
    /// Application-level failures inside 2xx responses
    /// can use RestError::new() with explicit classification.
    pub fn from_http_status(
        profile: impl Into<String>,
        endpoint: impl Into<String>,
        status: u16,
    ) -> Option<Self> {
        let kind = RestErrorKind::from_http_status(status)?;

        Some(Self::new(kind, profile, endpoint, format!("HTTP {status}")).with_http_status(status))
    }

    // ========================================================
    // ERROR METADATA
    // ========================================================

    pub fn with_http_status(mut self, status: u16) -> Self {
        self.http_status = Some(status);
        self
    }

    pub fn with_exchange_code(mut self, code: impl Into<String>) -> Self {
        self.exchange_code = Some(code.into());
        self
    }

    pub fn with_retry_after(mut self, delay: Duration) -> Self {
        self.retry_after = Some(delay);
        self
    }

    pub(crate) fn with_attempts(mut self, attempts: u32) -> Self {
        self.attempts = attempts.max(1);
        self
    }

    // ========================================================
    // ACCESSORS
    // ========================================================

    pub fn kind(&self) -> RestErrorKind {
        self.kind
    }

    pub fn profile(&self) -> &str {
        &self.profile
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn http_status(&self) -> Option<u16> {
        self.http_status
    }

    pub fn exchange_code(&self) -> Option<&str> {
        self.exchange_code.as_deref()
    }

    pub fn retry_after(&self) -> Option<Duration> {
        self.retry_after
    }

    pub fn attempts(&self) -> u32 {
        self.attempts
    }

    // ========================================================
    // RETRY HINT
    // ========================================================

    pub fn is_transient_candidate(&self) -> bool {
        self.kind.is_transient_candidate()
    }
}

// ============================================================
// TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::json;

    // ========================================================
    // RESPONSE
    // ========================================================

    fn response() -> ExchangeRestResponse {
        let mut headers = RestResponseHeaders::new();

        headers.insert("x-mbx-used-weight-1m".into(), "37".into());

        ExchangeRestResponse::new(
            "binance_spot",
            "depth_snapshot",
            200,
            json!({
                "lastUpdateId": 12345,
                "bids": [],
                "asks": []
            }),
            headers,
            Duration::from_millis(120),
            128,
        )
    }

    #[test]
    fn preserves_response_metadata() {
        let response = response();

        assert_eq!(response.profile(), "binance_spot");

        assert_eq!(response.endpoint(), "depth_snapshot",);

        assert_eq!(response.status(), 200);

        assert_eq!(response.elapsed(), Duration::from_millis(120),);

        assert_eq!(response.body_bytes(), 128);

        assert_eq!(response.attempts(), 1);
    }

    #[test]
    fn preserves_raw_exchange_json() {
        let response = response();

        assert_eq!(response.body()["lastUpdateId"], 12345,);

        assert_eq!(response.body()["bids"], json!([]),);
    }

    #[test]
    fn retrieves_headers_case_insensitively() {
        let response = response();

        assert_eq!(response.header("X-MBX-USED-WEIGHT-1M"), Some("37"),);

        assert_eq!(response.header("x-mbx-used-weight-1m"), Some("37"),);

        assert_eq!(response.header("missing"), None,);
    }

    #[test]
    fn records_retry_attempt_count() {
        let response = response().with_attempts(3);

        assert_eq!(response.attempts(), 3);
    }

    #[test]
    fn into_body_preserves_json() {
        let body = response().into_body();

        assert_eq!(body["lastUpdateId"], 12345,);
    }

    // ========================================================
    // HTTP ERROR CLASSIFICATION
    // ========================================================

    #[test]
    fn classifies_common_http_errors() {
        let cases = [
            (400, RestErrorKind::InvalidRequest),
            (401, RestErrorKind::Authentication),
            (403, RestErrorKind::PermissionDenied),
            (404, RestErrorKind::NotFound),
            (408, RestErrorKind::ServerError),
            (422, RestErrorKind::InvalidRequest),
            (429, RestErrorKind::RateLimited),
            (500, RestErrorKind::ServerError),
            (502, RestErrorKind::ServerError),
            (503, RestErrorKind::ServerError),
            (504, RestErrorKind::ServerError),
        ];

        for (status, expected) in cases {
            assert_eq!(
                RestErrorKind::from_http_status(status),
                Some(expected),
                "incorrect classification for HTTP {status}",
            );
        }
    }

    #[test]
    fn successful_http_status_is_not_an_error() {
        for status in [200, 201, 204, 206, 299] {
            assert_eq!(RestErrorKind::from_http_status(status), None,);
        }
    }

    #[test]
    fn unknown_http_status_is_not_automatically_retryable() {
        let kind = RestErrorKind::from_http_status(418).unwrap();

        assert_eq!(kind, RestErrorKind::HttpError);

        assert!(!kind.is_transient_candidate());
    }

    // ========================================================
    // RETRY CLASSIFICATION
    // ========================================================

    #[test]
    fn identifies_potentially_transient_failures() {
        for kind in [
            RestErrorKind::Network,
            RestErrorKind::Timeout,
            RestErrorKind::RateLimited,
            RestErrorKind::ServerError,
        ] {
            assert!(kind.is_transient_candidate());
        }
    }

    #[test]
    fn permanent_failures_are_not_retry_candidates() {
        for kind in [
            RestErrorKind::InvalidRequest,
            RestErrorKind::Authentication,
            RestErrorKind::PermissionDenied,
            RestErrorKind::NotFound,
            RestErrorKind::Configuration,
            RestErrorKind::InvalidResponse,
            RestErrorKind::ResponseTooLarge,
            RestErrorKind::Cancelled,
            RestErrorKind::ResourceUnavailable,
        ] {
            assert!(!kind.is_transient_candidate());
        }
    }

    // ========================================================
    // REST ERROR METADATA
    // ========================================================

    #[test]
    fn preserves_exchange_error_metadata() {
        let error = RestError::new(
            RestErrorKind::RateLimited,
            "bitget_linear",
            "recent_trades",
            "exchange request quota exceeded",
        )
        .with_http_status(429)
        .with_exchange_code("42900")
        .with_retry_after(Duration::from_secs(5))
        .with_attempts(3);

        assert_eq!(error.kind(), RestErrorKind::RateLimited,);

        assert_eq!(error.http_status(), Some(429),);

        assert_eq!(error.exchange_code(), Some("42900"),);

        assert_eq!(error.retry_after(), Some(Duration::from_secs(5)),);

        assert_eq!(error.attempts(), 3);

        assert!(error.is_transient_candidate());
    }

    #[test]
    fn creates_http_error_from_status() {
        let error = RestError::from_http_status("binance_spot", "depth_snapshot", 400).unwrap();

        assert_eq!(error.kind(), RestErrorKind::InvalidRequest,);

        assert_eq!(error.http_status(), Some(400));

        assert!(!error.is_transient_candidate());
    }

    #[test]
    fn does_not_create_http_error_from_success() {
        assert!(RestError::from_http_status("binance_spot", "depth_snapshot", 200,).is_none());
    }

    #[test]
    fn errors_have_stable_classification_names() {
        assert_eq!(RestErrorKind::RateLimited.as_str(), "rate_limited",);

        assert_eq!(RestErrorKind::InvalidResponse.as_str(), "invalid_response",);

        assert_eq!(RestErrorKind::Configuration.as_str(), "configuration",);
    }
}
