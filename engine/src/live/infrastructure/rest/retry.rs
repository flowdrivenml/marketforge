use std::time::Duration;

use crate::live::infrastructure::backoff::{BackoffConfig, ExponentialBackoff};

use super::{
    client::ExchangeRestClient,
    request::{ResolvedRestRequest, RestMethod},
    response::{ExchangeRestResponse, RestError, RestErrorKind},
};

// ============================================================
// RETRY POLICY ERRORS
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RestRetryPolicyError {
    #[error("maximum REST attempts must be greater than zero")]
    InvalidMaxAttempts,

    #[error("overall REST retry timeout must be greater than zero")]
    InvalidOverallTimeout,
}

// ============================================================
// REST RETRY POLICY
// ============================================================

/// Immutable retry policy for one logical REST operation.
///
/// max_attempts includes the initial attempt.
///
/// Example:
/// ```
/// use std::time::Duration;
/// use marketforge_engine::live::infrastructure::{backoff::BackoffConfig, rest::RestRetryPolicy};
/// let backoff = BackoffConfig::new(Duration::from_millis(10), Duration::from_millis(100), false).unwrap();
/// let policy = RestRetryPolicy::new(3, Duration::from_secs(1), backoff).unwrap();
/// assert_eq!(policy.max_attempts, 3); // Includes the initial HTTP attempt.
/// assert!(RestRetryPolicy::new(0, Duration::from_secs(1), backoff).is_err());
/// ```
///
/// The overall timeout includes:
/// - Rate-limit waiting.
/// - Concurrency waiting.
/// - HTTP execution.
/// - Backoff.
/// - Exchange-imposed cooldowns.
///
/// Each individual HTTP attempt additionally observes
/// the timeout from ResolvedRestRequest.
#[derive(Debug, Clone, Copy)]
pub struct RestRetryPolicy {
    pub max_attempts: u32,

    pub overall_timeout: Duration,

    pub backoff: BackoffConfig,
}

impl RestRetryPolicy {
    pub fn new(
        max_attempts: u32,
        overall_timeout: Duration,
        backoff: BackoffConfig,
    ) -> Result<Self, RestRetryPolicyError> {
        if max_attempts == 0 {
            return Err(RestRetryPolicyError::InvalidMaxAttempts);
        }

        if overall_timeout.is_zero() {
            return Err(RestRetryPolicyError::InvalidOverallTimeout);
        }

        Ok(Self {
            max_attempts,
            overall_timeout,
            backoff,
        })
    }
}

// ============================================================
// RETRY DECISION
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RetryDecision {
    Retry,
    Stop,
}

// ============================================================
// REST RETRY CLASSIFIER
// ============================================================

/// HTTP-specific retry classification.
///
/// Classification does not consider attempt limits or
/// deadlines. Those are enforced by the coordinator.
///
/// Only read-only GET operations are eligible for automatic
/// retries in MarketForge Live v1.
struct RestRetryClassifier;

impl RestRetryClassifier {
    fn classify(request: &ResolvedRestRequest, error: &RestError) -> RetryDecision {
        // ====================================================
        // OPERATION SAFETY
        // ====================================================

        if request.method() != RestMethod::Get {
            return RetryDecision::Stop;
        }

        // ====================================================
        // TRANSPORT FAILURES
        // ====================================================

        match error.kind() {
            RestErrorKind::Network | RestErrorKind::Timeout => {
                return RetryDecision::Retry;
            }

            RestErrorKind::RateLimited => {
                // A configured temporary exchange ban
                // should not automatically trigger repeated
                // requests, even if the limiter recorded
                // its cooldown.
                if error.http_status() == Some(418) {
                    return RetryDecision::Stop;
                }

                return RetryDecision::Retry;
            }

            RestErrorKind::ServerError => {
                // Some errors are grouped as ServerError
                // by the existing response classifier.
                //
                // Only documented transient HTTP statuses
                // are retried automatically.
                return match error.http_status() {
                    Some(408 | 425 | 500 | 502 | 503 | 504) => RetryDecision::Retry,

                    _ => RetryDecision::Stop,
                };
            }

            _ => {}
        }

        // ====================================================
        // PERMANENT OR CALLER-OWNED FAILURES
        // ====================================================

        RetryDecision::Stop
    }
}

// ============================================================
// REST RETRY EXECUTOR
// ============================================================

impl ExchangeRestClient {
    /// Execute one logical REST operation with bounded
    /// automatic retries.
    ///
    /// Every HTTP attempt delegates to execute_once().
    ///
    /// Therefore, each attempt:
    /// - Acquires fresh rate-limit capacity.
    /// - Observes active exchange cooldowns.
    /// - Uses the same resolved parameters.
    /// - Preserves HTTP timeout enforcement.
    /// - Applies exchange throttling feedback.
    ///
    /// The complete operation is bounded by the overall
    /// timeout, including waits and retries.
    ///
    /// Dropping this future cancels the retry operation.
    ///
    /// No detached tasks are created.
    pub async fn execute_with_retry(
        &self,
        request: &ResolvedRestRequest,
        policy: RestRetryPolicy,
    ) -> Result<ExchangeRestResponse, RestError> {
        // Validate before starting the operation.
        //
        // Normally policies are constructed through
        // RestRetryPolicy::new(), but fields are public.
        if policy.max_attempts == 0 || policy.overall_timeout.is_zero() {
            return Err(RestError::new(
                RestErrorKind::Configuration,
                request.profile(),
                request.endpoint(),
                "invalid REST retry policy",
            ));
        }

        let mut attempts = 0_u32;

        let result = tokio::time::timeout(policy.overall_timeout, async {
            let mut backoff = ExponentialBackoff::new(policy.backoff);

            loop {
                // ========================================
                // START NEXT ATTEMPT
                // ========================================

                attempts += 1;

                let response = self.execute_once(request).await;

                match response {
                    // ====================================
                    // SUCCESS
                    // ====================================
                    Ok(response) => {
                        return Ok(response.with_attempts(attempts));
                    }

                    // ====================================
                    // FAILURE
                    // ====================================
                    Err(error) => {
                        let error = error.with_attempts(attempts);

                        // --------------------------------
                        // MAXIMUM ATTEMPTS
                        // --------------------------------

                        if attempts >= policy.max_attempts {
                            return Err(error);
                        }

                        // --------------------------------
                        // FAILURE CLASSIFICATION
                        // --------------------------------

                        if RestRetryClassifier::classify(request, &error) == RetryDecision::Stop {
                            return Err(error);
                        }

                        // --------------------------------
                        // EXPONENTIAL BACKOFF
                        // --------------------------------

                        let delay = backoff.next_delay();

                        // Exchange cooldown is already
                        // maintained by RateLimitManager.
                        //
                        // Do not sleep for Retry-After
                        // independently here.
                        //
                        // After backoff, execute_once()
                        // will acquire quota and wait
                        // for any active cooldown.
                        tokio::time::sleep(delay).await;
                    }
                }
            }
        })
        .await;

        match result {
            Ok(result) => result,

            Err(_) => Err(RestError::new(
                RestErrorKind::Timeout,
                request.profile(),
                request.endpoint(),
                "overall REST retry deadline exceeded",
            )
            .with_attempts(attempts.max(1))),
        }
    }
}

// ============================================================
// TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    use crate::live::infrastructure::limiter::RequestPriority;

    // ========================================================
    // TEST HELPERS
    // ========================================================

    fn backoff_config() -> BackoffConfig {
        BackoffConfig::new(Duration::from_millis(100), Duration::from_secs(5), false).unwrap()
    }

    fn request(method: RestMethod) -> ResolvedRestRequest {
        ResolvedRestRequest::new(
            "binance_spot".into(),
            "depth_snapshot".into(),
            method,
            "https://example.invalid/depth".into(),
            Default::default(),
            Duration::from_secs(10),
            RequestPriority::Normal,
        )
    }

    fn error(kind: RestErrorKind, status: Option<u16>) -> RestError {
        let mut error = RestError::new(kind, "binance_spot", "depth_snapshot", "test failure");

        if let Some(status) = status {
            error = error.with_http_status(status);
        }

        error
    }

    // ========================================================
    // POLICY VALIDATION
    // ========================================================

    #[test]
    fn rejects_zero_attempts() {
        let result = RestRetryPolicy::new(0, Duration::from_secs(30), backoff_config());

        assert!(matches!(
            result,
            Err(RestRetryPolicyError::InvalidMaxAttempts)
        ));
    }

    #[test]
    fn rejects_zero_overall_timeout() {
        let result = RestRetryPolicy::new(3, Duration::ZERO, backoff_config());

        assert!(matches!(
            result,
            Err(RestRetryPolicyError::InvalidOverallTimeout)
        ));
    }

    #[test]
    fn accepts_valid_policy() {
        let policy = RestRetryPolicy::new(3, Duration::from_secs(30), backoff_config()).unwrap();

        assert_eq!(policy.max_attempts, 3);

        assert_eq!(policy.overall_timeout, Duration::from_secs(30),);
    }

    // ========================================================
    // TRANSIENT HTTP STATUS CLASSIFICATION
    // ========================================================

    #[test]
    fn retries_selected_transient_http_errors() {
        let request = request(RestMethod::Get);

        for status in [408_u16, 425, 500, 502, 503, 504] {
            let error = error(RestErrorKind::ServerError, Some(status));

            assert_eq!(
                RestRetryClassifier::classify(&request, &error,),
                RetryDecision::Retry,
                "HTTP {status} should be retryable",
            );
        }
    }

    #[test]
    fn does_not_retry_other_server_errors() {
        let request = request(RestMethod::Get);

        for status in [501_u16, 505, 507, 511] {
            let error = error(RestErrorKind::ServerError, Some(status));

            assert_eq!(
                RestRetryClassifier::classify(&request, &error,),
                RetryDecision::Stop,
                "HTTP {status} should not be retried",
            );
        }
    }

    // ========================================================
    // RATE LIMITING
    // ========================================================

    #[test]
    fn retries_http_429() {
        let request = request(RestMethod::Get);

        let error = error(RestErrorKind::RateLimited, Some(429));

        assert_eq!(
            RestRetryClassifier::classify(&request, &error,),
            RetryDecision::Retry,
        );
    }

    #[test]
    fn does_not_automatically_retry_temporary_ban() {
        let request = request(RestMethod::Get);

        let error = error(RestErrorKind::RateLimited, Some(418));

        assert_eq!(
            RestRetryClassifier::classify(&request, &error,),
            RetryDecision::Stop,
        );
    }

    // ========================================================
    // NETWORK FAILURES
    // ========================================================

    #[test]
    fn retries_network_and_http_timeouts() {
        let request = request(RestMethod::Get);

        for kind in [RestErrorKind::Network, RestErrorKind::Timeout] {
            let error = error(kind, None);

            assert_eq!(
                RestRetryClassifier::classify(&request, &error,),
                RetryDecision::Retry,
            );
        }
    }

    // ========================================================
    // PERMANENT FAILURES
    // ========================================================

    #[test]
    fn does_not_retry_permanent_errors() {
        let request = request(RestMethod::Get);

        for kind in [
            RestErrorKind::InvalidRequest,
            RestErrorKind::Authentication,
            RestErrorKind::PermissionDenied,
            RestErrorKind::NotFound,
            RestErrorKind::Configuration,
            RestErrorKind::InvalidResponse,
            RestErrorKind::ResponseTooLarge,
            RestErrorKind::ResourceUnavailable,
            RestErrorKind::Cancelled,
            RestErrorKind::HttpError,
        ] {
            let error = error(kind, None);

            assert_eq!(
                RestRetryClassifier::classify(&request, &error,),
                RetryDecision::Stop,
            );
        }
    }

    // ========================================================
    // REQUEST SAFETY
    // ========================================================

    #[test]
    fn does_not_retry_post_requests() {
        let request = request(RestMethod::Post);

        let error = error(RestErrorKind::ServerError, Some(503));

        assert_eq!(
            RestRetryClassifier::classify(&request, &error,),
            RetryDecision::Stop,
        );
    }

    // ========================================================
    // BACKOFF STATE
    // ========================================================

    #[test]
    fn independent_operations_have_independent_backoff() {
        let mut first = ExponentialBackoff::new(backoff_config());

        let mut second = ExponentialBackoff::new(backoff_config());

        assert_eq!(first.next_delay(), Duration::from_millis(100),);

        assert_eq!(first.next_delay(), Duration::from_millis(200),);

        assert_eq!(second.next_delay(), Duration::from_millis(100),);
    }
}
