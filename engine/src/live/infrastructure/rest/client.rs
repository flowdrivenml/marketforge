use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};

use reqwest::{Client, header::HeaderMap, redirect::Policy};
use serde_json::Value;
use tokio::sync::Semaphore;

use crate::live::{
    config::exchange::RestRateLimiting,
    infrastructure::limiter::{
        RateLimitManager, RateLimitedOperation,
        factory::{CompiledRestLimiters, WeightParameters},
    },
};

use super::{
    application::{RestApplicationOutcome, RestApplicationRule, RestApplicationValidator},
    feedback::{RestFeedback, RestFeedbackAdapter, RestFeedbackInterpreter, RestUsageBinding},
    request::{ResolvedRestRequest, RestMethod},
    response::{ExchangeRestResponse, RestError, RestErrorKind, RestResponseHeaders},
};

// ============================================================
// CLIENT CONFIGURATION
// ============================================================

#[derive(Debug, Clone, Copy)]
pub struct RestClientConfig {
    pub max_concurrent_requests: usize,
    pub max_response_bytes: usize,
}

impl RestClientConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.max_concurrent_requests == 0 {
            return Err("max_concurrent_requests must be positive".into());
        }

        if self.max_response_bytes == 0 {
            return Err("max_response_bytes must be positive".into());
        }

        Ok(())
    }
}

// ============================================================
// PROFILE FEEDBACK CONFIGURATION
// ============================================================

/// Immutable REST feedback configuration for one exchange
/// profile.
///
/// The caller supplies this at runtime initialization.
///
/// Usage bindings must identify actual compiled budgets.
/// Their identity and accounting window must not be guessed
/// from an arbitrary response header.
#[derive(Debug, Clone)]
pub struct RestClientFeedbackConfig {
    /// Exchange REST throttling configuration.
    pub rate_limiting: Option<RestRateLimiting>,

    /// Verified exchange usage-header mappings.
    pub usage_bindings: Vec<RestUsageBinding>,

    /// Conservative cooldown when Retry-After is unavailable.
    pub fallback_cooldown: Duration,

    /// Application-response validation by endpoint name.
    ///
    /// Endpoints absent from this map have no application
    /// envelope rule. They still require HTTP and JSON
    /// validation.
    pub application_rules: BTreeMap<String, RestApplicationRule>,
}

impl RestClientFeedbackConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.fallback_cooldown.is_zero() {
            return Err("fallback REST cooldown must be positive".into());
        }

        let mut headers = HashSet::new();
        let mut budgets = HashSet::new();

        for binding in &self.usage_bindings {
            if binding.header.trim().is_empty() {
                return Err("REST usage binding header is empty".into());
            }

            if binding.window.is_zero() {
                return Err("REST usage binding window must be positive".into());
            }

            if !headers.insert(binding.header.to_ascii_lowercase()) {
                return Err("duplicate REST usage header binding".into());
            }

            if !budgets.insert(&binding.budget) {
                return Err("duplicate REST usage budget binding".into());
            }
        }
        for endpoint in self.application_rules.keys() {
            if endpoint.trim().is_empty() {
                return Err("REST application rule endpoint name is empty".into());
            }
        }
        Ok(())
    }
}

// ============================================================
// REST CLIENT
// ============================================================

/// Shared public-market-data REST executor.
///
/// Performs exactly one HTTP request per execute_once() call.
///
/// Rate-limit state is shared across relevant exchange
/// profiles through RateLimitManager.
///
/// Exchange feedback is interpreted before the attempt
/// completes, allowing subsequent operations to observe
/// newly imposed cooldowns.
#[derive(Clone)]
pub struct ExchangeRestClient {
    http: Client,

    limiter: RateLimitManager,

    profiles: Arc<BTreeMap<String, CompiledRestLimiters>>,

    feedback: Arc<BTreeMap<String, RestClientFeedbackConfig>>,

    concurrency: Arc<Semaphore>,

    max_response_bytes: usize,
}

impl ExchangeRestClient {
    // ========================================================
    // CONSTRUCTION
    // ========================================================

    pub fn new(
        limiter: RateLimitManager,
        profiles: BTreeMap<String, CompiledRestLimiters>,
        feedback: BTreeMap<String, RestClientFeedbackConfig>,
        config: RestClientConfig,
    ) -> Result<Self, String> {
        config.validate()?;

        if profiles.is_empty() {
            return Err("REST limiter profiles cannot be empty".into());
        }

        if profiles.len() != feedback.len() || profiles.keys().ne(feedback.keys()) {
            return Err("REST limiter and feedback profile sets differ".into());
        }

        for (name, settings) in &feedback {
            settings
                .validate()
                .map_err(|error| format!("{name}: {error}"))?;

            let compiled = profiles
                .get(name)
                .ok_or_else(|| format!("missing compiled profile {name}"))?;
            // Every application rule must reference a compiled endpoint.
            //
            // Reject invalid mappings during initialization rather
            // than discovering them after receiving HTTP responses.
            for endpoint in settings.application_rules.keys() {
                if compiled.endpoint(endpoint).is_none() {
                    return Err(format!(
                        "{name}: application rule references \
             unknown endpoint '{endpoint}'"
                    ));
                }
            }
            // All configured usage bindings must refer to
            // budgets registered by this profile.
            //
            // The binding may be used by only some of the
            // profile's endpoints. We'll filter it against
            // each operation at execution time.
            for binding in &settings.usage_bindings {
                if compiled.policies.get(&binding.budget).is_none() {
                    return Err(format!(
                        "{name}: usage binding references \
                         unknown budget {:?}",
                        binding.budget,
                    ));
                }

                let quota = compiled
                    .policies
                    .get(&binding.budget)
                    .expect("budget existence already checked");

                if quota.window != binding.window {
                    return Err(format!(
                        "{name}: usage binding window differs \
                         from compiled budget window",
                    ));
                }
            }
        }

        let http = Client::builder()
            .redirect(Policy::none())
            .build()
            .map_err(|error| error.to_string())?;

        Ok(Self {
            http,
            limiter,
            profiles: Arc::new(profiles),
            feedback: Arc::new(feedback),
            concurrency: Arc::new(Semaphore::new(config.max_concurrent_requests)),
            max_response_bytes: config.max_response_bytes,
        })
    }
    // ============================================================
    // APPLICATION RESPONSE VALIDATION
    // ============================================================

    fn validate_application_response(
        request: &ResolvedRestRequest,
        config: &RestClientFeedbackConfig,
        body: &Value,
        status: u16,
    ) -> Result<(), RestError> {
        // Not every exchange returns application-level
        // status envelopes.
        //
        // Binance and Gate.io public market-data responses
        // may return raw JSON directly.
        //
        // Missing rules therefore mean no additional
        // application-envelope validation.
        let Some(rule) = config.application_rules.get(request.endpoint()) else {
            return Ok(());
        };

        match RestApplicationValidator::validate(rule, body) {
            RestApplicationOutcome::Success => Ok(()),

            RestApplicationOutcome::Failure { code, message } => {
                // We know the exchange rejected the operation,
                // but an arbitrary application code does not
                // prove which error category applies.
                //
                // Use the conservative non-retryable HttpError
                // classification until an explicit code mapping
                // identifies a more specific cause.
                Err(RestError::new(
                    RestErrorKind::HttpError,
                    request.profile(),
                    request.endpoint(),
                    message.unwrap_or_else(|| "exchange application error".into()),
                )
                .with_http_status(status)
                .with_exchange_code(code))
            }

            RestApplicationOutcome::InvalidEnvelope { reason } => Err(RestError::new(
                RestErrorKind::InvalidResponse,
                request.profile(),
                request.endpoint(),
                reason,
            )
            .with_http_status(status)),
        }
    }
    /// Execute one REST attempt under a single end-to-end deadline.
    ///
    /// The deadline includes:
    /// - Rate-limit admission.
    /// - Concurrency admission.
    /// - HTTP connection and response reading.
    /// - Exchange feedback and application validation.
    ///
    /// Dropping the inner future also cancels pending limiter
    /// admission through the existing PendingGuard.
    ///
    /// Retry coordination remains responsible for the overall
    /// deadline across multiple attempts and backoff periods.
    pub async fn execute_once(
        &self,
        request: &ResolvedRestRequest,
    ) -> Result<ExchangeRestResponse, RestError> {
        tokio::time::timeout(request.timeout(), self.execute_once_inner(request))
            .await
            .map_err(|_| {
                RestError::new(
                    RestErrorKind::Timeout,
                    request.profile(),
                    request.endpoint(),
                    "REST attempt deadline exceeded",
                )
            })?
    }
    // ========================================================
    // SINGLE HTTP ATTEMPT
    // ========================================================

    pub async fn execute_once_inner(
        &self,
        request: &ResolvedRestRequest,
    ) -> Result<ExchangeRestResponse, RestError> {
        let started = Instant::now();

        let error = |kind, message: String| {
            RestError::new(kind, request.profile(), request.endpoint(), message)
        };

        // ====================================================
        // RESOLVE COMPILED OPERATION
        // ====================================================

        let profile = self.profiles.get(request.profile()).ok_or_else(|| {
            error(
                RestErrorKind::Configuration,
                "missing compiled REST limiter profile".into(),
            )
        })?;

        let feedback_config = self.feedback.get(request.profile()).ok_or_else(|| {
            error(
                RestErrorKind::Configuration,
                "missing REST feedback configuration".into(),
            )
        })?;

        let mut weights = WeightParameters::new();

        for (name, value) in request.query() {
            if let Ok(number) = value.parse::<u64>() {
                weights.insert(name.clone(), number);
            }
        }

        let operation = profile
            .operation(request.endpoint(), &weights, request.priority())
            .map_err(|source| error(RestErrorKind::Configuration, source.to_string()))?;

        // ====================================================
        // EXECUTION DEADLINE
        // ====================================================

        let result: Result<ExchangeRestResponse, RestError> = async {
            // --------------------------------------------
            // RATE-LIMIT ADMISSION
            // --------------------------------------------

            self.limiter
                .acquire(&operation)
                .await
                .map_err(|source| error(RestErrorKind::ResourceUnavailable, source.to_string()))?;

            // --------------------------------------------
            // CONCURRENCY
            // --------------------------------------------

            let _permit =
                self.concurrency.acquire().await.map_err(|source| {
                    error(RestErrorKind::ResourceUnavailable, source.to_string())
                })?;

            // --------------------------------------------
            // HTTP REQUEST
            // --------------------------------------------

            let builder = match request.method() {
                RestMethod::Get => self.http.get(request.url()),

                RestMethod::Post => {
                    return Err(error(
                        RestErrorKind::Configuration,
                        "POST requests are not supported".into(),
                    ));
                }
            };

            let response = builder
                .query(request.query())
                .timeout(request.timeout())
                .send()
                .await
                .map_err(|source| self.transport_error(request, source))?;

            let status = response.status().as_u16();

            let headers =
                Self::filtered_headers(response.headers(), feedback_config.rate_limiting.as_ref());

            // =================================================
            // EARLY FEEDBACK
            // =================================================

            // Inspect status and headers BEFORE reading the
            // response body.
            //
            // This guarantees that an HTTP 429 or configured
            // ban applies a cooldown even when body reading
            // subsequently fails.
            let early_feedback = RestFeedbackInterpreter::inspect(
                feedback_config.rate_limiting.as_ref(),
                status,
                &headers,
                None,
            );

            self.apply_feedback(request, &operation, feedback_config, &early_feedback)?;

            // =================================================
            // BODY SIZE — CONTENT LENGTH
            // =================================================

            if response
                .content_length()
                .is_some_and(|length| length > self.max_response_bytes as u64)
            {
                return Err(error(
                    RestErrorKind::ResponseTooLarge,
                    "Content-Length exceeds configured limit".into(),
                )
                .with_http_status(status));
            }

            // =================================================
            // BOUNDED BODY READING
            // =================================================

            let mut response = response;
            let mut body = Vec::new();

            while let Some(chunk) = response
                .chunk()
                .await
                .map_err(|source| self.transport_error(request, source))?
            {
                let remaining = self.max_response_bytes.saturating_sub(body.len());

                if chunk.len() > remaining {
                    return Err(error(
                        RestErrorKind::ResponseTooLarge,
                        "response body exceeds configured limit".into(),
                    )
                    .with_http_status(status));
                }

                body.extend_from_slice(&chunk);
            }

            // =================================================
            // OPTIONAL JSON PARSING
            // =================================================

            // Some exchanges return non-JSON error bodies.
            //
            // We still need to preserve the original HTTP
            // error classification in those cases.
            let parsed: Option<Value> = serde_json::from_slice(&body).ok();

            // =================================================
            // APPLICATION-LEVEL FEEDBACK
            // =================================================

            let final_feedback = RestFeedbackInterpreter::inspect(
                feedback_config.rate_limiting.as_ref(),
                status,
                &headers,
                parsed.as_ref(),
            );

            // Early feedback already applied HTTP-status
            // cooldowns and passive usage observations.
            //
            // Apply a second feedback event only when
            // inspecting JSON revealed throttling that
            // wasn't known from the HTTP status.
            if final_feedback.requires_cooldown() && !early_feedback.requires_cooldown() {
                self.apply_feedback(request, &operation, feedback_config, &final_feedback)?;
            }

            // =================================================
            // THROTTLING ERROR
            // =================================================

            if final_feedback.requires_cooldown() {
                let mut result = error(
                    RestErrorKind::RateLimited,
                    if final_feedback.banned {
                        "exchange imposed a temporary restriction".into()
                    } else {
                        "exchange request was rate limited".into()
                    },
                )
                .with_http_status(status);

                if let Some(code) = final_feedback.exchange_code.as_ref() {
                    result = result.with_exchange_code(code.clone());
                }

                if let Some(delay) = final_feedback.retry_after {
                    result = result.with_retry_after(delay);
                } else {
                    result = result.with_retry_after(feedback_config.fallback_cooldown);
                }

                return Err(result);
            }

            // =================================================
            // HTTP ERROR CLASSIFICATION
            // =================================================

            if let Some(mut http_error) =
                RestError::from_http_status(request.profile(), request.endpoint(), status)
            {
                if let Some(code) = final_feedback.exchange_code {
                    http_error = http_error.with_exchange_code(code);
                }

                return Err(http_error);
            }

            // =================================================
            // SUCCESSFUL JSON RESPONSE
            // =================================================

            let json = parsed.ok_or_else(|| {
                error(
                    RestErrorKind::InvalidResponse,
                    "successful HTTP response contains invalid JSON".into(),
                )
                .with_http_status(status)
            })?;
            // =================================================
            // APPLICATION-LEVEL VALIDATION
            // =================================================

            // Rate-limit feedback and HTTP status classification
            // have already been processed.
            //
            // Now verify that the exchange itself considers
            // the response successful.
            Self::validate_application_response(request, feedback_config, &json, status)?;
            Ok(ExchangeRestResponse::new(
                request.profile(),
                request.endpoint(),
                status,
                json,
                headers,
                started.elapsed(),
                body.len(),
            ))
        }
        .await;

        result
    }

    // ========================================================
    // RATE-LIMIT FEEDBACK INTEGRATION
    // ========================================================

    fn apply_feedback(
        &self,
        request: &ResolvedRestRequest,
        operation: &RateLimitedOperation,
        config: &RestClientFeedbackConfig,
        feedback: &RestFeedback,
    ) -> Result<(), RestError> {
        let error = |message: String| {
            RestError::new(
                RestErrorKind::Configuration,
                request.profile(),
                request.endpoint(),
                message,
            )
        };

        // Only usage bindings relevant to this exact
        // operation should be passed to the adapter.
        //
        // A profile may define different endpoint budgets
        // that are not all charged by every request.
        let charged: HashSet<_> = operation
            .charges
            .iter()
            .map(|charge| &charge.budget)
            .collect();

        let applicable: Vec<RestUsageBinding> = config
            .usage_bindings
            .iter()
            .filter(|binding| charged.contains(&binding.budget))
            .cloned()
            .collect();

        let normalized = RestFeedbackAdapter::normalize(
            feedback,
            operation,
            config.fallback_cooldown,
            &applicable,
        )
        .map_err(|source| error(source.to_string()))?;

        if normalized.is_empty() {
            return Ok(());
        }

        // The manager validates all budget identities before
        // mutating any quota or cooldown state.
        //
        // It is the sole authority for rate-limit admission.
        self.limiter
            .observe_feedback(&normalized)
            .map_err(|source| error(source.to_string()))
    }

    // ========================================================
    // TRANSPORT ERROR CLASSIFICATION
    // ========================================================

    fn transport_error(&self, request: &ResolvedRestRequest, source: reqwest::Error) -> RestError {
        let kind = if source.is_timeout() {
            RestErrorKind::Timeout
        } else {
            RestErrorKind::Network
        };

        RestError::new(
            kind,
            request.profile(),
            request.endpoint(),
            source.to_string(),
        )
    }

    // ========================================================
    // RESPONSE HEADERS
    // ========================================================

    fn filtered_headers(
        headers: &HeaderMap,
        config: Option<&RestRateLimiting>,
    ) -> RestResponseHeaders {
        let mut result = RestResponseHeaders::new();

        let configured_usage_prefix = config
            .and_then(|config| config.usage_header_prefix.as_deref())
            .map(str::to_ascii_lowercase);

        for (name, value) in headers {
            let name = name.as_str();

            let configured_usage = configured_usage_prefix
                .as_ref()
                .is_some_and(|prefix| !prefix.is_empty() && name.starts_with(prefix));

            let relevant = name == "retry-after"
                || name == "content-type"
                || name == "content-length"
                || name.starts_with("x-mbx-")
                || name.starts_with("x-ratelimit-")
                || name.starts_with("x-rate-limit-")
                || name.starts_with("ratelimit-")
                || configured_usage;

            if !relevant {
                continue;
            }

            if let Ok(value) = value.to_str() {
                // Bound diagnostic header values.
                //
                // Extremely large values are not useful for
                // quota feedback and must not be retained.
                if value.len() <= 1024 {
                    result.insert(name.to_owned(), value.to_owned());
                }
            }
        }

        result
    }
}

// ============================================================
// TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    use crate::live::{
        config::exchange::ExchangeId,
        infrastructure::limiter::{
            BudgetCharge, BudgetId, BudgetScope, RateLimitPolicySet, WindowQuota,
        },
    };

    fn budget(rule: &str) -> BudgetId {
        BudgetId::new(
            ExchangeId::Binance,
            "spot_rest",
            rule,
            BudgetScope::Egress("primary".into()),
        )
        .unwrap()
    }

    fn operation(id: BudgetId) -> RateLimitedOperation {
        RateLimitedOperation::new(vec![BudgetCharge::new(id, 1).unwrap()]).unwrap()
    }

    fn feedback_config() -> RestClientFeedbackConfig {
        RestClientFeedbackConfig {
            rate_limiting: None,
            usage_bindings: Vec::new(),
            fallback_cooldown: Duration::from_secs(5),
            application_rules: BTreeMap::new(),
        }
    }

    // ========================================================
    // CONFIGURATION
    // ========================================================

    #[test]
    fn rejects_zero_concurrency() {
        assert!(
            RestClientConfig {
                max_concurrent_requests: 0,
                max_response_bytes: 1_000_000,
            }
            .validate()
            .is_err()
        );
    }

    #[test]
    fn rejects_zero_response_limit() {
        assert!(
            RestClientConfig {
                max_concurrent_requests: 8,
                max_response_bytes: 0,
            }
            .validate()
            .is_err()
        );
    }

    #[test]
    fn rejects_zero_fallback_cooldown() {
        let mut config = feedback_config();

        config.fallback_cooldown = Duration::ZERO;

        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_duplicate_usage_header_bindings() {
        let mut config = feedback_config();

        config.usage_bindings = vec![
            RestUsageBinding {
                header: "X-MBX-USED-WEIGHT-1M".into(),
                budget: budget("first"),
                window: Duration::from_secs(60),
            },
            RestUsageBinding {
                header: "x-mbx-used-weight-1m".into(),
                budget: budget("second"),
                window: Duration::from_secs(60),
            },
        ];

        assert!(config.validate().is_err());
    }

    // ========================================================
    // HEADER FILTERING
    // ========================================================

    #[test]
    fn retains_throttling_headers() {
        let mut headers = HeaderMap::new();

        headers.insert("retry-after", "10".parse().unwrap());

        headers.insert("x-mbx-used-weight-1m", "42".parse().unwrap());

        headers.insert("set-cookie", "secret=value".parse().unwrap());

        let result = ExchangeRestClient::filtered_headers(&headers, None);

        assert_eq!(result.get("retry-after"), Some(&"10".to_string()),);

        assert_eq!(result.get("x-mbx-used-weight-1m"), Some(&"42".to_string()),);

        assert!(!result.contains_key("set-cookie"));
    }

    // ========================================================
    // MANAGER INTEGRATION
    // ========================================================

    #[tokio::test]
    async fn feedback_updates_shared_manager_cooldown() {
        let id = budget("request_weight");

        let mut policies = RateLimitPolicySet::new();

        policies
            .insert(
                id.clone(),
                WindowQuota::new(100, Duration::from_secs(60)).unwrap(),
            )
            .unwrap();

        let manager = RateLimitManager::new(policies, 16).unwrap();

        // Construct a client with no HTTP request required.
        //
        // We only exercise the integration boundary.
        let client = ExchangeRestClient {
            http: Client::new(),
            limiter: manager.clone(),
            profiles: Arc::new(BTreeMap::new()),
            feedback: Arc::new(BTreeMap::new()),
            concurrency: Arc::new(Semaphore::new(1)),
            max_response_bytes: 1024,
        };

        let request = ResolvedRestRequest::new(
            "binance_spot".into(),
            "depth_snapshot".into(),
            RestMethod::Get,
            "https://example.invalid/depth".into(),
            Default::default(),
            Duration::from_secs(5),
            crate::live::infrastructure::limiter::RequestPriority::Normal,
        );

        let feedback = RestFeedback {
            throttled: true,
            retry_after: Some(Duration::from_secs(3)),
            ..Default::default()
        };

        client
            .apply_feedback(
                &request,
                &operation(id.clone()),
                &feedback_config(),
                &feedback,
            )
            .unwrap();

        let remaining = manager.cooldown_remaining(&id).unwrap();

        assert!(remaining > Duration::ZERO);
        assert!(remaining <= Duration::from_secs(3));
    }

    #[tokio::test]
    async fn feedback_usage_does_not_modify_local_accounting() {
        let id = budget("request_weight");

        let mut policies = RateLimitPolicySet::new();

        policies
            .insert(
                id.clone(),
                WindowQuota::new(100, Duration::from_secs(60)).unwrap(),
            )
            .unwrap();

        let manager = RateLimitManager::new(policies, 16).unwrap();

        let client = ExchangeRestClient {
            http: Client::new(),
            limiter: manager.clone(),
            profiles: Arc::new(BTreeMap::new()),
            feedback: Arc::new(BTreeMap::new()),
            concurrency: Arc::new(Semaphore::new(1)),
            max_response_bytes: 1024,
        };

        let request = ResolvedRestRequest::new(
            "binance_spot".into(),
            "depth_snapshot".into(),
            RestMethod::Get,
            "https://example.invalid/depth".into(),
            Default::default(),
            Duration::from_secs(5),
            crate::live::infrastructure::limiter::RequestPriority::Normal,
        );

        let feedback = RestFeedback {
            usage: vec![super::super::feedback::RestUsageObservation {
                header: "x-mbx-used-weight-1m".into(),
                used: 85,
            }],
            ..Default::default()
        };

        let mut config = feedback_config();

        config.usage_bindings.push(RestUsageBinding {
            header: "x-mbx-used-weight-1m".into(),
            budget: id.clone(),
            window: Duration::from_secs(60),
        });

        client
            .apply_feedback(&request, &operation(id.clone()), &config, &feedback)
            .unwrap();

        let snapshot = manager.snapshot();

        let usage = snapshot
            .budget(&id)
            .unwrap()
            .observed_exchange_usage
            .as_ref()
            .unwrap();

        assert_eq!(usage.used, 85);

        // Exchange-reported usage is observational.
        assert_eq!(snapshot.budget(&id).unwrap().locally_used, 0,);
    }
    // ============================================================
    // APPLICATION-LEVEL RESPONSE TESTS
    // ============================================================

    #[test]
    fn accepts_successful_application_response() {
        use serde_json::json;

        let mut config = feedback_config();

        config.application_rules.insert(
            "depth_snapshot".into(),
            RestApplicationRule::new(
                "retCode",
                vec!["0".into()],
                Some("retMsg".into()),
                Some("result".into()),
            )
            .unwrap(),
        );

        let request = ResolvedRestRequest::new(
            "binance_spot".into(),
            "depth_snapshot".into(),
            RestMethod::Get,
            "https://example.invalid/depth".into(),
            Default::default(),
            Duration::from_secs(5),
            crate::live::infrastructure::limiter::RequestPriority::Normal,
        );

        let body = json!({
            "retCode": 0,
            "retMsg": "OK",
            "result": {
                "list": []
            }
        });

        assert!(
            ExchangeRestClient::validate_application_response(&request, &config, &body, 200,)
                .is_ok()
        );
    }

    #[test]
    fn rejects_application_error_inside_http_200() {
        use serde_json::json;

        let mut config = feedback_config();

        config.application_rules.insert(
            "depth_snapshot".into(),
            RestApplicationRule::new(
                "retCode",
                vec!["0".into()],
                Some("retMsg".into()),
                Some("result".into()),
            )
            .unwrap(),
        );

        let request = ResolvedRestRequest::new(
            "binance_spot".into(),
            "depth_snapshot".into(),
            RestMethod::Get,
            "https://example.invalid/depth".into(),
            Default::default(),
            Duration::from_secs(5),
            crate::live::infrastructure::limiter::RequestPriority::Normal,
        );

        let body = json!({
            "retCode": 10001,
            "retMsg": "Invalid parameter",
            "result": {}
        });

        let error =
            ExchangeRestClient::validate_application_response(&request, &config, &body, 200)
                .unwrap_err();

        assert_eq!(error.kind(), RestErrorKind::HttpError);
        assert_eq!(error.http_status(), Some(200));
        assert_eq!(error.exchange_code(), Some("10001"));

        assert!(!error.is_transient_candidate());
    }

    #[test]
    fn rejects_missing_application_status() {
        use serde_json::json;

        let mut config = feedback_config();

        config.application_rules.insert(
            "depth_snapshot".into(),
            RestApplicationRule::new(
                "code",
                vec!["00000".into()],
                Some("msg".into()),
                Some("data".into()),
            )
            .unwrap(),
        );

        let request = ResolvedRestRequest::new(
            "binance_spot".into(),
            "depth_snapshot".into(),
            RestMethod::Get,
            "https://example.invalid/depth".into(),
            Default::default(),
            Duration::from_secs(5),
            crate::live::infrastructure::limiter::RequestPriority::Normal,
        );

        let body = json!({
            "msg": "success",
            "data": []
        });

        let error =
            ExchangeRestClient::validate_application_response(&request, &config, &body, 200)
                .unwrap_err();

        assert_eq!(error.kind(), RestErrorKind::InvalidResponse,);
    }

    #[test]
    fn raw_json_endpoint_requires_no_application_rule() {
        use serde_json::json;

        let config = feedback_config();

        let request = ResolvedRestRequest::new(
            "binance_spot".into(),
            "depth_snapshot".into(),
            RestMethod::Get,
            "https://example.invalid/depth".into(),
            Default::default(),
            Duration::from_secs(5),
            crate::live::infrastructure::limiter::RequestPriority::Normal,
        );

        let body = json!({
            "lastUpdateId": 123456,
            "bids": [],
            "asks": []
        });

        assert!(
            ExchangeRestClient::validate_application_response(&request, &config, &body, 200,)
                .is_ok()
        );
    }
    // ============================================================
    // END-TO-END DEADLINE REGRESSION
    // ============================================================

    fn deadline_test_client() -> (
        ExchangeRestClient,
        RateLimitManager,
        crate::live::infrastructure::limiter::RateLimitedOperation,
    ) {
        use crate::live::config::exchange::ExchangeConfig;
        use crate::live::infrastructure::limiter::factory::{
            RestBudgetBinding, RestLimiterBindings, RestLimiterCompiler, WeightParameters,
        };
        use crate::live::infrastructure::limiter::{BudgetScope, RequestPriority};

        let config: ExchangeConfig = toml::from_str(
            r#"
schema_version = 1

[exchange]
id = "binance"
market = "spot"
profile = "binance_spot"

[websocket]
base_url = "wss://example.invalid"

[channels.trades]
enabled = true
format = "trades/binance_spot"

[rest]
base_url = "http://127.0.0.1:9"

[rest.rate_limiting]
strategy = "ip_window"
ip_max_requests = 1
ip_window_seconds = 60

[rest.endpoints.recent_trades]
method = "GET"
path = "/trades"
"#,
        )
        .unwrap();

        let bindings = RestLimiterBindings {
            ip: Some(RestBudgetBinding::new(
                "spot_rest",
                BudgetScope::Egress("deadline-test".into()),
            )),
            ..Default::default()
        };

        let compiled = RestLimiterCompiler::compile(&config, &bindings).unwrap();

        let operation = compiled
            .operation(
                "recent_trades",
                &WeightParameters::new(),
                RequestPriority::Normal,
            )
            .unwrap();

        let manager = RateLimitManager::new(compiled.policies.clone(), 16).unwrap();

        let profiles = BTreeMap::from([("binance_spot".to_owned(), compiled)]);

        let feedback = BTreeMap::from([("binance_spot".to_owned(), feedback_config())]);

        let client = ExchangeRestClient::new(
            manager.clone(),
            profiles,
            feedback,
            RestClientConfig {
                max_concurrent_requests: 1,
                max_response_bytes: 1024,
            },
        )
        .unwrap();

        (client, manager, operation)
    }

    #[tokio::test]
    async fn single_attempt_deadline_covers_limiter_wait() {
        use crate::live::infrastructure::limiter::RequestPriority;

        let (client, manager, operation) = deadline_test_client();

        // Exhaust the only available request permit.
        manager.acquire(&operation).await.unwrap();

        let request = ResolvedRestRequest::new(
            "binance_spot".into(),
            "recent_trades".into(),
            RestMethod::Get,
            "http://127.0.0.1:9/trades".into(),
            Default::default(),
            Duration::from_millis(75),
            RequestPriority::Normal,
        );

        // No HTTP connection should be attempted because
        // the request is blocked by the limiter.
        let result = tokio::time::timeout(Duration::from_secs(2), client.execute_once(&request))
            .await
            .expect("single-attempt deadline was not enforced");

        let error = result.unwrap_err();

        assert_eq!(error.kind(), RestErrorKind::Timeout);
        assert_eq!(error.profile(), "binance_spot");
        assert_eq!(error.endpoint(), "recent_trades");

        // The cancelled limiter waiter must be removed.
        assert_eq!(manager.pending_count(), 0);

        // The timed-out request must not consume quota.
        let snapshot = manager.snapshot();

        assert_eq!(snapshot.total_pending_requests, 0);
        assert_eq!(snapshot.budgets.len(), 1);
        assert_eq!(snapshot.budgets[0].locally_used, 1);
    }

    #[tokio::test]
    async fn external_cancellation_removes_limiter_waiter() {
        use crate::live::infrastructure::limiter::RequestPriority;

        let (client, manager, operation) = deadline_test_client();

        manager.acquire(&operation).await.unwrap();

        let request = ResolvedRestRequest::new(
            "binance_spot".into(),
            "recent_trades".into(),
            RestMethod::Get,
            "http://127.0.0.1:9/trades".into(),
            Default::default(),
            Duration::from_secs(5),
            RequestPriority::Normal,
        );

        // The caller cancels earlier than the executor deadline.
        let result =
            tokio::time::timeout(Duration::from_millis(75), client.execute_once(&request)).await;

        assert!(
            result.is_err(),
            "the request unexpectedly completed despite exhausted quota",
        );

        // The future was dropped by the external timeout.
        assert_eq!(manager.pending_count(), 0);

        let snapshot = manager.snapshot();

        assert_eq!(snapshot.budgets.len(), 1);
        assert_eq!(snapshot.budgets[0].locally_used, 1);
    }
    // ============================================================
    // R2 — REAL LOCAL HTTP EXECUTOR INTEGRATION
    // ============================================================

    fn start_test_http_server(
        status: &str,
        body: &str,
        extra_headers: &[(&str, &str)],
        stall: bool,
    ) -> (String, std::thread::JoinHandle<()>) {
        use std::io::{Read, Write};
        use std::net::TcpListener;

        // The OS chooses an available ephemeral port.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();

        let address = listener.local_addr().unwrap();

        let status = status.to_owned();
        let body = body.to_owned();

        let headers: Vec<(String, String)> = extra_headers
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect();

        let handle = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();

            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();

            // Read enough of the incoming HTTP request to verify
            // that the real reqwest executor reached our server.
            let mut request = Vec::new();
            let mut buffer = [0_u8; 1024];

            loop {
                let count = stream.read(&mut buffer).unwrap();

                if count == 0 {
                    break;
                }

                request.extend_from_slice(&buffer[..count]);

                if request.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                    break;
                }

                assert!(
                    request.len() <= 16 * 1024,
                    "unexpectedly large test HTTP request",
                );
            }

            let request_text = String::from_utf8_lossy(&request);

            assert!(
                request_text.starts_with("GET /trades"),
                "unexpected HTTP request: {request_text}",
            );

            if stall {
                // Longer than the configured request deadline.
                std::thread::sleep(Duration::from_millis(300));
                return;
            }

            let mut response = format!(
                "HTTP/1.1 {status}\r\n\
             Content-Type: application/json\r\n\
             Content-Length: {}\r\n\
             Connection: close\r\n",
                body.len(),
            );

            for (name, value) in &headers {
                response.push_str(&format!("{name}: {value}\r\n"));
            }

            response.push_str("\r\n");
            response.push_str(&body);

            stream.write_all(response.as_bytes()).unwrap();
            stream.flush().unwrap();
        });

        (
            format!("http://127.0.0.1:{}/trades", address.port()),
            handle,
        )
    }

    fn local_http_request(url: String, timeout: Duration) -> ResolvedRestRequest {
        use crate::live::infrastructure::limiter::RequestPriority;

        ResolvedRestRequest::new(
            "binance_spot".into(),
            "recent_trades".into(),
            RestMethod::Get,
            url,
            Default::default(),
            timeout,
            RequestPriority::Normal,
        )
    }
    #[tokio::test]
    async fn real_http_200_success() {
        let (client, manager, _) = deadline_test_client();

        let (url, server) = start_test_http_server(
            "200 OK",
            r#"[{"id":123,"price":"80000","qty":"0.01"}]"#,
            &[],
            false,
        );

        let request = local_http_request(url, Duration::from_secs(2));

        let response = client.execute_once(&request).await.unwrap();

        assert_eq!(response.status(), 200);
        assert_eq!(response.profile(), "binance_spot");
        assert_eq!(response.endpoint(), "recent_trades");

        assert_eq!(response.body()[0]["id"], 123);
        assert_eq!(response.attempts(), 1);

        let snapshot = manager.snapshot();

        assert_eq!(snapshot.total_pending_requests, 0);
        assert_eq!(snapshot.budgets[0].locally_used, 1);

        server.join().unwrap();
    }

    #[tokio::test]
    async fn real_http_500_server_error() {
        let (client, manager, _) = deadline_test_client();

        let (url, server) = start_test_http_server(
            "500 Internal Server Error",
            r#"{"error":"internal"}"#,
            &[],
            false,
        );

        let request = local_http_request(url, Duration::from_secs(2));

        let error = client.execute_once(&request).await.unwrap_err();

        assert_eq!(error.kind(), RestErrorKind::ServerError);
        assert_eq!(error.http_status(), Some(500));

        assert_eq!(manager.pending_count(), 0);

        server.join().unwrap();
    }

    #[tokio::test]
    async fn real_http_429_applies_cooldown() {
        let (client, manager, _) = deadline_test_client();

        let (url, server) = start_test_http_server(
            "429 Too Many Requests",
            r#"{"code":429,"msg":"Too many requests"}"#,
            &[("Retry-After", "3")],
            false,
        );

        let request = local_http_request(url, Duration::from_secs(2));

        let error = client.execute_once(&request).await.unwrap_err();

        assert_eq!(error.kind(), RestErrorKind::RateLimited);
        assert_eq!(error.http_status(), Some(429));
        assert_eq!(error.retry_after(), Some(Duration::from_secs(3)),);

        let snapshot = manager.snapshot();

        assert_eq!(snapshot.total_pending_requests, 0);
        assert_eq!(snapshot.active_cooldowns, 1);

        assert!(snapshot.budgets[0].cooldown_remaining > Duration::ZERO,);

        server.join().unwrap();
    }

    #[tokio::test]
    async fn real_http_rejects_malformed_json() {
        let (client, manager, _) = deadline_test_client();

        let (url, server) = start_test_http_server("200 OK", r#"{"broken":"#, &[], false);

        let request = local_http_request(url, Duration::from_secs(2));

        let error = client.execute_once(&request).await.unwrap_err();

        assert_eq!(error.kind(), RestErrorKind::InvalidResponse,);

        assert_eq!(manager.pending_count(), 0);

        server.join().unwrap();
    }

    #[tokio::test]
    async fn real_http_rejects_oversized_body() {
        // deadline_test_client() configures max_response_bytes=1024.
        let (client, manager, _) = deadline_test_client();

        let body = "x".repeat(4096);

        let (url, server) = start_test_http_server("200 OK", &body, &[], false);

        let request = local_http_request(url, Duration::from_secs(2));

        let error = client.execute_once(&request).await.unwrap_err();

        assert_eq!(error.kind(), RestErrorKind::ResponseTooLarge,);

        assert_eq!(manager.pending_count(), 0);

        server.join().unwrap();
    }

    #[tokio::test]
    async fn real_http_timeout_during_response() {
        let (client, manager, _) = deadline_test_client();

        let (url, server) = start_test_http_server("200 OK", "{}", &[], true);

        let request = local_http_request(url, Duration::from_millis(75));

        let error = client.execute_once(&request).await.unwrap_err();

        assert_eq!(error.kind(), RestErrorKind::Timeout);

        // The request passed limiter admission, so one
        // unit of quota was legitimately consumed.
        let snapshot = manager.snapshot();

        assert_eq!(snapshot.total_pending_requests, 0);
        assert_eq!(snapshot.budgets[0].locally_used, 1);

        server.join().unwrap();
    }
    // ============================================================
    // R2.2 — REST SERVICE → CANONICAL TRADE INTEGRATION
    // ============================================================

    #[tokio::test]
    async fn real_http_service_normalizes_binance_trades() {
        use std::fs;
        use std::path::PathBuf;
        use std::time::{SystemTime, UNIX_EPOCH};

        use crate::canonical::TradeSide;

        use crate::live::config::{
            ExchangeConfigStore,
            exchange::{ExchangeId, MarketType},
            format::FormatConfigStore,
        };

        use crate::live::infrastructure::normalization::{
            NormalizationContext, NormalizationSource, QuantityPrecisionPolicy, RestTradeIngestion,
            RestTradeNormalizationCompiler, TradeDecoder, TradeNormalizationCompiler,
        };

        use crate::live::infrastructure::rest::{ExchangeRestService, RestRequest};

        use crate::live::stream::request::{
            CreateSourceStreamRequest, QuantityType, SourceChannel,
        };

        use rust_decimal::Decimal;

        // --------------------------------------------------------
        // 1. Start an actual HTTP server
        // --------------------------------------------------------

        let native_response = r#"[
        {
            "id": 6787820768,
            "price": "80000.50",
            "qty": "0.125",
            "time": 1792806211156,
            "isBuyerMaker": true
        },
        {
            "id": 6787820769,
            "price": "80001.25",
            "qty": "0.250",
            "time": 1792806211157,
            "isBuyerMaker": false
        }
    ]"#;

        let (url, server) = start_test_http_server("200 OK", native_response, &[], false);

        let base_url = url.strip_suffix("/trades").expect("invalid local test URL");

        // --------------------------------------------------------
        // 2. Load production Binance Spot configuration
        // --------------------------------------------------------

        let data_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../data/.config/live");

        let production_path = data_root.join("exchanges/binance_spot.toml");

        let production_toml = fs::read_to_string(production_path).unwrap();

        let mut document: toml::Value = toml::from_str(&production_toml).unwrap();

        // Redirect only the HTTP transport destination.
        //
        // All application-response, trade-normalization,
        // instrument and quantity mappings remain unchanged.

        document["rest"]["base_url"] = toml::Value::String(base_url.to_owned());

        document["rest"]["endpoints"]["recent_trades"]["path"] =
            toml::Value::String("/trades".into());

        // --------------------------------------------------------
        // 3. Create an isolated temporary config directory
        // --------------------------------------------------------

        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();

        let temp_dir =
            std::env::temp_dir().join(format!("marketforge-r22-{}-{nonce}", std::process::id(),));

        fs::create_dir(&temp_dir).unwrap();

        fs::write(
            temp_dir.join("binance_spot.toml"),
            toml::to_string(&document).unwrap(),
        )
        .unwrap();

        let configs = ExchangeConfigStore::load_dir(&temp_dir).unwrap();

        let exchange = configs.get("binance_spot").unwrap();

        let endpoint = exchange.rest.endpoint("recent_trades").unwrap();

        // --------------------------------------------------------
        // 4. Compile production trade normalization
        // --------------------------------------------------------

        let formats = FormatConfigStore::load_dir(data_root.join("formats")).unwrap();

        let format = formats.trade("binance_spot").unwrap();

        let websocket_plan = TradeNormalizationCompiler::compile_websocket(
            format,
            1000,
            QuantityPrecisionPolicy::Exact,
        )
        .unwrap();

        let rest_plan =
            RestTradeNormalizationCompiler::compile_from_config(&websocket_plan, endpoint, 1000)
                .unwrap();

        let decoder = TradeDecoder::new(rest_plan).unwrap();

        // --------------------------------------------------------
        // 5. Construct authoritative instrument context
        // --------------------------------------------------------

        let source_request = CreateSourceStreamRequest {
            instrument_id: 6163,
            exchange: ExchangeId::Binance,
            symbol: "BTCUSDT".into(),
            market_category: MarketType::Spot,
            instrument_type: "spot".into(),

            base_asset: Some("BTC".into()),
            quote_asset: Some("USDT".into()),
            settlement_asset: None,

            quantity_type: QuantityType::Base,
            contract_value: None,
            contract_value_asset: None,

            channel: SourceChannel::Trades,
        };

        let context = NormalizationContext::new(&source_request, "r22-binance-spot").unwrap();

        // --------------------------------------------------------
        // 6. Construct real REST service with existing limiter
        // --------------------------------------------------------

        // Reuses the compiled test limiter and HTTP client
        // from R1/R2. No bypass of request admission occurs.

        let (client, manager, _) = deadline_test_client();

        let service = ExchangeRestService::new(&configs, client);

        // --------------------------------------------------------
        // 7. Execute the logical REST request
        // --------------------------------------------------------

        let request =
            RestRequest::new("binance_spot", "recent_trades").with_parameter("symbol", "BTCUSDT");

        let normalized = RestTradeIngestion::execute_once(&service, request, &decoder, &context)
            .await
            .expect("real REST ingestion failed");

        // --------------------------------------------------------
        // 8. Verify canonical batch identity
        // --------------------------------------------------------

        assert_eq!(normalized.source(), NormalizationSource::Rest,);

        assert_eq!(normalized.stream_id(), "r22-binance-spot");

        assert_eq!(normalized.record_count(), 2);

        let trades = normalized.trades();

        // --------------------------------------------------------
        // 9. Verify first canonical trade
        // --------------------------------------------------------

        let first = &trades[0];

        assert_eq!(first.trade_id.as_deref(), Some("6787820768"),);

        assert_eq!(first.side, TradeSide::Sell);

        assert_eq!(first.price, Decimal::from_str_exact("80000.50").unwrap(),);

        assert_eq!(
            first.quantity_base,
            Some(Decimal::from_str_exact("0.125").unwrap()),
        );

        assert_eq!(
            first.quantity_quote,
            Some(Decimal::from_str_exact("10000.06250").unwrap()),
        );

        assert_eq!(first.envelope.event_timestamp_ns, 1_792_806_211_156_000_000,);

        assert_eq!(first.envelope.instrument_id, 6163);
        assert_eq!(first.envelope.symbol, "BTCUSDT");
        assert_eq!(first.envelope.stream_id, "r22-binance-spot");

        // --------------------------------------------------------
        // 10. Verify second canonical trade
        // --------------------------------------------------------

        let second = &trades[1];

        assert_eq!(second.trade_id.as_deref(), Some("6787820769"),);

        assert_eq!(second.side, TradeSide::Buy);

        assert_eq!(second.price, Decimal::from_str_exact("80001.25").unwrap(),);

        assert_eq!(
            second.quantity_base,
            Some(Decimal::from_str_exact("0.250").unwrap()),
        );

        assert_eq!(
            second.quantity_quote,
            Some(Decimal::from_str_exact("20000.31250").unwrap()),
        );

        assert_eq!(
            second.envelope.event_timestamp_ns,
            1_792_806_211_157_000_000,
        );

        // --------------------------------------------------------
        // 11. Verify actual limiter admission
        // --------------------------------------------------------

        let limiter = manager.snapshot();

        assert_eq!(limiter.total_pending_requests, 0);
        assert_eq!(limiter.budgets.len(), 1);

        // Exactly one HTTP request was admitted.
        assert_eq!(limiter.budgets[0].locally_used, 1);

        // --------------------------------------------------------
        // 12. Cleanup
        // --------------------------------------------------------

        server.join().unwrap();

        fs::remove_dir_all(&temp_dir).unwrap();

        println!(
            "PASS R2.2: real HTTP → REST service → \
         limiter → canonical trades"
        );
    }
}
