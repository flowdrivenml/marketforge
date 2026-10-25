use super::response::RestResponseHeaders;
use crate::live::config::exchange::{RateLimitErrorCode, RestRateLimiting};
use crate::live::infrastructure::limiter::{
    BudgetId, CooldownFeedback, RateLimitFeedback, RateLimitFeedbackError, RateLimitedOperation,
    ThrottleKind, UsageFeedback,
};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::time::Duration;
// ============================================================
// FEEDBACK MODEL
// ============================================================

/// Exchange-reported quota usage.
///
/// Header-based usage is observational. It must not overwrite
/// the local limiter's accounting without a defined
/// reconciliation policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestUsageObservation {
    pub header: String,
    pub used: u64,
}

/// Throttling and quota information extracted from one
/// exchange HTTP response.
///
/// This is an interpretation of exchange feedback, not an
/// independent rate limiter.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RestFeedback {
    pub throttled: bool,
    pub banned: bool,

    pub retry_after: Option<Duration>,

    pub exchange_code: Option<String>,

    pub usage: Vec<RestUsageObservation>,
}

impl RestFeedback {
    pub fn requires_cooldown(&self) -> bool {
        self.throttled || self.banned
    }
}

// ============================================================
// FEEDBACK INTERPRETER
// ============================================================

/// Interprets exchange HTTP throttling responses.
///
/// Inputs:
/// - HTTP status.
/// - Selected response headers.
/// - Optional parsed JSON body.
/// - The exchange profile's REST rate-limit configuration.
///
/// No state is mutated. The caller forwards the result to
/// RateLimitManager using the relevant compiled budget IDs.
pub struct RestFeedbackInterpreter;

impl RestFeedbackInterpreter {
    pub fn inspect(
        config: Option<&RestRateLimiting>,
        status: u16,
        headers: &RestResponseHeaders,
        body: Option<&Value>,
    ) -> RestFeedback {
        let mut result = RestFeedback::default();

        let configured_throttle_status = config
            .and_then(|value| value.http_throttled_status)
            .and_then(|value| u16::try_from(value).ok());

        let configured_ban_status = config
            .and_then(|value| value.http_banned_status)
            .and_then(|value| u16::try_from(value).ok());

        // HTTP 429 has standard rate-limit semantics.
        //
        // HTTP 418 is NOT universally a ban. Only treat it
        // as one when the exchange explicitly configures it.
        result.throttled = status == 429 || configured_throttle_status == Some(status);

        result.banned = configured_ban_status == Some(status);

        // ----------------------------------------------------
        // EXCHANGE APPLICATION ERROR CODE
        // ----------------------------------------------------

        if let Some(body) = body {
            result.exchange_code = Self::extract_error_code(body);

            if let Some(expected) = config.and_then(|value| value.rate_limit_error_code.as_ref()) {
                if Self::matches_error_code(expected, result.exchange_code.as_deref()) {
                    result.throttled = true;
                }
            }
        }

        // ----------------------------------------------------
        // RETRY-AFTER
        // ----------------------------------------------------

        let honor_retry_after = config
            .and_then(|value| value.honor_retry_after)
            .unwrap_or(true);

        if result.requires_cooldown() && honor_retry_after {
            result.retry_after = Self::retry_after(headers);
        }

        // ----------------------------------------------------
        // PASSIVE USAGE OBSERVATIONS
        // ----------------------------------------------------

        if let Some(config) = config {
            if let Some(prefix) = &config.usage_header_prefix {
                result.usage = Self::extract_usage_headers(headers, prefix);
            }
        }

        result
    }

    // ========================================================
    // APPLICATION ERROR CODE
    // ========================================================

    fn extract_error_code(body: &Value) -> Option<String> {
        let object = body.as_object()?;

        // Common exchange-native error-code fields.
        //
        // These are inspected only for a configured code
        // match. An arbitrary nonzero code is not assumed
        // to mean rate limiting.
        for field in ["code", "retCode", "ret_code", "errorCode"] {
            let Some(value) = object.get(field) else {
                continue;
            };

            match value {
                Value::String(code) => {
                    return Some(code.clone());
                }

                Value::Number(code) => {
                    return Some(code.to_string());
                }

                _ => {}
            }
        }

        None
    }

    fn matches_error_code(expected: &RateLimitErrorCode, actual: Option<&str>) -> bool {
        let Some(actual) = actual else {
            return false;
        };

        match expected {
            RateLimitErrorCode::Integer(value) => actual.parse::<i64>().ok() == Some(*value),

            RateLimitErrorCode::String(value) => actual == value,
        }
    }

    // ========================================================
    // RETRY-AFTER
    // ========================================================

    fn retry_after(headers: &RestResponseHeaders) -> Option<Duration> {
        let value = Self::header(headers, "retry-after")?;

        // Retry-After also supports HTTP-date values.
        // Numeric delta-seconds are supported here.
        //
        // Unrecognized values must never be interpreted
        // as a zero-second cooldown.
        let seconds = value.trim().parse::<u64>().ok()?;

        Some(Duration::from_secs(seconds))
    }

    // ========================================================
    // USAGE HEADERS
    // ========================================================

    fn extract_usage_headers(
        headers: &RestResponseHeaders,
        prefix: &str,
    ) -> Vec<RestUsageObservation> {
        if prefix.trim().is_empty() {
            return Vec::new();
        }

        let prefix = prefix.to_ascii_lowercase();

        headers
            .iter()
            .filter_map(|(name, value)| {
                if !name.to_ascii_lowercase().starts_with(&prefix) {
                    return None;
                }

                let used = value.trim().parse::<u64>().ok()?;

                Some(RestUsageObservation {
                    header: name.to_ascii_lowercase(),
                    used,
                })
            })
            .collect()
    }

    // ========================================================
    // HEADER ACCESS
    // ========================================================

    fn header<'a>(headers: &'a RestResponseHeaders, name: &str) -> Option<&'a str> {
        headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }
}
// ============================================================
// EXPLICIT USAGE-HEADER BINDING
// ============================================================

/// Explicit mapping between one exchange usage header
/// and one compiled rate-limit budget.
///
/// This mapping is provided by the REST runtime initialization
/// layer after the actual exchange quota semantics are known.
///
/// A header must not be mapped to a budget merely because
/// the HTTP request happened to consume that budget.
#[derive(Debug, Clone)]
pub struct RestUsageBinding {
    pub header: String,
    pub budget: BudgetId,
    pub window: Duration,
}

// ============================================================
// NORMALIZATION ERRORS
// ============================================================

#[derive(Debug, thiserror::Error)]
pub enum RestFeedbackMappingError {
    #[error(transparent)]
    InvalidFeedback(#[from] RateLimitFeedbackError),

    #[error("REST feedback requires a nonempty operation")]
    EmptyOperation,

    #[error("duplicate REST usage-header binding '{0}'")]
    DuplicateUsageHeader(String),

    #[error("duplicate REST usage-budget binding")]
    DuplicateUsageBudget,

    #[error("REST usage binding has an empty header")]
    EmptyUsageHeader,

    #[error("REST usage binding has an invalid window")]
    InvalidUsageWindow,

    #[error("REST usage binding references an unrelated budget")]
    UnrelatedUsageBudget,
}

// ============================================================
// REST → RATE-LIMITER FEEDBACK ADAPTER
// ============================================================

/// Converts exchange REST feedback into the existing generic
/// rate-limiter feedback model.
///
/// This adapter does not modify RateLimitManager.
///
/// The caller must explicitly forward the result to
/// RateLimitManager::observe_feedback().
pub struct RestFeedbackAdapter;

impl RestFeedbackAdapter {
    /// Construct normalized limiter feedback for one HTTP
    /// operation.
    ///
    /// `fallback_cooldown` is the operational fallback when
    /// throttling is reported without a usable Retry-After.
    ///
    /// All applicable budgets are cooled down when the
    /// exchange does not identify the exceeded quota.
    /// This is conservative and may delay unrelated requests
    /// sharing those budgets.
    ///
    /// Usage observations are accepted only through explicit
    /// header-to-budget mappings.
    pub fn normalize(
        feedback: &RestFeedback,
        operation: &RateLimitedOperation,
        fallback_cooldown: Duration,
        usage_bindings: &[RestUsageBinding],
    ) -> Result<RateLimitFeedback, RestFeedbackMappingError> {
        if operation.charges.is_empty() {
            return Err(RestFeedbackMappingError::EmptyOperation);
        }

        let mut normalized = RateLimitFeedback::new();

        // ====================================================
        // COOLDOWN
        // ====================================================

        if feedback.requires_cooldown() {
            let duration = feedback
                .retry_after
                .filter(|duration| !duration.is_zero())
                .unwrap_or(fallback_cooldown);

            let kind = if feedback.banned {
                ThrottleKind::TemporaryBan
            } else {
                ThrottleKind::RateLimited
            };

            // Every charged budget is a possible cause of
            // throttling unless the exchange explicitly
            // identifies the exceeded quota.
            //
            // RateLimitedOperation already enforces unique
            // budget identities.
            let budgets = operation
                .charges
                .iter()
                .map(|charge| charge.budget.clone())
                .collect();

            let cooldown = CooldownFeedback::new(budgets, kind, duration)?;

            normalized = normalized.with_cooldown(cooldown);
        }

        // ====================================================
        // EXPLICIT USAGE MAPPINGS
        // ====================================================

        let charged_budgets: HashSet<&BudgetId> = operation
            .charges
            .iter()
            .map(|charge| &charge.budget)
            .collect();

        let mut bindings = HashMap::new();
        let mut mapped_budgets = HashSet::new();

        for binding in usage_bindings {
            let header = binding.header.to_ascii_lowercase();

            if header.trim().is_empty() {
                return Err(RestFeedbackMappingError::EmptyUsageHeader);
            }

            if binding.window.is_zero() {
                return Err(RestFeedbackMappingError::InvalidUsageWindow);
            }

            if !charged_budgets.contains(&binding.budget) {
                return Err(RestFeedbackMappingError::UnrelatedUsageBudget);
            }

            if !mapped_budgets.insert(&binding.budget) {
                return Err(RestFeedbackMappingError::DuplicateUsageBudget);
            }

            if bindings.insert(header.clone(), binding).is_some() {
                return Err(RestFeedbackMappingError::DuplicateUsageHeader(header));
            }
        }

        // ====================================================
        // PASSIVE USAGE OBSERVATIONS
        // ====================================================

        for observation in &feedback.usage {
            let Some(binding) = bindings.get(&observation.header.to_ascii_lowercase()) else {
                // Unknown header semantics.
                //
                // Do not guess which budget it represents.
                continue;
            };

            let usage =
                UsageFeedback::new(binding.budget.clone(), observation.used, binding.window)?;

            normalized = normalized.with_usage(usage);
        }

        Ok(normalized)
    }
}
// ============================================================
// TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::json;

    fn config() -> RestRateLimiting {
        toml::from_str(
            r#"
strategy = "weighted_shared_pool"
http_throttled_status = 429
http_banned_status = 418
honor_retry_after = true
usage_header_prefix = "X-MBX-USED-WEIGHT"
rate_limit_error_code = -1003
"#,
        )
        .unwrap()
    }

    fn headers(values: &[(&str, &str)]) -> RestResponseHeaders {
        values
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect()
    }

    // ========================================================
    // HTTP THROTTLING
    // ========================================================

    #[test]
    fn detects_http_429() {
        let feedback =
            RestFeedbackInterpreter::inspect(None, 429, &RestResponseHeaders::new(), None);

        assert!(feedback.throttled);
        assert!(!feedback.banned);
    }

    #[test]
    fn detects_configured_ban_status() {
        let config = config();

        let feedback =
            RestFeedbackInterpreter::inspect(Some(&config), 418, &RestResponseHeaders::new(), None);

        assert!(feedback.banned);
        assert!(feedback.requires_cooldown());
    }

    #[test]
    fn does_not_treat_418_as_universal_ban() {
        let feedback =
            RestFeedbackInterpreter::inspect(None, 418, &RestResponseHeaders::new(), None);

        assert!(!feedback.banned);
        assert!(!feedback.throttled);
    }

    // ========================================================
    // APPLICATION ERROR CODES
    // ========================================================

    #[test]
    fn detects_throttling_inside_http_200() {
        let config = config();

        let body = json!({
            "code": -1003,
            "msg": "Too many requests"
        });

        let feedback = RestFeedbackInterpreter::inspect(
            Some(&config),
            200,
            &RestResponseHeaders::new(),
            Some(&body),
        );

        assert!(feedback.throttled);

        assert_eq!(feedback.exchange_code.as_deref(), Some("-1003"),);
    }

    #[test]
    fn does_not_treat_arbitrary_error_as_throttling() {
        let config = config();

        let body = json!({
            "code": -1121,
            "msg": "Invalid symbol"
        });

        let feedback = RestFeedbackInterpreter::inspect(
            Some(&config),
            200,
            &RestResponseHeaders::new(),
            Some(&body),
        );

        assert!(!feedback.throttled);
    }

    #[test]
    fn matches_string_error_codes() {
        let expected = RateLimitErrorCode::String("42900".into());

        assert!(RestFeedbackInterpreter::matches_error_code(
            &expected,
            Some("42900"),
        ));

        assert!(!RestFeedbackInterpreter::matches_error_code(
            &expected,
            Some("42901"),
        ));
    }

    // ========================================================
    // RETRY-AFTER
    // ========================================================

    #[test]
    fn extracts_retry_after_seconds() {
        let feedback =
            RestFeedbackInterpreter::inspect(None, 429, &headers(&[("retry-after", "12")]), None);

        assert_eq!(feedback.retry_after, Some(Duration::from_secs(12)),);
    }

    #[test]
    fn supports_case_insensitive_header_lookup() {
        let feedback =
            RestFeedbackInterpreter::inspect(None, 429, &headers(&[("Retry-After", "5")]), None);

        assert_eq!(feedback.retry_after, Some(Duration::from_secs(5)),);
    }

    #[test]
    fn rejects_invalid_retry_after() {
        let feedback = RestFeedbackInterpreter::inspect(
            None,
            429,
            &headers(&[("retry-after", "invalid")]),
            None,
        );

        assert!(feedback.throttled);
        assert_eq!(feedback.retry_after, None);
    }

    #[test]
    fn ignores_retry_after_without_throttling() {
        let feedback =
            RestFeedbackInterpreter::inspect(None, 200, &headers(&[("retry-after", "10")]), None);

        assert_eq!(feedback.retry_after, None);
    }

    // ========================================================
    // EXCHANGE USAGE
    // ========================================================

    #[test]
    fn extracts_matching_usage_headers() {
        let config = config();

        let feedback = RestFeedbackInterpreter::inspect(
            Some(&config),
            200,
            &headers(&[
                ("x-mbx-used-weight-1m", "37"),
                ("x-mbx-used-weight-10s", "12"),
                ("content-type", "application/json"),
            ]),
            None,
        );

        assert_eq!(feedback.usage.len(), 2);

        assert!(feedback.usage.contains(&RestUsageObservation {
            header: "x-mbx-used-weight-1m".into(),
            used: 37,
        }));

        assert!(feedback.usage.contains(&RestUsageObservation {
            header: "x-mbx-used-weight-10s".into(),
            used: 12,
        }));
    }

    #[test]
    fn ignores_malformed_usage_values() {
        let config = config();

        let feedback = RestFeedbackInterpreter::inspect(
            Some(&config),
            200,
            &headers(&[("x-mbx-used-weight-1m", "unknown")]),
            None,
        );

        assert!(feedback.usage.is_empty());
    }

    // ========================================================
    // EMPTY FEEDBACK
    // ========================================================

    #[test]
    fn clean_response_produces_no_throttling() {
        let feedback = RestFeedbackInterpreter::inspect(
            None,
            200,
            &RestResponseHeaders::new(),
            Some(&json!({"data": []})),
        );

        assert!(!feedback.throttled);
        assert!(!feedback.banned);
        assert!(feedback.retry_after.is_none());
        assert!(feedback.usage.is_empty());
    }
    // ============================================================
    // LIMITER INTEGRATION ADAPTER TESTS
    // ============================================================

    #[test]
    fn adapter_preserves_throttling_cooldown() {
        use crate::live::{
            config::exchange::ExchangeId,
            infrastructure::limiter::{BudgetCharge, BudgetScope, RateLimitedOperation},
        };

        let budget = BudgetId::new(
            ExchangeId::Binance,
            "spot_rest",
            "request_weight",
            BudgetScope::Egress("primary".into()),
        )
        .unwrap();

        let operation =
            RateLimitedOperation::new(vec![BudgetCharge::new(budget.clone(), 10).unwrap()])
                .unwrap();

        let feedback = RestFeedback {
            throttled: true,
            retry_after: Some(Duration::from_secs(12)),
            ..Default::default()
        };

        let normalized =
            RestFeedbackAdapter::normalize(&feedback, &operation, Duration::from_secs(5), &[])
                .unwrap();

        let cooldown = normalized.cooldown.unwrap();

        assert_eq!(cooldown.budgets, vec![budget]);
        assert_eq!(cooldown.duration, Duration::from_secs(12));
        assert_eq!(cooldown.kind, ThrottleKind::RateLimited);
    }

    #[test]
    fn adapter_uses_explicit_fallback_cooldown() {
        use crate::live::{
            config::exchange::ExchangeId,
            infrastructure::limiter::{BudgetCharge, BudgetScope, RateLimitedOperation},
        };

        let budget = BudgetId::new(
            ExchangeId::Bitget,
            "uta_v3_rest",
            "rest_global_requests",
            BudgetScope::Egress("primary".into()),
        )
        .unwrap();

        let operation =
            RateLimitedOperation::new(vec![BudgetCharge::new(budget, 1).unwrap()]).unwrap();

        let feedback = RestFeedback {
            throttled: true,
            retry_after: None,
            ..Default::default()
        };

        let normalized =
            RestFeedbackAdapter::normalize(&feedback, &operation, Duration::from_secs(5), &[])
                .unwrap();

        assert_eq!(
            normalized.cooldown.unwrap().duration,
            Duration::from_secs(5),
        );
    }

    #[test]
    fn adapter_does_not_guess_usage_budget() {
        use crate::live::{
            config::exchange::ExchangeId,
            infrastructure::limiter::{BudgetCharge, BudgetScope, RateLimitedOperation},
        };

        let budget = BudgetId::new(
            ExchangeId::Binance,
            "spot_rest",
            "request_weight",
            BudgetScope::Egress("primary".into()),
        )
        .unwrap();

        let operation =
            RateLimitedOperation::new(vec![BudgetCharge::new(budget, 5).unwrap()]).unwrap();

        let feedback = RestFeedback {
            usage: vec![RestUsageObservation {
                header: "x-mbx-used-weight-1m".into(),
                used: 85,
            }],
            ..Default::default()
        };

        let normalized =
            RestFeedbackAdapter::normalize(&feedback, &operation, Duration::from_secs(5), &[])
                .unwrap();

        // No verified mapping was supplied.
        // The observation must not be attributed arbitrarily.
        assert!(normalized.usage.is_empty());
    }

    #[test]
    fn adapter_maps_explicit_usage_to_correct_budget() {
        use crate::live::{
            config::exchange::ExchangeId,
            infrastructure::limiter::{BudgetCharge, BudgetScope, RateLimitedOperation},
        };

        let budget = BudgetId::new(
            ExchangeId::Binance,
            "spot_rest",
            "request_weight",
            BudgetScope::Egress("primary".into()),
        )
        .unwrap();

        let operation =
            RateLimitedOperation::new(vec![BudgetCharge::new(budget.clone(), 5).unwrap()]).unwrap();

        let feedback = RestFeedback {
            usage: vec![RestUsageObservation {
                header: "x-mbx-used-weight-1m".into(),
                used: 850,
            }],
            ..Default::default()
        };

        let bindings = [RestUsageBinding {
            header: "x-mbx-used-weight-1m".into(),
            budget: budget.clone(),
            window: Duration::from_secs(60),
        }];

        let normalized = RestFeedbackAdapter::normalize(
            &feedback,
            &operation,
            Duration::from_secs(5),
            &bindings,
        )
        .unwrap();

        assert_eq!(normalized.usage.len(), 1);

        assert_eq!(normalized.usage[0].budget, budget);
        assert_eq!(normalized.usage[0].used, 850);

        assert_eq!(normalized.usage[0].window, Duration::from_secs(60),);
    }
}
