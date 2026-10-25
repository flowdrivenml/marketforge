use std::time::Duration;

use super::policy::BudgetId;

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RateLimitFeedbackError {
    #[error("rate-limit feedback requires at least one budget")]
    EmptyBudgets,

    #[error("rate-limit feedback contains duplicate budgets")]
    DuplicateBudgets,

    #[error("rate-limit cooldown must be greater than zero")]
    InvalidCooldown,

    #[error("rate-limit usage window must be greater than zero")]
    InvalidUsageWindow,

    #[error("rate-limit Retry-After value is invalid")]
    InvalidRetryAfter,
}

// ============================================================
// THROTTLING CLASSIFICATION
// ============================================================

/// Describes the exchange's response to excessive traffic.
///
/// The interpretation of HTTP statuses and exchange-native
/// error codes belongs to the REST or WebSocket adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThrottleKind {
    /// Exchange rejected a request because of rate limiting.
    RateLimited,

    /// Exchange explicitly imposed a temporary restriction.
    TemporaryBan,
}

// ============================================================
// COOLDOWN FEEDBACK
// ============================================================

/// Exchange-directed suspension of one or more budgets.
///
/// The affected budgets must be resolved by the transport's
/// exchange-specific response interpreter.
///
/// The manager must not infer them from the HTTP status alone.
#[derive(Debug, Clone)]
pub struct CooldownFeedback {
    pub budgets: Vec<BudgetId>,

    pub kind: ThrottleKind,

    /// Required suspension duration.
    ///
    /// This is a relative duration, not a wall-clock deadline.
    pub duration: Duration,
}

impl CooldownFeedback {
    pub fn new(
        budgets: Vec<BudgetId>,
        kind: ThrottleKind,
        duration: Duration,
    ) -> Result<Self, RateLimitFeedbackError> {
        validate_budgets(&budgets)?;

        if duration.is_zero() {
            return Err(RateLimitFeedbackError::InvalidCooldown);
        }

        Ok(Self {
            budgets,
            kind,
            duration,
        })
    }
}

// ============================================================
// USAGE OBSERVATION
// ============================================================

/// Usage reported by an exchange for one known quota.
///
/// This represents an observation of external usage, not
/// a new locally admitted request.
///
/// The manager must reconcile it conservatively with
/// locally recorded usage rather than simply adding it.
#[derive(Debug, Clone)]
pub struct UsageFeedback {
    pub budget: BudgetId,

    /// Reported usage within the exchange's accounting window.
    pub used: u64,

    /// Exchange accounting window represented by this value.
    pub window: Duration,
}

impl UsageFeedback {
    pub fn new(
        budget: BudgetId,
        used: u64,
        window: Duration,
    ) -> Result<Self, RateLimitFeedbackError> {
        if window.is_zero() {
            return Err(RateLimitFeedbackError::InvalidUsageWindow);
        }

        Ok(Self {
            budget,
            used,
            window,
        })
    }
}

// ============================================================
// NORMALIZED FEEDBACK
// ============================================================

/// Generic rate-limit feedback produced by exchange adapters.
///
/// A response may report usage without throttling, or impose
/// a cooldown without providing usage counters.
#[derive(Debug, Clone)]
pub struct RateLimitFeedback {
    pub cooldown: Option<CooldownFeedback>,

    pub usage: Vec<UsageFeedback>,
}

impl RateLimitFeedback {
    pub fn new() -> Self {
        Self {
            cooldown: None,
            usage: Vec::new(),
        }
    }

    pub fn with_cooldown(mut self, cooldown: CooldownFeedback) -> Self {
        self.cooldown = Some(cooldown);
        self
    }

    pub fn with_usage(mut self, usage: UsageFeedback) -> Self {
        self.usage.push(usage);
        self
    }

    pub fn is_empty(&self) -> bool {
        self.cooldown.is_none() && self.usage.is_empty()
    }
}

impl Default for RateLimitFeedback {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================
// RETRY-AFTER
// ============================================================

/// Parse a numeric Retry-After header expressed in seconds.
///
/// HTTP also permits an absolute HTTP-date representation.
/// That representation must be handled separately by the
/// transport using its response-receipt time.
///
/// This function deliberately rejects unsupported formats
/// instead of guessing their meaning.
pub fn parse_retry_after_seconds(value: &str) -> Result<Duration, RateLimitFeedbackError> {
    let seconds = value
        .trim()
        .parse::<u64>()
        .map_err(|_| RateLimitFeedbackError::InvalidRetryAfter)?;

    if seconds == 0 {
        return Err(RateLimitFeedbackError::InvalidCooldown);
    }

    Ok(Duration::from_secs(seconds))
}

// ============================================================
// VALIDATION
// ============================================================

fn validate_budgets(budgets: &[BudgetId]) -> Result<(), RateLimitFeedbackError> {
    use std::collections::HashSet;

    if budgets.is_empty() {
        return Err(RateLimitFeedbackError::EmptyBudgets);
    }

    let mut unique = HashSet::new();

    for budget in budgets {
        if !unique.insert(budget) {
            return Err(RateLimitFeedbackError::DuplicateBudgets);
        }
    }

    Ok(())
}

// ============================================================
// TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    use crate::live::config::exchange::ExchangeId;

    use super::super::policy::BudgetScope;

    fn budget(rule: &str) -> BudgetId {
        BudgetId::new(
            ExchangeId::Binance,
            "coinm_rest",
            rule,
            BudgetScope::Egress("primary".into()),
        )
        .unwrap()
    }

    #[test]
    fn accepts_valid_cooldown() {
        let feedback = CooldownFeedback::new(
            vec![budget("request_weight")],
            ThrottleKind::RateLimited,
            Duration::from_secs(30),
        )
        .unwrap();

        assert_eq!(feedback.duration, Duration::from_secs(30));
        assert_eq!(feedback.kind, ThrottleKind::RateLimited);
    }

    #[test]
    fn rejects_duplicate_cooldown_budgets() {
        let id = budget("request_weight");

        let result = CooldownFeedback::new(
            vec![id.clone(), id],
            ThrottleKind::RateLimited,
            Duration::from_secs(10),
        );

        assert!(matches!(
            result,
            Err(RateLimitFeedbackError::DuplicateBudgets)
        ));
    }

    #[test]
    fn accepts_external_usage_observation() {
        let observation =
            UsageFeedback::new(budget("request_weight"), 850, Duration::from_secs(60)).unwrap();

        assert_eq!(observation.used, 850);
        assert_eq!(observation.window, Duration::from_secs(60));
    }

    #[test]
    fn parses_numeric_retry_after() {
        assert_eq!(
            parse_retry_after_seconds(" 15 ").unwrap(),
            Duration::from_secs(15)
        );

        assert!(parse_retry_after_seconds("invalid").is_err());
        assert!(parse_retry_after_seconds("0").is_err());
    }

    #[test]
    fn supports_usage_and_cooldown_together() {
        let cooldown = CooldownFeedback::new(
            vec![budget("request_weight")],
            ThrottleKind::RateLimited,
            Duration::from_secs(5),
        )
        .unwrap();

        let usage =
            UsageFeedback::new(budget("request_weight"), 100, Duration::from_secs(60)).unwrap();

        let feedback = RateLimitFeedback::new()
            .with_cooldown(cooldown)
            .with_usage(usage);

        assert!(!feedback.is_empty());
        assert_eq!(feedback.usage.len(), 1);
        assert!(feedback.cooldown.is_some());
    }
}
