use std::collections::BTreeMap;

use thiserror::Error;

use crate::live::infrastructure::limiter::{RateLimitManager, RateLimitManagerError};

use super::validation::{
    ExchangeLimiterValidator, LimiterProfileInput, LimiterValidationError, ValidatedLimiterProfile,
};

// ============================================================
// FACTORY ERRORS
// ============================================================

#[derive(Debug, Error)]
pub enum ExchangeLimiterFactoryError {
    #[error("rate-limit configuration validation failed")]
    Validation { errors: Vec<LimiterValidationError> },

    #[error(transparent)]
    Manager(#[from] RateLimitManagerError),
}

impl ExchangeLimiterFactoryError {
    /// Return configuration diagnostics when initialization
    /// failed because of invalid exchange policies.
    pub fn validation_errors(&self) -> Option<&[LimiterValidationError]> {
        match self {
            Self::Validation { errors } => Some(errors),
            Self::Manager(_) => None,
        }
    }
}

// ============================================================
// COMPILED EXCHANGE LIMITERS
// ============================================================

/// Complete rate-limiting infrastructure for one
/// MarketForge Live runtime.
///
/// Contains:
///
/// - One shared rate-limit manager.
/// - Compiled REST operation catalogs.
/// - Compiled WebSocket connection policy templates.
/// - Validated exchange-specific budget identities.
///
/// The factory performs no network operations.
///
/// Connection-scoped WebSocket budgets are instantiated
/// separately when physical connections are created.
#[derive(Debug)]
pub struct ExchangeLimiters {
    manager: RateLimitManager,

    profiles: BTreeMap<String, ValidatedLimiterProfile>,
}

// ============================================================
// ACCESSORS
// ============================================================

impl ExchangeLimiters {
    /// Shared manager used by REST and WebSocket transports.
    ///
    /// Cloning the handle preserves shared budget state.
    pub fn manager(&self) -> RateLimitManager {
        self.manager.clone()
    }

    /// Retrieve compiled rate-limit policies for one
    /// exchange/market profile.
    ///
    /// Example: "binance_spot".
    pub fn profile(&self, name: &str) -> Option<&ValidatedLimiterProfile> {
        self.profiles.get(name)
    }

    /// Iterate over registered exchange profiles.
    pub fn profiles(&self) -> impl Iterator<Item = (&str, &ValidatedLimiterProfile)> {
        self.profiles
            .iter()
            .map(|(name, profile)| (name.as_str(), profile))
    }

    /// Number of configured exchange profiles.
    pub fn profile_count(&self) -> usize {
        self.profiles.len()
    }

    /// Number of shared rate-limit budgets.
    ///
    /// Connection-scoped WebSocket budgets are not included
    /// until runtime registration is implemented.
    pub fn shared_budget_count(&self) -> usize {
        self.manager.budget_count()
    }
}

// ============================================================
// EXCHANGE LIMITER FACTORY
// ============================================================

/// Constructs rate-limiting infrastructure from validated
/// exchange configurations and resolved quota bindings.
///
/// The same generic implementation supports every exchange.
pub struct ExchangeLimiterFactory;

impl ExchangeLimiterFactory {
    /// Validate and compile all configured exchange profiles,
    /// merge shared budgets, and initialize the manager.
    ///
    /// Initialization is atomic from the caller's perspective:
    ///
    /// - All profiles succeed, or initialization fails.
    /// - No partially initialized manager is returned.
    /// - No exchange connections are opened.
    /// - No source streams are started.
    ///
    /// The maximum pending count limits the number of
    /// asynchronous requests waiting for rate-limit capacity.
    pub fn build(
        inputs: &[LimiterProfileInput<'_>],
        max_pending: usize,
    ) -> Result<ExchangeLimiters, ExchangeLimiterFactoryError> {
        // ====================================================
        // VALIDATE AND COMPILE
        // ====================================================

        let validated = ExchangeLimiterValidator::validate(inputs)
            .map_err(|errors| ExchangeLimiterFactoryError::Validation { errors })?;

        // ====================================================
        // INITIALIZE SHARED MANAGER
        // ====================================================

        let manager = RateLimitManager::new(validated.shared_policies, max_pending)?;

        // ====================================================
        // INDEX COMPILED PROFILES
        // ====================================================

        let profiles = validated
            .profiles
            .into_iter()
            .map(|profile| (profile.profile.clone(), profile))
            .collect();

        Ok(ExchangeLimiters { manager, profiles })
    }
}

// ============================================================
// TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    use crate::live::{config::exchange::ExchangeConfig, infrastructure::limiter::BudgetScope};

    use super::super::{
        rest::{RestBudgetBinding, RestLimiterBindings},
        websocket::WebSocketLimiterBindings,
    };

    // ========================================================
    // TEST CONFIGURATION
    // ========================================================

    fn exchange_config() -> ExchangeConfig {
        let content = r#"
schema_version = 1

[exchange]
id = "binance"
market = "spot"
profile = "binance_spot"

[websocket]
base_url = "wss://example.invalid"
max_client_messages_per_second = 10

[channels.trades]
enabled = true
format = "trades/binance_spot"

[rest]
base_url = "https://example.invalid"

[rest.rate_limiting]
strategy = "ip_window"
ip_max_requests = 100
ip_window_seconds = 60

[rest.endpoints.depth_snapshot]
method = "GET"
path = "/depth"
"#;

        toml::from_str(content).unwrap()
    }

    fn rest_bindings() -> RestLimiterBindings {
        RestLimiterBindings {
            ip: Some(RestBudgetBinding::new(
                "spot_rest",
                BudgetScope::Egress("primary".into()),
            )),
            ..Default::default()
        }
    }

    // ========================================================
    // TESTS
    // ========================================================

    #[test]
    fn builds_complete_infrastructure() {
        let exchange = exchange_config();

        let rest = rest_bindings();

        let websocket = WebSocketLimiterBindings::default();

        let inputs = [LimiterProfileInput::new(&exchange, &rest, &websocket)];

        let result = ExchangeLimiterFactory::build(&inputs, 100).unwrap();

        assert_eq!(result.profile_count(), 1);
        assert_eq!(result.shared_budget_count(), 1);

        assert!(result.profile("binance_spot").is_some());

        assert!(result.profile("binance_inverse").is_none());
    }

    #[test]
    fn manager_handles_share_budget_state() {
        let exchange = exchange_config();

        let rest = rest_bindings();

        let websocket = WebSocketLimiterBindings::default();

        let inputs = [LimiterProfileInput::new(&exchange, &rest, &websocket)];

        let result = ExchangeLimiterFactory::build(&inputs, 100).unwrap();

        let first = result.manager();
        let second = result.manager();

        assert_eq!(first.budget_count(), 1);
        assert_eq!(second.budget_count(), 1);
    }

    #[test]
    fn rejects_invalid_pending_limit() {
        let exchange = exchange_config();

        let rest = rest_bindings();

        let websocket = WebSocketLimiterBindings::default();

        let inputs = [LimiterProfileInput::new(&exchange, &rest, &websocket)];

        let result = ExchangeLimiterFactory::build(&inputs, 0);

        assert!(matches!(
            result,
            Err(ExchangeLimiterFactoryError::Manager(
                RateLimitManagerError::InvalidPendingLimit
            ))
        ));
    }

    #[test]
    fn rejects_missing_quota_bindings() {
        let exchange = exchange_config();

        let rest = RestLimiterBindings::default();

        let websocket = WebSocketLimiterBindings::default();

        let inputs = [LimiterProfileInput::new(&exchange, &rest, &websocket)];

        let result = ExchangeLimiterFactory::build(&inputs, 100);

        match result {
            Err(ExchangeLimiterFactoryError::Validation { errors }) => {
                assert!(!errors.is_empty());

                assert_eq!(errors[0].profile, "binance_spot");
            }

            other => {
                panic!("expected configuration validation error, got {other:?}");
            }
        }
    }
}
