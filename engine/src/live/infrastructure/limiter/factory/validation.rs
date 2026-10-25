use std::collections::BTreeSet;

use thiserror::Error;

use crate::live::{
    config::exchange::ExchangeConfig,
    infrastructure::limiter::{RateLimitPolicyError, RateLimitPolicySet},
};

use super::{
    rest::{
        CompiledRestLimiters, RestLimiterBindings, RestLimiterCompileError, RestLimiterCompiler,
    },
    websocket::{
        CompiledWebSocketLimiters, WebSocketLimiterBindings, WebSocketLimiterCompileError,
        WebSocketLimiterCompiler,
    },
};

// ============================================================
// VALIDATION ERRORS
// ============================================================

/// Identifies the stage at which a configuration failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LimiterValidationStage {
    Profile,
    Rest,
    WebSocket,
    SharedBudgets,
}

/// One configuration validation failure.
///
/// The profile name makes diagnostics actionable when
/// validating multiple exchange configurations.
#[derive(Debug, Error)]
#[error("[{profile}] {stage:?}: {message}")]
pub struct LimiterValidationError {
    pub profile: String,
    pub stage: LimiterValidationStage,
    pub message: String,
}

impl LimiterValidationError {
    fn new(
        profile: impl Into<String>,
        stage: LimiterValidationStage,
        message: impl Into<String>,
    ) -> Self {
        Self {
            profile: profile.into(),
            stage,
            message: message.into(),
        }
    }
}

// ============================================================
// PROFILE INPUT
// ============================================================

/// One exchange configuration and its resolved rate-limit
/// bindings.
///
/// Bindings identify the actual external quota scopes.
///
/// They are provided explicitly so that the compiler does
/// not infer IP, account, or API-service ownership from
/// the canonical market category.
#[derive(Debug)]
pub struct LimiterProfileInput<'a> {
    pub exchange: &'a ExchangeConfig,

    pub rest_bindings: &'a RestLimiterBindings,

    pub websocket_bindings: &'a WebSocketLimiterBindings,
}

impl<'a> LimiterProfileInput<'a> {
    pub fn new(
        exchange: &'a ExchangeConfig,
        rest_bindings: &'a RestLimiterBindings,
        websocket_bindings: &'a WebSocketLimiterBindings,
    ) -> Self {
        Self {
            exchange,
            rest_bindings,
            websocket_bindings,
        }
    }

    pub fn profile(&self) -> &str {
        self.exchange.profile()
    }
}

// ============================================================
// COMPILED PROFILE
// ============================================================

/// Rate-limit policies compiled for one exchange profile.
///
/// REST and WebSocket policies remain separate catalogs.
///
/// Their shared budget definitions are also merged into
/// the global RateLimitPolicySet.
#[derive(Debug, Clone)]
pub struct ValidatedLimiterProfile {
    pub profile: String,

    pub rest: CompiledRestLimiters,

    pub websocket: CompiledWebSocketLimiters,
}

// ============================================================
// VALIDATED CONFIGURATION
// ============================================================

/// Successfully compiled rate-limit configuration for
/// the complete MarketForge runtime.
///
/// `shared_policies` contains all startup-registered
/// REST and shared WebSocket budgets.
///
/// Connection-scoped WebSocket budgets are NOT included.
/// Their definitions remain in each WebSocket template.
#[derive(Debug, Clone)]
pub struct ValidatedLimiterConfiguration {
    pub shared_policies: RateLimitPolicySet,

    pub profiles: Vec<ValidatedLimiterProfile>,
}

impl ValidatedLimiterConfiguration {
    pub fn profile(&self, name: &str) -> Option<&ValidatedLimiterProfile> {
        self.profiles.iter().find(|profile| profile.profile == name)
    }

    pub fn profile_count(&self) -> usize {
        self.profiles.len()
    }

    pub fn shared_budget_count(&self) -> usize {
        self.shared_policies.len()
    }
}

// ============================================================
// VALIDATOR
// ============================================================

pub struct ExchangeLimiterValidator;

impl ExchangeLimiterValidator {
    /// Compile and validate a complete collection of
    /// exchange rate-limit configurations.
    ///
    /// This operation is atomic from the caller's
    /// perspective:
    ///
    /// - Success returns the complete compiled configuration.
    /// - Failure returns validation errors.
    /// - No runtime manager or mutable budget state is created.
    ///
    /// Each profile is compiled independently before its
    /// shared budgets are merged into the global policy set.
    pub fn validate(
        inputs: &[LimiterProfileInput<'_>],
    ) -> Result<ValidatedLimiterConfiguration, Vec<LimiterValidationError>> {
        let mut errors = Vec::new();

        let mut seen_profiles = BTreeSet::new();

        let mut shared_policies = RateLimitPolicySet::new();

        let mut compiled_profiles = Vec::new();

        // ====================================================
        // EMPTY INPUT
        // ====================================================

        if inputs.is_empty() {
            return Err(vec![LimiterValidationError::new(
                "<all>",
                LimiterValidationStage::Profile,
                "no exchange profiles supplied",
            )]);
        }

        // ====================================================
        // PROFILE COMPILATION
        // ====================================================

        for input in inputs {
            let profile = input.profile();

            // -----------------------------------------------
            // Profile identity
            // -----------------------------------------------

            if !seen_profiles.insert(profile.to_owned()) {
                errors.push(LimiterValidationError::new(
                    profile,
                    LimiterValidationStage::Profile,
                    "duplicate exchange profile",
                ));

                continue;
            }

            // -----------------------------------------------
            // REST compilation
            // -----------------------------------------------

            let rest = match RestLimiterCompiler::compile(input.exchange, input.rest_bindings) {
                Ok(compiled) => Some(compiled),

                Err(error) => {
                    errors.push(Self::rest_error(profile, error));

                    None
                }
            };

            // -----------------------------------------------
            // WebSocket compilation
            // -----------------------------------------------

            let websocket =
                match WebSocketLimiterCompiler::compile(input.exchange, input.websocket_bindings) {
                    Ok(compiled) => Some(compiled),

                    Err(error) => {
                        errors.push(Self::websocket_error(profile, error));

                        None
                    }
                };

            // Both compilers are attempted independently
            // so a profile can report REST and WebSocket
            // problems in the same validation run.
            let (Some(rest), Some(websocket)) = (rest, websocket) else {
                continue;
            };

            // -----------------------------------------------
            // Shared budget definitions
            // -----------------------------------------------

            let mut candidate_policies = shared_policies.clone();

            let rest_result = Self::merge_policies(&mut candidate_policies, &rest.policies);

            let websocket_result =
                Self::merge_policies(&mut candidate_policies, &websocket.shared.policies);

            if let Err(error) = rest_result {
                errors.push(LimiterValidationError::new(
                    profile,
                    LimiterValidationStage::SharedBudgets,
                    format!("REST budget conflict: {error}"),
                ));
            }

            if let Err(error) = websocket_result {
                errors.push(LimiterValidationError::new(
                    profile,
                    LimiterValidationStage::SharedBudgets,
                    format!("WebSocket budget conflict: {error}"),
                ));
            }

            // Do not commit a profile's partially merged
            // policies if either merge failed.
            if errors.iter().any(|error| {
                error.profile == profile && error.stage == LimiterValidationStage::SharedBudgets
            }) {
                continue;
            }

            // -----------------------------------------------
            // Commit successful profile
            // -----------------------------------------------

            shared_policies = candidate_policies;

            compiled_profiles.push(ValidatedLimiterProfile {
                profile: profile.to_owned(),
                rest,
                websocket,
            });
        }

        // ====================================================
        // FINAL RESULT
        // ====================================================

        if !errors.is_empty() {
            return Err(errors);
        }

        Ok(ValidatedLimiterConfiguration {
            shared_policies,
            profiles: compiled_profiles,
        })
    }

    // ========================================================
    // POLICY MERGING
    // ========================================================

    /// Merge registered budgets into a shared policy set.
    ///
    /// Identical definitions are accepted.
    ///
    /// Conflicting definitions for the same BudgetId
    /// are rejected.
    fn merge_policies(
        destination: &mut RateLimitPolicySet,
        source: &RateLimitPolicySet,
    ) -> Result<(), RateLimitPolicyError> {
        for (id, quota) in source.iter() {
            destination.insert(id.clone(), *quota)?;
        }

        Ok(())
    }

    // ========================================================
    // ERROR CONVERSION
    // ========================================================

    fn rest_error(profile: &str, error: RestLimiterCompileError) -> LimiterValidationError {
        LimiterValidationError::new(profile, LimiterValidationStage::Rest, error.to_string())
    }

    fn websocket_error(
        profile: &str,
        error: WebSocketLimiterCompileError,
    ) -> LimiterValidationError {
        LimiterValidationError::new(
            profile,
            LimiterValidationStage::WebSocket,
            error.to_string(),
        )
    }
}

// ============================================================
// TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    use crate::live::infrastructure::limiter::BudgetScope;

    use super::super::{rest::RestBudgetBinding, websocket::WebSocketBudgetBinding};

    // ========================================================
    // TEST CONFIGURATION
    // ========================================================

    fn exchange_config(market: &str, quota: u64) -> ExchangeConfig {
        let profile = format!("binance_{market}");

        let content = format!(
            r#"
schema_version = 1

[exchange]
id = "binance"
market = "{market}"
profile = "{profile}"

[websocket]
base_url = "wss://example.invalid"
max_client_messages_per_second = 10

[channels.trades]
enabled = true
format = "trades/{profile}"

[rest]
base_url = "https://example.invalid"

[rest.rate_limiting]
strategy = "ip_window"
ip_max_requests = {quota}
ip_window_seconds = 60

[rest.endpoints.depth_snapshot]
method = "GET"
path = "/depth"
"#
        );

        toml::from_str(&content).unwrap()
    }

    fn rest_bindings() -> RestLimiterBindings {
        RestLimiterBindings {
            ip: Some(RestBudgetBinding::new(
                "shared_rest",
                BudgetScope::Egress("primary".into()),
            )),
            ..Default::default()
        }
    }

    fn websocket_bindings() -> WebSocketLimiterBindings {
        WebSocketLimiterBindings {
            connection_service: Some("public_websocket".into()),

            connection_attempts: Some(WebSocketBudgetBinding::new(
                "public_websocket",
                BudgetScope::Egress("primary".into()),
            )),

            ..Default::default()
        }
    }

    // ========================================================
    // TESTS
    // ========================================================

    #[test]
    fn compiles_one_profile() {
        let config = exchange_config("spot", 100);

        let rest = rest_bindings();
        let websocket = websocket_bindings();

        let inputs = [LimiterProfileInput::new(&config, &rest, &websocket)];

        let result = ExchangeLimiterValidator::validate(&inputs).unwrap();

        assert_eq!(result.profile_count(), 1);
        assert_eq!(result.shared_budget_count(), 1);

        assert!(result.profile("binance_spot").is_some());
    }

    #[test]
    fn merges_identical_shared_budgets() {
        let spot = exchange_config("spot", 100);
        let linear = exchange_config("linear", 100);

        let rest = rest_bindings();
        let websocket = websocket_bindings();

        let inputs = [
            LimiterProfileInput::new(&spot, &rest, &websocket),
            LimiterProfileInput::new(&linear, &rest, &websocket),
        ];

        let result = ExchangeLimiterValidator::validate(&inputs).unwrap();

        assert_eq!(result.profile_count(), 2);

        // Both profiles reference the same external
        // quota, so only one shared budget is created.
        assert_eq!(result.shared_budget_count(), 1);
    }

    #[test]
    fn rejects_conflicting_shared_quotas() {
        let spot = exchange_config("spot", 100);
        let linear = exchange_config("linear", 200);

        let rest = rest_bindings();
        let websocket = websocket_bindings();

        let inputs = [
            LimiterProfileInput::new(&spot, &rest, &websocket),
            LimiterProfileInput::new(&linear, &rest, &websocket),
        ];

        let errors = ExchangeLimiterValidator::validate(&inputs).unwrap_err();

        assert!(
            errors
                .iter()
                .any(|error| { error.stage == LimiterValidationStage::SharedBudgets })
        );
    }

    #[test]
    fn rejects_duplicate_profiles() {
        let spot = exchange_config("spot", 100);

        let rest = rest_bindings();
        let websocket = websocket_bindings();

        let inputs = [
            LimiterProfileInput::new(&spot, &rest, &websocket),
            LimiterProfileInput::new(&spot, &rest, &websocket),
        ];

        let errors = ExchangeLimiterValidator::validate(&inputs).unwrap_err();

        assert!(
            errors
                .iter()
                .any(|error| { error.stage == LimiterValidationStage::Profile })
        );
    }

    #[test]
    fn reports_missing_rest_binding() {
        let config = exchange_config("spot", 100);

        let rest = RestLimiterBindings::default();
        let websocket = websocket_bindings();

        let inputs = [LimiterProfileInput::new(&config, &rest, &websocket)];

        let errors = ExchangeLimiterValidator::validate(&inputs).unwrap_err();

        assert!(errors.iter().any(|error| {
            error.stage == LimiterValidationStage::Rest && error.profile == "binance_spot"
        }));
    }

    #[test]
    fn rejects_empty_input() {
        let errors = ExchangeLimiterValidator::validate(&[]).unwrap_err();

        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].stage, LimiterValidationStage::Profile,);
    }
}
