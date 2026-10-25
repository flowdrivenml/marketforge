use std::{
    collections::{BTreeMap, HashSet},
    time::Duration,
};

use thiserror::Error;

use crate::live::{
    config::exchange::{ExchangeConfig, ExchangeId, RateLimitPolicy, RestEndpoint},
    infrastructure::limiter::{
        BudgetCharge, BudgetId, BudgetScope, RateLimitPolicyError, RateLimitPolicySet,
        RateLimitedOperation, RequestPriority, WindowQuota,
    },
};

use super::weight::{CompiledWeightPolicy, WeightError, WeightParameters};

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, Error)]
pub enum RestLimiterCompileError {
    #[error(transparent)]
    Policy(#[from] RateLimitPolicyError),

    #[error(transparent)]
    Weight(#[from] WeightError),

    #[error("missing budget binding for '{0}'")]
    MissingBinding(String),

    #[error("incomplete numeric quota for '{0}'")]
    IncompleteQuota(String),

    #[error("missing numeric quota for '{0}'")]
    MissingQuota(String),

    #[error("weighted REST strategy requires a resolved weight budget")]
    MissingWeightBudget,

    #[error("endpoint '{0}' requires a weight policy")]
    MissingWeightPolicy(String),

    #[error("endpoint '{0}' has no enforceable rate-limit budget")]
    UnprotectedEndpoint(String),

    #[error("unknown REST endpoint '{0}'")]
    UnknownEndpoint(String),

    #[error("configured rate-limit policy '{0}' is not found")]
    UnknownRateLimitPolicy(String),

    #[error("configured weight policy '{0}' is not found")]
    UnknownWeightPolicy(String),

    #[error("rate-limit policy '{0}' is marked unresolved: {1}")]
    UnresolvedPolicy(String, String),

    #[error("budget belongs to a different exchange")]
    ExchangeMismatch,

    #[error("duplicate budget charge for {0:?}")]
    DuplicateBudget(BudgetId),

    #[error("endpoint '{0}' has an invalid HTTP method or path")]
    InvalidEndpointIdentity(String),

    #[error(
        "endpoint '{endpoint}' has a weight policy \
         but no weighted quota"
    )]
    UnenforcedWeightPolicy { endpoint: String },
}

#[derive(Debug, Clone)]
pub struct CompiledRestEndpoint {
    count_budgets: Vec<BudgetId>,
    count_cost: u64,
    weight_budget: Option<BudgetId>,
    weight_policy: Option<CompiledWeightPolicy>,
}

// ============================================================
// REST QUOTA GROUPING
// ============================================================

/// Determines which REST operations share one quota.
///
/// This is independent of MarketForge's market category.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RestQuotaGrouping {
    /// All endpoints referencing the named policy share
    /// one budget within the resolved service and scope.
    #[default]
    SharedPolicy,

    /// Each distinct exchange-native HTTP endpoint gets
    /// one budget.
    ///
    /// The same endpoint across multiple market profiles
    /// shares its budget when service and scope match.
    PerEndpoint,
}

// ============================================================
// BUDGET BINDING
// ============================================================

/// Explicit runtime identity for a configured quota.
///
/// Service and scope describe the exchange's actual
/// rate-limit accounting group.
///
/// Grouping controls whether a named policy is shared
/// or instantiated separately for each REST endpoint.
#[derive(Debug, Clone)]
pub struct RestBudgetBinding {
    pub service: String,
    pub scope: BudgetScope,
    pub grouping: RestQuotaGrouping,
}

impl RestBudgetBinding {
    pub fn new(service: impl Into<String>, scope: BudgetScope) -> Self {
        Self {
            service: service.into(),
            scope,
            grouping: RestQuotaGrouping::SharedPolicy,
        }
    }

    pub fn with_grouping(mut self, grouping: RestQuotaGrouping) -> Self {
        self.grouping = grouping;
        self
    }

    fn budget_id(
        &self,
        exchange: ExchangeId,
        rule: impl Into<String>,
    ) -> Result<BudgetId, RestLimiterCompileError> {
        Ok(BudgetId::new(
            exchange,
            self.service.clone(),
            rule,
            self.scope.clone(),
        )?)
    }

    /// Resolve the identity of a named REST rate-limit
    /// policy as applied to one endpoint.
    fn policy_budget_id(
        &self,
        exchange: ExchangeId,
        policy_name: &str,
        endpoint_name: &str,
        endpoint: &RestEndpoint,
    ) -> Result<BudgetId, RestLimiterCompileError> {
        let rule = match self.grouping {
            RestQuotaGrouping::SharedPolicy => {
                format!("rest_policy:{policy_name}")
            }

            RestQuotaGrouping::PerEndpoint => {
                let identity = endpoint_identity(endpoint_name, endpoint)?;

                format!("rest_endpoint:{identity}")
            }
        };

        self.budget_id(exchange, rule)
    }
}

// ============================================================
// VERIFIED EXTERNAL QUOTA
// ============================================================

/// Numeric quota supplied by verified policy configuration
/// or exchange-specific quota discovery.
#[derive(Debug, Clone)]
pub struct ResolvedRestBudget {
    pub id: BudgetId,
    pub quota: WindowQuota,
}

// ============================================================
// COMPILATION BINDINGS
// ============================================================

#[derive(Debug, Clone, Default)]
pub struct RestLimiterBindings {
    /// Scope of the configured IP request quota.
    pub ip: Option<RestBudgetBinding>,

    /// Scope of the configured global request quota.
    pub global: Option<RestBudgetBinding>,

    /// Named endpoint policy bindings.
    ///
    /// Each binding selects SharedPolicy or PerEndpoint.
    pub endpoint_policies: BTreeMap<String, RestBudgetBinding>,

    /// Verified weighted quota, when applicable.
    pub shared_weight: Option<ResolvedRestBudget>,
}

// ============================================================
// ENDPOINT IDENTITY
// ============================================================

/// Produce a stable exchange-native endpoint identity.
///
/// Examples:
///
/// GET:/api/v3/market/orderbook
/// GET:/api/v3/market/fills
/// GET:/api/v5/market/books
///
/// Query parameters are intentionally excluded.
///
/// The configured path must be a canonical endpoint
/// template, not a symbol-specific resolved URL.
///
/// Path placeholders remain intact so instruments using
/// the same endpoint template can share a quota.
fn endpoint_identity(
    name: &str,
    endpoint: &RestEndpoint,
) -> Result<String, RestLimiterCompileError> {
    let method = endpoint.method.trim().to_ascii_uppercase();
    let path = endpoint.path.trim();

    if method.is_empty()
        || !method
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte == b'-')
        || !path.starts_with('/')
        || path.starts_with("//")
        || path.contains('?')
        || path.contains('#')
        || path.chars().any(char::is_whitespace)
    {
        return Err(RestLimiterCompileError::InvalidEndpointIdentity(
            name.to_owned(),
        ));
    }

    Ok(format!("{method}:{path}"))
}

// ============================================================
// COMPILED ENDPOINT
// ============================================================

impl CompiledRestEndpoint {
    /// Construct the complete rate-limited operation.
    ///
    /// Parameters must contain effective request values,
    /// including any endpoint defaults.
    pub fn operation(
        &self,
        parameters: &WeightParameters,
        priority: RequestPriority,
    ) -> Result<RateLimitedOperation, RestLimiterCompileError> {
        let mut charges = Vec::new();
        let mut seen = HashSet::new();

        for id in &self.count_budgets {
            if !seen.insert(id.clone()) {
                return Err(RestLimiterCompileError::DuplicateBudget(id.clone()));
            }

            charges.push(BudgetCharge::new(id.clone(), self.count_cost)?);
        }

        if let Some(id) = &self.weight_budget {
            if !seen.insert(id.clone()) {
                return Err(RestLimiterCompileError::DuplicateBudget(id.clone()));
            }

            let policy = self
                .weight_policy
                .as_ref()
                .expect("weighted endpoint has a compiled weight policy");

            let cost = policy.calculate(parameters)?;

            charges.push(BudgetCharge::new(id.clone(), cost)?);
        }

        Ok(RateLimitedOperation::new(charges)?.with_priority(priority))
    }
}

// ============================================================
// COMPILED REST PROFILE
// ============================================================

#[derive(Debug, Clone)]
pub struct CompiledRestLimiters {
    pub policies: RateLimitPolicySet,

    endpoints: BTreeMap<String, CompiledRestEndpoint>,
}

impl CompiledRestLimiters {
    pub fn endpoint(&self, name: &str) -> Option<&CompiledRestEndpoint> {
        self.endpoints.get(name)
    }

    pub fn endpoints(&self) -> impl Iterator<Item = (&str, &CompiledRestEndpoint)> {
        self.endpoints
            .iter()
            .map(|(name, endpoint)| (name.as_str(), endpoint))
    }

    pub fn operation(
        &self,
        endpoint_name: &str,
        parameters: &WeightParameters,
        priority: RequestPriority,
    ) -> Result<RateLimitedOperation, RestLimiterCompileError> {
        let endpoint = self
            .endpoint(endpoint_name)
            .ok_or_else(|| RestLimiterCompileError::UnknownEndpoint(endpoint_name.to_owned()))?;

        endpoint.operation(parameters, priority)
    }
}

// ============================================================
// REST LIMITER COMPILER
// ============================================================

pub struct RestLimiterCompiler;

impl RestLimiterCompiler {
    pub fn compile(
        exchange: &ExchangeConfig,
        bindings: &RestLimiterBindings,
    ) -> Result<CompiledRestLimiters, RestLimiterCompileError> {
        let rest = &exchange.rest;

        let mut policies = RateLimitPolicySet::new();
        let mut shared_count_budgets = Vec::new();

        let weighted = rest
            .rate_limiting
            .as_ref()
            .is_some_and(|config| config.strategy == "weighted_shared_pool");

        // ====================================================
        // SHARED IP AND GLOBAL REQUEST COUNT QUOTAS
        // ====================================================

        if let Some(rate_config) = &rest.rate_limiting {
            if let Some(quota) = compile_optional_quota(
                "rest.rate_limiting.ip",
                rate_config.ip_max_requests,
                rate_config.ip_window_seconds,
            )? {
                let binding = bindings.ip.as_ref().ok_or_else(|| {
                    RestLimiterCompileError::MissingBinding("rest.rate_limiting.ip".into())
                })?;

                let id = binding.budget_id(exchange.exchange.id, "rest_ip_requests")?;

                policies.insert(id.clone(), quota)?;
                shared_count_budgets.push(id);
            }

            if let Some(quota) = compile_optional_quota(
                "rest.rate_limiting.global",
                rate_config.global_max_requests,
                rate_config.global_window_seconds,
            )? {
                let binding = bindings.global.as_ref().ok_or_else(|| {
                    RestLimiterCompileError::MissingBinding("rest.rate_limiting.global".into())
                })?;

                let id = binding.budget_id(exchange.exchange.id, "rest_global_requests")?;

                policies.insert(id.clone(), quota)?;
                shared_count_budgets.push(id);
            }
        }

        // ====================================================
        // SHARED WEIGHTED QUOTA
        // ====================================================

        let weight_budget = if weighted {
            let resolved = bindings
                .shared_weight
                .as_ref()
                .ok_or(RestLimiterCompileError::MissingWeightBudget)?;

            if resolved.id.exchange != exchange.exchange.id {
                return Err(RestLimiterCompileError::ExchangeMismatch);
            }

            policies.insert(resolved.id.clone(), resolved.quota)?;

            Some(resolved.id.clone())
        } else {
            None
        };

        // ====================================================
        // ENDPOINTS
        // ====================================================

        let mut endpoints = BTreeMap::new();

        for (name, endpoint) in &rest.endpoints {
            let mut count_budgets = shared_count_budgets.clone();

            // ------------------------------------------------
            // NAMED ENDPOINT RATE-LIMIT POLICY
            // ------------------------------------------------

            if let Some(policy_name) = &endpoint.rate_limit_policy {
                let policy = rest.rate_limit_policies.get(policy_name).ok_or_else(|| {
                    RestLimiterCompileError::UnknownRateLimitPolicy(policy_name.clone())
                })?;

                let quota = compile_named_quota(policy_name, policy)?;

                let binding = bindings.endpoint_policies.get(policy_name).ok_or_else(|| {
                    RestLimiterCompileError::MissingBinding(format!(
                        "rest.rate_limit_policies.{policy_name}"
                    ))
                })?;

                // The grouping strategy determines whether
                // endpoints share a named policy budget
                // or receive endpoint-specific budgets.
                let id =
                    binding.policy_budget_id(exchange.exchange.id, policy_name, name, endpoint)?;

                policies.insert(id.clone(), quota)?;
                count_budgets.push(id);
            }

            // ------------------------------------------------
            // REQUEST WEIGHT POLICY
            // ------------------------------------------------

            let compiled_weight = match &endpoint.weight_policy {
                Some(policy_name) => {
                    let policy = rest.weight_policies.get(policy_name).ok_or_else(|| {
                        RestLimiterCompileError::UnknownWeightPolicy(policy_name.clone())
                    })?;

                    Some(CompiledWeightPolicy::compile(policy)?)
                }

                None => None,
            };
            // ====================================================
            // REQUEST COST SEMANTICS
            // ====================================================

            // A request_count policy charges the existing
            // request-count budgets.
            //
            // Fixed, discrete, and tiered policies represent
            // weighted request costs and require a weighted quota.

            let count_cost = match &compiled_weight {
                Some(CompiledWeightPolicy::RequestCount { cost }) => *cost,

                _ => 1,
            };

            let requires_weight_budget = matches!(
                compiled_weight,
                Some(
                    CompiledWeightPolicy::Fixed { .. }
                        | CompiledWeightPolicy::Discrete { .. }
                        | CompiledWeightPolicy::Tiered { .. }
                )
            );

            if weighted && !requires_weight_budget {
                return Err(RestLimiterCompileError::MissingWeightPolicy(name.clone()));
            }

            if requires_weight_budget && weight_budget.is_none() {
                return Err(RestLimiterCompileError::UnenforcedWeightPolicy {
                    endpoint: name.clone(),
                });
            }

            if count_budgets.is_empty() && weight_budget.is_none() {
                return Err(RestLimiterCompileError::UnprotectedEndpoint(name.clone()));
            }

            // ------------------------------------------------
            // DUPLICATE BUDGET DETECTION
            // ------------------------------------------------

            let mut seen = HashSet::new();

            for id in &count_budgets {
                if !seen.insert(id) {
                    return Err(RestLimiterCompileError::DuplicateBudget(id.clone()));
                }
            }

            if requires_weight_budget {
                if let Some(id) = &weight_budget {
                    if !seen.insert(id) {
                        return Err(RestLimiterCompileError::DuplicateBudget(id.clone()));
                    }
                }
            } // ------------------------------------------------
            // COMPILED ENDPOINT
            // ------------------------------------------------

            endpoints.insert(
                name.clone(),
                CompiledRestEndpoint {
                    count_budgets,
                    count_cost,
                    weight_budget: if requires_weight_budget {
                        weight_budget.clone()
                    } else {
                        None
                    },
                    weight_policy: compiled_weight,
                },
            );
        }

        Ok(CompiledRestLimiters {
            policies,
            endpoints,
        })
    }
}

// ============================================================
// NUMERIC QUOTA COMPILATION
// ============================================================

fn compile_optional_quota(
    name: &str,
    capacity: Option<u64>,
    seconds: Option<u64>,
) -> Result<Option<WindowQuota>, RestLimiterCompileError> {
    match (capacity, seconds) {
        (None, None) => Ok(None),

        (Some(capacity), Some(seconds)) => Ok(Some(WindowQuota::new(
            capacity,
            Duration::from_secs(seconds),
        )?)),

        _ => Err(RestLimiterCompileError::IncompleteQuota(name.to_owned())),
    }
}

fn compile_named_quota(
    name: &str,
    policy: &RateLimitPolicy,
) -> Result<WindowQuota, RestLimiterCompileError> {
    if let Some(status) = &policy.status {
        if status != "verified" {
            return Err(RestLimiterCompileError::UnresolvedPolicy(
                name.to_owned(),
                status.clone(),
            ));
        }
    }

    compile_optional_quota(name, policy.max_requests, policy.window_seconds)?
        .ok_or_else(|| RestLimiterCompileError::MissingQuota(name.to_owned()))
}

// ============================================================
// TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config(rest_toml: &str) -> ExchangeConfig {
        let content = format!(
            r#"
schema_version = 1

[exchange]
id = "bitget"
market = "spot"
profile = "bitget_spot"
api_generation = "uta_v3"

[websocket]
base_url = "wss://example.invalid"

[channels.trades]
enabled = true
format = "trades/bitget_spot"

{rest_toml}
"#
        );

        toml::from_str(&content).unwrap()
    }

    fn endpoint_binding() -> RestBudgetBinding {
        RestBudgetBinding::new("uta_v3_rest", BudgetScope::Egress("primary".into()))
            .with_grouping(RestQuotaGrouping::PerEndpoint)
    }

    #[test]
    fn compiles_shared_request_count_budget() {
        let config = test_config(
            r#"
[rest]
base_url = "https://example.invalid"

[rest.rate_limiting]
strategy = "endpoint_window"
global_max_requests = 6000
global_window_seconds = 60

[rest.endpoints.depth_snapshot]
method = "GET"
path = "/api/v3/market/orderbook"
"#,
        );

        let bindings = RestLimiterBindings {
            global: Some(RestBudgetBinding::new(
                "uta_v3_rest",
                BudgetScope::Egress("primary".into()),
            )),
            ..Default::default()
        };

        let compiled = RestLimiterCompiler::compile(&config, &bindings).unwrap();

        assert_eq!(compiled.policies.len(), 1);

        let operation = compiled
            .operation(
                "depth_snapshot",
                &WeightParameters::new(),
                RequestPriority::High,
            )
            .unwrap();

        assert_eq!(operation.charges.len(), 1);
        assert_eq!(operation.charges[0].cost, 1);
    }

    #[test]
    fn per_endpoint_creates_independent_budgets() {
        let config = test_config(
            r#"
[rest]
base_url = "https://example.invalid"

[rest.endpoints.depth_snapshot]
method = "GET"
path = "/api/v3/market/orderbook"
rate_limit_policy = "public"

[rest.endpoints.recent_trades]
method = "GET"
path = "/api/v3/market/fills"
rate_limit_policy = "public"

[rest.rate_limit_policies.public]
max_requests = 20
window_seconds = 1
"#,
        );

        let mut bindings = RestLimiterBindings::default();

        bindings
            .endpoint_policies
            .insert("public".into(), endpoint_binding());

        let compiled = RestLimiterCompiler::compile(&config, &bindings).unwrap();

        assert_eq!(compiled.policies.len(), 2);

        let depth = compiled
            .operation(
                "depth_snapshot",
                &WeightParameters::new(),
                RequestPriority::Normal,
            )
            .unwrap();

        let trades = compiled
            .operation(
                "recent_trades",
                &WeightParameters::new(),
                RequestPriority::Normal,
            )
            .unwrap();

        assert_ne!(depth.charges[0].budget, trades.charges[0].budget,);
    }

    #[test]
    fn shared_policy_uses_one_budget() {
        let config = test_config(
            r#"
[rest]
base_url = "https://example.invalid"

[rest.endpoints.depth_snapshot]
method = "GET"
path = "/api/v3/market/orderbook"
rate_limit_policy = "public"

[rest.endpoints.recent_trades]
method = "GET"
path = "/api/v3/market/fills"
rate_limit_policy = "public"

[rest.rate_limit_policies.public]
max_requests = 20
window_seconds = 1
"#,
        );

        let mut bindings = RestLimiterBindings::default();

        bindings.endpoint_policies.insert(
            "public".into(),
            RestBudgetBinding::new("uta_v3_rest", BudgetScope::Egress("primary".into())),
        );

        let compiled = RestLimiterCompiler::compile(&config, &bindings).unwrap();

        assert_eq!(compiled.policies.len(), 1);

        let depth = compiled
            .operation(
                "depth_snapshot",
                &WeightParameters::new(),
                RequestPriority::Normal,
            )
            .unwrap();

        let trades = compiled
            .operation(
                "recent_trades",
                &WeightParameters::new(),
                RequestPriority::Normal,
            )
            .unwrap();

        assert_eq!(depth.charges[0].budget, trades.charges[0].budget,);
    }

    #[test]
    fn endpoint_identity_ignores_market_category() {
        let config = test_config(
            r#"
[rest]
base_url = "https://example.invalid"

[rest.endpoints.depth_snapshot]
method = "GET"
path = "/api/v3/market/orderbook"
category = "SPOT"
rate_limit_policy = "snapshot"

[rest.rate_limit_policies.snapshot]
max_requests = 20
window_seconds = 1
"#,
        );

        let mut bindings = RestLimiterBindings::default();

        bindings
            .endpoint_policies
            .insert("snapshot".into(), endpoint_binding());

        let compiled = RestLimiterCompiler::compile(&config, &bindings).unwrap();

        let operation = compiled
            .operation(
                "depth_snapshot",
                &WeightParameters::new(),
                RequestPriority::Normal,
            )
            .unwrap();

        let id = &operation.charges[0].budget;

        assert_eq!(id.service, "uta_v3_rest",);

        assert_eq!(id.rule, "rest_endpoint:GET:/api/v3/market/orderbook",);

        assert_eq!(id.scope, BudgetScope::Egress("primary".into()),);
    }

    #[test]
    fn different_local_names_same_endpoint_share_identity() {
        let first = test_config(
            r#"
[rest]
base_url = "https://example.invalid"

[rest.endpoints.depth_snapshot]
method = "GET"
path = "/api/v3/market/orderbook"
rate_limit_policy = "snapshot"

[rest.rate_limit_policies.snapshot]
max_requests = 20
window_seconds = 1
"#,
        );

        let second = test_config(
            r#"
[rest]
base_url = "https://example.invalid"

[rest.endpoints.book]
method = "GET"
path = "/api/v3/market/orderbook"
rate_limit_policy = "book_limit"

[rest.rate_limit_policies.book_limit]
max_requests = 20
window_seconds = 1
"#,
        );

        let mut first_bindings = RestLimiterBindings::default();

        first_bindings
            .endpoint_policies
            .insert("snapshot".into(), endpoint_binding());

        let mut second_bindings = RestLimiterBindings::default();

        second_bindings
            .endpoint_policies
            .insert("book_limit".into(), endpoint_binding());

        let first = RestLimiterCompiler::compile(&first, &first_bindings).unwrap();

        let second = RestLimiterCompiler::compile(&second, &second_bindings).unwrap();

        let first_operation = first
            .operation(
                "depth_snapshot",
                &WeightParameters::new(),
                RequestPriority::Normal,
            )
            .unwrap();

        let second_operation = second
            .operation("book", &WeightParameters::new(), RequestPriority::Normal)
            .unwrap();

        assert_eq!(
            first_operation.charges[0].budget,
            second_operation.charges[0].budget,
        );
    }

    #[test]
    fn combines_global_and_endpoint_budgets() {
        let config = test_config(
            r#"
[rest]
base_url = "https://example.invalid"

[rest.rate_limiting]
strategy = "endpoint_window"
global_max_requests = 6000
global_window_seconds = 60

[rest.endpoints.depth_snapshot]
method = "GET"
path = "/api/v3/market/orderbook"
rate_limit_policy = "snapshot"

[rest.rate_limit_policies.snapshot]
max_requests = 20
window_seconds = 1
"#,
        );

        let mut bindings = RestLimiterBindings {
            global: Some(RestBudgetBinding::new(
                "uta_v3_rest",
                BudgetScope::Egress("primary".into()),
            )),
            ..Default::default()
        };

        bindings
            .endpoint_policies
            .insert("snapshot".into(), endpoint_binding());

        let compiled = RestLimiterCompiler::compile(&config, &bindings).unwrap();

        let operation = compiled
            .operation(
                "depth_snapshot",
                &WeightParameters::new(),
                RequestPriority::High,
            )
            .unwrap();

        assert_eq!(operation.charges.len(), 2);
        assert_eq!(compiled.policies.len(), 2);

        assert_ne!(operation.charges[0].budget, operation.charges[1].budget,);
    }

    #[test]
    fn rejects_missing_weighted_budget() {
        let config = test_config(
            r#"
[rest]
base_url = "https://example.invalid"

[rest.rate_limiting]
strategy = "weighted_shared_pool"

[rest.endpoints.depth_snapshot]
method = "GET"
path = "/api/v3/market/orderbook"
weight_policy = "snapshot"

[rest.weight_policies.snapshot]
type = "fixed"
weight = 10
"#,
        );

        assert!(matches!(
            RestLimiterCompiler::compile(&config, &RestLimiterBindings::default(),),
            Err(RestLimiterCompileError::MissingWeightBudget)
        ));
    }
    #[test]
    fn request_count_policy_uses_existing_request_budget() {
        let config = test_config(
            r#"
[rest]
base_url = "https://example.invalid"

[rest.rate_limiting]
strategy = "ip_window"
ip_max_requests = 100
ip_window_seconds = 60

[rest.endpoints.depth_snapshot]
method = "GET"
path = "/market/depth"
weight_policy = "public_market_data"

[rest.weight_policies.public_market_data]
type = "request_count"
cost = 2
"#,
        );

        let bindings = RestLimiterBindings {
            ip: Some(RestBudgetBinding::new(
                "public_rest",
                BudgetScope::Egress("primary".into()),
            )),
            ..Default::default()
        };

        let compiled = RestLimiterCompiler::compile(&config, &bindings).unwrap();

        let operation = compiled
            .operation(
                "depth_snapshot",
                &WeightParameters::new(),
                RequestPriority::Normal,
            )
            .unwrap();

        assert_eq!(operation.charges.len(), 1);
        assert_eq!(operation.charges[0].cost, 2);
    }
}
