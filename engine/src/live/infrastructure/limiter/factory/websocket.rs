use std::time::Duration;

use thiserror::Error;

use crate::live::{
    config::exchange::{ExchangeConfig, ExchangeId, WebSocketConfig, WebSocketLimits},
    infrastructure::limiter::{
        BudgetCharge, BudgetId, BudgetScope, RateLimitPolicyError, RateLimitPolicySet,
        RateLimitedOperation, RequestPriority, WindowQuota,
    },
};

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, Error)]
pub enum WebSocketLimiterCompileError {
    #[error(transparent)]
    Policy(#[from] RateLimitPolicyError),

    #[error("missing WebSocket rate-limit binding for '{0}'")]
    MissingBinding(String),

    #[error("incomplete WebSocket quota for '{0}'")]
    IncompleteQuota(String),

    #[error("conflicting WebSocket configuration for '{0}'")]
    ConflictingConfiguration(String),

    #[error("WebSocket limit '{0}' must be greater than zero")]
    InvalidLimit(String),

    #[error("WebSocket connection identifier must be greater than zero")]
    InvalidConnectionId,

    #[error("WebSocket operation '{0}' has no applicable budget")]
    UnprotectedOperation(String),
}

// ============================================================
// SHARED BUDGET BINDING
// ============================================================

/// Identifies the external resource against which an exchange
/// accounts a shared WebSocket quota.
///
/// Bindings must be determined from the documented exchange
/// API semantics, not guessed from the market category.
#[derive(Debug, Clone)]
pub struct WebSocketBudgetBinding {
    pub service: String,
    pub scope: BudgetScope,
}

impl WebSocketBudgetBinding {
    pub fn new(service: impl Into<String>, scope: BudgetScope) -> Self {
        Self {
            service: service.into(),
            scope,
        }
    }

    fn budget_id(
        &self,
        exchange: ExchangeId,
        rule: &str,
    ) -> Result<BudgetId, WebSocketLimiterCompileError> {
        Ok(BudgetId::new(
            exchange,
            self.service.clone(),
            rule,
            self.scope.clone(),
        )?)
    }
}

// ============================================================
// COMPILATION BINDINGS
// ============================================================

/// Explicit identities of shared WebSocket quota groups.
///
/// Different market profiles may reference the same binding
/// when the exchange counts their operations together.
#[derive(Debug, Clone, Default)]
pub struct WebSocketLimiterBindings {
    /// Shared quota for connection attempts over a window.
    pub connection_attempts: Option<WebSocketBudgetBinding>,

    /// Shared quota for connection attempts per second.
    pub connection_attempts_per_second: Option<WebSocketBudgetBinding>,

    /// API service identity for per-connection budgets.
    ///
    /// The scope of those budgets is always the physical
    /// connection identifier.
    pub connection_service: Option<String>,
}

// ============================================================
// CONNECTION CAPACITY CONSTRAINTS
// ============================================================

/// Non-time-window constraints declared by an exchange.
///
/// These are informational to the rate-limit factory.
///
/// The future connection manager must enforce connection
/// capacity; the subscription manager must enforce
/// subscription capacity.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WebSocketCapacityConstraints {
    pub max_connections_per_ip: Option<u64>,

    pub max_market_data_connections_per_ip: Option<u64>,

    pub max_subscriptions_per_connection: Option<u64>,

    pub recommended_max_subscriptions: Option<u64>,

    pub max_args_per_subscribe_request: Option<u64>,

    pub max_subscription_payload_bytes: Option<u64>,

    pub max_args_characters: Option<u64>,
}

// ============================================================
// SHARED WEBSOCKET POLICIES
// ============================================================

/// Budgets shared by multiple physical connections.
#[derive(Debug, Clone)]
pub struct CompiledWebSocketSharedLimits {
    pub policies: RateLimitPolicySet,

    connection_attempt_budgets: Vec<BudgetId>,
}

impl CompiledWebSocketSharedLimits {
    /// Admission operation required before opening a
    /// new physical WebSocket connection.
    ///
    /// When multiple quotas apply, all are charged.
    pub fn connection_attempt_operation(
        &self,
    ) -> Result<Option<RateLimitedOperation>, WebSocketLimiterCompileError> {
        if self.connection_attempt_budgets.is_empty() {
            return Ok(None);
        }

        let charges = self
            .connection_attempt_budgets
            .iter()
            .cloned()
            .map(|id| BudgetCharge::new(id, 1))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Some(
            RateLimitedOperation::new(charges)?.with_priority(RequestPriority::High),
        ))
    }
}

// ============================================================
// CONNECTION-SCOPED POLICY TEMPLATE
// ============================================================

/// Rate-limit definitions that apply independently to
/// each physical WebSocket connection.
///
/// This is a template, not registered runtime state.
///
/// The future WebSocket connection manager supplies a
/// unique physical connection ID when instantiating it.
#[derive(Debug, Clone)]
pub struct WebSocketConnectionTemplate {
    exchange: ExchangeId,
    service: String,

    outbound_messages: Option<WindowQuota>,

    subscription_operations: Option<WindowQuota>,

    subscription_requests: Option<WindowQuota>,
}

// ============================================================
// CONNECTION POLICY INSTANCE
// ============================================================

/// Concrete rate-limit definitions for one physical
/// WebSocket connection.
#[derive(Debug, Clone)]
pub struct CompiledWebSocketConnectionLimits {
    pub policies: RateLimitPolicySet,

    outbound_messages: Option<BudgetId>,

    subscription_operations: Option<BudgetId>,

    subscription_requests: Option<BudgetId>,
}

impl CompiledWebSocketConnectionLimits {
    /// Admission required before sending an outbound
    /// WebSocket message.
    ///
    /// Returns None when no outbound-message quota is
    /// configured. This is not permission to ignore
    /// other exchange protocol constraints.
    pub fn outbound_message_operation(
        &self,
        priority: RequestPriority,
    ) -> Result<Option<RateLimitedOperation>, WebSocketLimiterCompileError> {
        self.operation(self.outbound_messages.iter().cloned(), priority)
    }

    /// Admission required for a subscription operation.
    ///
    /// Subscription operations also consume outbound-message
    /// capacity when that quota is configured.
    pub fn subscription_operation(
        &self,
        priority: RequestPriority,
    ) -> Result<Option<RateLimitedOperation>, WebSocketLimiterCompileError> {
        self.operation(
            self.outbound_messages
                .iter()
                .chain(self.subscription_operations.iter())
                .chain(self.subscription_requests.iter())
                .cloned(),
            priority,
        )
    }

    fn operation(
        &self,
        budgets: impl Iterator<Item = BudgetId>,
        priority: RequestPriority,
    ) -> Result<Option<RateLimitedOperation>, WebSocketLimiterCompileError> {
        let charges = budgets
            .map(|id| BudgetCharge::new(id, 1))
            .collect::<Result<Vec<_>, _>>()?;

        if charges.is_empty() {
            return Ok(None);
        }

        Ok(Some(
            RateLimitedOperation::new(charges)?.with_priority(priority),
        ))
    }
}

// ============================================================
// CONNECTION TEMPLATE INSTANTIATION
// ============================================================

impl WebSocketConnectionTemplate {
    /// Construct unique budget identities for one physical
    /// WebSocket connection.
    ///
    /// Does not register them with RateLimitManager.
    pub fn instantiate(
        &self,
        connection_id: u64,
    ) -> Result<CompiledWebSocketConnectionLimits, WebSocketLimiterCompileError> {
        if connection_id == 0 {
            return Err(WebSocketLimiterCompileError::InvalidConnectionId);
        }

        let mut policies = RateLimitPolicySet::new();

        let mut register = |rule: &str,
                            quota: Option<WindowQuota>|
         -> Result<Option<BudgetId>, WebSocketLimiterCompileError> {
            let Some(quota) = quota else {
                return Ok(None);
            };

            let id = BudgetId::new(
                self.exchange,
                self.service.clone(),
                rule,
                BudgetScope::Connection(connection_id),
            )?;

            policies.insert(id.clone(), quota)?;

            Ok(Some(id))
        };

        let outbound_messages = register("ws_outbound_messages", self.outbound_messages)?;

        let subscription_operations =
            register("ws_subscription_operations", self.subscription_operations)?;

        let subscription_requests =
            register("ws_subscription_requests", self.subscription_requests)?;

        Ok(CompiledWebSocketConnectionLimits {
            policies,
            outbound_messages,
            subscription_operations,
            subscription_requests,
        })
    }
}

// ============================================================
// COMPILED WEBSOCKET PROFILE
// ============================================================

#[derive(Debug, Clone)]
pub struct CompiledWebSocketLimiters {
    pub shared: CompiledWebSocketSharedLimits,

    pub connection: WebSocketConnectionTemplate,

    pub constraints: WebSocketCapacityConstraints,
}

// ============================================================
// WEBSOCKET LIMITER COMPILER
// ============================================================

pub struct WebSocketLimiterCompiler;

impl WebSocketLimiterCompiler {
    pub fn compile(
        exchange: &ExchangeConfig,
        bindings: &WebSocketLimiterBindings,
    ) -> Result<CompiledWebSocketLimiters, WebSocketLimiterCompileError> {
        let websocket = &exchange.websocket;

        let limits = websocket.limits.as_ref();

        let mut policies = RateLimitPolicySet::new();
        let mut connection_attempt_budgets = Vec::new();

        // ====================================================
        // CONNECTION ATTEMPTS PER WINDOW
        // ====================================================

        let attempt_limit = resolve_duplicate(
            "connection_attempt_limit",
            websocket.connection_attempt_limit,
            limits.and_then(|l| l.connection_attempt_limit),
        )?;

        let attempt_window = resolve_duplicate(
            "connection_attempt_window_seconds",
            websocket.connection_attempt_window_seconds,
            limits.and_then(|l| l.connection_attempt_window_seconds),
        )?;

        if let Some(quota) =
            compile_optional_quota("connection_attempts", attempt_limit, attempt_window)?
        {
            let binding = bindings.connection_attempts.as_ref().ok_or_else(|| {
                WebSocketLimiterCompileError::MissingBinding("connection_attempts".into())
            })?;

            let id = binding.budget_id(exchange.exchange.id, "ws_connection_attempts")?;

            policies.insert(id.clone(), quota)?;
            connection_attempt_budgets.push(id);
        }

        // ====================================================
        // CONNECTION ATTEMPTS PER SECOND
        // ====================================================

        if let Some(per_second) = limits.and_then(|l| l.connection_attempts_per_second) {
            let quota = WindowQuota::new(per_second, Duration::from_secs(1))?;

            let binding = bindings
                .connection_attempts_per_second
                .as_ref()
                .ok_or_else(|| {
                    WebSocketLimiterCompileError::MissingBinding(
                        "connection_attempts_per_second".into(),
                    )
                })?;

            let id =
                binding.budget_id(exchange.exchange.id, "ws_connection_attempts_per_second")?;

            policies.insert(id.clone(), quota)?;
            connection_attempt_budgets.push(id);
        }

        // ====================================================
        // OUTBOUND MESSAGES PER SECOND
        // ====================================================

        let outbound_limit = resolve_duplicate(
            "max_client_messages_per_second",
            websocket.max_client_messages_per_second,
            limits.and_then(|l| l.max_client_messages_per_second),
        )?;

        let outbound_messages = outbound_limit
            .map(|capacity| WindowQuota::new(capacity, Duration::from_secs(1)))
            .transpose()?;

        // ====================================================
        // SUBSCRIPTION OPERATION LIMITS
        // ====================================================

        let subscription_operations = limits
            .and_then(|l| l.subscription_operations_per_hour)
            .map(|capacity| WindowQuota::new(capacity, Duration::from_secs(3600)))
            .transpose()?;

        let subscription_requests = limits
            .and_then(|l| l.subscription_requests_per_hour)
            .map(|capacity| WindowQuota::new(capacity, Duration::from_secs(3600)))
            .transpose()?;

        // ====================================================
        // CONNECTION SERVICE
        // ====================================================

        let connection_service = bindings
            .connection_service
            .clone()
            .unwrap_or_else(|| exchange.exchange.profile.clone());

        if connection_service.trim().is_empty() {
            return Err(WebSocketLimiterCompileError::MissingBinding(
                "connection_service".into(),
            ));
        }

        // ====================================================
        // CAPACITY CONSTRAINTS
        // ====================================================

        let constraints = compile_constraints(websocket, limits)?;

        Ok(CompiledWebSocketLimiters {
            shared: CompiledWebSocketSharedLimits {
                policies,
                connection_attempt_budgets,
            },

            connection: WebSocketConnectionTemplate {
                exchange: exchange.exchange.id,
                service: connection_service,
                outbound_messages,
                subscription_operations,
                subscription_requests,
            },

            constraints,
        })
    }
}

// ============================================================
// SHARED HELPERS
// ============================================================

/// Resolve fields repeated at top level and under
/// websocket.limits.
///
/// Conflicting values are rejected.
fn resolve_duplicate(
    name: &str,
    top_level: Option<u64>,
    nested: Option<u64>,
) -> Result<Option<u64>, WebSocketLimiterCompileError> {
    match (top_level, nested) {
        (Some(a), Some(b)) if a != b => Err(
            WebSocketLimiterCompileError::ConflictingConfiguration(name.to_owned()),
        ),

        (Some(value), _) | (_, Some(value)) => validate_positive(name, Some(value)),

        (None, None) => Ok(None),
    }
}

fn validate_positive(
    name: &str,
    value: Option<u64>,
) -> Result<Option<u64>, WebSocketLimiterCompileError> {
    if value == Some(0) {
        return Err(WebSocketLimiterCompileError::InvalidLimit(name.to_owned()));
    }

    Ok(value)
}

fn compile_optional_quota(
    name: &str,
    capacity: Option<u64>,
    window_seconds: Option<u64>,
) -> Result<Option<WindowQuota>, WebSocketLimiterCompileError> {
    match (capacity, window_seconds) {
        (None, None) => Ok(None),

        (Some(capacity), Some(seconds)) => Ok(Some(WindowQuota::new(
            capacity,
            Duration::from_secs(seconds),
        )?)),

        _ => Err(WebSocketLimiterCompileError::IncompleteQuota(
            name.to_owned(),
        )),
    }
}

// ============================================================
// CAPACITY CONSTRAINTS
// ============================================================

fn compile_constraints(
    websocket: &WebSocketConfig,
    limits: Option<&WebSocketLimits>,
) -> Result<WebSocketCapacityConstraints, WebSocketLimiterCompileError> {
    let max_subscriptions_per_connection = resolve_duplicate(
        "max_subscriptions_per_connection",
        websocket.max_subscriptions_per_connection,
        limits.and_then(|l| l.max_subscriptions_per_connection),
    )?;

    let constraints = WebSocketCapacityConstraints {
        max_connections_per_ip: limits.and_then(|l| l.max_connections_per_ip),

        max_market_data_connections_per_ip: limits
            .and_then(|l| l.max_market_data_connections_per_ip),

        max_subscriptions_per_connection,

        recommended_max_subscriptions: limits.and_then(|l| l.recommended_max_subscriptions),

        max_args_per_subscribe_request: limits.and_then(|l| l.max_args_per_subscribe_request),

        max_subscription_payload_bytes: limits.and_then(|l| l.max_subscription_payload_bytes),

        max_args_characters: limits.and_then(|l| l.max_args_characters),
    };

    for (name, value) in [
        ("max_connections_per_ip", constraints.max_connections_per_ip),
        (
            "max_market_data_connections_per_ip",
            constraints.max_market_data_connections_per_ip,
        ),
        (
            "recommended_max_subscriptions",
            constraints.recommended_max_subscriptions,
        ),
        (
            "max_args_per_subscribe_request",
            constraints.max_args_per_subscribe_request,
        ),
        (
            "max_subscription_payload_bytes",
            constraints.max_subscription_payload_bytes,
        ),
        ("max_args_characters", constraints.max_args_characters),
    ] {
        validate_positive(name, value)?;
    }

    if let (Some(recommended), Some(maximum)) = (
        constraints.recommended_max_subscriptions,
        constraints.max_subscriptions_per_connection,
    ) {
        if recommended > maximum {
            return Err(WebSocketLimiterCompileError::ConflictingConfiguration(
                "recommended_max_subscriptions".into(),
            ));
        }
    }

    Ok(constraints)
}

// ============================================================
// TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config(websocket_section: &str) -> ExchangeConfig {
        let content = format!(
            r#"
schema_version = 1

[exchange]
id = "binance"
market = "spot"
profile = "binance_spot"

{websocket_section}

[channels.trades]
enabled = true
format = "trades/binance_spot"

[rest]
base_url = "https://example.invalid"
"#
        );

        toml::from_str(&content).unwrap()
    }

    fn bindings() -> WebSocketLimiterBindings {
        WebSocketLimiterBindings {
            connection_attempts: Some(WebSocketBudgetBinding::new(
                "spot_websocket",
                BudgetScope::Egress("primary".into()),
            )),

            connection_attempts_per_second: Some(WebSocketBudgetBinding::new(
                "spot_websocket",
                BudgetScope::Egress("primary".into()),
            )),

            connection_service: Some("spot_websocket".into()),
        }
    }

    #[test]
    fn compiles_shared_connection_attempt_budget() {
        let config = test_config(
            r#"
[websocket]
base_url = "wss://example.invalid"
connection_attempt_limit = 300
connection_attempt_window_seconds = 300
"#,
        );

        let compiled = WebSocketLimiterCompiler::compile(&config, &bindings()).unwrap();

        assert_eq!(compiled.shared.policies.len(), 1);

        let operation = compiled
            .shared
            .connection_attempt_operation()
            .unwrap()
            .unwrap();

        assert_eq!(operation.charges.len(), 1);
        assert_eq!(operation.charges[0].cost, 1);
    }

    #[test]
    fn compiles_per_connection_message_budget() {
        let config = test_config(
            r#"
[websocket]
base_url = "wss://example.invalid"
max_client_messages_per_second = 10
"#,
        );

        let compiled = WebSocketLimiterCompiler::compile(&config, &bindings()).unwrap();

        let connection = compiled.connection.instantiate(42).unwrap();

        assert_eq!(connection.policies.len(), 1);

        let operation = connection
            .outbound_message_operation(RequestPriority::Normal)
            .unwrap()
            .unwrap();

        assert_eq!(operation.charges.len(), 1);
        assert_eq!(operation.charges[0].cost, 1);

        assert_eq!(
            operation.charges[0].budget.scope,
            BudgetScope::Connection(42),
        );
    }

    #[test]
    fn subscription_operation_consumes_all_applicable_budgets() {
        let config = test_config(
            r#"
[websocket]
base_url = "wss://example.invalid"

[websocket.limits]
max_client_messages_per_second = 10
subscription_operations_per_hour = 480
"#,
        );

        let compiled = WebSocketLimiterCompiler::compile(&config, &bindings()).unwrap();

        let connection = compiled.connection.instantiate(1).unwrap();

        let operation = connection
            .subscription_operation(RequestPriority::Normal)
            .unwrap()
            .unwrap();

        assert_eq!(operation.charges.len(), 2);
    }

    #[test]
    fn rejects_conflicting_duplicate_limits() {
        let config = test_config(
            r#"
[websocket]
base_url = "wss://example.invalid"
max_client_messages_per_second = 10

[websocket.limits]
max_client_messages_per_second = 20
"#,
        );

        assert!(matches!(
            WebSocketLimiterCompiler::compile(&config, &bindings(),),
            Err(WebSocketLimiterCompileError::ConflictingConfiguration(_))
        ));
    }

    #[test]
    fn rejects_incomplete_connection_attempt_quota() {
        let config = test_config(
            r#"
[websocket]
base_url = "wss://example.invalid"
connection_attempt_limit = 300
"#,
        );

        assert!(matches!(
            WebSocketLimiterCompiler::compile(&config, &bindings(),),
            Err(WebSocketLimiterCompileError::IncompleteQuota(_))
        ));
    }

    #[test]
    fn preserves_connection_capacity_constraints() {
        let config = test_config(
            r#"
[websocket]
base_url = "wss://example.invalid"

[websocket.limits]
max_connections_per_ip = 100
max_subscriptions_per_connection = 1000
recommended_max_subscriptions = 500
"#,
        );

        let compiled = WebSocketLimiterCompiler::compile(&config, &bindings()).unwrap();

        assert_eq!(compiled.constraints.max_connections_per_ip, Some(100));

        assert_eq!(
            compiled.constraints.max_subscriptions_per_connection,
            Some(1000)
        );

        assert_eq!(
            compiled.constraints.recommended_max_subscriptions,
            Some(500)
        );
    }

    #[test]
    fn connections_receive_independent_budget_identities() {
        let config = test_config(
            r#"
[websocket]
base_url = "wss://example.invalid"
max_client_messages_per_second = 10
"#,
        );

        let compiled = WebSocketLimiterCompiler::compile(&config, &bindings()).unwrap();

        let first = compiled.connection.instantiate(1).unwrap();

        let second = compiled.connection.instantiate(2).unwrap();

        let first_operation = first
            .outbound_message_operation(RequestPriority::Normal)
            .unwrap()
            .unwrap();

        let second_operation = second
            .outbound_message_operation(RequestPriority::Normal)
            .unwrap()
            .unwrap();

        assert_ne!(
            first_operation.charges[0].budget,
            second_operation.charges[0].budget,
        );
    }
}
