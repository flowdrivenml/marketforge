use std::time::Duration;

use super::{
    factory::{
        CompiledWebSocketConnectionLimits, WebSocketConnectionTemplate,
        WebSocketLimiterCompileError,
    },
    manager::{RateLimitManager, RateLimitManagerError},
    policy::{RateLimitedOperation, RequestPriority},
    snapshot::RateLimitSnapshot,
};

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, thiserror::Error)]
pub enum ConnectionLimiterError {
    #[error(transparent)]
    Configuration(#[from] WebSocketLimiterCompileError),

    #[error(transparent)]
    Manager(#[from] RateLimitManagerError),
}

// ============================================================
// CONNECTION RATE LIMITER
// ============================================================

/// Rate-limit enforcement for one physical WebSocket connection.
///
/// Owns independent connection-scoped budgets.
///
/// Shared exchange/IP connection-attempt budgets are NOT
/// managed here. They belong to the global RateLimitManager.
///
/// Dropping this object releases its manager and budget
/// state when no other references remain.
#[derive(Debug)]
pub struct ConnectionRateLimiter {
    connection_id: u64,

    manager: RateLimitManager,

    outbound_operation: Option<RateLimitedOperation>,

    subscription_operation: Option<RateLimitedOperation>,
}

// ============================================================
// CONSTRUCTION
// ============================================================

impl ConnectionRateLimiter {
    /// Create rate-limit state for one physical connection.
    ///
    /// Policies are instantiated from the already-compiled
    /// exchange WebSocket configuration.
    ///
    /// No network operation or connection establishment
    /// takes place here.
    pub fn new(
        template: &WebSocketConnectionTemplate,
        connection_id: u64,
    ) -> Result<Self, ConnectionLimiterError> {
        let compiled = template.instantiate(connection_id)?;

        Self::from_compiled(connection_id, compiled)
    }

    fn from_compiled(
        connection_id: u64,
        compiled: CompiledWebSocketConnectionLimits,
    ) -> Result<Self, ConnectionLimiterError> {
        let outbound_operation = compiled.outbound_message_operation(RequestPriority::Normal)?;

        let subscription_operation = compiled.subscription_operation(RequestPriority::Normal)?;

        // Connection-local traffic has a very small
        // bounded waiting queue.
        //
        // This is an initial operational constant,
        // not an exchange-enforced quota.
        let manager = RateLimitManager::new(compiled.policies, 64)?;

        Ok(Self {
            connection_id,
            manager,
            outbound_operation,
            subscription_operation,
        })
    }

    // ========================================================
    // OUTBOUND MESSAGE ADMISSION
    // ========================================================

    /// Acquire permission to send one ordinary outbound
    /// WebSocket message.
    ///
    /// Returns immediately when the exchange configuration
    /// declares no outbound-message quota.
    ///
    /// The WebSocket writer must still enforce any other
    /// applicable protocol requirements.
    pub async fn acquire_outbound(
        &self,
        priority: RequestPriority,
    ) -> Result<(), ConnectionLimiterError> {
        if let Some(operation) = &self.outbound_operation {
            let mut operation = operation.clone();
            operation.priority = priority;

            self.manager.acquire(&operation).await?;
        }

        Ok(())
    }

    // ========================================================
    // SUBSCRIPTION ADMISSION
    // ========================================================

    /// Acquire all applicable budgets for one physical
    /// outbound subscription request.
    ///
    /// This includes the outbound-message budget and
    /// configured subscription budgets.
    ///
    /// Do not call acquire_outbound() separately for the
    /// same message, or the outbound quota is charged twice.
    pub async fn acquire_subscription(
        &self,
        priority: RequestPriority,
    ) -> Result<(), ConnectionLimiterError> {
        if let Some(operation) = &self.subscription_operation {
            let mut operation = operation.clone();
            operation.priority = priority;

            self.manager.acquire(&operation).await?;
        }

        Ok(())
    }

    // ========================================================
    // TIMEOUT VARIANTS
    // ========================================================

    /// Acquire outbound-message permission with a deadline.
    pub async fn acquire_outbound_with_timeout(
        &self,
        priority: RequestPriority,
        timeout: Duration,
    ) -> Result<(), ConnectionLimiterError> {
        if let Some(operation) = &self.outbound_operation {
            let mut operation = operation.clone();
            operation.priority = priority;

            self.manager
                .acquire_with_timeout(&operation, timeout)
                .await?;
        }

        Ok(())
    }

    /// Acquire subscription permission with a deadline.
    pub async fn acquire_subscription_with_timeout(
        &self,
        priority: RequestPriority,
        timeout: Duration,
    ) -> Result<(), ConnectionLimiterError> {
        if let Some(operation) = &self.subscription_operation {
            let mut operation = operation.clone();
            operation.priority = priority;

            self.manager
                .acquire_with_timeout(&operation, timeout)
                .await?;
        }

        Ok(())
    }

    // ========================================================
    // OBSERVABILITY
    // ========================================================

    /// Unique identifier of the physical connection
    /// associated with these local budgets.
    pub fn connection_id(&self) -> u64 {
        self.connection_id
    }

    /// Point-in-time snapshot for monitoring and Ratatui.
    ///
    /// Contains only this physical connection's budgets.
    pub fn snapshot(&self) -> RateLimitSnapshot {
        self.manager.snapshot()
    }
}

// ============================================================
// TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    use crate::live::{
        config::exchange::ExchangeConfig,
        infrastructure::limiter::BudgetScope,
        infrastructure::limiter::factory::{
            WebSocketBudgetBinding, WebSocketLimiterBindings, WebSocketLimiterCompiler,
        },
    };

    // ========================================================
    // TEST CONFIGURATION
    // ========================================================

    fn template() -> WebSocketConnectionTemplate {
        let config: ExchangeConfig = toml::from_str(
            r#"
schema_version = 1

[exchange]
id = "bitget"
market = "spot"
profile = "bitget_spot"

[websocket]
url = "wss://example.invalid"

[websocket.limits]
max_client_messages_per_second = 2
subscription_requests_per_hour = 1

[channels.trades]
enabled = true
format = "trades/bitget_spot"

[rest]
base_url = "https://example.invalid"
"#,
        )
        .unwrap();

        let bindings = WebSocketLimiterBindings {
            connection_attempts: Some(WebSocketBudgetBinding::new(
                "uta_v3_websocket",
                BudgetScope::Egress("primary".into()),
            )),
            connection_attempts_per_second: None,
            connection_service: Some("uta_v3_websocket".into()),
        };

        WebSocketLimiterCompiler::compile(&config, &bindings)
            .unwrap()
            .connection
    }

    // ========================================================
    // TESTS
    // ========================================================

    #[tokio::test]
    async fn enforces_outbound_message_quota() {
        let limiter = ConnectionRateLimiter::new(&template(), 1).unwrap();

        limiter
            .acquire_outbound(RequestPriority::Normal)
            .await
            .unwrap();

        limiter
            .acquire_outbound(RequestPriority::Normal)
            .await
            .unwrap();

        let result = limiter
            .acquire_outbound_with_timeout(RequestPriority::Normal, Duration::from_millis(10))
            .await;

        assert!(matches!(
            result,
            Err(ConnectionLimiterError::Manager(
                RateLimitManagerError::Timeout
            ))
        ));
    }

    #[tokio::test]
    async fn subscription_consumes_outbound_and_subscription_budgets() {
        let limiter = ConnectionRateLimiter::new(&template(), 2).unwrap();

        limiter
            .acquire_subscription(RequestPriority::Normal)
            .await
            .unwrap();

        let snapshot = limiter.snapshot();

        assert_eq!(snapshot.budget_count(), 2);

        assert!(
            snapshot
                .budgets
                .iter()
                .all(|budget| { budget.locally_used == 1 })
        );

        // A second subscription is blocked by the
        // per-hour subscription quota.
        let result = limiter
            .acquire_subscription_with_timeout(RequestPriority::Normal, Duration::from_millis(10))
            .await;

        assert!(matches!(
            result,
            Err(ConnectionLimiterError::Manager(
                RateLimitManagerError::Timeout
            ))
        ));
    }

    #[tokio::test]
    async fn connections_have_independent_budgets() {
        let template = template();

        let first = ConnectionRateLimiter::new(&template, 10).unwrap();

        let second = ConnectionRateLimiter::new(&template, 11).unwrap();

        first
            .acquire_outbound(RequestPriority::Normal)
            .await
            .unwrap();

        first
            .acquire_outbound(RequestPriority::Normal)
            .await
            .unwrap();

        // Connection #10 is exhausted.
        let blocked = first
            .acquire_outbound_with_timeout(RequestPriority::Normal, Duration::from_millis(10))
            .await;

        assert!(blocked.is_err());

        // Connection #11 has independent capacity.
        second
            .acquire_outbound_with_timeout(RequestPriority::Normal, Duration::from_millis(100))
            .await
            .unwrap();

        assert_eq!(first.connection_id(), 10);
        assert_eq!(second.connection_id(), 11);
    }

    #[tokio::test]
    async fn snapshot_reports_connection_usage() {
        let limiter = ConnectionRateLimiter::new(&template(), 42).unwrap();

        limiter
            .acquire_subscription(RequestPriority::High)
            .await
            .unwrap();

        let snapshot = limiter.snapshot();

        assert_eq!(snapshot.budget_count(), 2);

        for budget in &snapshot.budgets {
            assert_eq!(budget.id.scope, BudgetScope::Connection(42),);

            assert_eq!(budget.locally_used, 1);
        }
    }

    #[test]
    fn rejects_invalid_connection_id() {
        assert!(ConnectionRateLimiter::new(&template(), 0).is_err());
    }

    #[tokio::test]
    async fn dropping_connection_does_not_affect_new_connection() {
        let template = template();

        {
            let old = ConnectionRateLimiter::new(&template, 100).unwrap();

            old.acquire_outbound(RequestPriority::Normal).await.unwrap();

            old.acquire_outbound(RequestPriority::Normal).await.unwrap();
        }

        // The old limiter has been dropped.
        //
        // A newly constructed connection starts with
        // independent, empty connection-scoped usage.
        let new = ConnectionRateLimiter::new(&template, 101).unwrap();

        let snapshot = new.snapshot();

        assert!(
            snapshot
                .budgets
                .iter()
                .all(|budget| { budget.locally_used == 0 })
        );
    }
}
