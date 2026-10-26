use std::time::Duration;

use futures_util::{SinkExt, StreamExt};

use tokio::{
    sync::{mpsc, oneshot},
    time,
};

use tokio_tungstenite::{accept_async, tungstenite::Message};

use marketforge_engine::live::{
    config::exchange::ExchangeConfig,
    infrastructure::{
        limiter::{
            BudgetScope, RateLimitManager,
            factory::{
                CompiledWebSocketLimiters, WebSocketBudgetBinding, WebSocketLimiterBindings,
                WebSocketLimiterCompiler,
            },
        },
        websocket::{
            CompiledHeartbeatPlan, HeartbeatPlanCompiler, ReconnectError, ReconnectEvent,
            ReconnectPolicy, ReconnectRunner, SupervisorExit, WebSocketSupervisor,
        },
    },
};

// ============================================================
// TEST INFRASTRUCTURE
// ============================================================

fn infrastructure() -> (
    CompiledWebSocketLimiters,
    RateLimitManager,
    CompiledHeartbeatPlan,
) {
    let config: ExchangeConfig = toml::from_str(
        r#"
schema_version = 1

[exchange]
id = "binance"
market = "spot"
profile = "binance_spot"

[websocket]
url = "wss://example.invalid"
connection_attempt_limit = 10
connection_attempt_window_seconds = 60
max_client_messages_per_second = 20

[websocket.heartbeat]
strategy = "websocket_control_frames"
respond_to_ping = true
echo_ping_payload = true

[channels.trades]
enabled = true
format = "trades/binance_spot"

[rest]
base_url = "https://example.invalid"
"#,
    )
    .unwrap();

    let bindings = WebSocketLimiterBindings {
        connection_attempts: Some(WebSocketBudgetBinding::new(
            "reconnect-test",
            BudgetScope::Egress("local".into()),
        )),
        connection_attempts_per_second: None,
        connection_service: Some("reconnect-test".into()),
    };

    let compiled = WebSocketLimiterCompiler::compile(&config, &bindings).unwrap();

    let shared = RateLimitManager::new(compiled.shared.policies.clone(), 32).unwrap();

    let plan = HeartbeatPlanCompiler::compile(&config).unwrap();

    (compiled, shared, plan)
}

fn retry_policy(maximum_failures: Option<u32>) -> ReconnectPolicy {
    ReconnectPolicy::new(
        Duration::from_millis(30),
        Duration::from_millis(200),
        Duration::from_secs(10),
        Duration::from_secs(2),
        maximum_failures,
    )
    .unwrap()
}

// ============================================================
// REUSABLE SESSION
// ============================================================

async fn run_session(
    connection: marketforge_engine::live::infrastructure::websocket::WebSocketConnection,
    plan: CompiledHeartbeatPlan,
) -> Result<
    SupervisorExit,
    marketforge_engine::live::infrastructure::websocket::WebSocketTransportError,
> {
    let (inbound_tx, _inbound_rx) = mpsc::channel(8);
    let (_outbound_tx, outbound_rx) = mpsc::channel(8);
    let (_shutdown_tx, shutdown_rx) = oneshot::channel();

    // Retain _shutdown_tx until the session completes.
    // Otherwise the supervisor would see a cancelled
    // shutdown sender and exit immediately.
    let result =
        WebSocketSupervisor::run_with_plan(connection, inbound_tx, outbound_rx, shutdown_rx, plan)
            .await;

    drop(_shutdown_tx);

    result
}

// ============================================================
// REAL RECONNECT
// ============================================================

#[tokio::test]
async fn reconnects_with_new_generation_and_connection_id() {
    let listener = crate::websocket_server::listen().await;

    let address = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        // First physical connection.
        let (stream, _) = listener.accept().await.unwrap();
        let mut first = accept_async(stream).await.unwrap();

        // Force the first session to terminate.
        first.send(Message::Close(None)).await.unwrap();
        drop(first);

        // Replacement physical connection.
        let (stream, _) = listener.accept().await.unwrap();
        let mut second = accept_async(stream).await.unwrap();

        // Remain connected until the runner shuts down.
        while let Some(message) = second.next().await {
            if message.is_err() {
                break;
            }

            if matches!(message, Ok(Message::Close(_))) {
                break;
            }
        }
    });

    let (compiled, shared, heartbeat) = infrastructure();

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let (events_tx, mut events_rx) = mpsc::channel(32);

    let runner = tokio::spawn(async move {
        ReconnectRunner::run(
            &format!("ws://127.0.0.1:{}", address.port()),
            &compiled,
            &shared,
            retry_policy(None),
            shutdown_rx,
            events_tx,
            move |_generation, connection| {
                let plan = heartbeat.clone();

                async move { run_session(connection, plan).await }
            },
        )
        .await
    });

    let mut connections = Vec::new();
    let mut disconnections = Vec::new();

    // Observe the lifecycle, not wall-clock sleeps.
    time::timeout(Duration::from_secs(5), async {
        while connections.len() < 2 {
            let event = events_rx.recv().await.expect("runner event channel closed");

            match event {
                ReconnectEvent::Connected {
                    generation,
                    connection_id,
                } => {
                    connections.push((generation.get(), connection_id));
                }

                ReconnectEvent::Disconnected { generation, .. } => {
                    disconnections.push(generation.get());
                }

                _ => {}
            }
        }
    })
    .await
    .expect("replacement connection was not established");

    assert_eq!(connections.len(), 2);

    assert!(
        connections[1].0 > connections[0].0,
        "reconnect reused the previous generation",
    );

    assert_ne!(
        connections[0].1, connections[1].1,
        "reconnect reused the physical connection ID",
    );

    assert_eq!(disconnections, vec![connections[0].0],);

    shutdown_tx.send(()).unwrap();

    time::timeout(Duration::from_secs(3), runner)
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    time::timeout(Duration::from_secs(3), server)
        .await
        .unwrap()
        .unwrap();
}

// ============================================================
// RETRY EXHAUSTION
// ============================================================

#[tokio::test]
async fn failed_connections_respect_retry_limit() {
    // Reserve an ephemeral port, then release it.
    // Connecting to it should fail.
    let listener = crate::websocket_server::listen().await;

    let address = listener.local_addr().unwrap();
    drop(listener);

    let (compiled, shared, heartbeat) = infrastructure();

    let (_shutdown_tx, shutdown_rx) = oneshot::channel();
    let (events_tx, mut events_rx) = mpsc::channel(32);

    let result = time::timeout(
        Duration::from_secs(5),
        ReconnectRunner::run(
            &format!("ws://127.0.0.1:{}", address.port()),
            &compiled,
            &shared,
            retry_policy(Some(3)),
            shutdown_rx,
            events_tx,
            move |_generation, connection| {
                let plan = heartbeat.clone();

                async move { run_session(connection, plan).await }
            },
        ),
    )
    .await
    .expect("retry limit was not enforced");

    assert!(matches!(result, Err(ReconnectError::RetryExhausted(3)),));

    let mut attempts = 0_usize;
    let mut failures = 0_usize;

    while let Ok(event) = events_rx.try_recv() {
        match event {
            ReconnectEvent::Connecting { .. } => {
                attempts += 1;
            }

            ReconnectEvent::Disconnected { .. } => {
                failures += 1;
            }

            _ => {}
        }
    }

    assert_eq!(attempts, 3);
    assert_eq!(failures, 3);

    // Every attempted connection consumes admission
    // under the shared exchange limiter.
    let snapshot = shared.snapshot();

    assert_eq!(snapshot.total_pending_requests, 0);
    assert_eq!(snapshot.budgets[0].locally_used, 3);
}

// ============================================================
// BACKOFF POLICY
// ============================================================

#[test]
fn backoff_is_bounded_and_monotonic() {
    let policy = retry_policy(None);

    assert_eq!(policy.delay_for_failure(1), Duration::from_millis(30),);

    assert_eq!(policy.delay_for_failure(2), Duration::from_millis(60),);

    assert_eq!(policy.delay_for_failure(3), Duration::from_millis(120),);

    assert_eq!(policy.delay_for_failure(4), Duration::from_millis(200),);

    assert_eq!(policy.delay_for_failure(100), Duration::from_millis(200),);
}

#[test]
fn rejects_invalid_retry_configuration() {
    assert!(
        ReconnectPolicy::new(
            Duration::ZERO,
            Duration::from_secs(1),
            Duration::from_secs(10),
            Duration::from_secs(2),
            None,
        )
        .is_err()
    );

    assert!(
        ReconnectPolicy::new(
            Duration::from_secs(2),
            Duration::from_secs(1),
            Duration::from_secs(10),
            Duration::from_secs(2),
            None,
        )
        .is_err()
    );
}
