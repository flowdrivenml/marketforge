use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

use futures_util::{SinkExt, StreamExt};

use tokio::{
    sync::{mpsc, oneshot},
    time,
};

use tokio_tungstenite::{accept_async, tungstenite::Message};

use marketforge_engine::live::{
    application::{compile_limiters, compile_websocket},
    config::{ExchangeConfigStore, exchange::ExchangeConfig, general::RecoveryDefaultsConfig},
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
            ReconnectPolicy, ReconnectRunner, SupervisorExit, WebSocketManagerRuntime,
            WebSocketSupervisor,
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

fn compiled_runtime(jitter: bool, initial_backoff_ms: u64) -> WebSocketManagerRuntime {
    let exchanges = ExchangeConfigStore::load_dir(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../data/.config/live/exchanges"
    ))
    .unwrap()
    .selected(&BTreeSet::from(["binance_spot".into()]))
    .unwrap();
    let weights = BTreeMap::from([("binance_spot".into(), (100, 60))]);
    let limiters = compile_limiters(&exchanges, "local-test", &weights).unwrap();
    compile_websocket(
        &exchanges,
        &limiters,
        &RecoveryDefaultsConfig {
            initial_backoff_ms,
            max_backoff_seconds: 60,
            jitter,
        },
        &BTreeMap::new(),
    )
    .unwrap()
}

#[test]
fn production_reconnect_compiles_recovery_settings() {
    for jitter in [false, true] {
        let policy = compiled_runtime(jitter, 501).reconnect_policy;
        assert_eq!(policy.jitter_enabled(), jitter);
        for (failure, maximum) in [
            (1, 501_u64),
            (2, 1002),
            (3, 2004),
            (8, 60_000),
            (u32::MAX, 60_000),
        ] {
            let minimum = if jitter { maximum.div_ceil(2) } else { maximum };
            let delay = policy.delay_for_failure(failure);
            assert!(
                (Duration::from_millis(minimum)..=Duration::from_millis(maximum)).contains(&delay)
            );
        }
    }
}

#[tokio::test]
async fn production_reconnect_jitter_wait_is_cancellable_and_charged_to_shared_quota() {
    let listener = crate::websocket_server::listen().await;
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let reject_handshakes = async {
        for _ in 0..2 {
            let (socket, _) = listener.accept().await.unwrap();
            drop(socket);
        }
    };
    let runtime = compiled_runtime(true, 60_000);
    let compiled = &runtime.profiles[0].compiled_limiters;

    let start = || async {
        let (stop, shutdown) = oneshot::channel();
        let (events, mut observed) = mpsc::channel(8);
        let runner = ReconnectRunner::run(
            &url,
            compiled,
            &runtime.shared_limiter,
            runtime.reconnect_policy,
            shutdown,
            events,
            |_, _| std::future::ready(Ok(SupervisorExit::Shutdown)),
        );
        let cancel = async {
            assert!(matches!(
                observed.recv().await,
                Some(ReconnectEvent::Connecting { .. })
            ));
            assert!(matches!(
                observed.recv().await,
                Some(ReconnectEvent::Disconnected { .. })
            ));
            match observed.recv().await.unwrap() {
                ReconnectEvent::Backoff {
                    consecutive_failures: 1,
                    delay,
                    ..
                } => {
                    assert!((Duration::from_secs(30)..=Duration::from_secs(60)).contains(&delay));
                }
                event => panic!("unexpected retry event {event:?}"),
            }
            stop.send(()).unwrap();
            assert_eq!(observed.recv().await, Some(ReconnectEvent::Stopped));
        };
        let (result, ()) = tokio::join!(runner, cancel);
        result.unwrap();
    };
    time::timeout(Duration::from_secs(5), async {
        tokio::join!(start(), start(), reject_handshakes)
    })
    .await
    .expect("both reconnect waits must cancel before their minimum delay");
    let snapshot = runtime.shared_limiter.snapshot();
    assert_eq!(snapshot.total_pending_requests, 0);
    assert!(
        snapshot
            .budgets
            .iter()
            .any(|budget| budget.locally_used == 2)
    );
}

#[tokio::test]
async fn reconnect_resets_failure_history_only_after_a_stable_session() {
    let listener = crate::websocket_server::listen().await;
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let server = async {
        for _ in 0..4 {
            let (stream, _) = listener.accept().await.unwrap();
            accept_async(stream).await.unwrap();
        }
    };
    let (compiled, shared, _) = infrastructure();
    let mut policy = retry_policy(Some(3));
    policy.stable_connection_duration = Duration::from_secs(1);
    let (_stop, shutdown) = oneshot::channel();
    let (events, mut observed) = mpsc::channel(16);
    let runner = ReconnectRunner::run(
        &url,
        &compiled,
        &shared,
        policy,
        shutdown,
        events,
        |generation, _| async move {
            match generation.get() {
                3 => time::sleep(policy.stable_connection_duration).await,
                4 => return Ok(SupervisorExit::Shutdown),
                _ => {}
            }
            Ok(SupervisorExit::PeerClosed)
        },
    );
    let verify = async {
        for (failures, delay) in [(1, 30), (2, 60), (1, 30)] {
            assert!(matches!(
                observed.recv().await,
                Some(ReconnectEvent::Connecting { .. })
            ));
            assert!(matches!(
                observed.recv().await,
                Some(ReconnectEvent::Connected { .. })
            ));
            assert!(matches!(
                observed.recv().await,
                Some(ReconnectEvent::Disconnected { .. })
            ));
            match observed.recv().await.unwrap() {
                ReconnectEvent::Backoff {
                    consecutive_failures,
                    delay: actual,
                    ..
                } => {
                    assert_eq!(consecutive_failures, failures);
                    assert_eq!(actual, Duration::from_millis(delay));
                }
                event => panic!("unexpected reconnect event {event:?}"),
            }
        }
        assert!(matches!(
            observed.recv().await,
            Some(ReconnectEvent::Connecting { .. })
        ));
        assert!(matches!(
            observed.recv().await,
            Some(ReconnectEvent::Connected { .. })
        ));
        assert_eq!(observed.recv().await, Some(ReconnectEvent::Stopped));
    };
    let (result, (), ()) = time::timeout(Duration::from_secs(8), async {
        tokio::join!(runner, server, verify)
    })
    .await
    .expect("local reconnect lifecycle timed out");
    result.unwrap();
}
