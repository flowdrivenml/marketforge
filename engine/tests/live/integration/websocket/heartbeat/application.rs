use std::time::Duration;

use futures_util::{SinkExt, StreamExt};

use tokio::{
    sync::{mpsc, oneshot},
    time,
};

use tokio_tungstenite::{accept_async, tungstenite::Message};

use marketforge_engine::live::{
    config::exchange::{ExchangeConfig, HeartbeatConfig},
    infrastructure::{
        limiter::{
            BudgetScope, RateLimitManager,
            factory::{
                CompiledWebSocketLimiters, WebSocketBudgetBinding, WebSocketLimiterBindings,
                WebSocketLimiterCompiler,
            },
        },
        websocket::{
            ApplicationHeartbeatPolicy, HeartbeatPolicy, SupervisorExit, WebSocketConnector,
            WebSocketSupervisor, WebSocketTransportError,
        },
    },
};

// ============================================================
// INFRASTRUCTURE
// ============================================================

fn limits() -> (CompiledWebSocketLimiters, RateLimitManager) {
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
max_client_messages_per_second = 100

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
            "application-heartbeat-test",
            BudgetScope::Egress("local".into()),
        )),
        connection_attempts_per_second: None,
        connection_service: Some("application-heartbeat-test".into()),
    };

    let compiled = WebSocketLimiterCompiler::compile(&config, &bindings).unwrap();

    let shared = RateLimitManager::new(compiled.shared.policies.clone(), 16).unwrap();

    (compiled, shared)
}

fn control_policy() -> HeartbeatPolicy {
    // Keep the control heartbeat well outside the test
    // interval so these tests isolate application heartbeats.
    HeartbeatPolicy::new(
        Duration::from_secs(10),
        Duration::from_secs(2),
        Duration::from_secs(2),
    )
    .unwrap()
}

fn application_policy(configuration: &str) -> ApplicationHeartbeatPolicy {
    let config: HeartbeatConfig = toml::from_str(configuration).unwrap();

    let protocol =
        marketforge_engine::live::infrastructure::websocket::HeartbeatProtocol::compile(&config)
            .unwrap();

    ApplicationHeartbeatPolicy::new(
        protocol,
        Duration::from_millis(40),
        Duration::from_millis(150),
        Duration::from_millis(100),
    )
    .unwrap()
}

// ============================================================
// TEXT HEARTBEAT
// ============================================================

#[tokio::test]
async fn text_heartbeat_keeps_connection_alive() {
    let listener = crate::websocket_server::listen().await;

    let address = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();

        let mut count = 0_usize;

        while let Some(Ok(message)) = socket.next().await {
            match message {
                Message::Text(text) if text == "ping" => {
                    count += 1;

                    socket.send(Message::Text("pong".into())).await.unwrap();
                }

                Message::Close(_) => break,

                _ => {}
            }
        }

        count
    });

    let (compiled, shared) = limits();

    let connection = WebSocketConnector::connect(
        &format!("ws://127.0.0.1:{}", address.port()),
        201,
        &compiled,
        &shared,
        Duration::from_secs(2),
    )
    .await
    .unwrap();

    let (inbound_tx, _inbound_rx) = mpsc::channel(8);
    let (_outbound_tx, outbound_rx) = mpsc::channel(8);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let supervisor = tokio::spawn(WebSocketSupervisor::run_with_application_heartbeat(
        connection,
        inbound_tx,
        outbound_rx,
        shutdown_rx,
        control_policy(),
        Some(application_policy(
            r#"
strategy = "text_ping_pong"
request = "ping"
response = "pong"
"#,
        )),
    ));

    time::sleep(Duration::from_millis(230)).await;

    shutdown_tx.send(()).unwrap();

    assert_eq!(
        time::timeout(Duration::from_secs(2), supervisor)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        SupervisorExit::Shutdown,
    );

    let count = time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();

    assert!(
        count >= 2,
        "expected repeated application heartbeats, got {count}"
    );
}

// ============================================================
// JSON HEARTBEAT + UNRELATED MARKET DATA
// ============================================================

#[tokio::test]
async fn json_heartbeat_preserves_market_data() {
    let listener = crate::websocket_server::listen().await;

    let address = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();

        let heartbeat = socket.next().await.unwrap().unwrap();

        assert_eq!(heartbeat.into_text().unwrap().as_str(), r#"{"op":"ping"}"#);

        // The unrelated market-data message arrives while
        // the heartbeat response is still pending.
        socket
            .send(Message::Text(
                r#"{"topic":"trades","price":"80000"}"#.into(),
            ))
            .await
            .unwrap();

        socket
            .send(Message::Text(r#"{"op":"pong","success":true}"#.into()))
            .await
            .unwrap();

        while let Some(Ok(message)) = socket.next().await {
            match message {
                Message::Text(text) if text == r#"{"op":"ping"}"# => {
                    socket
                        .send(Message::Text(r#"{"op":"pong","success":true}"#.into()))
                        .await
                        .unwrap();
                }

                Message::Close(_) => break,

                _ => {}
            }
        }
    });

    let (compiled, shared) = limits();

    let connection = WebSocketConnector::connect(
        &format!("ws://127.0.0.1:{}", address.port()),
        202,
        &compiled,
        &shared,
        Duration::from_secs(2),
    )
    .await
    .unwrap();

    let (inbound_tx, mut inbound_rx) = mpsc::channel(8);
    let (_outbound_tx, outbound_rx) = mpsc::channel(8);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let supervisor = tokio::spawn(WebSocketSupervisor::run_with_application_heartbeat(
        connection,
        inbound_tx,
        outbound_rx,
        shutdown_rx,
        control_policy(),
        Some(application_policy(
            r#"
strategy = "json_ping_pong"
request = '{"op":"ping"}'
response_op_field = "op"
response_op_value = "pong"
response_success_field = "success"
response_success_value = true
"#,
        )),
    ));

    let message = time::timeout(Duration::from_secs(2), inbound_rx.recv())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        message.into_text().unwrap().as_str(),
        r#"{"topic":"trades","price":"80000"}"#
    );

    // The heartbeat reply must not enter the market-data
    // queue.
    assert!(
        time::timeout(Duration::from_millis(20), inbound_rx.recv(),)
            .await
            .is_err()
    );

    shutdown_tx.send(()).unwrap();

    assert_eq!(supervisor.await.unwrap().unwrap(), SupervisorExit::Shutdown);

    server.await.unwrap();
}

// ============================================================
// MISSING APPLICATION HEARTBEAT RESPONSE
// ============================================================

#[tokio::test]
async fn missing_application_pong_fails_connection() {
    let listener = crate::websocket_server::listen().await;

    let address = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();

        // Read requests but deliberately never send
        // application heartbeat responses.
        while let Some(Ok(message)) = socket.next().await {
            if matches!(message, Message::Close(_)) {
                break;
            }
        }
    });

    let (compiled, shared) = limits();

    let connection = WebSocketConnector::connect(
        &format!("ws://127.0.0.1:{}", address.port()),
        203,
        &compiled,
        &shared,
        Duration::from_secs(2),
    )
    .await
    .unwrap();

    let (inbound_tx, _inbound_rx) = mpsc::channel(8);
    let (_outbound_tx, outbound_rx) = mpsc::channel(8);
    let (_shutdown_tx, shutdown_rx) = oneshot::channel();

    let result = time::timeout(
        Duration::from_secs(2),
        WebSocketSupervisor::run_with_application_heartbeat(
            connection,
            inbound_tx,
            outbound_rx,
            shutdown_rx,
            control_policy(),
            Some(application_policy(
                r#"
strategy = "text_ping_pong"
request = "ping"
response = "pong"
"#,
            )),
        ),
    )
    .await
    .expect("application heartbeat deadline was not enforced");

    assert!(matches!(
        result,
        Err(WebSocketTransportError::ApplicationHeartbeatTimeout),
    ));

    server.await.unwrap();
}
