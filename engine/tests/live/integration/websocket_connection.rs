use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
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
        websocket::{WebSocketConnector, WebSocketTransportError},
    },
};

// ============================================================
// LIMITER SETUP
// ============================================================

fn compiled_limits() -> (CompiledWebSocketLimiters, RateLimitManager) {
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
max_client_messages_per_second = 2

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
            "test-websocket",
            BudgetScope::Egress("local".into()),
        )),
        connection_attempts_per_second: None,
        connection_service: Some("test-websocket".into()),
    };

    let compiled = WebSocketLimiterCompiler::compile(&config, &bindings).unwrap();

    let manager = RateLimitManager::new(compiled.shared.policies.clone(), 16).unwrap();

    (compiled, manager)
}

// ============================================================
// CONNECT, SEND, RECEIVE, CLOSE
// ============================================================

#[tokio::test]
async fn w1_connect_send_receive_close() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

    let address = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();

        let incoming = socket.next().await.unwrap().unwrap();

        assert_eq!(incoming.into_text().unwrap().as_str(), "subscribe",);

        socket
            .send(Message::Text("trade-message".into()))
            .await
            .unwrap();

        // The client performs a close handshake.
        let close = socket.next().await.unwrap().unwrap();

        assert!(matches!(close, Message::Close(_)));
    });

    let (compiled, shared) = compiled_limits();

    let mut connection = WebSocketConnector::connect(
        &format!("ws://127.0.0.1:{}", address.port()),
        42,
        &compiled,
        &shared,
        Duration::from_secs(2),
    )
    .await
    .unwrap();

    assert_eq!(connection.connection_id(), 42);

    connection
        .send_subscription("subscribe", Duration::from_secs(2))
        .await
        .unwrap();

    let message = connection.receive(Duration::from_secs(2)).await.unwrap();

    assert_eq!(message.into_text().unwrap().as_str(), "trade-message",);

    connection.close(Duration::from_secs(2)).await.unwrap();

    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();

    let shared_snapshot = shared.snapshot();
    assert_eq!(shared_snapshot.total_pending_requests, 0);
    assert_eq!(shared_snapshot.budgets.len(), 1);
    assert_eq!(shared_snapshot.budgets[0].locally_used, 1);

    let local_snapshot = connection.limiter().snapshot();
    assert_eq!(local_snapshot.budgets.len(), 1);
    assert_eq!(local_snapshot.budgets[0].locally_used, 1);
}

// ============================================================
// INSECURE EXTERNAL URL REJECTION
// ============================================================

#[tokio::test]
async fn w1_rejects_insecure_external_url() {
    let (compiled, shared) = compiled_limits();

    let result = WebSocketConnector::connect(
        "ws://example.com/feed",
        1,
        &compiled,
        &shared,
        Duration::from_secs(1),
    )
    .await;

    assert!(matches!(
        result,
        Err(WebSocketTransportError::Configuration(_)),
    ));

    assert_eq!(shared.snapshot().budgets[0].locally_used, 0);
}

// ============================================================
// OUTBOUND LIMITER
// ============================================================

#[tokio::test]
async fn w1_outbound_limiter_rejects_third_message() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

    let address = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();

        for _ in 0..2 {
            assert!(socket.next().await.unwrap().is_ok());
        }
    });

    let (compiled, shared) = compiled_limits();

    let mut connection = WebSocketConnector::connect(
        &format!("ws://127.0.0.1:{}", address.port()),
        43,
        &compiled,
        &shared,
        Duration::from_secs(2),
    )
    .await
    .unwrap();

    for _ in 0..2 {
        connection
            .send_text("message", Duration::from_secs(1))
            .await
            .unwrap();
    }

    let third = connection
        .send_text("blocked", Duration::from_millis(50))
        .await;

    assert!(matches!(
        third,
        Err(WebSocketTransportError::OperationTimeout),
    ));

    let snapshot = connection.limiter().snapshot();

    assert_eq!(snapshot.budgets.len(), 1);
    assert_eq!(snapshot.budgets[0].locally_used, 2);
    assert_eq!(snapshot.total_pending_requests, 0);

    drop(connection);

    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();
}
