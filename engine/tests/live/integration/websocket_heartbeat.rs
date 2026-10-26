use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::{
    net::TcpListener,
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
            HeartbeatPolicy, OutboundMessage, SupervisorExit, WebSocketConnector,
            WebSocketSupervisor, WebSocketTransportError,
        },
    },
};

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
max_client_messages_per_second = 20

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
            "heartbeat-test",
            BudgetScope::Egress("local".into()),
        )),
        connection_attempts_per_second: None,
        connection_service: Some("heartbeat-test".into()),
    };

    let compiled = WebSocketLimiterCompiler::compile(&config, &bindings).unwrap();

    let shared = RateLimitManager::new(compiled.shared.policies.clone(), 16).unwrap();

    (compiled, shared)
}

fn heartbeat() -> HeartbeatPolicy {
    HeartbeatPolicy::new(
        Duration::from_millis(40),
        Duration::from_millis(100),
        Duration::from_millis(100),
    )
    .unwrap()
}

#[tokio::test]
async fn ping_pong_keeps_connection_alive() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

    let address = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();

        let mut ping_count = 0_usize;

        loop {
            match socket.next().await {
                Some(Ok(Message::Ping(payload))) => {
                    ping_count += 1;

                    // A real server echoes the Ping token.
                    socket.send(Message::Pong(payload)).await.unwrap();
                }

                Some(Ok(Message::Close(_))) | None => break,

                Some(Ok(_)) => {}

                Some(Err(_)) => break,
            }
        }

        ping_count
    });

    let (compiled, shared) = limits();

    let connection = WebSocketConnector::connect(
        &format!("ws://127.0.0.1:{}", address.port()),
        100,
        &compiled,
        &shared,
        Duration::from_secs(2),
    )
    .await
    .unwrap();

    let (inbound_tx, _inbound_rx) = mpsc::channel(8);
    let (_outbound_tx, outbound_rx) = mpsc::channel(8);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let supervisor = tokio::spawn(WebSocketSupervisor::run(
        connection,
        inbound_tx,
        outbound_rx,
        shutdown_rx,
        heartbeat(),
    ));

    // Keep the socket active across several Ping intervals.
    time::sleep(Duration::from_millis(230)).await;

    shutdown_tx.send(()).unwrap();

    let exit = time::timeout(Duration::from_secs(2), supervisor)
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    assert_eq!(exit, SupervisorExit::Shutdown);

    let observed = time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();

    assert!(
        observed >= 2,
        "expected multiple control-frame heartbeats, got {observed}",
    );
}

#[tokio::test]
async fn missing_pong_closes_connection() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

    let address = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();

        // Complete the handshake, but deliberately never
        // poll the WebSocket stream again.
        //
        // Therefore, the server cannot process or answer
        // a client-generated Ping.
        let _socket = accept_async(stream).await.unwrap();

        time::sleep(Duration::from_millis(300)).await;
    });

    let (compiled, shared) = limits();

    let connection = WebSocketConnector::connect(
        &format!("ws://127.0.0.1:{}", address.port()),
        101,
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
        WebSocketSupervisor::run(
            connection,
            inbound_tx,
            outbound_rx,
            shutdown_rx,
            heartbeat(),
        ),
    )
    .await
    .expect("supervisor did not enforce heartbeat deadline");

    assert!(matches!(
        result,
        Err(WebSocketTransportError::HeartbeatTimeout),
    ));

    server.await.unwrap();
}

#[tokio::test]
async fn inbound_queue_overflow_fails_closed() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

    let address = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();

        for value in ["one", "two", "three"] {
            socket.send(Message::Text(value.into())).await.unwrap();
        }

        time::sleep(Duration::from_millis(200)).await;
    });

    let (compiled, shared) = limits();

    let connection = WebSocketConnector::connect(
        &format!("ws://127.0.0.1:{}", address.port()),
        102,
        &compiled,
        &shared,
        Duration::from_secs(2),
    )
    .await
    .unwrap();

    // Capacity one; deliberately do not consume.
    let (inbound_tx, _inbound_rx) = mpsc::channel(1);
    let (_outbound_tx, outbound_rx) = mpsc::channel(8);
    let (_shutdown_tx, shutdown_rx) = oneshot::channel();

    let result = time::timeout(
        Duration::from_secs(2),
        WebSocketSupervisor::run(
            connection,
            inbound_tx,
            outbound_rx,
            shutdown_rx,
            heartbeat(),
        ),
    )
    .await
    .unwrap();

    assert!(matches!(
        result,
        Err(WebSocketTransportError::InboundBackpressure),
    ));

    server.await.unwrap();
}

#[tokio::test]
async fn outbound_queue_uses_connection_limiter() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

    let address = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();

        let mut received = false;

        while let Some(Ok(message)) = socket.next().await {
            match message {
                Message::Text(text) if text == "subscribe" => {
                    received = true;
                }

                Message::Ping(payload) => {
                    socket.send(Message::Pong(payload)).await.unwrap();
                }

                Message::Close(_) => break,

                _ => {}
            }
        }

        received
    });

    let (compiled, shared) = limits();

    let connection = WebSocketConnector::connect(
        &format!("ws://127.0.0.1:{}", address.port()),
        103,
        &compiled,
        &shared,
        Duration::from_secs(2),
    )
    .await
    .unwrap();

    let (inbound_tx, _inbound_rx) = mpsc::channel(8);
    let (outbound_tx, outbound_rx) = mpsc::channel(8);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let supervisor = tokio::spawn(WebSocketSupervisor::run(
        connection,
        inbound_tx,
        outbound_rx,
        shutdown_rx,
        heartbeat(),
    ));

    outbound_tx
        .send(OutboundMessage::Subscription("subscribe".into()))
        .await
        .unwrap();

    // The server must first acknowledge the message
    // before we request shutdown.
    time::sleep(Duration::from_millis(50)).await;

    shutdown_tx.send(()).unwrap();

    assert_eq!(supervisor.await.unwrap().unwrap(), SupervisorExit::Shutdown,);

    assert!(server.await.unwrap());
}
