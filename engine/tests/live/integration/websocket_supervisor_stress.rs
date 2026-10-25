use std::{path::PathBuf, time::Duration};

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
            RateLimitManager,
            factory::{
                CompiledWebSocketLimiters, WebSocketLimiterBindings, WebSocketLimiterCompiler,
            },
        },
        websocket::{
            HeartbeatPlanCompiler, OutboundMessage, SupervisorExit, WebSocketConnector,
            WebSocketSupervisor,
        },
    },
};

// ============================================================
// TEST CONFIGURATION
// ============================================================

fn configuration() -> ExchangeConfig {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../data/.config/live/exchanges/gateio_spot.toml");

    let mut config = ExchangeConfig::load(path).unwrap();

    // Test-only outbound quota: two messages per second.
    //
    // The production exchange configuration is unchanged.
    config.websocket.max_client_messages_per_second = Some(2);

    config.websocket.limits = None;

    config
}

fn infrastructure(config: &ExchangeConfig) -> (CompiledWebSocketLimiters, RateLimitManager) {
    let compiled =
        WebSocketLimiterCompiler::compile(config, &WebSocketLimiterBindings::default()).unwrap();

    let manager = RateLimitManager::new(compiled.shared.policies.clone(), 32).unwrap();

    (compiled, manager)
}

// ============================================================
// SOCKET READING DURING OUTBOUND ADMISSION
// ============================================================

#[tokio::test]
async fn w26_inbound_processing_continues_while_limiter_waits() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

    let address = listener.local_addr().unwrap();

    let (server_ready_tx, server_ready_rx) = oneshot::channel();

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();

        let mut socket = accept_async(stream).await.unwrap();

        // Consume the two permitted outbound messages.
        for expected in ["first", "second"] {
            let message = time::timeout(Duration::from_secs(2), socket.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();

            assert_eq!(message.into_text().unwrap().as_str(), expected,);
        }

        server_ready_tx.send(()).unwrap();

        // The client will be waiting for quota to send
        // "third". Deliver market data during that wait.
        socket
            .send(Message::Text(
                r#"{"type":"market-data","sequence":1}"#.into(),
            ))
            .await
            .unwrap();

        // Keep the connection open until client shutdown.
        while let Some(message) = socket.next().await {
            match message {
                Ok(Message::Close(_)) | Err(_) => break,
                _ => {}
            }
        }
    });

    let config = configuration();
    let plan = HeartbeatPlanCompiler::compile(&config).unwrap();

    let (compiled, shared) = infrastructure(&config);

    let connection = WebSocketConnector::connect(
        &format!("ws://127.0.0.1:{}", address.port()),
        601,
        &compiled,
        &shared,
        Duration::from_secs(2),
    )
    .await
    .unwrap();

    let (inbound_tx, mut inbound_rx) = mpsc::channel(8);

    let (outbound_tx, outbound_rx) = mpsc::channel(8);

    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let supervisor = tokio::spawn(WebSocketSupervisor::run_with_plan(
        connection,
        inbound_tx,
        outbound_rx,
        shutdown_rx,
        plan,
    ));

    // Fill the connection's outbound budget.
    outbound_tx
        .send(OutboundMessage::Text("first".into()))
        .await
        .unwrap();

    outbound_tx
        .send(OutboundMessage::Text("second".into()))
        .await
        .unwrap();

    server_ready_rx.await.unwrap();

    // Third message cannot immediately pass admission.
    outbound_tx
        .send(OutboundMessage::Text("third".into()))
        .await
        .unwrap();

    // Market data must remain readable independently of
    // the outbound quota wait.
    let received = time::timeout(Duration::from_millis(500), inbound_rx.recv())
        .await
        .expect("incoming market data was starved by limiter")
        .expect("inbound channel closed unexpectedly");

    assert_eq!(
        received.into_text().unwrap().as_str(),
        r#"{"type":"market-data","sequence":1}"#,
    );

    // Cancellation must also work during limiter admission.
    shutdown_tx.send(()).unwrap();

    let exit = time::timeout(Duration::from_secs(2), supervisor)
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    assert_eq!(exit, SupervisorExit::Shutdown);

    server.await.unwrap();
}
