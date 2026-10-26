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
            CompiledHeartbeatPlan, HeartbeatPlanCompiler, SupervisorExit, WebSocketConnector,
            WebSocketSupervisor,
        },
    },
};

// ============================================================
// FIXTURES
// ============================================================

fn load(profile: &str) -> ExchangeConfig {
    let path = crate::support::data(".config/live/exchanges").join(format!("{profile}.toml"));

    ExchangeConfig::load(path).unwrap()
}

fn limits(config: &ExchangeConfig) -> (CompiledWebSocketLimiters, RateLimitManager) {
    let bindings = WebSocketLimiterBindings {
        connection_attempts: Some(WebSocketBudgetBinding::new(
            "heartbeat-execution-test",
            BudgetScope::Egress("local".into()),
        )),
        connection_attempts_per_second: Some(WebSocketBudgetBinding::new(
            "heartbeat-execution-test",
            BudgetScope::Egress("local".into()),
        )),
        connection_service: Some("heartbeat-execution-test".into()),
    };

    let compiled = WebSocketLimiterCompiler::compile(config, &bindings).unwrap();

    let shared = RateLimitManager::new(compiled.shared.policies.clone(), 32).unwrap();

    (compiled, shared)
}

async fn connect(
    address: std::net::SocketAddr,
    config: &ExchangeConfig,
    connection_id: u64,
) -> marketforge_engine::live::infrastructure::websocket::WebSocketConnection {
    let (compiled, shared) = limits(config);

    WebSocketConnector::connect(
        &format!("ws://127.0.0.1:{}", address.port()),
        connection_id,
        &compiled,
        &shared,
        Duration::from_secs(2),
    )
    .await
    .unwrap()
}

fn plan(config: &ExchangeConfig) -> CompiledHeartbeatPlan {
    HeartbeatPlanCompiler::compile(config).unwrap()
}

// ============================================================
// SERVER-INITIATED CONTROL PING
// ============================================================

#[tokio::test]
async fn server_ping_receives_echoed_pong() {
    let listener = crate::websocket_server::listen().await;

    let address = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();

        let token = b"marketforge-ping".to_vec();

        socket
            .send(Message::Ping(token.clone().into()))
            .await
            .unwrap();

        let response = time::timeout(Duration::from_secs(2), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        match response {
            Message::Pong(payload) => {
                assert_eq!(payload.as_ref(), token.as_slice());
            }

            other => panic!("expected Pong, got {other:?}"),
        }
    });

    let config = load("binance_spot");

    let connection = connect(address, &config, 301).await;

    let (inbound_tx, _inbound_rx) = mpsc::channel(8);
    let (_outbound_tx, outbound_rx) = mpsc::channel(8);
    let (_shutdown_tx, shutdown_rx) = oneshot::channel();

    let result = time::timeout(
        Duration::from_secs(2),
        WebSocketSupervisor::run_with_plan(
            connection,
            inbound_tx,
            outbound_rx,
            shutdown_rx,
            plan(&config),
        ),
    )
    .await
    .unwrap();

    assert!(matches!(
        result,
        Ok(SupervisorExit::PeerClosed)
            | Err(
                marketforge_engine::live::infrastructure::
                    websocket::WebSocketTransportError::Protocol(_)
            )
    ));

    server.await.unwrap();
}

// ============================================================
// PERIODIC JSON APPLICATION HEARTBEAT
// ============================================================

#[tokio::test]
async fn periodic_json_heartbeat_executes() {
    let listener = crate::websocket_server::listen().await;

    let address = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();

        let message = time::timeout(Duration::from_secs(2), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        assert_eq!(message.into_text().unwrap().as_str(), r#"{"op":"ping"}"#,);

        socket
            .send(Message::Text(
                r#"{"op":"ping","success":true,"ret_msg":"pong"}"#.into(),
            ))
            .await
            .unwrap();

        // Unrelated market data must not be treated as
        // a heartbeat response.
        socket
            .send(Message::Text(
                r#"{"topic":"trades","price":"80000"}"#.into(),
            ))
            .await
            .unwrap();

        let _ = socket.next().await;
    });

    let mut config = load("bybit_spot");

    let heartbeat = config.websocket.heartbeat.as_mut().unwrap();

    heartbeat.interval_seconds = Some(1);
    heartbeat.timeout_seconds = Some(2);

    let connection = connect(address, &config, 302).await;

    let (inbound_tx, mut inbound_rx) = mpsc::channel(8);
    let (_outbound_tx, outbound_rx) = mpsc::channel(8);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let supervisor = tokio::spawn(WebSocketSupervisor::run_with_plan(
        connection,
        inbound_tx,
        outbound_rx,
        shutdown_rx,
        plan(&config),
    ));

    let message = time::timeout(Duration::from_secs(3), inbound_rx.recv())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        message.into_text().unwrap().as_str(),
        r#"{"topic":"trades","price":"80000"}"#,
    );

    shutdown_tx.send(()).unwrap();

    assert_eq!(supervisor.await.unwrap().unwrap(), SupervisorExit::Shutdown,);

    server.await.unwrap();
}

// ============================================================
// INACTIVITY-TRIGGERED TEXT HEARTBEAT
// ============================================================

#[tokio::test]
async fn inactivity_timer_resets_on_market_data() {
    let listener = crate::websocket_server::listen().await;

    let address = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();

        // Emit unrelated incoming traffic before the
        // inactivity deadline.
        socket
            .send(Message::Text(r#"{"event":"market-data"}"#.into()))
            .await
            .unwrap();

        time::sleep(Duration::from_millis(650)).await;

        socket
            .send(Message::Text(r#"{"event":"market-data-2"}"#.into()))
            .await
            .unwrap();

        // No heartbeat should have been sent yet,
        // because incoming traffic reset the timer.
        assert!(
            time::timeout(Duration::from_millis(300), socket.next(),)
                .await
                .is_err(),
        );

        // After another inactivity period, the
        // client should send literal text "ping".
        let ping = time::timeout(Duration::from_secs(2), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        assert_eq!(ping.into_text().unwrap().as_str(), "ping",);

        socket.send(Message::Text("pong".into())).await.unwrap();

        let _ = socket.next().await;
    });

    let mut config = load("okx_spot");

    let heartbeat = config.websocket.heartbeat.as_mut().unwrap();

    heartbeat.inactivity_seconds = Some(1);
    heartbeat.response_timeout_seconds = Some(2);

    let connection = connect(address, &config, 303).await;

    let (inbound_tx, mut inbound_rx) = mpsc::channel(8);
    let (_outbound_tx, outbound_rx) = mpsc::channel(8);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let supervisor = tokio::spawn(WebSocketSupervisor::run_with_plan(
        connection,
        inbound_tx,
        outbound_rx,
        shutdown_rx,
        plan(&config),
    ));

    for _ in 0..2 {
        time::timeout(Duration::from_secs(2), inbound_rx.recv())
            .await
            .unwrap()
            .unwrap();
    }

    // Keep the supervisor running long enough to process
    // the delayed heartbeat and its response.
    time::sleep(Duration::from_secs(2)).await;

    shutdown_tx.send(()).unwrap();

    assert_eq!(supervisor.await.unwrap().unwrap(), SupervisorExit::Shutdown,);

    server.await.unwrap();
}
