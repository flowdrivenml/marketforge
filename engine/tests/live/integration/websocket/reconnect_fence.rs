use std::time::Duration;

use futures_util::StreamExt;

use tokio::{
    sync::{mpsc, oneshot},
    time,
};

use tokio_tungstenite::accept_async;

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
            ConnectionFence, ConnectionState, HeartbeatPlanCompiler, ReconnectEvent,
            ReconnectPolicy, ReconnectRunner, WebSocketSupervisor,
        },
    },
};

fn infrastructure() -> (
    CompiledWebSocketLimiters,
    RateLimitManager,
    marketforge_engine::live::infrastructure::websocket::CompiledHeartbeatPlan,
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

    let compiled =
        WebSocketLimiterCompiler::compile(&config, &WebSocketLimiterBindings::default()).unwrap();

    let shared = RateLimitManager::new(compiled.shared.policies.clone(), 32).unwrap();

    let heartbeat = HeartbeatPlanCompiler::compile(&config).unwrap();

    (compiled, shared, heartbeat)
}

fn retry_policy() -> ReconnectPolicy {
    ReconnectPolicy::new(
        Duration::from_millis(20),
        Duration::from_millis(100),
        Duration::from_secs(10),
        Duration::from_secs(2),
        None,
    )
    .unwrap()
}

#[tokio::test]
async fn reconnect_runner_revokes_previous_generation() {
    let listener = crate::websocket_server::listen().await;

    let address = listener.local_addr().unwrap();

    let (close_first_tx, close_first_rx) = oneshot::channel::<()>();

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let first = accept_async(stream).await.unwrap();

        // Keep the first connection alive until the test
        // has verified that it is explicitly Ready.
        close_first_rx.await.unwrap();
        drop(first);

        // Accept the replacement connection.
        let (stream, _) = listener.accept().await.unwrap();
        let mut second = accept_async(stream).await.unwrap();

        while second.next().await.is_some() {}
    });

    let (compiled, shared, heartbeat) = infrastructure();

    let fence = ConnectionFence::new("test-runtime", "slot-1").unwrap();

    let consumer = fence.clone();
    let session_fence = fence.clone();

    let (ready_tx, mut ready_rx) = mpsc::channel(4);
    let (events_tx, mut events_rx) = mpsc::channel(32);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let runner = tokio::spawn(async move {
        ReconnectRunner::run_with_fence(
            &format!("ws://127.0.0.1:{}", address.port()),
            &compiled,
            &shared,
            retry_policy(),
            shutdown_rx,
            events_tx,
            fence,
            move |stamp, connection| {
                let fence = session_fence.clone();
                let heartbeat = heartbeat.clone();
                let ready_tx = ready_tx.clone();

                async move {
                    // Test session initializer explicitly
                    // grants readiness after handshake.
                    //
                    // Production readiness will come from
                    // subscription reconciliation.
                    fence.ready(&stamp).unwrap();

                    ready_tx.send(stamp).await.unwrap();

                    let (inbound_tx, _inbound_rx) = mpsc::channel(8);

                    let (_outbound_tx, outbound_rx) = mpsc::channel(8);

                    let (_session_shutdown_tx, session_shutdown_rx) = oneshot::channel();

                    let result = WebSocketSupervisor::run_with_plan(
                        connection,
                        inbound_tx,
                        outbound_rx,
                        session_shutdown_rx,
                        heartbeat,
                    )
                    .await;

                    result
                }
            },
        )
        .await
    });

    // First generation becomes Ready.
    let first = time::timeout(Duration::from_secs(3), ready_rx.recv())
        .await
        .unwrap()
        .unwrap();

    assert!(consumer.accepts(&first));

    // Terminate first physical connection.
    close_first_tx.send(()).unwrap();

    // Observe revocation before accepting replacement.
    time::timeout(Duration::from_secs(3), async {
        loop {
            match events_rx.recv().await.unwrap() {
                ReconnectEvent::Disconnected { generation, .. }
                    if generation == first.generation() =>
                {
                    break;
                }

                _ => {}
            }
        }
    })
    .await
    .unwrap();

    assert!(
        !consumer.accepts(&first),
        "failed generation retained publication authority",
    );

    // Replacement generation becomes Ready.
    let second = time::timeout(Duration::from_secs(3), ready_rx.recv())
        .await
        .unwrap()
        .unwrap();

    assert_ne!(first, second);
    assert!(consumer.accepts(&second));
    assert!(!consumer.accepts(&first));

    shutdown_tx.send(()).unwrap();

    time::timeout(Duration::from_secs(3), runner)
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    assert_eq!(consumer.state(), ConnectionState::Stopped);
    assert!(!consumer.accepts(&second));

    time::timeout(Duration::from_secs(3), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn aborting_runner_revokes_shared_authority() {
    let listener = crate::websocket_server::listen().await;

    let address = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();

        while socket.next().await.is_some() {}
    });

    let (compiled, shared, heartbeat) = infrastructure();

    let fence = ConnectionFence::new("test-runtime", "slot-2").unwrap();

    let consumer = fence.clone();
    let session_fence = fence.clone();

    let (ready_tx, ready_rx) = oneshot::channel();
    let mut ready_tx = Some(ready_tx);

    let (events_tx, _events_rx) = mpsc::channel(32);
    let (_shutdown_tx, shutdown_rx) = oneshot::channel();

    let runner = tokio::spawn(async move {
        ReconnectRunner::run_with_fence(
            &format!("ws://127.0.0.1:{}", address.port()),
            &compiled,
            &shared,
            retry_policy(),
            shutdown_rx,
            events_tx,
            fence,
            move |stamp, connection| {
                let fence = session_fence.clone();
                let heartbeat = heartbeat.clone();
                let notify = ready_tx.take();

                async move {
                    fence.ready(&stamp).unwrap();

                    if let Some(notify) = notify {
                        let _ = notify.send(stamp);
                    }

                    let (inbound_tx, _inbound_rx) = mpsc::channel(8);

                    let (_outbound_tx, outbound_rx) = mpsc::channel(8);

                    let (_session_shutdown_tx, session_shutdown_rx) = oneshot::channel();

                    WebSocketSupervisor::run_with_plan(
                        connection,
                        inbound_tx,
                        outbound_rx,
                        session_shutdown_rx,
                        heartbeat,
                    )
                    .await
                }
            },
        )
        .await
    });

    let stamp = time::timeout(Duration::from_secs(3), ready_rx)
        .await
        .unwrap()
        .unwrap();

    assert!(consumer.accepts(&stamp));

    // Simulate cancellation by the parent task.
    runner.abort();

    let result = runner.await;

    assert!(result.unwrap_err().is_cancelled());

    // The Drop guard must revoke the generation.
    assert_eq!(consumer.state(), ConnectionState::Stopped);
    assert!(!consumer.accepts(&stamp));

    time::timeout(Duration::from_secs(3), server)
        .await
        .unwrap()
        .unwrap();
}
