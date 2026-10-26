use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::json;

use tokio::{
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
            ConnectionFence, ExpectedSubscriptionAck, HeartbeatPlanCompiler, OutboundMessage,
            SubscriptionAckCompiler, SubscriptionCompletionKind, SubscriptionDispatch,
            SubscriptionPendingManager, WebSocketConnector, WebSocketSupervisor,
            subscription_send_receipt,
        },
    },
};

// ============================================================
// TEST INFRASTRUCTURE
// ============================================================

fn infrastructure() -> (
    CompiledWebSocketLimiters,
    RateLimitManager,
    marketforge_engine::live::infrastructure::websocket::CompiledHeartbeatPlan,
    marketforge_engine::live::infrastructure::websocket::CompiledSubscriptionAck,
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

[websocket.subscription]
strategy = "json_rpc"
subscribe_method = "SUBSCRIBE"
request_id_field = "id"
request_id_type = "unsigned_integer"
success_field = "result"
success_is_null = true
error_field = "error"
error_code_field = "error.code"
error_message_field = "error.msg"

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

    let ack = SubscriptionAckCompiler::compile(&config).unwrap();

    (compiled, shared, heartbeat, ack)
}

// ============================================================
// REAL SOCKET SEND + ACK
// ============================================================

#[tokio::test]
async fn tracked_subscription_receipt_follows_real_send() {
    let listener = crate::websocket_server::listen().await;

    let address = listener.local_addr().unwrap();

    // --------------------------------------------------------
    // TEST EXCHANGE
    // --------------------------------------------------------

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();

        let mut socket = accept_async(stream).await.unwrap();

        let incoming = time::timeout(Duration::from_secs(3), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        let Message::Text(text) = incoming else {
            panic!("expected subscription text frame");
        };

        let request: serde_json::Value = serde_json::from_str(text.as_str()).unwrap();

        assert_eq!(
            request,
            json!({
                "method": "SUBSCRIBE",
                "params": ["btcusdt@trade"],
                "id": 7
            }),
        );

        socket
            .send(Message::Text(
                json!({
                    "id": 7,
                    "result": null
                })
                .to_string()
                .into(),
            ))
            .await
            .unwrap();

        // Keep the test connection open until shutdown.
        while let Some(frame) = socket.next().await {
            if frame.is_err() {
                break;
            }

            if matches!(frame, Ok(Message::Close(_))) {
                break;
            }
        }
    });

    // --------------------------------------------------------
    // CONNECTION
    // --------------------------------------------------------

    let (compiled, shared, heartbeat, ack) = infrastructure();

    let connection = WebSocketConnector::connect(
        &format!("ws://127.0.0.1:{}", address.port()),
        101,
        &compiled,
        &shared,
        Duration::from_secs(2),
    )
    .await
    .unwrap();

    let fence = ConnectionFence::new("test-runtime", "subscription-wire-slot").unwrap();

    let stamp = fence.begin_connect().unwrap();

    fence.connected(&stamp).unwrap();

    // --------------------------------------------------------
    // PENDING TRANSACTION
    // --------------------------------------------------------

    let mut pending = SubscriptionPendingManager::new(8, Duration::from_secs(2)).unwrap();

    let id = pending
        .reserve(
            &fence,
            &stamp,
            "BTCUSDT:trades",
            ExpectedSubscriptionAck::subscribe(Some(json!(7))),
        )
        .unwrap();

    let (receipt_sender, receipt_receiver) = subscription_send_receipt(id, stamp.clone());

    assert_eq!(pending.next_deadline(), None);

    // --------------------------------------------------------
    // START SOCKET SUPERVISOR
    // --------------------------------------------------------

    let (inbound_tx, mut inbound_rx) = mpsc::channel(16);

    let (outbound_tx, outbound_rx) = mpsc::channel(16);

    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let supervisor = tokio::spawn(async move {
        WebSocketSupervisor::run_with_plan(
            connection,
            inbound_tx,
            outbound_rx,
            shutdown_rx,
            heartbeat,
        )
        .await
    });

    // --------------------------------------------------------
    // QUEUE TRACKED SUBSCRIPTION
    // --------------------------------------------------------

    outbound_tx
        .send(OutboundMessage::SubscriptionTracked {
            text: json!({
                "method": "SUBSCRIBE",
                "params": ["btcusdt@trade"],
                "id": 7
            })
            .to_string(),

            receipt: receipt_sender,
        })
        .await
        .unwrap();

    // Merely queueing the message must not start
    // the exchange ACK deadline.
    assert_eq!(pending.next_deadline(), None);

    // --------------------------------------------------------
    // WAIT FOR REAL TRANSMISSION
    // --------------------------------------------------------

    let receipt = receipt_receiver.wait(Duration::from_secs(3)).await.unwrap();

    assert_eq!(receipt.transaction_id, id);
    assert_eq!(receipt.stamp, stamp);

    pending
        .mark_sent(
            &fence,
            &receipt.stamp,
            receipt.transaction_id,
            receipt.sent_at,
        )
        .unwrap();

    assert!(pending.next_deadline().is_some());

    // --------------------------------------------------------
    // PROCESS EXCHANGE ACK
    // --------------------------------------------------------

    let frame = time::timeout(Duration::from_secs(3), inbound_rx.recv())
        .await
        .unwrap()
        .unwrap();

    let Message::Text(text) = frame else {
        panic!("expected exchange ACK");
    };

    let native: serde_json::Value = serde_json::from_str(text.as_str()).unwrap();

    let dispatch = pending
        .classify(&fence, &stamp, &ack, &native, time::Instant::now())
        .unwrap();

    let SubscriptionDispatch::Completed(completion) = dispatch else {
        panic!("matching exchange ACK was not consumed");
    };

    assert_eq!(completion.transaction_id, id);

    assert!(matches!(
        completion.kind,
        SubscriptionCompletionKind::Confirmed,
    ));

    assert_eq!(pending.pending_count(), 0);

    // --------------------------------------------------------
    // SHUTDOWN
    // --------------------------------------------------------

    shutdown_tx.send(()).unwrap();

    time::timeout(Duration::from_secs(3), supervisor)
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    time::timeout(Duration::from_secs(3), server)
        .await
        .unwrap()
        .unwrap();
}
