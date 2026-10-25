use std::time::Duration;

use serde_json::json;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

use marketforge_engine::live::{
    config::exchange::ExchangeConfig,
    infrastructure::websocket::{
        CompiledSubscriptionAck, ConnectionFence, ExpectedSubscriptionAck, OutboundMessage,
        RecoveryStreamKind, StreamRecoveryCoordinator, StreamRecoveryPhase,
        SubscriptionAckCompiler, SubscriptionCompletionKind, SubscriptionExecutor,
        SubscriptionSession, SubscriptionSessionEvent,
    },
};

fn protocol() -> CompiledSubscriptionAck {
    let config: ExchangeConfig = toml::from_str(
        r#"
schema_version = 1

[exchange]
id = "binance"
market = "spot"
profile = "binance_spot"

[websocket]
url = "wss://example.invalid"

[websocket.subscription]
strategy = "json_rpc"
request_id_field = "id"
success_field = "result"
success_is_null = true
error_field = "error"

[channels.trades]
enabled = true
format = "trades/binance_spot"

[rest]
base_url = "https://example.invalid"
"#,
    )
    .unwrap();

    SubscriptionAckCompiler::compile(&config).unwrap()
}

fn setup() -> (
    ConnectionFence,
    StreamRecoveryCoordinator,
    SubscriptionSession,
    marketforge_engine::live::infrastructure::websocket::StreamRecoveryToken,
) {
    let fence = ConnectionFence::new("runtime-1", "slot-1").unwrap();

    let stamp = fence.begin_connect().unwrap();
    fence.connected(&stamp).unwrap();

    let mut recovery = StreamRecoveryCoordinator::new();

    recovery
        .register("BTCUSDT:trades", RecoveryStreamKind::Trades)
        .unwrap();

    let token = recovery
        .begin_binding(&fence, &stamp, "BTCUSDT:trades")
        .unwrap();

    let session =
        SubscriptionSession::new(fence.clone(), stamp, 8, Duration::from_millis(200)).unwrap();

    (fence, recovery, session, token)
}

#[tokio::test]
async fn w43_ack_before_receipt_is_preserved() {
    let (_fence, mut recovery, mut session, token) = setup();

    let (outbound_tx, mut outbound_rx) = mpsc::channel(8);

    let (inbound_tx, mut inbound_rx) = mpsc::channel(8);

    let (unrelated_tx, mut unrelated_rx) = mpsc::channel(8);

    let exchange = tokio::spawn(async move {
        let outgoing = outbound_rx.recv().await.unwrap();

        let OutboundMessage::SubscriptionTracked { receipt, .. } = outgoing else {
            panic!("expected tracked subscription");
        };

        // The exchange's market-data response and ACK
        // are delivered before the receipt is processed.
        inbound_tx
            .send(Message::Text(
                json!({
                    "topic": "publicTrade.BTCUSDT",
                    "data": []
                })
                .to_string()
                .into(),
            ))
            .await
            .unwrap();

        inbound_tx
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

        // Only now report completed socket transmission.
        receipt.sent();
    });

    let result = SubscriptionExecutor::execute_one(
        &mut session,
        &mut recovery,
        &protocol(),
        &token,
        ExpectedSubscriptionAck::subscribe(Some(json!(7))),
        json!({
            "method": "SUBSCRIBE",
            "params": ["btcusdt@trade"],
            "id": 7
        })
        .to_string(),
        &outbound_tx,
        &mut inbound_rx,
        &unrelated_tx,
        Duration::from_secs(1),
        Duration::from_secs(1),
    )
    .await
    .unwrap();

    assert!(matches!(
        result.event,
        SubscriptionSessionEvent::Confirmed { .. }
    ));

    assert_eq!(
        recovery.phase("BTCUSDT:trades"),
        Some(StreamRecoveryPhase::AwaitingRecovery),
    );

    let unrelated = unrelated_rx.try_recv().unwrap();

    let Message::Text(text) = unrelated else {
        panic!("market data was not forwarded");
    };

    assert!(text.contains("publicTrade.BTCUSDT"));

    exchange.await.unwrap();
}

#[tokio::test]
async fn w43_subscription_timeout_does_not_grant_recovery() {
    let (_fence, mut recovery, mut session, token) = setup();

    let (outbound_tx, mut outbound_rx) = mpsc::channel(8);

    let (_inbound_tx, mut inbound_rx) = mpsc::channel(8);

    let (unrelated_tx, _unrelated_rx) = mpsc::channel(8);

    let exchange = tokio::spawn(async move {
        let outgoing = outbound_rx.recv().await.unwrap();

        let OutboundMessage::SubscriptionTracked { receipt, .. } = outgoing else {
            panic!("expected tracked subscription");
        };

        receipt.sent();

        // Intentionally never send an exchange ACK.
    });

    let result = SubscriptionExecutor::execute_one(
        &mut session,
        &mut recovery,
        &protocol(),
        &token,
        ExpectedSubscriptionAck::subscribe(Some(json!(8))),
        json!({
            "method": "SUBSCRIBE",
            "params": ["btcusdt@trade"],
            "id": 8
        })
        .to_string(),
        &outbound_tx,
        &mut inbound_rx,
        &unrelated_tx,
        Duration::from_secs(1),
        Duration::from_secs(1),
    )
    .await
    .unwrap();

    let SubscriptionSessionEvent::Failed { completion } = result.event else {
        panic!("expected subscription timeout");
    };

    assert!(matches!(
        completion.kind,
        SubscriptionCompletionKind::TimedOut
    ));

    assert_eq!(
        recovery.phase("BTCUSDT:trades"),
        Some(StreamRecoveryPhase::AwaitingSubscription),
    );

    assert_eq!(session.pending_count(), 0);

    exchange.await.unwrap();
}
