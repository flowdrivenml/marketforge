use std::time::Duration;

use serde_json::json;

use tokio::{
    sync::{mpsc, oneshot},
    time,
};

use tokio_tungstenite::tungstenite::Message;

use marketforge_engine::live::{
    config::exchange::ExchangeConfig,
    infrastructure::websocket::{
        CompiledSubscriptionAck, ConnectionFence, ExpectedSubscriptionAck, OutboundMessage,
        RecoveryStreamKind, StreamRecoveryCoordinator, SubscriptionAckCompiler,
        SubscriptionCommand, SubscriptionDriver, SubscriptionDriverEvent, SubscriptionDriverLimits,
        SubscriptionSession,
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
    SubscriptionSession,
    StreamRecoveryCoordinator,
    Vec<marketforge_engine::live::infrastructure::websocket::StreamRecoveryToken>,
) {
    let fence = ConnectionFence::new("runtime-1", "slot-1").unwrap();

    let stamp = fence.begin_connect().unwrap();
    fence.connected(&stamp).unwrap();

    let mut recovery = StreamRecoveryCoordinator::new();

    let mut tokens = Vec::new();

    for stream in ["BTCUSDT:trades", "ETHUSDT:trades"] {
        recovery
            .register(stream, RecoveryStreamKind::Trades)
            .unwrap();

        tokens.push(recovery.begin_binding(&fence, &stamp, stream).unwrap());
    }

    let session = SubscriptionSession::new(fence, stamp, 8, Duration::from_secs(2)).unwrap();

    (session, recovery, tokens)
}

fn limits() -> SubscriptionDriverLimits {
    SubscriptionDriverLimits {
        queue_timeout: Duration::from_secs(1),
        receipt_timeout: Duration::from_secs(1),
        maximum_early_frames: 64,
    }
}

#[tokio::test]
async fn driver_correlates_two_outstanding_subscriptions() {
    let (session, recovery, tokens) = setup();

    let (command_tx, command_rx) = mpsc::channel(8);
    let (outbound_tx, mut outbound_rx) = mpsc::channel(8);
    let (inbound_tx, inbound_rx) = mpsc::channel(32);
    let (unrelated_tx, mut unrelated_rx) = mpsc::channel(32);
    let (completion_tx, mut completion_rx) = mpsc::channel(8);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let driver = tokio::spawn(async move {
        SubscriptionDriver::run(
            session,
            recovery,
            protocol(),
            command_rx,
            outbound_tx,
            inbound_rx,
            unrelated_tx,
            completion_tx,
            shutdown_rx,
            limits(),
        )
        .await
    });

    command_tx
        .send(SubscriptionCommand {
            token: tokens[0].clone(),
            expected: ExpectedSubscriptionAck::subscribe(Some(json!(10))),
            wire_text: json!({
                "method": "SUBSCRIBE",
                "params": ["btcusdt@trade"],
                "id": 10
            })
            .to_string(),
        })
        .await
        .unwrap();

    // First request is transmitted.
    let first = time::timeout(Duration::from_secs(2), outbound_rx.recv())
        .await
        .unwrap()
        .unwrap();

    let OutboundMessage::SubscriptionTracked {
        receipt: first_receipt,
        ..
    } = first
    else {
        panic!("expected tracked subscription");
    };

    first_receipt.sent();

    command_tx
        .send(SubscriptionCommand {
            token: tokens[1].clone(),
            expected: ExpectedSubscriptionAck::subscribe(Some(json!(11))),
            wire_text: json!({
                "method": "SUBSCRIBE",
                "params": ["ethusdt@trade"],
                "id": 11
            })
            .to_string(),
        })
        .await
        .unwrap();

    // The second request can be transmitted before
    // receiving the first request's exchange ACK.
    let second = time::timeout(Duration::from_secs(2), outbound_rx.recv())
        .await
        .unwrap()
        .unwrap();

    let OutboundMessage::SubscriptionTracked {
        receipt: second_receipt,
        ..
    } = second
    else {
        panic!("expected tracked subscription");
    };

    second_receipt.sent();

    // Unrelated market data must be preserved.
    inbound_tx
        .send(Message::Text(
            json!({
                "e": "trade",
                "s": "BTCUSDT",
                "p": "80000"
            })
            .to_string()
            .into(),
        ))
        .await
        .unwrap();

    // ACKs arrive in reverse request order.
    inbound_tx
        .send(Message::Text(
            json!({"id": 11, "result": null}).to_string().into(),
        ))
        .await
        .unwrap();

    inbound_tx
        .send(Message::Text(
            json!({"id": 10, "result": null}).to_string().into(),
        ))
        .await
        .unwrap();

    let first_completion = time::timeout(Duration::from_secs(2), completion_rx.recv())
        .await
        .unwrap()
        .unwrap();

    let second_completion = time::timeout(Duration::from_secs(2), completion_rx.recv())
        .await
        .unwrap()
        .unwrap();

    let mut confirmed_streams = Vec::new();

    for completion in [first_completion, second_completion] {
        let SubscriptionDriverEvent::Confirmed { token, .. } = completion else {
            panic!("expected successful subscription");
        };

        confirmed_streams.push(token.stream_id().to_owned());
    }

    confirmed_streams.sort();

    assert_eq!(
        confirmed_streams,
        vec!["BTCUSDT:trades".to_owned(), "ETHUSDT:trades".to_owned(),],
    );

    let unrelated = unrelated_rx.try_recv().unwrap();

    let Message::Text(text) = unrelated else {
        panic!("market-data frame was lost");
    };

    assert!(text.contains("\"trade\""));

    shutdown_tx.send(()).unwrap();

    time::timeout(Duration::from_secs(3), driver)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn driver_preserves_ack_before_send_receipt() {
    let (session, recovery, tokens) = setup();

    let (command_tx, command_rx) = mpsc::channel(8);
    let (outbound_tx, mut outbound_rx) = mpsc::channel(8);
    let (inbound_tx, inbound_rx) = mpsc::channel(32);
    let (unrelated_tx, _unrelated_rx) = mpsc::channel(32);
    let (completion_tx, mut completion_rx) = mpsc::channel(8);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let driver = tokio::spawn(async move {
        SubscriptionDriver::run(
            session,
            recovery,
            protocol(),
            command_rx,
            outbound_tx,
            inbound_rx,
            unrelated_tx,
            completion_tx,
            shutdown_rx,
            limits(),
        )
        .await
    });

    command_tx
        .send(SubscriptionCommand {
            token: tokens[0].clone(),
            expected: ExpectedSubscriptionAck::subscribe(Some(json!(7))),
            wire_text: json!({
                "method": "SUBSCRIBE",
                "params": ["btcusdt@trade"],
                "id": 7
            })
            .to_string(),
        })
        .await
        .unwrap();

    let outgoing = time::timeout(Duration::from_secs(2), outbound_rx.recv())
        .await
        .unwrap()
        .unwrap();

    let OutboundMessage::SubscriptionTracked { receipt, .. } = outgoing else {
        panic!("expected tracked subscription");
    };

    // Deliver ACK before completing the send receipt.
    inbound_tx
        .send(Message::Text(
            json!({"id": 7, "result": null}).to_string().into(),
        ))
        .await
        .unwrap();

    receipt.sent();

    let completed = time::timeout(Duration::from_secs(2), completion_rx.recv())
        .await
        .unwrap()
        .unwrap();

    assert!(matches!(
        completed,
        SubscriptionDriverEvent::Confirmed { .. }
    ));

    shutdown_tx.send(()).unwrap();

    time::timeout(Duration::from_secs(3), driver)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}
