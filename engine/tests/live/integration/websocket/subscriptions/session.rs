use std::time::Duration;

use serde_json::json;
use tokio::time::Instant;

use marketforge_engine::live::{
    config::exchange::ExchangeConfig,
    infrastructure::websocket::{
        CompiledSubscriptionAck, ConnectionFence, ExpectedSubscriptionAck, RecoveryStreamKind,
        StreamRecoveryCoordinator, StreamRecoveryPhase, SubscriptionAckCompiler,
        SubscriptionCompletionKind, SubscriptionSession, SubscriptionSessionError,
        SubscriptionSessionEvent, subscription_send_receipt,
    },
};

fn ack_protocol() -> CompiledSubscriptionAck {
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
    marketforge_engine::live::infrastructure::websocket::ConnectionStamp,
    StreamRecoveryCoordinator,
    SubscriptionSession,
) {
    let fence = ConnectionFence::new("runtime-1", "slot-1").unwrap();

    let stamp = fence.begin_connect().unwrap();

    fence.connected(&stamp).unwrap();

    let mut recovery = StreamRecoveryCoordinator::new();

    recovery
        .register("BTCUSDT:trades", RecoveryStreamKind::Trades)
        .unwrap();

    let session =
        SubscriptionSession::new(fence.clone(), stamp.clone(), 8, Duration::from_secs(2)).unwrap();

    (fence, stamp, recovery, session)
}

#[tokio::test]
async fn ack_advances_subscription_but_not_recovery() {
    let (fence, stamp, mut recovery, mut session) = setup();

    let token = recovery
        .begin_binding(&fence, &stamp, "BTCUSDT:trades")
        .unwrap();

    let id = session
        .reserve(
            &recovery,
            &token,
            ExpectedSubscriptionAck::subscribe(Some(json!(7))),
        )
        .unwrap();

    let (sender, receiver) = subscription_send_receipt(id, stamp.clone());

    sender.sent();

    let receipt = receiver.wait(Duration::from_secs(1)).await.unwrap();

    session.transmitted(receipt).unwrap();

    let result = session
        .on_message(
            &mut recovery,
            &ack_protocol(),
            &json!({
                "id": 7,
                "result": null
            }),
            Instant::now(),
        )
        .unwrap();

    assert!(matches!(result, SubscriptionSessionEvent::Confirmed { .. }));

    assert_eq!(
        recovery.phase("BTCUSDT:trades"),
        Some(StreamRecoveryPhase::AwaitingRecovery),
    );

    // The source is NOT automatically recovered.
    assert!(!recovery.may_publish(&fence, &token));

    // Even physical connection readiness is insufficient.
    fence.ready(&stamp).unwrap();

    assert!(!recovery.may_publish(&fence, &token));
}

#[tokio::test]
async fn rejected_ack_never_advances_recovery() {
    let (fence, stamp, mut recovery, mut session) = setup();

    let token = recovery
        .begin_binding(&fence, &stamp, "BTCUSDT:trades")
        .unwrap();

    let id = session
        .reserve(
            &recovery,
            &token,
            ExpectedSubscriptionAck::subscribe(Some(json!(8))),
        )
        .unwrap();

    let (sender, receiver) = subscription_send_receipt(id, stamp);

    sender.sent();

    let receipt = receiver.wait(Duration::from_secs(1)).await.unwrap();

    session.transmitted(receipt).unwrap();

    let result = session
        .on_message(
            &mut recovery,
            &ack_protocol(),
            &json!({
                "id": 8,
                "error": {
                    "code": 400
                }
            }),
            Instant::now(),
        )
        .unwrap();

    assert!(matches!(result, SubscriptionSessionEvent::Failed { .. }));

    assert_eq!(
        recovery.phase("BTCUSDT:trades"),
        Some(StreamRecoveryPhase::AwaitingSubscription),
    );
}

#[test]
fn rebinding_invalidates_old_ack_authority() {
    let (fence, stamp, mut recovery, mut session) = setup();

    let old = recovery
        .begin_binding(&fence, &stamp, "BTCUSDT:trades")
        .unwrap();

    let id = session
        .reserve(
            &recovery,
            &old,
            ExpectedSubscriptionAck::subscribe(Some(json!(9))),
        )
        .unwrap();

    session
        .transmitted(
            marketforge_engine::live::infrastructure::websocket::SubscriptionSendReceipt {
                transaction_id: id,
                stamp: stamp.clone(),
                sent_at: Instant::now(),
            },
        )
        .unwrap();

    let new = recovery
        .begin_binding(&fence, &stamp, "BTCUSDT:trades")
        .unwrap();

    assert_ne!(old, new);

    let result = session.on_message(
        &mut recovery,
        &ack_protocol(),
        &json!({
            "id": 9,
            "result": null
        }),
        Instant::now(),
    );

    assert!(matches!(
        result,
        Err(SubscriptionSessionError::StaleRecoveryToken),
    ));

    assert_eq!(
        recovery.phase("BTCUSDT:trades"),
        Some(StreamRecoveryPhase::AwaitingSubscription),
    );
}

#[test]
fn disconnect_revokes_pending_subscription() {
    let (fence, stamp, mut recovery, mut session) = setup();

    let token = recovery
        .begin_binding(&fence, &stamp, "BTCUSDT:trades")
        .unwrap();

    session
        .reserve(
            &recovery,
            &token,
            ExpectedSubscriptionAck::subscribe(Some(json!(10))),
        )
        .unwrap();

    assert_eq!(session.pending_count(), 1);

    fence.failed(&stamp).unwrap();

    let cancelled = session.invalidate();

    assert_eq!(cancelled.len(), 1);

    assert!(matches!(
        cancelled[0].kind,
        SubscriptionCompletionKind::Cancelled
    ));

    assert_eq!(session.pending_count(), 0);
    assert!(!recovery.may_publish(&fence, &token));
}

#[test]
fn subscription_timeout_preserves_recovery_state() {
    let (fence, stamp, mut recovery, mut session) = setup();

    let token = recovery
        .begin_binding(&fence, &stamp, "BTCUSDT:trades")
        .unwrap();

    let id = session
        .reserve(
            &recovery,
            &token,
            ExpectedSubscriptionAck::subscribe(Some(json!(11))),
        )
        .unwrap();

    let sent_at = Instant::now();

    session
        .transmitted(
            marketforge_engine::live::infrastructure::websocket::SubscriptionSendReceipt {
                transaction_id: id,
                stamp,
                sent_at,
            },
        )
        .unwrap();

    let completions = session.expire(sent_at + Duration::from_secs(2));

    assert_eq!(completions.len(), 1);

    assert!(matches!(
        completions[0].kind,
        SubscriptionCompletionKind::TimedOut
    ));

    assert_eq!(
        recovery.phase("BTCUSDT:trades"),
        Some(StreamRecoveryPhase::AwaitingSubscription),
    );

    assert_eq!(session.pending_count(), 0);
}
