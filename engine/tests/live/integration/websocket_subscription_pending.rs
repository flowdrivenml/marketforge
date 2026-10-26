use std::time::Duration;

use serde_json::json;
use tokio::time::Instant;

use marketforge_engine::live::{
    config::exchange::ExchangeConfig,
    infrastructure::websocket::{
        ConnectionFence, ExpectedSubscriptionAck, SubscriptionAckCompiler,
        SubscriptionCompletionKind, SubscriptionDispatch, SubscriptionPendingError,
        SubscriptionPendingManager, SubscriptionTransactionPhase,
    },
};

fn ack_protocol() -> marketforge_engine::live::infrastructure::websocket::CompiledSubscriptionAck {
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

    SubscriptionAckCompiler::compile(&config).unwrap()
}

fn manager() -> SubscriptionPendingManager {
    SubscriptionPendingManager::new(8, Duration::from_secs(2)).unwrap()
}

fn connected_fence() -> (
    ConnectionFence,
    marketforge_engine::live::infrastructure::websocket::ConnectionStamp,
) {
    let fence = ConnectionFence::new("runtime-1", "connection-1").unwrap();

    let stamp = fence.begin_connect().unwrap();

    fence.connected(&stamp).unwrap();

    (fence, stamp)
}

#[test]
fn w43_ack_deadline_starts_after_transmission() {
    let (fence, stamp) = connected_fence();
    let mut pending = manager();

    let id = pending
        .reserve(
            &fence,
            &stamp,
            "BTCUSDT:trades",
            ExpectedSubscriptionAck::subscribe(Some(json!(1))),
        )
        .unwrap();

    assert_eq!(
        pending.phase(id),
        Some(SubscriptionTransactionPhase::Reserved),
    );

    assert_eq!(pending.next_deadline(), None);

    let sent_at = Instant::now();

    pending.mark_sent(&fence, &stamp, id, sent_at).unwrap();

    assert_eq!(
        pending.phase(id),
        Some(SubscriptionTransactionPhase::AwaitingAck),
    );

    assert_eq!(
        pending.next_deadline(),
        Some(sent_at + Duration::from_secs(2)),
    );
}

#[test]
fn w43_matching_ack_completes_correct_transaction() {
    let (fence, stamp) = connected_fence();
    let mut pending = manager();

    let first = pending
        .reserve(
            &fence,
            &stamp,
            "BTCUSDT:trades",
            ExpectedSubscriptionAck::subscribe(Some(json!(10))),
        )
        .unwrap();

    let second = pending
        .reserve(
            &fence,
            &stamp,
            "ETHUSDT:trades",
            ExpectedSubscriptionAck::subscribe(Some(json!(11))),
        )
        .unwrap();

    let now = Instant::now();

    pending.mark_sent(&fence, &stamp, first, now).unwrap();
    pending.mark_sent(&fence, &stamp, second, now).unwrap();

    let dispatch = pending
        .classify(
            &fence,
            &stamp,
            &ack_protocol(),
            &json!({"id": 11, "result": null}),
            now,
        )
        .unwrap();

    let SubscriptionDispatch::Completed(completion) = dispatch else {
        panic!("expected completed subscription");
    };

    assert_eq!(completion.transaction_id, second);
    assert_eq!(completion.stream_id, "ETHUSDT:trades");

    assert!(matches!(
        completion.kind,
        SubscriptionCompletionKind::Confirmed
    ));

    assert_eq!(pending.pending_count(), 1);

    assert_eq!(
        pending.phase(first),
        Some(SubscriptionTransactionPhase::AwaitingAck),
    );
}

#[test]
fn w43_late_ack_cannot_confirm_expired_transaction() {
    let (fence, stamp) = connected_fence();
    let mut pending = manager();

    let id = pending
        .reserve(
            &fence,
            &stamp,
            "BTCUSDT:trades",
            ExpectedSubscriptionAck::subscribe(Some(json!(1))),
        )
        .unwrap();

    let sent_at = Instant::now();

    pending.mark_sent(&fence, &stamp, id, sent_at).unwrap();

    let expired_at = sent_at + Duration::from_secs(2);

    let completions = pending.expire(&stamp, expired_at);

    assert_eq!(completions.len(), 1);

    assert!(matches!(
        completions[0].kind,
        SubscriptionCompletionKind::TimedOut
    ));

    let dispatch = pending
        .classify(
            &fence,
            &stamp,
            &ack_protocol(),
            &json!({"id": 1, "result": null}),
            expired_at,
        )
        .unwrap();

    assert!(matches!(dispatch, SubscriptionDispatch::Unrelated));

    assert_eq!(pending.pending_count(), 0);
}

#[test]
fn w43_cannot_confirm_ack_received_after_deadline() {
    let (fence, stamp) = connected_fence();
    let mut pending = manager();

    let id = pending
        .reserve(
            &fence,
            &stamp,
            "BTCUSDT:trades",
            ExpectedSubscriptionAck::subscribe(Some(json!(1))),
        )
        .unwrap();

    let sent_at = Instant::now();

    pending.mark_sent(&fence, &stamp, id, sent_at).unwrap();

    // Even if the caller hasn't collected the timeout
    // completion, classification must not accept a late ACK.
    let dispatch = pending
        .classify(
            &fence,
            &stamp,
            &ack_protocol(),
            &json!({"id": 1, "result": null}),
            sent_at + Duration::from_secs(3),
        )
        .unwrap();

    assert!(matches!(dispatch, SubscriptionDispatch::Unrelated));

    let completions = pending.expire(&stamp, sent_at + Duration::from_secs(3));

    assert_eq!(completions.len(), 1);
    assert!(matches!(
        completions[0].kind,
        SubscriptionCompletionKind::TimedOut
    ));
}

#[test]
fn w43_duplicate_request_id_is_rejected() {
    let (fence, stamp) = connected_fence();
    let mut pending = manager();

    pending
        .reserve(
            &fence,
            &stamp,
            "BTCUSDT:trades",
            ExpectedSubscriptionAck::subscribe(Some(json!(7))),
        )
        .unwrap();

    assert!(matches!(
        pending.reserve(
            &fence,
            &stamp,
            "ETHUSDT:trades",
            ExpectedSubscriptionAck::subscribe(Some(json!(7))),
        ),
        Err(SubscriptionPendingError::RetiredRequestId)
    ));
}

#[test]
fn w43_idless_subscription_requires_exclusive_flight() {
    let (fence, stamp) = connected_fence();
    let mut pending = manager();

    pending
        .reserve(
            &fence,
            &stamp,
            "BTCUSDT:trades",
            ExpectedSubscriptionAck::subscribe(None),
        )
        .unwrap();

    assert!(matches!(
        pending.reserve(
            &fence,
            &stamp,
            "ETHUSDT:trades",
            ExpectedSubscriptionAck::subscribe(Some(json!(2))),
        ),
        Err(SubscriptionPendingError::SingleFlightRequired)
    ));
}

#[test]
fn w43_reconnect_invalidates_all_old_pending_requests() {
    let (fence, first_stamp) = connected_fence();
    let mut pending = manager();

    let id = pending
        .reserve(
            &fence,
            &first_stamp,
            "BTCUSDT:trades",
            ExpectedSubscriptionAck::subscribe(Some(json!(1))),
        )
        .unwrap();

    pending
        .mark_sent(&fence, &first_stamp, id, Instant::now())
        .unwrap();

    fence.failed(&first_stamp).unwrap();

    let completions = pending.invalidate_generation(&first_stamp);

    assert_eq!(completions.len(), 1);
    assert_eq!(pending.pending_count(), 0);

    let second_stamp = fence.begin_connect().unwrap();
    fence.connected(&second_stamp).unwrap();

    assert!(matches!(
        pending.reserve(
            &fence,
            &first_stamp,
            "BTCUSDT:trades",
            ExpectedSubscriptionAck::subscribe(Some(json!(2))),
        ),
        Err(SubscriptionPendingError::StaleConnection)
    ));

    assert!(
        pending
            .reserve(
                &fence,
                &second_stamp,
                "BTCUSDT:trades",
                ExpectedSubscriptionAck::subscribe(Some(json!(2))),
            )
            .is_ok()
    );
}

#[test]
fn w43_transmission_failure_releases_reservation() {
    let (fence, stamp) = connected_fence();
    let mut pending = manager();

    let id = pending
        .reserve(
            &fence,
            &stamp,
            "BTCUSDT:trades",
            ExpectedSubscriptionAck::subscribe(Some(json!(1))),
        )
        .unwrap();

    let completion = pending.cancel(id).unwrap();

    assert!(matches!(
        completion.kind,
        SubscriptionCompletionKind::Cancelled
    ));

    assert_eq!(pending.pending_count(), 0);
}
