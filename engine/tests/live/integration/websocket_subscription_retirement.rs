use std::time::Duration;

use serde_json::json;
use tokio::time::Instant;

use marketforge_engine::live::{
    config::exchange::ExchangeConfig,
    infrastructure::websocket::{
        CompiledSubscriptionAck, ConnectionFence, ConnectionStamp, ExpectedSubscriptionAck,
        SubscriptionAckCompiler, SubscriptionAckIdentity, SubscriptionCompletionKind,
        SubscriptionDispatch, SubscriptionPendingError, SubscriptionPendingManager,
    },
};

// ============================================================
// HELPERS
// ============================================================

fn fence() -> (ConnectionFence, ConnectionStamp) {
    let fence = ConnectionFence::new("runtime-1", "slot-1").unwrap();

    let stamp = fence.begin_connect().unwrap();

    fence.connected(&stamp).unwrap();

    (fence, stamp)
}

fn pending() -> SubscriptionPendingManager {
    SubscriptionPendingManager::new(16, Duration::from_secs(2)).unwrap()
}

fn rpc_protocol() -> CompiledSubscriptionAck {
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

fn native_protocol() -> CompiledSubscriptionAck {
    let config: ExchangeConfig = toml::from_str(
        r#"
schema_version = 1

[exchange]
id = "okx"
market = "spot"
profile = "okx_spot"

[websocket]
url = "wss://example.invalid"

[websocket.subscription]
strategy = "json_operation"
operation_field = "op"
arguments_field = "args"

request_id_field = "id"
request_id_type = "string"
request_id_optional = true

acknowledgement_field = "event"
subscribe_acknowledgement = "subscribe"
unsubscribe_acknowledgement = "unsubscribe"

[channels.trades]
enabled = true
format = "trades/okx_spot"

[rest]
base_url = "https://example.invalid"
"#,
    )
    .unwrap();

    SubscriptionAckCompiler::compile(&config).unwrap()
}

fn identity(instrument: &str) -> SubscriptionAckIdentity {
    SubscriptionAckIdentity::builder()
        .field("arg.channel", json!("trades-all"))
        .unwrap()
        .field("arg.instId", json!(instrument))
        .unwrap()
        .build()
        .unwrap()
}

// ============================================================
// REQUEST-ID RETIREMENT
// ============================================================

#[test]
fn w45_completed_request_id_cannot_be_reused() {
    let (fence, stamp) = fence();

    let mut pending = pending();

    let first = pending
        .reserve(
            &fence,
            &stamp,
            "BTC:trades",
            ExpectedSubscriptionAck::subscribe(Some(json!(42))),
        )
        .unwrap();

    pending
        .mark_sent(&fence, &stamp, first, Instant::now())
        .unwrap();

    let result = pending
        .classify(
            &fence,
            &stamp,
            &rpc_protocol(),
            &json!({
                "id": 42,
                "result": null
            }),
            Instant::now(),
        )
        .unwrap();

    assert!(matches!(result, SubscriptionDispatch::Completed(_)));

    assert_eq!(pending.pending_count(), 0);

    assert!(matches!(
        pending.reserve(
            &fence,
            &stamp,
            "ETH:trades",
            ExpectedSubscriptionAck::subscribe(Some(json!(42)),),
        ),
        Err(SubscriptionPendingError::RetiredRequestId)
    ));

    assert_eq!(pending.retired_request_id_count(), 1);
}

#[test]
fn w45_timed_out_request_id_remains_retired() {
    let (fence, stamp) = fence();

    let mut pending = pending();

    let id = pending
        .reserve(
            &fence,
            &stamp,
            "BTC:trades",
            ExpectedSubscriptionAck::subscribe(Some(json!(7))),
        )
        .unwrap();

    let sent_at = Instant::now();

    pending.mark_sent(&fence, &stamp, id, sent_at).unwrap();

    let completions = pending.expire(&stamp, sent_at + Duration::from_secs(2));

    assert_eq!(completions.len(), 1);

    assert!(matches!(
        completions[0].kind,
        SubscriptionCompletionKind::TimedOut
    ));

    assert!(matches!(
        pending.reserve(
            &fence,
            &stamp,
            "ETH:trades",
            ExpectedSubscriptionAck::subscribe(Some(json!(7)),),
        ),
        Err(SubscriptionPendingError::RetiredRequestId)
    ));

    // A new request ID remains admissible.
    assert!(
        pending
            .reserve(
                &fence,
                &stamp,
                "ETH:trades",
                ExpectedSubscriptionAck::subscribe(Some(json!(8)),),
            )
            .is_ok()
    );
}

#[test]
fn w45_cancelled_request_id_remains_retired() {
    let (fence, stamp) = fence();

    let mut pending = pending();

    let id = pending
        .reserve(
            &fence,
            &stamp,
            "BTC:trades",
            ExpectedSubscriptionAck::subscribe(Some(json!("req-1"))),
        )
        .unwrap();

    pending.cancel(id).unwrap();

    assert!(matches!(
        pending.reserve(
            &fence,
            &stamp,
            "ETH:trades",
            ExpectedSubscriptionAck::subscribe(Some(json!("req-1")),),
        ),
        Err(SubscriptionPendingError::RetiredRequestId)
    ));
}

// ============================================================
// ID-LESS SUBSCRIPTIONS
// ============================================================

#[test]
fn w45_idless_timeout_requires_reconnect() {
    let (fence, stamp) = fence();

    let mut pending = pending();

    let id = pending
        .reserve(
            &fence,
            &stamp,
            "BTC:trades",
            ExpectedSubscriptionAck::subscribe(None).with_identity(identity("BTC-USDT")),
        )
        .unwrap();

    let sent_at = Instant::now();

    pending.mark_sent(&fence, &stamp, id, sent_at).unwrap();

    pending.expire(&stamp, sent_at + Duration::from_secs(2));

    assert!(pending.reconnect_required());

    assert!(matches!(
        pending.reserve(
            &fence,
            &stamp,
            "BTC:trades",
            ExpectedSubscriptionAck::subscribe(None).with_identity(identity("BTC-USDT")),
        ),
        Err(SubscriptionPendingError::ReconnectRequired)
    ));
}

#[test]
fn w45_idless_identity_cannot_be_reused() {
    let (fence, stamp) = fence();

    let mut pending = pending();

    let id = pending
        .reserve(
            &fence,
            &stamp,
            "BTC:trades",
            ExpectedSubscriptionAck::subscribe(None).with_identity(identity("BTC-USDT")),
        )
        .unwrap();

    pending
        .mark_sent(&fence, &stamp, id, Instant::now())
        .unwrap();

    let result = pending
        .classify(
            &fence,
            &stamp,
            &native_protocol(),
            &json!({
                "event": "subscribe",
                "arg": {
                    "channel": "trades-all",
                    "instId": "BTC-USDT"
                }
            }),
            Instant::now(),
        )
        .unwrap();

    assert!(matches!(result, SubscriptionDispatch::Completed(_)));

    assert!(matches!(
        pending.reserve(
            &fence,
            &stamp,
            "BTC:trades",
            ExpectedSubscriptionAck::subscribe(None).with_identity(identity("BTC-USDT")),
        ),
        Err(SubscriptionPendingError::RetiredIdlessIdentity)
    ));

    // Another native identity is independently
    // correlatable.
    assert!(
        pending
            .reserve(
                &fence,
                &stamp,
                "ETH:trades",
                ExpectedSubscriptionAck::subscribe(None).with_identity(identity("ETH-USDT")),
            )
            .is_ok()
    );
}

#[test]
fn w45_idless_without_identity_cannot_be_followed_by_another() {
    let (fence, stamp) = fence();

    let mut pending = pending();

    let first = pending
        .reserve(
            &fence,
            &stamp,
            "BTC:trades",
            ExpectedSubscriptionAck::subscribe(None),
        )
        .unwrap();

    // Even after cancellation, the previous request
    // might have reached the exchange.
    pending.cancel(first).unwrap();

    assert!(matches!(
        pending.reserve(
            &fence,
            &stamp,
            "ETH:trades",
            ExpectedSubscriptionAck::subscribe(None).with_identity(identity("ETH-USDT")),
        ),
        Err(SubscriptionPendingError::ReconnectRequired)
    ));
}

// ============================================================
// AMBIGUOUS CORRELATION
// ============================================================

#[test]
fn w45_ambiguous_ack_cannot_select_first_pending_entry() {
    let (fence, stamp) = fence();

    let mut pending = pending();

    let first = pending
        .reserve(
            &fence,
            &stamp,
            "BTC:trades",
            ExpectedSubscriptionAck::subscribe(Some(json!(10))),
        )
        .unwrap();

    let second = pending
        .reserve(
            &fence,
            &stamp,
            "ETH:trades",
            ExpectedSubscriptionAck::subscribe(Some(json!(11))),
        )
        .unwrap();

    let sent_at = Instant::now();

    pending.mark_sent(&fence, &stamp, first, sent_at).unwrap();

    pending.mark_sent(&fence, &stamp, second, sent_at).unwrap();

    // An event-only response with no request ID can
    // resemble multiple outstanding subscriptions.
    //
    // It must never complete the first transaction
    // merely because its map key sorts earlier.
    let result = pending.classify(
        &fence,
        &stamp,
        &native_protocol(),
        &json!({
            "event": "subscribe"
        }),
        sent_at + Duration::from_millis(1),
    );

    assert!(matches!(
        result,
        Err(SubscriptionPendingError::AmbiguousCorrelation)
    ));

    assert_eq!(pending.pending_count(), 2);

    assert!(pending.reconnect_required());
}

#[test]
fn w45_wrong_request_id_does_not_consume_pending_transaction() {
    let (fence, stamp) = fence();

    let mut pending = pending();

    let id = pending
        .reserve(
            &fence,
            &stamp,
            "BTC:trades",
            ExpectedSubscriptionAck::subscribe(Some(json!(42))),
        )
        .unwrap();

    pending
        .mark_sent(&fence, &stamp, id, Instant::now())
        .unwrap();

    let dispatch = pending
        .classify(
            &fence,
            &stamp,
            &rpc_protocol(),
            &json!({
                "id": 43,
                "result": null
            }),
            Instant::now(),
        )
        .unwrap();

    assert!(matches!(dispatch, SubscriptionDispatch::Unrelated));

    assert_eq!(pending.pending_count(), 1);
}
