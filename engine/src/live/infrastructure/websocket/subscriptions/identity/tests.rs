use serde_json::json;

use crate::live::infrastructure::websocket::{
    SubscriptionAckIdentity, SubscriptionIdentityError, SubscriptionIdentityMatch,
};

// ============================================================
// OKX-STYLE NATIVE ACK IDENTITY
// ============================================================

#[test]
fn matches_native_channel_and_instrument() {
    let identity = SubscriptionAckIdentity::builder()
        .field("arg.channel", json!("books"))
        .unwrap()
        .field("arg.instId", json!("BTC-USDT"))
        .unwrap()
        .build()
        .unwrap();

    let response = json!({
        "event": "subscribe",
        "arg": {
            "channel": "books",
            "instId": "BTC-USDT"
        }
    });

    assert_eq!(
        identity.classify(&response),
        SubscriptionIdentityMatch::Matched,
    );
}

#[test]
fn rejects_wrong_instrument() {
    let identity = SubscriptionAckIdentity::builder()
        .field("arg.channel", json!("books"))
        .unwrap()
        .field("arg.instId", json!("BTC-USDT"))
        .unwrap()
        .build()
        .unwrap();

    let response = json!({
        "event": "subscribe",
        "arg": {
            "channel": "books",
            "instId": "ETH-USDT"
        }
    });

    assert_eq!(
        identity.classify(&response),
        SubscriptionIdentityMatch::Mismatched,
    );
}

#[test]
fn missing_instrument_cannot_confirm() {
    let identity = SubscriptionAckIdentity::builder()
        .field("arg.channel", json!("books"))
        .unwrap()
        .field("arg.instId", json!("BTC-USDT"))
        .unwrap()
        .build()
        .unwrap();

    let response = json!({
        "event": "subscribe",
        "arg": {
            "channel": "books"
        }
    });

    assert_eq!(
        identity.classify(&response),
        SubscriptionIdentityMatch::Incomplete,
    );
}

// ============================================================
// BITGET-STYLE NATIVE ACK IDENTITY
// ============================================================

#[test]
fn matches_three_part_native_identity() {
    let identity = SubscriptionAckIdentity::builder()
        .field("arg.instType", json!("spot"))
        .unwrap()
        .field("arg.topic", json!("publicTrade"))
        .unwrap()
        .field("arg.symbol", json!("BTCUSDT"))
        .unwrap()
        .build()
        .unwrap();

    let response = json!({
        "event": "subscribe",
        "arg": {
            "instType": "spot",
            "topic": "publicTrade",
            "symbol": "BTCUSDT"
        }
    });

    assert_eq!(
        identity.classify(&response),
        SubscriptionIdentityMatch::Matched,
    );
}

#[test]
fn wrong_native_market_category_is_rejected() {
    let identity = SubscriptionAckIdentity::builder()
        .field("arg.instType", json!("spot"))
        .unwrap()
        .field("arg.topic", json!("books"))
        .unwrap()
        .field("arg.symbol", json!("BTCUSDT"))
        .unwrap()
        .build()
        .unwrap();

    let response = json!({
        "event": "subscribe",
        "arg": {
            "instType": "USDT-FUTURES",
            "topic": "books",
            "symbol": "BTCUSDT"
        }
    });

    assert_eq!(
        identity.classify(&response),
        SubscriptionIdentityMatch::Mismatched,
    );
}

// ============================================================
// GATE-STYLE NATIVE ACK IDENTITY
// ============================================================

#[test]
fn matches_channel_and_array_payload() {
    let identity = SubscriptionAckIdentity::builder()
        .field("channel", json!("spot.trades"))
        .unwrap()
        .field("result.payload", json!(["BTC_USDT"]))
        .unwrap()
        .build()
        .unwrap();

    let response = json!({
        "channel": "spot.trades",
        "event": "subscribe",
        "result": {
            "payload": ["BTC_USDT"]
        }
    });

    assert_eq!(
        identity.classify(&response),
        SubscriptionIdentityMatch::Matched,
    );
}

#[test]
fn rejects_wrong_payload_argument() {
    let identity = SubscriptionAckIdentity::builder()
        .field("channel", json!("spot.trades"))
        .unwrap()
        .field("result.payload", json!(["BTC_USDT"]))
        .unwrap()
        .build()
        .unwrap();

    assert_eq!(
        identity.classify(&json!({
            "channel": "spot.trades",
            "event": "subscribe",
            "result": {
                "payload": ["ETH_USDT"]
            }
        })),
        SubscriptionIdentityMatch::Mismatched,
    );
}

// ============================================================
// AMBIGUOUS RESPONSES
// ============================================================

#[test]
fn unrelated_event_without_identity_is_incomplete() {
    let identity = SubscriptionAckIdentity::builder()
        .field("arg.channel", json!("books"))
        .unwrap()
        .field("arg.instId", json!("BTC-USDT"))
        .unwrap()
        .build()
        .unwrap();

    assert_eq!(
        identity.classify(&json!({
            "event": "subscribe"
        })),
        SubscriptionIdentityMatch::Incomplete,
    );
}

// ============================================================
// CONFIGURATION VALIDATION
// ============================================================

#[test]
fn empty_identity_is_rejected() {
    assert!(matches!(
        SubscriptionAckIdentity::builder().build(),
        Err(SubscriptionIdentityError::EmptyIdentity),
    ));
}

#[test]
fn invalid_path_is_rejected() {
    assert!(
        SubscriptionAckIdentity::builder()
            .field("arg..symbol", json!("BTCUSDT"))
            .is_err()
    );
}

#[test]
fn null_identity_is_rejected() {
    assert!(matches!(
        SubscriptionAckIdentity::builder().field("arg.symbol", json!(null)),
        Err(SubscriptionIdentityError::NullValue(_)),
    ));
}

#[test]
fn conflicting_fields_are_rejected() {
    let builder = SubscriptionAckIdentity::builder()
        .field("arg.symbol", json!("BTCUSDT"))
        .unwrap();

    assert!(matches!(
        builder.field("arg.symbol", json!("ETHUSDT"),),
        Err(SubscriptionIdentityError::ConflictingField(_)),
    ));
}

#[test]
fn matching_field_with_missing_field_is_incomplete() {
    let identity = SubscriptionAckIdentity::builder()
        .field("arg.channel", json!("books"))
        .unwrap()
        .field("arg.instId", json!("BTC-USDT"))
        .unwrap()
        .build()
        .unwrap();

    assert_eq!(
        identity.classify(&json!({
            "arg": {
                "channel": "books"
            }
        })),
        SubscriptionIdentityMatch::Incomplete,
    );
}
