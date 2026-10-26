use serde_json::json;

use crate::live::{
    config::exchange::ExchangeConfig,
    infrastructure::websocket::{
        ExpectedSubscriptionAck, SubscriptionAckClassification, SubscriptionAckCompiler,
    },
};

// ============================================================
// TEST CONFIGURATION
// ============================================================

fn config(subscription: &str) -> ExchangeConfig {
    let content = format!(
        r#"
schema_version = 1

[exchange]
id = "binance"
market = "spot"
profile = "binance_spot"

[websocket]
url = "wss://example.invalid"

{subscription}

[channels.trades]
enabled = true
format = "trades/binance_spot"

[rest]
base_url = "https://example.invalid"
"#
    );

    toml::from_str(&content).unwrap()
}

// ============================================================
// JSON-RPC SUCCESS
// ============================================================

#[test]
fn json_rpc_confirms_matching_request() {
    let exchange = config(
        r#"
[websocket.subscription]
strategy = "json_rpc"
request_id_field = "id"
success_field = "result"
success_is_null = true
error_code_field = "code"
error_message_field = "msg"
"#,
    );

    let protocol = SubscriptionAckCompiler::compile(&exchange).unwrap();

    let expected = ExpectedSubscriptionAck::subscribe(Some(json!(42)));

    let response = json!({
        "id": 42,
        "result": null
    });

    assert_eq!(
        protocol.classify(&response, &expected),
        SubscriptionAckClassification::Confirmed
    );
}

// ============================================================
// WRONG REQUEST ID
// ============================================================

#[test]
fn rejects_unrelated_request_id() {
    let exchange = config(
        r#"
[websocket.subscription]
strategy = "json_rpc"
request_id_field = "id"
success_field = "result"
success_is_null = true
"#,
    );

    let protocol = SubscriptionAckCompiler::compile(&exchange).unwrap();

    let expected = ExpectedSubscriptionAck::subscribe(Some(json!(42)));

    assert_eq!(
        protocol.classify(
            &json!({
                "id": 43,
                "result": null
            }),
            &expected,
        ),
        SubscriptionAckClassification::Unrelated
    );
}

// ============================================================
// FAILED JSON-RPC RESPONSE
// ============================================================

#[test]
fn json_rpc_rejects_exchange_error() {
    let exchange = config(
        r#"
[websocket.subscription]
strategy = "json_rpc"
request_id_field = "id"
success_field = "result"
success_is_null = true
error_field = "error"
error_code_field = "error.code"
error_message_field = "error.msg"
"#,
    );

    let protocol = SubscriptionAckCompiler::compile(&exchange).unwrap();

    let expected = ExpectedSubscriptionAck::subscribe(Some(json!(7)));

    let response = json!({
        "id": 7,
        "error": {
            "code": 400,
            "msg": "Invalid subscription"
        }
    });

    assert_eq!(
        protocol.classify(&response, &expected),
        SubscriptionAckClassification::Rejected {
            code: Some("400".into()),
            message: Some("Invalid subscription".into()),
        }
    );
}

// ============================================================
// JSON OPERATION SUCCESS
// ============================================================

#[test]
fn json_operation_validates_success() {
    let exchange = config(
        r#"
[websocket.subscription]
strategy = "json_operation"
request_id_field = "req_id"
acknowledgement_field = "op"
subscribe_acknowledgement = "subscribe"
unsubscribe_acknowledgement = "unsubscribe"
success_field = "success"
success_value = true
error_message_field = "ret_msg"
"#,
    );

    let protocol = SubscriptionAckCompiler::compile(&exchange).unwrap();

    let expected = ExpectedSubscriptionAck::subscribe(Some(json!("req-1")));

    let response = json!({
        "op": "subscribe",
        "req_id": "req-1",
        "success": true
    });

    assert_eq!(
        protocol.classify(&response, &expected),
        SubscriptionAckClassification::Confirmed
    );

    let negative = json!({
        "op": "subscribe",
        "req_id": "req-1",
        "success": false,
        "ret_msg": "Invalid channel"
    });

    assert!(matches!(
        protocol.classify(&negative, &expected),
        SubscriptionAckClassification::Rejected { .. }
    ));
}

// ============================================================
// UNRELATED MARKET DATA
// ============================================================

#[test]
fn market_data_is_not_an_ack() {
    let exchange = config(
        r#"
[websocket.subscription]
strategy = "json_operation"
request_id_field = "req_id"
acknowledgement_field = "op"
subscribe_acknowledgement = "subscribe"
success_field = "success"
success_value = true
"#,
    );

    let protocol = SubscriptionAckCompiler::compile(&exchange).unwrap();

    let expected = ExpectedSubscriptionAck::subscribe(Some(json!("req-1")));

    let market_data = json!({
        "topic": "publicTrade.BTCUSDT",
        "data": [
            {
                "price": "80000",
                "size": "0.1"
            }
        ]
    });

    assert_eq!(
        protocol.classify(&market_data, &expected),
        SubscriptionAckClassification::Unrelated
    );
}

// ============================================================
// NO REQUEST CORRELATION
// ============================================================

#[test]
fn uncorrelated_ack_requires_single_flight() {
    let exchange = config(
        r#"
[websocket.subscription]
strategy = "gate_v4_channel_event"
acknowledgement_field = "event"
subscribe_acknowledgement = "subscribe"
unsubscribe_acknowledgement = "unsubscribe"
error_event = "error"
channel_field = "channel"
"#,
    );

    let protocol = SubscriptionAckCompiler::compile(&exchange).unwrap();

    let response = json!({
        "channel": "spot.trades",
        "event": "subscribe"
    });

    let unprotected = ExpectedSubscriptionAck::subscribe(None).with_channel("spot.trades");

    assert_eq!(
        protocol.classify(&response, &unprotected),
        SubscriptionAckClassification::Ambiguous
    );

    let protected = unprotected.with_single_flight();

    assert_eq!(
        protocol.classify(&response, &protected),
        SubscriptionAckClassification::Confirmed
    );
}

// ============================================================
// WRONG CHANNEL
// ============================================================

#[test]
fn channel_ack_rejects_wrong_channel() {
    let exchange = config(
        r#"
[websocket.subscription]
strategy = "gate_v4_channel_event"
acknowledgement_field = "event"
subscribe_acknowledgement = "subscribe"
channel_field = "channel"
"#,
    );

    let protocol = SubscriptionAckCompiler::compile(&exchange).unwrap();

    let expected = ExpectedSubscriptionAck::subscribe(None)
        .with_channel("spot.trades")
        .with_single_flight();

    assert_eq!(
        protocol.classify(
            &json!({
                "channel": "spot.order_book",
                "event": "subscribe"
            }),
            &expected,
        ),
        SubscriptionAckClassification::Unrelated
    );
}

// ============================================================
// MISSING CORRELATION IN AN ACK
// ============================================================

#[test]
fn missing_request_id_never_confirms() {
    let exchange = config(
        r#"
[websocket.subscription]
strategy = "json_operation"
request_id_field = "req_id"
acknowledgement_field = "op"
subscribe_acknowledgement = "subscribe"
success_field = "success"
success_value = true
"#,
    );

    let protocol = SubscriptionAckCompiler::compile(&exchange).unwrap();

    let expected = ExpectedSubscriptionAck::subscribe(Some(json!("req-1")));

    assert_eq!(
        protocol.classify(
            &json!({
                "op": "subscribe",
                "success": true
            }),
            &expected,
        ),
        SubscriptionAckClassification::Ambiguous
    );
}

// ============================================================
// INCOMPLETE SUCCESS MATCHER
// ============================================================

#[test]
fn incomplete_success_matcher_cannot_confirm() {
    let exchange = config(
        r#"
[websocket.subscription]
strategy = "json_operation"
request_id_field = "req_id"
acknowledgement_field = "op"
subscribe_acknowledgement = "subscribe"
success_field = "success"
"#,
    );

    let protocol = SubscriptionAckCompiler::compile(&exchange).unwrap();

    let expected = ExpectedSubscriptionAck::subscribe(Some(json!("req-1")));

    assert_eq!(
        protocol.classify(
            &json!({
                "op": "subscribe",
                "req_id": "req-1",
                "success": true
            }),
            &expected,
        ),
        SubscriptionAckClassification::Ambiguous
    );
}

mod ack_identity;
