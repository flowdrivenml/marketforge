use serde_json::json;

use crate::live::{
    config::exchange::ExchangeConfig,
    infrastructure::websocket::{
        ExpectedSubscriptionAck, SubscriptionAckClassification, SubscriptionAckCompiler,
        SubscriptionAckIdentity,
    },
};

// ============================================================
// TEST CONFIGURATION
// ============================================================

fn protocol() -> crate::live::infrastructure::websocket::CompiledSubscriptionAck {
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
subscribe_method = "subscribe"
operation_field = "op"
arguments_field = "args"

request_id_field = "id"
request_id_type = "string"

acknowledgement_field = "event"
subscribe_acknowledgement = "subscribe"
unsubscribe_acknowledgement = "unsubscribe"

error_event = "error"
error_code_field = "code"
error_message_field = "msg"

[channels.depth]
enabled = true
format = "depth/okx_spot"
channel = "books"
subscription_template = '{"channel":"books","instId":"{symbol}"}'

[rest]
base_url = "https://example.invalid"
"#,
    )
    .unwrap();

    SubscriptionAckCompiler::compile(&config).unwrap()
}

// ============================================================
// EXPECTED NATIVE IDENTITY
// ============================================================

fn expected(request_id: &str, instrument: &str) -> ExpectedSubscriptionAck {
    let identity = SubscriptionAckIdentity::builder()
        .field("arg.channel", json!("books"))
        .unwrap()
        .field("arg.instId", json!(instrument))
        .unwrap()
        .build()
        .unwrap();

    ExpectedSubscriptionAck::subscribe(Some(json!(request_id))).with_identity(identity)
}

// ============================================================
// CORRECT REQUEST + INSTRUMENT
// ============================================================

#[test]
fn confirms_matching_id_and_native_identity() {
    let protocol = protocol();

    let result = protocol.classify(
        &json!({
            "id": "req-42",
            "event": "subscribe",
            "arg": {
                "channel": "books",
                "instId": "BTC-USDT"
            }
        }),
        &expected("req-42", "BTC-USDT"),
    );

    assert_eq!(result, SubscriptionAckClassification::Confirmed);
}

// ============================================================
// CORRECT ID, WRONG INSTRUMENT
// ============================================================

#[test]
fn matching_id_with_wrong_instrument_fails_closed() {
    let protocol = protocol();

    let result = protocol.classify(
        &json!({
            "id": "req-42",
            "event": "subscribe",
            "arg": {
                "channel": "books",
                "instId": "ETH-USDT"
            }
        }),
        &expected("req-42", "BTC-USDT"),
    );

    assert_eq!(result, SubscriptionAckClassification::Ambiguous);
}

// ============================================================
// WRONG REQUEST ID
// ============================================================

#[test]
fn wrong_request_id_remains_unrelated() {
    let protocol = protocol();

    let result = protocol.classify(
        &json!({
            "id": "req-43",
            "event": "subscribe",
            "arg": {
                "channel": "books",
                "instId": "BTC-USDT"
            }
        }),
        &expected("req-42", "BTC-USDT"),
    );

    assert_eq!(result, SubscriptionAckClassification::Unrelated);
}

// ============================================================
// MISSING INSTRUMENT
// ============================================================

#[test]
fn missing_native_identity_cannot_confirm() {
    let protocol = protocol();

    let result = protocol.classify(
        &json!({
            "id": "req-42",
            "event": "subscribe",
            "arg": {
                "channel": "books"
            }
        }),
        &expected("req-42", "BTC-USDT"),
    );

    assert_eq!(result, SubscriptionAckClassification::Ambiguous);
}

// ============================================================
// CORRECT IDENTITY, WRONG OPERATION
// ============================================================

#[test]
fn wrong_operation_cannot_confirm() {
    let protocol = protocol();

    let result = protocol.classify(
        &json!({
            "id": "req-42",
            "event": "unsubscribe",
            "arg": {
                "channel": "books",
                "instId": "BTC-USDT"
            }
        }),
        &expected("req-42", "BTC-USDT"),
    );

    assert_eq!(result, SubscriptionAckClassification::Unrelated);
}

// ============================================================
// EXPLICIT EXCHANGE ERROR
// ============================================================

#[test]
fn matching_native_error_is_rejected() {
    let protocol = protocol();

    let result = protocol.classify(
        &json!({
            "id": "req-42",
            "event": "error",
            "code": "60018",
            "msg": "Invalid channel",
            "arg": {
                "channel": "books",
                "instId": "BTC-USDT"
            }
        }),
        &expected("req-42", "BTC-USDT"),
    );

    assert_eq!(
        result,
        SubscriptionAckClassification::Rejected {
            code: Some("60018".into()),
            message: Some("Invalid channel".into()),
        }
    );
}

// ============================================================
// OPTIONAL BACKWARD COMPATIBILITY
// ============================================================

#[test]
fn existing_id_only_expectations_still_work() {
    let protocol = protocol();

    let expected = ExpectedSubscriptionAck::subscribe(Some(json!("req-42")));

    let result = protocol.classify(
        &json!({
            "id": "req-42",
            "event": "subscribe"
        }),
        &expected,
    );

    assert_eq!(result, SubscriptionAckClassification::Confirmed);
}
