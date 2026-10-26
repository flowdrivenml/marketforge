use serde_json::Value;
use thiserror::Error;

use crate::live::{
    config::exchange::{ExchangeConfig, SubscriptionConfig},
    infrastructure::normalization::JsonExtractor,
};

use super::identity::{SubscriptionAckIdentity, SubscriptionIdentityMatch};
use super::protocol::SubscriptionStrategy;

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, Error)]
pub enum SubscriptionAckError {
    #[error("missing WebSocket subscription configuration")]
    MissingConfiguration,

    #[error("invalid subscription ACK configuration: {0}")]
    Configuration(String),
}

// ============================================================
// OPERATIONS
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscriptionAction {
    Subscribe,
    Unsubscribe,
}

// ============================================================
// EXPECTED ACKNOWLEDGMENT
// ============================================================

/// Identity of the request awaiting an exchange ACK.
///
/// When no request ID is available, single_flight must
/// be true. The caller must guarantee that there is only
/// one outstanding subscription operation within the
/// relevant connection/protocol correlation scope.
#[derive(Debug, Clone)]
pub struct ExpectedSubscriptionAck {
    pub action: SubscriptionAction,
    pub request_id: Option<Value>,
    pub single_flight: bool,
    pub channel: Option<String>,

    /// Optional exact native subscription identity.
    ///
    /// When present, every required identity field
    /// must match the exchange ACK.
    ///
    /// This does not replace operation, success,
    /// request-ID, or connection-generation validation.
    pub identity: Option<SubscriptionAckIdentity>,
}

impl ExpectedSubscriptionAck {
    pub fn subscribe(request_id: Option<Value>) -> Self {
        Self {
            action: SubscriptionAction::Subscribe,
            request_id,
            single_flight: false,
            channel: None,
            identity: None,
        }
    }

    pub fn unsubscribe(request_id: Option<Value>) -> Self {
        Self {
            action: SubscriptionAction::Unsubscribe,
            request_id,
            single_flight: false,
            channel: None,
            identity: None,
        }
    }
    pub fn with_identity(mut self, identity: SubscriptionAckIdentity) -> Self {
        self.identity = Some(identity);
        self
    }
    pub fn with_single_flight(mut self) -> Self {
        self.single_flight = true;
        self
    }

    pub fn with_channel(mut self, channel: impl Into<String>) -> Self {
        self.channel = Some(channel.into());
        self
    }
}

// ============================================================
// CLASSIFICATION
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubscriptionAckClassification {
    /// The message is unrelated to this pending request.
    Unrelated,

    /// The message resembles a subscription ACK, but
    /// cannot be safely correlated or fully validated.
    Ambiguous,

    /// The correct request was explicitly confirmed.
    Confirmed,

    /// The correct request was explicitly rejected.
    Rejected {
        code: Option<String>,
        message: Option<String>,
    },
}

// ============================================================
// COMPILED ACK RULES
// ============================================================

#[derive(Debug, Clone)]
pub struct CompiledSubscriptionAck {
    strategy: SubscriptionStrategy,

    request_id_field: Option<String>,

    acknowledgement_field: Option<String>,
    subscribe_acknowledgement: Option<String>,
    unsubscribe_acknowledgement: Option<String>,

    success_field: Option<String>,
    success_value: Option<Value>,
    success_is_null: bool,

    error_field: Option<String>,
    error_event: Option<String>,
    error_code_field: Option<String>,
    error_message_field: Option<String>,

    channel_field: Option<String>,
}

impl CompiledSubscriptionAck {
    pub fn classify(
        &self,
        message: &Value,
        expected: &ExpectedSubscriptionAck,
    ) -> SubscriptionAckClassification {
        use SubscriptionAckClassification as Result;

        if !message.is_object() {
            return Result::Unrelated;
        }

        // ----------------------------------------------------
        // ACK OPERATION
        // ----------------------------------------------------

        let expected_event = match expected.action {
            SubscriptionAction::Subscribe => self.subscribe_acknowledgement.as_deref(),

            SubscriptionAction::Unsubscribe => self.unsubscribe_acknowledgement.as_deref(),
        };

        let mut recognized = false;
        let mut error_event = false;

        if let Some(field) = &self.acknowledgement_field {
            let actual = read(message, field);

            if let Some(actual) = actual.and_then(Value::as_str) {
                if expected_event == Some(actual) {
                    recognized = true;
                } else if self.error_event.as_deref() == Some(actual) {
                    recognized = true;
                    error_event = true;
                } else {
                    return Result::Unrelated;
                }
            } else {
                // JSON-RPC may identify an ACK by request ID
                // without an operation/event discriminator.
                if self.strategy != SubscriptionStrategy::JsonRpc {
                    return Result::Unrelated;
                }
            }
        }

        // ----------------------------------------------------
        // REQUEST ID CORRELATION
        // ----------------------------------------------------

        match (
            self.request_id_field.as_deref(),
            expected.request_id.as_ref(),
        ) {
            (Some(field), Some(expected_id)) => {
                match read(message, field) {
                    Some(actual) if actual == expected_id => {
                        recognized = true;
                    }

                    Some(_) => {
                        // Another request's ACK.
                        return Result::Unrelated;
                    }

                    None => {
                        // An ACK with missing correlation
                        // information cannot confirm a request.
                        return if recognized {
                            Result::Ambiguous
                        } else {
                            Result::Unrelated
                        };
                    }
                }
            }

            (Some(_), None) | (None, None) => {
                if !expected.single_flight {
                    return if recognized {
                        Result::Ambiguous
                    } else {
                        Result::Unrelated
                    };
                }
            }

            (None, Some(_)) => {
                return Result::Ambiguous;
            }
        }

        // ----------------------------------------------------
        // CHANNEL CORRELATION
        // ----------------------------------------------------

        if let Some(channel) = expected.channel.as_deref() {
            let field = match self.channel_field.as_deref() {
                Some(field) => field,
                None => return Result::Ambiguous,
            };

            match read(message, field).and_then(Value::as_str) {
                Some(actual) if actual == channel => {
                    recognized = true;
                }

                Some(_) => return Result::Unrelated,

                None => {
                    return if recognized {
                        Result::Ambiguous
                    } else {
                        Result::Unrelated
                    };
                }
            }
        }
        // ----------------------------------------------------
        // NATIVE SUBSCRIPTION IDENTITY
        // ----------------------------------------------------
        //
        // Request-ID correlation has already completed.
        //
        // If the native identity contradicts a matching
        // request ID, treat the response as ambiguous and
        // fail closed.
        //
        // Without request-ID correlation, a mismatching
        // native identity belongs to another subscription
        // and must not complete this transaction.
        //
        // Missing required native fields can never
        // establish successful subscription confirmation.

        if let Some(identity) = expected.identity.as_ref() {
            let request_id_matched = match (
                self.request_id_field.as_deref(),
                expected.request_id.as_ref(),
            ) {
                (Some(field), Some(expected_id)) => read(message, field) == Some(expected_id),

                _ => false,
            };

            match identity.classify(message) {
                SubscriptionIdentityMatch::Matched => {
                    recognized = true;
                }

                SubscriptionIdentityMatch::Mismatched => {
                    return if request_id_matched {
                        Result::Ambiguous
                    } else {
                        Result::Unrelated
                    };
                }

                SubscriptionIdentityMatch::Incomplete => {
                    return Result::Ambiguous;
                }
            }
        }
        if !recognized {
            return Result::Unrelated;
        }

        // ----------------------------------------------------
        // EXPLICIT ERROR
        // ----------------------------------------------------

        let error_payload = self
            .error_field
            .as_deref()
            .and_then(|field| read(message, field))
            .filter(|value| !value.is_null() && *value != &Value::Bool(false));

        if error_event || error_payload.is_some() {
            return Result::Rejected {
                code: self
                    .error_code_field
                    .as_deref()
                    .and_then(|field| read(message, field))
                    .map(value_text),

                message: self
                    .error_message_field
                    .as_deref()
                    .and_then(|field| read(message, field))
                    .map(value_text),
            };
        }

        // ----------------------------------------------------
        // SUCCESS CRITERIA
        // ----------------------------------------------------

        if let Some(field) = &self.success_field {
            let Some(actual) = read(message, field) else {
                return Result::Ambiguous;
            };

            if self.success_is_null {
                return if actual.is_null() {
                    Result::Confirmed
                } else {
                    Result::Rejected {
                        code: self
                            .error_code_field
                            .as_deref()
                            .and_then(|field| read(message, field))
                            .map(value_text),

                        message: self
                            .error_message_field
                            .as_deref()
                            .and_then(|field| read(message, field))
                            .map(value_text),
                    }
                };
            }

            if let Some(expected_value) = &self.success_value {
                return if actual == expected_value {
                    Result::Confirmed
                } else {
                    Result::Rejected {
                        code: self
                            .error_code_field
                            .as_deref()
                            .and_then(|field| read(message, field))
                            .map(value_text),

                        message: self
                            .error_message_field
                            .as_deref()
                            .and_then(|field| read(message, field))
                            .map(value_text),
                    }
                };
            }

            return Result::Ambiguous;
        }

        // An explicitly matched native subscribe/unsubscribe
        // acknowledgment can be used as success evidence
        // when the protocol declares no separate success flag.
        //
        // A request ID alone is NOT enough.
        if self.acknowledgement_field.is_some() && expected_event.is_some() {
            return Result::Confirmed;
        }

        Result::Ambiguous
    }
}

// ============================================================
// COMPILER
// ============================================================

pub struct SubscriptionAckCompiler;

impl SubscriptionAckCompiler {
    pub fn compile(
        exchange: &ExchangeConfig,
    ) -> Result<CompiledSubscriptionAck, SubscriptionAckError> {
        let config = exchange
            .websocket
            .subscription
            .as_ref()
            .ok_or(SubscriptionAckError::MissingConfiguration)?;

        let strategy = match config.strategy.as_str() {
            "json_rpc" => SubscriptionStrategy::JsonRpc,

            "json_operation" => SubscriptionStrategy::JsonOperation,

            "gate_v4_channel_event" => SubscriptionStrategy::GateV4ChannelEvent,

            other => {
                return Err(invalid(format!(
                    "unsupported subscription ACK strategy '{other}'"
                )));
            }
        };

        validate_rules(config, strategy)?;

        let success_value = config
            .success_value
            .as_ref()
            .map(serde_json::to_value)
            .transpose()
            .map_err(|error| invalid(format!("cannot compile success value: {error}")))?;

        let channel_field = match strategy {
            SubscriptionStrategy::GateV4ChannelEvent => Some(
                config
                    .channel_field
                    .clone()
                    .unwrap_or_else(|| "channel".into()),
            ),

            _ => config.channel_field.clone(),
        };

        Ok(CompiledSubscriptionAck {
            strategy,

            request_id_field: config.request_id_field.clone(),

            acknowledgement_field: config.acknowledgement_field.clone(),

            subscribe_acknowledgement: config.subscribe_acknowledgement.clone(),

            unsubscribe_acknowledgement: config.unsubscribe_acknowledgement.clone(),

            success_field: config.success_field.clone(),
            success_value,
            success_is_null: config.success_is_null.unwrap_or(false),

            error_field: config.error_field.clone(),
            error_event: config.error_event.clone(),
            error_code_field: config.error_code_field.clone(),
            error_message_field: config.error_message_field.clone(),

            channel_field,
        })
    }
}

// ============================================================
// CONFIGURATION VALIDATION
// ============================================================

fn validate_rules(
    config: &SubscriptionConfig,
    strategy: SubscriptionStrategy,
) -> Result<(), SubscriptionAckError> {
    let has_event = config.acknowledgement_field.is_some();

    let has_request_id = config.request_id_field.is_some();

    if !has_event && !has_request_id {
        return Err(invalid(
            "ACK requires an operation discriminator \
             or request ID field",
        ));
    }

    if has_event && config.subscribe_acknowledgement.is_none() {
        return Err(invalid(
            "ACK discriminator requires subscribe_acknowledgement",
        ));
    }

    if (config.subscribe_acknowledgement.is_some()
        || config.unsubscribe_acknowledgement.is_some()
        || config.error_event.is_some())
        && !has_event
    {
        return Err(invalid("ACK event values require acknowledgement_field"));
    }

    if config.success_is_null == Some(true) && config.success_value.is_some() {
        return Err(invalid("success_is_null conflicts with success_value"));
    }

    if (config.success_is_null == Some(true) || config.success_value.is_some())
        && config.success_field.is_none()
    {
        return Err(invalid("success matcher requires success_field"));
    }

    // A JSON-RPC reply identified only by an ID must also
    // contain positive application-success evidence.
    if strategy == SubscriptionStrategy::JsonRpc && !has_event && config.success_field.is_none() {
        return Err(invalid("JSON-RPC ACK requires a success field"));
    }

    Ok(())
}

// ============================================================
// HELPERS
// ============================================================

fn read<'a>(message: &'a Value, path: &str) -> Option<&'a Value> {
    JsonExtractor::optional(message, path).ok().flatten()
}

fn value_text(value: &Value) -> String {
    value
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| value.to_string())
}

fn invalid(reason: impl Into<String>) -> SubscriptionAckError {
    SubscriptionAckError::Configuration(reason.into())
}

#[cfg(test)]
mod tests {
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

    mod ack_identity {
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
    }
}
