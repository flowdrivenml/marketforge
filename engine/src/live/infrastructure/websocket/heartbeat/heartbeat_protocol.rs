use serde_json::Value;

use crate::live::{
    config::exchange::{HeartbeatConfig, HeartbeatStrategy},
    infrastructure::normalization::JsonExtractor,
};

use super::WebSocketTransportError;

#[derive(Debug, Clone)]
enum ResponseMatcher {
    Text(String),
    JsonExact(Value),
    JsonFields(Vec<(String, Value)>),
}
use std::time::Duration;

/// Executable client-initiated application heartbeat policy.
///
/// The protocol determines what to send and how to recognize
/// the response. This policy additionally determines when
/// to send it and when to declare the connection unhealthy.
#[derive(Debug, Clone)]
pub struct ApplicationHeartbeatPolicy {
    pub protocol: HeartbeatProtocol,
    pub interval: Duration,
    pub response_timeout: Duration,
    pub send_timeout: Duration,
}

impl ApplicationHeartbeatPolicy {
    pub fn new(
        protocol: HeartbeatProtocol,
        interval: Duration,
        response_timeout: Duration,
        send_timeout: Duration,
    ) -> Result<Self, WebSocketTransportError> {
        if interval.is_zero() || response_timeout.is_zero() || send_timeout.is_zero() {
            return Err(WebSocketTransportError::Configuration(
                "application heartbeat durations must be positive".into(),
            ));
        }

        Ok(Self {
            protocol,
            interval,
            response_timeout,
            send_timeout,
        })
    }

    pub fn from_config(config: &HeartbeatConfig) -> Result<Self, WebSocketTransportError> {
        let protocol = HeartbeatProtocol::compile(config)?;

        let interval = config.interval_seconds.ok_or_else(|| {
            WebSocketTransportError::Configuration(
                "application heartbeat requires interval_seconds".into(),
            )
        })?;

        let response_timeout = config
            .response_timeout_seconds
            .or(config.timeout_seconds)
            .ok_or_else(|| {
                WebSocketTransportError::Configuration(
                    "application heartbeat requires a response deadline".into(),
                )
            })?;

        Self::new(
            protocol,
            Duration::from_secs(interval),
            Duration::from_secs(response_timeout),
            Duration::from_secs(response_timeout),
        )
    }
}
/// Immutable, exchange-independent application heartbeat plan.
///
/// A plan can:
/// - Produce the configured outbound heartbeat.
/// - Identify its response among unrelated incoming messages.
///
/// It does not own scheduling, socket I/O, or deadlines.
#[derive(Debug, Clone)]
pub struct HeartbeatProtocol {
    request: String,
    matcher: ResponseMatcher,
}

impl HeartbeatProtocol {
    pub fn compile(config: &HeartbeatConfig) -> Result<Self, WebSocketTransportError> {
        validate_matcher_fields(config)?;
        if config.enabled == Some(false) {
            return Err(invalid("heartbeat is disabled"));
        }

        let request = config
            .request
            .as_deref()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| invalid("application heartbeat requires request"))?
            .to_owned();

        let fields = response_fields(config);

        let matcher = match config.strategy {
            HeartbeatStrategy::TextPingPong => {
                if !fields.is_empty() {
                    return Err(invalid("text heartbeat cannot use JSON response fields"));
                }

                let response = config
                    .response
                    .as_deref()
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| invalid("text heartbeat requires response"))?;

                ResponseMatcher::Text(response.to_owned())
            }

            HeartbeatStrategy::JsonPingPong => {
                let parsed_request: Value = serde_json::from_str(&request).map_err(|error| {
                    invalid(format!("heartbeat request is not valid JSON: {error}"))
                })?;

                if !parsed_request.is_object() {
                    return Err(invalid("JSON heartbeat request must be an object"));
                }

                match (&config.response, fields.is_empty()) {
                    (Some(response), true) => {
                        let expected: Value = serde_json::from_str(response).map_err(|error| {
                            invalid(format!("heartbeat response is not valid JSON: {error}"))
                        })?;

                        if !expected.is_object() {
                            return Err(invalid("JSON heartbeat response must be an object"));
                        }

                        ResponseMatcher::JsonExact(expected)
                    }

                    (None, false) => ResponseMatcher::JsonFields(fields),

                    (Some(_), false) => {
                        return Err(invalid(
                            "configure either an exact JSON response \
                             or response field matchers, not both",
                        ));
                    }

                    (None, true) => {
                        return Err(invalid("JSON heartbeat requires a response matcher"));
                    }
                }
            }

            HeartbeatStrategy::WebsocketControlFrames => {
                return Err(invalid("control-frame heartbeat uses HeartbeatPolicy"));
            }

            HeartbeatStrategy::JsonChannelPing => {
                return Err(invalid(
                    "json_channel_ping requires a compiled \
                     channel-message serializer; the current \
                     configuration does not fully specify it",
                ));
            }
        };

        Ok(Self { request, matcher })
    }

    /// Exact text payload to send through the WebSocket writer.
    pub fn request_text(&self) -> &str {
        &self.request
    }

    /// Return true only for a configured heartbeat response.
    ///
    /// Unrelated market-data messages must return false
    /// and remain available for normal routing.
    pub fn matches_response(&self, text: &str) -> bool {
        match &self.matcher {
            ResponseMatcher::Text(expected) => text == expected,

            ResponseMatcher::JsonExact(expected) => {
                serde_json::from_str::<Value>(text).is_ok_and(|actual| actual == *expected)
            }

            ResponseMatcher::JsonFields(fields) => {
                let Ok(message) = serde_json::from_str::<Value>(text) else {
                    return false;
                };

                fields.iter().all(|(path, expected)| {
                    JsonExtractor::optional(&message, path)
                        .ok()
                        .flatten()
                        .is_some_and(|actual| actual == expected)
                })
            }
        }
    }
}

fn response_fields(config: &HeartbeatConfig) -> Vec<(String, Value)> {
    let mut fields = Vec::new();

    if let (Some(path), Some(value)) = (&config.response_op_field, &config.response_op_value) {
        fields.push((path.clone(), Value::String(value.clone())));
    }

    if let (Some(path), Some(value)) = (
        &config.response_success_field,
        config.response_success_value,
    ) {
        fields.push((path.clone(), Value::Bool(value)));
    }

    if let (Some(path), Some(value)) = (
        &config.response_message_field,
        &config.response_message_value,
    ) {
        fields.push((path.clone(), Value::String(value.clone())));
    }

    fields
}

fn invalid(message: impl Into<String>) -> WebSocketTransportError {
    WebSocketTransportError::Configuration(message.into())
}

fn validate_matcher_fields(config: &HeartbeatConfig) -> Result<(), WebSocketTransportError> {
    for (name, path, expected) in [
        (
            "response_op",
            config.response_op_field.as_ref(),
            config.response_op_value.as_ref().map(|_| ()),
        ),
        (
            "response_success",
            config.response_success_field.as_ref(),
            config.response_success_value.map(|_| ()),
        ),
        (
            "response_message",
            config.response_message_field.as_ref(),
            config.response_message_value.as_ref().map(|_| ()),
        ),
    ] {
        match (path, expected) {
            (None, None) => {}

            (Some(path), Some(())) if !path.trim().is_empty() => {
                let probe = serde_json::json!({});

                // Validate supported dotted-path syntax without
                // requiring the path to exist in the probe.
                JsonExtractor::optional(&probe, path)
                    .map_err(|error| invalid(format!("{name}: invalid response path: {error}")))?;
            }

            _ => {
                return Err(invalid(format!(
                    "{name}: field and expected value \
                     must be configured together"
                )));
            }
        }
    }

    Ok(())
}
