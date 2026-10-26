use serde_json::{Map, Value};
use thiserror::Error;

use crate::live::config::exchange::{ExchangeConfig, SubscriptionConfig};

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SubscriptionProtocolError {
    #[error("missing subscription configuration")]
    MissingConfiguration,

    #[error("unsupported subscription strategy: {0}")]
    UnsupportedStrategy(String),

    #[error("invalid subscription configuration: {0}")]
    Configuration(String),

    #[error("subscription arguments cannot be empty")]
    EmptyArguments,

    #[error("subscription request ID is required")]
    MissingRequestId,

    #[error("subscription request ID has the wrong type")]
    InvalidRequestId,

    #[error("subscription payload exceeds configured limit")]
    PayloadTooLarge,

    #[error("subscription request contains too many arguments")]
    TooManyArguments,

    #[error("subscription serialization failed: {0}")]
    Serialization(String),
}

// ============================================================
// STRATEGIES
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscriptionStrategy {
    JsonRpc,
    JsonOperation,
    GateV4ChannelEvent,
}

// ============================================================
// REQUEST INPUT
// ============================================================

/// Fully resolved exchange-native subscription arguments.
///
/// Arguments are supplied by the future subscription
/// resolution layer, not inferred by the serializer.
///
/// Examples:
///
/// Binance:
///     ["btcusdt@trade"]
///
/// Bybit:
///     ["publicTrade.BTCUSDT"]
///
/// OKX:
///     [{"channel":"trades-all","instId":"BTC-USDT"}]
///
/// Gate:
///     ["BTC_USDT"]
#[derive(Debug, Clone)]
pub struct SubscriptionRequest {
    pub arguments: Vec<Value>,
    pub request_id: Option<Value>,

    /// Required only by protocols containing a native
    /// Unix-seconds timestamp field.
    pub unix_seconds: Option<u64>,
}

impl SubscriptionRequest {
    pub fn new(arguments: Vec<Value>) -> Self {
        Self {
            arguments,
            request_id: None,
            unix_seconds: None,
        }
    }

    pub fn with_request_id(mut self, id: Value) -> Self {
        self.request_id = Some(id);
        self
    }

    pub fn with_unix_seconds(mut self, seconds: u64) -> Self {
        self.unix_seconds = Some(seconds);
        self
    }
}

// ============================================================
// COMPILED PROTOCOL
// ============================================================

#[derive(Debug, Clone)]
pub struct CompiledSubscriptionProtocol {
    strategy: SubscriptionStrategy,

    subscribe_method: String,
    unsubscribe_method: Option<String>,

    operation_field: Option<String>,
    arguments_field: Option<String>,

    request_id_field: Option<String>,
    request_id_type: Option<String>,
    request_id_optional: bool,

    channel: Option<String>,

    max_arguments: Option<usize>,
    max_payload_bytes: Option<usize>,
}

impl CompiledSubscriptionProtocol {
    pub fn strategy(&self) -> SubscriptionStrategy {
        self.strategy
    }

    pub fn channel(&self) -> Option<&str> {
        self.channel.as_deref()
    }

    pub fn supports_unsubscribe(&self) -> bool {
        self.unsubscribe_method.is_some()
    }

    // ========================================================
    // SERIALIZE SUBSCRIBE
    // ========================================================

    pub fn subscribe(
        &self,
        request: &SubscriptionRequest,
    ) -> Result<String, SubscriptionProtocolError> {
        self.serialize(&self.subscribe_method, request)
    }

    // ========================================================
    // SERIALIZE UNSUBSCRIBE
    // ========================================================

    pub fn unsubscribe(
        &self,
        request: &SubscriptionRequest,
    ) -> Result<String, SubscriptionProtocolError> {
        let method = self.unsubscribe_method.as_deref().ok_or_else(|| {
            SubscriptionProtocolError::Configuration("unsubscribe is not configured".into())
        })?;

        self.serialize(method, request)
    }

    // ========================================================
    // SERIALIZATION
    // ========================================================

    fn serialize(
        &self,
        method: &str,
        request: &SubscriptionRequest,
    ) -> Result<String, SubscriptionProtocolError> {
        if request.arguments.is_empty() {
            return Err(SubscriptionProtocolError::EmptyArguments);
        }

        if self
            .max_arguments
            .is_some_and(|maximum| request.arguments.len() > maximum)
        {
            return Err(SubscriptionProtocolError::TooManyArguments);
        }

        self.validate_request_id(request.request_id.as_ref())?;

        let mut object = Map::new();

        match self.strategy {
            SubscriptionStrategy::JsonRpc => {
                object.insert("method".into(), Value::String(method.to_owned()));

                object.insert("params".into(), Value::Array(request.arguments.clone()));
            }

            SubscriptionStrategy::JsonOperation => {
                let operation = self
                    .operation_field
                    .as_ref()
                    .expect("compiled operation field");

                let arguments = self
                    .arguments_field
                    .as_ref()
                    .expect("compiled arguments field");

                object.insert(operation.clone(), Value::String(method.to_owned()));

                object.insert(arguments.clone(), Value::Array(request.arguments.clone()));
            }

            SubscriptionStrategy::GateV4ChannelEvent => {
                let channel = self.channel.as_ref().expect("compiled channel");

                let seconds = request.unix_seconds.ok_or_else(|| {
                    SubscriptionProtocolError::Configuration(
                        "channel-event subscription requires Unix seconds".into(),
                    )
                })?;

                object.insert("time".into(), Value::from(seconds));

                object.insert("channel".into(), Value::String(channel.clone()));

                object.insert("event".into(), Value::String(method.to_owned()));

                object.insert("payload".into(), Value::Array(request.arguments.clone()));
            }
        }

        if let (Some(field), Some(id)) =
            (self.request_id_field.as_ref(), request.request_id.as_ref())
        {
            object.insert(field.clone(), id.clone());
        }

        let serialized = serde_json::to_string(&object)
            .map_err(|error| SubscriptionProtocolError::Serialization(error.to_string()))?;

        if self
            .max_payload_bytes
            .is_some_and(|maximum| serialized.len() > maximum)
        {
            return Err(SubscriptionProtocolError::PayloadTooLarge);
        }

        Ok(serialized)
    }

    // ========================================================
    // REQUEST ID VALIDATION
    // ========================================================

    fn validate_request_id(&self, id: Option<&Value>) -> Result<(), SubscriptionProtocolError> {
        let Some(field) = &self.request_id_field else {
            if id.is_some() {
                return Err(SubscriptionProtocolError::InvalidRequestId);
            }

            return Ok(());
        };

        let Some(id) = id else {
            return if self.request_id_optional {
                Ok(())
            } else {
                Err(SubscriptionProtocolError::MissingRequestId)
            };
        };

        let correct = match self.request_id_type.as_deref() {
            Some("string") => id.as_str().is_some_and(|value| !value.is_empty()),

            Some("unsigned_integer") => id.as_u64().is_some(),

            Some("signed_integer") => id.as_i64().is_some(),

            None => !id.is_null(),

            _ => false,
        };

        if !correct {
            return Err(SubscriptionProtocolError::InvalidRequestId);
        }

        // Ensure the compiled field name is meaningful.
        debug_assert!(!field.is_empty());

        Ok(())
    }
}

// ============================================================
// COMPILER
// ============================================================

pub struct SubscriptionProtocolCompiler;

impl SubscriptionProtocolCompiler {
    pub fn compile(
        exchange: &ExchangeConfig,
        channel_name: &str,
    ) -> Result<CompiledSubscriptionProtocol, SubscriptionProtocolError> {
        let subscription = exchange
            .websocket
            .subscription
            .as_ref()
            .ok_or(SubscriptionProtocolError::MissingConfiguration)?;

        let channel = exchange.channels.get(channel_name).ok_or_else(|| {
            SubscriptionProtocolError::Configuration(format!("unknown channel '{channel_name}'"))
        })?;

        if !channel.enabled {
            return Err(SubscriptionProtocolError::Configuration(format!(
                "channel '{channel_name}' is disabled"
            )));
        }

        let strategy = match subscription.strategy.as_str() {
            "json_rpc" => SubscriptionStrategy::JsonRpc,

            "json_operation" => SubscriptionStrategy::JsonOperation,

            "gate_v4_channel_event" => SubscriptionStrategy::GateV4ChannelEvent,

            other => {
                return Err(SubscriptionProtocolError::UnsupportedStrategy(
                    other.to_owned(),
                ));
            }
        };

        let subscribe_method = match strategy {
            SubscriptionStrategy::GateV4ChannelEvent => subscription
                .subscribe_method
                .as_deref()
                .unwrap_or("subscribe"),

            _ => subscription
                .subscribe_method
                .as_deref()
                .ok_or_else(|| invalid("missing subscribe_method"))?,
        };

        validate_nonempty(subscribe_method, "subscribe_method")?;

        let unsubscribe_method = if subscription.supports_unsubscribe == Some(false) {
            None
        } else {
            match subscription.unsubscribe_method.as_deref() {
                Some(value) => {
                    validate_nonempty(value, "unsubscribe_method")?;
                    Some(value.to_owned())
                }

                None if strategy == SubscriptionStrategy::GateV4ChannelEvent => {
                    Some("unsubscribe".into())
                }

                None => None,
            }
        };

        let (operation_field, arguments_field) = match strategy {
            SubscriptionStrategy::JsonRpc => (None, None),

            SubscriptionStrategy::JsonOperation => (
                Some(required(
                    subscription.operation_field.as_deref(),
                    "operation_field",
                )?),
                Some(required(
                    subscription.arguments_field.as_deref(),
                    "arguments_field",
                )?),
            ),

            SubscriptionStrategy::GateV4ChannelEvent => (None, None),
        };

        let channel_identity = match strategy {
            SubscriptionStrategy::GateV4ChannelEvent => {
                Some(required(channel.channel.as_deref(), "channel.channel")?)
            }

            _ => None,
        };

        validate_request_id_config(subscription)?;

        let limits = exchange.websocket.limits.as_ref();

        let max_arguments = limits
            .and_then(|value| value.max_args_per_subscribe_request)
            .map(|value| usize::try_from(value))
            .transpose()
            .map_err(|_| invalid("argument limit overflows usize"))?;

        let max_payload_bytes = limits
            .and_then(|value| value.max_subscription_payload_bytes)
            .map(|value| usize::try_from(value))
            .transpose()
            .map_err(|_| invalid("payload limit overflows usize"))?;

        Ok(CompiledSubscriptionProtocol {
            strategy,
            subscribe_method: subscribe_method.to_owned(),
            unsubscribe_method,
            operation_field,
            arguments_field,
            request_id_field: subscription.request_id_field.clone(),
            request_id_type: subscription.request_id_type.clone(),
            request_id_optional: subscription.request_id_optional.unwrap_or(false),
            channel: channel_identity,
            max_arguments,
            max_payload_bytes,
        })
    }
}

// ============================================================
// VALIDATION HELPERS
// ============================================================

fn validate_request_id_config(
    config: &SubscriptionConfig,
) -> Result<(), SubscriptionProtocolError> {
    if let Some(field) = &config.request_id_field {
        validate_nonempty(field, "request_id_field")?;
    } else if config.request_id_type.is_some() || config.request_id_optional.is_some() {
        return Err(invalid(
            "request ID type/optionality requires request_id_field",
        ));
    }

    if let Some(kind) = config.request_id_type.as_deref() {
        if !matches!(kind, "string" | "signed_integer" | "unsigned_integer") {
            return Err(invalid(format!("unsupported request ID type '{kind}'")));
        }
    }

    Ok(())
}

fn required(value: Option<&str>, name: &str) -> Result<String, SubscriptionProtocolError> {
    let value = value.ok_or_else(|| invalid(format!("missing {name}")))?;

    validate_nonempty(value, name)?;

    Ok(value.to_owned())
}

fn validate_nonempty(value: &str, name: &str) -> Result<(), SubscriptionProtocolError> {
    if value.trim().is_empty() || value.trim() != value {
        return Err(invalid(format!("invalid {name}")));
    }

    Ok(())
}

fn invalid(reason: impl Into<String>) -> SubscriptionProtocolError {
    SubscriptionProtocolError::Configuration(reason.into())
}

#[cfg(test)]
mod tests;
