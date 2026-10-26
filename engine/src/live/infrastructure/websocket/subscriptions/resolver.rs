use std::collections::BTreeMap;

use serde_json::Value;
use thiserror::Error;

use crate::live::config::exchange::ExchangeConfig;

use super::protocol::{
    CompiledSubscriptionProtocol, SubscriptionProtocolCompiler, SubscriptionProtocolError,
    SubscriptionRequest, SubscriptionStrategy,
};

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, Error)]
pub enum SubscriptionResolutionError {
    #[error("unknown channel: {0}")]
    UnknownChannel(String),

    #[error("channel is disabled: {0}")]
    DisabledChannel(String),

    #[error("missing native subscription template for {0}")]
    MissingTemplate(String),

    #[error("invalid native symbol")]
    InvalidSymbol,

    #[error("unresolved template variable: {0}")]
    MissingVariable(String),

    #[error("invalid template syntax")]
    InvalidTemplate,

    #[error("invalid JSON subscription template: {0}")]
    InvalidJson(String),

    #[error(transparent)]
    Protocol(#[from] SubscriptionProtocolError),
}

// ============================================================
// EXPLICIT INSTRUMENT CONTEXT
// ============================================================

/// Native instrument identity and explicitly selected
/// subscription parameters.
///
/// The native symbol must come from the authoritative
/// instrument metadata/resolution layer.
///
/// No exchange-specific symbol conversion occurs here.
#[derive(Debug, Clone)]
pub struct SubscriptionResolutionContext {
    variables: BTreeMap<String, String>,
}

impl SubscriptionResolutionContext {
    pub fn new(native_symbol: impl Into<String>) -> Result<Self, SubscriptionResolutionError> {
        let symbol = native_symbol.into();

        if symbol.is_empty() || symbol.trim() != symbol {
            return Err(SubscriptionResolutionError::InvalidSymbol);
        }

        let mut variables = BTreeMap::new();

        variables.insert("symbol".into(), symbol.clone());
        variables.insert("symbol_lower".into(), symbol.to_ascii_lowercase());
        variables.insert("symbol_upper".into(), symbol.to_ascii_uppercase());

        Ok(Self { variables })
    }

    /// Provide explicit subscription parameters such as
    /// depth, interval, instrument type, or native category.
    ///
    /// Values are supplied by validated upstream metadata
    /// and channel selection.
    pub fn with_variable(
        mut self,
        name: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<Self, SubscriptionResolutionError> {
        let name = name.into();
        let value = value.into();

        if name.is_empty()
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        {
            return Err(SubscriptionResolutionError::InvalidTemplate);
        }

        // Reserved symbol identities cannot be overwritten.
        if matches!(name.as_str(), "symbol" | "symbol_lower" | "symbol_upper") {
            return Err(SubscriptionResolutionError::InvalidTemplate);
        }

        self.variables.insert(name, value);

        Ok(self)
    }

    fn expand(&self, template: &str) -> Result<String, SubscriptionResolutionError> {
        let mut output = String::with_capacity(template.len());
        let mut cursor = 0;

        while let Some(offset) = template[cursor..].find('{') {
            let start = cursor + offset;

            output.push_str(&template[cursor..start]);

            let remaining = &template[start + 1..];

            let Some(end_offset) = remaining.find('}') else {
                return Err(SubscriptionResolutionError::InvalidTemplate);
            };

            let name = &remaining[..end_offset];

            if name.is_empty()
                || !name
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
            {
                return Err(SubscriptionResolutionError::InvalidTemplate);
            }

            let value = self
                .variables
                .get(name)
                .ok_or_else(|| SubscriptionResolutionError::MissingVariable(name.to_owned()))?;

            output.push_str(value);

            cursor = start + 1 + end_offset + 1;
        }

        output.push_str(&template[cursor..]);

        Ok(output)
    }

    fn expand_json_value(&self, value: &mut Value) -> Result<(), SubscriptionResolutionError> {
        match value {
            Value::String(text) => {
                *text = self.expand(text)?;
            }

            Value::Array(items) => {
                for item in items {
                    self.expand_json_value(item)?;
                }
            }

            Value::Object(fields) => {
                for value in fields.values_mut() {
                    self.expand_json_value(value)?;
                }
            }

            _ => {}
        }

        Ok(())
    }
}

// ============================================================
// RESOLVED SUBSCRIPTION
// ============================================================

#[derive(Debug)]
pub struct ResolvedSubscription {
    pub channel_name: String,
    pub route: Option<String>,
    pub protocol: CompiledSubscriptionProtocol,
    pub request: SubscriptionRequest,
}

// ============================================================
// RESOLVER
// ============================================================

pub struct SubscriptionResolver;

impl SubscriptionResolver {
    pub fn resolve(
        exchange: &ExchangeConfig,
        channel_name: &str,
        context: &SubscriptionResolutionContext,
    ) -> Result<ResolvedSubscription, SubscriptionResolutionError> {
        let channel = exchange
            .channels
            .get(channel_name)
            .ok_or_else(|| SubscriptionResolutionError::UnknownChannel(channel_name.to_owned()))?;

        if !channel.enabled {
            return Err(SubscriptionResolutionError::DisabledChannel(
                channel_name.to_owned(),
            ));
        }

        let protocol = SubscriptionProtocolCompiler::compile(exchange, channel_name)?;

        let arguments = match protocol.strategy() {
            SubscriptionStrategy::JsonRpc => {
                let template = channel
                    .stream_template
                    .as_deref()
                    .ok_or_else(|| missing(channel_name))?;

                vec![Value::String(context.expand(template)?)]
            }

            SubscriptionStrategy::JsonOperation => {
                match (
                    channel.subscription_template.as_deref(),
                    channel.stream_template.as_deref(),
                ) {
                    // Structured JSON arguments.
                    //
                    // Used by protocols whose native subscription
                    // argument is an object.
                    (Some(template), _) => {
                        let mut value: Value = serde_json::from_str(template).map_err(|error| {
                            SubscriptionResolutionError::InvalidJson(error.to_string())
                        })?;

                        context.expand_json_value(&mut value)?;

                        vec![value]
                    }

                    // String-based arguments.
                    //
                    // Several JSON-operation protocols use an array
                    // of native topic strings rather than objects.
                    //
                    // The surrounding WebSocket request is still
                    // serialized by CompiledSubscriptionProtocol.
                    (None, Some(template)) => {
                        vec![Value::String(context.expand(template)?)]
                    }

                    (None, None) => {
                        return Err(missing(channel_name));
                    }
                }
            }

            SubscriptionStrategy::GateV4ChannelEvent => {
                let payload = channel
                    .subscription_payload
                    .as_ref()
                    .ok_or_else(|| missing(channel_name))?;

                let mut arguments = Vec::with_capacity(payload.len());

                for template in payload {
                    arguments.push(Value::String(context.expand(template)?));
                }

                arguments
            }
        };

        if arguments.is_empty() {
            return Err(missing(channel_name));
        }

        Ok(ResolvedSubscription {
            channel_name: channel_name.to_owned(),
            route: channel.route.clone(),
            protocol,
            request: SubscriptionRequest::new(arguments),
        })
    }
}

fn missing(channel_name: &str) -> SubscriptionResolutionError {
    SubscriptionResolutionError::MissingTemplate(channel_name.to_owned())
}
