use serde_json::Value;
use thiserror::Error;

use super::ack::ExpectedSubscriptionAck;
use super::identity::{SubscriptionAckIdentity, SubscriptionIdentityError};
use super::resolver::ResolvedSubscription;

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, Error)]
pub enum SubscriptionAckBindingError {
    #[error("ACK binding requires at least one identity field")]
    EmptyBinding,

    #[error("ACK binding has too many identity fields")]
    TooManyFields,

    #[error("invalid subscription argument path: {0}")]
    InvalidArgumentPath(String),

    #[error("invalid subscription ACK path: {0}")]
    InvalidAckPath(String),

    #[error("missing native argument at path: {0}")]
    MissingArgument(String),

    #[error("null native argument at path: {0}")]
    NullArgument(String),

    #[error("request ID must agree with the serialized request")]
    RequestIdMismatch,

    #[error(transparent)]
    Identity(#[from] SubscriptionIdentityError),
}

// ============================================================
// ARGUMENT LOCATION
// ============================================================

/// A field inside one resolved native subscription argument.
///
/// Examples:
///
/// Argument 0: {"channel":"books","instId":"BTC-USDT"}
///     ObjectField("channel")
///     ObjectField("instId")
///
/// Argument 0: "publicTrade.BTCUSDT"
///     WholeArgument
///
/// Argument 0: "BTC_USDT"
/// Argument 1: "100ms"
///     WholeArgument at index 0 or 1.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubscriptionArgumentField {
    WholeArgument,

    /// Dotted object-member path relative to the
    /// selected argument.
    ObjectField(String),
}

// ============================================================
// ONE IDENTITY MAPPING
// ============================================================

#[derive(Debug, Clone)]
pub struct SubscriptionAckFieldBinding {
    pub argument_index: usize,
    pub argument_field: SubscriptionArgumentField,
    pub acknowledgement_path: String,
}

impl SubscriptionAckFieldBinding {
    pub fn whole(argument_index: usize, acknowledgement_path: impl Into<String>) -> Self {
        Self {
            argument_index,
            argument_field: SubscriptionArgumentField::WholeArgument,
            acknowledgement_path: acknowledgement_path.into(),
        }
    }

    pub fn object(
        argument_index: usize,
        argument_field: impl Into<String>,
        acknowledgement_path: impl Into<String>,
    ) -> Self {
        Self {
            argument_index,
            argument_field: SubscriptionArgumentField::ObjectField(argument_field.into()),
            acknowledgement_path: acknowledgement_path.into(),
        }
    }
}

// ============================================================
// COMPILED BINDING
// ============================================================

/// Immutable binding between a native subscription request
/// and the identity fields expected in its ACK.
///
/// A binding must be selected from verified protocol
/// configuration or an explicitly validated protocol layout.
///
/// The binding does not guess response paths based on an
/// exchange name.
#[derive(Debug, Clone)]
pub struct CompiledSubscriptionAckBinding {
    fields: Vec<SubscriptionAckFieldBinding>,
}

impl CompiledSubscriptionAckBinding {
    pub fn new(
        fields: Vec<SubscriptionAckFieldBinding>,
    ) -> Result<Self, SubscriptionAckBindingError> {
        if fields.is_empty() {
            return Err(SubscriptionAckBindingError::EmptyBinding);
        }

        const MAX_FIELDS: usize = 16;

        if fields.len() > MAX_FIELDS {
            return Err(SubscriptionAckBindingError::TooManyFields);
        }

        // Validate the configured paths at construction,
        // rather than accepting malformed mappings until
        // the first subscription is created.
        let mut seen_ack_paths = std::collections::BTreeSet::new();

        for field in &fields {
            validate_path(&field.acknowledgement_path).map_err(|_| {
                SubscriptionAckBindingError::InvalidAckPath(field.acknowledgement_path.clone())
            })?;

            if let SubscriptionArgumentField::ObjectField(path) = &field.argument_field {
                validate_path(path)
                    .map_err(|_| SubscriptionAckBindingError::InvalidArgumentPath(path.clone()))?;
            }

            if !seen_ack_paths.insert(field.acknowledgement_path.as_str()) {
                return Err(SubscriptionAckBindingError::InvalidAckPath(
                    field.acknowledgement_path.clone(),
                ));
            }
        }

        Ok(Self { fields })
    }

    // ========================================================
    // DERIVE EXACT NATIVE IDENTITY
    // ========================================================

    pub fn derive_identity(
        &self,
        resolved: &ResolvedSubscription,
    ) -> Result<SubscriptionAckIdentity, SubscriptionAckBindingError> {
        let mut builder = SubscriptionAckIdentity::builder();

        for mapping in &self.fields {
            let argument = resolved
                .request
                .arguments
                .get(mapping.argument_index)
                .ok_or_else(|| {
                    SubscriptionAckBindingError::MissingArgument(format!(
                        "arguments[{}]",
                        mapping.argument_index
                    ))
                })?;

            let value = match &mapping.argument_field {
                SubscriptionArgumentField::WholeArgument => argument,

                SubscriptionArgumentField::ObjectField(path) => read_object_path(argument, path)
                    .ok_or_else(|| {
                        SubscriptionAckBindingError::MissingArgument(format!(
                            "arguments[{}].{}",
                            mapping.argument_index, path
                        ))
                    })?,
            };

            if value.is_null() {
                return Err(SubscriptionAckBindingError::NullArgument(
                    mapping.acknowledgement_path.clone(),
                ));
            }

            builder = builder.field(mapping.acknowledgement_path.clone(), value.clone())?;
        }

        Ok(builder.build()?)
    }

    // ========================================================
    // CREATE ACK EXPECTATION
    // ========================================================

    /// Derive the expected ACK identity from the same
    /// resolved arguments used by the serializer.
    ///
    /// `resolved.request.request_id` remains the sole
    /// request-ID source.
    ///
    /// The binding never creates or modifies an ID.
    pub fn expected_subscribe(
        &self,
        resolved: &ResolvedSubscription,
    ) -> Result<ExpectedSubscriptionAck, SubscriptionAckBindingError> {
        let identity = self.derive_identity(resolved)?;

        Ok(
            ExpectedSubscriptionAck::subscribe(resolved.request.request_id.clone())
                .with_identity(identity),
        )
    }

    pub fn expected_unsubscribe(
        &self,
        resolved: &ResolvedSubscription,
    ) -> Result<ExpectedSubscriptionAck, SubscriptionAckBindingError> {
        let identity = self.derive_identity(resolved)?;

        Ok(
            ExpectedSubscriptionAck::unsubscribe(resolved.request.request_id.clone())
                .with_identity(identity),
        )
    }
}

// ============================================================
// PATH HELPERS
// ============================================================

fn validate_path(path: &str) -> Result<(), ()> {
    if path.is_empty()
        || path.trim() != path
        || path.split('.').any(|segment| {
            segment.is_empty()
                || !segment
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        })
    {
        return Err(());
    }

    Ok(())
}

fn read_object_path<'a>(argument: &'a Value, path: &str) -> Option<&'a Value> {
    let mut current = argument;

    for segment in path.split('.') {
        current = current.as_object()?.get(segment)?;
    }

    Some(current)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::live::{
        config::exchange::ExchangeConfig,
        infrastructure::websocket::{
            CompiledSubscriptionAckBinding, SubscriptionAckClassification, SubscriptionAckCompiler,
            SubscriptionAckFieldBinding, SubscriptionIdentityMatch, SubscriptionResolutionContext,
            SubscriptionResolver,
        },
    };

    // ============================================================
    // CONFIGURATION
    // ============================================================

    fn config(subscription: &str, channel: &str) -> ExchangeConfig {
        let input = format!(
            r#"
schema_version = 1

[exchange]
id = "okx"
market = "spot"
profile = "okx_spot"

[websocket]
url = "wss://example.invalid"

{subscription}

[channels.trades]
enabled = true
format = "trades/okx_spot"

{channel}

[rest]
base_url = "https://example.invalid"
"#
        );

        toml::from_str(&input).unwrap()
    }

    // ============================================================
    // OKX-STYLE OBJECT ARGUMENTS
    // ============================================================

    fn object_config() -> ExchangeConfig {
        config(
            r#"
[websocket.subscription]
strategy = "json_operation"
subscribe_method = "subscribe"
operation_field = "op"
arguments_field = "args"

request_id_field = "id"
request_id_type = "string"
request_id_optional = true

acknowledgement_field = "event"
subscribe_acknowledgement = "subscribe"
unsubscribe_acknowledgement = "unsubscribe"
"#,
            r#"
channel = "trades-all"
subscription_template = '{"channel":"trades-all","instId":"{symbol}"}'
"#,
        )
    }

    fn object_binding() -> CompiledSubscriptionAckBinding {
        CompiledSubscriptionAckBinding::new(vec![
            SubscriptionAckFieldBinding::object(0, "channel", "arg.channel"),
            SubscriptionAckFieldBinding::object(0, "instId", "arg.instId"),
        ])
        .unwrap()
    }

    // ============================================================
    // DERIVED IDENTITY MATCHES OUTGOING INSTRUMENT
    // ============================================================

    #[test]
    fn derives_identity_from_resolved_native_arguments() {
        let exchange = object_config();

        let context = SubscriptionResolutionContext::new("BTC-USDT").unwrap();

        let mut resolved = SubscriptionResolver::resolve(&exchange, "trades", &context).unwrap();

        resolved.request.request_id = Some(json!("req-42"));

        let binding = object_binding();

        let expected = binding.expected_subscribe(&resolved).unwrap();

        assert_eq!(expected.request_id, Some(json!("req-42")),);

        let identity = expected.identity.as_ref().unwrap();

        assert_eq!(
            identity.classify(&json!({
                "arg": {
                    "channel": "trades-all",
                    "instId": "BTC-USDT"
                }
            })),
            SubscriptionIdentityMatch::Matched,
        );

        assert_eq!(
            identity.classify(&json!({
                "arg": {
                    "channel": "trades-all",
                    "instId": "ETH-USDT"
                }
            })),
            SubscriptionIdentityMatch::Mismatched,
        );
    }

    // ============================================================
    // REQUEST AND EXPECTATION ARE CONSISTENT
    // ============================================================

    #[test]
    fn serialized_request_and_ack_identity_agree() {
        let exchange = object_config();

        let mut resolved = SubscriptionResolver::resolve(
            &exchange,
            "trades",
            &SubscriptionResolutionContext::new("BTC-USDT").unwrap(),
        )
        .unwrap();

        resolved.request.request_id = Some(json!("req-7"));

        let wire = resolved.protocol.subscribe(&resolved.request).unwrap();

        let native: serde_json::Value = serde_json::from_str(&wire).unwrap();

        let expected = object_binding().expected_subscribe(&resolved).unwrap();

        let argument = &native["args"][0];

        assert_eq!(
            expected
                .identity
                .as_ref()
                .unwrap()
                .fields()
                .get("arg.channel"),
            Some(&argument["channel"]),
        );

        assert_eq!(
            expected
                .identity
                .as_ref()
                .unwrap()
                .fields()
                .get("arg.instId"),
            Some(&argument["instId"]),
        );

        let protocol = SubscriptionAckCompiler::compile(&exchange).unwrap();

        assert_eq!(
            protocol.classify(
                &json!({
                    "id": "req-7",
                    "event": "subscribe",
                    "arg": {
                        "channel": "trades-all",
                        "instId": "BTC-USDT"
                    }
                }),
                &expected,
            ),
            SubscriptionAckClassification::Confirmed,
        );
    }

    // ============================================================
    // DIFFERENT SYMBOL PRODUCES DIFFERENT ACK EXPECTATION
    // ============================================================

    #[test]
    fn changing_instrument_changes_expected_identity() {
        let exchange = object_config();
        let binding = object_binding();

        let btc = SubscriptionResolver::resolve(
            &exchange,
            "trades",
            &SubscriptionResolutionContext::new("BTC-USDT").unwrap(),
        )
        .unwrap();

        let eth = SubscriptionResolver::resolve(
            &exchange,
            "trades",
            &SubscriptionResolutionContext::new("ETH-USDT").unwrap(),
        )
        .unwrap();

        let btc_identity = binding.derive_identity(&btc).unwrap();
        let eth_identity = binding.derive_identity(&eth).unwrap();

        assert_ne!(btc_identity, eth_identity);
    }

    // ============================================================
    // STRING-ARGUMENT PROTOCOL
    // ============================================================

    #[test]
    fn derives_identity_from_string_argument() {
        let exchange = config(
            r#"
[websocket.subscription]
strategy = "json_operation"
subscribe_method = "subscribe"
operation_field = "op"
arguments_field = "args"
"#,
            r#"
stream_template = "publicTrade.{symbol_upper}"
"#,
        );

        let resolved = SubscriptionResolver::resolve(
            &exchange,
            "trades",
            &SubscriptionResolutionContext::new("BTCUSDT").unwrap(),
        )
        .unwrap();

        let binding =
            CompiledSubscriptionAckBinding::new(vec![SubscriptionAckFieldBinding::whole(
                0, "topic",
            )])
            .unwrap();

        let identity = binding.derive_identity(&resolved).unwrap();

        assert_eq!(
            identity.fields().get("topic"),
            Some(&json!("publicTrade.BTCUSDT")),
        );
    }

    // ============================================================
    // POSITIONAL PAYLOAD
    // ============================================================

    #[test]
    fn derives_identity_from_positional_arguments() {
        let exchange = config(
            r#"
[websocket.subscription]
strategy = "gate_v4_channel_event"
"#,
            r#"
channel = "spot.trades"
subscription_payload = ["{symbol}"]
"#,
        );

        let resolved = SubscriptionResolver::resolve(
            &exchange,
            "trades",
            &SubscriptionResolutionContext::new("BTC_USDT").unwrap(),
        )
        .unwrap();

        let binding =
            CompiledSubscriptionAckBinding::new(vec![SubscriptionAckFieldBinding::whole(
                0,
                "result.currency_pair",
            )])
            .unwrap();

        let identity = binding.derive_identity(&resolved).unwrap();

        assert_eq!(
            identity.fields().get("result.currency_pair"),
            Some(&json!("BTC_USDT")),
        );
    }

    // ============================================================
    // MISSING ARGUMENT FAILS CLOSED
    // ============================================================

    #[test]
    fn missing_argument_mapping_is_rejected() {
        let exchange = object_config();

        let resolved = SubscriptionResolver::resolve(
            &exchange,
            "trades",
            &SubscriptionResolutionContext::new("BTC-USDT").unwrap(),
        )
        .unwrap();

        let binding =
            CompiledSubscriptionAckBinding::new(vec![SubscriptionAckFieldBinding::object(
                0,
                "nonexistent",
                "arg.nonexistent",
            )])
            .unwrap();

        assert!(binding.derive_identity(&resolved).is_err());
    }

    // ============================================================
    // INVALID BINDING FAILS DURING CONSTRUCTION
    // ============================================================

    #[test]
    fn invalid_ack_path_is_rejected() {
        assert!(
            CompiledSubscriptionAckBinding::new(vec![SubscriptionAckFieldBinding::whole(
                0,
                "arg..symbol",
            ),])
            .is_err()
        );
    }

    #[test]
    fn duplicate_ack_paths_are_rejected() {
        assert!(
            CompiledSubscriptionAckBinding::new(vec![
                SubscriptionAckFieldBinding::object(0, "channel", "arg.channel",),
                SubscriptionAckFieldBinding::object(0, "instId", "arg.channel",),
            ])
            .is_err()
        );
    }
}
