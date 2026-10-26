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
mod tests;
