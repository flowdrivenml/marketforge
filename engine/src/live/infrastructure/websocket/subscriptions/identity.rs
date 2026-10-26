use std::collections::BTreeMap;

use serde_json::Value;
use thiserror::Error;

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SubscriptionIdentityError {
    #[error("ACK identity requires at least one field")]
    EmptyIdentity,

    #[error("invalid ACK identity path: {0}")]
    InvalidPath(String),

    #[error("ACK identity field cannot be null: {0}")]
    NullValue(String),

    #[error("conflicting ACK identity field: {0}")]
    ConflictingField(String),
}

// ============================================================
// MATCH RESULT
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscriptionIdentityMatch {
    /// All required native identity fields matched.
    Matched,

    /// An identity field was present but differed.
    Mismatched,

    /// One or more required identity fields were missing.
    ///
    /// Missing identity is not evidence of success.
    Incomplete,
}

// ============================================================
// NATIVE ACK IDENTITY
// ============================================================

/// Exact exchange-native subscription identity.
///
/// Each entry maps an acknowledgment JSON path to its
/// expected value.
///
/// Examples:
///
/// OKX:
///     arg.channel = "books"
///     arg.instId  = "BTC-USDT"
///
/// Bitget:
///     arg.instType = "spot"
///     arg.topic    = "books"
///     arg.symbol   = "BTCUSDT"
///
/// Gate:
///     channel = "spot.trades"
///     result.payload = ["BTC_USDT"]
///
/// These mappings describe protocol shapes, not exchanges.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionAckIdentity {
    fields: BTreeMap<String, Value>,
}

impl SubscriptionAckIdentity {
    pub fn new(fields: BTreeMap<String, Value>) -> Result<Self, SubscriptionIdentityError> {
        if fields.is_empty() {
            return Err(SubscriptionIdentityError::EmptyIdentity);
        }

        for (path, value) in &fields {
            validate_path(path)?;

            if value.is_null() {
                return Err(SubscriptionIdentityError::NullValue(path.clone()));
            }
        }

        Ok(Self { fields })
    }

    pub fn builder() -> SubscriptionAckIdentityBuilder {
        SubscriptionAckIdentityBuilder::new()
    }

    pub fn fields(&self) -> &BTreeMap<String, Value> {
        &self.fields
    }

    // ========================================================
    // MATCH ONE NATIVE RESPONSE
    // ========================================================

    /// Match every required native identity field.
    ///
    /// A response with the right operation but wrong
    /// instrument must never confirm the subscription.
    ///
    /// Missing fields produce Incomplete, not Matched.
    ///
    /// This method does not validate:
    /// - ACK operation
    /// - Request ID
    /// - Exchange success/error status
    /// - Connection generation
    ///
    /// Those checks remain independent requirements.
    pub fn classify(&self, response: &Value) -> SubscriptionIdentityMatch {
        let mut incomplete = false;

        for (path, expected) in &self.fields {
            match read_path(response, path) {
                Some(actual) if actual == expected => {}

                Some(_) => {
                    return SubscriptionIdentityMatch::Mismatched;
                }

                None => {
                    incomplete = true;
                }
            }
        }

        if incomplete {
            SubscriptionIdentityMatch::Incomplete
        } else {
            SubscriptionIdentityMatch::Matched
        }
    }
}

// ============================================================
// BUILDER
// ============================================================

#[derive(Debug, Default)]
pub struct SubscriptionAckIdentityBuilder {
    fields: BTreeMap<String, Value>,
}

impl SubscriptionAckIdentityBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn field(
        mut self,
        path: impl Into<String>,
        expected: Value,
    ) -> Result<Self, SubscriptionIdentityError> {
        let path = path.into();

        validate_path(&path)?;

        if expected.is_null() {
            return Err(SubscriptionIdentityError::NullValue(path));
        }

        if let Some(previous) = self.fields.get(&path) {
            if previous != &expected {
                return Err(SubscriptionIdentityError::ConflictingField(path));
            }

            return Ok(self);
        }

        self.fields.insert(path, expected);

        Ok(self)
    }

    pub fn build(self) -> Result<SubscriptionAckIdentity, SubscriptionIdentityError> {
        SubscriptionAckIdentity::new(self.fields)
    }
}

// ============================================================
// PATH VALIDATION
// ============================================================

fn validate_path(path: &str) -> Result<(), SubscriptionIdentityError> {
    if path.is_empty()
        || path.trim() != path
        || path.split('.').any(|segment| {
            segment.is_empty()
                || !segment
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        })
    {
        return Err(SubscriptionIdentityError::InvalidPath(path.to_owned()));
    }

    Ok(())
}

// ============================================================
// JSON PATH EXTRACTION
// ============================================================

/// Read a dotted object path without coercion.
///
/// Missing and null-valued fields remain distinguishable.
///
/// Arrays are compared as values; this helper does not
/// interpret a dotted numeric segment as an array index.
fn read_path<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
    let mut current = value;

    for segment in path.split('.') {
        current = current.as_object()?.get(segment)?;
    }

    Some(current)
}

#[cfg(test)]
mod tests;
