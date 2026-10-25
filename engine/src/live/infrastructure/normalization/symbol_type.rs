use serde_json::Value;
use thiserror::Error;

// ============================================================
// SYMBOL TYPE VALIDATION ERRORS
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SymbolTypeValidationError {
    #[error("invalid symbol-type configuration: {0}")]
    Configuration(String),

    #[error("required native symbol-type field '{0}' is missing")]
    Missing(String),

    #[error("native symbol-type field '{0}' must be an unsigned integer")]
    InvalidType(String),

    #[error(
        "native symbol-type mismatch at '{path}': \
         received {actual}, expected one of {expected:?}"
    )]
    Mismatch {
        path: String,
        actual: u64,
        expected: Vec<u64>,
    },
}

// ============================================================
// MISSING FIELD POLICY
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SymbolTypeMissingPolicy {
    Allow,
    Reject,
}

// ============================================================
// COMPILED VALIDATOR
// ============================================================

/// Generic validator for exchange-native numeric
/// instrument discriminators.
///
/// The native field path and accepted values originate
/// from format configuration.
///
/// No exchange-specific logic is implemented here.
#[derive(Debug, Clone)]
pub struct SymbolTypeValidator {
    path: String,
    expected_values: Vec<u64>,
    missing_policy: SymbolTypeMissingPolicy,
}

impl SymbolTypeValidator {
    // ========================================================
    // CONSTRUCTION
    // ========================================================

    pub fn new(
        path: String,
        expected_values: Vec<u64>,
        missing_policy: SymbolTypeMissingPolicy,
    ) -> Result<Self, SymbolTypeValidationError> {
        if path.is_empty()
            || path.trim() != path
            || path == "$"
            || path.starts_with('$') && !path.starts_with("$.")
        {
            return Err(SymbolTypeValidationError::Configuration(format!(
                "invalid native field path '{path}'"
            )));
        }

        let normalized = path.strip_prefix("$.").unwrap_or(&path);

        if normalized.is_empty() || normalized.split('.').any(str::is_empty) {
            return Err(SymbolTypeValidationError::Configuration(format!(
                "invalid native field path '{path}'"
            )));
        }

        if expected_values.is_empty() {
            return Err(SymbolTypeValidationError::Configuration(
                "expected_values cannot be empty".into(),
            ));
        }

        let mut expected_values = expected_values;

        expected_values.sort_unstable();
        expected_values.dedup();

        Ok(Self {
            path,
            expected_values,
            missing_policy,
        })
    }

    // ========================================================
    // ACCESSORS
    // ========================================================

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn expected_values(&self) -> &[u64] {
        &self.expected_values
    }

    // ========================================================
    // VALIDATION
    // ========================================================

    /// Validate one native trade record.
    ///
    /// Missing is distinct from null.
    ///
    /// The following are rejected when a field is present:
    ///
    /// - Negative integers.
    /// - Floating-point numbers.
    /// - Numeric strings.
    /// - Boolean values.
    /// - Null.
    /// - Objects and arrays.
    ///
    /// Only exact JSON unsigned integers are accepted.
    pub fn validate(&self, record: &Value) -> Result<(), SymbolTypeValidationError> {
        let normalized = self.path.strip_prefix("$.").unwrap_or(&self.path);

        let mut current = record;

        for segment in normalized.split('.') {
            let Some(value) = current.as_object().and_then(|object| object.get(segment)) else {
                return match self.missing_policy {
                    SymbolTypeMissingPolicy::Allow => Ok(()),

                    SymbolTypeMissingPolicy::Reject => {
                        Err(SymbolTypeValidationError::Missing(self.path.clone()))
                    }
                };
            };

            current = value;
        }

        let actual = current
            .as_u64()
            .ok_or_else(|| SymbolTypeValidationError::InvalidType(self.path.clone()))?;

        if !self.expected_values.contains(&actual) {
            return Err(SymbolTypeValidationError::Mismatch {
                path: self.path.clone(),
                actual,
                expected: self.expected_values.clone(),
            });
        }

        Ok(())
    }
}

// ============================================================
// UNIT TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn validator() -> SymbolTypeValidator {
        SymbolTypeValidator::new("st".into(), vec![1], SymbolTypeMissingPolicy::Allow).unwrap()
    }

    #[test]
    fn accepts_expected_symbol_type() {
        assert!(validator().validate(&json!({"st": 1})).is_ok());
    }

    #[test]
    fn rejects_wrong_symbol_type() {
        assert!(matches!(
            validator().validate(&json!({"st": 2})),
            Err(SymbolTypeValidationError::Mismatch { .. })
        ));
    }

    #[test]
    fn allows_missing_when_configured() {
        assert!(validator().validate(&json!({})).is_ok());
    }

    #[test]
    fn rejects_missing_when_required() {
        let validator =
            SymbolTypeValidator::new("st".into(), vec![1], SymbolTypeMissingPolicy::Reject)
                .unwrap();

        assert!(matches!(
            validator.validate(&json!({})),
            Err(SymbolTypeValidationError::Missing(_))
        ));
    }

    #[test]
    fn rejects_null_even_when_missing_is_allowed() {
        assert!(matches!(
            validator().validate(&json!({"st": null})),
            Err(SymbolTypeValidationError::InvalidType(_))
        ));
    }

    #[test]
    fn rejects_numeric_string() {
        assert!(validator().validate(&json!({"st": "1"})).is_err());
    }

    #[test]
    fn rejects_fractional_number() {
        assert!(validator().validate(&json!({"st": 1.5})).is_err());
    }

    #[test]
    fn rejects_negative_integer() {
        assert!(validator().validate(&json!({"st": -1})).is_err());
    }

    #[test]
    fn supports_nested_field_paths() {
        let validator = SymbolTypeValidator::new(
            "meta.symbol_type".into(),
            vec![1, 2],
            SymbolTypeMissingPolicy::Reject,
        )
        .unwrap();

        assert!(
            validator
                .validate(&json!({
                    "meta": {"symbol_type": 2}
                }))
                .is_ok()
        );
    }

    #[test]
    fn rejects_empty_expected_values() {
        assert!(
            SymbolTypeValidator::new("st".into(), vec![], SymbolTypeMissingPolicy::Allow,).is_err()
        );
    }
}
