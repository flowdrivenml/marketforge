use serde_json::Value;

// ============================================================
// CONFIGURATION ERRORS
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RestApplicationConfigError {
    #[error("application success-code field cannot be empty")]
    EmptyCodeField,

    #[error("application success-code list cannot be empty")]
    EmptySuccessCodes,

    #[error("application success code cannot be empty")]
    EmptySuccessCode,

    #[error("duplicate application success code")]
    DuplicateSuccessCode,

    #[error("application message field cannot be empty")]
    EmptyMessageField,

    #[error("application data field cannot be empty")]
    EmptyDataField,
}

// ============================================================
// APPLICATION RESPONSE RULE
// ============================================================

/// Describes how one exchange REST API reports application
/// success or failure inside an HTTP response body.
///
/// Rules are immutable after initialization.
///
/// The caller must supply rules derived from the exchange's
/// documented response format.
///
/// No exchange-specific branching exists in the validator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestApplicationRule {
    code_field: String,

    success_codes: Vec<String>,

    message_field: Option<String>,

    data_field: Option<String>,
}

impl RestApplicationRule {
    pub fn new(
        code_field: impl Into<String>,
        success_codes: Vec<String>,
        message_field: Option<String>,
        data_field: Option<String>,
    ) -> Result<Self, RestApplicationConfigError> {
        let code_field = code_field.into();

        if code_field.trim().is_empty() {
            return Err(RestApplicationConfigError::EmptyCodeField);
        }

        if success_codes.is_empty() {
            return Err(RestApplicationConfigError::EmptySuccessCodes);
        }

        let mut seen = std::collections::HashSet::new();

        for code in &success_codes {
            if code.is_empty() {
                return Err(RestApplicationConfigError::EmptySuccessCode);
            }

            if !seen.insert(code) {
                return Err(RestApplicationConfigError::DuplicateSuccessCode);
            }
        }

        if message_field
            .as_deref()
            .is_some_and(|value| value.trim().is_empty())
        {
            return Err(RestApplicationConfigError::EmptyMessageField);
        }

        if data_field
            .as_deref()
            .is_some_and(|value| value.trim().is_empty())
        {
            return Err(RestApplicationConfigError::EmptyDataField);
        }

        Ok(Self {
            code_field,
            success_codes,
            message_field,
            data_field,
        })
    }

    pub fn code_field(&self) -> &str {
        &self.code_field
    }

    pub fn success_codes(&self) -> &[String] {
        &self.success_codes
    }

    pub fn message_field(&self) -> Option<&str> {
        self.message_field.as_deref()
    }

    pub fn data_field(&self) -> Option<&str> {
        self.data_field.as_deref()
    }
}

// ============================================================
// VALIDATION OUTCOME
// ============================================================

/// Result of examining one exchange-native JSON response.
///
/// Success means the configured application status indicates
/// success. It does not imply that an L2 snapshot is suitable
/// for synchronization or that market data is otherwise valid.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RestApplicationOutcome {
    Success,

    Failure {
        code: String,
        message: Option<String>,
    },

    InvalidEnvelope {
        reason: String,
    },
}

impl RestApplicationOutcome {
    pub fn is_success(&self) -> bool {
        matches!(self, Self::Success)
    }
}

// ============================================================
// APPLICATION RESPONSE VALIDATOR
// ============================================================

/// Generic validator for exchange REST application envelopes.
///
/// HTTP error classification is handled separately.
///
/// This validator must be invoked only after throttling
/// feedback has been inspected and applied.
///
/// It does not:
/// - Perform HTTP requests.
/// - Retry operations.
/// - Update rate-limit state.
/// - Normalize market data.
/// - Interpret order-book sequence semantics.
pub struct RestApplicationValidator;

impl RestApplicationValidator {
    /// Validate the application-level status of one JSON body.
    pub fn validate(rule: &RestApplicationRule, body: &Value) -> RestApplicationOutcome {
        // ====================================================
        // OBJECT ENVELOPE
        // ====================================================

        let Some(object) = body.as_object() else {
            return RestApplicationOutcome::InvalidEnvelope {
                reason: "application response root must be a JSON object".into(),
            };
        };

        // ====================================================
        // APPLICATION CODE
        // ====================================================

        let Some(raw_code) = object.get(rule.code_field()) else {
            return RestApplicationOutcome::InvalidEnvelope {
                reason: format!("missing application code field '{}'", rule.code_field(),),
            };
        };

        let code = match raw_code {
            Value::String(value) => value.clone(),

            Value::Number(value) => value.to_string(),

            _ => {
                return RestApplicationOutcome::InvalidEnvelope {
                    reason: format!(
                        "application code field '{}' \
                         must be a string or number",
                        rule.code_field(),
                    ),
                };
            }
        };

        // ====================================================
        // MESSAGE
        // ====================================================

        let message = rule
            .message_field()
            .and_then(|field| object.get(field))
            .and_then(Value::as_str)
            .map(str::to_owned);

        // ====================================================
        // APPLICATION FAILURE
        // ====================================================

        if !rule
            .success_codes()
            .iter()
            .any(|expected| expected == &code)
        {
            return RestApplicationOutcome::Failure { code, message };
        }

        // ====================================================
        // REQUIRED DATA FIELD
        // ====================================================

        // Some exchange response envelopes require a
        // top-level data/result field even on success.
        //
        // Its presence is checked here, but deeper
        // market-data schema validation belongs to the
        // consuming stream or normalizer.
        //
        // A null value is rejected when the configured
        // response requires data.
        if let Some(field) = rule.data_field() {
            match object.get(field) {
                Some(Value::Null) | None => {
                    return RestApplicationOutcome::InvalidEnvelope {
                        reason: format!("missing or null application data field '{field}'"),
                    };
                }

                Some(_) => {}
            }
        }

        RestApplicationOutcome::Success
    }
}

// ============================================================
// TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::json;

    fn bybit() -> RestApplicationRule {
        RestApplicationRule::new(
            "retCode",
            vec!["0".into()],
            Some("retMsg".into()),
            Some("result".into()),
        )
        .unwrap()
    }

    fn okx() -> RestApplicationRule {
        RestApplicationRule::new(
            "code",
            vec!["0".into()],
            Some("msg".into()),
            Some("data".into()),
        )
        .unwrap()
    }

    fn bitget() -> RestApplicationRule {
        RestApplicationRule::new(
            "code",
            vec!["00000".into()],
            Some("msg".into()),
            Some("data".into()),
        )
        .unwrap()
    }

    // ========================================================
    // CONFIGURATION
    // ========================================================

    #[test]
    fn rejects_empty_code_field() {
        let result = RestApplicationRule::new("", vec!["0".into()], None, None);

        assert!(matches!(
            result,
            Err(RestApplicationConfigError::EmptyCodeField)
        ));
    }

    #[test]
    fn rejects_missing_success_codes() {
        let result = RestApplicationRule::new("code", Vec::new(), None, None);

        assert!(matches!(
            result,
            Err(RestApplicationConfigError::EmptySuccessCodes)
        ));
    }

    #[test]
    fn rejects_duplicate_success_codes() {
        let result = RestApplicationRule::new("code", vec!["0".into(), "0".into()], None, None);

        assert!(matches!(
            result,
            Err(RestApplicationConfigError::DuplicateSuccessCode)
        ));
    }

    // ========================================================
    // BYBIT
    // ========================================================

    #[test]
    fn accepts_bybit_success() {
        let body = json!({
            "retCode": 0,
            "retMsg": "OK",
            "result": {
                "list": []
            },
            "time": 1720000000000_i64
        });

        assert_eq!(
            RestApplicationValidator::validate(&bybit(), &body,),
            RestApplicationOutcome::Success,
        );
    }

    #[test]
    fn rejects_bybit_application_failure() {
        let body = json!({
            "retCode": 10001,
            "retMsg": "Request parameter error",
            "result": {}
        });

        assert_eq!(
            RestApplicationValidator::validate(&bybit(), &body,),
            RestApplicationOutcome::Failure {
                code: "10001".into(),
                message: Some("Request parameter error".into(),),
            },
        );
    }

    // ========================================================
    // OKX
    // ========================================================

    #[test]
    fn accepts_okx_success() {
        let body = json!({
            "code": "0",
            "msg": "",
            "data": [
                {
                    "instId": "BTC-USDT",
                    "px": "65000"
                }
            ]
        });

        assert!(RestApplicationValidator::validate(&okx(), &body,).is_success());
    }

    #[test]
    fn rejects_okx_application_failure() {
        let body = json!({
            "code": "51000",
            "msg": "Parameter error",
            "data": []
        });

        assert!(matches!(
            RestApplicationValidator::validate(
                &okx(),
                &body,
            ),
            RestApplicationOutcome::Failure { code, .. }
                if code == "51000"
        ));
    }

    // ========================================================
    // BITGET
    // ========================================================

    #[test]
    fn accepts_bitget_uta_success() {
        let body = json!({
            "code": "00000",
            "msg": "success",
            "requestTime": 1720000000000_i64,
            "data": []
        });

        assert!(RestApplicationValidator::validate(&bitget(), &body,).is_success());
    }

    #[test]
    fn rejects_bitget_application_failure() {
        let body = json!({
            "code": "40017",
            "msg": "Invalid parameter",
            "data": null
        });

        assert_eq!(
            RestApplicationValidator::validate(&bitget(), &body,),
            RestApplicationOutcome::Failure {
                code: "40017".into(),
                message: Some("Invalid parameter".into()),
            },
        );
    }

    // ========================================================
    // ENVELOPE VALIDATION
    // ========================================================

    #[test]
    fn rejects_missing_application_code() {
        let body = json!({
            "msg": "success",
            "data": []
        });

        assert!(matches!(
            RestApplicationValidator::validate(&okx(), &body,),
            RestApplicationOutcome::InvalidEnvelope { .. }
        ));
    }

    #[test]
    fn rejects_invalid_application_code_type() {
        let body = json!({
            "code": {},
            "data": []
        });

        assert!(matches!(
            RestApplicationValidator::validate(&okx(), &body,),
            RestApplicationOutcome::InvalidEnvelope { .. }
        ));
    }

    #[test]
    fn rejects_missing_success_data() {
        let body = json!({
            "code": "0",
            "msg": ""
        });

        assert!(matches!(
            RestApplicationValidator::validate(&okx(), &body,),
            RestApplicationOutcome::InvalidEnvelope { .. }
        ));
    }

    #[test]
    fn rejects_null_success_data() {
        let body = json!({
            "code": "0",
            "msg": "",
            "data": null
        });

        assert!(matches!(
            RestApplicationValidator::validate(&okx(), &body,),
            RestApplicationOutcome::InvalidEnvelope { .. }
        ));
    }

    #[test]
    fn accepts_empty_data_array() {
        let body = json!({
            "code": "0",
            "msg": "",
            "data": []
        });

        assert!(RestApplicationValidator::validate(&okx(), &body,).is_success());
    }

    #[test]
    fn rejects_non_object_envelope() {
        let body = json!([
            {"code": "0", "data": []}
        ]);

        assert!(matches!(
            RestApplicationValidator::validate(&okx(), &body,),
            RestApplicationOutcome::InvalidEnvelope { .. }
        ));
    }

    // ========================================================
    // STRICT SUCCESS-CODE MATCHING
    // ========================================================

    #[test]
    fn preserves_leading_zero_semantics() {
        let body = json!({
            "code": "0",
            "msg": "success",
            "data": []
        });

        // Bitget UTA expects "00000", not "0".
        assert!(matches!(
            RestApplicationValidator::validate(&bitget(), &body,),
            RestApplicationOutcome::Failure { .. }
        ));
    }

    #[test]
    fn numeric_and_string_codes_are_compatible() {
        let body = json!({
            "code": 0,
            "msg": "",
            "data": []
        });

        assert!(RestApplicationValidator::validate(&okx(), &body,).is_success());
    }
}
