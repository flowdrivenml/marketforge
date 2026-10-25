use serde_json::Value;
use thiserror::Error;

use super::JsonExtractor;

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum EnvelopeValidationError {
    #[error("envelope validation plan cannot be empty")]
    EmptyPlan,

    #[error("invalid envelope rule: {0}")]
    InvalidRule(String),

    #[error("cannot read envelope field '{path}': {reason}")]
    Extraction { path: String, reason: String },

    #[error("envelope field '{path}' mismatch: expected {expected}, received {actual}")]
    Mismatch {
        path: String,
        expected: String,
        actual: String,
    },

    #[error("invalid stream symbol")]
    InvalidSymbol,
}

// ============================================================
// VALIDATION RULES
// ============================================================

/// Generic rules for validating native message identity.
///
/// Every configured rule must pass.
///
/// No rule is allowed to silently ignore a missing field.
#[derive(Debug, Clone, PartialEq)]
pub enum EnvelopeRule {
    /// Exact JSON value matching.
    ///
    /// Boolean true is different from string "true".
    Equals { path: String, expected: Value },

    /// Accept one of several exact JSON values.
    OneOf { path: String, accepted: Vec<Value> },

    /// Field must match the authoritative native symbol.
    Symbol { path: String },

    /// Native field must match a rendered subscription
    /// template.
    ///
    /// Supported placeholders:
    /// {symbol}
    /// {symbol_upper}
    /// {symbol_lower}
    TopicTemplate { path: String, template: String },
}

// ============================================================
// VALIDATOR
// ============================================================

#[derive(Debug, Clone)]
pub struct EnvelopeValidator {
    rules: Vec<EnvelopeRule>,
}

impl EnvelopeValidator {
    // ========================================================
    // CONSTRUCTION
    // ========================================================

    pub fn new(rules: Vec<EnvelopeRule>) -> Result<Self, EnvelopeValidationError> {
        if rules.is_empty() {
            return Err(EnvelopeValidationError::EmptyPlan);
        }

        for rule in &rules {
            let path = match rule {
                EnvelopeRule::Equals { path, .. }
                | EnvelopeRule::OneOf { path, .. }
                | EnvelopeRule::Symbol { path }
                | EnvelopeRule::TopicTemplate { path, .. } => path,
            };

            validate_path(path)?;

            match rule {
                EnvelopeRule::OneOf { accepted, .. } => {
                    if accepted.is_empty() {
                        return Err(EnvelopeValidationError::InvalidRule(
                            "OneOf requires accepted values".into(),
                        ));
                    }
                }

                EnvelopeRule::TopicTemplate { template, .. } => {
                    validate_template(template)?;
                }

                _ => {}
            }
        }

        Ok(Self { rules })
    }

    pub fn rules(&self) -> &[EnvelopeRule] {
        &self.rules
    }

    // ========================================================
    // VALIDATION
    // ========================================================

    /// Validate the native message against the expected
    /// subscription identity.
    ///
    /// `symbol` must be the native exchange symbol from
    /// NormalizationContext, not a display symbol or alias.
    ///
    /// This component does not infer exchange identity
    /// from the contents of the message.
    pub fn validate(&self, message: &Value, symbol: &str) -> Result<(), EnvelopeValidationError> {
        if symbol.is_empty() || symbol.trim() != symbol {
            return Err(EnvelopeValidationError::InvalidSymbol);
        }

        for rule in &self.rules {
            match rule {
                // ============================================
                // EXACT MATCH
                // ============================================
                EnvelopeRule::Equals { path, expected } => {
                    let actual = read(message, path)?;

                    if actual != expected {
                        return Err(mismatch(path, expected.to_string(), actual.to_string()));
                    }
                }

                // ============================================
                // ACCEPTED VALUES
                // ============================================
                EnvelopeRule::OneOf { path, accepted } => {
                    let actual = read(message, path)?;

                    if !accepted.contains(actual) {
                        return Err(mismatch(path, format!("{accepted:?}"), actual.to_string()));
                    }
                }

                // ============================================
                // INSTRUMENT SYMBOL
                // ============================================
                EnvelopeRule::Symbol { path } => {
                    let actual = read(message, path)?;

                    if actual.as_str() != Some(symbol) {
                        return Err(mismatch(path, format!("{symbol:?}"), actual.to_string()));
                    }
                }

                // ============================================
                // TOPIC TEMPLATE
                // ============================================
                EnvelopeRule::TopicTemplate { path, template } => {
                    let actual = read(message, path)?;

                    let expected = render_template(template, symbol);

                    if actual.as_str() != Some(expected.as_str()) {
                        return Err(mismatch(path, format!("{expected:?}"), actual.to_string()));
                    }
                }
            }
        }

        Ok(())
    }
}

// ============================================================
// HELPERS
// ============================================================

fn read<'a>(message: &'a Value, path: &str) -> Result<&'a Value, EnvelopeValidationError> {
    JsonExtractor::required(message, path).map_err(|error| EnvelopeValidationError::Extraction {
        path: path.to_owned(),
        reason: error.to_string(),
    })
}

fn mismatch(path: &str, expected: String, actual: String) -> EnvelopeValidationError {
    EnvelopeValidationError::Mismatch {
        path: path.to_owned(),
        expected,
        actual,
    }
}

fn validate_path(path: &str) -> Result<(), EnvelopeValidationError> {
    if path.is_empty() || path.trim() != path || path == "$" {
        return Err(EnvelopeValidationError::InvalidRule(format!(
            "invalid field path {path:?}"
        )));
    }

    let normalized = path.strip_prefix("$.").unwrap_or(path);

    if normalized.is_empty() || normalized.split('.').any(|part| part.is_empty()) {
        return Err(EnvelopeValidationError::InvalidRule(format!(
            "invalid field path {path:?}"
        )));
    }

    Ok(())
}

fn validate_template(template: &str) -> Result<(), EnvelopeValidationError> {
    if template.is_empty() {
        return Err(EnvelopeValidationError::InvalidRule(
            "topic template cannot be empty".into(),
        ));
    }

    let sanitized = template
        .replace("{symbol_upper}", "")
        .replace("{symbol_lower}", "")
        .replace("{symbol}", "");

    // Unknown template placeholders must never produce
    // a silently accepted subscription identity.
    if sanitized.contains('{') || sanitized.contains('}') {
        return Err(EnvelopeValidationError::InvalidRule(format!(
            "unsupported topic template {template:?}"
        )));
    }

    Ok(())
}

fn render_template(template: &str, symbol: &str) -> String {
    template
        .replace("{symbol_upper}", &symbol.to_ascii_uppercase())
        .replace("{symbol_lower}", &symbol.to_ascii_lowercase())
        .replace("{symbol}", symbol)
}

// ============================================================
// TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn validates_binance_trade_envelope() {
        let validator = EnvelopeValidator::new(vec![
            EnvelopeRule::Equals {
                path: "e".into(),
                expected: json!("trade"),
            },
            EnvelopeRule::Symbol { path: "s".into() },
        ])
        .unwrap();

        let message = json!({
            "e": "trade",
            "s": "BTCUSD_PERP",
            "t": 1160649918,
            "p": "79324.0",
            "q": "4"
        });

        validator.validate(&message, "BTCUSD_PERP").unwrap();
    }

    #[test]
    fn rejects_aggregate_trade_as_individual_trade() {
        let validator = EnvelopeValidator::new(vec![
            EnvelopeRule::Equals {
                path: "e".into(),
                expected: json!("trade"),
            },
            EnvelopeRule::Symbol { path: "s".into() },
        ])
        .unwrap();

        let message = json!({
            "e": "aggTrade",
            "s": "BTCUSDT"
        });

        assert!(matches!(
            validator.validate(&message, "BTCUSDT"),
            Err(EnvelopeValidationError::Mismatch { .. })
        ));
    }

    #[test]
    fn rejects_wrong_binance_symbol() {
        let validator =
            EnvelopeValidator::new(vec![EnvelopeRule::Symbol { path: "s".into() }]).unwrap();

        let message = json!({
            "s": "ETHUSDT"
        });

        assert!(validator.validate(&message, "BTCUSDT").is_err());
    }

    #[test]
    fn validates_bitget_envelope() {
        let validator = EnvelopeValidator::new(vec![
            EnvelopeRule::Equals {
                path: "arg.topic".into(),
                expected: json!("publicTrade"),
            },
            EnvelopeRule::Symbol {
                path: "arg.symbol".into(),
            },
            EnvelopeRule::OneOf {
                path: "arg.instType".into(),
                accepted: vec![json!("usdt-futures"), json!("usdc-futures")],
            },
        ])
        .unwrap();

        let message = json!({
            "arg": {
                "topic": "publicTrade",
                "symbol": "BTCUSDT",
                "instType": "usdt-futures"
            },
            "data": []
        });

        validator.validate(&message, "BTCUSDT").unwrap();
    }

    #[test]
    fn rejects_bitget_wrong_market_category() {
        let validator = EnvelopeValidator::new(vec![EnvelopeRule::OneOf {
            path: "arg.instType".into(),
            accepted: vec![json!("usdt-futures"), json!("usdc-futures")],
        }])
        .unwrap();

        let message = json!({
            "arg": {
                "instType": "coin-futures"
            }
        });

        assert!(validator.validate(&message, "BTCUSDT").is_err());
    }

    #[test]
    fn validates_bybit_topic_template() {
        let validator = EnvelopeValidator::new(vec![EnvelopeRule::TopicTemplate {
            path: "topic".into(),
            template: "publicTrade.{symbol_upper}".into(),
        }])
        .unwrap();

        let message = json!({
            "topic": "publicTrade.BTCUSDT",
            "data": []
        });

        validator.validate(&message, "BTCUSDT").unwrap();
    }

    #[test]
    fn rejects_wrong_bybit_topic() {
        let validator = EnvelopeValidator::new(vec![EnvelopeRule::TopicTemplate {
            path: "topic".into(),
            template: "publicTrade.{symbol_upper}".into(),
        }])
        .unwrap();

        let message = json!({
            "topic": "publicTrade.ETHUSDT"
        });

        assert!(validator.validate(&message, "BTCUSDT").is_err());
    }

    #[test]
    fn validates_okx_channel_and_instrument() {
        let validator = EnvelopeValidator::new(vec![
            EnvelopeRule::Equals {
                path: "arg.channel".into(),
                expected: json!("trades-all"),
            },
            EnvelopeRule::Symbol {
                path: "arg.instId".into(),
            },
        ])
        .unwrap();

        let message = json!({
            "arg": {
                "channel": "trades-all",
                "instId": "BTC-USDT"
            },
            "data": []
        });

        validator.validate(&message, "BTC-USDT").unwrap();
    }

    #[test]
    fn rejects_missing_required_field() {
        let validator = EnvelopeValidator::new(vec![EnvelopeRule::Equals {
            path: "e".into(),
            expected: json!("trade"),
        }])
        .unwrap();

        assert!(matches!(
            validator.validate(&json!({}), "BTCUSDT"),
            Err(EnvelopeValidationError::Extraction { .. })
        ));
    }

    #[test]
    fn rejects_wrong_json_value_type() {
        let validator = EnvelopeValidator::new(vec![EnvelopeRule::Equals {
            path: "e".into(),
            expected: json!("trade"),
        }])
        .unwrap();

        assert!(validator.validate(&json!({"e": true}), "BTCUSDT",).is_err());
    }

    #[test]
    fn rejects_empty_validation_plan() {
        assert!(matches!(
            EnvelopeValidator::new(vec![]),
            Err(EnvelopeValidationError::EmptyPlan)
        ));
    }

    #[test]
    fn rejects_empty_accepted_values() {
        assert!(
            EnvelopeValidator::new(vec![EnvelopeRule::OneOf {
                path: "arg.instType".into(),
                accepted: vec![],
            },])
            .is_err()
        );
    }

    #[test]
    fn rejects_unknown_template_placeholder() {
        assert!(
            EnvelopeValidator::new(vec![EnvelopeRule::TopicTemplate {
                path: "topic".into(),
                template: "publicTrade.{unknown}".into(),
            },])
            .is_err()
        );
    }

    #[test]
    fn rejects_empty_stream_symbol() {
        let validator =
            EnvelopeValidator::new(vec![EnvelopeRule::Symbol { path: "s".into() }]).unwrap();

        assert!(validator.validate(&json!({"s": "BTCUSDT"}), "",).is_err());
    }
}
