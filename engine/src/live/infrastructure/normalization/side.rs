use std::collections::BTreeMap;

use rust_decimal::Decimal;
use serde_json::Value;
use thiserror::Error;

use crate::shared::domain::canonical::TradeSide;

// ============================================================
// SIDE NORMALIZATION ERRORS
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SideNormalizationError {
    #[error("side mapping must contain at least one entry")]
    EmptyMapping,

    #[error("missing native aggressor-side value")]
    MissingSide,

    #[error("unsupported native aggressor-side value: {0}")]
    UnknownSide(String),

    #[error("unsupported native side representation: {0}")]
    UnsupportedType(&'static str),

    #[error("trade quantity must not be zero")]
    ZeroQuantity,

    #[error(
        "negative native trade quantity is not allowed \
         for explicitly mapped aggressor sides"
    )]
    UnexpectedNegativeQuantity,

    #[error(
        "signed-quantity side normalization must not \
         receive an additional native side value"
    )]
    UnexpectedSide,

    #[error("native trade quantity magnitude overflow")]
    QuantityOverflow,
}

// ============================================================
// TYPED NATIVE SIDE VALUE
// ============================================================

/// A native JSON scalar used to identify an aggressor side.
///
/// The variants deliberately distinguish:
///
/// - JSON true
/// - JSON "true"
/// - JSON 1
///
/// These values must never be treated as interchangeable.
///
/// Numeric side indicators are restricted to exact
/// signed/unsigned integers.
///
/// Floating-point JSON side indicators are rejected.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum NativeSideValue {
    Boolean(bool),

    String(String),

    Integer(i64),

    UnsignedInteger(u64),
}

impl NativeSideValue {
    pub fn from_json(value: &Value) -> Result<Self, SideNormalizationError> {
        match value {
            Value::Bool(value) => Ok(Self::Boolean(*value)),

            Value::String(value) => Ok(Self::String(value.clone())),

            Value::Number(number) => {
                if let Some(value) = number.as_i64() {
                    if value >= 0 {
                        return Ok(Self::UnsignedInteger(value as u64));
                    }

                    return Ok(Self::Integer(value));
                }

                if let Some(value) = number.as_u64() {
                    return Ok(Self::UnsignedInteger(value));
                }

                Err(SideNormalizationError::UnsupportedType(
                    "floating-point number",
                ))
            }

            Value::Null => Err(SideNormalizationError::UnsupportedType("null")),

            Value::Array(_) => Err(SideNormalizationError::UnsupportedType("array")),

            Value::Object(_) => Err(SideNormalizationError::UnsupportedType("object")),
        }
    }

    pub fn describe(&self) -> String {
        match self {
            Self::Boolean(value) => {
                format!("boolean:{value}")
            }

            Self::String(value) => {
                format!("string:{value:?}")
            }

            Self::Integer(value) => {
                format!("integer:{value}")
            }

            Self::UnsignedInteger(value) => {
                format!("unsigned_integer:{value}")
            }
        }
    }
}

// ============================================================
// SIGNED QUANTITY CONVENTION
// ============================================================

/// Which aggressor side is represented by a positive
/// native signed quantity.
///
/// This convention must come from verified format
/// configuration.
///
/// It must never be inferred from the exchange name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PositiveQuantitySide {
    Buy,
    Sell,
}

impl PositiveQuantitySide {
    pub const fn positive_side(self) -> TradeSide {
        match self {
            Self::Buy => TradeSide::Buy,
            Self::Sell => TradeSide::Sell,
        }
    }

    pub const fn negative_side(self) -> TradeSide {
        match self {
            Self::Buy => TradeSide::Sell,
            Self::Sell => TradeSide::Buy,
        }
    }
}

// ============================================================
// SIDE STRATEGY
// ============================================================

/// Generic native aggressor-side interpretation strategy.
///
/// Mapped:
///     An explicit native field identifies the aggressor.
///
/// SignedQuantity:
///     The sign of the native quantity identifies the
///     aggressor.
///
/// The magnitude is always returned as a positive Decimal.
///
/// Side determination does not perform contract conversion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SideNormalizationStrategy {
    Mapped {
        mappings: BTreeMap<NativeSideValue, TradeSide>,
    },

    SignedQuantity {
        positive_side: PositiveQuantitySide,
    },
}

// ============================================================
// NORMALIZED SIDE RESULT
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedTradeSide {
    pub side: TradeSide,

    /// Strictly positive native quantity magnitude.
    ///
    /// This retains the native quantity unit:
    /// base, quote, or contracts.
    ///
    /// Economic conversion is performed later by
    /// QuantityNormalizer using NormalizationContext.
    pub quantity: Decimal,
}

// ============================================================
// AGGRESSOR-SIDE NORMALIZER
// ============================================================

/// Stateless interpreter of native aggressor-side semantics.
///
/// This component does not:
///
/// - Infer native market categories.
/// - Convert contracts to asset quantities.
/// - Deduplicate trade records.
/// - Maintain market state.
/// - Infer side from price movement.
/// - Guess unsupported native values.
#[derive(Debug, Clone)]
pub struct AggressorSideNormalizer {
    strategy: SideNormalizationStrategy,
}

impl AggressorSideNormalizer {
    // ========================================================
    // MAPPED SIDE STRATEGY
    // ========================================================

    pub fn mapped(
        mappings: BTreeMap<NativeSideValue, TradeSide>,
    ) -> Result<Self, SideNormalizationError> {
        if mappings.is_empty() {
            return Err(SideNormalizationError::EmptyMapping);
        }

        Ok(Self {
            strategy: SideNormalizationStrategy::Mapped { mappings },
        })
    }

    // ========================================================
    // SIGNED QUANTITY STRATEGY
    // ========================================================

    pub fn signed_quantity(positive_side: PositiveQuantitySide) -> Self {
        Self {
            strategy: SideNormalizationStrategy::SignedQuantity { positive_side },
        }
    }

    pub fn strategy(&self) -> &SideNormalizationStrategy {
        &self.strategy
    }

    // ========================================================
    // RESOLVE ONE TRADE
    // ========================================================

    /// Interpret aggressor side and native quantity sign.
    ///
    /// `native_side`:
    ///
    /// - Required for Mapped.
    /// - Must be absent for SignedQuantity.
    ///
    /// `native_quantity` may be signed only under
    /// SignedQuantity.
    ///
    /// The returned quantity is strictly positive.
    pub fn resolve(
        &self,
        native_side: Option<&Value>,
        native_quantity: Decimal,
    ) -> Result<ResolvedTradeSide, SideNormalizationError> {
        // ====================================================
        // ZERO QUANTITY
        // ====================================================

        // A trade with zero execution quantity cannot
        // establish a valid positive trade magnitude.
        //
        // Zero is allowed for L2 deletions, but those
        // never pass through this trade-side normalizer.
        if native_quantity.is_zero() {
            return Err(SideNormalizationError::ZeroQuantity);
        }

        match &self.strategy {
            // =================================================
            // EXPLICIT NATIVE SIDE
            // =================================================
            SideNormalizationStrategy::Mapped { mappings } => {
                // A negative trade quantity must not be
                // silently converted when direction is
                // already represented by another field.
                //
                // If a native format genuinely requires
                // both signed quantities and explicit
                // sides, it needs a separately verified
                // consistency strategy.
                if native_quantity < Decimal::ZERO {
                    return Err(SideNormalizationError::UnexpectedNegativeQuantity);
                }

                let value = native_side.ok_or(SideNormalizationError::MissingSide)?;

                let native = NativeSideValue::from_json(value)?;

                let side = mappings
                    .get(&native)
                    .copied()
                    .ok_or_else(|| SideNormalizationError::UnknownSide(native.describe()))?;

                Ok(ResolvedTradeSide {
                    side,
                    quantity: native_quantity,
                })
            }

            // =================================================
            // SIGNED NATIVE QUANTITY
            // =================================================
            SideNormalizationStrategy::SignedQuantity { positive_side } => {
                if native_side.is_some() {
                    return Err(SideNormalizationError::UnexpectedSide);
                }

                let side = if native_quantity > Decimal::ZERO {
                    positive_side.positive_side()
                } else {
                    positive_side.negative_side()
                };

                let quantity = native_quantity.abs();

                Ok(ResolvedTradeSide { side, quantity })
            }
        }
    }
}

// ============================================================
// UNIT TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use serde_json::json;

    use super::*;

    fn d(value: &str) -> Decimal {
        Decimal::from_str_exact(value).unwrap()
    }

    // ========================================================
    // BINANCE BOOLEAN SIDE
    // ========================================================

    fn binance_side() -> AggressorSideNormalizer {
        AggressorSideNormalizer::mapped(BTreeMap::from([
            (NativeSideValue::Boolean(true), TradeSide::Sell),
            (NativeSideValue::Boolean(false), TradeSide::Buy),
        ]))
        .unwrap()
    }

    #[test]
    fn binance_buyer_maker_true_means_sell() {
        let result = binance_side()
            .resolve(Some(&json!(true)), d("0.001"))
            .unwrap();

        assert_eq!(result.side, TradeSide::Sell);
        assert_eq!(result.quantity, d("0.001"));
    }

    #[test]
    fn binance_buyer_maker_false_means_buy() {
        let result = binance_side()
            .resolve(Some(&json!(false)), d("0.001"))
            .unwrap();

        assert_eq!(result.side, TradeSide::Buy);
    }

    // ========================================================
    // BYBIT EXPLICIT SIDE
    // ========================================================

    fn bybit_side() -> AggressorSideNormalizer {
        AggressorSideNormalizer::mapped(BTreeMap::from([
            (NativeSideValue::String("Buy".into()), TradeSide::Buy),
            (NativeSideValue::String("Sell".into()), TradeSide::Sell),
        ]))
        .unwrap()
    }

    #[test]
    fn resolves_bybit_buy() {
        let result = bybit_side()
            .resolve(Some(&json!("Buy")), d("0.000123"))
            .unwrap();

        assert_eq!(result.side, TradeSide::Buy);
        assert_eq!(result.quantity, d("0.000123"));
    }

    #[test]
    fn resolves_bybit_sell() {
        let result = bybit_side()
            .resolve(Some(&json!("Sell")), d("0.5"))
            .unwrap();

        assert_eq!(result.side, TradeSide::Sell);
    }

    // ========================================================
    // LOWERCASE EXPLICIT SIDE
    // ========================================================

    #[test]
    fn resolves_lowercase_side_mapping() {
        let normalizer = AggressorSideNormalizer::mapped(BTreeMap::from([
            (NativeSideValue::String("buy".into()), TradeSide::Buy),
            (NativeSideValue::String("sell".into()), TradeSide::Sell),
        ]))
        .unwrap();

        let result = normalizer.resolve(Some(&json!("sell")), d("1.25")).unwrap();

        assert_eq!(result.side, TradeSide::Sell);
        assert_eq!(result.quantity, d("1.25"));
    }

    // ========================================================
    // SIGNED QUANTITY — POSITIVE BUY
    // ========================================================

    #[test]
    fn positive_signed_size_means_buy() {
        let normalizer = AggressorSideNormalizer::signed_quantity(PositiveQuantitySide::Buy);

        let result = normalizer.resolve(None, d("10")).unwrap();

        assert_eq!(result.side, TradeSide::Buy);
        assert_eq!(result.quantity, d("10"));
    }

    #[test]
    fn negative_signed_size_means_sell() {
        let normalizer = AggressorSideNormalizer::signed_quantity(PositiveQuantitySide::Buy);

        let result = normalizer.resolve(None, d("-10")).unwrap();

        assert_eq!(result.side, TradeSide::Sell);
        assert_eq!(result.quantity, d("10"));
    }

    // ========================================================
    // SIGNED QUANTITY — POSITIVE SELL
    // ========================================================

    #[test]
    fn supports_reversed_signed_side_convention() {
        let normalizer = AggressorSideNormalizer::signed_quantity(PositiveQuantitySide::Sell);

        let positive = normalizer.resolve(None, d("10")).unwrap();

        let negative = normalizer.resolve(None, d("-10")).unwrap();

        assert_eq!(positive.side, TradeSide::Sell);
        assert_eq!(negative.side, TradeSide::Buy);

        assert_eq!(positive.quantity, d("10"));
        assert_eq!(negative.quantity, d("10"));
    }

    // ========================================================
    // STRICT NATIVE TYPE MATCHING
    // ========================================================

    #[test]
    fn boolean_true_does_not_match_string_true() {
        let result = binance_side().resolve(Some(&json!("true")), d("1"));

        assert!(matches!(
            result,
            Err(SideNormalizationError::UnknownSide(_)),
        ));
    }

    #[test]
    fn integer_one_does_not_match_boolean_true() {
        let result = binance_side().resolve(Some(&json!(1)), d("1"));

        assert!(matches!(
            result,
            Err(SideNormalizationError::UnknownSide(_)),
        ));
    }

    #[test]
    fn preserves_case_sensitive_string_mappings() {
        let result = bybit_side().resolve(Some(&json!("buy")), d("1"));

        assert!(matches!(
            result,
            Err(SideNormalizationError::UnknownSide(_)),
        ));
    }

    // ========================================================
    // NUMERIC SIDE INDICATORS
    // ========================================================

    #[test]
    fn supports_integer_side_mappings() {
        let normalizer = AggressorSideNormalizer::mapped(BTreeMap::from([
            (NativeSideValue::UnsignedInteger(1), TradeSide::Buy),
            (NativeSideValue::Integer(-1), TradeSide::Sell),
        ]))
        .unwrap();

        assert_eq!(
            normalizer.resolve(Some(&json!(1)), d("2")).unwrap().side,
            TradeSide::Buy,
        );

        assert_eq!(
            normalizer.resolve(Some(&json!(-1)), d("2")).unwrap().side,
            TradeSide::Sell,
        );
    }

    // ========================================================
    // INVALID SIDE VALUES
    // ========================================================

    #[test]
    fn rejects_missing_explicit_side() {
        let result = bybit_side().resolve(None, d("1"));

        assert!(matches!(result, Err(SideNormalizationError::MissingSide),));
    }

    #[test]
    fn rejects_unknown_explicit_side() {
        let result = bybit_side().resolve(Some(&json!("Unknown")), d("1"));

        assert!(matches!(
            result,
            Err(SideNormalizationError::UnknownSide(_)),
        ));
    }

    #[test]
    fn rejects_invalid_side_type() {
        let result = bybit_side().resolve(Some(&json!({"side": "Buy"})), d("1"));

        assert!(matches!(
            result,
            Err(SideNormalizationError::UnsupportedType("object")),
        ));
    }

    // ========================================================
    // QUANTITY INTEGRITY
    // ========================================================

    #[test]
    fn rejects_zero_trade_quantity() {
        let result = bybit_side().resolve(Some(&json!("Buy")), Decimal::ZERO);

        assert!(matches!(result, Err(SideNormalizationError::ZeroQuantity),));
    }

    #[test]
    fn rejects_negative_quantity_with_explicit_side() {
        let result = bybit_side().resolve(Some(&json!("Buy")), d("-10"));

        assert!(matches!(
            result,
            Err(SideNormalizationError::UnexpectedNegativeQuantity),
        ));
    }

    #[test]
    fn rejects_extra_side_for_signed_quantity() {
        let normalizer = AggressorSideNormalizer::signed_quantity(PositiveQuantitySide::Buy);

        let result = normalizer.resolve(Some(&json!("Buy")), d("-10"));

        assert!(matches!(
            result,
            Err(SideNormalizationError::UnexpectedSide),
        ));
    }

    // ========================================================
    // CONFIGURATION VALIDATION
    // ========================================================

    #[test]
    fn rejects_empty_side_mapping() {
        let result = AggressorSideNormalizer::mapped(BTreeMap::new());

        assert!(matches!(result, Err(SideNormalizationError::EmptyMapping),));
    }

    // ========================================================
    // JSON VALUE TYPES
    // ========================================================

    #[test]
    fn refuses_fractional_numeric_side() {
        assert!(matches!(
            NativeSideValue::from_json(&json!(1.5)),
            Err(SideNormalizationError::UnsupportedType(
                "floating-point number"
            )),
        ));
    }
}
