use crate::shared::domain::canonical::TimestampNs;
use rust_decimal::{Decimal, prelude::ToPrimitive};
use serde_json::Value;
use thiserror::Error;

// ============================================================
// NUMERIC CONVERSION ERRORS
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum NumericError {
    #[error("unsupported numeric value type: {0}")]
    UnsupportedType(&'static str),

    #[error("empty numeric string")]
    EmptyValue,

    #[error("invalid decimal value '{value}': {reason}")]
    InvalidDecimal { value: String, reason: String },

    #[error("invalid integer value '{value}': {reason}")]
    InvalidInteger { value: String, reason: String },

    #[error("fractional JSON numbers are not accepted for exact conversion")]
    FloatingPointNumber,

    #[error("decimal value must be positive: {0}")]
    NonPositivePrice(Decimal),

    #[error("quantity must not be negative: {0}")]
    NegativeQuantity(Decimal),

    #[error("quantity must be positive: {0}")]
    NonPositiveQuantity(Decimal),

    #[error("timestamp conversion overflow: value={value}, unit={unit}")]
    TimestampOverflow { value: i64, unit: &'static str },

    #[error("timestamp value must not be negative: {0}")]
    NegativeTimestamp(i64),

    #[error("decimal-millisecond timestamp has subnanosecond precision: {0}")]
    SubNanosecondPrecision(String),

    #[error("decimal-millisecond timestamp exceeds the canonical i64 range: {0}")]
    DecimalTimestampOverflow(String),

    #[error("decimal-second timestamp has subnanosecond precision: {0}")]
    SubNanosecondSecondsPrecision(String),

    #[error("decimal-second timestamp exceeds the canonical i64 range: {0}")]
    DecimalSecondsTimestampOverflow(String),
}

// ============================================================
// TIMESTAMP UNITS
// ============================================================

/// Units supported by the generic timestamp converter.
///
/// MarketForge canonical timestamps use signed i64
/// nanoseconds.
///
/// Conversions must never silently overflow or truncate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TimestampUnit {
    Seconds,
    Milliseconds,
    Microseconds,
    Nanoseconds,
    DecimalMilliseconds,
    DecimalSeconds,
}

impl TimestampUnit {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Seconds => "seconds",
            Self::Milliseconds => "milliseconds",
            Self::Microseconds => "microseconds",
            Self::Nanoseconds => "nanoseconds",
            Self::DecimalMilliseconds => "milliseconds_decimal",
            Self::DecimalSeconds => "seconds_decimal",
        }
    }

    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "seconds" => Ok(Self::Seconds),
            "milliseconds" => Ok(Self::Milliseconds),
            "microseconds" => Ok(Self::Microseconds),
            "nanoseconds" => Ok(Self::Nanoseconds),
            "milliseconds_decimal" => Ok(Self::DecimalMilliseconds),
            "seconds_decimal" | "decimal_seconds" => Ok(Self::DecimalSeconds),

            other => Err(format!("unsupported timestamp unit '{other}'")),
        }
    }

    pub const fn nanosecond_multiplier(self) -> i64 {
        match self {
            Self::Seconds => 1_000_000_000,
            Self::Milliseconds => 1_000_000,
            Self::Microseconds => 1_000,
            Self::Nanoseconds => 1,
            Self::DecimalMilliseconds => 1_000_000,
            Self::DecimalSeconds => 1_000_000_000,
        }
    }
}

// ============================================================
// GENERIC NUMERIC CONVERTER
// ============================================================

/// Stateless, exact numeric conversion primitives.
///
/// All decimal conversions operate directly on:
///
/// - JSON strings.
/// - JSON signed integers.
/// - JSON unsigned integers.
///
/// Fractional JSON numbers are rejected because converting
/// an already-deserialized binary floating-point value
/// cannot establish preservation of the original decimal.
///
/// No f64 conversion is used.
pub struct NumericConverter;

impl NumericConverter {
    // ========================================================
    // EXACT DECIMAL
    // ========================================================

    /// Convert a native JSON numeric value to Decimal.
    ///
    /// Examples:
    ///
    /// "80000.12345678" -> Decimal
    /// 123              -> Decimal
    /// -123             -> Decimal
    ///
    /// JSON 1.25 is rejected. Exchanges must supply
    /// fractional values as exact decimal strings.
    pub fn decimal(value: &Value) -> Result<Decimal, NumericError> {
        match value {
            Value::String(value) => Self::decimal_string(value),

            Value::Number(number) => {
                if let Some(integer) = number.as_i64() {
                    return Ok(Decimal::from(integer));
                }

                if let Some(integer) = number.as_u64() {
                    return Ok(Decimal::from(integer));
                }

                Err(NumericError::FloatingPointNumber)
            }

            Value::Null => Err(NumericError::UnsupportedType("null")),

            Value::Bool(_) => Err(NumericError::UnsupportedType("boolean")),

            Value::Array(_) => Err(NumericError::UnsupportedType("array")),

            Value::Object(_) => Err(NumericError::UnsupportedType("object")),
        }
    }
    /// Convert a JSON number whose decimal representation was
    /// preserved by serde_json's arbitrary_precision parser.
    ///
    /// Only call this from an ingress path that parses original
    /// response bytes. A Value constructed from an f64 does not
    /// establish exact source-data provenance.
    pub fn decimal_lossless_json(value: &Value) -> Result<Decimal, NumericError> {
        match value {
            Value::String(text) => Self::decimal_string(text),

            Value::Number(number) => {
                // With serde_json/arbitrary_precision, this is
                // the preserved JSON number representation.
                //
                // We deliberately retain decimal_string's
                // existing rejection of scientific notation,
                // overflow, and unsupported precision.
                Self::decimal_string(&number.to_string())
            }

            Value::Null => Err(NumericError::UnsupportedType("null")),
            Value::Bool(_) => Err(NumericError::UnsupportedType("boolean")),
            Value::Array(_) => Err(NumericError::UnsupportedType("array")),
            Value::Object(_) => Err(NumericError::UnsupportedType("object")),
        }
    }
    /// Convert decimal seconds into canonical nanoseconds.
    ///
    /// JSON numbers must originate from a serde_json parser
    /// preserving their source decimal representation.
    ///
    /// No f64 arithmetic or timestamp rounding is performed.
    pub fn decimal_seconds_ns(value: &Value) -> Result<TimestampNs, NumericError> {
        use rust_decimal::prelude::ToPrimitive;

        let seconds = Self::decimal_lossless_json(value)?;

        let nanoseconds = seconds
            .checked_mul(Decimal::from(1_000_000_000_u64))
            .ok_or_else(|| NumericError::DecimalSecondsTimestampOverflow(seconds.to_string()))?;

        if nanoseconds.fract() != Decimal::ZERO {
            return Err(NumericError::SubNanosecondSecondsPrecision(
                seconds.to_string(),
            ));
        }

        nanoseconds
            .to_i64()
            .ok_or_else(|| NumericError::DecimalSecondsTimestampOverflow(seconds.to_string()))
    }
    /// Strictly positive exact price from losslessly parsed JSON.
    pub fn price_lossless_json(value: &Value) -> Result<Decimal, NumericError> {
        let price = Self::decimal_lossless_json(value)?;

        if price <= Decimal::ZERO {
            return Err(NumericError::NonPositivePrice(price));
        }

        Ok(price)
    }

    /// Nonnegative exact quantity from losslessly parsed JSON.
    ///
    /// Zero remains valid for L2 deletion semantics.
    pub fn quantity_lossless_json(value: &Value) -> Result<Decimal, NumericError> {
        let quantity = Self::decimal_lossless_json(value)?;

        if quantity < Decimal::ZERO {
            return Err(NumericError::NegativeQuantity(quantity));
        }

        Ok(quantity)
    }
    /// Parse a decimal string directly into Decimal.
    ///
    /// No floating-point intermediate representation.
    ///
    /// This accepts ordinary decimal notation.
    /// Scientific notation is rejected explicitly.
    pub fn decimal_string(value: &str) -> Result<Decimal, NumericError> {
        let value = value.trim();

        if value.is_empty() {
            return Err(NumericError::EmptyValue);
        }

        if value.contains('e') || value.contains('E') {
            return Err(NumericError::InvalidDecimal {
                value: value.into(),
                reason: "scientific notation is not supported".into(),
            });
        }

        Decimal::from_str_exact(value).map_err(|error| NumericError::InvalidDecimal {
            value: value.into(),
            reason: error.to_string(),
        })
    }

    // ========================================================
    // SIGNED INTEGER
    // ========================================================

    /// Extract an exact signed 64-bit integer.
    ///
    /// Accepts native integer JSON values and integer strings.
    ///
    /// Rejects fractional JSON numbers, even when
    /// their mathematical value appears integral.
    pub fn i64(value: &Value) -> Result<i64, NumericError> {
        match value {
            Value::String(value) => {
                let value = value.trim();

                if value.is_empty() {
                    return Err(NumericError::EmptyValue);
                }

                value
                    .parse::<i64>()
                    .map_err(|error| NumericError::InvalidInteger {
                        value: value.into(),
                        reason: error.to_string(),
                    })
            }

            Value::Number(number) => number.as_i64().ok_or_else(|| {
                if number.is_f64() {
                    NumericError::FloatingPointNumber
                } else {
                    NumericError::InvalidInteger {
                        value: number.to_string(),
                        reason: "outside signed 64-bit range".into(),
                    }
                }
            }),

            other => Err(NumericError::UnsupportedType(numeric_json_type(other))),
        }
    }

    // ========================================================
    // UNSIGNED INTEGER
    // ========================================================

    /// Extract an exact unsigned 64-bit integer.
    pub fn u64(value: &Value) -> Result<u64, NumericError> {
        match value {
            Value::String(value) => {
                let value = value.trim();

                if value.is_empty() {
                    return Err(NumericError::EmptyValue);
                }

                value
                    .parse::<u64>()
                    .map_err(|error| NumericError::InvalidInteger {
                        value: value.into(),
                        reason: error.to_string(),
                    })
            }

            Value::Number(number) => number.as_u64().ok_or_else(|| {
                if number.is_f64() {
                    NumericError::FloatingPointNumber
                } else {
                    NumericError::InvalidInteger {
                        value: number.to_string(),
                        reason: "negative or outside unsigned 64-bit range".into(),
                    }
                }
            }),

            other => Err(NumericError::UnsupportedType(numeric_json_type(other))),
        }
    }

    // ========================================================
    // PRICE
    // ========================================================

    /// Parse a canonical market price.
    ///
    /// Prices must be strictly positive.
    pub fn price(value: &Value) -> Result<Decimal, NumericError> {
        let price = Self::decimal(value)?;

        if price <= Decimal::ZERO {
            return Err(NumericError::NonPositivePrice(price));
        }

        Ok(price)
    }

    // ========================================================
    // QUANTITY
    // ========================================================

    /// Parse a canonical quantity.
    ///
    /// Zero is accepted because absolute L2 updates use
    /// zero quantity to indicate deletion.
    ///
    /// Trade decoders should use positive_quantity()
    /// when the format requires a positive execution size.
    pub fn quantity(value: &Value) -> Result<Decimal, NumericError> {
        let quantity = Self::decimal(value)?;

        if quantity < Decimal::ZERO {
            return Err(NumericError::NegativeQuantity(quantity));
        }

        Ok(quantity)
    }

    /// Parse a strictly positive quantity.
    ///
    /// Intended for ordinary nonzero trade executions.
    pub fn positive_quantity(value: &Value) -> Result<Decimal, NumericError> {
        let quantity = Self::quantity(value)?;

        if quantity == Decimal::ZERO {
            return Err(NumericError::NonPositiveQuantity(quantity));
        }

        Ok(quantity)
    }

    // ========================================================
    // TIMESTAMP NORMALIZATION
    // ========================================================

    /// Convert an integer timestamp into canonical
    /// nanoseconds.
    ///
    /// Uses checked integer multiplication.
    ///
    /// Negative timestamps are representable because
    /// the canonical timestamp type is signed.
    ///
    /// Call timestamp_ns_nonnegative() when the source
    /// contract explicitly requires a Unix timestamp
    /// at or after the epoch.
    pub fn timestamp_ns(value: &Value, unit: TimestampUnit) -> Result<TimestampNs, NumericError> {
        match unit {
            TimestampUnit::DecimalMilliseconds => Self::decimal_milliseconds_ns(value),
            TimestampUnit::DecimalSeconds => Self::decimal_seconds_ns(value),
            other => {
                let integer = Self::i64(value)?;

                integer.checked_mul(other.nanosecond_multiplier()).ok_or(
                    NumericError::TimestampOverflow {
                        value: integer,
                        unit: other.as_str(),
                    },
                )
            }
        }
    }

    /// Convert a timestamp while requiring a
    /// nonnegative source value.
    pub fn timestamp_ns_nonnegative(
        value: &Value,
        unit: TimestampUnit,
    ) -> Result<TimestampNs, NumericError> {
        // Reject negative input before converting units.
        //
        // This preserves the existing error contract:
        // NegativeTimestamp contains the source value
        // for integer timestamp units.

        match unit {
            TimestampUnit::DecimalMilliseconds => {
                let milliseconds = Self::decimal(value)?;

                if milliseconds < Decimal::ZERO {
                    // Decimal timestamps may contain fractions,
                    // so the existing i64 error cannot represent
                    // the original value exactly.
                    //
                    // Convert first for this special case.
                    let timestamp_ns = Self::decimal_milliseconds_ns(value)?;

                    return Err(NumericError::NegativeTimestamp(timestamp_ns));
                }

                Self::decimal_milliseconds_ns(value)
            }
            TimestampUnit::DecimalSeconds => {
                let seconds = Self::decimal_lossless_json(value)?;

                if seconds < Decimal::ZERO {
                    let timestamp_ns = Self::decimal_seconds_ns(value)?;

                    return Err(NumericError::NegativeTimestamp(timestamp_ns));
                }

                Self::decimal_seconds_ns(value)
            }
            other => {
                let integer = Self::i64(value)?;

                if integer < 0 {
                    return Err(NumericError::NegativeTimestamp(integer));
                }

                integer.checked_mul(other.nanosecond_multiplier()).ok_or(
                    NumericError::TimestampOverflow {
                        value: integer,
                        unit: other.as_str(),
                    },
                )
            }
        }
    }
    // ========================================================
    // EXACT DECIMAL-MILLISECOND TIMESTAMPS
    // ========================================================

    /// Convert an exact decimal-millisecond timestamp into
    /// canonical signed i64 nanoseconds.
    ///
    /// Accepted:
    ///
    /// - JSON decimal strings.
    /// - JSON signed/unsigned integer values.
    ///
    /// Rejected:
    ///
    /// - JSON floating-point values.
    /// - Values requiring fractional nanoseconds.
    /// - Values outside the signed i64 nanosecond range.
    ///
    /// No floating-point arithmetic is performed.
    pub fn decimal_milliseconds_ns(value: &Value) -> Result<TimestampNs, NumericError> {
        // ====================================================
        // EXACT DECIMAL EXTRACTION
        // ====================================================

        let milliseconds = Self::decimal(value)?;

        // ====================================================
        // CHECKED CONVERSION
        // ====================================================

        let nanoseconds = milliseconds
            .checked_mul(Decimal::from(1_000_000_u64))
            .ok_or_else(|| NumericError::DecimalTimestampOverflow(milliseconds.to_string()))?;

        // ====================================================
        // SUBNANOSECOND PRECISION
        // ====================================================

        // A fractional nanosecond cannot be represented
        // by the canonical TimestampNs = i64.
        //
        // Reject it rather than silently rounding.
        if nanoseconds.fract() != Decimal::ZERO {
            return Err(NumericError::SubNanosecondPrecision(
                milliseconds.to_string(),
            ));
        }

        // ====================================================
        // SIGNED I64 RANGE
        // ====================================================

        nanoseconds
            .to_i64()
            .ok_or_else(|| NumericError::DecimalTimestampOverflow(milliseconds.to_string()))
    }
}

// ============================================================
// JSON TYPE DESCRIPTION
// ============================================================

fn numeric_json_type(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

// ============================================================
// UNIT TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ========================================================
    // EXACT DECIMALS
    // ========================================================

    #[test]
    fn parses_exact_price_string() {
        let value = json!("80838.12345678");

        let result = NumericConverter::price(&value).unwrap();

        assert_eq!(result, Decimal::new(8083812345678, 8),);
    }

    #[test]
    fn preserves_decimal_precision() {
        let value = json!("0.123456789123456789");

        let result = NumericConverter::decimal(&value).unwrap();

        assert_eq!(result.to_string(), "0.123456789123456789",);
    }

    #[test]
    fn parses_exact_integer_json_number() {
        let value = json!(123456789_u64);

        assert_eq!(
            NumericConverter::decimal(&value).unwrap(),
            Decimal::from(123456789_u64),
        );
    }

    #[test]
    fn parses_negative_integer_decimal() {
        let value = json!(-42);

        assert_eq!(
            NumericConverter::decimal(&value).unwrap(),
            Decimal::from(-42),
        );
    }

    #[test]
    fn rejects_json_fractional_number() {
        let value = json!(1.25);

        assert!(matches!(
            NumericConverter::decimal(&value),
            Err(NumericError::FloatingPointNumber),
        ));
    }

    #[test]
    fn rejects_scientific_notation() {
        assert!(NumericConverter::decimal_string("1e-8").is_err(),);
    }

    #[test]
    fn rejects_unrepresentable_decimal_precision() {
        let result = NumericConverter::decimal_string("0.123456789123456789123456789123456789");

        assert!(
            result.is_err(),
            "decimal conversion must not silently round",
        );
    }

    #[test]
    fn rejects_invalid_decimal_string() {
        assert!(NumericConverter::decimal_string("invalid").is_err(),);
    }

    #[test]
    fn rejects_empty_decimal_string() {
        assert!(NumericConverter::decimal_string("   ").is_err(),);
    }

    // ========================================================
    // INTEGER CONVERSION
    // ========================================================

    #[test]
    fn parses_signed_integer_string() {
        assert_eq!(NumericConverter::i64(&json!("-123")).unwrap(), -123,);
    }

    #[test]
    fn parses_unsigned_integer_string() {
        assert_eq!(NumericConverter::u64(&json!("123")).unwrap(), 123,);
    }

    #[test]
    fn rejects_negative_unsigned_integer() {
        assert!(NumericConverter::u64(&json!(-1)).is_err(),);
    }

    #[test]
    fn rejects_integer_overflow() {
        let value = json!("999999999999999999999999999999");

        assert!(NumericConverter::i64(&value).is_err(),);
    }

    #[test]
    fn rejects_fractional_integer() {
        assert!(matches!(
            NumericConverter::i64(&json!(1.5)),
            Err(NumericError::FloatingPointNumber),
        ));
    }

    // ========================================================
    // PRICE VALIDATION
    // ========================================================

    #[test]
    fn rejects_zero_price() {
        assert!(matches!(
            NumericConverter::price(&json!("0")),
            Err(NumericError::NonPositivePrice(_)),
        ));
    }

    #[test]
    fn rejects_negative_price() {
        assert!(matches!(
            NumericConverter::price(&json!("-1")),
            Err(NumericError::NonPositivePrice(_)),
        ));
    }

    // ========================================================
    // QUANTITY VALIDATION
    // ========================================================

    #[test]
    fn accepts_zero_depth_quantity() {
        let result = NumericConverter::quantity(&json!("0")).unwrap();

        assert_eq!(result, Decimal::ZERO);
    }

    #[test]
    fn rejects_negative_quantity() {
        assert!(matches!(
            NumericConverter::quantity(&json!("-0.1")),
            Err(NumericError::NegativeQuantity(_)),
        ));
    }

    #[test]
    fn rejects_zero_trade_quantity() {
        assert!(matches!(
            NumericConverter::positive_quantity(&json!("0")),
            Err(NumericError::NonPositiveQuantity(_)),
        ));
    }

    // ========================================================
    // TIMESTAMP UNITS
    // ========================================================

    #[test]
    fn parses_timestamp_units() {
        assert_eq!(
            TimestampUnit::parse("milliseconds").unwrap(),
            TimestampUnit::Milliseconds,
        );

        assert!(TimestampUnit::parse("minutes").is_err(),);
    }

    // ========================================================
    // TIMESTAMP CONVERSIONS
    // ========================================================

    #[test]
    fn converts_seconds_to_nanoseconds() {
        let value = json!(1_700_000_000_i64);

        let result = NumericConverter::timestamp_ns(&value, TimestampUnit::Seconds).unwrap();

        assert_eq!(result, 1_700_000_000_000_000_000,);
    }

    #[test]
    fn converts_milliseconds_to_nanoseconds() {
        let value = json!(1_700_000_000_123_i64);

        let result = NumericConverter::timestamp_ns(&value, TimestampUnit::Milliseconds).unwrap();

        assert_eq!(result, 1_700_000_000_123_000_000,);
    }

    #[test]
    fn converts_microseconds_to_nanoseconds() {
        let value = json!(1_700_000_000_123_456_i64);

        let result = NumericConverter::timestamp_ns(&value, TimestampUnit::Microseconds).unwrap();

        assert_eq!(result, 1_700_000_000_123_456_000,);
    }

    #[test]
    fn preserves_nanosecond_timestamp() {
        let value = json!("1700000000123456789");

        let result = NumericConverter::timestamp_ns(&value, TimestampUnit::Nanoseconds).unwrap();

        assert_eq!(result, 1_700_000_000_123_456_789,);
    }

    #[test]
    fn converts_timestamp_string() {
        let value = json!("1792806119498");

        let result = NumericConverter::timestamp_ns(&value, TimestampUnit::Milliseconds).unwrap();

        assert_eq!(result, 1_792_806_119_498_000_000,);
    }

    #[test]
    fn rejects_timestamp_overflow() {
        let value = json!(i64::MAX);

        assert!(matches!(
            NumericConverter::timestamp_ns(&value, TimestampUnit::Seconds,),
            Err(NumericError::TimestampOverflow { .. }),
        ));
    }

    #[test]
    fn preserves_negative_timestamp_when_allowed() {
        let value = json!(-1);

        let result = NumericConverter::timestamp_ns(&value, TimestampUnit::Seconds).unwrap();

        assert_eq!(result, -1_000_000_000);
    }

    #[test]
    fn rejects_negative_timestamp_when_required() {
        let value = json!(-1);

        assert!(matches!(
            NumericConverter::timestamp_ns_nonnegative(&value, TimestampUnit::Milliseconds,),
            Err(NumericError::NegativeTimestamp(-1)),
        ));
    }

    #[test]
    fn rejects_fractional_timestamp() {
        assert!(matches!(
            NumericConverter::timestamp_ns(&json!(123.5), TimestampUnit::Seconds,),
            Err(NumericError::FloatingPointNumber),
        ));
    }
    // ========================================================
    // DECIMAL-MILLISECOND TIMESTAMPS
    // ========================================================

    #[test]
    fn parses_decimal_millisecond_unit() {
        assert_eq!(
            TimestampUnit::parse("milliseconds_decimal").unwrap(),
            TimestampUnit::DecimalMilliseconds,
        );
    }

    #[test]
    fn converts_fractional_milliseconds_exactly() {
        let timestamp = NumericConverter::timestamp_ns(
            &json!("1792806119498.123456"),
            TimestampUnit::DecimalMilliseconds,
        )
        .unwrap();

        assert_eq!(timestamp, 1_792_806_119_498_123_456,);
    }

    #[test]
    fn preserves_microsecond_precision() {
        let timestamp =
            NumericConverter::decimal_milliseconds_ns(&json!("1792806119498.123")).unwrap();

        assert_eq!(timestamp, 1_792_806_119_498_123_000,);
    }

    #[test]
    fn accepts_integer_millisecond_string() {
        let timestamp = NumericConverter::decimal_milliseconds_ns(&json!("1792806119498")).unwrap();

        assert_eq!(timestamp, 1_792_806_119_498_000_000,);
    }

    #[test]
    fn rejects_subnanosecond_precision() {
        let result = NumericConverter::decimal_milliseconds_ns(&json!("1792806119498.1234567"));

        assert!(matches!(
            result,
            Err(NumericError::SubNanosecondPrecision(_)),
        ));
    }

    #[test]
    fn rejects_fractional_json_number_timestamp() {
        let result = NumericConverter::decimal_milliseconds_ns(&json!(1792806119498.125));

        assert!(matches!(result, Err(NumericError::FloatingPointNumber),));
    }

    #[test]
    fn rejects_decimal_timestamp_overflow() {
        let result = NumericConverter::decimal_milliseconds_ns(&json!("999999999999999999999"));

        assert!(result.is_err());
    }

    #[test]
    fn rejects_negative_decimal_timestamp() {
        let result = NumericConverter::timestamp_ns_nonnegative(
            &json!("-1.5"),
            TimestampUnit::DecimalMilliseconds,
        );

        assert!(matches!(
            result,
            Err(NumericError::NegativeTimestamp(-1_500_000)),
        ));
    }

    #[test]
    fn preserves_one_nanosecond_fraction() {
        let result =
            NumericConverter::decimal_milliseconds_ns(&json!("1792806119498.000001")).unwrap();

        assert_eq!(result, 1_792_806_119_498_000_001,);
    }
    #[test]
    fn preserves_exact_fractional_json_number() {
        let value: serde_json::Value =
            serde_json::from_str(r#"{"price":80895.83,"quantity":0.101665}"#).unwrap();

        let price = NumericConverter::price_lossless_json(&value["price"]).unwrap();

        let quantity = NumericConverter::quantity_lossless_json(&value["quantity"]).unwrap();

        assert_eq!(price.to_string(), "80895.83");
        assert_eq!(quantity.to_string(), "0.101665");
    }

    #[test]
    fn preserves_decimal_precision_beyond_f64() {
        let value: serde_json::Value =
            serde_json::from_str("0.1234567890123456789012345678").unwrap();

        let decoded = NumericConverter::decimal_lossless_json(&value).unwrap();

        assert_eq!(decoded.to_string(), "0.1234567890123456789012345678",);
    }

    #[test]
    fn lossless_decimal_rejects_scientific_notation() {
        let value: serde_json::Value = serde_json::from_str("1.25e-8").unwrap();

        assert!(NumericConverter::decimal_lossless_json(&value).is_err());
    }

    #[test]
    fn lossless_decimal_rejects_excessive_precision() {
        let value: serde_json::Value =
            serde_json::from_str("0.12345678901234567890123456789").unwrap();

        assert!(NumericConverter::decimal_lossless_json(&value).is_err());
    }

    #[test]
    fn legacy_decimal_parser_remains_strict() {
        let value: serde_json::Value = serde_json::from_str("0.101665").unwrap();

        assert!(NumericConverter::decimal(&value).is_err());
    }
    #[test]
    fn converts_gateio_fractional_seconds_exactly() {
        // Parse original JSON text, preserving its
        // decimal representation via arbitrary_precision.
        let value: serde_json::Value = serde_json::from_str("1792806317.933").unwrap();

        let timestamp =
            NumericConverter::timestamp_ns_nonnegative(&value, TimestampUnit::DecimalSeconds)
                .unwrap();

        assert_eq!(timestamp, 1_792_806_317_933_000_000,);
    }

    #[test]
    fn decimal_seconds_preserve_nanosecond_precision() {
        let value: serde_json::Value = serde_json::from_str("1792806317.933123456").unwrap();

        let timestamp =
            NumericConverter::timestamp_ns_nonnegative(&value, TimestampUnit::DecimalSeconds)
                .unwrap();

        assert_eq!(timestamp, 1_792_806_317_933_123_456,);
    }

    #[test]
    fn decimal_seconds_reject_subnanosecond_precision() {
        let value: serde_json::Value = serde_json::from_str("1792806317.9331234567").unwrap();

        assert!(matches!(
            NumericConverter::timestamp_ns_nonnegative(&value, TimestampUnit::DecimalSeconds,),
            Err(NumericError::SubNanosecondSecondsPrecision(_))
        ));
    }

    #[test]
    fn decimal_seconds_reject_negative_timestamps() {
        let value: serde_json::Value = serde_json::from_str("-1.25").unwrap();

        assert!(matches!(
            NumericConverter::timestamp_ns_nonnegative(&value, TimestampUnit::DecimalSeconds,),
            Err(NumericError::NegativeTimestamp(_))
        ));
    }

    #[test]
    fn decimal_seconds_reject_overflow() {
        let value: serde_json::Value = serde_json::from_str("99999999999.123").unwrap();

        assert!(matches!(
            NumericConverter::timestamp_ns_nonnegative(&value, TimestampUnit::DecimalSeconds,),
            Err(NumericError::DecimalSecondsTimestampOverflow(_))
        ));
    }
}
