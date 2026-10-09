use rust_decimal::Decimal;
use serde_json::Value;

use crate::{
    canonical::{Price, Quantity},
    error::{MarketForgeError, Result},
    job::{IntegrityCategory, TimestampEncoding},
    normalize::parse_decimal,
};
use rust_decimal::prelude::ToPrimitive;

use super::json::required_json_field;

// -----------------------------------------------------------------------------
// Scalar extraction
// -----------------------------------------------------------------------------

/// Extract a JSON string or number as a decimal.
pub fn extract_decimal(value: &Value) -> Result<Decimal> {
    match value {
        Value::String(text) => parse_decimal(text.as_bytes(), "depth_decimal"),

        Value::Number(number) => parse_decimal(number.to_string().as_bytes(), "depth_decimal"),

        _ => invalid("expected numeric JSON value"),
    }
}

/// Extract an unsigned integer from a JSON string or number.
pub fn extract_u64(value: &Value) -> Result<u64> {
    match value {
        Value::String(text) => text
            .parse::<u64>()
            .map_err(|_| invalid_error("invalid unsigned integer")),

        Value::Number(number) => number
            .as_u64()
            .ok_or_else(|| invalid_error("invalid unsigned integer")),

        _ => invalid("expected unsigned integer"),
    }
}

// -----------------------------------------------------------------------------
// Timestamp extraction
// -----------------------------------------------------------------------------

pub fn extract_timestamp_ns(
    record: &Value,
    path: &str,
    encoding: TimestampEncoding,
) -> Result<i64> {
    let value = required_json_field(record, path)?;

    let timestamp = extract_decimal(value)?;

    let multiplier = match encoding {
        TimestampEncoding::Seconds => Decimal::new(1_000_000_000, 0),
        TimestampEncoding::Milliseconds => Decimal::new(1_000_000, 0),
        TimestampEncoding::Microseconds => Decimal::new(1_000, 0),
        TimestampEncoding::Nanoseconds => Decimal::ONE,
    };

    let timestamp_ns = timestamp
        .checked_mul(multiplier)
        .ok_or_else(|| invalid_error("timestamp multiplication overflow"))?;

    if !timestamp_ns.fract().is_zero() {
        return invalid("timestamp cannot be represented exactly in nanoseconds");
    }

    timestamp_ns
        .to_i64()
        .ok_or_else(|| invalid_error("timestamp exceeds i64 range"))
}

// -----------------------------------------------------------------------------
// Level extraction
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtractedLevel {
    pub price: Price,
    pub quantity: Quantity,
    pub order_count: Option<u64>,
}

/// Extract a price-level array using a compiled LevelArraySpec.
pub fn extract_level_array(
    record: &Value,
    spec: &super::LevelArraySpec,
) -> Result<Vec<ExtractedLevel>> {
    let value = required_json_field(record, &spec.source)?;

    let levels = value
        .as_array()
        .ok_or_else(|| invalid_error("depth levels must be a JSON array"))?;

    let mut extracted = Vec::with_capacity(levels.len());

    for level in levels {
        let fields = level
            .as_array()
            .ok_or_else(|| invalid_error("depth level must be a JSON array"))?;

        let price_value = fields
            .get(spec.price_index)
            .ok_or_else(|| invalid_error("missing depth price index"))?;

        let quantity_value = fields
            .get(spec.quantity_index)
            .ok_or_else(|| invalid_error("missing depth quantity index"))?;

        let price = extract_decimal(price_value)?;
        let quantity = extract_decimal(quantity_value)?;

        if price <= Decimal::ZERO {
            return invalid("depth price must be greater than zero");
        }

        if quantity < Decimal::ZERO {
            return invalid("depth quantity cannot be negative");
        }

        let order_count = match spec.order_count_index {
            Some(index) => {
                let value = fields
                    .get(index)
                    .ok_or_else(|| invalid_error("missing depth order count index"))?;

                Some(extract_u64(value)?)
            }

            None => None,
        };

        extracted.push(ExtractedLevel {
            price,
            quantity,
            order_count,
        });
    }

    Ok(extracted)
}

// -----------------------------------------------------------------------------
// Sequence extraction
// -----------------------------------------------------------------------------

pub fn extract_optional_sequence(record: &Value, path: Option<&str>) -> Result<Option<u64>> {
    let Some(path) = path else {
        return Ok(None);
    };

    let value = required_json_field(record, path)?;

    Ok(Some(extract_u64(value)?))
}

// -----------------------------------------------------------------------------
// Errors
// -----------------------------------------------------------------------------

fn invalid_error(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::RecordIntegrity {
        category: IntegrityCategory::InvalidRecord,
        message: message.into(),
    }
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(invalid_error(message))
}
