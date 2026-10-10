use rust_decimal::Decimal;
use serde_json::Value;

use crate::{
    canonical::BookSide,
    error::{MarketForgeError, Result},
    job::IntegrityCategory,
};

use super::{
    json::required_json_field,
    spec::{ScalarLevelSpec, SideSpec},
    transforms::{ExtractedLevel, extract_decimal},
};

pub fn extract_scalar_level(
    record: &Value,
    spec: &ScalarLevelSpec,
) -> Result<(BookSide, ExtractedLevel)> {
    let price = extract_decimal(required_json_field(record, &spec.price.source)?)?;

    let raw_quantity = extract_decimal(required_json_field(record, &spec.quantity.source)?)?;

    if price <= Decimal::ZERO {
        return invalid("depth price must be positive");
    }

    let side = extract_side(record, &spec.side, raw_quantity)?;

    let quantity = match spec.quantity.transform.as_deref() {
        None => raw_quantity,

        Some("abs") => raw_quantity.abs(),

        Some(other) => {
            return invalid(format!("unsupported scalar quantity transform: {other}"));
        }
    };

    if quantity < Decimal::ZERO {
        return invalid("depth quantity cannot be negative");
    }

    Ok((
        side,
        ExtractedLevel {
            price,
            quantity,
            order_count: None,
        },
    ))
}

fn extract_side(record: &Value, spec: &SideSpec, raw_quantity: Decimal) -> Result<BookSide> {
    match spec.transform.as_deref() {
        Some("sign_to_book_side") => {
            if raw_quantity > Decimal::ZERO {
                Ok(BookSide::Bid)
            } else if raw_quantity < Decimal::ZERO {
                Ok(BookSide::Ask)
            } else {
                invalid("cannot determine side from zero quantity")
            }
        }

        Some("map") => {
            let value = required_json_field(record, &spec.source)?;

            let key = match value {
                Value::String(value) => value.clone(),
                Value::Number(value) => value.to_string(),
                _ => return invalid("invalid scalar side value"),
            };

            let mapping = spec
                .values
                .as_ref()
                .and_then(Value::as_object)
                .ok_or_else(|| invalid_error("missing side mapping"))?;

            let side = mapping
                .get(&key)
                .and_then(Value::as_str)
                .ok_or_else(|| invalid_error("unmapped scalar side"))?;

            parse_side(side)
        }

        None => {
            let value = required_json_field(record, &spec.source)?;

            let side = value
                .as_str()
                .ok_or_else(|| invalid_error("side must be a string"))?;

            parse_side(side)
        }

        Some(other) => invalid(format!("unsupported scalar side transform: {other}")),
    }
}

fn parse_side(value: &str) -> Result<BookSide> {
    match value {
        "bid" => Ok(BookSide::Bid),
        "ask" => Ok(BookSide::Ask),
        _ => invalid(format!("invalid book side: {value}")),
    }
}

fn invalid_error(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::RecordIntegrity {
        category: IntegrityCategory::InvalidRecord,
        message: message.into(),
    }
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(invalid_error(message))
}
