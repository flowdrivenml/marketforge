use rust_decimal::Decimal;
use serde_json::Value;

use crate::{
    canonical::TradeSide,
    error::{MarketForgeError, Result},
    normalize::{parse_buy_sell, parse_decimal},
};

use super::spec::{
    BoolSpec, BoolTransform, QuantitySpec, QuantityTransform, SideSpec, SideTransform,
};

pub fn transform_side(raw: &[u8], spec: &SideSpec) -> Result<TradeSide> {
    match spec.transform {
        SideTransform::Casefold => parse_buy_sell(raw),

        SideTransform::Map => {
            let key = parse_utf8(raw, "side")?.to_ascii_lowercase();

            let mapped = spec.values.get(&key).ok_or_else(|| {
                MarketForgeError::InvalidCanonical(format!(
                    "side mapping does not contain value: {key}"
                ))
            })?;

            parse_buy_sell(mapped.as_bytes())
        }

        SideTransform::SignToSide => {
            let quantity = parse_decimal(raw, "signed side quantity")?;

            let mapped = if quantity > Decimal::ZERO {
                spec.positive.as_deref()
            } else if quantity < Decimal::ZERO {
                spec.negative.as_deref()
            } else {
                return invalid("cannot determine trade side from zero quantity");
            };

            let mapped = mapped.ok_or_else(|| {
                MarketForgeError::InvalidConfiguration(
                    "sign_to_side mapping is incomplete".to_owned(),
                )
            })?;

            parse_buy_sell(mapped.as_bytes())
        }
    }
}

pub fn transform_quantity(raw: &[u8], spec: &QuantitySpec) -> Result<Decimal> {
    let quantity = parse_decimal(raw, "quantity")?;

    match spec.transform {
        None => Ok(quantity),

        Some(QuantityTransform::Abs) => Ok(quantity.abs()),
    }
}

pub fn transform_bool(raw: &[u8], spec: &BoolSpec) -> Result<bool> {
    match spec.transform {
        BoolTransform::Bool => {
            let value = parse_utf8(raw, "boolean")?.trim();

            match value.to_ascii_lowercase().as_str() {
                "true" | "1" => Ok(true),
                "false" | "0" => Ok(false),

                _ => invalid(format!("invalid boolean value: {value}")),
            }
        }

        BoolTransform::Equals => {
            let expected = spec.value.as_ref().ok_or_else(|| {
                MarketForgeError::InvalidConfiguration(
                    "equals transform requires a comparison value".to_owned(),
                )
            })?;

            let actual = parse_utf8(raw, "equals comparison")?.trim();

            Ok(value_equals(actual, expected))
        }
    }
}

fn value_equals(actual: &str, expected: &Value) -> bool {
    match expected {
        Value::String(value) => actual == value,

        Value::Number(value) => {
            let actual_number = actual.parse::<Decimal>();
            let expected_number = value.to_string().parse::<Decimal>();

            matches!(
                (actual_number, expected_number),
                (Ok(left), Ok(right)) if left == right
            )
        }

        Value::Bool(value) => match actual.to_ascii_lowercase().as_str() {
            "true" | "1" => *value,
            "false" | "0" => !*value,
            _ => false,
        },

        _ => false,
    }
}

fn parse_utf8<'a>(raw: &'a [u8], field: &str) -> Result<&'a str> {
    std::str::from_utf8(raw).map_err(|error| {
        MarketForgeError::InvalidCanonical(format!("invalid UTF-8 in {field}: {error}"))
    })
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(MarketForgeError::InvalidCanonical(message.into()))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn side_spec(transform: SideTransform) -> SideSpec {
        SideSpec {
            source: "side".to_owned(),
            transform,
            values: HashMap::new(),
            positive: None,
            negative: None,
        }
    }

    #[test]
    fn casefold_buy_sell() {
        let spec = side_spec(SideTransform::Casefold);

        assert_eq!(transform_side(b"BUY", &spec).unwrap(), TradeSide::Buy);
        assert_eq!(transform_side(b"Sell", &spec).unwrap(), TradeSide::Sell);
    }

    #[test]
    fn binance_buyer_maker_mapping() {
        let mut spec = side_spec(SideTransform::Map);

        spec.values.insert("true".to_owned(), "sell".to_owned());
        spec.values.insert("false".to_owned(), "buy".to_owned());

        assert_eq!(transform_side(b"true", &spec).unwrap(), TradeSide::Sell);
        assert_eq!(transform_side(b"false", &spec).unwrap(), TradeSide::Buy);
    }

    #[test]
    fn gateio_numeric_side_mapping() {
        let mut spec = side_spec(SideTransform::Map);

        spec.values.insert("1".to_owned(), "sell".to_owned());
        spec.values.insert("2".to_owned(), "buy".to_owned());

        assert_eq!(transform_side(b"1", &spec).unwrap(), TradeSide::Sell);
        assert_eq!(transform_side(b"2", &spec).unwrap(), TradeSide::Buy);
    }

    #[test]
    fn gateio_signed_side_mapping() {
        let mut spec = side_spec(SideTransform::SignToSide);

        spec.positive = Some("buy".to_owned());
        spec.negative = Some("sell".to_owned());

        assert_eq!(transform_side(b"15", &spec).unwrap(), TradeSide::Buy);
        assert_eq!(transform_side(b"-15", &spec).unwrap(), TradeSide::Sell);
        assert!(transform_side(b"0", &spec).is_err());
    }

    #[test]
    fn absolute_quantity() {
        let spec = QuantitySpec {
            source: "size".to_owned(),
            transform: Some(QuantityTransform::Abs),
        };

        assert_eq!(
            transform_quantity(b"-15", &spec).unwrap(),
            Decimal::new(15, 0)
        );
    }

    #[test]
    fn direct_quantity() {
        let spec = QuantitySpec {
            source: "size".to_owned(),
            transform: None,
        };

        assert_eq!(
            transform_quantity(b"2.5", &spec).unwrap(),
            Decimal::new(25, 1)
        );
    }

    #[test]
    fn bybit_rpi_boolean() {
        let spec = BoolSpec {
            source: "RPI".to_owned(),
            transform: BoolTransform::Bool,
            value: None,
        };

        assert!(transform_bool(b"1", &spec).unwrap());
        assert!(!transform_bool(b"0", &spec).unwrap());
        assert!(transform_bool(b"invalid", &spec).is_err());
    }

    #[test]
    fn okx_rpi_equals() {
        let spec = BoolSpec {
            source: "source".to_owned(),
            transform: BoolTransform::Equals,
            value: Some(Value::from(1)),
        };

        assert!(transform_bool(b"1", &spec).unwrap());
        assert!(!transform_bool(b"0", &spec).unwrap());
    }

    #[test]
    fn rejects_unmapped_side() {
        let spec = side_spec(SideTransform::Map);

        assert!(transform_side(b"unknown", &spec).is_err());
    }

    #[test]
    fn rejects_invalid_quantity() {
        let spec = QuantitySpec {
            source: "size".to_owned(),
            transform: None,
        };

        assert!(transform_quantity(b"invalid", &spec).is_err());
    }
}
