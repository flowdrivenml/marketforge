use std::str::FromStr;

use rust_decimal::Decimal;

use crate::error::{MarketForgeError, Result};

pub fn parse_decimal(value: &[u8], field: &str) -> Result<Decimal> {
    let value = std::str::from_utf8(value).map_err(|error| {
        MarketForgeError::InvalidCanonical(format!("invalid UTF-8 in {field}: {error}",))
    })?;

    Decimal::from_str(value).map_err(|error| {
        MarketForgeError::InvalidCanonical(format!("invalid decimal in {field}: {value}: {error}",))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_exact_decimal() {
        let value = parse_decimal(b"84500.12345678", "price").expect("parse decimal");

        assert_eq!(value.to_string(), "84500.12345678",);
    }

    #[test]
    fn rejects_invalid_decimal() {
        let result = parse_decimal(b"not-a-number", "price");

        assert!(result.is_err());
    }
}
