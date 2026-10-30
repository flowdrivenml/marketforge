use rust_decimal::Decimal;

use crate::{
    shared::error::{MarketForgeError, Result},
    shared::validation::IntegrityCategory,
};

pub fn parse_decimal(value: &[u8], field: &str) -> Result<Decimal> {
    let value = std::str::from_utf8(value).map_err(|error| MarketForgeError::RecordIntegrity {
        category: IntegrityCategory::InvalidRecord,
        message: format!("invalid UTF-8 in {field}: {error}"),
    })?;

    let parsed = if let Some((base, _)) = value.split_once(['e', 'E']) {
        Decimal::from_str_exact(base).and_then(|_| Decimal::from_scientific(value))
    } else {
        Decimal::from_str_exact(value)
    };
    parsed.map_err(|error| MarketForgeError::RecordIntegrity {
        category: IntegrityCategory::InvalidRecord,
        message: format!("invalid decimal in {field}: {value}: {error}"),
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

#[cfg(test)]
mod exact_input_tests {
    use super::*;
    #[test]
    fn rejects_decimal_that_would_be_rounded() {
        assert!(parse_decimal(b"0.12345678901234567890123456789", "price").is_err());
        assert_eq!(
            parse_decimal(b"0.1234567890123456789012345678", "price")
                .unwrap()
                .to_string(),
            "0.1234567890123456789012345678"
        );
    }
}

#[cfg(test)]
mod scientific_input_tests {
    use super::*;
    #[test]
    fn exact_scientific_archive_quantities_remain_supported() {
        assert_eq!(
            parse_decimal(b"7.98e-05", "quantity").unwrap().to_string(),
            "0.0000798"
        );
        assert!(parse_decimal(b"0.12345678901234567890123456789e-1", "quantity").is_err());
        assert!(parse_decimal(b"1e-29", "quantity").is_err());
    }
}
