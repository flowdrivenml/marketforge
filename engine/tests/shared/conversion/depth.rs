use arrow_buffer::i256;
use num_bigint::BigInt;

use marketforge_engine::shared::conversion::{
    ConversionDecimal, ConvertedDepthLevel, convert_depth_level, evaluation::EvaluatedConversion,
};

fn decimal(mantissa: i128) -> ConversionDecimal {
    ConversionDecimal::new(i256::from_i128(mantissa)).unwrap()
}

fn integer(value: i128) -> ConversionDecimal {
    decimal(value * 10_i128.pow(28))
}

fn conversion(numerator: i128, denominator: i128) -> EvaluatedConversion {
    let numerator = BigInt::from(numerator);
    let denominator = BigInt::from(denominator);

    EvaluatedConversion {
        rate: ConversionDecimal::from_ratio(numerator.clone(), denominator.clone()).unwrap(),
        numerator,
        denominator,
        references: Vec::new(),
    }
}

// -----------------------------------------------------------------------------
// Identity conversion
// -----------------------------------------------------------------------------

#[test]
fn identity_preserves_depth_values() {
    let result = convert_depth_level(integer(100), Some(integer(250)), &conversion(1, 1)).unwrap();

    assert_eq!(
        result,
        ConvertedDepthLevel {
            price_target: integer(100),
            quantity_quote_target: Some(integer(250)),
        }
    );
}

// -----------------------------------------------------------------------------
// Direct conversion
// -----------------------------------------------------------------------------

#[test]
fn converts_depth_price_and_quote_quantity() {
    let result = convert_depth_level(integer(5), Some(integer(12)), &conversion(3, 2)).unwrap();

    assert_eq!(result.price_target, decimal(75 * 10_i128.pow(27)),);

    assert_eq!(result.quantity_quote_target, Some(integer(18)),);
}

// -----------------------------------------------------------------------------
// Missing quote quantity
// -----------------------------------------------------------------------------

#[test]
fn preserves_missing_quote_quantity() {
    let result = convert_depth_level(integer(100), None, &conversion(2, 1)).unwrap();

    assert_eq!(result.price_target, integer(200));
    assert_eq!(result.quantity_quote_target, None);
}

// -----------------------------------------------------------------------------
// Inverse conversion
// -----------------------------------------------------------------------------

#[test]
fn supports_inverse_conversion() {
    let result = convert_depth_level(integer(100), Some(integer(50)), &conversion(1, 4)).unwrap();

    assert_eq!(result.price_target, integer(25));

    assert_eq!(
        result.quantity_quote_target,
        Some(decimal(125 * 10_i128.pow(27))),
    );
}

// -----------------------------------------------------------------------------
// Zero quantities
// -----------------------------------------------------------------------------

#[test]
fn preserves_zero_quote_quantity() {
    let result = convert_depth_level(
        integer(100),
        Some(ConversionDecimal::zero()),
        &conversion(3, 2),
    )
    .unwrap();

    assert_eq!(
        result.quantity_quote_target,
        Some(ConversionDecimal::zero()),
    );
}

// -----------------------------------------------------------------------------
// Exact rational arithmetic
// -----------------------------------------------------------------------------

#[test]
fn avoids_intermediate_rate_rounding() {
    let conversion = conversion(2, 3);

    let result = convert_depth_level(integer(3), Some(integer(6)), &conversion).unwrap();

    assert_eq!(result.price_target, integer(2));

    assert_eq!(result.quantity_quote_target, Some(integer(4)),);
}

// -----------------------------------------------------------------------------
// Overflow
// -----------------------------------------------------------------------------

#[test]
fn rejects_decimal_overflow() {
    let maximum = ConversionDecimal::new(
        i256::from_string(
            "9999999999999999999999999999999999999999999999999999999999999999999999999999",
        )
        .unwrap(),
    )
    .unwrap();

    assert!(convert_depth_level(maximum, None, &conversion(10, 1),).is_err());
}
