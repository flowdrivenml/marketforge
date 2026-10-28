use arrow_buffer::i256;
use num_bigint::BigInt;

use marketforge_engine::historical::convert::{
    ConversionDecimal, convert_trade_values, reference::EvaluatedConversion,
};

fn decimal(value: &str) -> ConversionDecimal {
    let negative = value.starts_with('-');
    let unsigned = value.trim_start_matches('-');

    let (integer, fractional) = unsigned.split_once('.').unwrap_or((unsigned, ""));

    assert!(fractional.len() <= 28);

    let mantissa = format!(
        "{}{}{:0<28}",
        if negative { "-" } else { "" },
        integer,
        fractional
    );

    ConversionDecimal::new(i256::from_string(&mantissa).unwrap()).unwrap()
}

fn conversion(numerator: i64, denominator: i64) -> EvaluatedConversion {
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
fn identity_preserves_trade_values() {
    let result =
        convert_trade_values(decimal("60000"), Some(decimal("120000")), &conversion(1, 1)).unwrap();

    assert_eq!(result.price_target, decimal("60000"));
    assert_eq!(result.quantity_quote_target, Some(decimal("120000")));
}

// -----------------------------------------------------------------------------
// Direct conversion
// -----------------------------------------------------------------------------

#[test]
fn converts_ethbtc_to_usdt() {
    let result =
        convert_trade_values(decimal("0.05"), Some(decimal("0.1")), &conversion(60000, 1)).unwrap();

    assert_eq!(result.price_target, decimal("3000"));
    assert_eq!(result.quantity_quote_target, Some(decimal("6000")));
}

// -----------------------------------------------------------------------------
// Inverse conversion
// -----------------------------------------------------------------------------

#[test]
fn converts_btcusdc_to_usdt() {
    let result = convert_trade_values(
        decimal("59950"),
        Some(decimal("119900")),
        &conversion(60000, 59950),
    )
    .unwrap();

    assert_eq!(result.price_target, decimal("60000"));
    assert_eq!(result.quantity_quote_target, Some(decimal("120000")));
}

// -----------------------------------------------------------------------------
// Nullable quantities
// -----------------------------------------------------------------------------

#[test]
fn preserves_null_quote_quantity() {
    let result = convert_trade_values(decimal("0.05"), None, &conversion(60000, 1)).unwrap();

    assert_eq!(result.price_target, decimal("3000"));
    assert_eq!(result.quantity_quote_target, None);
}

// -----------------------------------------------------------------------------
// Decimal precision
// -----------------------------------------------------------------------------

#[test]
fn preserves_small_decimal_values() {
    let result = convert_trade_values(
        decimal("0.00000001"),
        Some(decimal("0.00000002")),
        &conversion(3, 2),
    )
    .unwrap();

    assert_eq!(result.price_target, decimal("0.000000015"));
    assert_eq!(result.quantity_quote_target, Some(decimal("0.00000003")));
}

#[test]
fn rounds_half_even_down() {
    let result = convert_trade_values(
        decimal("0.0000000000000000000000000001"),
        None,
        &conversion(1, 2),
    )
    .unwrap();

    assert_eq!(result.price_target, decimal("0"));
}

#[test]
fn rounds_half_even_up() {
    let result = convert_trade_values(
        decimal("0.0000000000000000000000000003"),
        None,
        &conversion(1, 2),
    )
    .unwrap();

    assert_eq!(
        result.price_target,
        decimal("0.0000000000000000000000000002")
    );
}

// -----------------------------------------------------------------------------
// Large intermediate arithmetic
// -----------------------------------------------------------------------------

#[test]
fn supports_large_intermediate_products() {
    let result = convert_trade_values(
        decimal("1000000000000000000000000"),
        None,
        &conversion(1000000000000000000, 1),
    )
    .unwrap();

    assert_eq!(
        result.price_target,
        decimal("1000000000000000000000000000000000000000000")
    );
}

// -----------------------------------------------------------------------------
// Overflow
// -----------------------------------------------------------------------------

#[test]
fn rejects_decimal256_precision_overflow() {
    let result = convert_trade_values(
        decimal("1000000000000000000000000000000"),
        None,
        &conversion(1000000000000000000, 1),
    );

    assert!(result.is_err());
}

// -----------------------------------------------------------------------------
// Determinism
// -----------------------------------------------------------------------------

#[test]
fn conversion_is_deterministic() {
    let price = decimal("59950.1234567890123456789012345678");
    let quantity = Some(decimal("119900.987654321"));

    let rate = conversion(60000, 59950);

    let first = convert_trade_values(price, quantity, &rate).unwrap();
    let second = convert_trade_values(price, quantity, &rate).unwrap();

    assert_eq!(first, second);
}
