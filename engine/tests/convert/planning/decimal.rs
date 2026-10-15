use arrow_buffer::i256;

use marketforge_engine::historical::convert::ConversionDecimal;

fn decimal(value: &str) -> ConversionDecimal {
    let negative = value.starts_with('-');
    let unsigned = value.trim_start_matches('-');

    let mut parts = unsigned.split('.');
    let integer = parts.next().unwrap();
    let fractional = parts.next().unwrap_or("");

    assert!(parts.next().is_none());
    assert!(fractional.len() <= 28);

    let mantissa = format!(
        "{}{}{}",
        if negative { "-" } else { "" },
        integer,
        format!("{fractional:0<28}")
    );

    ConversionDecimal::new(i256::from_string(&mantissa).unwrap()).unwrap()
}

fn assert_decimal(actual: ConversionDecimal, expected: &str) {
    assert_eq!(actual, decimal(expected));
}

// -----------------------------------------------------------------------------
// Construction
// -----------------------------------------------------------------------------

#[test]
fn constructs_zero_and_one() {
    assert_decimal(ConversionDecimal::zero(), "0");
    assert_decimal(ConversionDecimal::one(), "1");
}

#[test]
fn preserves_28_decimal_places() {
    let value = decimal("0.1234567890123456789012345678");

    assert_decimal(value, "0.1234567890123456789012345678");
}

#[test]
fn accepts_maximum_decimal256_precision() {
    let maximum = "9".repeat(76);

    let value = ConversionDecimal::new(i256::from_string(&maximum).unwrap());

    assert!(value.is_ok());
}

#[test]
fn rejects_decimal256_precision_overflow() {
    let overflow = format!("1{}", "0".repeat(76));

    let value = ConversionDecimal::new(i256::from_string(&overflow).unwrap());

    assert!(value.is_err());
}

// -----------------------------------------------------------------------------
// Multiplication
// -----------------------------------------------------------------------------

#[test]
fn multiplies_ethbtc_by_btcusdt() {
    let ethbtc = decimal("0.05");
    let btcusdt = decimal("60000");

    assert_decimal(ethbtc.checked_mul(&btcusdt).unwrap(), "3000");
}

#[test]
fn multiplication_preserves_fractional_precision() {
    let a = decimal("0.00000001");
    let b = decimal("0.00000001");

    assert_decimal(a.checked_mul(&b).unwrap(), "0.0000000000000001");
}

#[test]
fn multiplication_uses_large_intermediate() {
    let a = decimal("1000000000000000000000000");
    let b = decimal("1000000000000000000000000");

    assert!(a.checked_mul(&b).is_err());
}

#[test]
fn multiplication_detects_precision_overflow() {
    let a = decimal("1000000000000000000000000000000");
    let b = decimal("1000000000000000000000000000000");

    assert!(a.checked_mul(&b).is_err());
}

// -----------------------------------------------------------------------------
// Division
// -----------------------------------------------------------------------------

#[test]
fn divides_exactly() {
    let a = decimal("1");
    let b = decimal("2");

    assert_decimal(a.checked_div(&b).unwrap(), "0.5");
}

#[test]
fn divides_repeating_decimal() {
    let a = decimal("1");
    let b = decimal("3");

    assert_decimal(a.checked_div(&b).unwrap(), "0.3333333333333333333333333333");
}

#[test]
fn rejects_division_by_zero() {
    let a = decimal("1");

    assert!(a.checked_div(&ConversionDecimal::zero()).is_err());
}

#[test]
fn computes_inverse() {
    let a = decimal("2");

    assert_decimal(a.checked_inverse().unwrap(), "0.5");
}

// -----------------------------------------------------------------------------
// Round-half-even
// -----------------------------------------------------------------------------

#[test]
fn rounds_half_to_even_down() {
    let a = decimal("0.0000000000000000000000000001");
    let b = decimal("0.5");

    assert_decimal(a.checked_mul(&b).unwrap(), "0");
}

#[test]
fn rounds_half_to_even_up() {
    let a = decimal("0.0000000000000000000000000003");
    let b = decimal("0.5");

    assert_decimal(a.checked_mul(&b).unwrap(), "0.0000000000000000000000000002");
}

#[test]
fn rounds_negative_half_to_even() {
    let a = decimal("-0.0000000000000000000000000003");
    let b = decimal("0.5");

    assert_decimal(
        a.checked_mul(&b).unwrap(),
        "-0.0000000000000000000000000002",
    );
}

// -----------------------------------------------------------------------------
// Real conversion examples
// -----------------------------------------------------------------------------

#[test]
fn converts_usdc_to_usdt_using_inverse_path() {
    let btcusdt = decimal("60000");
    let btcusdc = decimal("59950");

    let result = btcusdt.checked_div(&btcusdc).unwrap();

    assert_decimal(result, "1.0008340283569641367806505421");
}

#[test]
fn conversion_is_deterministic() {
    let a = decimal("0.05000000");
    let b = decimal("60000.00000000");

    let first = a.checked_mul(&b).unwrap();
    let second = a.checked_mul(&b).unwrap();

    assert_eq!(first, second);
}
