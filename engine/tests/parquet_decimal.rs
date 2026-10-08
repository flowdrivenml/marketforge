// #![cfg(feature = "process")]

use rust_decimal::Decimal;

use marketforge_engine::process::parquet::{decimal_to_i256, i256_to_decimal};

fn assert_round_trip(value: &str) {
    let original: Decimal = value.parse().expect("parse decimal");

    let encoded = decimal_to_i256(original).expect("encode Decimal256");

    let decoded = i256_to_decimal(encoded).expect("decode Decimal256");

    assert_eq!(original, decoded, "decimal round-trip mismatch for {value}");
}

#[test]
fn round_trip_integer() {
    assert_round_trip("84500");
}

#[test]
fn round_trip_fractional_price() {
    assert_round_trip("84500.12345678");
}

#[test]
fn round_trip_small_quantity() {
    assert_round_trip("0.0000000000000000000000000001");
}

#[test]
fn round_trip_negative_value() {
    assert_round_trip("-123.456789");
}

#[test]
fn round_trip_zero() {
    assert_round_trip("0");
}

#[test]
fn round_trip_maximum_decimal() {
    assert_round_trip("79228162514264337593543950335");
}

#[test]
fn round_trip_minimum_decimal() {
    assert_round_trip("-79228162514264337593543950335");
}

#[test]
fn round_trip_high_precision_fraction() {
    assert_round_trip("1.1234567890123456789012345678");
}
