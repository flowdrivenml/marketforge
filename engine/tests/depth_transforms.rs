use marketforge_engine::{
    formats::depth::{
        LevelArraySpec, extract_decimal, extract_level_array, extract_optional_sequence,
        extract_timestamp_ns,
    },
    job::TimestampEncoding,
};

use rust_decimal::Decimal;
use serde_json::json;

// -----------------------------------------------------------------------------
// Decimal extraction
// -----------------------------------------------------------------------------

#[test]
fn extracts_decimal_from_string() {
    let value = json!("85000.12345678");

    assert_eq!(
        extract_decimal(&value).unwrap(),
        Decimal::new(8500012345678, 8)
    );
}

#[test]
fn extracts_decimal_from_json_number() {
    let value = json!(85000.5);

    assert_eq!(extract_decimal(&value).unwrap(), Decimal::new(850005, 1));
}

#[test]
fn rejects_invalid_decimal() {
    assert!(extract_decimal(&json!("invalid")).is_err());
    assert!(extract_decimal(&json!(null)).is_err());
}

// -----------------------------------------------------------------------------
// Timestamp extraction
// -----------------------------------------------------------------------------

#[test]
fn extracts_bybit_millisecond_timestamp() {
    let record = json!({
        "cts": 1788220800125_i64
    });

    let timestamp = extract_timestamp_ns(&record, "cts", TimestampEncoding::Milliseconds).unwrap();

    assert_eq!(timestamp, 1_788_220_800_125_000_000);
}

#[test]
fn extracts_okx_string_timestamp() {
    let record = json!({
        "ts": "1788220800001"
    });

    let timestamp = extract_timestamp_ns(&record, "ts", TimestampEncoding::Milliseconds).unwrap();

    assert_eq!(timestamp, 1_788_220_800_001_000_000);
}

#[test]
fn extracts_fractional_seconds_exactly() {
    let record = json!({
        "timestamp": "1788220800.123456789"
    });

    let timestamp = extract_timestamp_ns(&record, "timestamp", TimestampEncoding::Seconds).unwrap();

    assert_eq!(timestamp, 1_788_220_800_123_456_789);
}

#[test]
fn rejects_subnanosecond_precision() {
    let record = json!({
        "timestamp": "1788220800.1234567891"
    });

    assert!(extract_timestamp_ns(&record, "timestamp", TimestampEncoding::Seconds).is_err());
}

// -----------------------------------------------------------------------------
// Bybit level extraction
// -----------------------------------------------------------------------------

#[test]
fn extracts_bybit_price_levels() {
    let record = json!({
        "data": {
            "b": [
                ["85000.5", "2.5"],
                ["85000.0", "0"]
            ]
        }
    });

    let spec = LevelArraySpec {
        source: "data.b".to_owned(),
        price_index: 0,
        quantity_index: 1,
        order_count_index: None,
        decode: None,
    };

    let levels = extract_level_array(&record, &spec).unwrap();

    assert_eq!(levels.len(), 2);

    assert_eq!(levels[0].price, Decimal::new(850005, 1));
    assert_eq!(levels[0].quantity, Decimal::new(25, 1));

    assert_eq!(levels[1].quantity, Decimal::ZERO);
    assert_eq!(levels[0].order_count, None);
}

// -----------------------------------------------------------------------------
// OKX level extraction
// -----------------------------------------------------------------------------

#[test]
fn extracts_okx_levels_with_order_counts() {
    let record = json!({
        "bids": [
            ["85000", "12", "5"],
            ["84999", "0", "0"]
        ]
    });

    let spec = LevelArraySpec {
        source: "bids".to_owned(),
        price_index: 0,
        quantity_index: 1,
        order_count_index: Some(2),
        decode: None,
    };

    let levels = extract_level_array(&record, &spec).unwrap();

    assert_eq!(levels.len(), 2);

    assert_eq!(levels[0].quantity, Decimal::new(12, 0));
    assert_eq!(levels[0].order_count, Some(5));

    assert_eq!(levels[1].quantity, Decimal::ZERO);
    assert_eq!(levels[1].order_count, Some(0));
}

// -----------------------------------------------------------------------------
// Sequence extraction
// -----------------------------------------------------------------------------

#[test]
fn extracts_nested_sequence() {
    let record = json!({
        "data": {
            "u": 34096958
        }
    });

    assert_eq!(
        extract_optional_sequence(&record, Some("data.u")).unwrap(),
        Some(34096958)
    );
}

#[test]
fn accepts_missing_sequence_configuration() {
    let record = json!({});

    assert_eq!(extract_optional_sequence(&record, None).unwrap(), None);
}

#[test]
fn rejects_invalid_sequence() {
    let record = json!({
        "data": {
            "u": -1
        }
    });

    assert!(extract_optional_sequence(&record, Some("data.u")).is_err());
}

// -----------------------------------------------------------------------------
// Invalid level data
// -----------------------------------------------------------------------------

#[test]
fn rejects_negative_level_quantity() {
    let record = json!({
        "bids": [["85000", "-1"]]
    });

    let spec = LevelArraySpec {
        source: "bids".to_owned(),
        price_index: 0,
        quantity_index: 1,
        order_count_index: None,
        decode: None,
    };

    assert!(extract_level_array(&record, &spec).is_err());
}

#[test]
fn rejects_zero_price() {
    let record = json!({
        "bids": [["0", "5"]]
    });

    let spec = LevelArraySpec {
        source: "bids".to_owned(),
        price_index: 0,
        quantity_index: 1,
        order_count_index: None,
        decode: None,
    };

    assert!(extract_level_array(&record, &spec).is_err());
}
