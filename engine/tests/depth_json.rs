use marketforge_engine::formats::depth::{
    EventFilter, matches_event_filter, required_json_field, resolve_json_path,
};

use serde_json::json;

#[test]
fn resolves_nested_bybit_fields() {
    let record = json!({
        "type": "delta",
        "cts": 1788220800125_i64,
        "data": {
            "s": "BTCUSDT",
            "u": 123,
            "b": [["100.5", "2.5"]],
            "a": []
        }
    });

    assert_eq!(
        resolve_json_path(&record, "data.s"),
        Some(&json!("BTCUSDT"))
    );

    assert_eq!(resolve_json_path(&record, "data.u"), Some(&json!(123)));

    assert!(required_json_field(&record, "data.b").is_ok());
    assert!(required_json_field(&record, "data.missing").is_err());
}

#[test]
fn resolves_okx_fields() {
    let record = json!({
        "instId": "BTC-USDT",
        "action": "snapshot",
        "ts": "1788220800001",
        "bids": [["100", "5", "3"]],
        "asks": []
    });

    assert_eq!(
        resolve_json_path(&record, "instId"),
        Some(&json!("BTC-USDT"))
    );

    assert_eq!(
        resolve_json_path(&record, "bids"),
        Some(&json!([["100", "5", "3"]]))
    );
}

#[test]
fn matches_bybit_delta_filter() {
    let record = json!({
        "type": "delta"
    });

    let filter = EventFilter {
        source: "type".to_owned(),
        equals: Some("delta".to_owned()),
        one_of: None,
    };

    assert!(matches_event_filter(&record, &filter).unwrap());
}

#[test]
fn rejects_nonmatching_filter() {
    let record = json!({
        "type": "snapshot"
    });

    let filter = EventFilter {
        source: "type".to_owned(),
        equals: Some("delta".to_owned()),
        one_of: None,
    };

    assert!(!matches_event_filter(&record, &filter).unwrap());
}

#[test]
fn supports_multiple_event_types() {
    let record = json!({
        "action": "make"
    });

    let filter = EventFilter {
        source: "action".to_owned(),
        equals: None,
        one_of: Some(vec!["make".to_owned(), "take".to_owned()]),
    };

    assert!(matches_event_filter(&record, &filter).unwrap());
}

#[test]
fn rejects_missing_filter_field() {
    let record = json!({
        "data": {}
    });

    let filter = EventFilter {
        source: "type".to_owned(),
        equals: Some("delta".to_owned()),
        one_of: None,
    };

    assert!(matches_event_filter(&record, &filter).is_err());
}
