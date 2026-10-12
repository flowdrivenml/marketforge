use csv::ByteRecord;
use serde_json::json;

use marketforge_engine::formats::depth::DepthCsvAdapter;

#[test]
fn adapts_headerless_spot_row() {
    let schema = json!({
        "fields": [
            {"name": "timestamp", "position": 1},
            {"name": "side", "position": 2},
            {"name": "action", "position": 3},
            {"name": "price", "position": 4},
            {"name": "amount", "position": 5},
            {"name": "begin_id", "position": 6},
            {"name": "merged_count", "position": 7}
        ]
    });

    let adapter = DepthCsvAdapter::new(&schema).unwrap();

    let row = ByteRecord::from(vec![
        "1788220800",
        "2",
        "set",
        "78582.8",
        "0.003844",
        "39510882245",
        "0",
    ]);

    let record = adapter.adapt(&row).unwrap();

    assert_eq!(record["timestamp"], "1788220800");
    assert_eq!(record["side"], "2");
    assert_eq!(record["action"], "set");
    assert_eq!(record["price"], "78582.8");
    assert_eq!(record["amount"], "0.003844");
    assert_eq!(record["begin_id"], "39510882245");
    assert_eq!(record["merged_count"], "0");
}

#[test]
fn adapts_signed_perpetual_row() {
    let schema = json!({
        "fields": [
            {"name": "timestamp", "position": 1},
            {"name": "action", "position": 2},
            {"name": "price", "position": 3},
            {"name": "size", "position": 4},
            {"name": "begin_id", "position": 5},
            {"name": "merged_count", "position": 6}
        ]
    });

    let adapter = DepthCsvAdapter::new(&schema).unwrap();

    let row = ByteRecord::from(vec![
        "1788220800.1",
        "make",
        "78519.9",
        "-52.0",
        "5726726488",
        "1",
    ]);

    let record = adapter.adapt(&row).unwrap();

    assert_eq!(record["size"], "-52.0");
    assert_eq!(record["action"], "make");
    assert_eq!(record["timestamp"], "1788220800.1");
}

#[test]
fn rejects_incorrect_column_count() {
    let schema = json!({
        "fields": [
            {"name": "timestamp", "position": 1},
            {"name": "price", "position": 2}
        ]
    });

    let adapter = DepthCsvAdapter::new(&schema).unwrap();

    let row = ByteRecord::from(vec!["1788220800"]);

    assert!(adapter.adapt(&row).is_err());
}

#[test]
fn rejects_duplicate_positions() {
    let schema = json!({
        "fields": [
            {"name": "timestamp", "position": 1},
            {"name": "price", "position": 1}
        ]
    });

    assert!(DepthCsvAdapter::new(&schema).is_err());
}
