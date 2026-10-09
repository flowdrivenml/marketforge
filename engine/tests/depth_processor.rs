use std::path::{Path, PathBuf};

use marketforge_engine::{
    canonical::Exchange,
    formats::depth::{DepthContext, DepthProcessor},
    job::load_processing_job,
};

use rust_decimal::Decimal;
use serde_json::json;

fn project_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("project root")
        .to_path_buf()
}

fn processor() -> DepthProcessor {
    let path = project_root()
        .join("data/.jobs/process/bybit-spot-spot-BTCUSDT-l2-20260901-20260904-d105.json");

    let job = load_processing_job(path).expect("load depth processing job");
    let task = &job.tasks[0];

    DepthProcessor::new(
        &task.normalizations,
        task.instrument.clone(),
        DepthContext {
            exchange: Exchange::Bybit,
            instrument_id: task.instrument_id,
            symbol: task.symbol.clone(),
            stream_id: task.stream_id.0.clone(),
        },
    )
    .expect("construct depth processor")
}

#[test]
fn processes_snapshot_and_absolute_updates() {
    let mut processor = processor();

    let snapshot = json!({
        "type": "snapshot",
        "ts": 1788220800001_i64,
        "cts": 1788220800000_i64,
        "data": {
            "s": "BTCUSDT",
            "u": 100,
            "b": [
                ["85000", "2"],
                ["84999", "3"]
            ],
            "a": [
                ["85001", "4"]
            ]
        }
    });

    let events = processor.process_record(&snapshot).unwrap();

    assert_eq!(events.len(), 3);
    assert!(processor.book().is_initialized());

    let delta = json!({
        "type": "delta",
        "ts": 1788220800003_i64,
        "cts": 1788220800002_i64,
        "data": {
            "s": "BTCUSDT",
            "u": 101,
            "b": [
                ["85000", "5"],
                ["84999", "0"]
            ],
            "a": [
                ["85001", "4"]
            ]
        }
    });

    let events = processor.process_record(&delta).unwrap();

    // One replacement and one deletion.
    // The unchanged ask is suppressed.
    assert_eq!(events.len(), 2);

    assert!(events.iter().any(|event| {
        event.price == Decimal::new(85000, 0) && event.quantity_base == Some(Decimal::new(5, 0))
    }));

    assert!(events.iter().any(|event| {
        event.price == Decimal::new(84999, 0) && event.quantity_base == Some(Decimal::ZERO)
    }));

    assert_eq!(
        events[0].envelope.event_timestamp_ns,
        1_788_220_800_002_000_000
    );
}

#[test]
fn rejects_delta_before_snapshot() {
    let mut processor = processor();

    let delta = json!({
        "type": "delta",
        "ts": 1788220800001_i64,
        "cts": 1788220800000_i64,
        "data": {
            "s": "BTCUSDT",
            "u": 101,
            "b": [["85000", "5"]],
            "a": []
        }
    });

    assert!(processor.process_record(&delta).is_err());
    assert!(!processor.book().is_initialized());
}

#[test]
fn rejects_invalid_batch_without_modifying_book() {
    let mut processor = processor();

    let snapshot = json!({
        "type": "snapshot",
        "ts": 1788220800001_i64,
        "cts": 1788220800000_i64,
        "data": {
            "s": "BTCUSDT",
            "u": 100,
            "b": [["85000", "2"]],
            "a": [["85001", "4"]]
        }
    });

    processor.process_record(&snapshot).unwrap();

    let previous_bids = processor.book().bids().clone();
    let previous_asks = processor.book().asks().clone();

    let invalid_delta = json!({
        "type": "delta",
        "ts": 1788220800003_i64,
        "cts": 1788220800002_i64,
        "data": {
            "s": "BTCUSDT",
            "u": 101,
            "b": [
                ["85000", "5"],
                ["84999", "-1"]
            ],
            "a": []
        }
    });

    assert!(processor.process_record(&invalid_delta).is_err());

    assert_eq!(processor.book().bids(), &previous_bids);
    assert_eq!(processor.book().asks(), &previous_asks);
}
