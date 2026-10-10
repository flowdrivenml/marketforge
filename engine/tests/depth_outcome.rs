use std::path::{Path, PathBuf};

use marketforge_engine::{
    book::SequencePolicy,
    canonical::Exchange,
    formats::depth::{DepthContext, DepthEventBoundary, DepthProcessor},
    job::load_processing_job,
};

use serde_json::json;

fn project_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

fn processor() -> DepthProcessor {
    let root = project_root();

    let job = load_processing_job(
        root.join("data/.jobs/process/bybit-spot-spot-BTCUSDT-l2-20260901-20260904-d105.json"),
    )
    .unwrap();

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
        SequencePolicy::Consecutive,
    )
    .unwrap()
}

#[test]
fn classifies_initial_snapshot_and_updates() {
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

    let outcome = processor.process_record_with_boundary(&snapshot).unwrap();

    assert_eq!(outcome.boundary, DepthEventBoundary::Initialization);

    assert_eq!(outcome.events.len(), 3);

    let delta = json!({
        "type": "delta",
        "ts": 1788220800002_i64,
        "cts": 1788220800001_i64,
        "data": {
            "s": "BTCUSDT",
            "u": 101,
            "b": [["85000", "5"]],
            "a": []
        }
    });

    let outcome = processor.process_record_with_boundary(&delta).unwrap();

    assert_eq!(outcome.boundary, DepthEventBoundary::Changes);

    assert_eq!(outcome.events.len(), 1);
}

#[test]
fn recovery_snapshot_starts_new_segment() {
    let mut processor = processor();

    let snapshot = json!({
        "type": "snapshot",
        "ts": 1788220800001_i64,
        "cts": 1788220800000_i64,
        "data": {
            "s": "BTCUSDT",
            "u": 100,
            "b": [["85000", "2"]],
            "a": []
        }
    });

    processor.process_record_with_boundary(&snapshot).unwrap();

    processor.invalidate_synchronization();

    let recovery = json!({
        "type": "snapshot",
        "ts": 1788220800003_i64,
        "cts": 1788220800002_i64,
        "data": {
            "s": "BTCUSDT",
            "u": 200,
            "b": [["85000", "8"]],
            "a": [["85001", "6"]]
        }
    });

    let outcome = processor.process_record_with_boundary(&recovery).unwrap();

    assert_eq!(outcome.boundary, DepthEventBoundary::Initialization);

    assert_eq!(outcome.events.len(), 2);
}
