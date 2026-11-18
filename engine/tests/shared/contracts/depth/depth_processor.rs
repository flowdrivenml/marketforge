#[path = "../../../historical/support/mod.rs"]
mod support;

use marketforge_engine::{
    shared::domain::canonical::Exchange,
    shared::formats::depth::{DepthContext, DepthProcessor},
};

use rust_decimal::Decimal;
use serde_json::json;

fn processor() -> DepthProcessor {
    let job = support::migration::template("bybit.spot.spot.depth:BTCUSDT")
        .expect("resolve synthetic depth template by profile:symbol");
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
        marketforge_engine::historical::normalize::resolve_sequence_policy(task).unwrap(),
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

#[test]
fn invalidation_requires_new_snapshot() {
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

    assert!(processor.book().is_initialized());
    assert!(processor.sequence().is_synchronized());

    processor.invalidate_synchronization();

    assert!(!processor.book().is_initialized());
    assert!(!processor.sequence().is_synchronized());

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

    assert!(processor.process_record(&delta).is_err());

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

    let events = processor.process_record(&recovery).unwrap();

    assert_eq!(events.len(), 2);
    assert!(processor.book().is_initialized());
    assert!(processor.sequence().is_synchronized());
}

#[test]
fn bounded_observations_preserve_repeated_and_sparse_sequences() {
    use marketforge_engine::shared::{
        domain::{
            book::SequencePolicy,
            canonical::DepthSourceOperation,
            instrument::{InstrumentKind, InstrumentSpec},
        },
        normalization::{NormalizationConfig, QuantityEncoding, TargetSchema, TimestampEncoding},
    };

    let config = NormalizationConfig {
        timestamp_encoding: TimestampEncoding::Nanoseconds,
        quantity_encoding: QuantityEncoding::Base,
        target_schema: TargetSchema::Depth,
        rules: json!({
            "event_timestamp": {"source": "ts"},
            "sequence_start": {"source": "seq"},
            "bids": {"source": "levels", "price_field": "bp", "quantity_field": "bq"},
            "asks": {"source": "levels", "price_field": "ap", "quantity_field": "aq"},
            "snapshot": {
                "type": "bounded_observation",
                "levels_per_side": 1,
                "sequence_order": "nondecreasing"
            }
        }),
    };
    let mut processor = DepthProcessor::new(
        &[config],
        InstrumentSpec {
            instrument_kind: InstrumentKind::Equity,
            contract_kind: None,
            tick_size: Decimal::ONE,
            contract_value: None,
            contract_value_asset: None,
        },
        DepthContext {
            exchange: Exchange::from_code("example").unwrap(),
            instrument_id: 1,
            symbol: "EXAMPLE".into(),
            stream_id: "example.equity.equity.EXAMPLE.depth".into(),
        },
        SequencePolicy::Unsequenced,
    )
    .unwrap();

    let mut observation = json!({
        "ts": 1_730_125_800_000_054_842_i64,
        "seq": 100,
        "levels": [{"bp": 99, "bq": 2, "ap": 101, "aq": 4}]
    });
    for sequence in [100, 100, 150] {
        observation["seq"] = json!(sequence);
        let outcome = processor
            .process_record_with_boundary(&observation)
            .unwrap();
        assert_eq!(outcome.operation, DepthSourceOperation::BoundedObservation);
        assert_eq!(outcome.source.sequence_start, Some(sequence));
    }

    observation["seq"] = json!(149);
    let error = processor
        .process_record_with_boundary(&observation)
        .unwrap_err();
    assert!(error.to_string().contains("strictly decreased"));
    observation["seq"] = json!(151);
    observation["levels"]
        .as_array_mut()
        .unwrap()
        .push(json!({"bp": 98, "bq": 1, "ap": 102, "aq": 1}));
    let error = processor
        .process_record_with_boundary(&observation)
        .unwrap_err();
    assert!(error.to_string().contains("exceeds its declared depth"));
}
