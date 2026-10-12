#![cfg(feature = "process")]

use marketforge_engine::process::metrics::{
    IntegrityCategory, IntegrityDiagnostic, IntegrityMetrics, ProcessingCounters,
};

#[test]
fn processing_counters_validate() {
    let counters = ProcessingCounters {
        records_read: 100,
        records_matched: 80,
        records_skipped_instrument: 20,
        records_rejected: 5,
        events_normalized: 75,
        events_written: 75,
        tasks_completed: 1,
        tasks_failed: 0,
        records_rejected_unmatched: 0,
        records_processed: 75,
    };

    counters.validate().unwrap();
}

#[test]
fn processing_counters_reject_inconsistent_accounting() {
    let counters = ProcessingCounters {
        records_read: 100,
        records_matched: 80,
        records_skipped_instrument: 20,
        records_rejected: 5,
        events_normalized: 74,
        events_written: 74,
        tasks_completed: 1,
        tasks_failed: 0,
        records_rejected_unmatched: 0,
        records_processed: 0,
    };

    assert!(counters.validate().is_err());
}

#[test]
fn integrity_metrics_count_all_violations() {
    let mut metrics = IntegrityMetrics::default();

    metrics.max_diagnostics = 2;

    for index in 0..10 {
        metrics
            .record(IntegrityDiagnostic {
                category: IntegrityCategory::InvalidRecord,
                task_id: 120,
                source_record: Some(index),
                message: "invalid trade price".to_owned(),
                event_timestamp_ns: None,
            })
            .unwrap();
    }

    assert_eq!(metrics.counters.invalid_record, 10);
    assert_eq!(metrics.diagnostics.len(), 2);
}

#[test]
fn integrity_categories_are_serializable() {
    let category = IntegrityCategory::TimestampRegression;

    let json = serde_json::to_string(&category).unwrap();

    assert_eq!(json, "\"timestamp_regression\"");
}

#[test]
fn processing_counters_merge() {
    let mut first = ProcessingCounters {
        records_read: 100,
        records_matched: 100,
        events_normalized: 100,
        events_written: 100,
        tasks_completed: 1,
        ..Default::default()
    };

    let second = ProcessingCounters {
        records_read: 50,
        records_matched: 50,
        events_normalized: 50,
        events_written: 50,
        tasks_completed: 1,
        ..Default::default()
    };

    first.merge(&second).unwrap();

    assert_eq!(first.records_read, 150);
    assert_eq!(first.events_written, 150);
    assert_eq!(first.tasks_completed, 2);
}
