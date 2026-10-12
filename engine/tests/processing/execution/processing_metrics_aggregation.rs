#![cfg(feature = "process")]

use marketforge_engine::process::metrics::{
    IntegrityCategory, IntegrityDiagnostic, IntegrityScope, ProcessingMetricsReport, TaskMetrics,
    TaskMetricsReport,
};

fn task_report(
    task_id: u64,
    stream_id: &str,
    records: u64,
    rejected: u64,
    timestamp: i64,
) -> TaskMetricsReport {
    let mut metrics = TaskMetrics::default();

    metrics.counters.records_read = records;
    metrics.counters.records_matched = records;
    metrics.counters.records_rejected = rejected;
    metrics.counters.events_normalized = records - rejected;
    metrics.counters.events_written = records - rejected;
    metrics.counters.tasks_completed = 1;

    metrics.record_timestamp(timestamp);

    let scope = IntegrityScope {
        task_id,
        stream_id: stream_id.to_owned(),
        source_file: format!("source-{task_id}.csv"),
    };

    for _ in 0..(records - rejected) {
        metrics
            .scoped_integrity
            .record(
                &scope,
                IntegrityCategory::InvalidRecord,
                false,
                Some(timestamp),
            )
            .unwrap();
    }

    for index in 0..rejected {
        metrics
            .scoped_integrity
            .record(&scope, IntegrityCategory::InvalidRecord, true, None)
            .unwrap();

        metrics
            .integrity
            .record(IntegrityDiagnostic {
                category: IntegrityCategory::InvalidRecord,
                task_id,
                source_record: Some(index + 1),
                event_timestamp_ns: None,
                message: "invalid trade price".to_owned(),
            })
            .unwrap();
    }

    TaskMetricsReport {
        task_id,
        stream_id: stream_id.to_owned(),

        counters: metrics.counters,
        integrity: metrics.integrity,
        scoped_integrity: metrics.scoped_integrity,

        start_timestamp_ns: metrics.start_timestamp_ns,
        end_timestamp_ns: metrics.end_timestamp_ns,
    }
}

#[test]
fn aggregates_multiple_tasks() {
    let mut report = ProcessingMetricsReport::default();

    report
        .add_task(task_report(1, "okx:BTCUSDT:trades", 100, 2, 1_000))
        .unwrap();

    report
        .add_task(task_report(2, "okx:BTCUSDT:trades", 200, 3, 2_000))
        .unwrap();

    assert_eq!(report.tasks.len(), 2);

    assert_eq!(report.counters.records_read, 300);
    assert_eq!(report.counters.records_rejected, 5);
    assert_eq!(report.counters.events_written, 295);

    assert_eq!(report.integrity.counters.invalid_record, 5);

    assert_eq!(
        report
            .scoped_integrity
            .global
            .get(IntegrityCategory::InvalidRecord)
            .violations,
        5
    );

    assert_eq!(report.start_timestamp_ns, Some(1_000));
    assert_eq!(report.end_timestamp_ns, Some(2_000));
}

#[test]
fn rejects_duplicate_task_reports() {
    let mut report = ProcessingMetricsReport::default();

    report
        .add_task(task_report(1, "stream-a", 10, 0, 1_000))
        .unwrap();

    let original = report.clone();

    assert!(
        report
            .add_task(task_report(1, "stream-a", 10, 0, 2_000))
            .is_err()
    );

    assert_eq!(report, original);
}

#[test]
fn aggregation_overflow_is_atomic() {
    let mut report = ProcessingMetricsReport::default();

    report.counters.records_read = u64::MAX;

    let original = report.clone();

    assert!(
        report
            .add_task(task_report(1, "stream-a", 1, 0, 1_000))
            .is_err()
    );

    assert_eq!(report, original);
}

#[test]
fn preserves_integrity_diagnostics() {
    let mut report = ProcessingMetricsReport::default();

    report
        .add_task(task_report(1, "stream-a", 10, 2, 1_000))
        .unwrap();

    report
        .add_task(task_report(2, "stream-a", 10, 3, 2_000))
        .unwrap();

    assert_eq!(report.integrity.counters.invalid_record, 5);
    assert_eq!(report.integrity.diagnostics.len(), 5);
}
