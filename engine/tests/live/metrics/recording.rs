use marketforge_engine::live::metrics::{RecordingMetrics, RecordingMetricsSnapshot};

// ============================================================
// INITIAL STATE
// ============================================================

#[test]
fn empty_recording_metrics() {
    let metrics = RecordingMetrics::new();

    let snapshot = metrics.snapshot();

    assert_eq!(snapshot.records_written_total, 0);
    assert_eq!(snapshot.write_failures_total, 0);
    assert_eq!(snapshot.pending_records, 0);
    assert_eq!(snapshot.last_successful_write_at_ns, None);
}

// ============================================================
// SUCCESSFUL COMMITS
// ============================================================

#[test]
fn records_confirmed_commits() {
    let metrics = RecordingMetrics::new();

    metrics.record_successful_commit(1_000, 10_000);
    metrics.record_successful_commit(2_000, 20_000);

    let snapshot = metrics.snapshot();

    assert_eq!(snapshot.records_written_total, 3_000);
    assert_eq!(snapshot.last_successful_write_at_ns, Some(20_000));

    // A zero-record commit is a no-op.
    metrics.record_successful_commit(0, 30_000);

    assert_eq!(metrics.records_written_total(), 3_000);
    assert_eq!(metrics.last_successful_write_at_ns(), Some(20_000));
}

// ============================================================
// FAILED WRITES
// ============================================================

#[test]
fn failed_writes_do_not_count_as_committed_records() {
    let metrics = RecordingMetrics::new();

    metrics.record_write_failure();
    metrics.record_write_failure();

    assert_eq!(metrics.write_failures_total(), 2);
    assert_eq!(metrics.records_written_total(), 0);
    assert_eq!(metrics.last_successful_write_at_ns(), None);

    // A subsequent confirmed commit is counted normally.
    metrics.record_successful_commit(500, 10_000);

    assert_eq!(metrics.records_written_total(), 500);
    assert_eq!(metrics.write_failures_total(), 2);
}

// ============================================================
// PENDING RECORDS
// ============================================================

#[test]
fn pending_records_are_independent_of_commit_counters() {
    let metrics = RecordingMetrics::new();

    metrics.set_pending_records(10_000);

    assert_eq!(metrics.pending_records(), 10_000);

    metrics.record_successful_commit(5_000, 10_000);

    // The metrics component observes queue occupancy.
    // It does not modify the recorder's actual queue.
    assert_eq!(metrics.pending_records(), 10_000);

    // The authoritative recorder reports its updated count.
    metrics.set_pending_records(5_000);

    let snapshot = metrics.snapshot();

    assert_eq!(snapshot.pending_records, 5_000);
    assert_eq!(snapshot.records_written_total, 5_000);

    metrics.set_pending_records(0);

    assert_eq!(metrics.pending_records(), 0);

    // Snapshots never reset cumulative counters.
    assert_eq!(metrics.records_written_total(), 5_000);
}

// ============================================================
// SERIALIZATION
// ============================================================

#[test]
fn recording_snapshot_json_round_trip() {
    let metrics = RecordingMetrics::new();

    metrics.record_successful_commit(1_000, 50_000);
    metrics.record_write_failure();
    metrics.set_pending_records(250);

    let original = metrics.snapshot();

    let serialized = serde_json::to_string(&original).unwrap();

    let restored: RecordingMetricsSnapshot = serde_json::from_str(&serialized).unwrap();

    assert_eq!(original, restored);

    // Snapshot collection does not change the measurements.
    assert_eq!(metrics.snapshot(), original);
}
