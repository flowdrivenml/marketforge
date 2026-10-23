use std::{thread, time::Duration};

use marketforge_engine::live::metrics::{RuntimeMetrics, RuntimeMetricsSnapshot};

// ============================================================
// INITIAL STATE
// ============================================================

#[test]
fn empty_runtime_metrics() {
    let metrics = RuntimeMetrics::new();

    let snapshot = metrics.snapshot();

    assert_eq!(snapshot.source_streams_running, 0);
    assert_eq!(snapshot.derived_streams_running, 0);
    assert_eq!(snapshot.streams_degraded, 0);
    assert_eq!(snapshot.streams_failed, 0);

    assert_eq!(snapshot.source_events_total, 0);
    assert_eq!(snapshot.derived_events_total, 0);
    assert_eq!(snapshot.internal_queue_overflows_total, 0);

    assert_eq!(snapshot.memory_rss_bytes, None);
    assert_eq!(snapshot.cpu_usage_percent, None);
}

// ============================================================
// STREAM COUNTS
// ============================================================

#[test]
fn tracks_stream_counts() {
    let metrics = RuntimeMetrics::new();

    metrics.set_running_stream_counts(150, 12);
    metrics.set_unhealthy_stream_counts(3, 1);

    let snapshot = metrics.snapshot();

    assert_eq!(snapshot.source_streams_running, 150);
    assert_eq!(snapshot.derived_streams_running, 12);
    assert_eq!(snapshot.streams_degraded, 3);
    assert_eq!(snapshot.streams_failed, 1);

    // Gauges represent current state, not cumulative counts.
    metrics.set_running_stream_counts(100, 8);
    metrics.set_unhealthy_stream_counts(0, 0);

    let updated = metrics.snapshot();

    assert_eq!(updated.source_streams_running, 100);
    assert_eq!(updated.derived_streams_running, 8);
    assert_eq!(updated.streams_degraded, 0);
    assert_eq!(updated.streams_failed, 0);
}

// ============================================================
// EVENT COUNTERS
// ============================================================

#[test]
fn tracks_source_derived_events_and_overflows() {
    let metrics = RuntimeMetrics::new();

    metrics.record_source_events(1_000);
    metrics.record_source_events(500);

    metrics.record_derived_events(750);
    metrics.record_derived_events(250);

    metrics.record_internal_queue_overflow();
    metrics.record_internal_queue_overflow();

    let snapshot = metrics.snapshot();

    assert_eq!(snapshot.source_events_total, 1_500);
    assert_eq!(snapshot.derived_events_total, 1_000);
    assert_eq!(snapshot.internal_queue_overflows_total, 2);

    // Zero-count updates do not change cumulative counters.
    metrics.record_source_events(0);
    metrics.record_derived_events(0);

    // Snapshot collection does not reset counters.
    assert_eq!(metrics.snapshot(), snapshot);
}

// ============================================================
// CPU AND MEMORY
// ============================================================

#[test]
fn records_cpu_and_memory_usage() {
    let metrics = RuntimeMetrics::new();

    metrics.set_memory_rss_bytes(1_073_741_824);
    metrics.set_cpu_usage_percent(175.5).unwrap();

    let snapshot = metrics.snapshot();

    assert_eq!(snapshot.memory_rss_bytes, Some(1_073_741_824));

    assert_eq!(snapshot.cpu_usage_percent, Some(175.5));

    // Resource measurements are gauges.
    metrics.set_memory_rss_bytes(536_870_912);
    metrics.set_cpu_usage_percent(25.0).unwrap();

    assert_eq!(metrics.memory_rss_bytes(), Some(536_870_912));
    assert_eq!(metrics.cpu_usage_percent(), Some(25.0));
}

// ============================================================
// INVALID CPU MEASUREMENTS
// ============================================================

#[test]
fn rejects_invalid_cpu_usage_without_changing_state() {
    let metrics = RuntimeMetrics::new();

    metrics.set_cpu_usage_percent(100.0).unwrap();

    for invalid in [-1.0, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        assert!(metrics.set_cpu_usage_percent(invalid).is_err());

        assert_eq!(metrics.cpu_usage_percent(), Some(100.0));
    }

    // More than 100% is valid for a multithreaded process.
    metrics.set_cpu_usage_percent(250.0).unwrap();

    assert_eq!(metrics.cpu_usage_percent(), Some(250.0));
}

// ============================================================
// UPTIME AND SERIALIZATION
// ============================================================

#[test]
fn uptime_and_snapshot_json_round_trip() {
    let metrics = RuntimeMetrics::new();

    metrics.set_running_stream_counts(10, 2);
    metrics.record_source_events(1_000);
    metrics.record_derived_events(500);

    // Check that uptime uses elapsed monotonic time.
    let before = metrics.uptime_seconds();

    thread::sleep(Duration::from_millis(5));

    let after = metrics.uptime_seconds();

    assert!(after >= before);

    let original = metrics.snapshot();

    let serialized = serde_json::to_string(&original).unwrap();

    let restored: RuntimeMetricsSnapshot = serde_json::from_str(&serialized).unwrap();

    assert_eq!(original, restored);

    assert_eq!(restored.source_streams_running, 10);
    assert_eq!(restored.derived_streams_running, 2);
    assert_eq!(restored.source_events_total, 1_000);
    assert_eq!(restored.derived_events_total, 500);
}
