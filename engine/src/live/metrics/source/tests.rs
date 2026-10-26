use std::time::Duration;

use crate::live::metrics::{SourceMetrics, SourceMetricsSnapshot};

// ============================================================
// INITIAL STATE
// ============================================================

#[test]
fn empty_source_metrics() {
    let metrics = SourceMetrics::new();

    let snapshot = metrics.take_snapshot();

    assert_eq!(snapshot.messages_received_total, 0);
    assert_eq!(snapshot.events_produced_total, 0);
    assert_eq!(snapshot.normalization_errors_total, 0);
    assert_eq!(snapshot.reconnections_total, 0);

    assert_eq!(snapshot.last_message_at_ns, None);
    assert_eq!(snapshot.last_event_at_ns, None);
    assert_eq!(snapshot.source_delay_ns, None);

    assert_eq!(snapshot.processing_latency.samples, 0);
    assert_eq!(snapshot.processing_latency.last_ns, None);
}

// ============================================================
// EVENT COUNTERS
// ============================================================

#[test]
fn records_messages_events_errors_and_reconnections() {
    let metrics = SourceMetrics::new();

    metrics.record_message_received(1_000);
    metrics.record_message_received(2_000);

    // One native message can produce multiple canonical events.
    metrics.record_events_produced(25, 3_000);
    metrics.record_events_produced(10, 4_000);

    metrics.record_normalization_error();
    metrics.record_reconnection();

    let snapshot = metrics.take_snapshot();

    assert_eq!(snapshot.messages_received_total, 2);
    assert_eq!(snapshot.events_produced_total, 35);
    assert_eq!(snapshot.normalization_errors_total, 1);
    assert_eq!(snapshot.reconnections_total, 1);

    assert_eq!(snapshot.last_message_at_ns, Some(2_000));
    assert_eq!(snapshot.last_event_at_ns, Some(4_000));
}

#[test]
fn zero_produced_events_do_not_change_metrics() {
    let metrics = SourceMetrics::new();

    metrics.record_events_produced(0, 1_000);

    assert_eq!(metrics.events_produced_total(), 0);
    assert_eq!(metrics.last_event_at_ns(), None);
}

// ============================================================
// SIGNED SOURCE DELAY
// ============================================================

#[test]
fn source_delay_preserves_positive_and_negative_values() {
    let metrics = SourceMetrics::new();

    assert!(metrics.record_source_delay(125_000, 100_000));
    assert_eq!(metrics.source_delay_ns(), Some(25_000));

    // Exchange clock ahead of the local clock.
    assert!(metrics.record_source_delay(100_000, 125_000));
    assert_eq!(metrics.source_delay_ns(), Some(-25_000));

    // Overflow must not modify the previous valid measurement.
    assert!(!metrics.record_source_delay(i64::MAX, -1));
    assert_eq!(metrics.source_delay_ns(), Some(-25_000));
}

// ============================================================
// PROCESSING LATENCY
// ============================================================

#[test]
fn processing_latency_resets_interval_but_preserves_last() {
    let metrics = SourceMetrics::new();

    metrics.record_processing_latency_ns(100);
    metrics.record_processing_latency(Duration::from_nanos(300));

    let first = metrics.take_snapshot();

    assert_eq!(first.processing_latency.samples, 2);
    assert_eq!(first.processing_latency.last_ns, Some(300));
    assert_eq!(first.processing_latency.mean_ns, Some(200));
    assert_eq!(first.processing_latency.max_ns, Some(300));

    let second = metrics.take_snapshot();

    assert_eq!(second.processing_latency.samples, 0);
    assert_eq!(second.processing_latency.last_ns, Some(300));
    assert_eq!(second.processing_latency.mean_ns, None);
    assert_eq!(second.processing_latency.max_ns, None);

    // Cumulative event counters remain independent of latency resets.
    assert_eq!(second.messages_received_total, 0);
}

// ============================================================
// SERIALIZATION
// ============================================================

#[test]
fn source_snapshot_json_round_trip() {
    let metrics = SourceMetrics::new();

    metrics.record_message_received(1_000);
    metrics.record_events_produced(5, 2_000);
    metrics.record_source_delay(1_500, 1_000);
    metrics.record_processing_latency_ns(250);

    let original = metrics.take_snapshot();

    let serialized = serde_json::to_string(&original).unwrap();

    let restored: SourceMetricsSnapshot = serde_json::from_str(&serialized).unwrap();

    assert_eq!(original, restored);
}
