use std::time::Duration;

use marketforge_engine::live::metrics::{DerivedMetrics, DerivedMetricsSnapshot};

// ============================================================
// INITIAL STATE
// ============================================================

#[test]
fn empty_derived_metrics() {
    let metrics = DerivedMetrics::new(
        100,    // Configured source inputs
        65_536, // Input queue capacity
        true,   // Event-time ordering enabled
    );

    let snapshot = metrics.take_snapshot();

    assert_eq!(snapshot.inputs_total, 100);
    assert_eq!(snapshot.inputs_healthy, 0);

    assert_eq!(snapshot.events_received_total, 0);
    assert_eq!(snapshot.events_produced_total, 0);

    assert_eq!(snapshot.input_queue_depth, 0);
    assert_eq!(snapshot.input_queue_capacity, 65_536);
    assert_eq!(snapshot.input_overflows_total, 0);

    assert_eq!(snapshot.late_events_total, 0);
    assert_eq!(snapshot.conversion_failures_total, 0);

    assert_eq!(snapshot.last_output_at_ns, None);

    assert_eq!(snapshot.processing_latency.samples, 0);
    assert!(snapshot.buffer_wait_latency.is_some());
}

// ============================================================
// INPUT HEALTH AND EVENT COUNTERS
// ============================================================

#[test]
fn records_input_health_and_events() {
    let metrics = DerivedMetrics::new(100, 65_536, true);

    metrics.set_input_health(100, 98).unwrap();

    metrics.record_events_received(1_000);
    metrics.record_events_produced(950, 5_000);

    let snapshot = metrics.take_snapshot();

    assert_eq!(snapshot.inputs_total, 100);
    assert_eq!(snapshot.inputs_healthy, 98);

    assert_eq!(snapshot.events_received_total, 1_000);
    assert_eq!(snapshot.events_produced_total, 950);

    assert_eq!(snapshot.last_output_at_ns, Some(5_000));

    // Invalid updates must not modify the previous values.
    assert!(metrics.set_input_health(100, 101).is_err());

    assert_eq!(metrics.inputs_total(), 100);
    assert_eq!(metrics.inputs_healthy(), 98);
}

// ============================================================
// INPUT QUEUE
// ============================================================

#[test]
fn tracks_queue_capacity_and_overflows() {
    let metrics = DerivedMetrics::new(2, 1_000, false);

    metrics.set_input_queue_depth(750).unwrap();

    assert_eq!(metrics.input_queue_depth(), 750);

    // Queue occupancy cannot exceed its reported capacity.
    assert!(metrics.set_input_queue_depth(1_001).is_err());

    assert_eq!(metrics.input_queue_depth(), 750);

    // Capacity cannot be reduced below current occupancy.
    assert!(metrics.set_input_queue_capacity(500).is_err());

    assert_eq!(metrics.input_queue_capacity(), 1_000);

    metrics.set_input_queue_capacity(2_000).unwrap();

    metrics.record_input_overflow();
    metrics.record_input_overflow();

    let snapshot = metrics.take_snapshot();

    assert_eq!(snapshot.input_queue_depth, 750);
    assert_eq!(snapshot.input_queue_capacity, 2_000);
    assert_eq!(snapshot.input_overflows_total, 2);
}

// ============================================================
// PROCESSING FAILURES
// ============================================================

#[test]
fn records_late_events_and_conversion_failures() {
    let metrics = DerivedMetrics::new(3, 1_000, true);

    metrics.record_late_event();
    metrics.record_late_event();

    metrics.record_conversion_failure();

    let snapshot = metrics.take_snapshot();

    assert_eq!(snapshot.late_events_total, 2);
    assert_eq!(snapshot.conversion_failures_total, 1);

    // Cumulative counters must survive repeated snapshots.
    let second = metrics.take_snapshot();

    assert_eq!(second.late_events_total, 2);
    assert_eq!(second.conversion_failures_total, 1);
}

// ============================================================
// LATENCY
// ============================================================

#[test]
fn records_processing_and_optional_buffer_wait_latency() {
    let ordered = DerivedMetrics::new(3, 1_000, true);

    ordered.record_processing_latency_ns(100);
    ordered.record_processing_latency(Duration::from_nanos(300));

    assert!(ordered.record_buffer_wait_latency_ns(1_000));
    assert!(ordered.record_buffer_wait_latency(Duration::from_nanos(3_000)));

    let snapshot = ordered.take_snapshot();

    assert_eq!(snapshot.processing_latency.samples, 2);
    assert_eq!(snapshot.processing_latency.mean_ns, Some(200));
    assert_eq!(snapshot.processing_latency.max_ns, Some(300));

    let wait = snapshot.buffer_wait_latency.unwrap();

    assert_eq!(wait.samples, 2);
    assert_eq!(wait.mean_ns, Some(2_000));
    assert_eq!(wait.max_ns, Some(3_000));

    // Latency intervals reset without losing the last observation.
    let second = ordered.take_snapshot();

    assert_eq!(second.processing_latency.samples, 0);
    assert_eq!(second.processing_latency.last_ns, Some(300));

    // Arrival-order merging does not measure reorder waiting.
    let arrival_order = DerivedMetrics::new(2, 1_000, false);

    assert!(!arrival_order.uses_event_time_ordering());

    assert!(!arrival_order.record_buffer_wait_latency_ns(100));

    assert!(arrival_order.take_snapshot().buffer_wait_latency.is_none());
}

// ============================================================
// SERIALIZATION
// ============================================================

#[test]
fn derived_snapshot_json_round_trip() {
    let metrics = DerivedMetrics::new(2, 1_000, true);

    metrics.set_input_health(2, 2).unwrap();

    metrics.record_events_received(100);
    metrics.record_events_produced(95, 10_000);

    metrics.record_processing_latency_ns(500);
    metrics.record_buffer_wait_latency_ns(10_000);

    let original = metrics.take_snapshot();

    let serialized = serde_json::to_string(&original).unwrap();

    let restored: DerivedMetricsSnapshot = serde_json::from_str(&serialized).unwrap();

    assert_eq!(original, restored);
}
