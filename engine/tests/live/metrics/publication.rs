use std::time::Duration;

use marketforge_engine::live::metrics::{
    PublicationConnectionMetrics, PublicationConnectionSnapshot, PublicationMetrics,
    PublicationMetricsSnapshot,
};

// ============================================================
// INITIAL STATE
// ============================================================

#[test]
fn empty_publication_metrics() {
    let metrics = PublicationMetrics::new();

    let snapshot = metrics.take_snapshot();

    assert_eq!(snapshot.connections_active, 0);
    assert_eq!(snapshot.subscriptions_active, 0);
    assert_eq!(snapshot.events_queued_total, 0);
    assert_eq!(snapshot.queued_bytes_total, 0);
    assert_eq!(snapshot.queue_overflows_total, 0);
    assert_eq!(snapshot.connections_disconnected_total, 0);

    assert_eq!(snapshot.publication_latency.samples, 0);
    assert_eq!(snapshot.publication_latency.last_ns, None);

    let connection = PublicationConnectionMetrics::new("conn_1", 16_777_216).unwrap();

    let snapshot = connection.take_snapshot();

    assert_eq!(snapshot.connection_id, "conn_1");
    assert!(snapshot.subscriptions.is_empty());
    assert_eq!(snapshot.queued_bytes, 0);
    assert_eq!(snapshot.queue_limit_bytes, 16_777_216);
    assert_eq!(snapshot.events_queued_total, 0);
    assert_eq!(snapshot.queue_overflows_total, 0);
}

// ============================================================
// GLOBAL COUNTERS
// ============================================================

#[test]
fn records_global_publication_activity() {
    let metrics = PublicationMetrics::new();

    metrics.set_connections_active(3);
    metrics.set_subscriptions_active(7);

    // One event queued to three consumers counts as
    // three event-to-connection admissions.
    metrics.record_events_queued(3);
    metrics.record_events_queued(2);

    metrics.set_queued_bytes_total(12_000);

    metrics.record_queue_overflow();
    metrics.record_connection_disconnected();

    metrics.set_connections_active(2);
    metrics.set_subscriptions_active(5);

    let snapshot = metrics.take_snapshot();

    assert_eq!(snapshot.connections_active, 2);
    assert_eq!(snapshot.subscriptions_active, 5);
    assert_eq!(snapshot.events_queued_total, 5);
    assert_eq!(snapshot.queued_bytes_total, 12_000);
    assert_eq!(snapshot.queue_overflows_total, 1);
    assert_eq!(snapshot.connections_disconnected_total, 1);

    // Counters remain cumulative after snapshot collection.
    let second = metrics.take_snapshot();

    assert_eq!(second.events_queued_total, 5);
    assert_eq!(second.queue_overflows_total, 1);
}

// ============================================================
// CONNECTION SUBSCRIPTIONS
// ============================================================

#[test]
fn connection_tracks_subscriptions_and_queue_occupancy() {
    let connection = PublicationConnectionMetrics::new("conn_1", 16_777_216).unwrap();

    connection.set_subscriptions([
        "source_a".to_string(),
        "derived_c".to_string(),
        "source_a".to_string(),
    ]);

    connection.set_queued_bytes(8_388_608);
    connection.record_events_queued(100);

    let snapshot = connection.take_snapshot();

    // BTreeSet removes duplicate subscriptions and
    // produces deterministic ordering.
    assert_eq!(
        snapshot.subscriptions,
        vec!["derived_c".to_string(), "source_a".to_string(),]
    );

    assert_eq!(snapshot.queued_bytes, 8_388_608);
    assert_eq!(snapshot.events_queued_total, 100);

    // Metrics report the authoritative queue state;
    // they do not own or enforce queue capacity.
    connection.set_queued_bytes(20_000_000);

    assert_eq!(connection.queued_bytes(), 20_000_000);
}

// ============================================================
// RUNTIME QUEUE LIMIT CHANGES
// ============================================================

#[test]
fn connection_supports_runtime_queue_limit_updates() {
    let connection = PublicationConnectionMetrics::new("conn_1", 16_777_216).unwrap();

    // Increase the observed limit.
    connection.set_queue_limit_bytes(33_554_432).unwrap();

    assert_eq!(connection.queue_limit_bytes(), 33_554_432);

    // Decrease the observed limit.
    connection.set_queue_limit_bytes(4_194_304).unwrap();

    assert_eq!(connection.queue_limit_bytes(), 4_194_304);

    // A zero limit is invalid and must not change state.
    assert!(connection.set_queue_limit_bytes(0).is_err());

    assert_eq!(connection.queue_limit_bytes(), 4_194_304);

    // Actual disconnection remains the publication
    // manager's responsibility.
    connection.record_queue_overflow();
    connection.mark_disconnected();

    assert!(connection.is_disconnected());
    assert_eq!(connection.queue_overflows_total(), 1);

    assert!(PublicationConnectionMetrics::new("invalid", 0).is_err());
}

// ============================================================
// PUBLICATION LATENCY
// ============================================================

#[test]
fn publication_latency_resets_intervals() {
    let global = PublicationMetrics::new();

    let connection = PublicationConnectionMetrics::new("conn_1", 16_777_216).unwrap();

    global.record_publication_latency_ns(100);
    global.record_publication_latency(Duration::from_nanos(300));

    connection.record_publication_latency_ns(100);
    connection.record_publication_latency(Duration::from_nanos(300));

    let global_snapshot = global.take_snapshot();
    let connection_snapshot = connection.take_snapshot();

    assert_eq!(global_snapshot.publication_latency.samples, 2);
    assert_eq!(global_snapshot.publication_latency.mean_ns, Some(200));
    assert_eq!(global_snapshot.publication_latency.max_ns, Some(300));

    assert_eq!(connection_snapshot.publication_latency.samples, 2);
    assert_eq!(connection_snapshot.publication_latency.mean_ns, Some(200));

    // New snapshots contain no interval samples,
    // but preserve the most recent observation.
    let next = global.take_snapshot();

    assert_eq!(next.publication_latency.samples, 0);
    assert_eq!(next.publication_latency.last_ns, Some(300));
    assert_eq!(next.publication_latency.mean_ns, None);
}

// ============================================================
// JSON SERIALIZATION
// ============================================================

#[test]
fn publication_snapshots_json_round_trip() {
    let global = PublicationMetrics::new();

    global.set_connections_active(2);
    global.set_subscriptions_active(4);
    global.record_events_queued(100);
    global.record_publication_latency_ns(500);

    let original_global = global.take_snapshot();

    let serialized = serde_json::to_string(&original_global).unwrap();

    let restored_global: PublicationMetricsSnapshot = serde_json::from_str(&serialized).unwrap();

    assert_eq!(original_global, restored_global);

    let connection = PublicationConnectionMetrics::new("conn_42", 16_777_216).unwrap();

    connection.set_subscriptions(["source_a".to_string(), "derived_c".to_string()]);

    connection.set_queued_bytes(1_024);
    connection.record_events_queued(10);
    connection.record_publication_latency_ns(250);

    let original_connection = connection.take_snapshot();

    let serialized = serde_json::to_string(&original_connection).unwrap();

    let restored_connection: PublicationConnectionSnapshot =
        serde_json::from_str(&serialized).unwrap();

    assert_eq!(original_connection, restored_connection);
}
