use std::sync::Arc;

use marketforge_engine::live::metrics::{
    DerivedMetrics, DerivedMetricsRef, MetricsSnapshot, MetricsSnapshotCollector,
    PublicationMetrics, RecordingMetrics, RuntimeMetrics, SourceMetrics, SourceMetricsRef,
};

// ============================================================
// COMPLETE METRICS COLLECTION
// ============================================================

#[test]
fn collects_all_metrics_components() {
    let collector = MetricsSnapshotCollector::new();

    let runtime = RuntimeMetrics::new();
    let publication = PublicationMetrics::new();
    let recording = RecordingMetrics::new();

    let source = SourceMetrics::new();
    let derived = DerivedMetrics::new(2, 1_000, true);

    // Global runtime.
    runtime.set_running_stream_counts(1, 1);
    runtime.record_source_events(100);
    runtime.record_derived_events(80);

    // Source stream.
    source.record_message_received(1_000);
    source.record_events_produced(100, 2_000);
    source.record_processing_latency_ns(250);

    // Derived stream.
    derived.set_input_health(2, 2).unwrap();
    derived.record_events_received(100);
    derived.record_events_produced(80, 3_000);
    derived.record_processing_latency_ns(500);
    derived.record_buffer_wait_latency_ns(10_000);

    // Publication.
    publication.set_connections_active(2);
    publication.set_subscriptions_active(3);
    publication.record_events_queued(150);
    publication.record_publication_latency_ns(750);

    // PostgreSQL recording.
    recording.record_successful_commit(50, 4_000);
    recording.set_pending_records(10);

    let snapshot = collector
        .collect(
            &runtime,
            &publication,
            &recording,
            &[SourceMetricsRef {
                stream_id: "source_a",
                generation: 1,
                metrics: &source,
            }],
            &[DerivedMetricsRef {
                stream_id: "derived_c",
                generation: 1,
                metrics: &derived,
            }],
        )
        .unwrap();

    // Global runtime.
    assert_eq!(snapshot.runtime.source_streams_running, 1);
    assert_eq!(snapshot.runtime.derived_streams_running, 1);
    assert_eq!(snapshot.runtime.source_events_total, 100);
    assert_eq!(snapshot.runtime.derived_events_total, 80);

    // Source stream.
    let source_snapshot = &snapshot.sources["source_a"];

    assert_eq!(source_snapshot.metrics.messages_received_total, 1);
    assert_eq!(source_snapshot.metrics.events_produced_total, 100);
    assert_eq!(source_snapshot.metrics.processing_latency.samples, 1);

    // Derived stream.
    let derived_snapshot = &snapshot.derived["derived_c"];

    assert_eq!(derived_snapshot.metrics.inputs_healthy, 2);
    assert_eq!(derived_snapshot.metrics.events_produced_total, 80);
    assert_eq!(derived_snapshot.metrics.processing_latency.samples, 1);

    assert_eq!(
        derived_snapshot
            .metrics
            .buffer_wait_latency
            .as_ref()
            .unwrap()
            .samples,
        1
    );

    // Publication.
    assert_eq!(snapshot.publication.connections_active, 2);
    assert_eq!(snapshot.publication.subscriptions_active, 3);
    assert_eq!(snapshot.publication.events_queued_total, 150);
    assert_eq!(snapshot.publication.publication_latency.samples, 1);

    // Recording.
    assert_eq!(snapshot.recording.records_written_total, 50);
    assert_eq!(snapshot.recording.pending_records, 10);

    // First collection has no rate baseline.
    assert!(snapshot.interval_seconds.is_none());

    assert!(snapshot.throughput.source_events_per_second.is_none());
}

// ============================================================
// IMMUTABLE SNAPSHOT UPDATES
// ============================================================

#[test]
fn new_collections_preserve_previous_snapshots() {
    let collector = MetricsSnapshotCollector::new();

    let runtime = RuntimeMetrics::new();
    let publication = PublicationMetrics::new();
    let recording = RecordingMetrics::new();

    let first = collector
        .collect(&runtime, &publication, &recording, &[], &[])
        .unwrap();

    assert_eq!(first.runtime.source_events_total, 0);

    runtime.record_source_events(100);

    let second = collector
        .collect(&runtime, &publication, &recording, &[], &[])
        .unwrap();

    // Previous immutable snapshot remains unchanged.
    assert_eq!(first.runtime.source_events_total, 0);

    // New snapshot contains updated measurements.
    assert_eq!(second.runtime.source_events_total, 100);

    // The collector publishes the latest Arc.
    let latest = collector.latest().unwrap();

    assert!(Arc::ptr_eq(&latest, &second));
    assert!(!Arc::ptr_eq(&first, &second));
}

// ============================================================
// JSON SERIALIZATION
// ============================================================

#[test]
fn complete_metrics_snapshot_json_round_trip() {
    let collector = MetricsSnapshotCollector::new();

    let runtime = RuntimeMetrics::new();
    let publication = PublicationMetrics::new();
    let recording = RecordingMetrics::new();

    let source = SourceMetrics::new();
    let derived = DerivedMetrics::new(1, 1_000, false);

    runtime.set_running_stream_counts(1, 1);

    source.record_events_produced(50, 1_000);
    derived.record_events_produced(25, 2_000);

    let original = collector
        .collect(
            &runtime,
            &publication,
            &recording,
            &[SourceMetricsRef {
                stream_id: "source_a",
                generation: 1,
                metrics: &source,
            }],
            &[DerivedMetricsRef {
                stream_id: "derived_a",
                generation: 1,
                metrics: &derived,
            }],
        )
        .unwrap();

    let serialized = serde_json::to_string(&*original).unwrap();

    let restored: MetricsSnapshot = serde_json::from_str(&serialized).unwrap();

    assert_eq!(*original, restored);

    // Verify the expected top-level JSON sections.
    let json = serde_json::to_value(&restored).unwrap();

    assert!(json.get("runtime").is_some());
    assert!(json.get("throughput").is_some());
    assert!(json.get("sources").is_some());
    assert!(json.get("derived").is_some());
    assert!(json.get("publication").is_some());
    assert!(json.get("recording").is_some());

    // Integrity remains a separate subsystem.
    assert!(json.get("integrity").is_none());
}
