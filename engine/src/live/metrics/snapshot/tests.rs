use std::{sync::Arc, thread, time::Duration};

use crate::live::metrics::{
    DerivedMetrics, DerivedMetricsRef, MetricsSnapshotCollector, PublicationMetrics,
    RecordingMetrics, RuntimeMetrics, SourceMetrics, SourceMetricsRef,
};

// ============================================================
// HELPERS
// ============================================================

fn collect(
    collector: &MetricsSnapshotCollector,
    runtime: &RuntimeMetrics,
    publication: &PublicationMetrics,
    recording: &RecordingMetrics,
    sources: &[SourceMetricsRef<'_>],
    derived: &[DerivedMetricsRef<'_>],
) -> std::sync::Arc<crate::live::metrics::MetricsSnapshot> {
    collector
        .collect(runtime, publication, recording, sources, derived)
        .unwrap()
}

fn assert_rate_close(actual: Option<f64>, expected: f64, interval_seconds: f64) {
    let actual = actual.expect("Expected an available rate");

    let tolerance = expected.abs().max(1.0) * 1e-9;

    assert!(
        (actual - expected).abs() <= tolerance,
        "Expected {expected}, got {actual}; interval={interval_seconds}"
    );
}

// ============================================================
// INITIAL COLLECTION
// ============================================================

#[test]
fn first_collection_has_no_throughput_baseline() {
    let collector = MetricsSnapshotCollector::new();

    let runtime = RuntimeMetrics::new();
    let publication = PublicationMetrics::new();
    let recording = RecordingMetrics::new();

    let source = SourceMetrics::new();
    let derived = DerivedMetrics::new(2, 1_000, false);

    source.record_events_produced(100, 1_000);
    derived.record_events_produced(50, 1_000);

    runtime.record_source_events(100);
    runtime.record_derived_events(50);

    let snapshot = collect(
        &collector,
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
    );

    assert_eq!(snapshot.interval_seconds, None);

    assert_eq!(snapshot.throughput.source_events_per_second, None);

    assert_eq!(snapshot.throughput.derived_events_per_second, None);

    assert_eq!(
        snapshot.sources["source_a"].throughput.events_per_second,
        None
    );

    assert_eq!(
        snapshot.derived["derived_a"].throughput.events_per_second,
        None
    );
}

// ============================================================
// THROUGHPUT CALCULATION
// ============================================================

#[test]
fn calculates_source_and_derived_throughput() {
    let collector = MetricsSnapshotCollector::new();

    let runtime = RuntimeMetrics::new();
    let publication = PublicationMetrics::new();
    let recording = RecordingMetrics::new();

    let source = SourceMetrics::new();
    let derived = DerivedMetrics::new(2, 1_000, false);

    let sources = [SourceMetricsRef {
        stream_id: "source_a",
        generation: 1,
        metrics: &source,
    }];

    let derived_streams = [DerivedMetricsRef {
        stream_id: "derived_a",
        generation: 1,
        metrics: &derived,
    }];

    collect(
        &collector,
        &runtime,
        &publication,
        &recording,
        &sources,
        &derived_streams,
    );

    thread::sleep(Duration::from_millis(10));

    source.record_events_produced(1_000, 2_000);
    derived.record_events_produced(500, 2_000);

    runtime.record_source_events(1_000);
    runtime.record_derived_events(500);

    let snapshot = collect(
        &collector,
        &runtime,
        &publication,
        &recording,
        &sources,
        &derived_streams,
    );

    let interval = snapshot.interval_seconds.unwrap();

    assert!(interval > 0.0);

    let expected_source = 1_000.0 / interval;
    let expected_derived = 500.0 / interval;

    assert_rate_close(
        snapshot.throughput.source_events_per_second,
        expected_source,
        interval,
    );

    assert_rate_close(
        snapshot.throughput.derived_events_per_second,
        expected_derived,
        interval,
    );

    assert_rate_close(
        snapshot.sources["source_a"].throughput.events_per_second,
        expected_source,
        interval,
    );

    assert_rate_close(
        snapshot.derived["derived_a"].throughput.events_per_second,
        expected_derived,
        interval,
    );
}

// ============================================================
// STREAM-GENERATION CHANGES
// ============================================================

#[test]
fn generation_change_resets_stream_rate_baseline() {
    let collector = MetricsSnapshotCollector::new();

    let runtime = RuntimeMetrics::new();
    let publication = PublicationMetrics::new();
    let recording = RecordingMetrics::new();

    let old_source = SourceMetrics::new();

    old_source.record_events_produced(10_000, 1_000);

    collect(
        &collector,
        &runtime,
        &publication,
        &recording,
        &[SourceMetricsRef {
            stream_id: "source_a",
            generation: 1,
            metrics: &old_source,
        }],
        &[],
    );

    // Simulate a restarted stream with fresh metrics.
    let new_source = SourceMetrics::new();

    new_source.record_events_produced(25, 2_000);

    let snapshot = collect(
        &collector,
        &runtime,
        &publication,
        &recording,
        &[SourceMetricsRef {
            stream_id: "source_a",
            generation: 2,
            metrics: &new_source,
        }],
        &[],
    );

    assert_eq!(snapshot.sources["source_a"].generation, 2);

    assert_eq!(
        snapshot.sources["source_a"].throughput.events_per_second,
        None
    );
}

// ============================================================
// COUNTER RESETS
// ============================================================

#[test]
fn decreasing_counter_produces_unavailable_rate() {
    let collector = MetricsSnapshotCollector::new();

    let runtime = RuntimeMetrics::new();
    let publication = PublicationMetrics::new();
    let recording = RecordingMetrics::new();

    let old_source = SourceMetrics::new();
    old_source.record_events_produced(1_000, 1_000);

    collect(
        &collector,
        &runtime,
        &publication,
        &recording,
        &[SourceMetricsRef {
            stream_id: "source_a",
            generation: 1,
            metrics: &old_source,
        }],
        &[],
    );

    // Deliberately reuse the same generation to verify
    // counter-reset detection independently.
    let reset_source = SourceMetrics::new();
    reset_source.record_events_produced(10, 2_000);

    let snapshot = collect(
        &collector,
        &runtime,
        &publication,
        &recording,
        &[SourceMetricsRef {
            stream_id: "source_a",
            generation: 1,
            metrics: &reset_source,
        }],
        &[],
    );

    assert_eq!(
        snapshot.sources["source_a"].throughput.events_per_second,
        None
    );
}

// ============================================================
// STREAM REMOVAL
// ============================================================

#[test]
fn removing_stream_removes_its_rate_baseline() {
    let collector = MetricsSnapshotCollector::new();

    let runtime = RuntimeMetrics::new();
    let publication = PublicationMetrics::new();
    let recording = RecordingMetrics::new();

    let source = SourceMetrics::new();

    source.record_events_produced(100, 1_000);

    let source_ref = [SourceMetricsRef {
        stream_id: "source_a",
        generation: 1,
        metrics: &source,
    }];

    collect(
        &collector,
        &runtime,
        &publication,
        &recording,
        &source_ref,
        &[],
    );

    // The authoritative registry removes the stream.
    let removed = collect(&collector, &runtime, &publication, &recording, &[], &[]);

    assert!(!removed.sources.contains_key("source_a"));

    // Re-register the same ID.
    let recreated = collect(
        &collector,
        &runtime,
        &publication,
        &recording,
        &source_ref,
        &[],
    );

    assert_eq!(
        recreated.sources["source_a"].throughput.events_per_second,
        None
    );
}

// ============================================================
// IMMUTABLE SNAPSHOT PUBLICATION
// ============================================================

#[test]
fn previously_published_snapshots_remain_unchanged() {
    let collector = MetricsSnapshotCollector::new();

    let runtime = RuntimeMetrics::new();
    let publication = PublicationMetrics::new();
    let recording = RecordingMetrics::new();

    let first = collect(&collector, &runtime, &publication, &recording, &[], &[]);

    runtime.record_source_events(100);

    let second = collect(&collector, &runtime, &publication, &recording, &[], &[]);

    assert_eq!(first.runtime.source_events_total, 0);
    assert_eq!(second.runtime.source_events_total, 100);

    let latest = collector.latest().unwrap();

    assert!(Arc::ptr_eq(&latest, &second));
    assert!(!Arc::ptr_eq(&first, &second));
}

// ============================================================
// READS DO NOT RESET LATENCY
// ============================================================

#[test]
fn latest_reads_do_not_reset_latency_statistics() {
    let collector = MetricsSnapshotCollector::new();

    let runtime = RuntimeMetrics::new();
    let publication = PublicationMetrics::new();
    let recording = RecordingMetrics::new();

    let source = SourceMetrics::new();

    source.record_processing_latency_ns(100);
    source.record_processing_latency_ns(300);

    let source_ref = [SourceMetricsRef {
        stream_id: "source_a",
        generation: 1,
        metrics: &source,
    }];

    let first = collect(
        &collector,
        &runtime,
        &publication,
        &recording,
        &source_ref,
        &[],
    );

    assert_eq!(
        first.sources["source_a"].metrics.processing_latency.samples,
        2
    );

    for _ in 0..10 {
        let latest = collector.latest().unwrap();

        assert_eq!(
            latest.sources["source_a"]
                .metrics
                .processing_latency
                .samples,
            2
        );
    }

    // Only a new collection resets the interval.
    let second = collect(
        &collector,
        &runtime,
        &publication,
        &recording,
        &source_ref,
        &[],
    );

    assert_eq!(
        second.sources["source_a"]
            .metrics
            .processing_latency
            .samples,
        0
    );

    assert_eq!(
        second.sources["source_a"]
            .metrics
            .processing_latency
            .last_ns,
        Some(300)
    );
}

// ============================================================
// INPUT VALIDATION
// ============================================================

#[test]
fn duplicate_stream_ids_are_rejected() {
    let collector = MetricsSnapshotCollector::new();

    let runtime = RuntimeMetrics::new();
    let publication = PublicationMetrics::new();
    let recording = RecordingMetrics::new();

    let source = SourceMetrics::new();
    let derived = DerivedMetrics::new(1, 100, false);

    let result = collector.collect(
        &runtime,
        &publication,
        &recording,
        &[SourceMetricsRef {
            stream_id: "duplicate",
            generation: 1,
            metrics: &source,
        }],
        &[DerivedMetricsRef {
            stream_id: "duplicate",
            generation: 1,
            metrics: &derived,
        }],
    );

    assert!(result.is_err());

    // Invalid collection must not publish a snapshot.
    assert!(collector.latest().is_none());

    // It must not consume latency measurements either.
    source.record_processing_latency_ns(500);

    let valid = collect(
        &collector,
        &runtime,
        &publication,
        &recording,
        &[SourceMetricsRef {
            stream_id: "source_a",
            generation: 1,
            metrics: &source,
        }],
        &[],
    );

    assert_eq!(
        valid.sources["source_a"].metrics.processing_latency.samples,
        1
    );
}
