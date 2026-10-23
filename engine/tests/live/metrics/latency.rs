use std::{sync::Arc, thread, time::Duration};

use marketforge_engine::live::metrics::LatencyTracker;

// ============================================================
// BASIC MEASUREMENTS
// ============================================================

#[test]
fn empty_tracker() {
    let tracker = LatencyTracker::new();

    assert_eq!(tracker.last(), None);

    let snapshot = tracker.take_snapshot();

    assert_eq!(snapshot.samples, 0);
    assert_eq!(snapshot.last_ns, None);
    assert_eq!(snapshot.mean_ns, None);
    assert_eq!(snapshot.max_ns, None);
}

#[test]
fn records_latency_statistics() {
    let tracker = LatencyTracker::new();

    tracker.record(100);
    tracker.record(200);
    tracker.record(300);

    let snapshot = tracker.take_snapshot();

    assert_eq!(snapshot.samples, 3);
    assert_eq!(snapshot.last_ns, Some(300));
    assert_eq!(snapshot.mean_ns, Some(200));
    assert_eq!(snapshot.max_ns, Some(300));
}

#[test]
fn snapshot_resets_interval_but_preserves_last() {
    let tracker = LatencyTracker::new();

    tracker.record(100);
    tracker.take_snapshot();

    let empty = tracker.take_snapshot();

    assert_eq!(empty.samples, 0);
    assert_eq!(empty.last_ns, Some(100));
    assert_eq!(empty.mean_ns, None);
    assert_eq!(empty.max_ns, None);

    tracker.record(500);

    let next = tracker.take_snapshot();

    assert_eq!(next.samples, 1);
    assert_eq!(next.mean_ns, Some(500));
    assert_eq!(next.max_ns, Some(500));
}

#[test]
fn accepts_zero_and_duration() {
    let tracker = LatencyTracker::new();

    tracker.record(0);
    tracker.record_duration(Duration::from_micros(2));

    let snapshot = tracker.take_snapshot();

    assert_eq!(snapshot.samples, 2);
    assert_eq!(snapshot.last_ns, Some(2_000));
    assert_eq!(snapshot.mean_ns, Some(1_000));
    assert_eq!(snapshot.max_ns, Some(2_000));
}

// ============================================================
// CONCURRENCY
// ============================================================

#[test]
fn concurrent_recording_preserves_all_samples() {
    const THREADS: usize = 4;
    const SAMPLES_PER_THREAD: usize = 1_000;

    let tracker = Arc::new(LatencyTracker::new());

    let handles: Vec<_> = (0..THREADS)
        .map(|_| {
            let tracker = Arc::clone(&tracker);

            thread::spawn(move || {
                for _ in 0..SAMPLES_PER_THREAD {
                    tracker.record(100);
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    let snapshot = tracker.take_snapshot();

    assert_eq!(snapshot.samples, (THREADS * SAMPLES_PER_THREAD) as u64);

    assert_eq!(snapshot.mean_ns, Some(100));
    assert_eq!(snapshot.max_ns, Some(100));
}

#[test]
fn concurrent_snapshots_do_not_split_observations() {
    const THREADS: usize = 4;
    const SAMPLES_PER_THREAD: usize = 1_000;

    let tracker = Arc::new(LatencyTracker::new());

    let writer_handles: Vec<_> = (0..THREADS)
        .map(|_| {
            let tracker = Arc::clone(&tracker);

            thread::spawn(move || {
                for _ in 0..SAMPLES_PER_THREAD {
                    tracker.record(250);
                }
            })
        })
        .collect();

    let mut total_samples = 0_u64;

    // Collect while writers may still be active.
    for _ in 0..10 {
        let snapshot = tracker.take_snapshot();

        total_samples += snapshot.samples;

        if snapshot.samples > 0 {
            assert_eq!(snapshot.mean_ns, Some(250));
            assert_eq!(snapshot.max_ns, Some(250));
        }

        thread::yield_now();
    }

    for handle in writer_handles {
        handle.join().unwrap();
    }

    total_samples += tracker.take_snapshot().samples;

    assert_eq!(total_samples, (THREADS * SAMPLES_PER_THREAD) as u64);
}
