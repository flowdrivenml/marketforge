use std::{sync::Mutex, time::Duration};

use serde::{Deserialize, Serialize};

// ============================================================
// LATENCY SNAPSHOT
// ============================================================

/// Read-only latency statistics for one measurement interval.
///
/// All durations are expressed in nanoseconds.
///
/// `last_ns` retains the latest observation across intervals.
///
/// `mean_ns`, `max_ns`, and `samples` describe only the
/// completed measurement interval.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LatencySnapshot {
    pub last_ns: Option<u64>,

    pub mean_ns: Option<u64>,

    pub max_ns: Option<u64>,

    pub samples: u64,
}

// ============================================================
// LATENCY ACCUMULATOR
// ============================================================

#[derive(Debug, Default)]
struct LatencyAccumulator {
    // Most recent observation across all intervals.
    last_ns: Option<u64>,

    // Current interval measurements.
    samples: u64,
    total_ns: u128,
    max_ns: Option<u64>,
}

impl LatencyAccumulator {
    // ========================================================
    // RECORD
    // ========================================================

    fn record(&mut self, duration_ns: u64) {
        self.last_ns = Some(duration_ns);

        self.samples = self.samples.saturating_add(1);

        self.total_ns = self.total_ns.saturating_add(u128::from(duration_ns));

        self.max_ns = Some(
            self.max_ns
                .map_or(duration_ns, |previous| previous.max(duration_ns)),
        );
    }

    // ========================================================
    // SNAPSHOT
    // ========================================================

    fn take_snapshot(&mut self) -> LatencySnapshot {
        let mean_ns = if self.samples == 0 {
            None
        } else {
            let mean = self.total_ns / u128::from(self.samples);

            Some(mean.min(u128::from(u64::MAX)) as u64)
        };

        let snapshot = LatencySnapshot {
            last_ns: self.last_ns,
            mean_ns,
            max_ns: self.max_ns,
            samples: self.samples,
        };

        // Reset interval measurements only.
        //
        // Keep last_ns so REST clients can inspect the most
        // recently observed latency even when the current
        // interval has no samples.
        self.samples = 0;
        self.total_ns = 0;
        self.max_ns = None;

        snapshot
    }
}

// ============================================================
// LATENCY TRACKER
// ============================================================

/// Thread-safe latency measurements.
///
/// Every observation is committed atomically with respect
/// to snapshot collection.
///
/// A snapshot contains either the entire observation or
/// none of it.
///
/// No observation can contribute its sample count to one
/// interval and its duration to another.
///
/// The tracker does not create background tasks or persist
/// measurement history.
#[derive(Debug, Default)]
pub struct LatencyTracker {
    accumulator: Mutex<LatencyAccumulator>,
}

impl LatencyTracker {
    // ========================================================
    // CONSTRUCTION
    // ========================================================

    pub fn new() -> Self {
        Self {
            accumulator: Mutex::new(LatencyAccumulator::default()),
        }
    }

    // ========================================================
    // RECORDING
    // ========================================================

    /// Record one latency observation in nanoseconds.
    ///
    /// A zero duration is a valid measurement.
    ///
    /// Updates to count, sum, maximum, and last observation
    /// occur under the same lock.
    pub fn record(&self, duration_ns: u64) {
        let mut accumulator = self
            .accumulator
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        accumulator.record(duration_ns);
    }

    /// Record a duration measured using Instant.
    ///
    /// Durations exceeding u64 nanoseconds are saturated
    /// to u64::MAX.
    pub fn record_duration(&self, duration: Duration) {
        let duration_ns = duration.as_nanos().min(u128::from(u64::MAX)) as u64;

        self.record(duration_ns);
    }

    // ========================================================
    // SNAPSHOT COLLECTION
    // ========================================================

    /// Return the completed interval and reset its
    /// aggregate measurements.
    ///
    /// The most recent latency observation is preserved.
    ///
    /// Concurrent record() calls cannot partially overlap
    /// the snapshot boundary.
    ///
    /// Only the periodic metrics collector should call
    /// this method during normal runtime operation.
    pub fn take_snapshot(&self) -> LatencySnapshot {
        let mut accumulator = self
            .accumulator
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        accumulator.take_snapshot()
    }

    // ========================================================
    // INSPECTION
    // ========================================================

    /// Return the latest latency observation without
    /// resetting interval statistics.
    ///
    /// None means no observation has been recorded yet.
    pub fn last(&self) -> Option<u64> {
        let accumulator = self
            .accumulator
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        accumulator.last_ns
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, thread, time::Duration};

    use crate::live::metrics::LatencyTracker;

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
}
