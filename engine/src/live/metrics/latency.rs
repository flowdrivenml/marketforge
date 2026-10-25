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
