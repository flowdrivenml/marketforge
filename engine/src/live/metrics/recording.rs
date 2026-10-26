use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

// ============================================================
// RECORDING METRICS SNAPSHOT
// ============================================================

/// Read-only operational metrics for PostgreSQL recording.
///
/// Counters are cumulative within the lifetime of this
/// RecordingMetrics instance.
///
/// Timestamps are expressed in nanoseconds.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordingMetricsSnapshot {
    /// Number of canonical records successfully committed
    /// to PostgreSQL.
    ///
    /// This must count confirmed commits, not records merely
    /// submitted to a recording queue.
    pub records_written_total: u64,

    /// Number of failed database write operations.
    ///
    /// This counts failed attempts, not necessarily
    /// distinct records lost.
    pub write_failures_total: u64,

    /// Current number of records awaiting persistence.
    ///
    /// Includes queued and in-flight records that have not
    /// yet reached a confirmed terminal outcome.
    pub pending_records: u64,

    /// Local timestamp of the most recent confirmed
    /// successful database commit.
    ///
    /// None means no successful commit has been observed.
    pub last_successful_write_at_ns: Option<i64>,
}

// ============================================================
// RECORDING METRICS
// ============================================================

/// Lightweight measurements for the PostgreSQL recorder.
///
/// This component owns metrics only.
///
/// It does not:
/// - Own database connections.
/// - Perform database writes.
/// - Own recording queues.
/// - Determine recorder lifecycle status.
/// - Retry failed operations.
/// - Track market-data integrity gaps.
/// - Guarantee that records are persisted.
///
/// The authoritative recording component reports completed
/// operations and current queue occupancy.
#[derive(Debug)]
pub struct RecordingMetrics {
    // --------------------------------------------------------
    // Cumulative counters
    // --------------------------------------------------------
    records_written_total: AtomicU64,

    write_failures_total: AtomicU64,

    // --------------------------------------------------------
    // Outstanding recording work
    // --------------------------------------------------------
    pending_records: AtomicU64,

    // --------------------------------------------------------
    // Last confirmed commit
    // --------------------------------------------------------
    last_successful_write_at_ns: AtomicI64,

    has_successful_write: AtomicBool,
}

// ============================================================
// CONSTRUCTION
// ============================================================

impl Default for RecordingMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl RecordingMetrics {
    /// Create empty recording metrics.
    ///
    /// Counters and pending-record measurements begin at zero.
    ///
    /// No successful commit timestamp is initially available.
    pub fn new() -> Self {
        Self {
            records_written_total: AtomicU64::new(0),

            write_failures_total: AtomicU64::new(0),

            pending_records: AtomicU64::new(0),

            last_successful_write_at_ns: AtomicI64::new(0),

            has_successful_write: AtomicBool::new(false),
        }
    }

    // ========================================================
    // SUCCESSFUL DATABASE COMMITS
    // ========================================================

    /// Record a confirmed successful PostgreSQL commit.
    ///
    /// `count` is the number of canonical records whose
    /// persistence was confirmed by the completed transaction.
    ///
    /// `committed_at_ns` is the local wall-clock timestamp
    /// when commit success was observed.
    ///
    /// A count of zero is a no-op.
    ///
    /// This method does not modify pending_records.
    /// Queue ownership and pending-work accounting remain
    /// with the recording manager.
    pub fn record_successful_commit(&self, count: u64, committed_at_ns: i64) {
        if count == 0 {
            return;
        }

        self.records_written_total
            .fetch_add(count, Ordering::Relaxed);

        self.last_successful_write_at_ns
            .store(committed_at_ns, Ordering::Relaxed);

        self.has_successful_write.store(true, Ordering::Relaxed);
    }

    // ========================================================
    // FAILED DATABASE WRITES
    // ========================================================

    /// Record one failed database write attempt.
    ///
    /// A failed attempt does not necessarily imply that all
    /// associated records were permanently lost.
    ///
    /// The recording manager determines whether retrying is
    /// safe and whether the outcome is uncertain.
    pub fn record_write_failure(&self) {
        self.write_failures_total.fetch_add(1, Ordering::Relaxed);
    }

    // ========================================================
    // PENDING RECORDS
    // ========================================================

    /// Publish the authoritative number of records awaiting
    /// a confirmed terminal recording outcome.
    ///
    /// The value should include:
    /// - Records waiting in the recording queue.
    /// - Records currently being written.
    ///
    /// Successfully committed or definitively discarded
    /// records must no longer be counted as pending.
    ///
    /// This method does not modify the recorder's actual queue.
    pub fn set_pending_records(&self, count: u64) {
        self.pending_records.store(count, Ordering::Relaxed);
    }

    // ========================================================
    // SNAPSHOT
    // ========================================================

    /// Collect a read-only recording metrics snapshot.
    ///
    /// Unlike latency snapshots, recording metrics are
    /// cumulative counters and current gauges.
    ///
    /// Calling this method does not reset any measurements.
    ///
    /// Fields are read independently and do not form a
    /// transactional snapshot under concurrent updates.
    pub fn snapshot(&self) -> RecordingMetricsSnapshot {
        RecordingMetricsSnapshot {
            records_written_total: self.records_written_total(),

            write_failures_total: self.write_failures_total(),

            pending_records: self.pending_records(),

            last_successful_write_at_ns: self.last_successful_write_at_ns(),
        }
    }

    // ========================================================
    // COUNTER INSPECTION
    // ========================================================

    /// Total records confirmed committed to PostgreSQL.
    pub fn records_written_total(&self) -> u64 {
        self.records_written_total.load(Ordering::Relaxed)
    }

    /// Total failed database write attempts.
    pub fn write_failures_total(&self) -> u64 {
        self.write_failures_total.load(Ordering::Relaxed)
    }

    // ========================================================
    // PENDING-WORK INSPECTION
    // ========================================================

    /// Current records awaiting a confirmed terminal outcome.
    pub fn pending_records(&self) -> u64 {
        self.pending_records.load(Ordering::Relaxed)
    }

    // ========================================================
    // COMMIT TIMESTAMP INSPECTION
    // ========================================================

    /// Most recent confirmed successful commit timestamp.
    ///
    /// Returns None until the first confirmed commit.
    pub fn last_successful_write_at_ns(&self) -> Option<i64> {
        if !self.has_successful_write.load(Ordering::Relaxed) {
            return None;
        }

        Some(self.last_successful_write_at_ns.load(Ordering::Relaxed))
    }
}

#[cfg(test)]
mod tests {
    use crate::live::metrics::{RecordingMetrics, RecordingMetricsSnapshot};

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
}
