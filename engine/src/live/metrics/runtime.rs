use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};

use serde::{Deserialize, Serialize};

// ============================================================
// RUNTIME METRICS SNAPSHOT
// ============================================================

/// Read-only operational measurements for MarketForge Live.
///
/// Counters are cumulative within this RuntimeMetrics instance.
///
/// Gauges represent the most recently reported authoritative
/// values and are not guaranteed to describe the exact same
/// instant under concurrent updates.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeMetricsSnapshot {
    // --------------------------------------------------------
    // Process uptime
    // --------------------------------------------------------
    pub uptime_seconds: u64,

    // --------------------------------------------------------
    // Stream counts
    // --------------------------------------------------------
    pub source_streams_running: u64,

    pub derived_streams_running: u64,

    pub streams_degraded: u64,

    pub streams_failed: u64,

    // --------------------------------------------------------
    // Resource utilization
    // --------------------------------------------------------
    pub memory_rss_bytes: Option<u64>,

    /// Process CPU utilization expressed as a percentage.
    ///
    /// 100.0 represents one fully utilized logical CPU.
    ///
    /// A multithreaded process can exceed 100.0.
    pub cpu_usage_percent: Option<f64>,

    // --------------------------------------------------------
    // Event counters
    // --------------------------------------------------------
    pub source_events_total: u64,

    pub derived_events_total: u64,

    // --------------------------------------------------------
    // Internal processing overload
    // --------------------------------------------------------
    pub internal_queue_overflows_total: u64,
}

// ============================================================
// RUNTIME METRICS
// ============================================================

/// Global operational measurements for MarketForge Live.
///
/// This structure does not own:
/// - Runtime lifecycle state.
/// - Source or derived-stream registries.
/// - Actual stream-processing queues.
/// - Process resource-monitoring implementations.
/// - Publication queue state.
/// - Integrity gap history.
///
/// Authoritative components report their state through
/// the methods provided here.
///
/// This structure can be shared across runtime components.
#[derive(Debug)]
pub struct RuntimeMetrics {
    // --------------------------------------------------------
    // Uptime
    // --------------------------------------------------------
    started_at: Instant,

    // --------------------------------------------------------
    // Stream counts
    // --------------------------------------------------------
    source_streams_running: AtomicU64,

    derived_streams_running: AtomicU64,

    streams_degraded: AtomicU64,

    streams_failed: AtomicU64,

    // --------------------------------------------------------
    // Process resource utilization
    // --------------------------------------------------------
    memory_rss_bytes: AtomicU64,

    has_memory_rss: std::sync::atomic::AtomicBool,

    /// CPU utilization stored as raw f64 bits.
    cpu_usage_bits: AtomicU64,

    has_cpu_usage: std::sync::atomic::AtomicBool,

    // --------------------------------------------------------
    // Cumulative canonical event counts
    // --------------------------------------------------------
    source_events_total: AtomicU64,

    derived_events_total: AtomicU64,

    // --------------------------------------------------------
    // Processing overload
    // --------------------------------------------------------
    internal_queue_overflows_total: AtomicU64,
}

// ============================================================
// CONSTRUCTION
// ============================================================

impl Default for RuntimeMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl RuntimeMetrics {
    /// Create empty global runtime metrics.
    ///
    /// Uptime begins when this instance is created.
    ///
    /// All counters and stream-count gauges begin at zero.
    ///
    /// Memory and CPU utilization remain unavailable until
    /// an external resource collector reports measurements.
    pub fn new() -> Self {
        Self {
            started_at: Instant::now(),

            source_streams_running: AtomicU64::new(0),
            derived_streams_running: AtomicU64::new(0),

            streams_degraded: AtomicU64::new(0),
            streams_failed: AtomicU64::new(0),

            memory_rss_bytes: AtomicU64::new(0),
            has_memory_rss: std::sync::atomic::AtomicBool::new(false),

            cpu_usage_bits: AtomicU64::new(0),
            has_cpu_usage: std::sync::atomic::AtomicBool::new(false),

            source_events_total: AtomicU64::new(0),
            derived_events_total: AtomicU64::new(0),

            internal_queue_overflows_total: AtomicU64::new(0),
        }
    }

    // ========================================================
    // STREAM COUNTS
    // ========================================================

    /// Report authoritative running-stream counts.
    ///
    /// The stream registry/runtime remains responsible for
    /// determining actual lifecycle states.
    pub fn set_running_stream_counts(&self, source: u64, derived: u64) {
        self.source_streams_running.store(source, Ordering::Relaxed);

        self.derived_streams_running
            .store(derived, Ordering::Relaxed);
    }

    /// Report authoritative degraded and failed stream counts.
    ///
    /// A degraded stream may remain running.
    ///
    /// These measurements describe different dimensions:
    /// running is lifecycle state, while degraded is health.
    pub fn set_unhealthy_stream_counts(&self, degraded: u64, failed: u64) {
        self.streams_degraded.store(degraded, Ordering::Relaxed);

        self.streams_failed.store(failed, Ordering::Relaxed);
    }

    // ========================================================
    // PROCESS MEMORY
    // ========================================================

    /// Publish the most recently measured process resident
    /// memory usage, expressed in bytes.
    ///
    /// The caller is responsible for retrieving the actual
    /// operating-system measurement.
    ///
    /// This function does not query /proc or other OS APIs.
    pub fn set_memory_rss_bytes(&self, bytes: u64) {
        self.memory_rss_bytes.store(bytes, Ordering::Relaxed);

        self.has_memory_rss.store(true, Ordering::Relaxed);
    }

    // ========================================================
    // PROCESS CPU
    // ========================================================

    /// Publish the most recently measured process CPU usage.
    ///
    /// The value is expressed as a percentage where:
    ///
    /// 100.0 = one fully utilized logical CPU.
    /// 200.0 = two fully utilized logical CPUs.
    ///
    /// This is process utilization, not necessarily the
    /// percentage of total machine capacity.
    ///
    /// Rejects negative, NaN, and infinite values.
    pub fn set_cpu_usage_percent(&self, percent: f64) -> Result<(), String> {
        if !percent.is_finite() {
            return Err("cpu_usage_percent must be finite".into());
        }

        if percent < 0.0 {
            return Err("cpu_usage_percent must not be negative".into());
        }

        self.cpu_usage_bits
            .store(percent.to_bits(), Ordering::Relaxed);

        self.has_cpu_usage.store(true, Ordering::Relaxed);

        Ok(())
    }

    // ========================================================
    // SOURCE EVENTS
    // ========================================================

    /// Record canonical events produced by source streams.
    ///
    /// Events must be counted once at their source output.
    ///
    /// Forwarding the same event to subscribers or derived
    /// processors must not increment this counter again.
    pub fn record_source_events(&self, count: u64) {
        if count == 0 {
            return;
        }

        self.source_events_total.fetch_add(count, Ordering::Relaxed);
    }

    // ========================================================
    // DERIVED EVENTS
    // ========================================================

    /// Record canonical events produced by derived streams.
    ///
    /// These are counted separately from source events to
    /// avoid confusing transformations with new exchange
    /// acquisition.
    pub fn record_derived_events(&self, count: u64) {
        if count == 0 {
            return;
        }

        self.derived_events_total
            .fetch_add(count, Ordering::Relaxed);
    }

    // ========================================================
    // INTERNAL QUEUE OVERFLOW
    // ========================================================

    /// Record one internal processing queue-overflow incident.
    ///
    /// This excludes external publication subscriber queues,
    /// which have their own overflow counters.
    ///
    /// The responsible runtime component must independently
    /// handle the associated processing and integrity outcome.
    pub fn record_internal_queue_overflow(&self) {
        self.internal_queue_overflows_total
            .fetch_add(1, Ordering::Relaxed);
    }

    // ========================================================
    // UPTIME
    // ========================================================

    /// Return elapsed process-monitoring uptime in seconds.
    ///
    /// This uses a monotonic clock, so wall-clock adjustments
    /// do not affect the measurement.
    pub fn uptime_seconds(&self) -> u64 {
        self.started_at.elapsed().as_secs()
    }

    // ========================================================
    // SNAPSHOT
    // ========================================================

    /// Collect current global runtime metrics.
    ///
    /// This method is read-only and does not reset counters.
    ///
    /// Individual atomic measurements are not transactionally
    /// synchronized with each other.
    pub fn snapshot(&self) -> RuntimeMetricsSnapshot {
        RuntimeMetricsSnapshot {
            uptime_seconds: self.uptime_seconds(),

            source_streams_running: self.source_streams_running.load(Ordering::Relaxed),

            derived_streams_running: self.derived_streams_running.load(Ordering::Relaxed),

            streams_degraded: self.streams_degraded.load(Ordering::Relaxed),

            streams_failed: self.streams_failed.load(Ordering::Relaxed),

            memory_rss_bytes: self.memory_rss_bytes(),

            cpu_usage_percent: self.cpu_usage_percent(),

            source_events_total: self.source_events_total(),

            derived_events_total: self.derived_events_total(),

            internal_queue_overflows_total: self.internal_queue_overflows_total(),
        }
    }

    // ========================================================
    // INSPECTION
    // ========================================================

    pub fn memory_rss_bytes(&self) -> Option<u64> {
        if !self.has_memory_rss.load(Ordering::Relaxed) {
            return None;
        }

        Some(self.memory_rss_bytes.load(Ordering::Relaxed))
    }

    pub fn cpu_usage_percent(&self) -> Option<f64> {
        if !self.has_cpu_usage.load(Ordering::Relaxed) {
            return None;
        }

        Some(f64::from_bits(self.cpu_usage_bits.load(Ordering::Relaxed)))
    }

    pub fn source_events_total(&self) -> u64 {
        self.source_events_total.load(Ordering::Relaxed)
    }

    pub fn derived_events_total(&self) -> u64 {
        self.derived_events_total.load(Ordering::Relaxed)
    }

    pub fn internal_queue_overflows_total(&self) -> u64 {
        self.internal_queue_overflows_total.load(Ordering::Relaxed)
    }

    pub fn source_streams_running(&self) -> u64 {
        self.source_streams_running.load(Ordering::Relaxed)
    }

    pub fn derived_streams_running(&self) -> u64 {
        self.derived_streams_running.load(Ordering::Relaxed)
    }

    pub fn streams_degraded(&self) -> u64 {
        self.streams_degraded.load(Ordering::Relaxed)
    }

    pub fn streams_failed(&self) -> u64 {
        self.streams_failed.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use std::{thread, time::Duration};

    use crate::live::metrics::{RuntimeMetrics, RuntimeMetricsSnapshot};

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
}
