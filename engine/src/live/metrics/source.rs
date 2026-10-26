use std::{
    sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering},
    time::Duration,
};

use serde::{Deserialize, Serialize};

use super::latency::{LatencySnapshot, LatencyTracker};

// ============================================================
// SOURCE METRICS SNAPSHOT
// ============================================================

/// Read-only operational metrics for one source stream.
///
/// Counters are cumulative within the lifetime of this
/// SourceMetrics instance.
///
/// Timestamps and source delay use nanoseconds.
///
/// Processing latency statistics represent the interval
/// since the previous snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceMetricsSnapshot {
    // --------------------------------------------------------
    // Counters
    // --------------------------------------------------------
    pub messages_received_total: u64,

    pub events_produced_total: u64,

    pub normalization_errors_total: u64,

    pub reconnections_total: u64,

    // --------------------------------------------------------
    // Timestamps
    // --------------------------------------------------------
    pub last_message_at_ns: Option<i64>,

    pub last_event_at_ns: Option<i64>,

    // --------------------------------------------------------
    // Source delay
    // --------------------------------------------------------
    /// Most recently observed difference between local
    /// receipt time and exchange event time.
    ///
    /// May be negative because of clock differences.
    pub source_delay_ns: Option<i64>,

    // --------------------------------------------------------
    // Processing latency
    // --------------------------------------------------------
    pub processing_latency: LatencySnapshot,
}

// ============================================================
// SOURCE METRICS
// ============================================================

/// Lightweight operational measurements for one source stream.
///
/// This component owns metrics only.
///
/// It does not own:
/// - Source-stream lifecycle state.
/// - WebSocket connections.
/// - Canonical events.
/// - Sequence validation.
/// - Gap integrity history.
/// - L2 synchronization state.
///
/// The source-stream runtime is responsible for calling these
/// methods at the appropriate processing boundaries.
#[derive(Debug)]
pub struct SourceMetrics {
    // --------------------------------------------------------
    // Counters
    // --------------------------------------------------------
    messages_received_total: AtomicU64,

    events_produced_total: AtomicU64,

    normalization_errors_total: AtomicU64,

    reconnections_total: AtomicU64,

    // --------------------------------------------------------
    // Last message timestamp
    // --------------------------------------------------------
    last_message_at_ns: AtomicI64,

    has_last_message: AtomicBool,

    // --------------------------------------------------------
    // Last canonical event timestamp
    // --------------------------------------------------------
    last_event_at_ns: AtomicI64,

    has_last_event: AtomicBool,

    // --------------------------------------------------------
    // Observed source delay
    // --------------------------------------------------------
    source_delay_ns: AtomicI64,

    has_source_delay: AtomicBool,

    // --------------------------------------------------------
    // Internal processing duration
    // --------------------------------------------------------
    processing_latency: LatencyTracker,
}

// ============================================================
// CONSTRUCTION
// ============================================================

impl Default for SourceMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl SourceMetrics {
    /// Create empty operational metrics for a source stream.
    ///
    /// Counters begin at zero.
    /// Timestamp measurements begin as unavailable.
    pub fn new() -> Self {
        Self {
            messages_received_total: AtomicU64::new(0),

            events_produced_total: AtomicU64::new(0),

            normalization_errors_total: AtomicU64::new(0),

            reconnections_total: AtomicU64::new(0),

            last_message_at_ns: AtomicI64::new(0),

            has_last_message: AtomicBool::new(false),

            last_event_at_ns: AtomicI64::new(0),

            has_last_event: AtomicBool::new(false),

            source_delay_ns: AtomicI64::new(0),

            has_source_delay: AtomicBool::new(false),

            processing_latency: LatencyTracker::new(),
        }
    }

    // ========================================================
    // MESSAGE RECEIPT
    // ========================================================

    /// Record one successfully received native WebSocket message.
    ///
    /// `received_at_ns` is the local wall-clock timestamp
    /// captured when MarketForge received the message.
    ///
    /// This counts native messages, not canonical records.
    pub fn record_message_received(&self, received_at_ns: i64) {
        self.messages_received_total.fetch_add(1, Ordering::Relaxed);

        self.last_message_at_ns
            .store(received_at_ns, Ordering::Relaxed);

        self.has_last_message.store(true, Ordering::Relaxed);
    }

    // ========================================================
    // CANONICAL EVENT PRODUCTION
    // ========================================================

    /// Record successfully produced canonical events.
    ///
    /// One exchange message may produce multiple canonical
    /// events, particularly for batched trades or L2 updates.
    ///
    /// `produced_at_ns` is the local wall-clock timestamp
    /// when the canonical events became available.
    ///
    /// A count of zero is a no-op.
    pub fn record_events_produced(&self, count: u64, produced_at_ns: i64) {
        if count == 0 {
            return;
        }

        self.events_produced_total
            .fetch_add(count, Ordering::Relaxed);

        self.last_event_at_ns
            .store(produced_at_ns, Ordering::Relaxed);

        self.has_last_event.store(true, Ordering::Relaxed);
    }

    // ========================================================
    // NORMALIZATION ERRORS
    // ========================================================

    /// Record one failed normalization operation.
    ///
    /// This should represent an actual processing failure,
    /// not a legitimate source message containing no events.
    pub fn record_normalization_error(&self) {
        self.normalization_errors_total
            .fetch_add(1, Ordering::Relaxed);
    }

    // ========================================================
    // RECONNECTIONS
    // ========================================================

    /// Record one exchange reconnection attempt.
    ///
    /// The initial connection attempt is not a reconnection.
    pub fn record_reconnection(&self) {
        self.reconnections_total.fetch_add(1, Ordering::Relaxed);
    }

    // ========================================================
    // SOURCE DELAY
    // ========================================================

    /// Record the observed source delay.
    ///
    /// Formula:
    ///
    /// local_receive_timestamp_ns
    ///     - exchange_event_timestamp_ns
    ///
    /// This is not necessarily network latency.
    ///
    /// It may include:
    /// - Exchange-side processing time.
    /// - Network delivery time.
    /// - Clock synchronization differences.
    ///
    /// Negative values are valid observations.
    ///
    /// Returns false if subtraction would overflow i64.
    /// In that case, the previous measurement is preserved.
    pub fn record_source_delay(
        &self,
        local_receive_timestamp_ns: i64,
        exchange_event_timestamp_ns: i64,
    ) -> bool {
        let Some(delay_ns) = local_receive_timestamp_ns.checked_sub(exchange_event_timestamp_ns)
        else {
            return false;
        };

        self.source_delay_ns.store(delay_ns, Ordering::Relaxed);

        self.has_source_delay.store(true, Ordering::Relaxed);

        true
    }

    // ========================================================
    // PROCESSING LATENCY
    // ========================================================

    /// Record one completed source-processing duration.
    ///
    /// Measure elapsed time using Instant rather than
    /// subtracting wall-clock timestamps.
    ///
    /// Intended measurement boundary:
    ///
    /// Native message received
    ///     -> decode / parse / normalize
    ///     -> canonical output ready
    pub fn record_processing_latency(&self, duration: Duration) {
        self.processing_latency.record_duration(duration);
    }

    /// Record a processing duration already expressed
    /// in nanoseconds.
    pub fn record_processing_latency_ns(&self, duration_ns: u64) {
        self.processing_latency.record(duration_ns);
    }

    // ========================================================
    // SNAPSHOT
    // ========================================================

    /// Collect a read-only operational metrics snapshot.
    ///
    /// Cumulative counters are not reset.
    ///
    /// Processing latency interval statistics are reset
    /// after collection, while the latest latency observation
    /// remains available.
    ///
    /// Snapshot fields are collected using independent
    /// atomic reads and are not a transactional snapshot.
    pub fn take_snapshot(&self) -> SourceMetricsSnapshot {
        let messages_received_total = self.messages_received_total.load(Ordering::Relaxed);

        let events_produced_total = self.events_produced_total.load(Ordering::Relaxed);

        let normalization_errors_total = self.normalization_errors_total.load(Ordering::Relaxed);

        let reconnections_total = self.reconnections_total.load(Ordering::Relaxed);

        let last_message_at_ns = self.last_message_at_ns();

        let last_event_at_ns = self.last_event_at_ns();

        let source_delay_ns = self.source_delay_ns();

        let processing_latency = self.processing_latency.take_snapshot();

        SourceMetricsSnapshot {
            messages_received_total,
            events_produced_total,
            normalization_errors_total,
            reconnections_total,
            last_message_at_ns,
            last_event_at_ns,
            source_delay_ns,
            processing_latency,
        }
    }

    // ========================================================
    // COUNTER INSPECTION
    // ========================================================

    /// Total native exchange messages received.
    pub fn messages_received_total(&self) -> u64 {
        self.messages_received_total.load(Ordering::Relaxed)
    }

    /// Total canonical events successfully produced.
    pub fn events_produced_total(&self) -> u64 {
        self.events_produced_total.load(Ordering::Relaxed)
    }

    /// Total normalization failures.
    pub fn normalization_errors_total(&self) -> u64 {
        self.normalization_errors_total.load(Ordering::Relaxed)
    }

    /// Total reconnection attempts.
    pub fn reconnections_total(&self) -> u64 {
        self.reconnections_total.load(Ordering::Relaxed)
    }

    // ========================================================
    // TIMESTAMP INSPECTION
    // ========================================================

    /// Most recent local native-message receipt timestamp.
    pub fn last_message_at_ns(&self) -> Option<i64> {
        if !self.has_last_message.load(Ordering::Relaxed) {
            return None;
        }

        Some(self.last_message_at_ns.load(Ordering::Relaxed))
    }

    /// Most recent local canonical-event production timestamp.
    pub fn last_event_at_ns(&self) -> Option<i64> {
        if !self.has_last_event.load(Ordering::Relaxed) {
            return None;
        }

        Some(self.last_event_at_ns.load(Ordering::Relaxed))
    }

    /// Most recently observed signed source delay.
    pub fn source_delay_ns(&self) -> Option<i64> {
        if !self.has_source_delay.load(Ordering::Relaxed) {
            return None;
        }

        Some(self.source_delay_ns.load(Ordering::Relaxed))
    }

    // ========================================================
    // LATENCY INSPECTION
    // ========================================================

    /// Latest recorded internal processing latency.
    ///
    /// Unlike take_snapshot(), this does not reset
    /// interval statistics.
    pub fn last_processing_latency_ns(&self) -> Option<u64> {
        self.processing_latency.last()
    }
}

#[cfg(test)]
mod tests {
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
}
