use std::{
    collections::BTreeSet,
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use serde::{Deserialize, Serialize};

use super::latency::{LatencySnapshot, LatencyTracker};

// ============================================================
// GLOBAL PUBLICATION SNAPSHOT
// ============================================================

/// Read-only operational statistics for the publication service.
///
/// Counters are cumulative within the lifetime of this
/// PublicationMetrics instance.
///
/// Latency measurements describe the interval since the
/// previous snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicationMetricsSnapshot {
    pub connections_active: u64,

    pub subscriptions_active: u64,

    /// Successful event-to-connection queue admissions.
    ///
    /// One canonical event queued for three connections
    /// counts as three admissions.
    ///
    /// This does not guarantee consumer receipt.
    pub events_queued_total: u64,

    pub queued_bytes_total: u64,

    pub queue_overflows_total: u64,

    pub connections_disconnected_total: u64,

    pub publication_latency: LatencySnapshot,
}

// ============================================================
// CONNECTION SNAPSHOT
// ============================================================

/// Read-only publication information for one consumer connection.
///
/// This structure is suitable for REST inspection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicationConnectionSnapshot {
    pub connection_id: String,

    pub subscriptions: Vec<String>,

    pub queued_bytes: u64,

    pub queue_limit_bytes: u64,

    pub events_queued_total: u64,

    pub queue_overflows_total: u64,

    pub publication_latency: LatencySnapshot,
}

// ============================================================
// GLOBAL PUBLICATION METRICS
// ============================================================

/// Operational metrics shared by the publication manager.
///
/// This component owns measurements only.
///
/// The publication manager remains authoritative for:
/// - Connection registration and removal.
/// - Subscriber queue storage.
/// - Subscription registration.
/// - Queue admission and overflow handling.
/// - Runtime queue-limit changes.
/// - Consumer disconnection.
///
/// Metrics must be updated after the corresponding operation
/// succeeds or changes the authoritative state.
#[derive(Debug)]
pub struct PublicationMetrics {
    connections_active: AtomicU64,

    subscriptions_active: AtomicU64,

    events_queued_total: AtomicU64,

    queued_bytes_total: AtomicU64,

    queue_overflows_total: AtomicU64,

    connections_disconnected_total: AtomicU64,

    publication_latency: LatencyTracker,
}

impl Default for PublicationMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl PublicationMetrics {
    // ========================================================
    // CONSTRUCTION
    // ========================================================

    pub fn new() -> Self {
        Self {
            connections_active: AtomicU64::new(0),

            subscriptions_active: AtomicU64::new(0),

            events_queued_total: AtomicU64::new(0),

            queued_bytes_total: AtomicU64::new(0),

            queue_overflows_total: AtomicU64::new(0),

            connections_disconnected_total: AtomicU64::new(0),

            publication_latency: LatencyTracker::new(),
        }
    }

    // ========================================================
    // CONNECTIONS
    // ========================================================

    /// Publish the authoritative number of active connections.
    ///
    /// The publication manager must supply this value.
    pub fn set_connections_active(&self, count: u64) {
        self.connections_active.store(count, Ordering::Relaxed);
    }

    /// Record one closed consumer connection.
    ///
    /// This does not modify connections_active.
    ///
    /// The publication manager separately publishes its
    /// authoritative active-connection count.
    pub fn record_connection_disconnected(&self) {
        self.connections_disconnected_total
            .fetch_add(1, Ordering::Relaxed);
    }

    // ========================================================
    // SUBSCRIPTIONS
    // ========================================================

    /// Publish the authoritative count of active subscriptions.
    ///
    /// One subscription means one stream/connection pair.
    ///
    /// A connection subscribed to five streams contributes five.
    pub fn set_subscriptions_active(&self, count: u64) {
        self.subscriptions_active.store(count, Ordering::Relaxed);
    }

    // ========================================================
    // SUCCESSFUL QUEUE ADMISSION
    // ========================================================

    /// Record successful admission of canonical events to
    /// subscriber queues.
    ///
    /// `count` represents event-to-connection admissions,
    /// not unique canonical events.
    ///
    /// This does not update queued_bytes_total because the
    /// publication manager owns actual queue occupancy.
    pub fn record_events_queued(&self, count: u64) {
        if count == 0 {
            return;
        }

        self.events_queued_total.fetch_add(count, Ordering::Relaxed);
    }

    // ========================================================
    // QUEUED BYTES
    // ========================================================

    /// Publish authoritative aggregate queued bytes.
    ///
    /// This includes pending application-level publication
    /// queue bytes across all active consumer connections.
    ///
    /// It does not automatically include kernel socket buffers
    /// or other temporary serialization allocations.
    pub fn set_queued_bytes_total(&self, bytes: u64) {
        self.queued_bytes_total.store(bytes, Ordering::Relaxed);
    }

    // ========================================================
    // QUEUE OVERFLOW
    // ========================================================

    /// Record one publication queue overflow incident.
    ///
    /// The publication manager is responsible for disconnecting
    /// the affected consumer according to the v1 policy.
    pub fn record_queue_overflow(&self) {
        self.queue_overflows_total.fetch_add(1, Ordering::Relaxed);
    }

    // ========================================================
    // PUBLICATION LATENCY
    // ========================================================

    /// Record the elapsed time between successful queue
    /// admission and completion of the associated socket write.
    ///
    /// A completed socket write does not guarantee that the
    /// downstream application processed the event.
    ///
    /// Measure elapsed time using a monotonic clock.
    pub fn record_publication_latency(&self, duration: Duration) {
        self.publication_latency.record_duration(duration);
    }

    pub fn record_publication_latency_ns(&self, duration_ns: u64) {
        self.publication_latency.record(duration_ns);
    }

    // ========================================================
    // SNAPSHOT
    // ========================================================

    /// Collect a read-only operational metrics snapshot.
    ///
    /// Counters remain cumulative.
    ///
    /// Latency interval statistics reset after collection.
    pub fn take_snapshot(&self) -> PublicationMetricsSnapshot {
        PublicationMetricsSnapshot {
            connections_active: self.connections_active.load(Ordering::Relaxed),

            subscriptions_active: self.subscriptions_active.load(Ordering::Relaxed),

            events_queued_total: self.events_queued_total.load(Ordering::Relaxed),

            queued_bytes_total: self.queued_bytes_total.load(Ordering::Relaxed),

            queue_overflows_total: self.queue_overflows_total.load(Ordering::Relaxed),

            connections_disconnected_total: self
                .connections_disconnected_total
                .load(Ordering::Relaxed),

            publication_latency: self.publication_latency.take_snapshot(),
        }
    }

    // ========================================================
    // INSPECTION
    // ========================================================

    pub fn connections_active(&self) -> u64 {
        self.connections_active.load(Ordering::Relaxed)
    }

    pub fn subscriptions_active(&self) -> u64 {
        self.subscriptions_active.load(Ordering::Relaxed)
    }

    pub fn events_queued_total(&self) -> u64 {
        self.events_queued_total.load(Ordering::Relaxed)
    }

    pub fn queued_bytes_total(&self) -> u64 {
        self.queued_bytes_total.load(Ordering::Relaxed)
    }

    pub fn queue_overflows_total(&self) -> u64 {
        self.queue_overflows_total.load(Ordering::Relaxed)
    }

    pub fn connections_disconnected_total(&self) -> u64 {
        self.connections_disconnected_total.load(Ordering::Relaxed)
    }

    pub fn last_publication_latency_ns(&self) -> Option<u64> {
        self.publication_latency.last()
    }
}

// ============================================================
// PER-CONNECTION PUBLICATION METRICS
// ============================================================

/// Operational measurements for one subscriber connection.
///
/// The publication manager owns the actual queue and
/// subscription lifecycle.
///
/// This structure observes those authoritative operations.
///
/// Its queue limit may be updated at runtime without creating
/// a new connection.
#[derive(Debug)]
pub struct PublicationConnectionMetrics {
    connection_id: String,

    // --------------------------------------------------------
    // Subscription inspection
    // --------------------------------------------------------
    subscriptions: Mutex<BTreeSet<String>>,

    // --------------------------------------------------------
    // Queue occupancy and current allowance
    // --------------------------------------------------------
    queued_bytes: AtomicU64,

    queue_limit_bytes: AtomicU64,

    // --------------------------------------------------------
    // Counters
    // --------------------------------------------------------
    events_queued_total: AtomicU64,

    queue_overflows_total: AtomicU64,

    // --------------------------------------------------------
    // Latency
    // --------------------------------------------------------
    publication_latency: LatencyTracker,

    // --------------------------------------------------------
    // Connection lifecycle observation
    // --------------------------------------------------------
    disconnected: AtomicBool,
}

impl PublicationConnectionMetrics {
    // ========================================================
    // CONSTRUCTION
    // ========================================================

    /// Create metrics for one connected consumer.
    ///
    /// `queue_limit_bytes` is the current publication queue
    /// allowance assigned by the publication manager.
    pub fn new(connection_id: impl Into<String>, queue_limit_bytes: u64) -> Result<Self, String> {
        let connection_id = connection_id.into();

        if connection_id.trim().is_empty() {
            return Err("connection_id must not be empty".into());
        }

        if connection_id.chars().any(char::is_whitespace) {
            return Err("connection_id must not contain whitespace".into());
        }

        if queue_limit_bytes == 0 {
            return Err("queue_limit_bytes must be greater than zero".into());
        }

        Ok(Self {
            connection_id,

            subscriptions: Mutex::new(BTreeSet::new()),

            queued_bytes: AtomicU64::new(0),

            queue_limit_bytes: AtomicU64::new(queue_limit_bytes),

            events_queued_total: AtomicU64::new(0),

            queue_overflows_total: AtomicU64::new(0),

            publication_latency: LatencyTracker::new(),

            disconnected: AtomicBool::new(false),
        })
    }

    // ========================================================
    // SUBSCRIPTIONS
    // ========================================================

    /// Update the observed subscription set.
    ///
    /// The publication registry remains authoritative.
    ///
    /// This method should be called after a successful
    /// subscription change.
    pub fn set_subscriptions(&self, streams: impl IntoIterator<Item = String>) {
        let mut subscriptions = self
            .subscriptions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        subscriptions.clear();
        subscriptions.extend(streams);
    }

    // ========================================================
    // QUEUE OCCUPANCY
    // ========================================================

    /// Publish the observed number of application-level
    /// bytes currently pending on this connection.
    ///
    /// This must reflect the actual queue state.
    pub fn set_queued_bytes(&self, bytes: u64) {
        self.queued_bytes.store(bytes, Ordering::Relaxed);
    }

    // ========================================================
    // QUEUE LIMIT
    // ========================================================

    /// Observe a successful runtime queue-limit update.
    ///
    /// This does not resize or reconfigure the actual queue.
    ///
    /// The publication manager must enforce the new limit
    /// and perform any required disconnections.
    pub fn set_queue_limit_bytes(&self, bytes: u64) -> Result<(), String> {
        if bytes == 0 {
            return Err("queue_limit_bytes must be greater than zero".into());
        }

        self.queue_limit_bytes.store(bytes, Ordering::Relaxed);

        Ok(())
    }

    // ========================================================
    // QUEUE ADMISSIONS
    // ========================================================

    /// Record successful event-to-connection queue admission.
    pub fn record_events_queued(&self, count: u64) {
        if count == 0 {
            return;
        }

        self.events_queued_total.fetch_add(count, Ordering::Relaxed);
    }

    // ========================================================
    // QUEUE OVERFLOW
    // ========================================================

    pub fn record_queue_overflow(&self) {
        self.queue_overflows_total.fetch_add(1, Ordering::Relaxed);
    }

    // ========================================================
    // PUBLICATION LATENCY
    // ========================================================

    pub fn record_publication_latency(&self, duration: Duration) {
        self.publication_latency.record_duration(duration);
    }

    pub fn record_publication_latency_ns(&self, duration_ns: u64) {
        self.publication_latency.record(duration_ns);
    }

    // ========================================================
    // DISCONNECTION
    // ========================================================

    /// Mark this connection as disconnected.
    ///
    /// Actual socket closure and registry removal are owned
    /// by the publication manager.
    pub fn mark_disconnected(&self) {
        self.disconnected.store(true, Ordering::Relaxed);
    }

    pub fn is_disconnected(&self) -> bool {
        self.disconnected.load(Ordering::Relaxed)
    }

    // ========================================================
    // SNAPSHOT
    // ========================================================

    /// Collect current connection measurements.
    ///
    /// Latency interval statistics reset after collection.
    ///
    /// This is intended for administrative inspection.
    pub fn take_snapshot(&self) -> PublicationConnectionSnapshot {
        let subscriptions = self
            .subscriptions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .cloned()
            .collect();

        PublicationConnectionSnapshot {
            connection_id: self.connection_id.clone(),

            subscriptions,

            queued_bytes: self.queued_bytes.load(Ordering::Relaxed),

            queue_limit_bytes: self.queue_limit_bytes.load(Ordering::Relaxed),

            events_queued_total: self.events_queued_total.load(Ordering::Relaxed),

            queue_overflows_total: self.queue_overflows_total.load(Ordering::Relaxed),

            publication_latency: self.publication_latency.take_snapshot(),
        }
    }

    // ========================================================
    // INSPECTION
    // ========================================================

    pub fn connection_id(&self) -> &str {
        &self.connection_id
    }

    pub fn queued_bytes(&self) -> u64 {
        self.queued_bytes.load(Ordering::Relaxed)
    }

    pub fn queue_limit_bytes(&self) -> u64 {
        self.queue_limit_bytes.load(Ordering::Relaxed)
    }

    pub fn events_queued_total(&self) -> u64 {
        self.events_queued_total.load(Ordering::Relaxed)
    }

    pub fn queue_overflows_total(&self) -> u64 {
        self.queue_overflows_total.load(Ordering::Relaxed)
    }

    pub fn last_publication_latency_ns(&self) -> Option<u64> {
        self.publication_latency.last()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::live::metrics::{
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

        let restored_global: PublicationMetricsSnapshot =
            serde_json::from_str(&serialized).unwrap();

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
}
