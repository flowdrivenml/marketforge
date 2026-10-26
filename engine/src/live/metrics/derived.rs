use std::{
    sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering},
    time::Duration,
};

use serde::{Deserialize, Serialize};

use super::latency::{LatencySnapshot, LatencyTracker};

// ============================================================
// DERIVED METRICS SNAPSHOT
// ============================================================

/// Read-only operational metrics for one derived stream.
///
/// Counters are cumulative within the lifetime of this
/// DerivedMetrics instance.
///
/// Timestamps and latency durations use nanoseconds.
///
/// Latency statistics represent the measurement interval
/// since the previous snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DerivedMetricsSnapshot {
    // --------------------------------------------------------
    // Input availability
    // --------------------------------------------------------
    pub inputs_total: u64,
    pub inputs_healthy: u64,

    // --------------------------------------------------------
    // Event counters
    // --------------------------------------------------------
    pub events_received_total: u64,
    pub events_produced_total: u64,

    // --------------------------------------------------------
    // Input queue
    // --------------------------------------------------------
    pub input_queue_depth: u64,
    pub input_queue_capacity: u64,
    pub input_overflows_total: u64,

    // --------------------------------------------------------
    // Processing-specific counters
    // --------------------------------------------------------
    pub late_events_total: u64,
    pub conversion_failures_total: u64,

    // --------------------------------------------------------
    // Output timestamp
    // --------------------------------------------------------
    pub last_output_at_ns: Option<i64>,

    // --------------------------------------------------------
    // Latency measurements
    // --------------------------------------------------------
    pub processing_latency: LatencySnapshot,

    /// Only applicable to derived streams that perform
    /// event-time reordering.
    pub buffer_wait_latency: Option<LatencySnapshot>,
}

// ============================================================
// DERIVED METRICS
// ============================================================

/// Lightweight operational measurements for one derived stream.
///
/// This component does not own:
/// - Derived-stream lifecycle state.
/// - Source-stream dependency registration.
/// - Input health authority.
/// - Actual input queue storage.
/// - Merging or currency-conversion logic.
/// - Gap integrity history.
///
/// The derived runtime reports observations from its
/// authoritative processing components.
#[derive(Debug)]
pub struct DerivedMetrics {
    // --------------------------------------------------------
    // Input availability
    // --------------------------------------------------------
    inputs_total: AtomicU64,
    inputs_healthy: AtomicU64,

    // --------------------------------------------------------
    // Event counters
    // --------------------------------------------------------
    events_received_total: AtomicU64,
    events_produced_total: AtomicU64,

    // --------------------------------------------------------
    // Input queue
    // --------------------------------------------------------
    input_queue_depth: AtomicU64,
    input_queue_capacity: AtomicU64,
    input_overflows_total: AtomicU64,

    // --------------------------------------------------------
    // Processing-specific counters
    // --------------------------------------------------------
    late_events_total: AtomicU64,
    conversion_failures_total: AtomicU64,

    // --------------------------------------------------------
    // Last output timestamp
    // --------------------------------------------------------
    last_output_at_ns: AtomicI64,
    has_last_output: AtomicBool,

    // --------------------------------------------------------
    // Latency measurements
    // --------------------------------------------------------
    processing_latency: LatencyTracker,

    buffer_wait_latency: Option<LatencyTracker>,
}

// ============================================================
// CONSTRUCTION
// ============================================================

impl DerivedMetrics {
    /// Create operational metrics for one derived stream.
    ///
    /// `inputs_total` describes the configured number
    /// of input dependencies.
    ///
    /// `input_queue_capacity` is the queue's event-count limit.
    ///
    /// `event_time_ordering` controls whether reorder-wait
    /// measurements are applicable.
    pub fn new(inputs_total: u64, input_queue_capacity: u64, event_time_ordering: bool) -> Self {
        Self {
            inputs_total: AtomicU64::new(inputs_total),
            inputs_healthy: AtomicU64::new(0),

            events_received_total: AtomicU64::new(0),
            events_produced_total: AtomicU64::new(0),

            input_queue_depth: AtomicU64::new(0),
            input_queue_capacity: AtomicU64::new(input_queue_capacity),
            input_overflows_total: AtomicU64::new(0),

            late_events_total: AtomicU64::new(0),
            conversion_failures_total: AtomicU64::new(0),

            last_output_at_ns: AtomicI64::new(0),
            has_last_output: AtomicBool::new(false),

            processing_latency: LatencyTracker::new(),

            buffer_wait_latency: event_time_ordering.then(LatencyTracker::new),
        }
    }

    // ========================================================
    // INPUT AVAILABILITY
    // ========================================================

    /// Publish the latest observed input availability.
    ///
    /// The source dependency manager remains authoritative.
    ///
    /// This method is intended to be called when dependency
    /// health changes or when monitoring state is refreshed.
    pub fn set_input_health(&self, total: u64, healthy: u64) -> Result<(), String> {
        if healthy > total {
            return Err("inputs_healthy must not exceed inputs_total".into());
        }

        self.inputs_total.store(total, Ordering::Relaxed);
        self.inputs_healthy.store(healthy, Ordering::Relaxed);

        Ok(())
    }

    // ========================================================
    // EVENT COUNTERS
    // ========================================================

    /// Record canonical events accepted for processing.
    ///
    /// This should be called after the derived processor
    /// successfully accepts the events into its input path.
    pub fn record_events_received(&self, count: u64) {
        self.events_received_total
            .fetch_add(count, Ordering::Relaxed);
    }

    /// Record successfully produced derived events.
    ///
    /// A merge may produce one output event per accepted
    /// input event, but this is not universally guaranteed.
    pub fn record_events_produced(&self, count: u64, produced_at_ns: i64) {
        if count == 0 {
            return;
        }

        self.events_produced_total
            .fetch_add(count, Ordering::Relaxed);

        self.last_output_at_ns
            .store(produced_at_ns, Ordering::Relaxed);

        self.has_last_output.store(true, Ordering::Relaxed);
    }

    // ========================================================
    // INPUT QUEUE
    // ========================================================

    /// Set the observed number of events awaiting processing.
    ///
    /// The actual queue remains owned by the derived runtime.
    ///
    /// This is an observation, not a queue mutation.
    pub fn set_input_queue_depth(&self, depth: u64) -> Result<(), String> {
        let capacity = self.input_queue_capacity.load(Ordering::Relaxed);

        if depth > capacity {
            return Err("input_queue_depth must not exceed input_queue_capacity".into());
        }

        self.input_queue_depth.store(depth, Ordering::Relaxed);

        Ok(())
    }

    /// Update the observed queue capacity.
    ///
    /// The derived runtime must update its actual queue
    /// independently before publishing the new capacity.
    ///
    /// Unlike publication subscriber queues, derived queue
    /// resizing is not currently a required REST operation.
    pub fn set_input_queue_capacity(&self, capacity: u64) -> Result<(), String> {
        let depth = self.input_queue_depth.load(Ordering::Relaxed);

        if capacity < depth {
            return Err("input_queue_capacity must not be smaller than input_queue_depth".into());
        }

        self.input_queue_capacity.store(capacity, Ordering::Relaxed);

        Ok(())
    }

    /// Record one rejected input-queue admission due to capacity.
    ///
    /// Overload consequences belong to the derived runtime,
    /// which must not silently claim complete input coverage
    /// after dropping a required event.
    pub fn record_input_overflow(&self) {
        self.input_overflows_total.fetch_add(1, Ordering::Relaxed);
    }

    // ========================================================
    // EVENT-TIME MERGING
    // ========================================================

    /// Record an event excluded from the derived output because
    /// it arrived too late for the configured ordering policy.
    ///
    /// The derived runtime is responsible for separately
    /// reporting the resulting integrity discontinuity.
    pub fn record_late_event(&self) {
        self.late_events_total.fetch_add(1, Ordering::Relaxed);
    }

    // ========================================================
    // CURRENCY CONVERSION
    // ========================================================

    /// Record one failed currency-conversion operation.
    ///
    /// This counts conversion failures, not necessarily
    /// the number of affected input events.
    pub fn record_conversion_failure(&self) {
        self.conversion_failures_total
            .fetch_add(1, Ordering::Relaxed);
    }

    // ========================================================
    // PROCESSING LATENCY
    // ========================================================

    /// Record active derived-processing duration.
    ///
    /// This should exclude intentional event-time reorder
    /// waiting whenever that waiting is measured separately.
    pub fn record_processing_latency(&self, duration: Duration) {
        self.processing_latency.record_duration(duration);
    }

    /// Record active processing duration in nanoseconds.
    pub fn record_processing_latency_ns(&self, duration_ns: u64) {
        self.processing_latency.record(duration_ns);
    }

    // ========================================================
    // EVENT-TIME BUFFER WAIT
    // ========================================================

    /// Record how long an event waited for event-time ordering.
    ///
    /// Returns false when the derived stream does not use
    /// event-time ordering.
    pub fn record_buffer_wait_latency(&self, duration: Duration) -> bool {
        let Some(tracker) = &self.buffer_wait_latency else {
            return false;
        };

        tracker.record_duration(duration);

        true
    }

    /// Record buffer waiting time in nanoseconds.
    ///
    /// Returns false when event-time ordering is disabled.
    pub fn record_buffer_wait_latency_ns(&self, duration_ns: u64) -> bool {
        let Some(tracker) = &self.buffer_wait_latency else {
            return false;
        };

        tracker.record(duration_ns);

        true
    }

    // ========================================================
    // SNAPSHOT
    // ========================================================

    /// Collect a read-only operational snapshot.
    ///
    /// Cumulative counters are preserved.
    ///
    /// Latency interval statistics are reset upon collection.
    ///
    /// Independent atomic loads do not form a transactional
    /// snapshot across all metrics fields.
    pub fn take_snapshot(&self) -> DerivedMetricsSnapshot {
        DerivedMetricsSnapshot {
            inputs_total: self.inputs_total.load(Ordering::Relaxed),

            inputs_healthy: self.inputs_healthy.load(Ordering::Relaxed),

            events_received_total: self.events_received_total.load(Ordering::Relaxed),

            events_produced_total: self.events_produced_total.load(Ordering::Relaxed),

            input_queue_depth: self.input_queue_depth.load(Ordering::Relaxed),

            input_queue_capacity: self.input_queue_capacity.load(Ordering::Relaxed),

            input_overflows_total: self.input_overflows_total.load(Ordering::Relaxed),

            late_events_total: self.late_events_total.load(Ordering::Relaxed),

            conversion_failures_total: self.conversion_failures_total.load(Ordering::Relaxed),

            last_output_at_ns: self.last_output_at_ns(),

            processing_latency: self.processing_latency.take_snapshot(),

            buffer_wait_latency: self
                .buffer_wait_latency
                .as_ref()
                .map(LatencyTracker::take_snapshot),
        }
    }

    // ========================================================
    // INSPECTION
    // ========================================================

    pub fn inputs_total(&self) -> u64 {
        self.inputs_total.load(Ordering::Relaxed)
    }

    pub fn inputs_healthy(&self) -> u64 {
        self.inputs_healthy.load(Ordering::Relaxed)
    }

    pub fn events_received_total(&self) -> u64 {
        self.events_received_total.load(Ordering::Relaxed)
    }

    pub fn events_produced_total(&self) -> u64 {
        self.events_produced_total.load(Ordering::Relaxed)
    }

    pub fn input_queue_depth(&self) -> u64 {
        self.input_queue_depth.load(Ordering::Relaxed)
    }

    pub fn input_queue_capacity(&self) -> u64 {
        self.input_queue_capacity.load(Ordering::Relaxed)
    }

    pub fn input_overflows_total(&self) -> u64 {
        self.input_overflows_total.load(Ordering::Relaxed)
    }

    pub fn late_events_total(&self) -> u64 {
        self.late_events_total.load(Ordering::Relaxed)
    }

    pub fn conversion_failures_total(&self) -> u64 {
        self.conversion_failures_total.load(Ordering::Relaxed)
    }

    pub fn last_output_at_ns(&self) -> Option<i64> {
        if !self.has_last_output.load(Ordering::Relaxed) {
            return None;
        }

        Some(self.last_output_at_ns.load(Ordering::Relaxed))
    }

    pub fn last_processing_latency_ns(&self) -> Option<u64> {
        self.processing_latency.last()
    }

    pub fn last_buffer_wait_latency_ns(&self) -> Option<u64> {
        self.buffer_wait_latency
            .as_ref()
            .and_then(LatencyTracker::last)
    }

    pub fn uses_event_time_ordering(&self) -> bool {
        self.buffer_wait_latency.is_some()
    }
}

#[cfg(test)]
mod tests;
