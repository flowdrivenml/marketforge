use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, RwLock},
    time::Instant,
};

use serde::{Deserialize, Serialize};

use super::{
    derived::{DerivedMetrics, DerivedMetricsSnapshot},
    publication::{PublicationMetrics, PublicationMetricsSnapshot},
    recording::{RecordingMetrics, RecordingMetricsSnapshot},
    runtime::{RuntimeMetrics, RuntimeMetricsSnapshot},
    source::{SourceMetrics, SourceMetricsSnapshot},
};

// ============================================================
// THROUGHPUT
// ============================================================

/// Output throughput measured over one collection interval.
///
/// None means a reliable rate is not yet available, for example:
/// - First observation.
/// - Stream generation changed.
/// - Counter decreased.
/// - No positive elapsed measurement interval.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct ThroughputSnapshot {
    pub events_per_second: Option<f64>,
}

// ============================================================
// SOURCE STREAM SNAPSHOT
// ============================================================

/// Operational metrics for one source stream.
///
/// Lifecycle status and integrity information are deliberately
/// excluded because their authoritative owners are elsewhere.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SourceStreamMetricsSnapshot {
    pub generation: u64,

    pub metrics: SourceMetricsSnapshot,

    pub throughput: ThroughputSnapshot,
}

// ============================================================
// DERIVED STREAM SNAPSHOT
// ============================================================

/// Operational metrics for one derived stream.
///
/// The generation identifies the runtime instance producing
/// the measurements.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DerivedStreamMetricsSnapshot {
    pub generation: u64,

    pub metrics: DerivedMetricsSnapshot,

    pub throughput: ThroughputSnapshot,
}

// ============================================================
// GLOBAL THROUGHPUT
// ============================================================

/// Aggregate canonical output throughput.
///
/// Source and derived events are reported separately.
///
/// Derived events are not additional exchange acquisitions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GlobalThroughputSnapshot {
    pub source_events_per_second: Option<f64>,

    pub derived_events_per_second: Option<f64>,
}

// ============================================================
// COMPLETE METRICS SNAPSHOT
// ============================================================

/// Immutable operational metrics snapshot.
///
/// REST, Ratatui, and Prometheus can all consume this snapshot
/// without resetting counters or latency intervals.
///
/// Gap integrity is intentionally excluded.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    /// Elapsed monotonic time since the preceding collection.
    ///
    /// None for the initial snapshot.
    pub interval_seconds: Option<f64>,

    pub runtime: RuntimeMetricsSnapshot,

    pub throughput: GlobalThroughputSnapshot,

    pub sources: BTreeMap<String, SourceStreamMetricsSnapshot>,

    pub derived: BTreeMap<String, DerivedStreamMetricsSnapshot>,

    pub publication: PublicationMetricsSnapshot,

    pub recording: RecordingMetricsSnapshot,
}

// ============================================================
// STREAM METRICS REFERENCES
// ============================================================

/// Borrowed source-stream measurements supplied by the
/// authoritative runtime registry.
#[derive(Debug, Clone, Copy)]
pub struct SourceMetricsRef<'a> {
    pub stream_id: &'a str,

    pub generation: u64,

    pub metrics: &'a SourceMetrics,
}

/// Borrowed derived-stream measurements supplied by the
/// authoritative runtime registry.
#[derive(Debug, Clone, Copy)]
pub struct DerivedMetricsRef<'a> {
    pub stream_id: &'a str,

    pub generation: u64,

    pub metrics: &'a DerivedMetrics,
}

// ============================================================
// INTERNAL RATE BASELINE
// ============================================================

#[derive(Debug, Clone, Copy)]
struct RateBaseline {
    generation: u64,
    total: u64,
}

// ============================================================
// INTERNAL COLLECTOR STATE
// ============================================================

#[derive(Debug, Default)]
struct CollectorState {
    previous_collection_at: Option<Instant>,

    previous_source_total: Option<u64>,

    previous_derived_total: Option<u64>,

    source_baselines: BTreeMap<String, RateBaseline>,

    derived_baselines: BTreeMap<String, RateBaseline>,
}

// ============================================================
// METRICS SNAPSHOT COLLECTOR
// ============================================================

/// Collects periodic operational metrics and publishes
/// immutable snapshots.
///
/// This component does not:
/// - Own runtime components.
/// - Query PostgreSQL.
/// - Collect gap integrity.
/// - Execute stream lifecycle operations.
/// - Poll operating-system resource usage.
/// - Run its own background task.
///
/// The surrounding runtime is responsible for calling
/// collect() at the configured snapshot interval.
#[derive(Debug, Default)]
pub struct MetricsSnapshotCollector {
    state: Mutex<CollectorState>,

    latest: RwLock<Option<Arc<MetricsSnapshot>>>,
}

impl MetricsSnapshotCollector {
    // ========================================================
    // CONSTRUCTION
    // ========================================================

    pub fn new() -> Self {
        Self {
            state: Mutex::new(CollectorState::default()),

            latest: RwLock::new(None),
        }
    }

    // ========================================================
    // COLLECTION
    // ========================================================

    /// Collect and publish a complete metrics snapshot.
    ///
    /// The caller supplies the currently registered streams.
    ///
    /// Collection is serialized because latency trackers
    /// reset their interval measurements when read.
    ///
    /// Rates use actual monotonic elapsed time.
    ///
    /// A stream's first observation, changed generation,
    /// or decreased event counter produces an unavailable
    /// rate rather than an incorrect value.
    pub fn collect(
        &self,
        runtime: &RuntimeMetrics,
        publication: &PublicationMetrics,
        recording: &RecordingMetrics,
        sources: &[SourceMetricsRef<'_>],
        derived: &[DerivedMetricsRef<'_>],
    ) -> Result<Arc<MetricsSnapshot>, String> {
        // Validate stream identities before consuming any
        // latency interval measurements.
        validate_stream_inputs(sources, derived)?;

        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        let now = Instant::now();

        let interval_seconds = state
            .previous_collection_at
            .map(|previous| now.duration_since(previous).as_secs_f64())
            .filter(|seconds| *seconds > 0.0);

        // ----------------------------------------------------
        // Global runtime measurements
        // ----------------------------------------------------

        let runtime_snapshot = runtime.snapshot();

        let source_events_per_second = calculate_rate(
            state.previous_source_total,
            runtime_snapshot.source_events_total,
            interval_seconds,
        );

        let derived_events_per_second = calculate_rate(
            state.previous_derived_total,
            runtime_snapshot.derived_events_total,
            interval_seconds,
        );

        // ----------------------------------------------------
        // Source stream measurements
        // ----------------------------------------------------

        let mut source_snapshots = BTreeMap::new();

        let mut next_source_baselines = BTreeMap::new();

        for source in sources {
            let metrics = source.metrics.take_snapshot();

            let previous = state.source_baselines.get(source.stream_id).copied();

            let events_per_second = calculate_stream_rate(
                previous,
                source.generation,
                metrics.events_produced_total,
                interval_seconds,
            );

            source_snapshots.insert(
                source.stream_id.to_owned(),
                SourceStreamMetricsSnapshot {
                    generation: source.generation,

                    metrics: metrics.clone(),

                    throughput: ThroughputSnapshot { events_per_second },
                },
            );

            next_source_baselines.insert(
                source.stream_id.to_owned(),
                RateBaseline {
                    generation: source.generation,
                    total: metrics.events_produced_total,
                },
            );
        }

        // ----------------------------------------------------
        // Derived stream measurements
        // ----------------------------------------------------

        let mut derived_snapshots = BTreeMap::new();

        let mut next_derived_baselines = BTreeMap::new();

        for stream in derived {
            let metrics = stream.metrics.take_snapshot();

            let previous = state.derived_baselines.get(stream.stream_id).copied();

            let events_per_second = calculate_stream_rate(
                previous,
                stream.generation,
                metrics.events_produced_total,
                interval_seconds,
            );

            derived_snapshots.insert(
                stream.stream_id.to_owned(),
                DerivedStreamMetricsSnapshot {
                    generation: stream.generation,

                    metrics: metrics.clone(),

                    throughput: ThroughputSnapshot { events_per_second },
                },
            );

            next_derived_baselines.insert(
                stream.stream_id.to_owned(),
                RateBaseline {
                    generation: stream.generation,
                    total: metrics.events_produced_total,
                },
            );
        }

        // ----------------------------------------------------
        // Publication and recording
        // ----------------------------------------------------

        let publication_snapshot = publication.take_snapshot();

        let recording_snapshot = recording.snapshot();

        // ----------------------------------------------------
        // Build immutable snapshot
        // ----------------------------------------------------

        let snapshot = Arc::new(MetricsSnapshot {
            interval_seconds,

            runtime: runtime_snapshot.clone(),

            throughput: GlobalThroughputSnapshot {
                source_events_per_second,
                derived_events_per_second,
            },

            sources: source_snapshots,

            derived: derived_snapshots,

            publication: publication_snapshot,

            recording: recording_snapshot,
        });

        // ----------------------------------------------------
        // Commit new rate baselines
        // ----------------------------------------------------

        state.previous_collection_at = Some(now);

        state.previous_source_total = Some(runtime_snapshot.source_events_total);

        state.previous_derived_total = Some(runtime_snapshot.derived_events_total);

        // Replacing these maps removes baselines for streams
        // that no longer exist in the runtime registry.
        state.source_baselines = next_source_baselines;

        state.derived_baselines = next_derived_baselines;

        // ----------------------------------------------------
        // Publish the snapshot
        // ----------------------------------------------------

        {
            let mut latest = self
                .latest
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());

            *latest = Some(Arc::clone(&snapshot));
        }

        Ok(snapshot)
    }

    // ========================================================
    // READ-ONLY ACCESS
    // ========================================================

    /// Return the latest completed metrics snapshot.
    ///
    /// Does not reset any measurement interval.
    ///
    /// Readers share the immutable snapshot through Arc.
    pub fn latest(&self) -> Option<Arc<MetricsSnapshot>> {
        self.latest
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

// ============================================================
// RATE CALCULATION
// ============================================================

/// Calculate events per second from cumulative counters.
///
/// Counter decreases are treated as resets.
///
/// Unavailable or zero-length intervals do not produce rates.
fn calculate_rate(
    previous_total: Option<u64>,
    current_total: u64,
    interval_seconds: Option<f64>,
) -> Option<f64> {
    let previous_total = previous_total?;

    let interval_seconds = interval_seconds?;

    let difference = current_total.checked_sub(previous_total)?;

    Some(difference as f64 / interval_seconds)
}

// ============================================================
// GENERATION-AWARE STREAM RATE
// ============================================================

fn calculate_stream_rate(
    previous: Option<RateBaseline>,
    generation: u64,
    current_total: u64,
    interval_seconds: Option<f64>,
) -> Option<f64> {
    let previous = previous?;

    if previous.generation != generation {
        return None;
    }

    calculate_rate(Some(previous.total), current_total, interval_seconds)
}

// ============================================================
// INPUT VALIDATION
// ============================================================

fn validate_stream_inputs(
    sources: &[SourceMetricsRef<'_>],
    derived: &[DerivedMetricsRef<'_>],
) -> Result<(), String> {
    let mut seen = std::collections::HashSet::new();

    for source in sources {
        if source.stream_id.trim().is_empty() {
            return Err("source metrics stream_id must not be empty".into());
        }

        if !seen.insert(source.stream_id) {
            return Err(format!("duplicate metrics stream_id: {}", source.stream_id));
        }
    }

    for stream in derived {
        if stream.stream_id.trim().is_empty() {
            return Err("derived metrics stream_id must not be empty".into());
        }

        if !seen.insert(stream.stream_id) {
            return Err(format!("duplicate metrics stream_id: {}", stream.stream_id));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests;
