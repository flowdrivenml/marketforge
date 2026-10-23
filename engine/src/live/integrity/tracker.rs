use super::model::{
    GapHistoryCoverage, GapHistorySnapshot, GapReason, GapRecord, GapWindowStats, GapWindows,
    IntegrityWindow, StreamIntegritySnapshot,
};
use super::validation;
use std::collections::VecDeque;

// ============================================================
// CONSTANTS
// ============================================================

const HISTORY_DURATION_NS: i64 = IntegrityWindow::SixHours.duration_ns();

// ============================================================
// GAP TRACKER
// ============================================================

/// Bounded in-memory integrity history for one source stream.
///
/// The tracker:
/// - Records detected acquisition discontinuities.
/// - Retains recent gaps for up to six hours.
/// - Enforces a maximum number of retained records.
/// - Calculates rolling integrity statistics.
/// - Exposes read-only integrity snapshots.
///
/// The tracker does not detect gaps or perform recovery.
///
/// One instance belongs to one stream runtime generation.
#[derive(Debug)]
pub struct GapTracker {
    stream_id: String,
    generation: u64,

    history_started_at_ns: i64,

    max_records: usize,

    next_gap_id: u64,

    last_gap_at_ns: Option<i64>,

    records: VecDeque<GapRecord>,

    /// Most recent record removed because of the capacity limit.
    ///
    /// Used to indicate when the retained history cannot
    /// fully represent the requested monitoring window.
    last_capacity_eviction_at_ns: Option<i64>,
}

// ============================================================
// CONSTRUCTION
// ============================================================

impl GapTracker {
    /// Create an empty gap tracker for one stream generation.
    ///
    /// `history_started_at_ns` must be the local timestamp
    /// at which monitoring began.
    ///
    /// `max_records` is the maximum number of gap records
    /// retained simultaneously.
    pub fn new(
        stream_id: impl Into<String>,
        generation: u64,
        history_started_at_ns: i64,
        max_records: usize,
    ) -> Result<Self, String> {
        let stream_id = stream_id.into();

        validation::validate_tracker(&stream_id, history_started_at_ns, max_records)?;

        Ok(Self {
            stream_id,
            generation,
            history_started_at_ns,
            max_records,
            next_gap_id: 1,
            last_gap_at_ns: None,
            records: VecDeque::new(),
            last_capacity_eviction_at_ns: None,
        })
    }

    // ========================================================
    // GAP RECORDING
    // ========================================================

    /// Record one detected acquisition discontinuity.
    ///
    /// The caller is responsible for:
    /// - Detecting the discontinuity.
    /// - Avoiding duplicate reports of the same gap.
    /// - Supplying exact missing counts only when provable.
    /// - Providing valid exchange-specific sequence evidence.
    ///
    /// Detection timestamps must be nondecreasing.
    ///
    /// Returns the assigned gap ID.
    pub fn record_gap(
        &mut self,
        detected_at_ns: i64,
        reason: GapReason,
        missing_events_exact: Option<u64>,
        sequence_before: Option<u64>,
        sequence_after: Option<u64>,
    ) -> Result<u64, String> {
        // Reserve the next ID without mutating state.
        let next_gap_id = self
            .next_gap_id
            .checked_add(1)
            .ok_or_else(|| "gap ID counter exhausted".to_string())?;

        let gap_id = self.next_gap_id;

        let record = GapRecord {
            gap_id,
            detected_at_ns,
            reason,
            missing_events_exact,
            sequence_before,
            sequence_after,
        };

        // Reject invalid records before updating any state.
        validation::validate_gap(&record, self.history_started_at_ns, self.last_gap_at_ns)?;

        // Commit the validated record.
        self.next_gap_id = next_gap_id;
        self.last_gap_at_ns = Some(detected_at_ns);

        self.records.push_back(record);

        self.prune_expired(detected_at_ns);
        self.enforce_capacity();

        Ok(gap_id)
    }

    // ========================================================
    // INTEGRITY SUMMARY
    // ========================================================

    /// Create a read-only integrity snapshot.
    ///
    /// Rolling statistics are calculated relative to `now_ns`.
    ///
    /// This method does not mutate tracker state.
    pub fn snapshot(&self, now_ns: i64) -> Result<StreamIntegritySnapshot, String> {
        self.validate_query_timestamp(now_ns)?;

        Ok(StreamIntegritySnapshot {
            stream_id: self.stream_id.clone(),
            generation: self.generation,
            last_gap_at_ns: self.last_gap_at_ns,
            windows: self.calculate_windows(now_ns),
            coverage: self.coverage(IntegrityWindow::SixHours, now_ns),
        })
    }

    // ========================================================
    // GAP HISTORY
    // ========================================================

    /// Return gap records within a requested rolling window.
    ///
    /// Records are returned newest first.
    ///
    /// The caller may apply an additional response limit
    /// when exposing this through the REST API.
    pub fn history(
        &self,
        window: IntegrityWindow,
        now_ns: i64,
    ) -> Result<GapHistorySnapshot, String> {
        self.validate_query_timestamp(now_ns)?;

        let cutoff = now_ns.saturating_sub(window.duration_ns());

        let gaps = self
            .records
            .iter()
            .rev()
            .filter(|record| record.detected_at_ns >= cutoff && record.detected_at_ns <= now_ns)
            .cloned()
            .collect();

        Ok(GapHistorySnapshot {
            stream_id: self.stream_id.clone(),
            generation: self.generation,
            window,
            gaps,
            coverage: self.coverage(window, now_ns),
        })
    }

    // ========================================================
    // ROLLING STATISTICS
    // ========================================================

    fn calculate_windows(&self, now_ns: i64) -> GapWindows {
        GapWindows {
            one_minute: self.calculate_window(IntegrityWindow::OneMinute, now_ns),
            five_minutes: self.calculate_window(IntegrityWindow::FiveMinutes, now_ns),
            thirty_minutes: self.calculate_window(IntegrityWindow::ThirtyMinutes, now_ns),
            six_hours: self.calculate_window(IntegrityWindow::SixHours, now_ns),
        }
    }

    fn calculate_window(&self, window: IntegrityWindow, now_ns: i64) -> GapWindowStats {
        let cutoff = now_ns.saturating_sub(window.duration_ns());

        let mut stats = GapWindowStats::default();

        for record in self.records.iter().rev() {
            if record.detected_at_ns > now_ns {
                continue;
            }

            if record.detected_at_ns < cutoff {
                break;
            }

            stats.gaps = stats.gaps.saturating_add(1);

            match record.missing_events_exact {
                Some(count) => {
                    stats.missing_events_exact = stats.missing_events_exact.saturating_add(count);
                }

                None => {
                    stats.unknown_missing_gaps = stats.unknown_missing_gaps.saturating_add(1);
                }
            }
        }

        stats
    }

    // ========================================================
    // HISTORY COVERAGE
    // ========================================================

    fn coverage(&self, window: IntegrityWindow, now_ns: i64) -> GapHistoryCoverage {
        let cutoff = now_ns.saturating_sub(window.duration_ns());

        let history_truncated = self
            .last_capacity_eviction_at_ns
            .is_some_and(|evicted_at| evicted_at >= cutoff && evicted_at <= now_ns);

        GapHistoryCoverage {
            history_started_at_ns: self.history_started_at_ns,
            history_truncated,
        }
    }

    // ========================================================
    // HISTORY EXPIRATION
    // ========================================================

    /// Remove records older than the six-hour retention window.
    ///
    /// This operates only on the integrity incident history.
    /// It does not retain or evict market-data events.
    fn prune_expired(&mut self, now_ns: i64) {
        let cutoff = now_ns.saturating_sub(HISTORY_DURATION_NS);

        while self
            .records
            .front()
            .is_some_and(|record| record.detected_at_ns < cutoff)
        {
            self.records.pop_front();
        }

        // Once an earlier capacity eviction falls outside the
        // maximum supported window, it no longer affects the
        // completeness of any supported rolling window.
        if self
            .last_capacity_eviction_at_ns
            .is_some_and(|timestamp| timestamp < cutoff)
        {
            self.last_capacity_eviction_at_ns = None;
        }
    }

    // ========================================================
    // CAPACITY ENFORCEMENT
    // ========================================================

    /// Enforce the maximum number of retained gap records.
    ///
    /// The oldest records are evicted first.
    ///
    /// These evictions must be visible through coverage metadata
    /// while they affect a supported rolling window.
    fn enforce_capacity(&mut self) {
        while self.records.len() > self.max_records {
            if let Some(evicted) = self.records.pop_front() {
                self.last_capacity_eviction_at_ns = Some(evicted.detected_at_ns);
            }
        }
    }

    // ========================================================
    // TIMESTAMP VALIDATION
    // ========================================================

    fn validate_query_timestamp(&self, now_ns: i64) -> Result<(), String> {
        if now_ns < self.history_started_at_ns {
            return Err("query timestamp precedes history start".into());
        }

        Ok(())
    }

    // ========================================================
    // INSPECTION
    // ========================================================

    pub fn stream_id(&self) -> &str {
        &self.stream_id
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn history_started_at_ns(&self) -> i64 {
        self.history_started_at_ns
    }

    pub fn last_gap_at_ns(&self) -> Option<i64> {
        self.last_gap_at_ns
    }

    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    pub fn max_records(&self) -> usize {
        self.max_records
    }

    pub fn records(&self) -> impl Iterator<Item = &GapRecord> {
        self.records.iter()
    }
}
