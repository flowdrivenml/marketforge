use crate::live::integrity::model::{
    GapHistoryCoverage, GapHistorySnapshot, GapReason, GapRecord, GapWindowStats, GapWindows,
    IntegrityWindow, StreamIntegritySnapshot,
};
use crate::live::integrity::validation;
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

#[cfg(test)]
mod tests {
    use crate::live::integrity::{GapReason, GapTracker, IntegrityWindow};

    // ============================================================
    // CONSTANTS
    // ============================================================

    const NS: i64 = 1;
    const SECOND: i64 = 1_000_000_000;
    const MINUTE: i64 = 60 * SECOND;
    const HOUR: i64 = 60 * MINUTE;

    const SIX_HOURS: i64 = 6 * HOUR;

    // ============================================================
    // HELPERS
    // ============================================================

    fn tracker(max_records: usize) -> GapTracker {
        GapTracker::new("binance_btcusdt_trades", 1, 0, max_records).unwrap()
    }

    fn record_exact(tracker: &mut GapTracker, timestamp: i64, missing: u64) -> u64 {
        tracker
            .record_gap(
                timestamp,
                GapReason::SequenceGap,
                Some(missing),
                Some(100),
                Some(200),
            )
            .unwrap()
    }

    fn record_unknown(tracker: &mut GapTracker, timestamp: i64) -> u64 {
        tracker
            .record_gap(
                timestamp,
                GapReason::ConnectionInterruption,
                None,
                None,
                None,
            )
            .unwrap()
    }

    // ============================================================
    // CONSTRUCTION
    // ============================================================

    #[test]
    fn new_tracker_is_empty() {
        let tracker = tracker(100);

        assert!(tracker.is_empty());
        assert_eq!(tracker.len(), 0);

        assert_eq!(tracker.stream_id(), "binance_btcusdt_trades");
        assert_eq!(tracker.generation(), 1);

        assert_eq!(tracker.history_started_at_ns(), 0);
        assert_eq!(tracker.last_gap_at_ns(), None);

        assert_eq!(tracker.max_records(), 100);
    }

    #[test]
    fn new_tracker_accepts_different_generations() {
        let first = GapTracker::new("stream_a", 1, 0, 100).unwrap();
        let second = GapTracker::new("stream_a", 2, 0, 100).unwrap();

        assert_eq!(first.generation(), 1);
        assert_eq!(second.generation(), 2);
    }

    #[test]
    fn new_generation_has_independent_history() {
        let mut first = GapTracker::new("stream_a", 1, 0, 100).unwrap();

        record_exact(&mut first, 10 * SECOND, 5);

        let second = GapTracker::new("stream_a", 2, 20 * SECOND, 100).unwrap();

        assert_eq!(first.len(), 1);
        assert_eq!(second.len(), 0);

        assert_eq!(first.last_gap_at_ns(), Some(10 * SECOND));
        assert_eq!(second.last_gap_at_ns(), None);
    }

    // ============================================================
    // RECORDING
    // ============================================================

    #[test]
    fn recording_gap_increases_length() {
        let mut tracker = tracker(100);

        record_exact(&mut tracker, 10 * SECOND, 5);

        assert_eq!(tracker.len(), 1);
        assert!(!tracker.is_empty());
    }

    #[test]
    fn gap_ids_are_sequential() {
        let mut tracker = tracker(100);

        let first = record_exact(&mut tracker, 10 * SECOND, 5);
        let second = record_exact(&mut tracker, 20 * SECOND, 10);
        let third = record_unknown(&mut tracker, 30 * SECOND);

        assert_eq!(first, 1);
        assert_eq!(second, 2);
        assert_eq!(third, 3);
    }

    #[test]
    fn gap_ids_remain_sequential_after_eviction() {
        let mut tracker = tracker(2);

        let first = record_exact(&mut tracker, 10 * SECOND, 5);
        let second = record_exact(&mut tracker, 20 * SECOND, 10);
        let third = record_exact(&mut tracker, 30 * SECOND, 15);

        assert_eq!((first, second, third), (1, 2, 3));

        assert_eq!(tracker.len(), 2);

        let ids: Vec<_> = tracker.records().map(|record| record.gap_id).collect();

        assert_eq!(ids, vec![2, 3]);
    }

    #[test]
    fn last_gap_timestamp_tracks_latest_detection() {
        let mut tracker = tracker(100);

        record_exact(&mut tracker, 10 * SECOND, 5);

        assert_eq!(tracker.last_gap_at_ns(), Some(10 * SECOND));

        record_unknown(&mut tracker, 20 * SECOND);

        assert_eq!(tracker.last_gap_at_ns(), Some(20 * SECOND));
    }

    #[test]
    fn equal_detection_timestamps_are_allowed() {
        let mut tracker = tracker(100);

        record_exact(&mut tracker, 10 * SECOND, 5);
        record_exact(&mut tracker, 10 * SECOND, 10);

        assert_eq!(tracker.len(), 2);

        let snapshot = tracker.snapshot(10 * SECOND).unwrap();

        assert_eq!(snapshot.windows.one_minute.gaps, 2);
    }

    #[test]
    fn different_gap_reasons_are_preserved() {
        let mut tracker = tracker(100);

        tracker
            .record_gap(
                10 * SECOND,
                GapReason::SequenceGap,
                Some(5),
                Some(100),
                Some(106),
            )
            .unwrap();

        tracker
            .record_gap(
                20 * SECOND,
                GapReason::ConnectionInterruption,
                None,
                None,
                None,
            )
            .unwrap();

        tracker
            .record_gap(30 * SECOND, GapReason::DataInterruption, None, None, None)
            .unwrap();

        let reasons: Vec<_> = tracker.records().map(|record| record.reason).collect();

        assert_eq!(
            reasons,
            vec![
                GapReason::SequenceGap,
                GapReason::ConnectionInterruption,
                GapReason::DataInterruption,
            ]
        );
    }

    // ============================================================
    // EXACT AND UNKNOWN COUNTS
    // ============================================================

    #[test]
    fn exact_missing_counts_are_summed() {
        let mut tracker = tracker(100);

        record_exact(&mut tracker, 10 * SECOND, 5);
        record_exact(&mut tracker, 20 * SECOND, 12);
        record_exact(&mut tracker, 30 * SECOND, 8);

        let snapshot = tracker.snapshot(40 * SECOND).unwrap();

        assert_eq!(snapshot.windows.one_minute.gaps, 3);

        assert_eq!(snapshot.windows.one_minute.missing_events_exact, 25);

        assert_eq!(snapshot.windows.one_minute.unknown_missing_gaps, 0);
    }

    #[test]
    fn unknown_missing_counts_are_counted_separately() {
        let mut tracker = tracker(100);

        record_unknown(&mut tracker, 10 * SECOND);
        record_unknown(&mut tracker, 20 * SECOND);

        let snapshot = tracker.snapshot(30 * SECOND).unwrap();

        assert_eq!(snapshot.windows.one_minute.gaps, 2);

        assert_eq!(snapshot.windows.one_minute.missing_events_exact, 0);

        assert_eq!(snapshot.windows.one_minute.unknown_missing_gaps, 2);
    }

    #[test]
    fn exact_and_unknown_counts_can_coexist() {
        let mut tracker = tracker(100);

        record_exact(&mut tracker, 10 * SECOND, 5);
        record_unknown(&mut tracker, 20 * SECOND);
        record_exact(&mut tracker, 30 * SECOND, 12);

        let snapshot = tracker.snapshot(40 * SECOND).unwrap();

        let stats = &snapshot.windows.one_minute;

        assert_eq!(stats.gaps, 3);
        assert_eq!(stats.missing_events_exact, 17);
        assert_eq!(stats.unknown_missing_gaps, 1);
    }

    #[test]
    fn large_exact_counts_do_not_overflow_statistics() {
        let mut tracker = tracker(100);

        record_exact(&mut tracker, 10 * SECOND, u64::MAX);
        record_exact(&mut tracker, 20 * SECOND, 1);

        let snapshot = tracker.snapshot(30 * SECOND).unwrap();

        assert_eq!(snapshot.windows.one_minute.missing_events_exact, u64::MAX);

        // The current implementation uses saturating arithmetic.
        assert_eq!(snapshot.windows.one_minute.gaps, 2);
    }

    // ============================================================
    // ONE-MINUTE WINDOW BOUNDARIES
    // ============================================================

    #[test]
    fn one_minute_includes_exact_lower_boundary() {
        let mut tracker = tracker(100);

        record_exact(&mut tracker, 10 * SECOND, 5);

        let snapshot = tracker.snapshot(10 * SECOND + MINUTE).unwrap();

        assert_eq!(snapshot.windows.one_minute.gaps, 1);
    }

    #[test]
    fn one_minute_excludes_event_one_nanosecond_too_old() {
        let mut tracker = tracker(100);

        record_exact(&mut tracker, 10 * SECOND, 5);

        let snapshot = tracker.snapshot(10 * SECOND + MINUTE + NS).unwrap();

        assert_eq!(snapshot.windows.one_minute.gaps, 0);
    }

    #[test]
    fn one_minute_includes_gap_at_current_timestamp() {
        let mut tracker = tracker(100);

        record_exact(&mut tracker, 10 * SECOND, 5);

        let snapshot = tracker.snapshot(10 * SECOND).unwrap();

        assert_eq!(snapshot.windows.one_minute.gaps, 1);
    }

    // ============================================================
    // FIVE-MINUTE WINDOW BOUNDARIES
    // ============================================================

    #[test]
    fn five_minutes_includes_exact_lower_boundary() {
        let mut tracker = tracker(100);

        record_exact(&mut tracker, 10 * SECOND, 5);

        let snapshot = tracker.snapshot(10 * SECOND + 5 * MINUTE).unwrap();

        assert_eq!(snapshot.windows.five_minutes.gaps, 1);
    }

    #[test]
    fn five_minutes_excludes_event_one_nanosecond_too_old() {
        let mut tracker = tracker(100);

        record_exact(&mut tracker, 10 * SECOND, 5);

        let snapshot = tracker.snapshot(10 * SECOND + 5 * MINUTE + NS).unwrap();

        assert_eq!(snapshot.windows.five_minutes.gaps, 0);
    }

    // ============================================================
    // THIRTY-MINUTE WINDOW BOUNDARIES
    // ============================================================

    #[test]
    fn thirty_minutes_includes_exact_lower_boundary() {
        let mut tracker = tracker(100);

        record_exact(&mut tracker, 10 * SECOND, 5);

        let snapshot = tracker.snapshot(10 * SECOND + 30 * MINUTE).unwrap();

        assert_eq!(snapshot.windows.thirty_minutes.gaps, 1);
    }

    #[test]
    fn thirty_minutes_excludes_event_one_nanosecond_too_old() {
        let mut tracker = tracker(100);

        record_exact(&mut tracker, 10 * SECOND, 5);

        let snapshot = tracker.snapshot(10 * SECOND + 30 * MINUTE + NS).unwrap();

        assert_eq!(snapshot.windows.thirty_minutes.gaps, 0);
    }

    // ============================================================
    // SIX-HOUR WINDOW BOUNDARIES
    // ============================================================

    #[test]
    fn six_hours_includes_exact_lower_boundary() {
        let mut tracker = tracker(100);

        record_exact(&mut tracker, 10 * SECOND, 5);

        let snapshot = tracker.snapshot(10 * SECOND + SIX_HOURS).unwrap();

        assert_eq!(snapshot.windows.six_hours.gaps, 1);
    }

    #[test]
    fn six_hours_excludes_event_one_nanosecond_too_old() {
        let mut tracker = tracker(100);

        record_exact(&mut tracker, 10 * SECOND, 5);

        let snapshot = tracker.snapshot(10 * SECOND + SIX_HOURS + NS).unwrap();

        assert_eq!(snapshot.windows.six_hours.gaps, 0);
    }

    // ============================================================
    // MULTIPLE ROLLING WINDOWS
    // ============================================================

    #[test]
    fn windows_include_correct_subsets() {
        let mut tracker = tracker(100);

        // At now = 6 hours:
        //
        // 1m:  5h 59m
        // 5m:  5h 55m
        // 30m: 5h 30m
        // 6h:  0h

        record_exact(&mut tracker, 1 * HOUR, 5);
        record_exact(&mut tracker, 5 * HOUR + 40 * MINUTE, 10);
        record_exact(&mut tracker, 5 * HOUR + 57 * MINUTE, 15);
        record_unknown(&mut tracker, 5 * HOUR + 59 * MINUTE);

        let snapshot = tracker.snapshot(6 * HOUR).unwrap();

        assert_eq!(snapshot.windows.one_minute.gaps, 1);
        assert_eq!(snapshot.windows.five_minutes.gaps, 2);
        assert_eq!(snapshot.windows.thirty_minutes.gaps, 3);
        assert_eq!(snapshot.windows.six_hours.gaps, 4);

        assert_eq!(snapshot.windows.one_minute.missing_events_exact, 0);

        assert_eq!(snapshot.windows.five_minutes.missing_events_exact, 15);

        assert_eq!(snapshot.windows.thirty_minutes.missing_events_exact, 25);

        assert_eq!(snapshot.windows.six_hours.missing_events_exact, 30);
    }

    // ============================================================
    // SIX-HOUR EXPIRATION
    // ============================================================

    #[test]
    fn recording_new_gap_prunes_expired_records() {
        let mut tracker = tracker(100);

        record_exact(&mut tracker, 10 * SECOND, 5);
        record_exact(&mut tracker, 20 * SECOND, 10);

        assert_eq!(tracker.len(), 2);

        record_exact(&mut tracker, 10 * SECOND + SIX_HOURS + NS, 15);

        assert_eq!(tracker.len(), 2);

        let ids: Vec<_> = tracker.records().map(|record| record.gap_id).collect();

        assert_eq!(ids, vec![2, 3]);
    }

    #[test]
    fn expiration_does_not_reset_gap_ids() {
        let mut tracker = tracker(100);

        record_exact(&mut tracker, 10 * SECOND, 5);

        let id = record_exact(&mut tracker, SIX_HOURS + 20 * SECOND, 10);

        assert_eq!(id, 2);
        assert_eq!(tracker.len(), 1);
    }

    #[test]
    fn expiration_does_not_reset_last_gap_timestamp() {
        let mut tracker = tracker(100);

        record_exact(&mut tracker, 10 * SECOND, 5);

        let snapshot = tracker.snapshot(SIX_HOURS + 20 * SECOND).unwrap();

        assert_eq!(snapshot.windows.six_hours.gaps, 0);

        // Last detected gap remains available even after
        // it falls outside the rolling windows.
        assert_eq!(tracker.last_gap_at_ns(), Some(10 * SECOND));
    }

    // ============================================================
    // CAPACITY EVICTION
    // ============================================================

    #[test]
    fn capacity_one_retains_only_latest_record() {
        let mut tracker = tracker(1);

        record_exact(&mut tracker, 10 * SECOND, 5);
        record_exact(&mut tracker, 20 * SECOND, 10);
        record_exact(&mut tracker, 30 * SECOND, 15);

        assert_eq!(tracker.len(), 1);

        let record = tracker.records().next().unwrap();

        assert_eq!(record.gap_id, 3);
        assert_eq!(record.missing_events_exact, Some(15));
    }

    #[test]
    fn capacity_evicts_oldest_records_first() {
        let mut tracker = tracker(3);

        for index in 1..=5 {
            record_exact(&mut tracker, index * SECOND, index as u64);
        }

        assert_eq!(tracker.len(), 3);

        let ids: Vec<_> = tracker.records().map(|record| record.gap_id).collect();

        assert_eq!(ids, vec![3, 4, 5]);
    }

    #[test]
    fn capacity_never_exceeds_configured_limit() {
        let mut tracker = tracker(5);

        for index in 1..=100 {
            record_exact(&mut tracker, index * SECOND, 1);

            assert!(tracker.len() <= 5);
        }

        assert_eq!(tracker.len(), 5);
    }

    #[test]
    fn capacity_eviction_marks_relevant_history_truncated() {
        let mut tracker = tracker(2);

        record_exact(&mut tracker, 10 * SECOND, 5);
        record_exact(&mut tracker, 20 * SECOND, 10);
        record_exact(&mut tracker, 30 * SECOND, 15);

        let snapshot = tracker.snapshot(40 * SECOND).unwrap();

        assert!(snapshot.coverage.history_truncated);

        assert_eq!(snapshot.windows.one_minute.gaps, 2);
        assert_eq!(snapshot.windows.one_minute.missing_events_exact, 25);
    }

    #[test]
    fn capacity_eviction_does_not_affect_shorter_unrelated_window() {
        let mut tracker = tracker(2);

        record_exact(&mut tracker, 10 * SECOND, 5);
        record_exact(&mut tracker, 20 * SECOND, 10);
        record_exact(&mut tracker, 30 * SECOND, 15);

        let history = tracker
            .history(IntegrityWindow::OneMinute, 2 * MINUTE)
            .unwrap();

        // The evicted record at 10s lies outside [60s, 120s].
        assert!(!history.coverage.history_truncated);

        assert!(history.gaps.is_empty());
    }

    #[test]
    fn longer_window_remains_truncated_after_shorter_window_recovers() {
        let mut tracker = tracker(2);

        record_exact(&mut tracker, 10 * SECOND, 5);
        record_exact(&mut tracker, 20 * SECOND, 10);
        record_exact(&mut tracker, 30 * SECOND, 15);

        let one_minute = tracker
            .history(IntegrityWindow::OneMinute, 2 * MINUTE)
            .unwrap();

        let six_hours = tracker
            .history(IntegrityWindow::SixHours, 2 * MINUTE)
            .unwrap();

        assert!(!one_minute.coverage.history_truncated);
        assert!(six_hours.coverage.history_truncated);
    }

    #[test]
    fn expired_capacity_eviction_no_longer_marks_history_truncated() {
        let mut tracker = tracker(2);

        record_exact(&mut tracker, 10 * SECOND, 5);
        record_exact(&mut tracker, 20 * SECOND, 10);
        record_exact(&mut tracker, 30 * SECOND, 15);

        let snapshot = tracker.snapshot(SIX_HOURS + 20 * SECOND).unwrap();

        // The evicted record at 10s is outside the six-hour window.
        assert!(!snapshot.coverage.history_truncated);
    }

    // ============================================================
    // HISTORY QUERIES
    // ============================================================

    #[test]
    fn history_returns_newest_records_first() {
        let mut tracker = tracker(100);

        record_exact(&mut tracker, 10 * SECOND, 5);
        record_exact(&mut tracker, 20 * SECOND, 10);
        record_exact(&mut tracker, 30 * SECOND, 15);

        let history = tracker
            .history(IntegrityWindow::OneMinute, 40 * SECOND)
            .unwrap();

        let ids: Vec<_> = history.gaps.iter().map(|record| record.gap_id).collect();

        assert_eq!(ids, vec![3, 2, 1]);
    }

    #[test]
    fn history_filters_by_requested_window() {
        let mut tracker = tracker(100);

        record_exact(&mut tracker, 10 * SECOND, 5);
        record_exact(&mut tracker, 5 * MINUTE, 10);
        record_exact(&mut tracker, 6 * MINUTE, 15);

        let history = tracker
            .history(IntegrityWindow::OneMinute, 6 * MINUTE)
            .unwrap();

        let ids: Vec<_> = history.gaps.iter().map(|record| record.gap_id).collect();

        // Both 5m and 6m lie inside the inclusive one-minute window.
        assert_eq!(ids, vec![3, 2]);
    }

    #[test]
    fn history_excludes_future_records() {
        let mut tracker = tracker(100);

        record_exact(&mut tracker, 10 * SECOND, 5);
        record_exact(&mut tracker, 20 * SECOND, 10);

        let history = tracker
            .history(IntegrityWindow::OneMinute, 15 * SECOND)
            .unwrap();

        assert_eq!(history.gaps.len(), 1);
        assert_eq!(history.gaps[0].gap_id, 1);
    }

    #[test]
    fn history_preserves_sequence_evidence() {
        let mut tracker = tracker(100);

        tracker
            .record_gap(
                10 * SECOND,
                GapReason::SequenceGap,
                Some(5),
                Some(100),
                Some(106),
            )
            .unwrap();

        let history = tracker
            .history(IntegrityWindow::OneMinute, 20 * SECOND)
            .unwrap();

        let record = &history.gaps[0];

        assert_eq!(record.sequence_before, Some(100));
        assert_eq!(record.sequence_after, Some(106));

        assert_eq!(record.missing_events_exact, Some(5));
    }

    // ============================================================
    // INVALID RECORDS
    // ============================================================

    #[test]
    fn reject_timestamp_before_history_start() {
        let mut tracker = GapTracker::new("stream_a", 1, 100 * SECOND, 100).unwrap();

        let result = tracker.record_gap(
            99 * SECOND,
            GapReason::ConnectionInterruption,
            None,
            None,
            None,
        );

        assert!(result.is_err());
        assert!(tracker.is_empty());
    }

    #[test]
    fn reject_decreasing_detection_timestamps() {
        let mut tracker = tracker(100);

        record_exact(&mut tracker, 20 * SECOND, 5);

        let result = tracker.record_gap(
            10 * SECOND,
            GapReason::ConnectionInterruption,
            None,
            None,
            None,
        );

        assert!(result.is_err());

        assert_eq!(tracker.len(), 1);

        assert_eq!(tracker.last_gap_at_ns(), Some(20 * SECOND));
    }

    #[test]
    fn reject_zero_exact_missing_count() {
        let mut tracker = tracker(100);

        let result = tracker.record_gap(
            10 * SECOND,
            GapReason::SequenceGap,
            Some(0),
            Some(100),
            Some(106),
        );

        assert!(result.is_err());
        assert!(tracker.is_empty());
    }

    #[test]
    fn reject_sequence_gap_without_sequence_evidence() {
        let mut tracker = tracker(100);

        let result = tracker.record_gap(10 * SECOND, GapReason::SequenceGap, Some(5), None, None);

        assert!(result.is_err());
        assert!(tracker.is_empty());
    }

    #[test]
    fn invalid_record_does_not_consume_gap_id() {
        let mut tracker = tracker(100);

        let invalid = tracker.record_gap(
            10 * SECOND,
            GapReason::SequenceGap,
            Some(0),
            Some(100),
            Some(106),
        );

        assert!(invalid.is_err());

        let id = record_exact(&mut tracker, 20 * SECOND, 5);

        assert_eq!(id, 1);
    }

    #[test]
    fn invalid_record_does_not_change_history() {
        let mut tracker = tracker(100);

        record_exact(&mut tracker, 10 * SECOND, 5);

        let before = tracker.snapshot(20 * SECOND).unwrap();

        let result = tracker.record_gap(
            5 * SECOND,
            GapReason::ConnectionInterruption,
            None,
            None,
            None,
        );

        assert!(result.is_err());

        let after = tracker.snapshot(20 * SECOND).unwrap();

        assert_eq!(before, after);
    }

    // ============================================================
    // INVALID QUERIES
    // ============================================================

    #[test]
    fn reject_snapshot_before_history_start() {
        let tracker = GapTracker::new("stream_a", 1, 100 * SECOND, 100).unwrap();

        assert!(tracker.snapshot(99 * SECOND).is_err());
    }

    #[test]
    fn reject_history_query_before_history_start() {
        let tracker = GapTracker::new("stream_a", 1, 100 * SECOND, 100).unwrap();

        assert!(
            tracker
                .history(IntegrityWindow::OneMinute, 99 * SECOND,)
                .is_err()
        );
    }

    // ============================================================
    // READ-ONLY BEHAVIOR
    // ============================================================

    #[test]
    fn snapshot_does_not_mutate_history() {
        let mut tracker = tracker(100);

        record_exact(&mut tracker, 10 * SECOND, 5);

        let before: Vec<_> = tracker.records().cloned().collect();

        tracker.snapshot(20 * SECOND).unwrap();
        tracker.snapshot(30 * SECOND).unwrap();

        let after: Vec<_> = tracker.records().cloned().collect();

        assert_eq!(before, after);
    }

    #[test]
    fn history_query_does_not_mutate_tracker() {
        let mut tracker = tracker(100);

        record_exact(&mut tracker, 10 * SECOND, 5);

        let before: Vec<_> = tracker.records().cloned().collect();

        tracker
            .history(IntegrityWindow::SixHours, 20 * SECOND)
            .unwrap();

        let after: Vec<_> = tracker.records().cloned().collect();

        assert_eq!(before, after);
    }

    #[test]
    fn repeated_snapshots_are_deterministic() {
        let mut tracker = tracker(100);

        record_exact(&mut tracker, 10 * SECOND, 5);
        record_unknown(&mut tracker, 20 * SECOND);

        let first = tracker.snapshot(30 * SECOND).unwrap();
        let second = tracker.snapshot(30 * SECOND).unwrap();

        assert_eq!(first, second);
    }

    // ============================================================
    // GENERATION ISOLATION
    // ============================================================

    #[test]
    fn generation_restart_resets_gap_ids() {
        let mut first = GapTracker::new("stream_a", 1, 0, 100).unwrap();

        assert_eq!(record_exact(&mut first, 10 * SECOND, 5), 1);

        assert_eq!(record_exact(&mut first, 20 * SECOND, 10), 2);

        let mut second = GapTracker::new("stream_a", 2, 30 * SECOND, 100).unwrap();

        assert_eq!(record_exact(&mut second, 40 * SECOND, 15), 1);

        assert_eq!(first.generation(), 1);
        assert_eq!(second.generation(), 2);
    }

    // ============================================================
    // LONG-RUNNING BEHAVIOR
    // ============================================================

    #[test]
    fn tracker_remains_bounded_over_many_gaps() {
        let mut tracker = tracker(100);

        for index in 1..=10_000 {
            record_exact(&mut tracker, index * SECOND, 1);
        }

        assert_eq!(tracker.len(), 100);

        assert_eq!(tracker.last_gap_at_ns(), Some(10_000 * SECOND));

        let snapshot = tracker.snapshot(10_000 * SECOND).unwrap();

        assert_eq!(snapshot.windows.six_hours.gaps, 100);
        assert!(snapshot.coverage.history_truncated);
    }

    #[test]
    fn empty_recent_windows_after_long_inactivity() {
        let mut tracker = tracker(100);

        record_exact(&mut tracker, 10 * SECOND, 5);

        let snapshot = tracker.snapshot(12 * HOUR).unwrap();

        assert_eq!(snapshot.windows.one_minute.gaps, 0);
        assert_eq!(snapshot.windows.five_minutes.gaps, 0);
        assert_eq!(snapshot.windows.thirty_minutes.gaps, 0);
        assert_eq!(snapshot.windows.six_hours.gaps, 0);

        // Historical last-gap information remains available.
        assert_eq!(snapshot.last_gap_at_ns, Some(10 * SECOND));
    }
}
