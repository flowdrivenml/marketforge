use serde::{Deserialize, Serialize};

// ============================================================
// INTEGRITY WINDOW
// ============================================================

/// Supported rolling integrity monitoring windows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum IntegrityWindow {
    #[serde(rename = "1m")]
    OneMinute,

    #[serde(rename = "5m")]
    FiveMinutes,

    #[serde(rename = "30m")]
    ThirtyMinutes,

    #[serde(rename = "6h")]
    SixHours,
}

impl IntegrityWindow {
    pub const fn duration_ns(self) -> i64 {
        match self {
            Self::OneMinute => 60_000_000_000,
            Self::FiveMinutes => 300_000_000_000,
            Self::ThirtyMinutes => 1_800_000_000_000,
            Self::SixHours => 21_600_000_000_000,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::OneMinute => "1m",
            Self::FiveMinutes => "5m",
            Self::ThirtyMinutes => "30m",
            Self::SixHours => "6h",
        }
    }
}

// ============================================================
// GAP REASON
// ============================================================

/// Why an acquisition discontinuity was reported.
///
/// This describes the detection mechanism, not a recovery outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GapReason {
    SequenceGap,
    ConnectionInterruption,
    DataInterruption,
}

// ============================================================
// GAP RECORD
// ============================================================

/// One detected acquisition discontinuity.
///
/// Records describe the gap as originally detected.
///
/// Subsequent recovery does not modify the recorded
/// missing-event count.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GapRecord {
    /// Unique within one stream runtime generation.
    pub gap_id: u64,

    /// Local detection timestamp, in nanoseconds.
    pub detected_at_ns: i64,

    /// Reason for detecting the discontinuity.
    pub reason: GapReason,

    /// Exact number of missing events, if provable.
    ///
    /// None means the count is unknown.
    ///
    /// Sequence-number differences must not automatically
    /// be interpreted as missing-event counts.
    pub missing_events_exact: Option<u64>,

    /// Last observed sequence before the discontinuity.
    pub sequence_before: Option<u64>,

    /// First observed sequence after the discontinuity.
    pub sequence_after: Option<u64>,
}

// ============================================================
// WINDOW STATISTICS
// ============================================================

/// Aggregated gap statistics for one rolling window.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GapWindowStats {
    /// Number of distinct detected gaps.
    pub gaps: u64,

    /// Sum of exact missing-event counts.
    ///
    /// This describes missing events detected before recovery,
    /// not events that necessarily remain missing.
    pub missing_events_exact: u64,

    /// Number of gaps for which the missing-event count
    /// could not be determined.
    pub unknown_missing_gaps: u64,
}

// ============================================================
// ALL ROLLING WINDOWS
// ============================================================

/// Integrity statistics for all supported time windows.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GapWindows {
    #[serde(rename = "1m")]
    pub one_minute: GapWindowStats,

    #[serde(rename = "5m")]
    pub five_minutes: GapWindowStats,

    #[serde(rename = "30m")]
    pub thirty_minutes: GapWindowStats,

    #[serde(rename = "6h")]
    pub six_hours: GapWindowStats,
}

// ============================================================
// HISTORY COVERAGE
// ============================================================

/// Describes the completeness of the in-memory gap history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GapHistoryCoverage {
    /// Timestamp at which monitoring started for this
    /// stream runtime generation.
    pub history_started_at_ns: i64,

    /// Whether records were removed early because the
    /// configured maximum record count was reached.
    pub history_truncated: bool,
}

// ============================================================
// INTEGRITY SNAPSHOT
// ============================================================

/// Read-only integrity summary for one source stream.
///
/// This is suitable for REST responses and runtime inspection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamIntegritySnapshot {
    /// Source stream identifier.
    pub stream_id: String,

    /// Identifies the current stream runtime generation.
    pub generation: u64,

    /// Most recent detected gap, if any.
    pub last_gap_at_ns: Option<i64>,

    /// Aggregated rolling statistics.
    pub windows: GapWindows,

    /// History availability and truncation information.
    pub coverage: GapHistoryCoverage,
}

// ============================================================
// GAP HISTORY SNAPSHOT
// ============================================================

/// Read-only gap history returned to an API consumer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GapHistorySnapshot {
    pub stream_id: String,

    pub generation: u64,

    /// Requested history window.
    pub window: IntegrityWindow,

    /// Gap records selected for the requested window.
    pub gaps: Vec<GapRecord>,

    /// History availability and truncation information.
    pub coverage: GapHistoryCoverage,
}

#[cfg(test)]
mod tests {
    use crate::live::integrity::{
        GapHistoryCoverage, GapHistorySnapshot, GapReason, GapRecord, GapTracker, GapWindowStats,
        GapWindows, IntegrityWindow, StreamIntegritySnapshot,
    };

    // ============================================================
    // CONSTANTS
    // ============================================================

    const SECOND_NS: i64 = 1_000_000_000;
    const MINUTE_NS: i64 = 60 * SECOND_NS;

    // ============================================================
    // HELPERS
    // ============================================================

    fn tracker() -> GapTracker {
        GapTracker::new("binance_btcusdt_trades", 1, 0, 10_000).unwrap()
    }

    fn sample_gap() -> GapRecord {
        GapRecord {
            gap_id: 1,
            detected_at_ns: 10 * SECOND_NS,
            reason: GapReason::SequenceGap,
            missing_events_exact: Some(5),
            sequence_before: Some(100),
            sequence_after: Some(106),
        }
    }

    // ============================================================
    // INTEGRITY WINDOWS
    // ============================================================

    #[test]
    fn integrity_window_durations_are_nanoseconds() {
        assert_eq!(IntegrityWindow::OneMinute.duration_ns(), 60_000_000_000);

        assert_eq!(IntegrityWindow::FiveMinutes.duration_ns(), 300_000_000_000);

        assert_eq!(
            IntegrityWindow::ThirtyMinutes.duration_ns(),
            1_800_000_000_000
        );

        assert_eq!(IntegrityWindow::SixHours.duration_ns(), 21_600_000_000_000);
    }

    #[test]
    fn integrity_window_names_are_correct() {
        assert_eq!(IntegrityWindow::OneMinute.as_str(), "1m");
        assert_eq!(IntegrityWindow::FiveMinutes.as_str(), "5m");
        assert_eq!(IntegrityWindow::ThirtyMinutes.as_str(), "30m");
        assert_eq!(IntegrityWindow::SixHours.as_str(), "6h");
    }

    #[test]
    fn integrity_windows_serialize_correctly() {
        let cases = [
            (IntegrityWindow::OneMinute, "1m"),
            (IntegrityWindow::FiveMinutes, "5m"),
            (IntegrityWindow::ThirtyMinutes, "30m"),
            (IntegrityWindow::SixHours, "6h"),
        ];

        for (window, expected) in cases {
            let serialized = serde_json::to_value(window).unwrap();

            assert_eq!(serialized, serde_json::json!(expected));
        }
    }

    #[test]
    fn integrity_windows_round_trip() {
        for window in [
            IntegrityWindow::OneMinute,
            IntegrityWindow::FiveMinutes,
            IntegrityWindow::ThirtyMinutes,
            IntegrityWindow::SixHours,
        ] {
            let serialized = serde_json::to_string(&window).unwrap();

            let restored: IntegrityWindow = serde_json::from_str(&serialized).unwrap();

            assert_eq!(restored, window);
        }
    }

    #[test]
    fn reject_unsupported_integrity_window() {
        assert!(serde_json::from_str::<IntegrityWindow>(r#""24h""#).is_err());
    }

    // ============================================================
    // GAP REASONS
    // ============================================================

    #[test]
    fn gap_reasons_serialize_correctly() {
        let cases = [
            (GapReason::SequenceGap, "sequence_gap"),
            (GapReason::ConnectionInterruption, "connection_interruption"),
            (GapReason::DataInterruption, "data_interruption"),
        ];

        for (reason, expected) in cases {
            let serialized = serde_json::to_value(reason).unwrap();

            assert_eq!(serialized, serde_json::json!(expected));
        }
    }

    #[test]
    fn gap_reasons_round_trip() {
        for reason in [
            GapReason::SequenceGap,
            GapReason::ConnectionInterruption,
            GapReason::DataInterruption,
        ] {
            let serialized = serde_json::to_string(&reason).unwrap();

            let restored: GapReason = serde_json::from_str(&serialized).unwrap();

            assert_eq!(restored, reason);
        }
    }

    #[test]
    fn reject_unknown_gap_reason() {
        assert!(serde_json::from_str::<GapReason>(r#""unknown_reason""#).is_err());
    }

    // ============================================================
    // GAP RECORDS
    // ============================================================

    #[test]
    fn gap_record_preserves_exact_missing_count() {
        let gap = sample_gap();

        assert_eq!(gap.gap_id, 1);
        assert_eq!(gap.missing_events_exact, Some(5));
        assert_eq!(gap.sequence_before, Some(100));
        assert_eq!(gap.sequence_after, Some(106));
    }

    #[test]
    fn gap_record_supports_unknown_missing_count() {
        let gap = GapRecord {
            gap_id: 2,
            detected_at_ns: 20 * SECOND_NS,
            reason: GapReason::ConnectionInterruption,
            missing_events_exact: None,
            sequence_before: None,
            sequence_after: None,
        };

        assert_eq!(gap.missing_events_exact, None);
        assert_eq!(gap.sequence_before, None);
        assert_eq!(gap.sequence_after, None);
    }

    #[test]
    fn gap_record_round_trip() {
        let original = sample_gap();

        let serialized = serde_json::to_string(&original).unwrap();

        let restored: GapRecord = serde_json::from_str(&serialized).unwrap();

        assert_eq!(original, restored);
    }

    #[test]
    fn gap_record_null_fields_are_preserved() {
        let original = GapRecord {
            gap_id: 2,
            detected_at_ns: 20 * SECOND_NS,
            reason: GapReason::ConnectionInterruption,
            missing_events_exact: None,
            sequence_before: None,
            sequence_after: None,
        };

        let serialized = serde_json::to_value(&original).unwrap();

        assert!(serialized["missing_events_exact"].is_null());
        assert!(serialized["sequence_before"].is_null());
        assert!(serialized["sequence_after"].is_null());

        let restored: GapRecord = serde_json::from_value(serialized).unwrap();

        assert_eq!(original, restored);
    }

    // ============================================================
    // WINDOW STATISTICS
    // ============================================================

    #[test]
    fn default_window_statistics_are_zero() {
        let stats = GapWindowStats::default();

        assert_eq!(stats.gaps, 0);
        assert_eq!(stats.missing_events_exact, 0);
        assert_eq!(stats.unknown_missing_gaps, 0);
    }

    #[test]
    fn window_statistics_round_trip() {
        let original = GapWindowStats {
            gaps: 3,
            missing_events_exact: 87,
            unknown_missing_gaps: 1,
        };

        let serialized = serde_json::to_string(&original).unwrap();

        let restored: GapWindowStats = serde_json::from_str(&serialized).unwrap();

        assert_eq!(original, restored);
    }

    // ============================================================
    // ALL ROLLING WINDOWS
    // ============================================================

    #[test]
    fn gap_windows_serialize_with_expected_keys() {
        let windows = GapWindows::default();

        let serialized = serde_json::to_value(&windows).unwrap();

        assert!(serialized.get("1m").is_some());
        assert!(serialized.get("5m").is_some());
        assert!(serialized.get("30m").is_some());
        assert!(serialized.get("6h").is_some());

        assert!(serialized.get("one_minute").is_none());
        assert!(serialized.get("five_minutes").is_none());
        assert!(serialized.get("thirty_minutes").is_none());
        assert!(serialized.get("six_hours").is_none());
    }

    #[test]
    fn gap_windows_round_trip() {
        let original = GapWindows {
            one_minute: GapWindowStats {
                gaps: 0,
                missing_events_exact: 0,
                unknown_missing_gaps: 0,
            },
            five_minutes: GapWindowStats {
                gaps: 1,
                missing_events_exact: 12,
                unknown_missing_gaps: 0,
            },
            thirty_minutes: GapWindowStats {
                gaps: 3,
                missing_events_exact: 87,
                unknown_missing_gaps: 1,
            },
            six_hours: GapWindowStats {
                gaps: 9,
                missing_events_exact: 245,
                unknown_missing_gaps: 2,
            },
        };

        let serialized = serde_json::to_string(&original).unwrap();

        let restored: GapWindows = serde_json::from_str(&serialized).unwrap();

        assert_eq!(original, restored);
    }

    // ============================================================
    // HISTORY COVERAGE
    // ============================================================

    #[test]
    fn history_coverage_round_trip() {
        let original = GapHistoryCoverage {
            history_started_at_ns: 1_000_000_000,
            history_truncated: false,
        };

        let serialized = serde_json::to_string(&original).unwrap();

        let restored: GapHistoryCoverage = serde_json::from_str(&serialized).unwrap();

        assert_eq!(original, restored);
    }

    #[test]
    fn history_coverage_supports_truncation() {
        let coverage = GapHistoryCoverage {
            history_started_at_ns: 0,
            history_truncated: true,
        };

        assert!(coverage.history_truncated);
    }

    // ============================================================
    // INTEGRATION: INTEGRITY SNAPSHOT
    // ============================================================

    #[test]
    fn empty_tracker_produces_valid_snapshot() {
        let tracker = tracker();

        let snapshot = tracker.snapshot(10 * SECOND_NS).unwrap();

        assert_eq!(snapshot.stream_id, "binance_btcusdt_trades");

        assert_eq!(snapshot.generation, 1);
        assert_eq!(snapshot.last_gap_at_ns, None);

        assert_eq!(snapshot.windows.one_minute.gaps, 0);
        assert_eq!(snapshot.windows.five_minutes.gaps, 0);
        assert_eq!(snapshot.windows.thirty_minutes.gaps, 0);
        assert_eq!(snapshot.windows.six_hours.gaps, 0);

        assert_eq!(snapshot.coverage.history_started_at_ns, 0);
        assert!(!snapshot.coverage.history_truncated);
    }

    #[test]
    fn tracker_snapshot_aggregates_gap_statistics() {
        let mut tracker = tracker();

        tracker
            .record_gap(
                10 * SECOND_NS,
                GapReason::SequenceGap,
                Some(5),
                Some(100),
                Some(106),
            )
            .unwrap();

        tracker
            .record_gap(
                120 * SECOND_NS,
                GapReason::SequenceGap,
                Some(12),
                Some(500),
                Some(513),
            )
            .unwrap();

        tracker
            .record_gap(
                240 * SECOND_NS,
                GapReason::ConnectionInterruption,
                None,
                None,
                None,
            )
            .unwrap();

        let snapshot = tracker.snapshot(300 * SECOND_NS).unwrap();

        // One-minute window: [240s, 300s].
        assert_eq!(snapshot.windows.one_minute.gaps, 1);
        assert_eq!(snapshot.windows.one_minute.missing_events_exact, 0);
        assert_eq!(snapshot.windows.one_minute.unknown_missing_gaps, 1);

        // Five-minute window: [0s, 300s].
        assert_eq!(snapshot.windows.five_minutes.gaps, 3);
        assert_eq!(snapshot.windows.five_minutes.missing_events_exact, 17);
        assert_eq!(snapshot.windows.five_minutes.unknown_missing_gaps, 1);

        assert_eq!(snapshot.windows.thirty_minutes.gaps, 3);
        assert_eq!(snapshot.windows.six_hours.gaps, 3);

        assert_eq!(snapshot.last_gap_at_ns, Some(240 * SECOND_NS));
    }

    #[test]
    fn integrity_snapshot_round_trip() {
        let mut tracker = tracker();

        tracker
            .record_gap(
                10 * SECOND_NS,
                GapReason::SequenceGap,
                Some(5),
                Some(100),
                Some(106),
            )
            .unwrap();

        let original = tracker.snapshot(20 * SECOND_NS).unwrap();

        let serialized = serde_json::to_string(&original).unwrap();

        let restored: StreamIntegritySnapshot = serde_json::from_str(&serialized).unwrap();

        assert_eq!(original, restored);
    }

    // ============================================================
    // INTEGRATION: GAP HISTORY
    // ============================================================

    #[test]
    fn empty_tracker_produces_empty_history() {
        let tracker = tracker();

        let history = tracker
            .history(IntegrityWindow::OneMinute, 10 * SECOND_NS)
            .unwrap();

        assert!(history.gaps.is_empty());
        assert_eq!(history.window, IntegrityWindow::OneMinute);
        assert_eq!(history.generation, 1);
    }

    #[test]
    fn gap_history_returns_newest_first() {
        let mut tracker = tracker();

        for (timestamp, count) in [
            (10 * SECOND_NS, 5),
            (20 * SECOND_NS, 10),
            (30 * SECOND_NS, 15),
        ] {
            tracker
                .record_gap(
                    timestamp,
                    GapReason::SequenceGap,
                    Some(count),
                    Some(100),
                    Some(200),
                )
                .unwrap();
        }

        let history = tracker
            .history(IntegrityWindow::OneMinute, 40 * SECOND_NS)
            .unwrap();

        let ids: Vec<_> = history.gaps.iter().map(|gap| gap.gap_id).collect();

        assert_eq!(ids, vec![3, 2, 1]);
    }

    #[test]
    fn gap_history_snapshot_round_trip() {
        let mut tracker = tracker();

        tracker
            .record_gap(
                10 * SECOND_NS,
                GapReason::SequenceGap,
                Some(5),
                Some(100),
                Some(106),
            )
            .unwrap();

        let original = tracker
            .history(IntegrityWindow::OneMinute, 20 * SECOND_NS)
            .unwrap();

        let serialized = serde_json::to_string(&original).unwrap();

        let restored: GapHistorySnapshot = serde_json::from_str(&serialized).unwrap();

        assert_eq!(original, restored);
    }

    // ============================================================
    // STREAM GENERATIONS
    // ============================================================

    #[test]
    fn snapshots_preserve_stream_generation() {
        let tracker =
            GapTracker::new("binance_btcusdt_trades", 42, 1_000 * SECOND_NS, 100).unwrap();

        let snapshot = tracker.snapshot(1_010 * SECOND_NS).unwrap();

        assert_eq!(snapshot.generation, 42);
    }

    #[test]
    fn new_generation_starts_with_empty_history() {
        let mut first = GapTracker::new("binance_btcusdt_trades", 1, 0, 100).unwrap();

        first
            .record_gap(
                10 * SECOND_NS,
                GapReason::SequenceGap,
                Some(5),
                Some(100),
                Some(106),
            )
            .unwrap();

        let second = GapTracker::new("binance_btcusdt_trades", 2, 20 * SECOND_NS, 100).unwrap();

        assert_eq!(first.len(), 1);
        assert!(second.is_empty());

        assert_eq!(first.generation(), 1);
        assert_eq!(second.generation(), 2);

        assert_eq!(first.last_gap_at_ns(), Some(10 * SECOND_NS));

        assert_eq!(second.last_gap_at_ns(), None);
    }

    // ============================================================
    // API RESPONSE COMPATIBILITY
    // ============================================================

    #[test]
    fn integrity_snapshot_contains_expected_json_fields() {
        let tracker = tracker();

        let snapshot = tracker.snapshot(MINUTE_NS).unwrap();

        let json = serde_json::to_value(&snapshot).unwrap();

        assert!(json.get("stream_id").is_some());
        assert!(json.get("generation").is_some());
        assert!(json.get("last_gap_at_ns").is_some());
        assert!(json.get("windows").is_some());
        assert!(json.get("coverage").is_some());

        assert!(json["windows"].get("1m").is_some());
        assert!(json["windows"].get("5m").is_some());
        assert!(json["windows"].get("30m").is_some());
        assert!(json["windows"].get("6h").is_some());
    }

    #[test]
    fn history_snapshot_contains_expected_json_fields() {
        let tracker = tracker();

        let history = tracker
            .history(IntegrityWindow::ThirtyMinutes, MINUTE_NS)
            .unwrap();

        let json = serde_json::to_value(&history).unwrap();

        assert_eq!(json["stream_id"], "binance_btcusdt_trades");

        assert_eq!(json["generation"], 1);
        assert_eq!(json["window"], "30m");

        assert!(json["gaps"].is_array());
        assert!(json.get("coverage").is_some());
    }
}
