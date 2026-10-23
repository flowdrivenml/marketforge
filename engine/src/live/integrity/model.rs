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
