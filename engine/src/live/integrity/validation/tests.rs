use crate::live::integrity::{GapReason, GapTracker, IntegrityWindow};

// ============================================================
// CONSTANTS
// ============================================================

const SECOND_NS: i64 = 1_000_000_000;

// ============================================================
// HELPERS
// ============================================================

fn tracker() -> GapTracker {
    GapTracker::new("binance_btcusdt_trades", 1, 0, 100).unwrap()
}

fn record_sequence_gap(
    tracker: &mut GapTracker,
    detected_at_ns: i64,
    missing_events_exact: Option<u64>,
    sequence_before: Option<u64>,
    sequence_after: Option<u64>,
) -> Result<u64, String> {
    tracker.record_gap(
        detected_at_ns,
        GapReason::SequenceGap,
        missing_events_exact,
        sequence_before,
        sequence_after,
    )
}

fn assert_invalid(result: Result<u64, String>, expected: &str) {
    let error = result.expect_err("Expected validation to fail");

    assert!(
        error.contains(expected),
        "Expected error containing {expected:?}, got: {error}"
    );
}

// ============================================================
// TRACKER INITIALIZATION
// ============================================================

#[test]
fn accept_valid_tracker() {
    let tracker = GapTracker::new("binance_btcusdt_trades", 1, 0, 100).unwrap();

    assert_eq!(tracker.stream_id(), "binance_btcusdt_trades");
    assert_eq!(tracker.generation(), 1);
    assert_eq!(tracker.history_started_at_ns(), 0);
    assert_eq!(tracker.max_records(), 100);
}

#[test]
fn accept_positive_history_start_timestamp() {
    let tracker = GapTracker::new("stream_a", 1, 1_000_000_000, 100).unwrap();

    assert_eq!(tracker.history_started_at_ns(), 1_000_000_000);
}

#[test]
fn accept_minimum_positive_capacity() {
    let tracker = GapTracker::new("stream_a", 1, 0, 1).unwrap();

    assert_eq!(tracker.max_records(), 1);
}

#[test]
fn accept_large_generation_number() {
    let tracker = GapTracker::new("stream_a", u64::MAX, 0, 100).unwrap();

    assert_eq!(tracker.generation(), u64::MAX);
}

#[test]
fn reject_empty_stream_id() {
    let result = GapTracker::new("", 1, 0, 100);

    assert!(result.is_err());
}

#[test]
fn reject_whitespace_only_stream_id() {
    let result = GapTracker::new("   ", 1, 0, 100);

    assert!(result.is_err());
}

#[test]
fn reject_leading_whitespace_stream_id() {
    let result = GapTracker::new(" stream_a", 1, 0, 100);

    assert!(result.is_err());
}

#[test]
fn reject_trailing_whitespace_stream_id() {
    let result = GapTracker::new("stream_a ", 1, 0, 100);

    assert!(result.is_err());
}

#[test]
fn reject_internal_whitespace_stream_id() {
    let result = GapTracker::new("stream a", 1, 0, 100);

    assert!(result.is_err());
}

#[test]
fn reject_control_characters_stream_id() {
    let result = GapTracker::new("stream\n_a", 1, 0, 100);

    assert!(result.is_err());
}

#[test]
fn reject_negative_history_start_timestamp() {
    let result = GapTracker::new("stream_a", 1, -1, 100);

    assert!(result.is_err());
}

#[test]
fn reject_zero_capacity() {
    let result = GapTracker::new("stream_a", 1, 0, 0);

    assert!(result.is_err());
}

// ============================================================
// VALID GAP RECORDS
// ============================================================

#[test]
fn accept_sequence_gap_with_both_boundaries() {
    let mut tracker = tracker();

    let id =
        record_sequence_gap(&mut tracker, 10 * SECOND_NS, Some(5), Some(100), Some(106)).unwrap();

    assert_eq!(id, 1);
    assert_eq!(tracker.len(), 1);
}

#[test]
fn accept_sequence_gap_with_only_previous_boundary() {
    let mut tracker = tracker();

    record_sequence_gap(&mut tracker, 10 * SECOND_NS, None, Some(100), None).unwrap();

    assert_eq!(tracker.len(), 1);
}

#[test]
fn accept_sequence_gap_with_only_next_boundary() {
    let mut tracker = tracker();

    record_sequence_gap(&mut tracker, 10 * SECOND_NS, None, None, Some(106)).unwrap();

    assert_eq!(tracker.len(), 1);
}

#[test]
fn accept_sequence_gap_with_unknown_missing_count() {
    let mut tracker = tracker();

    record_sequence_gap(&mut tracker, 10 * SECOND_NS, None, Some(100), Some(106)).unwrap();

    let snapshot = tracker.snapshot(20 * SECOND_NS).unwrap();

    assert_eq!(snapshot.windows.one_minute.gaps, 1);
    assert_eq!(snapshot.windows.one_minute.unknown_missing_gaps, 1);
}

#[test]
fn accept_connection_interruption_without_sequences() {
    let mut tracker = tracker();

    let id = tracker
        .record_gap(
            10 * SECOND_NS,
            GapReason::ConnectionInterruption,
            None,
            None,
            None,
        )
        .unwrap();

    assert_eq!(id, 1);
}

#[test]
fn accept_data_interruption_without_sequences() {
    let mut tracker = tracker();

    let id = tracker
        .record_gap(
            10 * SECOND_NS,
            GapReason::DataInterruption,
            None,
            None,
            None,
        )
        .unwrap();

    assert_eq!(id, 1);
}

#[test]
fn accept_connection_interruption_with_sequence_evidence() {
    let mut tracker = tracker();

    tracker
        .record_gap(
            10 * SECOND_NS,
            GapReason::ConnectionInterruption,
            None,
            Some(100),
            Some(106),
        )
        .unwrap();

    assert_eq!(tracker.len(), 1);
}

#[test]
fn accept_exact_count_without_sequence_boundaries_for_interruption() {
    let mut tracker = tracker();

    tracker
        .record_gap(
            10 * SECOND_NS,
            GapReason::DataInterruption,
            Some(5),
            None,
            None,
        )
        .unwrap();

    assert_eq!(
        tracker.records().next().unwrap().missing_events_exact,
        Some(5)
    );
}

#[test]
fn accept_equal_sequence_boundaries() {
    let mut tracker = tracker();

    record_sequence_gap(&mut tracker, 10 * SECOND_NS, None, Some(100), Some(100)).unwrap();
}

#[test]
fn accept_decreasing_sequence_boundaries() {
    let mut tracker = tracker();

    // Generic integrity validation must not assume that
    // exchange sequence numbers are globally increasing.
    //
    // Sequence resets and wraparounds belong to the
    // exchange-specific validator.
    record_sequence_gap(&mut tracker, 10 * SECOND_NS, None, Some(1_000), Some(10)).unwrap();
}

#[test]
fn accept_large_sequence_numbers() {
    let mut tracker = tracker();

    record_sequence_gap(
        &mut tracker,
        10 * SECOND_NS,
        None,
        Some(u64::MAX - 1),
        Some(u64::MAX),
    )
    .unwrap();
}

// ============================================================
// GAP TIMESTAMP VALIDATION
// ============================================================

#[test]
fn accept_gap_at_history_start() {
    let mut tracker = tracker();

    record_sequence_gap(&mut tracker, 0, Some(5), Some(100), Some(106)).unwrap();

    assert_eq!(tracker.last_gap_at_ns(), Some(0));
}

#[test]
fn accept_equal_detection_timestamps() {
    let mut tracker = tracker();

    for _ in 0..2 {
        record_sequence_gap(&mut tracker, 10 * SECOND_NS, Some(5), Some(100), Some(106)).unwrap();
    }

    assert_eq!(tracker.len(), 2);
}

#[test]
fn reject_gap_before_history_start() {
    let mut tracker = GapTracker::new("stream_a", 1, 100 * SECOND_NS, 100).unwrap();

    assert_invalid(
        record_sequence_gap(&mut tracker, 99 * SECOND_NS, Some(5), Some(100), Some(106)),
        "history start",
    );
}

#[test]
fn reject_decreasing_gap_timestamp() {
    let mut tracker = tracker();

    record_sequence_gap(&mut tracker, 20 * SECOND_NS, Some(5), Some(100), Some(106)).unwrap();

    assert_invalid(
        record_sequence_gap(&mut tracker, 10 * SECOND_NS, Some(5), Some(200), Some(206)),
        "nondecreasing",
    );
}

#[test]
fn reject_negative_gap_timestamp_when_history_starts_at_zero() {
    let mut tracker = tracker();

    assert_invalid(
        record_sequence_gap(&mut tracker, -1, Some(5), Some(100), Some(106)),
        "history start",
    );
}

// ============================================================
// MISSING COUNT VALIDATION
// ============================================================

#[test]
fn reject_zero_exact_missing_count() {
    let mut tracker = tracker();

    assert_invalid(
        record_sequence_gap(&mut tracker, 10 * SECOND_NS, Some(0), Some(100), Some(106)),
        "missing_events_exact",
    );
}

#[test]
fn accept_positive_exact_missing_count() {
    let mut tracker = tracker();

    record_sequence_gap(&mut tracker, 10 * SECOND_NS, Some(1), Some(100), Some(102)).unwrap();

    assert_eq!(
        tracker.records().next().unwrap().missing_events_exact,
        Some(1)
    );
}

#[test]
fn accept_maximum_exact_missing_count() {
    let mut tracker = tracker();

    record_sequence_gap(
        &mut tracker,
        10 * SECOND_NS,
        Some(u64::MAX),
        Some(100),
        Some(106),
    )
    .unwrap();

    assert_eq!(
        tracker.records().next().unwrap().missing_events_exact,
        Some(u64::MAX)
    );
}

#[test]
fn unknown_count_is_not_treated_as_zero() {
    let mut tracker = tracker();

    record_sequence_gap(&mut tracker, 10 * SECOND_NS, None, Some(100), Some(106)).unwrap();

    let snapshot = tracker.snapshot(20 * SECOND_NS).unwrap();

    assert_eq!(snapshot.windows.one_minute.missing_events_exact, 0);

    assert_eq!(snapshot.windows.one_minute.unknown_missing_gaps, 1);
}

// ============================================================
// SEQUENCE EVIDENCE VALIDATION
// ============================================================

#[test]
fn reject_sequence_gap_without_boundaries() {
    let mut tracker = tracker();

    assert_invalid(
        record_sequence_gap(&mut tracker, 10 * SECOND_NS, Some(5), None, None),
        "sequence boundary",
    );
}

#[test]
fn reject_sequence_gap_without_boundaries_even_when_count_unknown() {
    let mut tracker = tracker();

    assert_invalid(
        record_sequence_gap(&mut tracker, 10 * SECOND_NS, None, None, None),
        "sequence boundary",
    );
}

#[test]
fn sequence_difference_is_not_used_to_infer_missing_count() {
    let mut tracker = tracker();

    record_sequence_gap(&mut tracker, 10 * SECOND_NS, None, Some(100), Some(1_000)).unwrap();

    let record = tracker.records().next().unwrap();

    assert_eq!(record.missing_events_exact, None);

    let snapshot = tracker.snapshot(20 * SECOND_NS).unwrap();

    assert_eq!(snapshot.windows.one_minute.unknown_missing_gaps, 1);
}

#[test]
fn exact_count_is_not_recomputed_from_sequence_difference() {
    let mut tracker = tracker();

    record_sequence_gap(
        &mut tracker,
        10 * SECOND_NS,
        Some(3),
        Some(100),
        Some(1_000),
    )
    .unwrap();

    assert_eq!(
        tracker.records().next().unwrap().missing_events_exact,
        Some(3)
    );
}

// ============================================================
// INVALID RECORD ATOMICITY
// ============================================================

#[test]
fn invalid_gap_does_not_consume_id() {
    let mut tracker = tracker();

    let invalid = record_sequence_gap(&mut tracker, 10 * SECOND_NS, Some(0), Some(100), Some(106));

    assert!(invalid.is_err());

    let valid_id =
        record_sequence_gap(&mut tracker, 20 * SECOND_NS, Some(5), Some(100), Some(106)).unwrap();

    assert_eq!(valid_id, 1);
}

#[test]
fn invalid_gap_does_not_change_last_gap_timestamp() {
    let mut tracker = tracker();

    record_sequence_gap(&mut tracker, 20 * SECOND_NS, Some(5), Some(100), Some(106)).unwrap();

    let invalid = record_sequence_gap(&mut tracker, 10 * SECOND_NS, Some(5), Some(200), Some(206));

    assert!(invalid.is_err());

    assert_eq!(tracker.last_gap_at_ns(), Some(20 * SECOND_NS));
}

#[test]
fn invalid_gap_does_not_change_history_length() {
    let mut tracker = tracker();

    record_sequence_gap(&mut tracker, 10 * SECOND_NS, Some(5), Some(100), Some(106)).unwrap();

    let before = tracker.len();

    let invalid = record_sequence_gap(&mut tracker, 20 * SECOND_NS, Some(0), Some(200), Some(206));

    assert!(invalid.is_err());
    assert_eq!(tracker.len(), before);
}

#[test]
fn invalid_gap_does_not_change_statistics() {
    let mut tracker = tracker();

    record_sequence_gap(&mut tracker, 10 * SECOND_NS, Some(5), Some(100), Some(106)).unwrap();

    let before = tracker.snapshot(30 * SECOND_NS).unwrap();

    let invalid = record_sequence_gap(&mut tracker, 20 * SECOND_NS, Some(0), Some(200), Some(206));

    assert!(invalid.is_err());

    let after = tracker.snapshot(30 * SECOND_NS).unwrap();

    assert_eq!(before, after);
}

// ============================================================
// QUERY VALIDATION
// ============================================================

#[test]
fn accept_snapshot_at_history_start() {
    let tracker = tracker();

    tracker.snapshot(0).unwrap();
}

#[test]
fn reject_snapshot_before_history_start() {
    let tracker = GapTracker::new("stream_a", 1, 100 * SECOND_NS, 100).unwrap();

    assert!(tracker.snapshot(99 * SECOND_NS).is_err());
}

#[test]
fn reject_history_before_history_start() {
    let tracker = GapTracker::new("stream_a", 1, 100 * SECOND_NS, 100).unwrap();

    assert!(
        tracker
            .history(IntegrityWindow::OneMinute, 99 * SECOND_NS,)
            .is_err()
    );
}

#[test]
fn accept_history_at_history_start() {
    let tracker = tracker();

    tracker.history(IntegrityWindow::OneMinute, 0).unwrap();
}

// ============================================================
// VALIDATION IS NON-MUTATING
// ============================================================

#[test]
fn repeated_valid_recording_preserves_sequential_ids() {
    let mut tracker = tracker();

    let first =
        record_sequence_gap(&mut tracker, 10 * SECOND_NS, Some(5), Some(100), Some(106)).unwrap();

    let second =
        record_sequence_gap(&mut tracker, 20 * SECOND_NS, Some(10), Some(200), Some(211)).unwrap();

    assert_eq!((first, second), (1, 2));
}

#[test]
fn valid_gap_record_preserves_supplied_evidence() {
    let mut tracker = tracker();

    record_sequence_gap(&mut tracker, 10 * SECOND_NS, Some(5), Some(100), Some(106)).unwrap();

    let record = tracker.records().next().unwrap();

    assert_eq!(record.reason, GapReason::SequenceGap);
    assert_eq!(record.missing_events_exact, Some(5));
    assert_eq!(record.sequence_before, Some(100));
    assert_eq!(record.sequence_after, Some(106));
}
