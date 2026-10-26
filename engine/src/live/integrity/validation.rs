use crate::live::integrity::model::{GapReason, GapRecord};

// ============================================================
// TRACKER INITIALIZATION
// ============================================================

/// Validate the configuration of a new gap tracker.
///
/// Each tracker belongs to one source-stream runtime generation.
pub fn validate_tracker(
    stream_id: &str,
    history_started_at_ns: i64,
    max_records: usize,
) -> Result<(), String> {
    if stream_id.is_empty() {
        return Err("stream_id must not be empty".into());
    }

    if stream_id.trim() != stream_id {
        return Err("stream_id must not contain leading or trailing whitespace".into());
    }

    if stream_id.chars().any(char::is_whitespace) {
        return Err("stream_id must not contain whitespace".into());
    }

    if stream_id.chars().any(char::is_control) {
        return Err("stream_id must not contain control characters".into());
    }

    if history_started_at_ns < 0 {
        return Err("history_started_at_ns must not be negative".into());
    }

    if max_records == 0 {
        return Err("max_records must be greater than zero".into());
    }

    Ok(())
}

// ============================================================
// GAP RECORD VALIDATION
// ============================================================

/// Validate a detected gap before inserting it into history.
///
/// This function validates the record's internal structure.
///
/// It does not:
/// - Detect sequence gaps.
/// - Infer missing-event counts.
/// - Interpret exchange-specific sequence numbers.
/// - Determine whether recovery succeeded.
/// - Verify that an exchange actually lost events.
pub fn validate_gap(
    record: &GapRecord,
    history_started_at_ns: i64,
    previous_gap_at_ns: Option<i64>,
) -> Result<(), String> {
    validate_gap_identity(record)?;

    validate_gap_timestamp(
        record.detected_at_ns,
        history_started_at_ns,
        previous_gap_at_ns,
    )?;

    validate_missing_count(record)?;

    validate_sequence_evidence(record)?;

    Ok(())
}

// ============================================================
// GAP IDENTITY
// ============================================================

fn validate_gap_identity(record: &GapRecord) -> Result<(), String> {
    if record.gap_id == 0 {
        return Err("gap_id must be greater than zero".into());
    }

    Ok(())
}

// ============================================================
// TIMESTAMP VALIDATION
// ============================================================

fn validate_gap_timestamp(
    detected_at_ns: i64,
    history_started_at_ns: i64,
    previous_gap_at_ns: Option<i64>,
) -> Result<(), String> {
    if detected_at_ns < history_started_at_ns {
        return Err("gap detection timestamp precedes history start".into());
    }

    if let Some(previous) = previous_gap_at_ns {
        if detected_at_ns < previous {
            return Err("gap detection timestamps must be nondecreasing".into());
        }
    }

    Ok(())
}

// ============================================================
// EXACT MISSING-EVENT COUNT
// ============================================================

fn validate_missing_count(record: &GapRecord) -> Result<(), String> {
    if let Some(count) = record.missing_events_exact {
        if count == 0 {
            return Err("missing_events_exact must be positive when provided".into());
        }
    }

    // None explicitly represents an unknown count.
    //
    // Do not infer counts by subtracting sequence numbers.
    // The exchange-specific sequence validator is responsible
    // for determining whether exact counts are available.

    Ok(())
}

// ============================================================
// SEQUENCE EVIDENCE
// ============================================================

fn validate_sequence_evidence(record: &GapRecord) -> Result<(), String> {
    match record.reason {
        GapReason::SequenceGap => {
            // A sequence-gap incident must include some
            // sequence evidence.
            //
            // Both boundaries are preferred but are not
            // universally available.

            if record.sequence_before.is_none() && record.sequence_after.is_none() {
                return Err("sequence_gap requires at least one sequence boundary".into());
            }
        }

        GapReason::ConnectionInterruption | GapReason::DataInterruption => {
            // Sequence boundaries are optional.
            //
            // An interruption can be detected independently
            // of exchange sequence metadata.
        }
    }

    // Do not require sequence_after > sequence_before.
    //
    // Some exchanges reset or wrap sequence numbers.
    // Other exchanges use ranges or nonconsecutive values.
    //
    // Sequence semantics belong to exchange-specific
    // validators, not the generic integrity tracker.

    Ok(())
}
