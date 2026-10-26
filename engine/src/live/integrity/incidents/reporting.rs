use crate::live::integrity::model::GapReason;
use crate::live::integrity::tracker::GapTracker;

// ============================================================
// INCIDENT IDENTITY
// ============================================================

/// Identifies the runtime generation that detected an
/// acquisition discontinuity.
///
/// The reporting boundary rejects stale generations.
#[derive(Debug, Clone, Copy)]
pub struct IncidentOrigin<'a> {
    pub stream_id: &'a str,
    pub generation: u64,
}

// ============================================================
// CONFIRMED INCIDENTS
// ============================================================

/// Acquisition discontinuities eligible for GapTracker.
///
/// Callers must have already established that the event
/// represents an actual acquisition discontinuity.
///
/// A suspicious numerical trade-ID gap alone is NOT
/// sufficient evidence to construct a confirmed incident.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfirmedIntegrityIncident {
    /// A sequence-continuity requirement was violated.
    ///
    /// Sequence boundaries are native protocol evidence.
    /// They must not automatically be interpreted as
    /// counts of missing executions or messages.
    SequenceDiscontinuity {
        sequence_before: Option<u64>,
        sequence_after: Option<u64>,
        missing_events_exact: Option<u64>,
    },

    /// Acquisition was interrupted by a connection failure.
    ConnectionInterruption {
        sequence_before: Option<u64>,
        sequence_after: Option<u64>,
    },

    /// A required acquisition path lost data independently
    /// of a connection interruption.
    DataInterruption {
        sequence_before: Option<u64>,
        sequence_after: Option<u64>,
        missing_events_exact: Option<u64>,
    },
}

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IncidentReportingError {
    WrongStream,
    ObsoleteGeneration,
    InvalidEvidence(String),
}

// ============================================================
// REPORTER
// ============================================================

/// Stateless adapter from confirmed acquisition incidents
/// to the existing per-generation GapTracker.
///
/// GapTracker remains the authoritative incident-history
/// owner.
///
/// This adapter does not:
/// - Detect sequence gaps.
/// - Decide whether a trade-ID anomaly proves data loss.
/// - Execute recovery.
/// - Modify order books.
/// - Publish market-data events.
/// - Persist incident history.
/// - Deduplicate repeated reports of the same incident.
///
/// The authoritative stream runtime must report each
/// detected incident exactly once.
pub struct IntegrityIncidentReporter;

impl IntegrityIncidentReporter {
    pub(in crate::live::integrity) fn record(
        tracker: &mut GapTracker,
        origin: IncidentOrigin<'_>,
        detected_at_ns: i64,
        incident: ConfirmedIntegrityIncident,
    ) -> Result<u64, IncidentReportingError> {
        // ----------------------------------------------------
        // STREAM IDENTITY
        // ----------------------------------------------------

        if tracker.stream_id() != origin.stream_id {
            return Err(IncidentReportingError::WrongStream);
        }

        // ----------------------------------------------------
        // GENERATION FENCING
        // ----------------------------------------------------

        if tracker.generation() != origin.generation {
            return Err(IncidentReportingError::ObsoleteGeneration);
        }

        // ----------------------------------------------------
        // INCIDENT MAPPING
        // ----------------------------------------------------

        let (reason, missing_events_exact, sequence_before, sequence_after) = match incident {
            ConfirmedIntegrityIncident::SequenceDiscontinuity {
                sequence_before,
                sequence_after,
                missing_events_exact,
            } => (
                GapReason::SequenceGap,
                missing_events_exact,
                sequence_before,
                sequence_after,
            ),

            ConfirmedIntegrityIncident::ConnectionInterruption {
                sequence_before,
                sequence_after,
            } => (
                GapReason::ConnectionInterruption,
                None,
                sequence_before,
                sequence_after,
            ),

            ConfirmedIntegrityIncident::DataInterruption {
                sequence_before,
                sequence_after,
                missing_events_exact,
            } => (
                GapReason::DataInterruption,
                missing_events_exact,
                sequence_before,
                sequence_after,
            ),
        };

        // GapTracker validates timestamps, sequence evidence,
        // exact missing-event counts, and gap-ID allocation.
        //
        // Invalid records do not mutate its state.
        tracker
            .record_gap(
                detected_at_ns,
                reason,
                missing_events_exact,
                sequence_before,
                sequence_after,
            )
            .map_err(IncidentReportingError::InvalidEvidence)
    }
}

// ============================================================
// TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    use crate::live::integrity::model::IntegrityWindow;

    const STREAM: &str = "binance:spot:BTCUSDT:depth";
    const START: i64 = 1_000_000_000;

    fn tracker() -> GapTracker {
        GapTracker::new(STREAM, 10, START, 100).unwrap()
    }

    fn origin() -> IncidentOrigin<'static> {
        IncidentOrigin {
            stream_id: STREAM,
            generation: 10,
        }
    }

    #[test]
    fn records_confirmed_sequence_discontinuity() {
        let mut tracker = tracker();

        let gap_id = IntegrityIncidentReporter::record(
            &mut tracker,
            origin(),
            START + 100,
            ConfirmedIntegrityIncident::SequenceDiscontinuity {
                sequence_before: Some(100),
                sequence_after: Some(105),
                missing_events_exact: None,
            },
        )
        .unwrap();

        assert_eq!(gap_id, 1);
        assert_eq!(tracker.len(), 1);

        let record = tracker.records().next().unwrap();

        assert_eq!(record.reason, GapReason::SequenceGap);
        assert_eq!(record.sequence_before, Some(100));
        assert_eq!(record.sequence_after, Some(105));

        // The difference between 100 and 105 does not
        // establish an exact missing-event count.
        assert_eq!(record.missing_events_exact, None);
    }

    #[test]
    fn connection_interruption_has_unknown_missing_count() {
        let mut tracker = tracker();

        IntegrityIncidentReporter::record(
            &mut tracker,
            origin(),
            START + 100,
            ConfirmedIntegrityIncident::ConnectionInterruption {
                sequence_before: Some(100),
                sequence_after: None,
            },
        )
        .unwrap();

        let record = tracker.records().next().unwrap();

        assert_eq!(record.reason, GapReason::ConnectionInterruption,);

        assert_eq!(record.missing_events_exact, None);
    }

    #[test]
    fn records_data_interruption_without_sequence_ids() {
        let mut tracker = tracker();

        IntegrityIncidentReporter::record(
            &mut tracker,
            origin(),
            START + 100,
            ConfirmedIntegrityIncident::DataInterruption {
                sequence_before: None,
                sequence_after: None,
                missing_events_exact: None,
            },
        )
        .unwrap();

        assert_eq!(
            tracker.records().next().unwrap().reason,
            GapReason::DataInterruption,
        );
    }

    #[test]
    fn rejects_obsolete_generation_without_mutation() {
        let mut tracker = tracker();

        let stale = IncidentOrigin {
            stream_id: STREAM,
            generation: 9,
        };

        let result = IntegrityIncidentReporter::record(
            &mut tracker,
            stale,
            START + 100,
            ConfirmedIntegrityIncident::ConnectionInterruption {
                sequence_before: None,
                sequence_after: None,
            },
        );

        assert_eq!(result, Err(IncidentReportingError::ObsoleteGeneration),);

        assert!(tracker.is_empty());
    }

    #[test]
    fn rejects_wrong_stream_without_mutation() {
        let mut tracker = tracker();

        let wrong = IncidentOrigin {
            stream_id: "different-stream",
            generation: 10,
        };

        let result = IntegrityIncidentReporter::record(
            &mut tracker,
            wrong,
            START + 100,
            ConfirmedIntegrityIncident::DataInterruption {
                sequence_before: None,
                sequence_after: None,
                missing_events_exact: None,
            },
        );

        assert_eq!(result, Err(IncidentReportingError::WrongStream),);

        assert!(tracker.is_empty());
    }

    #[test]
    fn rejects_invalid_sequence_evidence() {
        let mut tracker = tracker();

        let result = IntegrityIncidentReporter::record(
            &mut tracker,
            origin(),
            START + 100,
            ConfirmedIntegrityIncident::SequenceDiscontinuity {
                sequence_before: None,
                sequence_after: None,
                missing_events_exact: None,
            },
        );

        assert!(matches!(
            result,
            Err(IncidentReportingError::InvalidEvidence(_)),
        ));

        assert!(tracker.is_empty());
    }

    #[test]
    fn invalid_missing_count_does_not_mutate_history() {
        let mut tracker = tracker();

        let result = IntegrityIncidentReporter::record(
            &mut tracker,
            origin(),
            START + 100,
            ConfirmedIntegrityIncident::DataInterruption {
                sequence_before: Some(100),
                sequence_after: Some(105),
                missing_events_exact: Some(0),
            },
        );

        assert!(matches!(
            result,
            Err(IncidentReportingError::InvalidEvidence(_)),
        ));

        assert!(tracker.is_empty());
    }

    #[test]
    fn preserves_rolling_window_statistics() {
        let mut tracker = tracker();

        IntegrityIncidentReporter::record(
            &mut tracker,
            origin(),
            START + 100,
            ConfirmedIntegrityIncident::SequenceDiscontinuity {
                sequence_before: Some(100),
                sequence_after: Some(105),
                missing_events_exact: None,
            },
        )
        .unwrap();

        IntegrityIncidentReporter::record(
            &mut tracker,
            origin(),
            START + 200,
            ConfirmedIntegrityIncident::DataInterruption {
                sequence_before: None,
                sequence_after: None,
                missing_events_exact: Some(2),
            },
        )
        .unwrap();

        let snapshot = tracker.snapshot(START + 300).unwrap();

        assert_eq!(snapshot.windows.one_minute.gaps, 2);
        assert_eq!(snapshot.windows.one_minute.missing_events_exact, 2,);
        assert_eq!(snapshot.windows.one_minute.unknown_missing_gaps, 1,);

        let history = tracker
            .history(IntegrityWindow::OneMinute, START + 300)
            .unwrap();

        assert_eq!(history.gaps.len(), 2);
        assert_eq!(history.gaps[0].gap_id, 2);
        assert_eq!(history.gaps[1].gap_id, 1);
    }

    #[test]
    fn incident_history_remains_after_recovery() {
        let mut tracker = tracker();

        IntegrityIncidentReporter::record(
            &mut tracker,
            origin(),
            START + 100,
            ConfirmedIntegrityIncident::ConnectionInterruption {
                sequence_before: Some(100),
                sequence_after: None,
            },
        )
        .unwrap();

        // A successful book resynchronization does not
        // modify or erase the original gap record.
        //
        // Recovery state is owned outside GapTracker.

        let snapshot = tracker.snapshot(START + 1_000).unwrap();

        assert_eq!(snapshot.windows.one_minute.gaps, 1);
        assert_eq!(tracker.len(), 1);
    }
    // ============================================================
    // N5N.7 — INCIDENT GENERATION FENCING
    // ============================================================

    #[test]
    fn old_generation_cannot_write_into_new_gap_tracker() {
        let mut tracker = GapTracker::new(STREAM, 11, START, 100).unwrap();

        let stale_origin = IncidentOrigin {
            stream_id: STREAM,
            generation: 10,
        };

        let result = IntegrityIncidentReporter::record(
            &mut tracker,
            stale_origin,
            START + 100,
            ConfirmedIntegrityIncident::ConnectionInterruption {
                sequence_before: Some(100),
                sequence_after: None,
            },
        );

        assert_eq!(result, Err(IncidentReportingError::ObsoleteGeneration));

        assert_eq!(tracker.len(), 0);
        assert_eq!(tracker.generation(), 11);
    }
}
