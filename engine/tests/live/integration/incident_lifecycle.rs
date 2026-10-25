use marketforge_engine::live::integrity::{
    ClassifiedEpisodeOutcome, ConfirmedIntegrityIncident, EpisodeReportOutcome, GapReason,
    GapTracker, IntegrityEpisodeResolution, IntegrityFailureIdentity, IntegrityFailureKind,
    IntegrityIncidentLifecycle,
};

const STREAM: &str = "integration:binance:spot:BTCUSDT:depth";
const START: i64 = 1_000_000_000;

fn lifecycle() -> IntegrityIncidentLifecycle {
    IntegrityIncidentLifecycle::new(STREAM, 10).unwrap()
}

fn tracker() -> GapTracker {
    GapTracker::new(STREAM, 10, START, 100).unwrap()
}

fn failure(id: u64) -> IntegrityFailureIdentity {
    IntegrityFailureIdentity {
        generation: 10,
        failure_id: id,
        kind: IntegrityFailureKind::SequenceDiscontinuity,
    }
}

fn incident() -> ConfirmedIntegrityIncident {
    ConfirmedIntegrityIncident::SequenceDiscontinuity {
        sequence_before: Some(100),
        sequence_after: Some(105),
        missing_events_exact: None,
    }
}

#[test]
fn confirmed_failure_is_recorded_once_across_recovery_retries() {
    let mut lifecycle = lifecycle();
    let mut tracker = tracker();

    let ClassifiedEpisodeOutcome::Opened(token) = lifecycle.open_for_failure(failure(1)).unwrap()
    else {
        panic!("expected new recovery episode");
    };

    let first = lifecycle
        .report_confirmed(token, &mut tracker, START + 100, incident())
        .unwrap();

    assert_eq!(first, EpisodeReportOutcome::Recorded { gap_id: 1 },);

    // Three recovery attempts fail.
    // None creates another acquisition gap.
    for attempt in 1..=3 {
        lifecycle.record_recovery_failure(token, attempt).unwrap();
    }

    let repeated = lifecycle
        .report_confirmed(token, &mut tracker, START + 200, incident())
        .unwrap();

    assert_eq!(
        repeated,
        EpisodeReportOutcome::AlreadyRecorded { gap_id: 1 },
    );

    assert_eq!(tracker.len(), 1);

    assert_eq!(lifecycle.active_recovery_failures_total(), Some(3),);

    let closed = lifecycle
        .close(token, IntegrityEpisodeResolution::CurrentStateRestored)
        .unwrap();

    assert_eq!(closed.gap_id, Some(1));

    // Recovery closure does not erase historical evidence.
    assert_eq!(tracker.len(), 1);

    assert_eq!(
        tracker.records().next().unwrap().reason,
        GapReason::SequenceGap,
    );

    assert_eq!(tracker.records().next().unwrap().missing_events_exact, None,);
}

#[test]
fn independent_failure_cannot_merge_with_active_episode() {
    let mut lifecycle = lifecycle();

    lifecycle.open_for_failure(failure(1)).unwrap();

    let second = lifecycle.open_for_failure(failure(2));

    assert!(second.is_err());

    assert_eq!(lifecycle.active_failure(), Some(failure(1)));

    assert_eq!(lifecycle.episodes_opened_total(), 1);
}

#[test]
fn unverified_trade_coverage_does_not_create_confirmed_gap() {
    let mut lifecycle = lifecycle();
    let mut tracker = tracker();

    let ClassifiedEpisodeOutcome::Opened(token) = lifecycle
        .open_for_failure(IntegrityFailureIdentity {
            generation: 10,
            failure_id: 1,
            kind: IntegrityFailureKind::TradeCoverageUnverified,
        })
        .unwrap()
    else {
        panic!("expected episode");
    };

    assert!(
        lifecycle
            .report_confirmed(token, &mut tracker, START + 100, incident(),)
            .is_err()
    );

    assert!(tracker.is_empty());

    lifecycle
        .close(token, IntegrityEpisodeResolution::AbandonedUnverified)
        .unwrap();

    assert_eq!(lifecycle.unverified_episodes_total(), 1);
}

#[test]
fn old_episode_cannot_write_into_new_generation() {
    let mut lifecycle = lifecycle();

    let ClassifiedEpisodeOutcome::Opened(old_token) =
        lifecycle.open_for_failure(failure(1)).unwrap()
    else {
        panic!("expected episode");
    };

    lifecycle
        .close(old_token, IntegrityEpisodeResolution::AbandonedUnverified)
        .unwrap();

    lifecycle.begin_generation(11).unwrap();

    let mut new_tracker = GapTracker::new(STREAM, 11, START, 100).unwrap();

    let result = lifecycle.report_confirmed(old_token, &mut new_tracker, START + 100, incident());

    assert!(result.is_err());
    assert!(new_tracker.is_empty());
}
