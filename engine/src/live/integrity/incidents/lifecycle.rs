use crate::live::integrity::incidents::reporting::{
    ConfirmedIntegrityIncident, IncidentOrigin, IncidentReportingError, IntegrityIncidentReporter,
};
use crate::live::integrity::tracker::GapTracker;

// ============================================================
// EPISODE IDENTITY
// ============================================================

/// Identifies one recovery/coverage episode.
///
/// Episode IDs increase monotonically for the lifetime
/// of one IntegrityIncidentLifecycle instance.
///
/// Generation is included to fence obsolete callbacks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct IntegrityEpisodeToken {
    pub generation: u64,
    pub episode_id: u64,
}
// ============================================================
// FAILURE IDENTITY AND CLASSIFICATION
// ============================================================

/// Classification of the event that opened an episode.
///
/// Only some kinds can directly support a confirmed
/// GapTracker incident.
///
/// Classification is diagnostic and does not itself
/// establish that acquisition loss occurred.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IntegrityFailureKind {
    SequenceDiscontinuity,
    ConnectionInterruption,
    DataInterruption,

    /// Suspicious trade continuity requiring investigation.
    /// Not automatically a confirmed acquisition gap.
    TradeCoverageUnverified,

    /// Canonical message did not match the authoritative
    /// exchange/instrument/source binding.
    SourceProvenance,

    /// Downstream publication handoff was uncertain.
    /// This is not automatically an exchange acquisition gap.
    PublicationDelivery,
}

/// Identifies one independently detected failure.
///
/// `failure_id` is allocated by the source runtime.
///
/// It must remain stable across repeated reports of
/// the same failure and must not be reused for a
/// different failure in the same stream generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct IntegrityFailureIdentity {
    pub generation: u64,
    pub failure_id: u64,
    pub kind: IntegrityFailureKind,
}

/// Result of classifying an initial failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClassifiedEpisodeOutcome {
    /// A new recovery episode was opened.
    Opened(IntegrityEpisodeToken),

    /// The same initiating failure was reported again.
    Existing(IntegrityEpisodeToken),
}
// ============================================================
// REPORTING OUTCOME
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EpisodeReportOutcome {
    /// First confirmed incident record for this episode.
    Recorded { gap_id: u64 },

    /// The episode already owns a confirmed gap record.
    ///
    /// No additional GapTracker record was created.
    AlreadyRecorded { gap_id: u64 },
}

// ============================================================
// EPISODE CLOSURE
// ============================================================

/// The reason an episode was closed.
///
/// This describes the operational lifecycle only.
///
/// It does not rewrite or delete the original GapRecord.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntegrityEpisodeResolution {
    /// The owning recovery component established that
    /// the current acquisition state may resume.
    ///
    /// This does NOT necessarily establish that every
    /// historical event was recovered.
    CurrentStateRestored,

    /// Recovery was abandoned or the old stream epoch
    /// was retired without proving historical coverage.
    AbandonedUnverified,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClosedIntegrityEpisode {
    pub token: IntegrityEpisodeToken,

    /// None means that no confirmed acquisition gap
    /// was recorded for this episode.
    pub gap_id: Option<u64>,

    pub resolution: IntegrityEpisodeResolution,
}

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IntegrityEpisodeError {
    EmptyStreamId,

    ObsoleteGeneration,

    NoActiveEpisode,

    WrongEpisode,

    EpisodeIdExhausted,

    ActiveEpisodeMustBeClosed,

    GenerationMustIncrease,

    WrongTracker,

    IncidentReporting(IncidentReportingError),

    InvalidFailureIdentity,

    /// An unrelated failure was detected while another
    /// classified episode remains active.
    ///
    /// The caller must not silently merge these events.
    DistinctFailureWhileActive {
        active: IntegrityFailureIdentity,
        incoming: IntegrityFailureIdentity,
    },

    /// A classified episode was opened previously using
    /// the legacy, unclassified API.
    UnclassifiedEpisodeActive,

    /// Legacy generation-only grouping was attempted on
    /// an explicitly classified episode.
    ClassifiedEpisodeRequiresFailureIdentity,

    /// A recovery attempt must be uniquely identified
    /// and newer than previously reported attempts.
    InvalidRecoveryAttempt,

    /// Retry belongs to the wrong active episode.
    WrongRecoveryEpisode,

    /// Confirmed incident evidence is incompatible with
    /// the episode's failure classification.
    IncidentKindMismatch,
}

// ============================================================
// ACTIVE EPISODE
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ActiveIntegrityEpisode {
    token: IntegrityEpisodeToken,

    gap_id: Option<u64>,

    /// None is supported for existing legacy callers
    /// of open_or_get().
    ///
    /// New runtime integration must use open_for_failure()
    /// and establish a classified origin.
    initiating_failure: Option<IntegrityFailureIdentity>,

    /// Highest recovery-attempt ID reported for this
    /// episode.
    ///
    /// Recovery attempts do not open new episodes or
    /// create GapTracker records.
    last_recovery_attempt: Option<u64>,

    recovery_failures_total: u64,
}

// ============================================================
// INCIDENT LIFECYCLE
// ============================================================

/// Single-owner recovery-episode accounting for one stream.
///
/// Owns:
/// - Current stream generation.
/// - At most one active recovery episode.
/// - Monotonic episode-ID allocation.
/// - A bounded amount of episode metadata.
///
/// Does not own:
/// - GapTracker.
/// - Order books.
/// - Trade controllers.
/// - WebSocket connections.
/// - Recovery scheduling.
/// - Snapshot requests.
/// - Historical trade reconciliation.
///
/// The authoritative source actor must serialize calls.
///
/// All confirmed incident writes pass through
/// IntegrityIncidentReporter.
///
/// One episode may generate at most one GapTracker record.
///
/// The surrounding source runtime is responsible for
/// grouping related failures into the correct episode.
#[derive(Debug)]
pub struct IntegrityIncidentLifecycle {
    stream_id: String,

    generation: u64,

    next_episode_id: u64,

    active: Option<ActiveIntegrityEpisode>,

    episodes_opened_total: u64,

    episodes_closed_total: u64,

    /// Episodes closed without a confirmed gap record.
    ///
    /// This is diagnostic accounting, not a count of
    /// proven missing market-data events.
    unverified_episodes_total: u64,
}

impl IntegrityIncidentLifecycle {
    // ========================================================
    // CONSTRUCTION
    // ========================================================

    pub fn new(
        stream_id: impl Into<String>,
        generation: u64,
    ) -> Result<Self, IntegrityEpisodeError> {
        let stream_id = stream_id.into();

        if stream_id.trim().is_empty() {
            return Err(IntegrityEpisodeError::EmptyStreamId);
        }

        Ok(Self {
            stream_id,
            generation,

            next_episode_id: 1,

            active: None,

            episodes_opened_total: 0,
            episodes_closed_total: 0,
            unverified_episodes_total: 0,
        })
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

    pub fn active_episode(&self) -> Option<IntegrityEpisodeToken> {
        self.active.map(|episode| episode.token)
    }

    pub fn has_active_episode(&self) -> bool {
        self.active.is_some()
    }

    pub fn active_gap_id(&self) -> Option<u64> {
        self.active.and_then(|episode| episode.gap_id)
    }

    pub fn episodes_opened_total(&self) -> u64 {
        self.episodes_opened_total
    }

    pub fn episodes_closed_total(&self) -> u64 {
        self.episodes_closed_total
    }

    pub fn unverified_episodes_total(&self) -> u64 {
        self.unverified_episodes_total
    }
    // ========================================================
    // CLASSIFIED EPISODE OPENING
    // ========================================================

    /// Open or retrieve an episode for one specific
    /// independently detected failure.
    ///
    /// Unlike open_or_get(), this method does not merge
    /// arbitrary failures simply because they occur
    /// during the same recovery period.
    ///
    /// A different initiating failure while an episode
    /// is active must be handled explicitly by the
    /// authoritative runtime.
    ///
    /// This method never modifies GapTracker.
    pub fn open_for_failure(
        &mut self,
        failure: IntegrityFailureIdentity,
    ) -> Result<ClassifiedEpisodeOutcome, IntegrityEpisodeError> {
        if failure.generation != self.generation {
            return Err(IntegrityEpisodeError::ObsoleteGeneration);
        }

        if failure.failure_id == 0 {
            return Err(IntegrityEpisodeError::InvalidFailureIdentity);
        }

        if let Some(active) = self.active {
            let Some(origin) = active.initiating_failure else {
                return Err(IntegrityEpisodeError::UnclassifiedEpisodeActive);
            };

            if origin == failure {
                return Ok(ClassifiedEpisodeOutcome::Existing(active.token));
            }

            return Err(IntegrityEpisodeError::DistinctFailureWhileActive {
                active: origin,
                incoming: failure,
            });
        }

        // Preserve the existing episode ID allocation and
        // counter semantics.
        let token = self.open_or_get(failure.generation)?;

        let active = self.active.as_mut().expect("episode was just opened");

        active.initiating_failure = Some(failure);

        Ok(ClassifiedEpisodeOutcome::Opened(token))
    }

    /// Read the initiating failure of the active episode.
    pub fn active_failure(&self) -> Option<IntegrityFailureIdentity> {
        self.active.and_then(|episode| episode.initiating_failure)
    }

    /// Read the number of failed recovery attempts
    /// associated with the current episode.
    pub fn active_recovery_failures_total(&self) -> Option<u64> {
        self.active.map(|episode| episode.recovery_failures_total)
    }
    // ========================================================
    // BEGIN EPISODE
    // ========================================================

    /// Open a recovery episode for the current generation.
    ///
    /// Repeated calls while an episode is active return
    /// the same token, preventing each failed recovery
    /// attempt from becoming a new incident.
    ///
    /// A different generation is rejected.
    pub fn open_or_get(
        &mut self,
        generation: u64,
    ) -> Result<IntegrityEpisodeToken, IntegrityEpisodeError> {
        if generation != self.generation {
            return Err(IntegrityEpisodeError::ObsoleteGeneration);
        }
        // Legacy callers may still use generation-only
        // episode grouping for existing tests.
        //
        // A classified episode must never be silently
        // retrieved through that weaker API.
        if self
            .active
            .is_some_and(|episode| episode.initiating_failure.is_some())
        {
            return Err(IntegrityEpisodeError::ClassifiedEpisodeRequiresFailureIdentity);
        }
        if let Some(active) = self.active {
            return Ok(active.token);
        }

        // Reserve the next ID before mutating state.
        let next_id = self
            .next_episode_id
            .checked_add(1)
            .ok_or(IntegrityEpisodeError::EpisodeIdExhausted)?;

        let token = IntegrityEpisodeToken {
            generation,
            episode_id: self.next_episode_id,
        };

        self.next_episode_id = next_id;

        self.active = Some(ActiveIntegrityEpisode {
            token,
            gap_id: None,
            initiating_failure: None,
            last_recovery_attempt: None,
            recovery_failures_total: 0,
        });

        self.episodes_opened_total = self.episodes_opened_total.saturating_add(1);

        Ok(token)
    }
    // ========================================================
    // RECOVERY ATTEMPT FAILURE
    // ========================================================

    /// Record a failed recovery attempt belonging to an
    /// existing classified episode.
    ///
    /// Attempt IDs must increase monotonically within
    /// the episode.
    ///
    /// This operation does not:
    /// - Create a new episode.
    /// - Record a confirmed gap.
    /// - Close the episode.
    /// - Advance stream generation.
    ///
    /// The Recovery Coordinator remains responsible
    /// for executing retries.
    pub fn record_recovery_failure(
        &mut self,
        token: IntegrityEpisodeToken,
        recovery_attempt_id: u64,
    ) -> Result<(), IntegrityEpisodeError> {
        self.require_active(token)?;

        if recovery_attempt_id == 0 {
            return Err(IntegrityEpisodeError::InvalidRecoveryAttempt);
        }

        let active = self.active.as_mut().expect("active episode validated");

        if active.initiating_failure.is_none() {
            return Err(IntegrityEpisodeError::UnclassifiedEpisodeActive);
        }

        if active
            .last_recovery_attempt
            .is_some_and(|previous| recovery_attempt_id <= previous)
        {
            return Err(IntegrityEpisodeError::InvalidRecoveryAttempt);
        }

        let next_total = active
            .recovery_failures_total
            .checked_add(1)
            .ok_or(IntegrityEpisodeError::InvalidRecoveryAttempt)?;

        active.last_recovery_attempt = Some(recovery_attempt_id);

        active.recovery_failures_total = next_total;

        Ok(())
    }
    // ========================================================
    // RECORD CONFIRMED GAP
    // ========================================================

    /// Record a confirmed acquisition discontinuity.
    ///
    /// Exactly one GapTracker record is allowed per
    /// active episode.
    ///
    /// Repeated calls return the original gap ID.
    ///
    /// The caller must provide the same authoritative
    /// per-generation GapTracker used by the source runtime.
    ///
    /// Invalid evidence does not mark the episode recorded,
    /// so a corrected report can be submitted later.
    pub fn report_confirmed(
        &mut self,
        token: IntegrityEpisodeToken,
        tracker: &mut GapTracker,
        detected_at_ns: i64,
        incident: ConfirmedIntegrityIncident,
    ) -> Result<EpisodeReportOutcome, IntegrityEpisodeError> {
        self.require_active(token)?;
        // Confirmed acquisition-gap evidence must match
        // the classified initiating failure.
        //
        // Unverified trade coverage, source provenance,
        // and publication delivery failures are not
        // silently promoted to confirmed acquisition loss.
        if let Some(kind) = self
            .active
            .and_then(|episode| episode.initiating_failure.map(|origin| origin.kind))
        {
            if !incident_matches_failure(kind, &incident) {
                return Err(IntegrityEpisodeError::IncidentKindMismatch);
            }
        }
        // Validate tracker ownership even when the episode
        // has already been recorded.
        //
        // Otherwise an incorrectly routed duplicate report
        // could appear to succeed against another tracker.
        if tracker.stream_id() != self.stream_id || tracker.generation() != self.generation {
            return Err(IntegrityEpisodeError::WrongTracker);
        }

        if let Some(gap_id) = self.active.and_then(|episode| episode.gap_id) {
            return Ok(EpisodeReportOutcome::AlreadyRecorded { gap_id });
        }

        let gap_id = IntegrityIncidentReporter::record(
            tracker,
            IncidentOrigin {
                stream_id: &self.stream_id,
                generation: self.generation,
            },
            detected_at_ns,
            incident,
        )
        .map_err(IntegrityEpisodeError::IncidentReporting)?;

        // The reporter succeeded; establish ownership.
        //
        // The active episode cannot change during this
        // synchronous operation under exclusive &mut self.
        let active = self
            .active
            .as_mut()
            .expect("active episode validated before reporting");

        active.gap_id = Some(gap_id);

        Ok(EpisodeReportOutcome::Recorded { gap_id })
    }

    // ========================================================
    // CLOSE EPISODE
    // ========================================================

    /// Close the active recovery episode explicitly.
    ///
    /// The recovery coordinator decides when current
    /// acquisition state has been restored or abandoned.
    ///
    /// An episode with no confirmed gap may be closed;
    /// it is then counted as an unverified episode.
    ///
    /// Closure never alters the original GapTracker record.
    pub fn close(
        &mut self,
        token: IntegrityEpisodeToken,
        resolution: IntegrityEpisodeResolution,
    ) -> Result<ClosedIntegrityEpisode, IntegrityEpisodeError> {
        self.require_active(token)?;

        let active = self
            .active
            .take()
            .expect("active episode validated before closure");

        self.episodes_closed_total = self.episodes_closed_total.saturating_add(1);

        if active.gap_id.is_none() {
            self.unverified_episodes_total = self.unverified_episodes_total.saturating_add(1);
        }

        Ok(ClosedIntegrityEpisode {
            token,
            gap_id: active.gap_id,
            resolution,
        })
    }

    // ========================================================
    // GENERATION TRANSITION
    // ========================================================

    /// Advance to a newer authoritative stream generation.
    ///
    /// An existing episode must first be explicitly closed.
    ///
    /// This prevents a generation transition from silently
    /// discarding unresolved incident ownership.
    ///
    /// The source runtime is responsible for retaining or
    /// persisting the old generation's GapTracker history
    /// according to the application's retention contract.
    ///
    /// The lifecycle's episode ID counter is NOT reset.
    pub fn begin_generation(&mut self, generation: u64) -> Result<(), IntegrityEpisodeError> {
        if generation <= self.generation {
            return Err(IntegrityEpisodeError::GenerationMustIncrease);
        }

        if self.active.is_some() {
            return Err(IntegrityEpisodeError::ActiveEpisodeMustBeClosed);
        }

        self.generation = generation;

        Ok(())
    }

    // ========================================================
    // INTERNAL VALIDATION
    // ========================================================

    fn require_active(&self, token: IntegrityEpisodeToken) -> Result<(), IntegrityEpisodeError> {
        if token.generation != self.generation {
            return Err(IntegrityEpisodeError::ObsoleteGeneration);
        }

        let Some(active) = self.active else {
            return Err(IntegrityEpisodeError::NoActiveEpisode);
        };

        if active.token != token {
            return Err(IntegrityEpisodeError::WrongEpisode);
        }

        Ok(())
    }
}
fn incident_matches_failure(
    kind: IntegrityFailureKind,
    incident: &ConfirmedIntegrityIncident,
) -> bool {
    matches!(
        (kind, incident),
        (
            IntegrityFailureKind::SequenceDiscontinuity,
            ConfirmedIntegrityIncident::SequenceDiscontinuity { .. }
        ) | (
            IntegrityFailureKind::ConnectionInterruption,
            ConfirmedIntegrityIncident::ConnectionInterruption { .. }
        ) | (
            IntegrityFailureKind::DataInterruption,
            ConfirmedIntegrityIncident::DataInterruption { .. }
        )
    )
}
#[cfg(test)]
mod tests {
    use super::*;

    use crate::live::integrity::model::GapReason;

    const STREAM: &str = "binance:spot:BTCUSDT:depth";
    const START: i64 = 1_000_000_000;

    fn lifecycle() -> IntegrityIncidentLifecycle {
        IntegrityIncidentLifecycle::new(STREAM, 10).unwrap()
    }

    fn tracker(generation: u64) -> GapTracker {
        GapTracker::new(STREAM, generation, START, 100).unwrap()
    }

    fn confirmed_gap() -> ConfirmedIntegrityIncident {
        ConfirmedIntegrityIncident::SequenceDiscontinuity {
            sequence_before: Some(100),
            sequence_after: Some(105),
            missing_events_exact: None,
        }
    }

    #[test]
    fn opens_one_episode_for_repeated_failures() {
        let mut lifecycle = lifecycle();

        let first = lifecycle.open_or_get(10).unwrap();
        let second = lifecycle.open_or_get(10).unwrap();
        let third = lifecycle.open_or_get(10).unwrap();

        assert_eq!(first, second);
        assert_eq!(second, third);

        assert_eq!(lifecycle.episodes_opened_total(), 1);
        assert!(lifecycle.has_active_episode());
    }

    #[test]
    fn confirmed_incident_is_recorded_only_once() {
        let mut lifecycle = lifecycle();
        let mut tracker = tracker(10);

        let episode = lifecycle.open_or_get(10).unwrap();

        let first = lifecycle
            .report_confirmed(episode, &mut tracker, START + 100, confirmed_gap())
            .unwrap();

        assert_eq!(first, EpisodeReportOutcome::Recorded { gap_id: 1 },);

        // A repeated failure notification belongs to
        // the same recovery episode.
        let second = lifecycle
            .report_confirmed(episode, &mut tracker, START + 200, confirmed_gap())
            .unwrap();

        assert_eq!(second, EpisodeReportOutcome::AlreadyRecorded { gap_id: 1 },);

        assert_eq!(tracker.len(), 1);
        assert_eq!(lifecycle.active_gap_id(), Some(1));
    }

    #[test]
    fn recovery_retries_do_not_create_new_episodes() {
        let mut lifecycle = lifecycle();
        let mut tracker = tracker(10);

        let episode = lifecycle.open_or_get(10).unwrap();

        lifecycle
            .report_confirmed(episode, &mut tracker, START + 100, confirmed_gap())
            .unwrap();

        // Three failed recovery attempts.
        //
        // Opening an episode again returns the existing
        // identity rather than creating another gap.
        for _ in 0..3 {
            assert_eq!(lifecycle.open_or_get(10).unwrap(), episode,);
        }

        assert_eq!(tracker.len(), 1);
        assert_eq!(lifecycle.episodes_opened_total(), 1);
    }

    #[test]
    fn failed_incident_validation_does_not_consume_episode() {
        let mut lifecycle = lifecycle();
        let mut tracker = tracker(10);

        let episode = lifecycle.open_or_get(10).unwrap();

        let invalid = ConfirmedIntegrityIncident::SequenceDiscontinuity {
            sequence_before: None,
            sequence_after: None,
            missing_events_exact: None,
        };

        let result = lifecycle.report_confirmed(episode, &mut tracker, START + 100, invalid);

        assert!(matches!(
            result,
            Err(IntegrityEpisodeError::IncidentReporting(
                IncidentReportingError::InvalidEvidence(_)
            ))
        ));

        assert_eq!(tracker.len(), 0);
        assert_eq!(lifecycle.active_gap_id(), None);

        // Corrected evidence is still permitted.
        let corrected = lifecycle
            .report_confirmed(episode, &mut tracker, START + 200, confirmed_gap())
            .unwrap();

        assert_eq!(corrected, EpisodeReportOutcome::Recorded { gap_id: 1 },);

        assert_eq!(tracker.len(), 1);
    }

    #[test]
    fn wrong_tracker_cannot_receive_episode_report() {
        let mut lifecycle = lifecycle();
        let mut wrong_tracker = tracker(11);

        let episode = lifecycle.open_or_get(10).unwrap();

        let result =
            lifecycle.report_confirmed(episode, &mut wrong_tracker, START + 100, confirmed_gap());

        assert_eq!(result, Err(IntegrityEpisodeError::WrongTracker),);

        assert!(wrong_tracker.is_empty());
        assert_eq!(lifecycle.active_gap_id(), None);
    }

    #[test]
    fn closing_episode_preserves_gap_record() {
        let mut lifecycle = lifecycle();
        let mut tracker = tracker(10);

        let episode = lifecycle.open_or_get(10).unwrap();

        lifecycle
            .report_confirmed(episode, &mut tracker, START + 100, confirmed_gap())
            .unwrap();

        let closed = lifecycle
            .close(episode, IntegrityEpisodeResolution::CurrentStateRestored)
            .unwrap();

        assert_eq!(closed.token, episode);
        assert_eq!(closed.gap_id, Some(1));

        assert_eq!(
            closed.resolution,
            IntegrityEpisodeResolution::CurrentStateRestored,
        );

        assert!(!lifecycle.has_active_episode());

        // Recovery completion does not erase the
        // previously recorded acquisition gap.
        assert_eq!(tracker.len(), 1);

        assert_eq!(
            tracker.records().next().unwrap().reason,
            GapReason::SequenceGap,
        );
    }

    #[test]
    fn unconfirmed_episode_is_not_fabricated_as_gap() {
        let mut lifecycle = lifecycle();
        let tracker = tracker(10);

        let episode = lifecycle.open_or_get(10).unwrap();

        // Example: a suspicious numeric trade-ID
        // discontinuity that was never verified as
        // an acquisition gap.
        //
        // No confirmed gap is submitted.
        let closed = lifecycle
            .close(episode, IntegrityEpisodeResolution::AbandonedUnverified)
            .unwrap();

        assert_eq!(closed.gap_id, None);
        assert!(tracker.is_empty());

        assert_eq!(lifecycle.unverified_episodes_total(), 1,);

        assert_eq!(lifecycle.episodes_closed_total(), 1);
    }

    #[test]
    fn active_episode_blocks_generation_change() {
        let mut lifecycle = lifecycle();

        let episode = lifecycle.open_or_get(10).unwrap();

        assert_eq!(
            lifecycle.begin_generation(11),
            Err(IntegrityEpisodeError::ActiveEpisodeMustBeClosed),
        );

        assert_eq!(lifecycle.generation(), 10);
        assert_eq!(lifecycle.active_episode(), Some(episode));
    }

    #[test]
    fn closed_episode_allows_new_generation() {
        let mut lifecycle = lifecycle();

        let old_episode = lifecycle.open_or_get(10).unwrap();

        lifecycle
            .close(old_episode, IntegrityEpisodeResolution::AbandonedUnverified)
            .unwrap();

        lifecycle.begin_generation(11).unwrap();

        assert_eq!(lifecycle.generation(), 11);
        assert!(!lifecycle.has_active_episode());

        let new_episode = lifecycle.open_or_get(11).unwrap();

        assert_eq!(new_episode.generation, 11);

        assert!(new_episode.episode_id > old_episode.episode_id);
    }

    #[test]
    fn old_episode_cannot_record_into_new_generation() {
        let mut lifecycle = lifecycle();

        let old_episode = lifecycle.open_or_get(10).unwrap();

        lifecycle
            .close(old_episode, IntegrityEpisodeResolution::AbandonedUnverified)
            .unwrap();

        lifecycle.begin_generation(11).unwrap();

        let mut new_tracker = tracker(11);

        let result =
            lifecycle.report_confirmed(old_episode, &mut new_tracker, START + 100, confirmed_gap());

        assert_eq!(result, Err(IntegrityEpisodeError::ObsoleteGeneration),);

        assert!(new_tracker.is_empty());
    }

    #[test]
    fn closed_episode_cannot_be_reported_again() {
        let mut lifecycle = lifecycle();
        let mut tracker = tracker(10);

        let episode = lifecycle.open_or_get(10).unwrap();

        lifecycle
            .close(episode, IntegrityEpisodeResolution::AbandonedUnverified)
            .unwrap();

        assert_eq!(
            lifecycle.report_confirmed(episode, &mut tracker, START + 100, confirmed_gap(),),
            Err(IntegrityEpisodeError::NoActiveEpisode),
        );

        assert!(tracker.is_empty());
    }

    #[test]
    fn new_episode_is_distinct_from_previous_closed_episode() {
        let mut lifecycle = lifecycle();
        let mut tracker = tracker(10);

        let first = lifecycle.open_or_get(10).unwrap();

        lifecycle
            .report_confirmed(first, &mut tracker, START + 100, confirmed_gap())
            .unwrap();

        lifecycle
            .close(first, IntegrityEpisodeResolution::CurrentStateRestored)
            .unwrap();

        let second = lifecycle.open_or_get(10).unwrap();

        assert_ne!(first, second);

        // A genuinely new interruption, occurring
        // after the old episode was resolved.
        let report = lifecycle
            .report_confirmed(
                second,
                &mut tracker,
                START + 200,
                ConfirmedIntegrityIncident::ConnectionInterruption {
                    sequence_before: Some(105),
                    sequence_after: None,
                },
            )
            .unwrap();

        assert_eq!(report, EpisodeReportOutcome::Recorded { gap_id: 2 },);

        assert_eq!(tracker.len(), 2);
        assert_eq!(lifecycle.episodes_opened_total(), 2);
    }

    #[test]
    fn stale_episode_cannot_close_current_episode() {
        let mut lifecycle = lifecycle();

        let first = lifecycle.open_or_get(10).unwrap();

        lifecycle
            .close(first, IntegrityEpisodeResolution::CurrentStateRestored)
            .unwrap();

        let second = lifecycle.open_or_get(10).unwrap();

        assert_eq!(
            lifecycle.close(first, IntegrityEpisodeResolution::CurrentStateRestored,),
            Err(IntegrityEpisodeError::WrongEpisode),
        );

        assert_eq!(lifecycle.active_episode(), Some(second));
    }

    #[test]
    fn generation_must_increase_monotonically() {
        let mut lifecycle = lifecycle();

        assert_eq!(
            lifecycle.begin_generation(10),
            Err(IntegrityEpisodeError::GenerationMustIncrease),
        );

        assert_eq!(
            lifecycle.begin_generation(9),
            Err(IntegrityEpisodeError::GenerationMustIncrease),
        );

        assert_eq!(lifecycle.generation(), 10);
    }
    // ============================================================
    // N5N.7.7c — FAILURE CLASSIFICATION
    // ============================================================

    fn failure(id: u64, kind: IntegrityFailureKind) -> IntegrityFailureIdentity {
        IntegrityFailureIdentity {
            generation: 10,
            failure_id: id,
            kind,
        }
    }

    #[test]
    fn classified_failure_opens_one_episode() {
        let mut lifecycle = lifecycle();

        let origin = failure(1, IntegrityFailureKind::SequenceDiscontinuity);

        let first = lifecycle.open_for_failure(origin).unwrap();

        let ClassifiedEpisodeOutcome::Opened(token) = first else {
            panic!("expected new classified episode");
        };

        assert_eq!(lifecycle.active_failure(), Some(origin),);

        let repeated = lifecycle.open_for_failure(origin).unwrap();

        assert_eq!(repeated, ClassifiedEpisodeOutcome::Existing(token),);

        assert_eq!(lifecycle.episodes_opened_total(), 1);
    }

    #[test]
    fn independent_failure_cannot_be_silently_deduplicated() {
        let mut lifecycle = lifecycle();

        let first = failure(1, IntegrityFailureKind::SequenceDiscontinuity);

        let second = failure(2, IntegrityFailureKind::ConnectionInterruption);

        lifecycle.open_for_failure(first).unwrap();

        assert_eq!(
            lifecycle.open_for_failure(second),
            Err(IntegrityEpisodeError::DistinctFailureWhileActive {
                active: first,
                incoming: second,
            }),
        );

        assert_eq!(lifecycle.active_failure(), Some(first),);

        assert_eq!(lifecycle.episodes_opened_total(), 1);
    }

    #[test]
    fn different_ids_of_same_kind_are_distinct_failures() {
        let mut lifecycle = lifecycle();

        let first = failure(100, IntegrityFailureKind::SequenceDiscontinuity);

        let second = failure(101, IntegrityFailureKind::SequenceDiscontinuity);

        lifecycle.open_for_failure(first).unwrap();

        assert!(matches!(
            lifecycle.open_for_failure(second),
            Err(IntegrityEpisodeError::DistinctFailureWhileActive { .. })
        ));
    }

    #[test]
    fn legacy_open_cannot_bypass_classified_identity() {
        let mut lifecycle = lifecycle();

        lifecycle
            .open_for_failure(failure(1, IntegrityFailureKind::ConnectionInterruption))
            .unwrap();

        assert_eq!(
            lifecycle.open_or_get(10),
            Err(IntegrityEpisodeError::ClassifiedEpisodeRequiresFailureIdentity),
        );
    }

    #[test]
    fn retry_failures_do_not_create_additional_gap_records() {
        let mut lifecycle = lifecycle();
        let mut tracker = tracker(10);

        let origin = failure(1, IntegrityFailureKind::SequenceDiscontinuity);

        let ClassifiedEpisodeOutcome::Opened(token) = lifecycle.open_for_failure(origin).unwrap()
        else {
            panic!("expected new episode");
        };

        lifecycle
            .report_confirmed(token, &mut tracker, START + 100, confirmed_gap())
            .unwrap();

        for attempt in 1..=3 {
            lifecycle.record_recovery_failure(token, attempt).unwrap();
        }

        assert_eq!(lifecycle.active_recovery_failures_total(), Some(3),);

        assert_eq!(lifecycle.episodes_opened_total(), 1);
        assert_eq!(tracker.len(), 1);
    }

    #[test]
    fn repeated_recovery_attempt_id_is_rejected() {
        let mut lifecycle = lifecycle();

        let ClassifiedEpisodeOutcome::Opened(token) = lifecycle
            .open_for_failure(failure(1, IntegrityFailureKind::ConnectionInterruption))
            .unwrap()
        else {
            panic!("expected new episode");
        };

        lifecycle.record_recovery_failure(token, 10).unwrap();

        assert_eq!(
            lifecycle.record_recovery_failure(token, 10),
            Err(IntegrityEpisodeError::InvalidRecoveryAttempt),
        );

        assert_eq!(
            lifecycle.record_recovery_failure(token, 9),
            Err(IntegrityEpisodeError::InvalidRecoveryAttempt),
        );

        assert_eq!(lifecycle.active_recovery_failures_total(), Some(1),);
    }

    #[test]
    fn unverified_trade_failure_cannot_create_confirmed_gap() {
        let mut lifecycle = lifecycle();
        let mut tracker = tracker(10);

        let ClassifiedEpisodeOutcome::Opened(token) = lifecycle
            .open_for_failure(failure(1, IntegrityFailureKind::TradeCoverageUnverified))
            .unwrap()
        else {
            panic!("expected new episode");
        };

        assert_eq!(
            lifecycle.report_confirmed(token, &mut tracker, START + 100, confirmed_gap(),),
            Err(IntegrityEpisodeError::IncidentKindMismatch),
        );

        assert!(tracker.is_empty());

        let closed = lifecycle
            .close(token, IntegrityEpisodeResolution::AbandonedUnverified)
            .unwrap();

        assert_eq!(closed.gap_id, None);
        assert_eq!(lifecycle.unverified_episodes_total(), 1);
    }

    #[test]
    fn publication_failure_is_not_exchange_acquisition_gap() {
        let mut lifecycle = lifecycle();
        let mut tracker = tracker(10);

        let ClassifiedEpisodeOutcome::Opened(token) = lifecycle
            .open_for_failure(failure(1, IntegrityFailureKind::PublicationDelivery))
            .unwrap()
        else {
            panic!("expected new episode");
        };

        let result = lifecycle.report_confirmed(
            token,
            &mut tracker,
            START + 100,
            ConfirmedIntegrityIncident::DataInterruption {
                sequence_before: None,
                sequence_after: None,
                missing_events_exact: None,
            },
        );

        assert_eq!(result, Err(IntegrityEpisodeError::IncidentKindMismatch),);

        assert!(tracker.is_empty());
    }

    #[test]
    fn matching_classified_incident_records_normally() {
        let mut lifecycle = lifecycle();
        let mut tracker = tracker(10);

        let ClassifiedEpisodeOutcome::Opened(token) = lifecycle
            .open_for_failure(failure(1, IntegrityFailureKind::SequenceDiscontinuity))
            .unwrap()
        else {
            panic!("expected new episode");
        };

        let result = lifecycle
            .report_confirmed(token, &mut tracker, START + 100, confirmed_gap())
            .unwrap();

        assert_eq!(result, EpisodeReportOutcome::Recorded { gap_id: 1 },);

        assert_eq!(tracker.len(), 1);
    }

    #[test]
    fn obsolete_failure_cannot_enter_new_generation() {
        let mut lifecycle = lifecycle();

        lifecycle.begin_generation(11).unwrap();

        let old = failure(1, IntegrityFailureKind::ConnectionInterruption);

        assert_eq!(
            lifecycle.open_for_failure(old),
            Err(IntegrityEpisodeError::ObsoleteGeneration),
        );

        assert!(!lifecycle.has_active_episode());
    }
}
