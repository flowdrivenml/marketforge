use std::collections::HashSet;

use crate::{
    live::infrastructure::normalization::{
        NormalizationSource, NormalizedTrades, TradeRecordSemantics,
    },
    shared::domain::canonical::Trade,
};

use crate::live::integrity::trades::compiler::{CompiledTradeIntegrityPlan, TradeCoverageAction};
use crate::live::integrity::trades::evaluator::{
    TradeIntegrityBaseline, TradeIntegrityEvaluationError, TradeIntegrityEvaluator,
    TradeIntegrityObservation,
};
use crate::live::integrity::trades::identity::{
    TradeIdentityDecision, TradeIdentityError, TradeIdentityLimits, TradeIdentityTracker,
};

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TradeControllerError {
    WrongStream,

    UnsupportedSource,

    UnsupportedSemantics,

    MissingTradeId { index: usize },

    InvalidIdentity(TradeIdentityError),

    InvalidEvaluation(TradeIntegrityEvaluationError),
}

// ============================================================
// ACCEPTED BATCH
// ============================================================

/// Individual canonical trades accepted by integrity.
///
/// Only identities classified as new within the retained
/// deduplication window appear in `trades`.
///
/// Observations are evidence of uncertain continuity,
/// not confirmed counts of missing executions.
#[derive(Debug)]
pub struct AcceptedTradeBatch {
    pub trades: Vec<Trade>,

    pub received_records: usize,

    pub duplicate_records: usize,

    /// True when the current bounded native-ID history
    /// has evicted previously observed identities.
    pub identity_history_truncated: bool,

    /// True when the controller cannot establish
    /// uninterrupted historical coverage.
    ///
    /// This includes stream-generation transitions
    /// and bounded identity-history eviction.
    ///
    /// False does not establish global exactly-once
    /// delivery or complete exchange history.
    pub coverage_history_unverified: bool,

    /// Ordering or coverage evidence accepted under
    /// the configured ReportUnverifiedCoverage policy.
    pub observations: Vec<TradeIntegrityObservation>,

    pub baseline: TradeIntegrityBaseline,
}

// ============================================================
// PROCESSING OUTCOMES
// ============================================================

#[derive(Debug)]
pub enum TradeControllerOutcome {
    /// The batch has been accepted and committed.
    Accepted(AcceptedTradeBatch),

    /// This batch triggered a new coverage hold.
    ///
    /// Neither its identities nor its ordering baseline
    /// were committed.
    ///
    /// No trades from this batch may be published as
    /// accepted market data.
    CoverageReviewRequired {
        observations: Vec<TradeIntegrityObservation>,
    },

    /// A previous batch already triggered a coverage hold.
    ///
    /// No validation, deduplication, or baseline mutation
    /// was performed for the incoming batch.
    ///
    /// The caller must not silently discard these trades
    /// while claiming continuous historical coverage.
    CoverageReviewPending { generation: u64 },

    /// Message belongs to an obsolete stream generation.
    ObsoleteGeneration,

    /// Batch validation failed without committing state.
    Rejected(TradeControllerError),
}

// ============================================================
// LIVE TRADE CONTROLLER
// ============================================================

/// Per-source-stream integrity owner for individual trades.
///
/// Owns:
/// - Bounded native trade identity history.
/// - Accepted ordering baseline.
/// - Runtime generation.
/// - Compiled trade integrity policy.
///
/// Does not own:
/// - Normalization.
/// - Network acquisition.
/// - Recovery execution.
/// - Gap incident history.
/// - Publication or recording.
///
/// The source runtime must serialize processing calls.
///
///
/// ============================================================
// TRADE RECOVERY PREFLIGHT
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TradeRecoveryPreflightError {
    ObsoleteGeneration,
    NoCoverageHold,
    UnsupportedIdentityPolicy,
    BaselineMismatch,
    EmptyCandidate,
    CandidateTooLarge,
    WrongStream,
    MissingTradeId,
    InvalidNumericTradeId,
    NonConsecutiveTradeIds,
    PreviouslyAcceptedIdentity,
    MissingTriggeringTrade,
    ObsoleteEpoch,
    SourceQuarantined,
    WrongExchange,
    WrongInstrument,
    WrongSymbol,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TradeRecoveryCommitError {
    Preflight(TradeRecoveryPreflightError),
    Evaluation(TradeIntegrityEvaluationError),
    Publication(String),
    IdentityCommit(TradeIdentityError),
    UnexpectedCoverageObservation,
}
/// A candidate validated against the current trade controller.
///
/// This is NOT a commitment or publication authorization.
///
/// In particular, successful numeric validation does not
/// prove that the REST endpoint returned complete exchange
/// execution history.
#[derive(Debug)]
pub struct TradeRecoveryPreflight {
    generation: u64,
    baseline: TradeIntegrityBaseline,
    trades: Vec<Trade>,
}

impl TradeRecoveryPreflight {
    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn baseline(&self) -> TradeIntegrityBaseline {
        self.baseline
    }

    pub fn trades(&self) -> &[Trade] {
        &self.trades
    }

    pub fn record_count(&self) -> usize {
        self.trades.len()
    }
}
#[derive(Debug)]
pub struct LiveTradeController {
    stream_id: String,

    generation: u64,

    limits: TradeIdentityLimits,

    plan: CompiledTradeIntegrityPlan,

    identities: TradeIdentityTracker,

    baseline: TradeIntegrityBaseline,

    /// Unresolved coverage review.
    ///
    /// While present, process() must not admit any
    /// new batch for this generation.
    coverage_hold: Option<TradeCoverageHold>,

    /// Whether the controller's local identity and
    /// ordering history crossed an unverified
    /// generation boundary.
    generation_history_discontinuous: bool,
    /// The canonical batch that triggered the current coverage hold.
    ///
    /// Retained only while that hold exists.
    ///
    /// Its size is bounded by TradeIdentityLimits::max_batch_records.
    /// These trades have not been accepted or published.
    coverage_triggering_trades: Option<Vec<Trade>>,
}

impl LiveTradeController {
    // ========================================================
    // CONSTRUCTION
    // ========================================================

    /// Construct a controller with a validated policy.
    ///
    /// This API requires the caller to supply the compiled
    /// trade-integrity configuration for the source stream.
    pub(in crate::live::integrity) fn new(
        stream_id: impl Into<String>,
        generation: u64,
        limits: TradeIdentityLimits,
        plan: CompiledTradeIntegrityPlan,
    ) -> Result<Self, String> {
        if !plan.supports_individual_trades {
            return Err("trade controller requires an individual-trade plan".into());
        }

        if !plan.detect_duplicates {
            return Err("trade controller requires native identity \
                 duplicate detection"
                .into());
        }

        let stream_id = stream_id.into();

        let identities = TradeIdentityTracker::new(stream_id.clone(), generation, limits)
            .map_err(|error| format!("invalid trade identity tracker: {error:?}"))?;

        Ok(Self {
            stream_id,
            generation,
            limits,
            plan,
            identities,
            baseline: TradeIntegrityBaseline::default(),
            generation_history_discontinuous: false,
            coverage_hold: None,
            coverage_triggering_trades: None,
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

    pub fn identities(&self) -> &TradeIdentityTracker {
        &self.identities
    }

    pub fn plan(&self) -> &CompiledTradeIntegrityPlan {
        &self.plan
    }

    pub fn baseline(&self) -> TradeIntegrityBaseline {
        self.baseline
    }

    pub fn generation_history_discontinuous(&self) -> bool {
        self.generation_history_discontinuous
    }
    /// Whether normal trade acceptance is suspended.
    pub fn is_coverage_review_pending(&self) -> bool {
        self.coverage_hold.is_some()
    }

    /// Read-only evidence responsible for the current hold.
    pub fn coverage_hold(&self) -> Option<&TradeCoverageHold> {
        self.coverage_hold.as_ref()
    }
    /// Canonical trades from the batch that triggered
    /// the unresolved coverage hold.
    ///
    /// These records are not committed to identity history
    /// and must not be published as accepted trades.
    ///
    /// None means there is no retained triggering batch.
    pub fn coverage_triggering_trades(&self) -> Option<&[Trade]> {
        self.coverage_triggering_trades.as_deref()
    }
    /// Validate a numerically reconciled recovery candidate
    /// against the authoritative held generation.
    ///
    /// This operation is read-only:
    ///
    /// - Does not insert trade identities.
    /// - Does not advance the accepted baseline.
    /// - Does not clear the coverage hold.
    /// - Does not authorize publication.
    ///
    /// Source provenance, REST request identity, full acquisition
    /// fencing, and exchange-specific coverage guarantees must
    /// be validated by the surrounding recovery workflow.
    pub(in crate::live::integrity) fn preflight_numeric_recovery(
        &self,
        generation: u64,
        expected_baseline: TradeIntegrityBaseline,
        candidate: &[Trade],
    ) -> Result<TradeRecoveryPreflight, TradeRecoveryPreflightError> {
        use TradeRecoveryPreflightError as Error;

        // --------------------------------------------------------
        // GENERATION AND HOLD
        // --------------------------------------------------------

        if generation != self.generation {
            return Err(Error::ObsoleteGeneration);
        }

        let hold = self.coverage_hold.as_ref().ok_or(Error::NoCoverageHold)?;

        if hold.generation != generation {
            return Err(Error::ObsoleteGeneration);
        }

        if hold.baseline != expected_baseline || self.baseline != expected_baseline {
            return Err(Error::BaselineMismatch);
        }

        // --------------------------------------------------------
        // SUPPORTED RECOVERY POLICY
        // --------------------------------------------------------

        if self.plan.identity_kind != super::compiler::TradeIdentityKind::Numeric
            || !self.plan.detect_id_gaps
            || self.plan.coverage_action != TradeCoverageAction::RequestRecovery
        {
            return Err(Error::UnsupportedIdentityPolicy);
        }

        // --------------------------------------------------------
        // BOUNDED INPUT
        // --------------------------------------------------------

        if candidate.is_empty() {
            return Err(Error::EmptyCandidate);
        }

        let limits = self.identities.limits();

        if candidate.len() > limits.max_ids || candidate.len() > limits.max_batch_records {
            return Err(Error::CandidateTooLarge);
        }

        // --------------------------------------------------------
        // NUMERIC CONTINUITY AND IDENTITY VALIDATION
        // --------------------------------------------------------

        let mut previous = expected_baseline
            .numeric_trade_id
            .ok_or(Error::BaselineMismatch)?;

        let mut candidate_ids = HashSet::with_capacity(candidate.len());

        for trade in candidate {
            if trade.envelope.stream_id != self.stream_id {
                return Err(Error::WrongStream);
            }

            let id = trade.trade_id.as_deref().ok_or(Error::MissingTradeId)?;

            if id.is_empty() || id.len() > limits.max_id_bytes {
                return Err(Error::InvalidNumericTradeId);
            }

            let numeric = id
                .parse::<u64>()
                .map_err(|_| Error::InvalidNumericTradeId)?;

            // The candidate must continue the last accepted
            // numeric baseline exactly.
            //
            // This also rejects regressions and repeated IDs.
            if previous.checked_add(1) != Some(numeric) {
                return Err(Error::NonConsecutiveTradeIds);
            }

            if self.identities.contains(id) {
                return Err(Error::PreviouslyAcceptedIdentity);
            }

            candidate_ids.insert(numeric);
            previous = numeric;
        }

        // --------------------------------------------------------
        // PRESERVED TRIGGERING BATCH
        // --------------------------------------------------------

        let triggering = self
            .coverage_triggering_trades
            .as_deref()
            .ok_or(Error::MissingTriggeringTrade)?;

        if triggering.is_empty() {
            return Err(Error::MissingTriggeringTrade);
        }

        // Every triggering trade beyond the accepted baseline
        // must appear in the candidate.
        //
        // The source reconciliation layer separately checks
        // conflicting canonical contents across REST and WS.
        let baseline_id = expected_baseline
            .numeric_trade_id
            .ok_or(Error::BaselineMismatch)?;

        for trade in triggering {
            let id = trade
                .trade_id
                .as_deref()
                .ok_or(Error::MissingTradeId)?
                .parse::<u64>()
                .map_err(|_| Error::InvalidNumericTradeId)?;

            if id > baseline_id && !candidate_ids.contains(&id) {
                return Err(Error::MissingTriggeringTrade);
            }
        }

        // --------------------------------------------------------
        // READ-ONLY RESULT
        // --------------------------------------------------------

        Ok(TradeRecoveryPreflight {
            generation,
            baseline: expected_baseline,
            trades: candidate.to_vec(),
        })
    }
    /// Complete one bounded numeric trade recovery.
    ///
    /// The caller must have independently validated:
    /// - REST request/response provenance.
    /// - The complete trusted acquisition epoch.
    /// - The exchange-specific recovery coverage policy.
    ///
    /// The publication callback acknowledges synchronous handoff
    /// to the downstream publication boundary, not durable storage.
    ///
    /// A failed callback preserves the coverage hold, triggering
    /// batch, identities, and accepted ordering baseline.
    ///
    /// This method does not establish globally complete trade
    /// history or exactly-once delivery.
    pub(in crate::live::integrity) fn complete_numeric_recovery(
        &mut self,
        prepared: TradeRecoveryPreflight,
        publish: impl FnOnce(&[Trade]) -> Result<(), String>,
    ) -> Result<AcceptedTradeBatch, TradeRecoveryCommitError> {
        use TradeRecoveryCommitError as Error;

        // Revalidate the current authoritative controller state.
        //
        // A preflight is not a permanent authorization. Another
        // lifecycle transition may have made it obsolete.
        self.preflight_numeric_recovery(prepared.generation, prepared.baseline, &prepared.trades)
            .map_err(Error::Preflight)?;

        // Establish the complete proposed ordering transition
        // BEFORE performing the downstream handoff.
        let evaluation =
            TradeIntegrityEvaluator::evaluate(&self.plan, self.baseline, &prepared.trades)
                .map_err(Error::Evaluation)?;

        if evaluation.has_observations() {
            return Err(Error::UnexpectedCoverageObservation);
        }

        // All identities are checked before publication.
        //
        // The existing preflight guarantees nonempty,
        // bounded, valid numeric IDs.
        let ids: Vec<&str> = prepared
            .trades
            .iter()
            .map(|trade| {
                trade
                    .trade_id
                    .as_deref()
                    .expect("preflight validated every trade identity")
            })
            .collect();

        // --------------------------------------------------------
        // PUBLICATION HANDOFF
        // --------------------------------------------------------

        // No authoritative controller mutation has occurred.
        //
        // An error leaves the hold and accepted baseline intact.
        publish(&prepared.trades).map_err(Error::Publication)?;

        // --------------------------------------------------------
        // AUTHORITATIVE COMMIT
        // --------------------------------------------------------

        // All fallible input validation occurred before handoff.
        //
        // This existing identity tracker is the authoritative
        // deduplication owner. Do not construct another tracker.
        // The preflight validated every identity and the batch limits.
        // The single-owner controller has not changed since revalidation.
        //
        // Failure here would be an internal invariant violation AFTER
        // downstream publication, not a recoverable exchange-data error.
        let decisions = self
            .identities
            .observe_batch(&ids)
            .expect("validated trade recovery identity commit must succeed");

        assert!(
            decisions
                .iter()
                .all(|decision| { matches!(decision, TradeIdentityDecision::NewWithinWindow) }),
            "recovery preflight and identity commit diverged"
        );

        let received_records = ids.len();

        self.baseline = evaluation.next_baseline;

        self.coverage_hold = None;
        self.coverage_triggering_trades = None;

        // Numeric reconciliation does not, by itself, prove
        // globally complete exchange execution history.
        //
        // Preserve this uncertainty until an explicit
        // exchange-specific coverage contract can establish
        // a stronger guarantee.
        self.generation_history_discontinuous = true;

        let history_truncated = self.identities.history_truncated();

        Ok(AcceptedTradeBatch {
            trades: prepared.trades,
            received_records: received_records,
            duplicate_records: 0,
            identity_history_truncated: history_truncated,
            coverage_history_unverified: true,
            observations: evaluation.observations,
            baseline: self.baseline,
        })
    }
    /// Whether the controller can currently evaluate new
    /// trade batches for acceptance.
    ///
    /// This reports controller admission eligibility only.
    ///
    /// It does not establish complete historical coverage.
    pub fn can_accept_trades(&self) -> bool {
        self.coverage_hold.is_none()
    }

    // ========================================================
    // GENERATION TRANSITION
    // ========================================================

    /// Start a newer stream generation.
    ///
    /// Old identity history and ordering baselines cannot
    /// automatically be assumed valid in the new generation.
    ///
    /// Cross-generation reconciliation remains the
    /// responsibility of the surrounding recovery workflow.
    pub(in crate::live::integrity) fn begin_generation(
        &mut self,
        generation: u64,
    ) -> Result<(), String> {
        if generation <= self.generation {
            return Err("trade generation must increase monotonically".into());
        }

        let replacement =
            TradeIdentityTracker::new(self.stream_id.clone(), generation, self.limits).map_err(
                |error| {
                    format!(
                        "cannot initialize trade identity generation: \
                 {error:?}"
                    )
                },
            )?;

        // The newer generation starts with a fresh local
        // deduplication window and ordering baseline.
        self.identities = replacement;
        self.generation = generation;

        self.baseline = TradeIntegrityBaseline::default();

        // A hold from an older generation cannot block the
        // newly established acquisition epoch.
        //
        // This does NOT mean the previous coverage failure
        // was successfully reconciled.
        self.coverage_hold = None;
        self.coverage_triggering_trades = None;
        // Any generation transition makes continuity with
        // the preceding generation unverified until an
        // external recovery/coverage process establishes
        // a stronger guarantee.
        self.generation_history_discontinuous = true;

        Ok(())
    }

    // ========================================================
    // BATCH PROCESSING
    // ========================================================

    /// Evaluate and conditionally commit one individual
    /// WebSocket trade batch.
    ///
    /// Acceptance is transactional with respect to the
    /// controller's logical identity and baseline state:
    ///
    /// 1. Validate all input identities.
    /// 2. Preview duplicates without mutation.
    /// 3. Evaluate ordering evidence.
    /// 4. Apply the configured coverage policy.
    /// 5. Commit identity history and baseline.
    ///
    /// The controller does not claim that the resulting
    /// output has been durably persisted or published.
    #[cfg(test)]
    pub(in crate::live::integrity) fn process(
        &mut self,
        generation: u64,
        message: &NormalizedTrades,
    ) -> TradeControllerOutcome {
        self.process_with(generation, message, Ok)
            .expect("infallible integrity-only handoff")
    }

    pub(in crate::live::integrity) fn process_with(
        &mut self,
        generation: u64,
        message: &NormalizedTrades,
        publish: impl FnOnce(TradeControllerOutcome) -> Result<TradeControllerOutcome, String>,
    ) -> Result<TradeControllerOutcome, String> {
        // ----------------------------------------------------
        // GENERATION FENCING
        // ----------------------------------------------------

        if generation != self.generation {
            return Ok(TradeControllerOutcome::ObsoleteGeneration);
        }
        // ----------------------------------------------------
        // PERSISTENT COVERAGE HOLD
        // ----------------------------------------------------

        // Once coverage review is required, no later batch
        // may advance identities or the accepted baseline.
        //
        // This check precedes all further message validation.
        //
        // The source runtime must explicitly transition the
        // stream lifecycle rather than continuing to publish
        // trades after a detected continuity problem.
        if self.coverage_hold.is_some() {
            return Ok(TradeControllerOutcome::CoverageReviewPending {
                generation: self.generation,
            });
        }
        // ----------------------------------------------------
        // SOURCE AND STREAM VALIDATION
        // ----------------------------------------------------

        if message.stream_id() != self.stream_id {
            return Ok(TradeControllerOutcome::Rejected(TradeControllerError::WrongStream));
        }

        if message.source() != NormalizationSource::WebSocket {
            return Ok(TradeControllerOutcome::Rejected(TradeControllerError::UnsupportedSource));
        }

        if message.semantics() != TradeRecordSemantics::Individual {
            return Ok(TradeControllerOutcome::Rejected(TradeControllerError::UnsupportedSemantics));
        }

        // ----------------------------------------------------
        // PREVALIDATE COMPLETE BATCH
        // ----------------------------------------------------

        let records = message.trades();

        let limits = self.identities.limits();

        // Match the tracker's admission limits before
        // performing any allocation-heavy preparation.
        if records.len() > limits.max_batch_records || records.len() > limits.max_ids {
            return Ok(TradeControllerOutcome::Rejected(TradeControllerError::InvalidIdentity(
                TradeIdentityError::BatchTooLarge,
            )));
        }

        let mut ids = Vec::with_capacity(records.len());

        for (index, trade) in records.iter().enumerate() {
            if trade.envelope.stream_id != self.stream_id {
                return Ok(TradeControllerOutcome::Rejected(TradeControllerError::WrongStream));
            }

            let Some(id) = trade.trade_id.as_deref() else {
                return Ok(TradeControllerOutcome::Rejected(TradeControllerError::MissingTradeId {
                    index,
                }));
            };

            if id.is_empty() {
                return Ok(TradeControllerOutcome::Rejected(TradeControllerError::InvalidIdentity(
                    TradeIdentityError::EmptyTradeId,
                )));
            }

            if id.len() > limits.max_id_bytes {
                return Ok(TradeControllerOutcome::Rejected(TradeControllerError::InvalidIdentity(
                    TradeIdentityError::TradeIdTooLong,
                )));
            }

            ids.push(id);
        }

        // ----------------------------------------------------
        // PREVIEW DEDUPLICATION
        // ----------------------------------------------------

        // This mirrors TradeIdentityTracker's classification
        // semantics without mutating its retained history.
        //
        // It considers both previously retained identities
        // and identities already encountered in this batch.

        let mut seen_in_batch = HashSet::new();

        let mut candidate_trades = Vec::with_capacity(records.len());

        let mut expected_decisions = Vec::with_capacity(records.len());

        let mut duplicates = 0usize;

        for (trade, id) in records.iter().zip(ids.iter()) {
            let duplicate = self.identities.contains(id) || !seen_in_batch.insert(*id);

            if duplicate {
                duplicates += 1;

                expected_decisions.push(TradeIdentityDecision::DuplicateWithinWindow);
            } else {
                candidate_trades.push(trade.clone());

                expected_decisions.push(TradeIdentityDecision::NewWithinWindow);
            }
        }

        // ----------------------------------------------------
        // EVALUATE ORDERING EVIDENCE
        // ----------------------------------------------------

        // Only candidate trades are evaluated.
        //
        // Repeated identities should not produce false
        // ordering regressions or discontinuities.

        let evaluation =
            match TradeIntegrityEvaluator::evaluate(&self.plan, self.baseline, &candidate_trades) {
                Ok(evaluation) => evaluation,

                Err(error) => {
                    return Ok(TradeControllerOutcome::Rejected(
                        TradeControllerError::InvalidEvaluation(error),
                    ));
                }
            };

        // ----------------------------------------------------
        // APPLY COVERAGE POLICY
        // ----------------------------------------------------

        if evaluation.has_observations()
            && self.plan.coverage_action == TradeCoverageAction::RequestRecovery
        {
            // No identity or ordering baseline has been committed
            // for the triggering batch.
            //
            // Capture the accepted baseline and evidence at the
            // exact point normal admission becomes suspended.
            let hold = TradeCoverageHold {
                generation: self.generation,

                baseline: self.baseline,

                received_records: records.len(),

                observations: evaluation.observations,
            };

            let observations = hold.observations.clone();

            // The entire batch was already checked against the
            // controller's configured bounded record limit.
            //
            // Retain its canonical records because none of them
            // were committed when the coverage hold was created.
            let triggering_trades = records.to_vec();

            // Commit the hold and its corresponding triggering
            // batch together, before returning to the source runtime.
            self.coverage_triggering_trades = Some(triggering_trades);
            self.coverage_hold = Some(hold);

            return Ok(TradeControllerOutcome::CoverageReviewRequired { observations });
        }

        let will_truncate = self.identities.history_truncated()
            || self.identities.retained_count().saturating_add(candidate_trades.len()) > limits.max_ids;
        let accepted = AcceptedTradeBatch {
            trades: candidate_trades,

            received_records: records.len(),

            duplicate_records: duplicates,

            identity_history_truncated: will_truncate,

            // A successful local admission does not erase
            // earlier uncertainty about historical coverage.
            coverage_history_unverified: self.generation_history_discontinuous
                || will_truncate
                || !evaluation.observations.is_empty(),

            observations: evaluation.observations,

            baseline: evaluation.next_baseline,
        };
        let accepted = publish(TradeControllerOutcome::Accepted(accepted))?;

        // ----------------------------------------------------
        // COMMIT IDENTITY HISTORY
        // ----------------------------------------------------

        // This is the only identity-state mutation in the
        // successful acceptance path.
        //
        // Complete identity validation occurred above.
        let committed_decisions = match self.identities.observe_batch(&ids) {
            Ok(decisions) => decisions,

            Err(error) => {
                return Ok(TradeControllerOutcome::Rejected(TradeControllerError::InvalidIdentity(
                    error,
                )));
            }
        };

        // The preview and commit use the same single-owner
        // state and identical classification rules.
        //
        // A mismatch would indicate an internal invariant
        // violation, rather than an exchange-data problem.
        assert_eq!(
            committed_decisions, expected_decisions,
            "trade identity preview and commit diverged"
        );

        // ----------------------------------------------------
        // COMMIT ORDERING BASELINE
        // ----------------------------------------------------

        self.baseline = evaluation.next_baseline;

        // ----------------------------------------------------
        // RETURN ACCEPTED RECORDS
        // ----------------------------------------------------


        Ok(accepted)
    }
}
// ============================================================
// COVERAGE REVIEW STATE
// ============================================================

/// An unresolved coverage review belonging to one
/// authoritative stream generation.
///
/// This structure retains the evidence that caused
/// acceptance to stop.
///
/// It does not retain the rejected canonical trade batch.
///
/// The surrounding runtime must retain that batch through
/// a separately bounded recovery mechanism or explicitly
/// account for its loss.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TradeCoverageHold {
    pub generation: u64,

    /// Accepted ordering baseline at the moment the
    /// suspicious batch was detected.
    pub baseline: TradeIntegrityBaseline,

    /// Number of source records in the triggering batch.
    pub received_records: usize,

    /// Evidence requiring reconciliation.
    pub observations: Vec<TradeIntegrityObservation>,
}
// ============================================================
// TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    use rust_decimal::Decimal;

    use crate::live::integrity::trades::compiler::{
        TradeCoverageAction, TradeIdentityKind, TradeSequenceKind,
    };
    use crate::shared::domain::canonical::{EventEnvelope, Exchange, TradeSide};
    const STREAM_ID: &str = "bybit:spot:BTCUSDT:trades";
    fn recovery_numeric_controller() -> LiveTradeController {
        LiveTradeController::new(
            STREAM_ID,
            10,
            TradeIdentityLimits {
                max_ids: 100,
                max_id_bytes: 64,
                max_batch_records: 100,
            },
            numeric_plan(),
        )
        .unwrap()
    }
    fn limits() -> TradeIdentityLimits {
        TradeIdentityLimits {
            max_ids: 3,
            max_id_bytes: 64,
            max_batch_records: 3,
        }
    }
    fn test_plan() -> CompiledTradeIntegrityPlan {
        CompiledTradeIntegrityPlan {
            format_id: "individual_test".into(),

            identity_kind: TradeIdentityKind::Opaque,
            sequence_kind: TradeSequenceKind::None,

            detect_duplicates: true,
            detect_id_regression: false,
            detect_id_gaps: false,

            detect_cross_sequence_regression: false,
            allow_equal_cross_sequence: false,

            coverage_action: TradeCoverageAction::ReportUnverifiedCoverage,

            supports_individual_trades: true,
        }
    }
    fn controller() -> LiveTradeController {
        LiveTradeController::new(STREAM_ID, 10, limits(), test_plan()).unwrap()
    }

    fn trade(id: Option<&str>) -> Trade {
        Trade {
            envelope: EventEnvelope {
                event_timestamp_ns: 1_000_000_000,
                system_timestamp_ns: None,
                exchange: Exchange::Bybit,
                instrument_id: 42,
                symbol: "BTCUSDT".into(),
                stream_id: STREAM_ID.into(),
            },

            trade_id: id.map(str::to_owned),
            sequence: None,

            side: TradeSide::Buy,
            price: Decimal::from(80_000),

            quantity_base: Some(Decimal::new(1, 2)),
            quantity_quote: None,
            quantity_contracts: None,

            is_rpi: None,

            trade_iv: None,
            mark_iv: None,

            index_price: None,
            mark_price: None,
        }
    }

    fn message(ids: &[&str]) -> NormalizedTrades {
        NormalizedTrades::new(
            STREAM_ID,
            NormalizationSource::WebSocket,
            TradeRecordSemantics::Individual,
            ids.iter().map(|id| trade(Some(id))).collect(),
        )
        .unwrap()
    }

    #[test]
    fn accepts_individual_trades_in_source_order() {
        let mut controller = controller();

        let outcome = controller.process(10, &message(&["A", "B", "C"]));

        let TradeControllerOutcome::Accepted(batch) = outcome else {
            panic!("expected accepted batch");
        };

        let ids: Vec<_> = batch
            .trades
            .iter()
            .map(|trade| trade.trade_id.as_deref().unwrap())
            .collect();

        assert_eq!(ids, vec!["A", "B", "C"]);
        assert_eq!(batch.received_records, 3);
        assert_eq!(batch.duplicate_records, 0);
    }

    #[test]
    fn removes_duplicates_inside_one_message() {
        let mut controller = controller();

        let outcome = controller.process(10, &message(&["A", "B", "A"]));

        let TradeControllerOutcome::Accepted(batch) = outcome else {
            panic!("expected accepted batch");
        };

        assert_eq!(batch.trades.len(), 2);
        assert_eq!(batch.duplicate_records, 1);

        assert_eq!(batch.trades[0].trade_id.as_deref(), Some("A"),);

        assert_eq!(batch.trades[1].trade_id.as_deref(), Some("B"),);
    }

    #[test]
    fn removes_duplicates_across_messages() {
        let mut controller = controller();

        controller.process(10, &message(&["A", "B"]));

        let outcome = controller.process(10, &message(&["B", "C"]));

        let TradeControllerOutcome::Accepted(batch) = outcome else {
            panic!("expected accepted batch");
        };

        assert_eq!(batch.trades.len(), 1);
        assert_eq!(batch.duplicate_records, 1);

        assert_eq!(batch.trades[0].trade_id.as_deref(), Some("C"),);
    }

    #[test]
    fn rejects_missing_identity_without_state_mutation() {
        let mut controller = controller();

        let input = NormalizedTrades::new(
            STREAM_ID,
            NormalizationSource::WebSocket,
            TradeRecordSemantics::Individual,
            vec![trade(Some("A")), trade(None), trade(Some("C"))],
        )
        .unwrap();

        let outcome = controller.process(10, &input);

        assert!(matches!(
            outcome,
            TradeControllerOutcome::Rejected(TradeControllerError::MissingTradeId { index: 1 })
        ));

        assert_eq!(controller.identities().retained_count(), 0,);
    }

    #[test]
    fn rejects_oversized_batch_atomically() {
        let mut controller = controller();

        let outcome = controller.process(10, &message(&["A", "B", "C", "D"]));

        assert!(matches!(
            outcome,
            TradeControllerOutcome::Rejected(TradeControllerError::InvalidIdentity(
                TradeIdentityError::BatchTooLarge
            ))
        ));

        assert_eq!(controller.identities().retained_count(), 0,);
    }

    #[test]
    fn obsolete_generation_does_not_mutate_state() {
        let mut controller = controller();

        let outcome = controller.process(9, &message(&["A"]));

        assert!(matches!(
            outcome,
            TradeControllerOutcome::ObsoleteGeneration
        ));

        assert_eq!(controller.identities().retained_count(), 0,);
    }

    #[test]
    fn new_generation_discards_identity_window() {
        let mut controller = controller();

        controller.process(10, &message(&["A"]));

        controller.begin_generation(11).unwrap();

        assert_eq!(controller.generation(), 11);

        assert_eq!(controller.identities().retained_count(), 0,);

        assert!(controller.generation_history_discontinuous());

        let outcome = controller.process(11, &message(&["A"]));

        let TradeControllerOutcome::Accepted(batch) = outcome else {
            panic!("expected accepted batch");
        };

        // This ID is new only within generation 11.
        // Cross-generation duplication is not ruled out.
        assert_eq!(batch.trades.len(), 1);
    }

    #[test]
    fn rejects_aggregated_trade_semantics() {
        let mut controller = controller();

        let input = NormalizedTrades::new(
            STREAM_ID,
            NormalizationSource::WebSocket,
            TradeRecordSemantics::Aggregated,
            vec![trade(Some("A"))],
        )
        .unwrap();

        assert!(matches!(
            controller.process(10, &input),
            TradeControllerOutcome::Rejected(TradeControllerError::UnsupportedSemantics)
        ));
    }

    #[test]
    fn rejects_rest_without_reconciliation_contract() {
        let mut controller = controller();

        let input = NormalizedTrades::new(
            STREAM_ID,
            NormalizationSource::Rest,
            TradeRecordSemantics::Individual,
            vec![trade(Some("A"))],
        )
        .unwrap();

        assert!(matches!(
            controller.process(10, &input),
            TradeControllerOutcome::Rejected(TradeControllerError::UnsupportedSource)
        ));
    }

    #[test]
    fn empty_batch_is_valid() {
        let mut controller = controller();

        let outcome = controller.process(10, &message(&[]));

        let TradeControllerOutcome::Accepted(batch) = outcome else {
            panic!("expected accepted batch");
        };

        assert!(batch.trades.is_empty());
        assert_eq!(batch.received_records, 0);
        assert_eq!(batch.duplicate_records, 0);
    }

    #[test]
    fn eviction_is_visible_in_accepted_output() {
        let mut controller = controller();

        controller.process(10, &message(&["A", "B", "C"]));

        let outcome = controller.process(10, &message(&["D"]));

        let TradeControllerOutcome::Accepted(batch) = outcome else {
            panic!("expected accepted batch");
        };

        assert_eq!(batch.trades.len(), 1);
        assert!(batch.identity_history_truncated);
        assert_eq!(controller.identities().evictions_total(), 1);
    }

    #[test]
    fn invalid_generation_transition_preserves_state() {
        let mut controller = controller();

        controller.process(10, &message(&["A"]));

        assert!(controller.begin_generation(10).is_err());
        assert!(controller.begin_generation(9).is_err());

        assert_eq!(controller.generation(), 10);
        assert!(controller.identities().contains("A"));
    }
    fn numeric_plan() -> CompiledTradeIntegrityPlan {
        CompiledTradeIntegrityPlan {
            format_id: "numeric_test".into(),

            identity_kind: TradeIdentityKind::Numeric,
            sequence_kind: TradeSequenceKind::None,

            detect_duplicates: true,
            detect_id_regression: true,
            detect_id_gaps: true,

            detect_cross_sequence_regression: false,
            allow_equal_cross_sequence: false,

            coverage_action: TradeCoverageAction::RequestRecovery,

            supports_individual_trades: true,
        }
    }

    fn numeric_controller() -> LiveTradeController {
        LiveTradeController::new(STREAM_ID, 10, limits(), numeric_plan()).unwrap()
    }

    #[test]
    fn numeric_sequence_advances_after_clean_batch() {
        let mut controller = numeric_controller();

        let outcome = controller.process(10, &message(&["100", "101", "102"]));

        let TradeControllerOutcome::Accepted(batch) = outcome else {
            panic!("expected accepted batch");
        };

        assert_eq!(batch.trades.len(), 3);

        assert_eq!(controller.baseline().numeric_trade_id, Some(102),);

        assert_eq!(controller.identities().retained_count(), 3,);
    }

    #[test]
    fn suspected_gap_does_not_commit_identity_or_baseline() {
        let mut controller = numeric_controller();

        controller.process(10, &message(&["100"]));

        let outcome = controller.process(10, &message(&["105"]));

        assert!(matches!(
            outcome,
            TradeControllerOutcome::CoverageReviewRequired { .. }
        ));

        // The suspicious trade has not been accepted.
        assert!(!controller.identities().contains("105"));

        // The previous ordering baseline is preserved.
        assert_eq!(controller.baseline().numeric_trade_id, Some(100),);

        assert_eq!(controller.identities().observations_total(), 1,);
    }

    #[test]
    fn invalid_later_record_does_not_commit_earlier_ids() {
        let mut controller = numeric_controller();

        let outcome = controller.process(10, &message(&["100", "not-numeric"]));

        assert!(matches!(
            outcome,
            TradeControllerOutcome::Rejected(TradeControllerError::InvalidEvaluation(_))
        ));

        assert_eq!(controller.baseline().numeric_trade_id, None,);

        assert_eq!(controller.identities().retained_count(), 0,);
    }

    #[test]
    fn duplicate_filtering_precedes_ordering_evaluation() {
        let mut controller = numeric_controller();

        controller.process(10, &message(&["100", "101"]));

        // ID 100 is already known. Its presence must not
        // generate a numeric regression observation.
        let outcome = controller.process(10, &message(&["100", "102"]));

        let TradeControllerOutcome::Accepted(batch) = outcome else {
            panic!("expected accepted batch");
        };

        assert_eq!(batch.duplicate_records, 1);
        assert_eq!(batch.trades.len(), 1);

        assert!(batch.observations.is_empty());

        assert_eq!(controller.baseline().numeric_trade_id, Some(102),);
    }

    #[test]
    fn coverage_report_policy_accepts_with_observations() {
        let mut plan = numeric_plan();

        plan.coverage_action = TradeCoverageAction::ReportUnverifiedCoverage;

        let mut controller = LiveTradeController::new(STREAM_ID, 10, limits(), plan).unwrap();

        controller.process(10, &message(&["100"]));

        let outcome = controller.process(10, &message(&["105"]));

        let TradeControllerOutcome::Accepted(batch) = outcome else {
            panic!("expected accepted batch");
        };

        assert_eq!(batch.trades.len(), 1);
        assert_eq!(batch.observations.len(), 1);

        assert_eq!(controller.baseline().numeric_trade_id, Some(105),);
    }

    #[test]
    fn generation_change_resets_ordering_baseline() {
        let mut controller = numeric_controller();

        controller.process(10, &message(&["100", "101"]));

        assert_eq!(controller.baseline().numeric_trade_id, Some(101),);

        controller.begin_generation(11).unwrap();

        assert_eq!(controller.baseline().numeric_trade_id, None,);

        assert!(controller.generation_history_discontinuous());
    }

    #[test]
    fn rejects_plan_without_duplicate_detection() {
        let mut plan = test_plan();

        plan.detect_duplicates = false;

        let result = LiveTradeController::new(STREAM_ID, 10, limits(), plan);

        assert!(result.is_err());
    }
    // ============================================================
    // N5N.7 — TRADE ACCEPTANCE CONTRACT TESTS
    // ============================================================

    #[test]
    fn held_batch_remains_uncommitted_on_repeated_submission() {
        let mut controller = numeric_controller();

        controller.process(10, &message(&["100"]));

        let first = controller.process(10, &message(&["105"]));

        assert!(matches!(
            first,
            TradeControllerOutcome::CoverageReviewRequired { .. }
        ));

        assert!(controller.is_coverage_review_pending());
        assert!(!controller.can_accept_trades());

        for _ in 0..3 {
            let outcome = controller.process(10, &message(&["105"]));

            assert!(matches!(
                outcome,
                TradeControllerOutcome::CoverageReviewPending { generation: 10 }
            ));

            assert_eq!(controller.baseline().numeric_trade_id, Some(100),);

            assert!(!controller.identities().contains("105"));
        }

        assert_eq!(controller.identities().observations_total(), 1,);
    }

    #[test]
    fn coverage_hold_blocks_even_valid_later_trade() {
        let mut controller = numeric_controller();

        controller.process(10, &message(&["100"]));

        let suspicious = controller.process(10, &message(&["105"]));

        assert!(matches!(
            suspicious,
            TradeControllerOutcome::CoverageReviewRequired { .. }
        ));

        // ID 101 would ordinarily continue the baseline.
        // It must not bypass an unresolved coverage hold.
        let later = controller.process(10, &message(&["101"]));

        assert!(matches!(
            later,
            TradeControllerOutcome::CoverageReviewPending { generation: 10 }
        ));

        assert_eq!(controller.baseline().numeric_trade_id, Some(100),);

        assert!(!controller.identities().contains("101"));
        assert!(!controller.identities().contains("105"));
    }

    #[test]
    fn identity_eviction_never_claims_complete_history() {
        let mut controller =
            LiveTradeController::new(STREAM_ID, 10, limits(), test_plan()).unwrap();

        controller.process(10, &message(&["A", "B", "C"]));

        // This observation evicts A from the bounded history.
        controller.process(10, &message(&["D"]));

        let outcome = controller.process(10, &message(&["A"]));

        let TradeControllerOutcome::Accepted(batch) = outcome else {
            panic!("expected accepted batch");
        };

        // A is no longer in the retained window.
        assert_eq!(batch.trades.len(), 1);

        // Crucially, the output admits that deduplication
        // history is incomplete.
        assert!(batch.identity_history_truncated);
    }
    // ============================================================
    // N5N.7.3 — TRADE COVERAGE LIFECYCLE
    // ============================================================

    #[test]
    fn coverage_hold_preserves_original_evidence() {
        let mut controller = numeric_controller();

        controller.process(10, &message(&["100"]));

        let outcome = controller.process(10, &message(&["105"]));

        assert!(matches!(
            outcome,
            TradeControllerOutcome::CoverageReviewRequired { .. }
        ));

        let hold = controller.coverage_hold().unwrap();

        assert_eq!(hold.generation, 10);
        assert_eq!(hold.received_records, 1);

        assert_eq!(hold.baseline.numeric_trade_id, Some(100),);

        assert_eq!(hold.observations.len(), 1);

        assert_eq!(hold.observations[0].previous, 100,);

        assert_eq!(hold.observations[0].current, 105,);
    }

    #[test]
    fn coverage_hold_does_not_mutate_on_later_messages() {
        let mut controller = numeric_controller();

        controller.process(10, &message(&["100"]));
        controller.process(10, &message(&["105"]));

        let original = controller.coverage_hold().unwrap().clone();

        let original_observations = controller.identities().observations_total();

        for id in ["101", "102", "999"] {
            let outcome = controller.process(10, &message(&[id]));

            assert!(matches!(
                outcome,
                TradeControllerOutcome::CoverageReviewPending { generation: 10 }
            ));
        }

        assert_eq!(controller.coverage_hold(), Some(&original),);

        assert_eq!(
            controller.identities().observations_total(),
            original_observations,
        );

        assert_eq!(controller.baseline().numeric_trade_id, Some(100),);
    }

    #[test]
    fn obsolete_generation_cannot_clear_coverage_hold() {
        let mut controller = numeric_controller();

        controller.process(10, &message(&["100"]));
        controller.process(10, &message(&["105"]));

        let outcome = controller.process(9, &message(&["101"]));

        assert!(matches!(
            outcome,
            TradeControllerOutcome::ObsoleteGeneration
        ));

        assert!(controller.is_coverage_review_pending());

        assert_eq!(controller.coverage_hold().unwrap().generation, 10,);
    }

    #[test]
    fn invalid_generation_change_preserves_coverage_hold() {
        let mut controller = numeric_controller();

        controller.process(10, &message(&["100"]));
        controller.process(10, &message(&["105"]));

        let original = controller.coverage_hold().unwrap().clone();

        assert!(controller.begin_generation(10).is_err());
        assert!(controller.begin_generation(9).is_err());

        assert_eq!(controller.generation(), 10);

        assert_eq!(controller.coverage_hold(), Some(&original),);
    }

    #[test]
    fn new_generation_retires_old_coverage_hold() {
        let mut controller = numeric_controller();

        controller.process(10, &message(&["100"]));
        controller.process(10, &message(&["105"]));

        assert!(controller.is_coverage_review_pending());

        controller.begin_generation(11).unwrap();

        assert!(!controller.is_coverage_review_pending());
        assert!(controller.can_accept_trades());

        assert!(controller.coverage_hold().is_none());

        assert_eq!(controller.baseline(), TradeIntegrityBaseline::default(),);

        // Local history continuity across the generation
        // boundary remains unverified.
        assert!(controller.generation_history_discontinuous());

        // New generation can establish its own baseline.
        let outcome = controller.process(11, &message(&["200"]));

        let TradeControllerOutcome::Accepted(batch) = outcome else {
            panic!("expected accepted batch");
        };

        assert_eq!(batch.trades.len(), 1);

        // The successful new epoch must not falsely claim
        // complete historical coverage.
        assert!(batch.coverage_history_unverified);
    }

    #[test]
    fn initial_clean_generation_reports_no_known_coverage_issue() {
        let mut controller = numeric_controller();

        let outcome = controller.process(10, &message(&["100", "101"]));

        let TradeControllerOutcome::Accepted(batch) = outcome else {
            panic!("expected accepted batch");
        };

        assert!(!batch.identity_history_truncated);
        assert!(!batch.coverage_history_unverified);

        // This only means no local uncertainty has yet been
        // recorded; it is not proof of global completeness.
    }

    #[test]
    fn report_policy_exposes_uncertainty_without_holding() {
        let mut plan = numeric_plan();

        plan.coverage_action = TradeCoverageAction::ReportUnverifiedCoverage;

        let mut controller = LiveTradeController::new(STREAM_ID, 10, limits(), plan).unwrap();

        controller.process(10, &message(&["100"]));

        let outcome = controller.process(10, &message(&["105"]));

        let TradeControllerOutcome::Accepted(batch) = outcome else {
            panic!("expected accepted batch");
        };

        assert_eq!(batch.trades.len(), 1);
        assert_eq!(batch.observations.len(), 1);

        assert!(batch.coverage_history_unverified);

        assert!(!controller.is_coverage_review_pending());

        assert_eq!(controller.baseline().numeric_trade_id, Some(105),);
    }

    #[test]
    fn malformed_batch_cannot_clear_existing_hold() {
        let mut controller = numeric_controller();

        controller.process(10, &message(&["100"]));
        controller.process(10, &message(&["105"]));

        let input = NormalizedTrades::new(
            STREAM_ID,
            NormalizationSource::WebSocket,
            TradeRecordSemantics::Individual,
            vec![trade(None)],
        )
        .unwrap();

        let outcome = controller.process(10, &input);

        // Admission remains suspended before record-level
        // validation is attempted.
        assert!(matches!(
            outcome,
            TradeControllerOutcome::CoverageReviewPending { generation: 10 }
        ));

        assert!(controller.is_coverage_review_pending());

        assert_eq!(controller.baseline().numeric_trade_id, Some(100),);
    }
    #[test]
    fn coverage_hold_retains_complete_triggering_batch() {
        let mut controller = numeric_controller();

        // Establish the last accepted baseline.
        let accepted = controller.process(10, &message(&["100"]));

        assert!(matches!(accepted, TradeControllerOutcome::Accepted(_)));

        // The first record is continuous, but the second
        // introduces a discontinuity.
        //
        // Neither record may be committed because the
        // controller evaluates the batch atomically.
        let outcome = controller.process(10, &message(&["101", "105"]));

        assert!(matches!(
            outcome,
            TradeControllerOutcome::CoverageReviewRequired { .. }
        ));

        let retained = controller
            .coverage_triggering_trades()
            .expect("triggering batch must be retained");

        let ids: Vec<_> = retained
            .iter()
            .map(|trade| trade.trade_id.as_deref().unwrap())
            .collect();

        assert_eq!(ids, vec!["101", "105"]);

        assert_eq!(controller.baseline().numeric_trade_id, Some(100));

        assert!(!controller.identities().contains("101"));
        assert!(!controller.identities().contains("105"));

        assert_eq!(controller.coverage_hold().unwrap().received_records, 2);
    }

    #[test]
    fn pending_coverage_hold_preserves_original_triggering_batch() {
        let mut controller = numeric_controller();

        controller.process(10, &message(&["100"]));

        controller.process(10, &message(&["105"]));

        // Subsequent messages cannot replace the batch
        // that originally triggered the coverage hold.
        let outcome = controller.process(10, &message(&["106", "107"]));

        assert!(matches!(
            outcome,
            TradeControllerOutcome::CoverageReviewPending { generation: 10 }
        ));

        let retained = controller.coverage_triggering_trades().unwrap();

        assert_eq!(retained.len(), 1);
        assert_eq!(retained[0].trade_id.as_deref(), Some("105"));

        assert!(!controller.identities().contains("105"));
        assert!(!controller.identities().contains("106"));
    }

    #[test]
    fn newer_generation_retires_triggering_batch() {
        let mut controller = numeric_controller();

        controller.process(10, &message(&["100"]));
        controller.process(10, &message(&["105"]));

        assert!(controller.coverage_hold().is_some());
        assert!(controller.coverage_triggering_trades().is_some());

        controller.begin_generation(11).unwrap();

        assert!(controller.coverage_hold().is_none());
        assert!(controller.coverage_triggering_trades().is_none());

        // The new generation is locally admissible,
        // but continuity with the previous generation
        // remains unverified.
        assert!(controller.can_accept_trades());

        assert!(controller.generation_history_discontinuous());
    }

    #[test]
    fn rejected_batch_is_not_retained_as_recovery_data() {
        let mut controller = numeric_controller();

        let result = controller.process(10, &message(&["100", "not-numeric"]));

        assert!(matches!(result, TradeControllerOutcome::Rejected(_)));

        assert!(controller.coverage_hold().is_none());

        assert!(controller.coverage_triggering_trades().is_none());

        assert_eq!(controller.identities().retained_count(), 0);
    }
    #[test]
    fn recovery_preflight_preserves_authoritative_state() {
        let mut controller = recovery_numeric_controller();

        controller.process(10, &message(&["100"]));
        controller.process(10, &message(&["105"]));

        let baseline = controller.baseline();
        let original_hold = controller.coverage_hold().unwrap().clone();

        let candidate = ["101", "102", "103", "104", "105"]
            .into_iter()
            .map(|id| trade(Some(id)))
            .collect::<Vec<_>>();

        let prepared = controller
            .preflight_numeric_recovery(10, baseline, &candidate)
            .unwrap();

        assert_eq!(prepared.record_count(), 5);
        assert_eq!(prepared.generation(), 10);
        assert_eq!(prepared.baseline(), baseline);

        // No recovery records have been committed.
        assert!(!controller.identities().contains("101"));
        assert!(!controller.identities().contains("105"));

        assert_eq!(controller.baseline(), baseline);
        assert_eq!(controller.coverage_hold(), Some(&original_hold));

        assert!(controller.is_coverage_review_pending());
    }
    #[test]
    fn recovery_preflight_rejects_obsolete_generation() {
        let mut controller = recovery_numeric_controller();

        controller.process(10, &message(&["100"]));
        controller.process(10, &message(&["105"]));

        let result =
            controller.preflight_numeric_recovery(9, controller.baseline(), &[trade(Some("101"))]);

        assert!(matches!(
            result,
            Err(TradeRecoveryPreflightError::ObsoleteGeneration)
        ));

        assert!(controller.is_coverage_review_pending());
    }
    #[test]
    fn recovery_preflight_requires_triggering_trade() {
        let mut controller = recovery_numeric_controller();

        controller.process(10, &message(&["100"]));
        controller.process(10, &message(&["105"]));

        let candidate = ["101", "102", "103"]
            .into_iter()
            .map(|id| trade(Some(id)))
            .collect::<Vec<_>>();

        let result = controller.preflight_numeric_recovery(10, controller.baseline(), &candidate);

        assert!(matches!(
            result,
            Err(TradeRecoveryPreflightError::MissingTriggeringTrade)
        ));

        assert!(controller.is_coverage_review_pending());
    }
    #[test]
    fn recovery_preflight_rejects_discontinuous_candidate() {
        let mut controller = recovery_numeric_controller();

        controller.process(10, &message(&["100"]));
        controller.process(10, &message(&["105"]));

        let candidate = ["101", "103", "104", "105"]
            .into_iter()
            .map(|id| trade(Some(id)))
            .collect::<Vec<_>>();

        let result = controller.preflight_numeric_recovery(10, controller.baseline(), &candidate);

        assert!(matches!(
            result,
            Err(TradeRecoveryPreflightError::NonConsecutiveTradeIds)
        ));

        assert_eq!(controller.baseline().numeric_trade_id, Some(100));

        assert!(!controller.identities().contains("105"));
    }
    #[test]
    fn failed_recovery_publication_preserves_coverage_hold() {
        let mut controller = recovery_numeric_controller();

        controller.process(10, &message(&["100"]));
        controller.process(10, &message(&["105"]));

        let baseline = controller.baseline();
        let original_hold = controller.coverage_hold().unwrap().clone();

        let candidate = ["101", "102", "103", "104", "105"]
            .into_iter()
            .map(|id| trade(Some(id)))
            .collect::<Vec<_>>();

        let prepared = controller
            .preflight_numeric_recovery(10, baseline, &candidate)
            .unwrap();

        let result = controller
            .complete_numeric_recovery(prepared, |_| Err("downstream unavailable".into()));

        assert!(matches!(
            result,
            Err(TradeRecoveryCommitError::Publication(_))
        ));

        assert_eq!(controller.baseline(), baseline);
        assert_eq!(controller.coverage_hold(), Some(&original_hold));

        assert!(controller.coverage_triggering_trades().is_some());

        for id in ["101", "102", "103", "104", "105"] {
            assert!(!controller.identities().contains(id));
        }

        assert!(controller.is_coverage_review_pending());
    }
    #[test]
    fn successful_recovery_handoff_commits_and_clears_hold() {
        let mut controller = recovery_numeric_controller();

        controller.process(10, &message(&["100"]));
        controller.process(10, &message(&["105"]));

        let candidate = ["101", "102", "103", "104", "105"]
            .into_iter()
            .map(|id| trade(Some(id)))
            .collect::<Vec<_>>();

        let prepared = controller
            .preflight_numeric_recovery(10, controller.baseline(), &candidate)
            .unwrap();

        let mut published_ids = Vec::new();

        let accepted = controller
            .complete_numeric_recovery(prepared, |trades| {
                published_ids = trades
                    .iter()
                    .map(|trade| trade.trade_id.clone().unwrap())
                    .collect();

                Ok(())
            })
            .unwrap();

        assert_eq!(published_ids, ["101", "102", "103", "104", "105"]);

        assert_eq!(accepted.trades.len(), 5);

        assert_eq!(controller.baseline().numeric_trade_id, Some(105));

        for id in ["101", "102", "103", "104", "105"] {
            assert!(controller.identities().contains(id));
        }

        assert!(controller.coverage_hold().is_none());
        assert!(controller.coverage_triggering_trades().is_none());

        assert!(controller.can_accept_trades());

        // No global completeness guarantee is fabricated.
        assert!(accepted.coverage_history_unverified);
    }
    #[test]
    fn obsolete_recovery_preflight_cannot_publish() {
        let mut controller = recovery_numeric_controller();

        controller.process(10, &message(&["100"]));
        controller.process(10, &message(&["105"]));

        let candidate = ["101", "102", "103", "104", "105"]
            .into_iter()
            .map(|id| trade(Some(id)))
            .collect::<Vec<_>>();

        let prepared = controller
            .preflight_numeric_recovery(10, controller.baseline(), &candidate)
            .unwrap();

        // A newer authoritative acquisition retires
        // the previous generation's coverage hold.
        controller.begin_generation(11).unwrap();

        let result = controller.complete_numeric_recovery(prepared, |_| {
            panic!("obsolete recovery must never publish")
        });

        assert!(matches!(
            result,
            Err(TradeRecoveryCommitError::Preflight(
                TradeRecoveryPreflightError::ObsoleteGeneration
            ))
        ));

        assert_eq!(controller.generation(), 11);
        assert!(controller.coverage_hold().is_none());
    }
}
