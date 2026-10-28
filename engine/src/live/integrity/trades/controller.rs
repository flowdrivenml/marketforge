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
    pub(in crate::live::integrity) fn process(
        &mut self,
        generation: u64,
        message: &NormalizedTrades,
    ) -> TradeControllerOutcome {
        // ----------------------------------------------------
        // GENERATION FENCING
        // ----------------------------------------------------

        if generation != self.generation {
            return TradeControllerOutcome::ObsoleteGeneration;
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
            return TradeControllerOutcome::CoverageReviewPending {
                generation: self.generation,
            };
        }
        // ----------------------------------------------------
        // SOURCE AND STREAM VALIDATION
        // ----------------------------------------------------

        if message.stream_id() != self.stream_id {
            return TradeControllerOutcome::Rejected(TradeControllerError::WrongStream);
        }

        if message.source() != NormalizationSource::WebSocket {
            return TradeControllerOutcome::Rejected(TradeControllerError::UnsupportedSource);
        }

        if message.semantics() != TradeRecordSemantics::Individual {
            return TradeControllerOutcome::Rejected(TradeControllerError::UnsupportedSemantics);
        }

        // ----------------------------------------------------
        // PREVALIDATE COMPLETE BATCH
        // ----------------------------------------------------

        let records = message.trades();

        let limits = self.identities.limits();

        // Match the tracker's admission limits before
        // performing any allocation-heavy preparation.
        if records.len() > limits.max_batch_records || records.len() > limits.max_ids {
            return TradeControllerOutcome::Rejected(TradeControllerError::InvalidIdentity(
                TradeIdentityError::BatchTooLarge,
            ));
        }

        let mut ids = Vec::with_capacity(records.len());

        for (index, trade) in records.iter().enumerate() {
            if trade.envelope.stream_id != self.stream_id {
                return TradeControllerOutcome::Rejected(TradeControllerError::WrongStream);
            }

            let Some(id) = trade.trade_id.as_deref() else {
                return TradeControllerOutcome::Rejected(TradeControllerError::MissingTradeId {
                    index,
                });
            };

            if id.is_empty() {
                return TradeControllerOutcome::Rejected(TradeControllerError::InvalidIdentity(
                    TradeIdentityError::EmptyTradeId,
                ));
            }

            if id.len() > limits.max_id_bytes {
                return TradeControllerOutcome::Rejected(TradeControllerError::InvalidIdentity(
                    TradeIdentityError::TradeIdTooLong,
                ));
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
                    return TradeControllerOutcome::Rejected(
                        TradeControllerError::InvalidEvaluation(error),
                    );
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

            self.coverage_hold = Some(hold);

            return TradeControllerOutcome::CoverageReviewRequired { observations };
        }

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
                return TradeControllerOutcome::Rejected(TradeControllerError::InvalidIdentity(
                    error,
                ));
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

        let identity_history_truncated = self.identities.history_truncated();

        TradeControllerOutcome::Accepted(AcceptedTradeBatch {
            trades: candidate_trades,

            received_records: records.len(),

            duplicate_records: duplicates,

            identity_history_truncated,

            // A successful local admission does not erase
            // earlier uncertainty about historical coverage.
            coverage_history_unverified: self.generation_history_discontinuous
                || identity_history_truncated
                || !evaluation.observations.is_empty(),

            observations: evaluation.observations,

            baseline: self.baseline,
        })
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
}
