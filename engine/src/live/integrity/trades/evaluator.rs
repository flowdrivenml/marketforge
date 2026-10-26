use crate::canonical::Trade;

use crate::live::integrity::trades::compiler::{
    CompiledTradeIntegrityPlan, TradeIdentityKind, TradeSequenceKind,
};

// ============================================================
// ACCEPTED BASELINE
// ============================================================

/// Last accepted ordering evidence for one trade stream.
///
/// This is owned by the future LiveTradeController integration.
///
/// Numeric trade IDs and cross-sequence values represent
/// different concepts and are tracked independently.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TradeIntegrityBaseline {
    pub numeric_trade_id: Option<u64>,
    pub cross_sequence: Option<u64>,
}

// ============================================================
// INTEGRITY OBSERVATIONS
// ============================================================

/// A potentially relevant continuity observation.
///
/// None of these variants proves an exact number of
/// missing market executions.
///
/// In particular, a numeric ID difference must not
/// automatically be interpreted as missing-trade count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradeIntegrityObservationKind {
    NumericIdRegression,
    NumericIdDiscontinuity,
    CrossSequenceRegression,
    UnexpectedEqualCrossSequence,
}

/// Evidence observed at one position in an accepted batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TradeIntegrityObservation {
    /// Index in the already-deduplicated input batch.
    pub index: usize,

    pub kind: TradeIntegrityObservationKind,

    /// Last accepted value for this particular comparison.
    pub previous: u64,

    /// Incoming value.
    pub current: u64,
}

// ============================================================
// EVALUATION RESULT
// ============================================================

/// Result of evaluating one complete trade batch.
///
/// The proposed baseline is not committed automatically.
///
/// The caller must decide how observations affect
/// stream health, recovery, and downstream acceptance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TradeIntegrityEvaluation {
    pub next_baseline: TradeIntegrityBaseline,

    pub observations: Vec<TradeIntegrityObservation>,

    pub inspected_records: usize,
}

impl TradeIntegrityEvaluation {
    pub fn has_observations(&self) -> bool {
        !self.observations.is_empty()
    }
}

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TradeIntegrityEvaluationError {
    AggregateTradesUnsupported,

    MissingTradeId { index: usize },

    InvalidNumericTradeId { index: usize, value: String },

    MissingCrossSequence { index: usize },
}

// ============================================================
// EVALUATOR
// ============================================================

/// Stateless trade-ordering and coverage-evidence evaluator.
///
/// Input requirements:
///
/// - Individual canonical trades only.
/// - Source-order preservation.
/// - Duplicate identities already removed.
/// - All trades belong to the same authoritative stream.
/// - A plan compiled from the relevant exchange format.
///
/// This evaluator does not:
///
/// - Own identity history.
/// - Modify accepted trade baselines.
/// - Deduplicate records.
/// - Fetch recovery data.
/// - Record GapTracker incidents.
/// - Authorize publication.
/// - Infer exact missing executions from ID arithmetic.
pub struct TradeIntegrityEvaluator;

impl TradeIntegrityEvaluator {
    pub fn evaluate(
        plan: &CompiledTradeIntegrityPlan,
        baseline: TradeIntegrityBaseline,
        trades: &[Trade],
    ) -> Result<TradeIntegrityEvaluation, TradeIntegrityEvaluationError> {
        if !plan.supports_individual_trades || plan.identity_kind == TradeIdentityKind::Aggregate {
            return Err(TradeIntegrityEvaluationError::AggregateTradesUnsupported);
        }

        // Work entirely with a local candidate.
        //
        // No authoritative state is modified if one record
        // later fails validation.
        let mut next = baseline;

        let mut observations = Vec::new();

        let numeric_checks = plan.detect_id_regression || plan.detect_id_gaps;

        for (index, trade) in trades.iter().enumerate() {
            // ------------------------------------------------
            // NUMERIC IDENTITY
            // ------------------------------------------------

            if numeric_checks {
                let id = trade
                    .trade_id
                    .as_deref()
                    .ok_or(TradeIntegrityEvaluationError::MissingTradeId { index })?;

                // Parse the entire string as an unsigned
                // integer. Never convert via floating point.
                //
                // This applies only where the compiled policy
                // explicitly enables numeric ordering checks.
                let current = id.parse::<u64>().map_err(|_| {
                    TradeIntegrityEvaluationError::InvalidNumericTradeId {
                        index,
                        value: id.to_owned(),
                    }
                })?;

                if let Some(previous) = next.numeric_trade_id {
                    if current < previous {
                        if plan.detect_id_regression {
                            observations.push(TradeIntegrityObservation {
                                index,
                                kind: TradeIntegrityObservationKind::NumericIdRegression,
                                previous,
                                current,
                            });
                        }
                    } else if current > previous {
                        if plan.detect_id_gaps && current - previous > 1 {
                            observations.push(TradeIntegrityObservation {
                                index,
                                kind: TradeIntegrityObservationKind::NumericIdDiscontinuity,
                                previous,
                                current,
                            });
                        }
                    }
                }

                // Keep the highest observed numeric ID.
                //
                // A regressing or out-of-order record must
                // not silently lower the accepted high-water
                // mark used for subsequent comparisons.
                next.numeric_trade_id = Some(
                    next.numeric_trade_id
                        .map_or(current, |old| old.max(current)),
                );
            }

            // ------------------------------------------------
            // CROSS-SEQUENCE METADATA
            // ------------------------------------------------

            if plan.sequence_kind == TradeSequenceKind::CrossSequence
                && plan.detect_cross_sequence_regression
            {
                let current = trade
                    .sequence
                    .ok_or(TradeIntegrityEvaluationError::MissingCrossSequence { index })?;

                if let Some(previous) = next.cross_sequence {
                    if current < previous {
                        observations.push(TradeIntegrityObservation {
                            index,
                            kind: TradeIntegrityObservationKind::CrossSequenceRegression,
                            previous,
                            current,
                        });
                    } else if current == previous && !plan.allow_equal_cross_sequence {
                        observations.push(TradeIntegrityObservation {
                            index,
                            kind: TradeIntegrityObservationKind::UnexpectedEqualCrossSequence,
                            previous,
                            current,
                        });
                    }
                }

                // Preserve the high-water mark.
                //
                // Multiple trades may legitimately share
                // a cross-sequence value where configured.
                next.cross_sequence =
                    Some(next.cross_sequence.map_or(current, |old| old.max(current)));
            }
        }

        Ok(TradeIntegrityEvaluation {
            next_baseline: next,
            observations,
            inspected_records: trades.len(),
        })
    }
}

// ============================================================
// TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    use rust_decimal::Decimal;

    use crate::canonical::{EventEnvelope, Exchange, TradeSide};

    use crate::live::integrity::trades::compiler::TradeCoverageAction;

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

    fn opaque_plan() -> CompiledTradeIntegrityPlan {
        CompiledTradeIntegrityPlan {
            format_id: "opaque_test".into(),

            identity_kind: TradeIdentityKind::Opaque,
            sequence_kind: TradeSequenceKind::CrossSequence,

            detect_duplicates: true,
            detect_id_regression: false,
            detect_id_gaps: false,

            detect_cross_sequence_regression: true,
            allow_equal_cross_sequence: true,

            coverage_action: TradeCoverageAction::ReportUnverifiedCoverage,

            supports_individual_trades: true,
        }
    }

    fn trade(id: &str, sequence: Option<u64>) -> Trade {
        Trade {
            envelope: EventEnvelope {
                event_timestamp_ns: 1_000_000_000,
                system_timestamp_ns: None,
                exchange: Exchange::Bybit,
                instrument_id: 42,
                symbol: "BTCUSDT".into(),
                stream_id: "stream-42".into(),
            },

            trade_id: Some(id.to_owned()),
            sequence,

            side: TradeSide::Buy,
            price: Decimal::from(80_000),

            quantity_base: Some(Decimal::ONE),
            quantity_quote: None,
            quantity_contracts: None,

            is_rpi: None,
            trade_iv: None,
            mark_iv: None,
            index_price: None,
            mark_price: None,
        }
    }

    #[test]
    fn consecutive_numeric_ids_produce_no_observations() {
        let trades = vec![trade("101", None), trade("102", None), trade("103", None)];

        let result = TradeIntegrityEvaluator::evaluate(
            &numeric_plan(),
            TradeIntegrityBaseline {
                numeric_trade_id: Some(100),
                cross_sequence: None,
            },
            &trades,
        )
        .unwrap();

        assert!(!result.has_observations());

        assert_eq!(result.next_baseline.numeric_trade_id, Some(103),);
    }

    #[test]
    fn numeric_gap_produces_unverified_observation() {
        let trades = vec![trade("105", None)];

        let result = TradeIntegrityEvaluator::evaluate(
            &numeric_plan(),
            TradeIntegrityBaseline {
                numeric_trade_id: Some(100),
                cross_sequence: None,
            },
            &trades,
        )
        .unwrap();

        assert_eq!(
            result.observations,
            vec![TradeIntegrityObservation {
                index: 0,
                kind: TradeIntegrityObservationKind::NumericIdDiscontinuity,
                previous: 100,
                current: 105,
            }],
        );

        // No exact missing-execution count is produced.
        assert_eq!(result.next_baseline.numeric_trade_id, Some(105),);
    }

    #[test]
    fn numeric_regression_preserves_high_water_mark() {
        let trades = vec![trade("95", None), trade("101", None)];

        let result = TradeIntegrityEvaluator::evaluate(
            &numeric_plan(),
            TradeIntegrityBaseline {
                numeric_trade_id: Some(100),
                cross_sequence: None,
            },
            &trades,
        )
        .unwrap();

        assert_eq!(
            result.observations[0].kind,
            TradeIntegrityObservationKind::NumericIdRegression,
        );

        assert_eq!(result.next_baseline.numeric_trade_id, Some(101),);
    }

    #[test]
    fn opaque_ids_are_not_parsed_as_numbers() {
        let trades = vec![trade("abc-123", Some(501)), trade("xyz-999", Some(502))];

        let result = TradeIntegrityEvaluator::evaluate(
            &opaque_plan(),
            TradeIntegrityBaseline {
                numeric_trade_id: None,
                cross_sequence: Some(500),
            },
            &trades,
        )
        .unwrap();

        assert!(!result.has_observations());

        assert_eq!(result.next_baseline.numeric_trade_id, None,);

        assert_eq!(result.next_baseline.cross_sequence, Some(502),);
    }

    #[test]
    fn equal_cross_sequences_are_allowed_when_configured() {
        let trades = vec![trade("A", Some(500)), trade("B", Some(500))];

        let result = TradeIntegrityEvaluator::evaluate(
            &opaque_plan(),
            TradeIntegrityBaseline {
                numeric_trade_id: None,
                cross_sequence: Some(500),
            },
            &trades,
        )
        .unwrap();

        assert!(!result.has_observations());
    }

    #[test]
    fn cross_sequence_regression_is_reported() {
        let trades = vec![trade("A", Some(498))];

        let result = TradeIntegrityEvaluator::evaluate(
            &opaque_plan(),
            TradeIntegrityBaseline {
                numeric_trade_id: None,
                cross_sequence: Some(500),
            },
            &trades,
        )
        .unwrap();

        assert_eq!(
            result.observations[0].kind,
            TradeIntegrityObservationKind::CrossSequenceRegression,
        );

        assert_eq!(result.next_baseline.cross_sequence, Some(500),);
    }

    #[test]
    fn unexpected_equal_cross_sequence_is_reported() {
        let mut plan = opaque_plan();

        plan.allow_equal_cross_sequence = false;

        let result = TradeIntegrityEvaluator::evaluate(
            &plan,
            TradeIntegrityBaseline {
                numeric_trade_id: None,
                cross_sequence: Some(500),
            },
            &[trade("A", Some(500))],
        )
        .unwrap();

        assert_eq!(
            result.observations[0].kind,
            TradeIntegrityObservationKind::UnexpectedEqualCrossSequence,
        );
    }

    #[test]
    fn invalid_numeric_id_rejects_entire_evaluation() {
        let trades = vec![trade("101", None), trade("not-a-number", None)];

        let result = TradeIntegrityEvaluator::evaluate(
            &numeric_plan(),
            TradeIntegrityBaseline {
                numeric_trade_id: Some(100),
                cross_sequence: None,
            },
            &trades,
        );

        assert!(matches!(
            result,
            Err(TradeIntegrityEvaluationError::InvalidNumericTradeId { index: 1, .. })
        ));
    }

    #[test]
    fn missing_cross_sequence_is_rejected_when_required() {
        let result = TradeIntegrityEvaluator::evaluate(
            &opaque_plan(),
            TradeIntegrityBaseline::default(),
            &[trade("A", None)],
        );

        assert!(matches!(
            result,
            Err(TradeIntegrityEvaluationError::MissingCrossSequence { index: 0 })
        ));
    }

    #[test]
    fn empty_batch_preserves_baseline() {
        let baseline = TradeIntegrityBaseline {
            numeric_trade_id: Some(100),
            cross_sequence: Some(500),
        };

        let result = TradeIntegrityEvaluator::evaluate(&numeric_plan(), baseline, &[]).unwrap();

        assert_eq!(result.next_baseline, baseline);
        assert!(result.observations.is_empty());
    }

    #[test]
    fn large_numeric_gap_does_not_overflow() {
        let result = TradeIntegrityEvaluator::evaluate(
            &numeric_plan(),
            TradeIntegrityBaseline {
                numeric_trade_id: Some(0),
                cross_sequence: None,
            },
            &[trade(&u64::MAX.to_string(), None)],
        )
        .unwrap();

        assert_eq!(
            result.observations[0].kind,
            TradeIntegrityObservationKind::NumericIdDiscontinuity,
        );

        assert_eq!(result.next_baseline.numeric_trade_id, Some(u64::MAX),);
    }

    #[test]
    fn aggregate_plan_is_rejected() {
        let mut plan = numeric_plan();

        plan.identity_kind = TradeIdentityKind::Aggregate;
        plan.supports_individual_trades = false;

        let result = TradeIntegrityEvaluator::evaluate(
            &plan,
            TradeIntegrityBaseline::default(),
            &[trade("100", None)],
        );

        assert_eq!(
            result,
            Err(TradeIntegrityEvaluationError::AggregateTradesUnsupported)
        );
    }
}
