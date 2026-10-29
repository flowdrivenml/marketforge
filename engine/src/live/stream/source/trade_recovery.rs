use thiserror::Error;

use crate::live::integrity::{
    StreamIntegrityHandle, TradeCoverageHold, TradeIntegrityBaseline, TradeIntegrityObservation,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceTradeRecoveryReview {
    pub stream_id: String,
    pub stream_generation: u64,

    /// Last committed ordering evidence.
    pub baseline: TradeIntegrityBaseline,

    /// Records in the batch that triggered the hold.
    ///
    /// This is not a missing-trade count.
    pub triggering_records: usize,

    /// Evidence requiring review.
    pub observations: Vec<TradeIntegrityObservation>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SourceTradeRecoveryError {
    #[error("stream has no pending trade coverage hold")]
    NoPendingCoverageReview,

    #[error("trade coverage hold belongs to another generation")]
    ObsoleteGeneration,
}

impl SourceTradeRecoveryReview {
    /// Capture the exact integrity evidence responsible
    /// for suspending trade admission.
    ///
    /// This does not calculate a missing-trade interval,
    /// schedule REST requests, or resolve the coverage hold.
    pub fn capture(integrity: &StreamIntegrityHandle) -> Result<Self, SourceTradeRecoveryError> {
        let hold = integrity
            .trade_coverage_hold()
            .ok_or(SourceTradeRecoveryError::NoPendingCoverageReview)?;

        Self::from_hold(integrity.stream_id(), integrity.stream_generation(), hold)
    }

    pub fn from_hold(
        stream_id: &str,
        current_generation: u64,
        hold: &TradeCoverageHold,
    ) -> Result<Self, SourceTradeRecoveryError> {
        if hold.generation != current_generation {
            return Err(SourceTradeRecoveryError::ObsoleteGeneration);
        }

        Ok(Self {
            stream_id: stream_id.to_owned(),
            stream_generation: hold.generation,
            baseline: hold.baseline,
            triggering_records: hold.received_records,
            observations: hold.observations.clone(),
        })
    }

    /// Verify that this review still belongs to the
    /// currently authoritative integrity generation.
    pub fn is_current(&self, integrity: &StreamIntegrityHandle) -> bool {
        self.stream_id == integrity.stream_id()
            && self.stream_generation == integrity.stream_generation()
            && integrity.trade_coverage_hold().is_some_and(|hold| {
                hold.generation == self.stream_generation
                    && hold.baseline == self.baseline
                    && hold.observations == self.observations
                    && hold.received_records == self.triggering_records
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::live::integrity::TradeIntegrityObservationKind;

    fn hold() -> TradeCoverageHold {
        TradeCoverageHold {
            generation: 10,

            baseline: TradeIntegrityBaseline {
                numeric_trade_id: Some(100),
                cross_sequence: Some(50),
            },

            received_records: 3,

            observations: vec![TradeIntegrityObservation {
                index: 1,
                kind: TradeIntegrityObservationKind::NumericIdDiscontinuity,
                previous: 100,
                current: 105,
            }],
        }
    }

    #[test]
    fn preserves_original_coverage_evidence() {
        let hold = hold();

        let review = SourceTradeRecoveryReview::from_hold("btc-trades", 10, &hold).unwrap();

        assert_eq!(review.stream_id, "btc-trades");
        assert_eq!(review.stream_generation, 10);

        assert_eq!(review.baseline.numeric_trade_id, Some(100));

        assert_eq!(review.triggering_records, 3);

        assert_eq!(review.observations, hold.observations);
    }

    #[test]
    fn obsolete_generation_cannot_capture_review() {
        let hold = hold();

        let result = SourceTradeRecoveryReview::from_hold("btc-trades", 11, &hold);

        assert!(matches!(
            result,
            Err(SourceTradeRecoveryError::ObsoleteGeneration)
        ));
    }

    #[test]
    fn trade_id_discontinuity_is_not_missing_trade_count() {
        let hold = hold();

        let review = SourceTradeRecoveryReview::from_hold("btc-trades", 10, &hold).unwrap();

        let observation = &review.observations[0];

        assert_eq!(observation.previous, 100);
        assert_eq!(observation.current, 105);

        // The review preserves an observation, not an
        // inferred count of missing market executions.
        assert_eq!(
            observation.kind,
            TradeIntegrityObservationKind::NumericIdDiscontinuity
        );
    }

    #[test]
    fn rejects_review_without_matching_generation() {
        let mut hold = hold();

        hold.generation = 9;

        assert!(matches!(
            SourceTradeRecoveryReview::from_hold("btc-trades", 10, &hold,),
            Err(SourceTradeRecoveryError::ObsoleteGeneration)
        ));
    }
    #[test]
    fn review_evidence_is_generation_specific() {
        let original = hold();

        let review = SourceTradeRecoveryReview::from_hold("btc-trades", 10, &original).unwrap();

        assert_eq!(review.stream_generation, 10);

        let mut successor = original.clone();
        successor.generation = 11;

        assert!(matches!(
            SourceTradeRecoveryReview::from_hold("btc-trades", 10, &successor,),
            Err(SourceTradeRecoveryError::ObsoleteGeneration)
        ));

        assert_eq!(review.baseline, original.baseline);
        assert_eq!(review.observations, original.observations);
    }
}
