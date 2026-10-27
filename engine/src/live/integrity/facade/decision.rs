use crate::live::integrity::{
    BoundTradeIngressOutcome, DepthControllerOutcome, TradeControllerOutcome,
};

use super::processing::IntegrityOutcome;

// ============================================================
// UNIFIED ADMISSION DECISION
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntegrityDecision {
    /// Market data was accepted by Integrity.
    Accepted,

    /// Input was intentionally ignored without acceptance.
    Ignored,

    /// Integrity cannot yet accept or publish this input.
    Pending,

    /// The current depth state requires resynchronization.
    RecoveryRequired,

    /// Input or source provenance was rejected.
    Rejected,
}

// ============================================================
// RUNTIME EFFECTS
// ============================================================

/// Signals which the stream runtime must interpret.
///
/// These are descriptions of required work, not commands
/// executed by Integrity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntegrityEffect {
    /// Accepted data is available in the original outcome.
    AcceptedData,

    /// Depth continuity or synchronization failed.
    ///
    /// The runtime must establish the appropriate recovery
    /// strategy and update downstream publication state.
    DepthRecoveryRequired,

    /// Trade coverage requires investigation.
    ///
    /// This does not establish an exact missing-trade count.
    TradeCoverageReviewRequired,

    /// Trade admission remains suspended by a coverage hold.
    TradeCoverageReviewPending,

    /// Source provenance cannot be trusted.
    SourceQuarantined,
}

// ============================================================
// UNIFIED INTERPRETATION
// ============================================================

impl IntegrityOutcome {
    pub fn decision(&self) -> IntegrityDecision {
        match self {
            IntegrityOutcome::Depth(outcome) => match outcome {
                DepthControllerOutcome::Accepted { .. } => IntegrityDecision::Accepted,

                DepthControllerOutcome::Ignored { .. } => IntegrityDecision::Ignored,

                DepthControllerOutcome::SnapshotStaged { .. } => IntegrityDecision::Pending,

                DepthControllerOutcome::RecoveryRequired { .. } => {
                    IntegrityDecision::RecoveryRequired
                }
            },

            IntegrityOutcome::Trades(outcome) => match outcome {
                BoundTradeIngressOutcome::Processed(TradeControllerOutcome::Accepted(_)) => {
                    IntegrityDecision::Accepted
                }

                BoundTradeIngressOutcome::Processed(
                    TradeControllerOutcome::CoverageReviewRequired { .. }
                    | TradeControllerOutcome::CoverageReviewPending { .. },
                ) => IntegrityDecision::Pending,

                BoundTradeIngressOutcome::Processed(TradeControllerOutcome::ObsoleteGeneration)
                | BoundTradeIngressOutcome::ObsoleteEpoch => IntegrityDecision::Ignored,

                BoundTradeIngressOutcome::Processed(TradeControllerOutcome::Rejected(_))
                | BoundTradeIngressOutcome::SourceQuarantined { .. }
                | BoundTradeIngressOutcome::AdmissionSuspended { .. } => {
                    IntegrityDecision::Rejected
                }
            },
            IntegrityOutcome::AdmissionBlocked(_) => IntegrityDecision::Rejected,
            IntegrityOutcome::WrongChannel => IntegrityDecision::Rejected,
        }
    }

    /// Visit the runtime effects associated with this outcome.
    ///
    /// Avoids allocating a Vec for every incoming message.
    pub fn for_each_effect(&self, mut emit: impl FnMut(IntegrityEffect)) {
        match self {
            IntegrityOutcome::Depth(DepthControllerOutcome::Accepted { .. }) => {
                emit(IntegrityEffect::AcceptedData);
            }

            IntegrityOutcome::Depth(DepthControllerOutcome::RecoveryRequired { .. }) => {
                emit(IntegrityEffect::DepthRecoveryRequired);
            }

            IntegrityOutcome::Trades(BoundTradeIngressOutcome::Processed(
                TradeControllerOutcome::Accepted(_),
            )) => {
                emit(IntegrityEffect::AcceptedData);
            }

            IntegrityOutcome::Trades(BoundTradeIngressOutcome::Processed(
                TradeControllerOutcome::CoverageReviewRequired { .. },
            )) => {
                emit(IntegrityEffect::TradeCoverageReviewRequired);
            }

            IntegrityOutcome::Trades(BoundTradeIngressOutcome::Processed(
                TradeControllerOutcome::CoverageReviewPending { .. },
            )) => {
                emit(IntegrityEffect::TradeCoverageReviewPending);
            }

            IntegrityOutcome::Trades(
                BoundTradeIngressOutcome::SourceQuarantined { .. }
                | BoundTradeIngressOutcome::AdmissionSuspended { .. },
            ) => {
                emit(IntegrityEffect::SourceQuarantined);
            }

            _ => {}
        }
    }
}
