use crate::live::integrity::{
    DepthPublicationPhase, DepthPublicationState, IntegrityChannel, StreamIntegrityStatus,
};

use super::SourceRunnerState;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceReadiness {
    /// An accepted acquisition has not yet established readiness.
    AwaitingData,

    /// Integrity or continuity must be restored.
    Recovering,

    /// Downstream publication is uncertain or blocked.
    Degraded,

    /// All required readiness conditions have been satisfied.
    Ready,
}

impl SourceReadiness {
    pub fn evaluate(
        runner_state: SourceRunnerState,
        integrity: StreamIntegrityStatus,
        publication: Option<(DepthPublicationPhase, DepthPublicationState)>,
        accepted_trade_publication: bool,
    ) -> Self {
        if runner_state != SourceRunnerState::Connected {
            return Self::Recovering;
        }

        match integrity.channel {
            IntegrityChannel::Depth => {
                let Some((phase, published)) = publication else {
                    return Self::Degraded;
                };

                if phase != DepthPublicationPhase::Ready {
                    return Self::Degraded;
                }

                if published.stream_generation != integrity.stream_generation {
                    return Self::Recovering;
                }

                if integrity.depth_synchronized != Some(true) {
                    return Self::Recovering;
                }

                if published.published_book_generation.is_none() {
                    return Self::AwaitingData;
                }

                Self::Ready
            }

            IntegrityChannel::Trades => {
                if integrity.trades_quarantined == Some(true) {
                    return Self::Degraded;
                }

                if integrity.trades_admissible != Some(true) {
                    return Self::Recovering;
                }

                if !accepted_trade_publication {
                    return Self::AwaitingData;
                }

                Self::Ready
            }
        }
    }
}
