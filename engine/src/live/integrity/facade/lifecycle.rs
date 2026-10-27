use super::admission::IntegrityAdmissionState;
use super::handle::{IntegrityState, StreamIntegrityHandle};
use crate::live::integrity::{DepthSourceBinding, TradeSourceBinding};

// ============================================================
// LIFECYCLE INPUT
// ============================================================

/// Trusted lifecycle events delivered by the owning
/// source-stream runtime.
///
/// These events must originate from the source registry
/// or connection lifecycle infrastructure, never from
/// exchange-controlled message fields.
#[derive(Debug)]
pub enum IntegrityLifecycleEvent {
    /// Acquisition has been interrupted.
    ///
    /// A transport interruption does not prove exactly
    /// which market-data records were lost.
    SourceInterrupted,

    /// Establish a newer authoritative depth epoch.
    DepthGeneration(DepthSourceBinding),

    /// Establish a newer authoritative trade epoch.
    TradeGeneration(TradeSourceBinding),

    /// Explicitly stop accepting the current source.
    SourceStopped,
}

// ============================================================
// LIFECYCLE OUTCOME
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntegrityLifecycleOutcome {
    /// Depth has been invalidated.
    ///
    /// The runtime must notify publication so consumers
    /// do not continue treating the previous book as live.
    DepthInvalidated,

    /// Trade acquisition continuity has been interrupted.
    ///
    /// The runtime must record the coverage uncertainty.
    /// This is not proof that trades were lost.
    TradeCoverageInterrupted,

    /// A trusted newer acquisition generation is active.
    ///
    /// This does not imply depth synchronization or
    /// complete historical trade coverage.
    GenerationAdvanced,

    /// An explicit stop has been applied.
    SourceStopped,
}

// ============================================================
// LIFECYCLE ERRORS
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IntegrityLifecycleError {
    WrongChannel,
    InvalidGeneration(String),
}

// ============================================================
// LIFECYCLE PROCESSING
// ============================================================

impl StreamIntegrityHandle {
    pub fn on_lifecycle(
        &mut self,
        event: IntegrityLifecycleEvent,
    ) -> Result<IntegrityLifecycleOutcome, IntegrityLifecycleError> {
        match event {
            IntegrityLifecycleEvent::SourceInterrupted => self.on_source_interrupted(),

            IntegrityLifecycleEvent::DepthGeneration(next) => {
                self.begin_depth_generation(next)
                    .map_err(IntegrityLifecycleError::InvalidGeneration)?;

                Ok(IntegrityLifecycleOutcome::GenerationAdvanced)
            }

            IntegrityLifecycleEvent::TradeGeneration(next) => {
                self.begin_trade_generation(next)
                    .map_err(IntegrityLifecycleError::InvalidGeneration)?;

                Ok(IntegrityLifecycleOutcome::GenerationAdvanced)
            }

            IntegrityLifecycleEvent::SourceStopped => {
                if let IntegrityState::Depth(ingress) = &mut self.state {
                    ingress.invalidate();
                }

                self.admission = IntegrityAdmissionState::Stopped;

                Ok(IntegrityLifecycleOutcome::SourceStopped)
            }
        }
    }

    fn on_source_interrupted(
        &mut self,
    ) -> Result<IntegrityLifecycleOutcome, IntegrityLifecycleError> {
        // A stop remains a stop. An old connection
        // notification must not downgrade it.
        if self.admission == IntegrityAdmissionState::Stopped {
            return Ok(IntegrityLifecycleOutcome::SourceStopped);
        }

        let outcome = match &mut self.state {
            IntegrityState::Depth(ingress) => {
                ingress.invalidate();

                IntegrityLifecycleOutcome::DepthInvalidated
            }

            IntegrityState::Trades(_) => IntegrityLifecycleOutcome::TradeCoverageInterrupted,
        };

        self.admission = IntegrityAdmissionState::Interrupted;

        Ok(outcome)
    }
}
