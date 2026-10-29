use super::admission::IntegrityAdmissionError;
use super::{
    handle::{IntegrityState, StreamIntegrityHandle},
    processing::IntegrityOutcome,
};
use crate::live::integrity::TradeSourceEpoch;
pub use crate::live::integrity::trades::controller::TradeRecoveryPreflight;
use crate::live::integrity::trades::controller::TradeRecoveryPreflightError;
use crate::live::integrity::trades::evaluator::TradeIntegrityBaseline;
use crate::live::integrity::trades::{
    controller::AcceptedTradeBatch, ingress::BoundTradeRecoveryCommitError,
};
use crate::live::{
    infrastructure::normalization::NormalizedDepth,
    integrity::{
        DepthControllerOutcome, DepthSourceEpoch, DepthSyncToken, RestDepthRequestContext,
    },
};
use crate::shared::domain::canonical::Trade;
// ============================================================
// RECOVERY ERRORS
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IntegrityRecoveryError {
    WrongChannel,
    InvalidRequest(String),
    AdmissionBlocked(IntegrityAdmissionError),
    IntegrityTradeRecoveryError,
    TradeRecoveryPreflight,
}
#[derive(Debug, thiserror::Error)]
pub enum IntegrityTradeRecoveryCommitError {
    #[error("trade recovery completion requires a trade integrity channel")]
    WrongChannel,

    #[error("trade recovery completion blocked by admission: {0:?}")]
    AdmissionBlocked(IntegrityAdmissionError),

    #[error("trade recovery completion rejected: {0:?}")]
    Commit(BoundTradeRecoveryCommitError),
}
/// Failure to validate a trade recovery candidate.
///
/// Unlike normal trade admission, this operation never
/// changes the controller's identity or coverage state.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum IntegrityTradeRecoveryError {
    #[error("trade recovery requires a trade integrity channel")]
    WrongChannel,

    #[error("trade recovery blocked by integrity admission: {0:?}")]
    AdmissionBlocked(IntegrityAdmissionError),

    #[error("trade recovery preflight rejected: {0:?}")]
    Preflight(TradeRecoveryPreflightError),
}
// ============================================================
// RECOVERY RESULT
// ============================================================

/// A normalized recovery result associated with
/// authoritative acquisition and attempt identity.
///
/// The snapshot must already have passed normalization.
///
/// The source epoch and synchronization token must come
/// from the trusted source runtime/recovery bookkeeping.
#[derive(Debug)]
pub enum IntegrityRecoveryResult<'a> {
    DepthSnapshot {
        epoch: DepthSourceEpoch,
        token: DepthSyncToken,
        snapshot: &'a NormalizedDepth,
    },
}

// ============================================================
// RECOVERY PROCESSING
// ============================================================

impl StreamIntegrityHandle {
    /// Prepare a REST-based depth synchronization attempt.
    ///
    /// The request context is produced by trusted runtime
    /// recovery bookkeeping and validated by the existing
    /// depth ingress.
    ///
    /// Calling this does not perform HTTP I/O.
    pub fn prepare_depth_recovery(
        &mut self,
        request: RestDepthRequestContext,
    ) -> Result<(), IntegrityRecoveryError> {
        if let Some(reason) = self.admission.rejection() {
            return Err(IntegrityRecoveryError::AdmissionBlocked(reason));
        }
        match &mut self.state {
            IntegrityState::Depth(ingress) => ingress
                .begin_rest_attempt(request)
                .map_err(IntegrityRecoveryError::InvalidRequest),

            IntegrityState::Trades(_) => Err(IntegrityRecoveryError::WrongChannel),
        }
    }

    /// Deliver normalized recovery data to the
    /// appropriate existing integrity coordinator.
    ///
    /// Obsolete recovery attempts are rejected by the
    /// underlying ingress without corrupting newer state.
    pub fn on_recovery_result(&mut self, result: IntegrityRecoveryResult<'_>) -> IntegrityOutcome {
        if let Some(reason) = self.admission.rejection() {
            return IntegrityOutcome::AdmissionBlocked(reason);
        }
        match (&mut self.state, result) {
            (
                IntegrityState::Depth(ingress),
                IntegrityRecoveryResult::DepthSnapshot {
                    epoch,
                    token,
                    snapshot,
                },
            ) => IntegrityOutcome::Depth(ingress.on_rest_snapshot(epoch, token, snapshot)),

            _ => IntegrityOutcome::WrongChannel,
        }
    }

    /// Expire an outstanding staged WebSocket snapshot,
    /// where supported by the existing depth controller.
    ///
    /// This does not implement general REST request
    /// cancellation or timeout scheduling.
    pub fn expire_pending_depth_snapshot(
        &mut self,
    ) -> Result<Option<DepthControllerOutcome>, IntegrityRecoveryError> {
        match &mut self.state {
            IntegrityState::Depth(ingress) => Ok(ingress.expire_pending_snapshot()),

            IntegrityState::Trades(_) => Err(IntegrityRecoveryError::WrongChannel),
        }
    }

    ///
    /// The caller must obtain `epoch` from the trusted source
    /// runtime, never from an exchange response.
    ///
    /// This operation does not commit, publish or resolve
    /// the controller's coverage hold.
    ///
    /// REST provenance and completeness evidence must be
    /// validated separately before recovery can be committed.
    pub fn preflight_trade_recovery(
        &self,
        epoch: TradeSourceEpoch,
        expected_baseline: TradeIntegrityBaseline,
        candidate: &[Trade],
    ) -> Result<TradeRecoveryPreflight, IntegrityTradeRecoveryError> {
        use IntegrityTradeRecoveryError as Error;

        // Stopped or interrupted handles cannot authorize
        // recovery work for an acquisition.
        if let Some(reason) = self.admission.rejection() {
            return Err(Error::AdmissionBlocked(reason));
        }

        match &self.state {
            IntegrityState::Trades(ingress) => ingress
                .preflight_numeric_recovery(epoch, expected_baseline, candidate)
                .map_err(Error::Preflight),

            IntegrityState::Depth(_) => Err(Error::WrongChannel),
        }
    }
    /// Complete a validated numeric recovery through the
    /// currently authoritative trade integrity ingress.
    ///
    /// The callback acknowledges downstream handoff only.
    /// This operation does not guarantee durable delivery.
    ///
    /// No publication occurs if lifecycle admission, source
    /// epoch, provenance, or controller preflight fails.
    pub fn complete_trade_recovery(
        &mut self,
        epoch: TradeSourceEpoch,
        prepared: TradeRecoveryPreflight,
        publish: impl FnOnce(&[Trade]) -> Result<(), String>,
    ) -> Result<AcceptedTradeBatch, IntegrityTradeRecoveryCommitError> {
        use IntegrityTradeRecoveryCommitError as Error;

        if let Some(reason) = self.admission.rejection() {
            return Err(Error::AdmissionBlocked(reason));
        }

        match &mut self.state {
            IntegrityState::Trades(ingress) => ingress
                .complete_numeric_recovery(epoch, prepared, publish)
                .map_err(Error::Commit),

            IntegrityState::Depth(_) => Err(Error::WrongChannel),
        }
    }
}
