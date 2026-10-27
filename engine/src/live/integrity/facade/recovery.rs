use super::admission::IntegrityAdmissionError;
use super::{
    handle::{IntegrityState, StreamIntegrityHandle},
    processing::IntegrityOutcome,
};
use crate::live::{
    infrastructure::normalization::NormalizedDepth,
    integrity::{
        DepthControllerOutcome, DepthSourceEpoch, DepthSyncToken, RestDepthRequestContext,
    },
};

// ============================================================
// RECOVERY ERRORS
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IntegrityRecoveryError {
    WrongChannel,
    InvalidRequest(String),
    AdmissionBlocked(IntegrityAdmissionError),
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
}
