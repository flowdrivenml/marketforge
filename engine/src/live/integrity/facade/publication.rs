use crate::live::integrity::{
    DepthPublicationError, DepthPublicationFence, DepthPublicationPhase, DepthPublicationState,
    PreparedDepthPublication,
};

use super::{
    handle::{IntegrityState, StreamIntegrityHandle},
    processing::IntegrityOutcome,
};

// ============================================================
// PUBLICATION ADAPTER ERRORS
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IntegrityPublicationError {
    WrongChannel,
    Fence(DepthPublicationError),
}

impl From<DepthPublicationError> for IntegrityPublicationError {
    fn from(value: DepthPublicationError) -> Self {
        Self::Fence(value)
    }
}

// ============================================================
// PUBLICATION ADAPTER
// ============================================================

/// Coordinates safe depth publication using the existing
/// acknowledged publication fence.
///
/// Does not write to a socket, queue, database, or consumer.
///
/// The runtime must serialize:
///
/// - Integrity state transitions.
/// - Publication preparation.
/// - Downstream handoff.
/// - Handoff acknowledgement or failure reporting.
#[derive(Debug)]
pub struct IntegrityPublicationAdapter {
    fence: DepthPublicationFence,
}

impl IntegrityPublicationAdapter {
    /// Construct a publication adapter for a depth stream.
    pub fn new(integrity: &StreamIntegrityHandle) -> Result<Self, IntegrityPublicationError> {
        let IntegrityState::Depth(ingress) = &integrity.state else {
            return Err(IntegrityPublicationError::WrongChannel);
        };

        let controller = ingress.controller();

        let fence =
            DepthPublicationFence::new(controller.stream_id(), controller.stream_generation())
                .map_err(|_| {
                    IntegrityPublicationError::Fence(DepthPublicationError::PreparationIdExhausted)
                })?;

        Ok(Self { fence })
    }

    /// Prepare accepted depth data, or an invalidation
    /// resulting from an Integrity state transition.
    ///
    /// Must be called before processing a subsequent
    /// mutation of this stream's depth state when the
    /// outcome contains accepted depth data.
    pub fn prepare<'a>(
        &mut self,
        integrity: &StreamIntegrityHandle,
        outcome: &'a IntegrityOutcome,
    ) -> Result<PreparedDepthPublication<'a>, IntegrityPublicationError> {
        let IntegrityState::Depth(ingress) = &integrity.state else {
            return Err(IntegrityPublicationError::WrongChannel);
        };

        let IntegrityOutcome::Depth(depth_outcome) = outcome else {
            return Err(IntegrityPublicationError::WrongChannel);
        };

        self.fence
            .prepare(ingress.controller(), depth_outcome)
            .map_err(Into::into)
    }

    /// Prepare an invalidation or generation transition
    /// even when no depth message produced an outcome.
    ///
    /// This should be used after an applicable lifecycle
    /// transition, including source interruption.
    pub fn prepare_state_change(
        &mut self,
        integrity: &StreamIntegrityHandle,
    ) -> Result<PreparedDepthPublication<'static>, IntegrityPublicationError> {
        let IntegrityState::Depth(ingress) = &integrity.state else {
            return Err(IntegrityPublicationError::WrongChannel);
        };

        self.fence
            .prepare_state_change(ingress.controller())
            .map_err(Into::into)
    }

    /// Record a successful downstream handoff.
    ///
    /// This acknowledges handoff to the publication
    /// boundary, not end-to-end consumer persistence.
    pub fn acknowledge(
        &mut self,
        prepared: PreparedDepthPublication<'_>,
    ) -> Result<(), IntegrityPublicationError> {
        self.fence.acknowledge(prepared).map_err(Into::into)
    }

    /// Record a failed or uncertain downstream handoff.
    ///
    /// The publication fence will require its explicit
    /// reset protocol before normal deltas can resume.
    pub fn handoff_failed(
        &mut self,
        prepared: PreparedDepthPublication<'_>,
    ) -> Result<(), IntegrityPublicationError> {
        self.fence.handoff_failed(prepared).map_err(Into::into)
    }

    /// Mark a lost outstanding preparation as uncertain.
    pub fn mark_outstanding_handoff_uncertain(&mut self) -> Result<(), IntegrityPublicationError> {
        self.fence
            .mark_outstanding_handoff_uncertain()
            .map_err(Into::into)
    }

    /// Prepare consumer invalidation after a failed handoff.
    pub fn prepare_reset_invalidation(
        &mut self,
    ) -> Result<PreparedDepthPublication<'static>, IntegrityPublicationError> {
        self.fence.prepare_reset_invalidation().map_err(Into::into)
    }

    /// Prepare a complete replacement snapshot after
    /// reset invalidation was acknowledged.
    pub fn prepare_reset_snapshot<'a>(
        &mut self,
        integrity: &StreamIntegrityHandle,
        outcome: &'a IntegrityOutcome,
    ) -> Result<PreparedDepthPublication<'a>, IntegrityPublicationError> {
        let IntegrityState::Depth(ingress) = &integrity.state else {
            return Err(IntegrityPublicationError::WrongChannel);
        };

        let IntegrityOutcome::Depth(depth_outcome) = outcome else {
            return Err(IntegrityPublicationError::WrongChannel);
        };

        self.fence
            .prepare_reset_snapshot(ingress.controller(), depth_outcome)
            .map_err(Into::into)
    }

    pub fn state(&self) -> DepthPublicationState {
        self.fence.state()
    }

    pub fn phase(&self) -> DepthPublicationPhase {
        self.fence.phase()
    }

    pub fn has_outstanding_preparation(&self) -> bool {
        self.fence.has_outstanding_preparation()
    }
}
