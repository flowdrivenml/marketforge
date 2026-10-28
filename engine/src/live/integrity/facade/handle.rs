use crate::shared::domain::book::BookStore;

use super::admission::IntegrityAdmissionState;
use crate::live::integrity::{
    BoundDepthIngress, BoundTradeIngress, DepthCoverage, DepthSourceBinding, TradeSourceBinding,
};

const STOPPED_GENERATION_ERROR: &str = "stopped integrity handle cannot begin another generation";
// ============================================================
// INTEGRITY CHANNEL
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntegrityChannel {
    Depth,
    Trades,
}

// ============================================================
// INTEGRITY STATUS
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamIntegrityStatus {
    pub channel: IntegrityChannel,
    pub stream_generation: u64,

    /// Only meaningful for depth streams.
    pub depth_synchronized: Option<bool>,

    /// Whether trade admission is currently permitted.
    /// This does not guarantee historical completeness.
    pub trades_admissible: Option<bool>,

    /// Whether trade source provenance is quarantined.
    pub trades_quarantined: Option<bool>,

    pub depth_coverage: Option<DepthCoverage>,
}

// ============================================================
// INTERNAL CHANNEL STATE
// ============================================================

#[derive(Debug)]
pub(super) enum IntegrityState {
    Depth(BoundDepthIngress),
    Trades(BoundTradeIngress),
}

// ============================================================
// STREAM INTEGRITY HANDLE
// ============================================================

/// Integrity state owned by one logical source stream.
///
/// This handle does not own connections, HTTP clients,
/// recovery workers, or publication transports.
///
/// All mutation must occur through the owning stream's
/// serialized execution context.
#[derive(Debug)]
pub struct StreamIntegrityHandle {
    pub(super) state: IntegrityState,
    pub(super) admission: IntegrityAdmissionState,
}

// ============================================================
// CONSTRUCTION
// ============================================================

impl StreamIntegrityHandle {
    pub fn depth(ingress: BoundDepthIngress) -> Self {
        Self {
            state: IntegrityState::Depth(ingress),
            admission: IntegrityAdmissionState::Active,
        }
    }

    pub fn trades(ingress: BoundTradeIngress) -> Self {
        Self {
            state: IntegrityState::Trades(ingress),
            admission: IntegrityAdmissionState::Active,
        }
    }

    pub fn admission_state(&self) -> IntegrityAdmissionState {
        self.admission
    }

    // ========================================================
    // IDENTITY
    // ========================================================

    pub fn channel(&self) -> IntegrityChannel {
        match &self.state {
            IntegrityState::Depth(_) => IntegrityChannel::Depth,
            IntegrityState::Trades(_) => IntegrityChannel::Trades,
        }
    }

    pub fn stream_id(&self) -> &str {
        match &self.state {
            IntegrityState::Depth(ingress) => ingress.binding().stream_id(),

            IntegrityState::Trades(ingress) => ingress.binding().stream_id(),
        }
    }

    pub fn stream_generation(&self) -> u64 {
        match &self.state {
            IntegrityState::Depth(ingress) => ingress.binding().epoch().stream_generation,

            IntegrityState::Trades(ingress) => ingress.binding().epoch().stream_generation,
        }
    }

    // ========================================================
    // STATUS
    // ========================================================

    pub fn status(&self) -> StreamIntegrityStatus {
        match &self.state {
            IntegrityState::Depth(ingress) => StreamIntegrityStatus {
                channel: IntegrityChannel::Depth,
                stream_generation: self.stream_generation(),

                depth_synchronized: Some(ingress.is_synchronized()),

                trades_admissible: None,
                trades_quarantined: None,

                depth_coverage: Some(ingress.controller().coverage()),
            },

            IntegrityState::Trades(ingress) => StreamIntegrityStatus {
                channel: IntegrityChannel::Trades,
                stream_generation: self.stream_generation(),

                depth_synchronized: None,

                trades_admissible: Some(ingress.can_accept_trades()),

                trades_quarantined: Some(ingress.is_quarantined()),

                depth_coverage: None,
            },
        }
    }

    // ========================================================
    // AUTHORITATIVE ORDER BOOK
    // ========================================================

    /// Borrow the currently synchronized L2 book.
    ///
    /// Returns None for trade streams and for depth
    /// streams without an authoritative synchronized book.
    ///
    /// No book clone is performed.
    pub fn book(&self) -> Option<&BookStore> {
        match &self.state {
            IntegrityState::Depth(ingress) if ingress.is_synchronized() => {
                Some(ingress.controller().book())
            }

            _ => None,
        }
    }

    // ========================================================
    // AUTHORITATIVE GENERATION TRANSITIONS
    // ========================================================

    /// Install a trusted newer depth acquisition binding.
    ///
    /// This invalidates previous depth synchronization
    /// according to the existing ingress contract.
    pub fn begin_depth_generation(&mut self, next: DepthSourceBinding) -> Result<(), String> {
        if self.admission == IntegrityAdmissionState::Stopped {
            return Err(STOPPED_GENERATION_ERROR.into());
        }
        match &mut self.state {
            IntegrityState::Depth(ingress) => {
                ingress.begin_generation(next)?;

                self.admission = IntegrityAdmissionState::Active;

                Ok(())
            }

            IntegrityState::Trades(_) => {
                Err("cannot install depth binding on trade integrity".into())
            }
        }
    }

    /// Install a trusted newer trade acquisition binding.
    ///
    /// Existing trade identity and ordering state is reset
    /// by the underlying ingress.
    ///
    /// Historical coverage is not thereby verified.
    pub fn begin_trade_generation(&mut self, next: TradeSourceBinding) -> Result<(), String> {
        if self.admission == IntegrityAdmissionState::Stopped {
            return Err(STOPPED_GENERATION_ERROR.into());
        }
        match &mut self.state {
            IntegrityState::Trades(ingress) => {
                ingress.begin_generation(next)?;

                self.admission = IntegrityAdmissionState::Active;

                Ok(())
            }

            IntegrityState::Depth(_) => {
                Err("cannot install trade binding on depth integrity".into())
            }
        }
    }

    // ========================================================
    // EXPLICIT DEPTH INVALIDATION
    // ========================================================

    /// Invalidate the authoritative depth book.
    ///
    /// The owning runtime must separately arrange
    /// downstream publication invalidation.
    pub fn invalidate_depth(&mut self) -> Result<(), String> {
        match &mut self.state {
            IntegrityState::Depth(ingress) => {
                ingress.invalidate();
                Ok(())
            }

            IntegrityState::Trades(_) => Err("depth invalidation requires a depth stream".into()),
        }
    }
}
