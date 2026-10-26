use thiserror::Error;

use crate::live::{
    infrastructure::normalization::{NormalizedDepth, NormalizedTrades},
    integrity::{
        BoundDepthIngress, BoundTradeIngress, BoundTradeIngressOutcome, DepthControllerOutcome,
        DepthSourceEpoch, TradeSourceEpoch,
    },
};

use super::{ConnectionFence, ConnectionStamp};

// ============================================================
// REJECTION REASONS
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum FencedIngressError {
    #[error("connection generation is not authorized")]
    ConnectionNotReady,

    #[error(
        "source epoch connection generation {source_generation} \
     does not match physical generation {connection_generation}"
    )]
    GenerationMismatch {
        source_generation: u64,
        connection_generation: u64,
    },
}

// ============================================================
// FENCED INGESTION
// ============================================================

/// Transport-generation authorization before delegating
/// to the existing Live integrity controllers.
///
/// This adapter does not:
/// - Allocate source-stream epochs.
/// - Rebind integrity controllers.
/// - Perform depth synchronization.
/// - Deduplicate trades itself.
/// - Publish canonical events.
pub struct FencedIngress;

impl FencedIngress {
    // ========================================================
    // TRADE INGESTION
    // ========================================================

    pub fn trade(
        fence: &ConnectionFence,
        stamp: &ConnectionStamp,
        epoch: TradeSourceEpoch,
        ingress: &mut BoundTradeIngress,
        message: &NormalizedTrades,
    ) -> Result<BoundTradeIngressOutcome, FencedIngressError> {
        Self::validate_generation(epoch.connection_generation, stamp)?;

        fence
            .with_ready(stamp, || ingress.on_websocket(epoch, message))
            .ok_or(FencedIngressError::ConnectionNotReady)
    }

    // ========================================================
    // DEPTH INGESTION
    // ========================================================

    pub fn depth(
        fence: &ConnectionFence,
        stamp: &ConnectionStamp,
        epoch: DepthSourceEpoch,
        ingress: &mut BoundDepthIngress,
        message: NormalizedDepth,
        encoded_bytes: usize,
    ) -> Result<DepthControllerOutcome, FencedIngressError> {
        Self::validate_generation(epoch.connection_generation, stamp)?;

        fence
            .with_ready(stamp, || {
                ingress.on_websocket(epoch, message, encoded_bytes)
            })
            .ok_or(FencedIngressError::ConnectionNotReady)
    }

    // ========================================================
    // GENERATION BINDING
    // ========================================================

    fn validate_generation(
        source_generation: u64,
        stamp: &ConnectionStamp,
    ) -> Result<(), FencedIngressError> {
        let connection_generation = stamp.generation().get();

        if source_generation != connection_generation {
            return Err(FencedIngressError::GenerationMismatch {
                source_generation,
                connection_generation,
            });
        }

        Ok(())
    }
}
