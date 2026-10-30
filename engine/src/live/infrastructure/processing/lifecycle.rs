use thiserror::Error;

use crate::live::stream::registry::{RuntimeGeneration, StreamRuntimeError, StreamRuntimeRegistry};

use super::{
    pool::{ProcessingPool, ProcessorFailure},
    scheduler::ProcessorToken,
};

/// Result of reconciling a processor failure with its
/// owning stream's runtime generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureReconcileOutcome {
    /// No terminal failure exists for this processor.
    NoFailure,

    /// The failure was applied to the current stream
    /// generation and acknowledged by the processing pool.
    Applied,

    /// The failure belongs to an obsolete stream generation.
    ///
    /// It remains unacknowledged until the lifecycle owner
    /// explicitly handles the obsolete generation.
    Stale,
}

#[derive(Debug, Error)]
pub enum FailureReconcileError {
    #[error(transparent)]
    Runtime(#[from] StreamRuntimeError),

    #[error("processor failure identity mismatch: expected {expected:?}, got {actual:?}")]
    ProcessorMismatch {
        expected: ProcessorToken,
        actual: ProcessorToken,
    },

    #[error("processor failure acknowledgment was lost")]
    AcknowledgmentLost,
}

/// Propagate one processor failure into the authoritative
/// MarketForge stream lifecycle.
///
/// The caller must own lifecycle serialization for this stream.
///
/// This function does not acquire the stream-management lock;
/// the owning controller passes its mutable registry reference.
///
/// No processing pool lock is held while the registry is changed.
///
/// Only acknowledge a failure after mark_failed() succeeds.
/// Obsolete failures remain available for explicit cleanup.
pub fn reconcile_processor_failure(
    registry: &mut StreamRuntimeRegistry,
    pool: &ProcessingPool,
    stream_id: &str,
    stream_generation: RuntimeGeneration,
    processor: ProcessorToken,
) -> Result<FailureReconcileOutcome, FailureReconcileError> {
    // Validate stream identity before inspecting the processor.
    let status = registry
        .status(stream_id)
        .ok_or_else(|| StreamRuntimeError::UnknownStream(stream_id.to_owned()))?;

    let failure = pool.failure(processor.id);

    let Some(ProcessorFailure {
        processor: failed_processor,
        kind,
    }) = failure
    else {
        return Ok(FailureReconcileOutcome::NoFailure);
    };

    if failed_processor != processor {
        return Err(FailureReconcileError::ProcessorMismatch {
            expected: processor,
            actual: failed_processor,
        });
    }

    // The stream controller supplies the processor-to-stream
    // generation association. A processor's local generation
    // must never be assumed to equal the registry generation.
    if status.generation != stream_generation {
        return Ok(FailureReconcileOutcome::Stale);
    }

    let reason = format!("derived processor {:?} failed: {:?}", processor, kind);

    // This operation also revokes the current publication
    // authority inside StreamRuntimeRegistry.
    let applied = registry.mark_failed(stream_id, stream_generation, reason)?;

    if !applied {
        return Ok(FailureReconcileOutcome::Stale);
    }

    // Publication authority is now revoked.
    //
    // The failure may be acknowledged because the authoritative
    // registry has recorded it.
    if !pool.acknowledge_failure(processor) {
        return Err(FailureReconcileError::AcknowledgmentLost);
    }

    Ok(FailureReconcileOutcome::Applied)
}
