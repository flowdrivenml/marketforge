use std::time::{SystemTime, UNIX_EPOCH};

use thiserror::Error;

use crate::live::{
    infrastructure::normalization::{NormalizedMessage, NormalizerError},
    integrity::{DepthPublicationEvent, RestDepthRequestContext},
    recovery::{RecoveryCompletion, RecoverySourceEpoch},
};

use super::{SourceRunnerError, SourceStreamRunner};

#[derive(Debug, Error)]
pub enum SourceDepthRecoveryError {
    #[error("recovery completion belongs to another stream")]
    WrongStream,

    #[error("recovery completion belongs to another source epoch")]
    WrongEpoch,

    #[error("recovery completion belongs to another REST attempt")]
    WrongAttempt,

    #[error("REST response came from another endpoint")]
    WrongEndpoint,

    #[error("REST acquisition failed: {0}")]
    Acquisition(String),

    #[error("REST response serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("REST depth normalization failed: {0}")]
    Normalization(#[from] NormalizerError),

    #[error("REST response did not normalize as depth")]
    WrongChannel,

    #[error("trusted receive timestamp is unavailable")]
    InvalidTimestamp,

    #[error(transparent)]
    Runner(#[from] SourceRunnerError),
}

/// Complete one REST depth synchronization attempt.
///
/// The request context must be the exact context previously
/// passed to prepare_rest_depth_recovery().
///
/// The mailbox must already have accepted the completion.
///
/// All processing runs synchronously on the source actor.
pub fn complete_depth_recovery(
    runner: &mut SourceStreamRunner,
    completion: RecoveryCompletion,
    context: &RestDepthRequestContext,
    publish_depth: impl FnMut(&[DepthPublicationEvent<'_>]) -> Result<(), String>,
) -> Result<bool, SourceDepthRecoveryError> {
    validate_completion(runner.stream_id(), &completion, context)?;

    // HTTP acquisition success is not book synchronization.
    let response = completion
        .result
        .map_err(|error| SourceDepthRecoveryError::Acquisition(error.to_string()))?;

    if response.endpoint() != context.endpoint_key {
        return Err(SourceDepthRecoveryError::WrongEndpoint);
    }

    if !(200..300).contains(&response.status()) {
        return Err(SourceDepthRecoveryError::Acquisition(format!(
            "HTTP status {}",
            response.status()
        )));
    }

    // The existing REST facade returns parsed JSON.
    //
    // Re-serialize for the existing byte-oriented depth
    // normalizer. No exchange-specific decoding is added here.
    let response_bytes = serde_json::to_vec(response.body())?;

    // Capture a trusted local timestamp at completion handling.
    //
    // This is not the original HTTP receive timestamp.
    // The REST response envelope should eventually preserve
    // that timestamp directly.
    let received_at_ns = system_time_ns()?;

    let normalized = runner.instance().normalizer().normalize_rest_depth(
        &response_bytes,
        received_at_ns,
        context.requested_limit,
    )?;

    let NormalizedMessage::Depth(snapshot) = normalized else {
        return Err(SourceDepthRecoveryError::WrongChannel);
    };

    // Integrity owns the synchronization algorithm.
    //
    // Publication acknowledgement is handled by the existing
    // SourceProcessor recovery path.
    //
    // The caller must not interpret `false` as readiness.
    runner
        .complete_rest_depth_recovery(
            context.source_epoch,
            context.sync_token,
            &snapshot,
            publish_depth,
        )
        .map_err(SourceDepthRecoveryError::Runner)
}

fn validate_completion(
    stream_id: &str,
    completion: &RecoveryCompletion,
    context: &RestDepthRequestContext,
) -> Result<(), SourceDepthRecoveryError> {
    if completion.requester.stream_id != stream_id {
        return Err(SourceDepthRecoveryError::WrongStream);
    }

    let epoch = RecoverySourceEpoch {
        stream_generation: context.source_epoch.stream_generation,
        connection_generation: context.source_epoch.connection_generation,
        subscription_generation: context.source_epoch.subscription_generation,
    };

    if completion.requester.epoch != epoch {
        return Err(SourceDepthRecoveryError::WrongEpoch);
    }

    if completion.requester.attempt.0 != context.sync_token.attempt {
        return Err(SourceDepthRecoveryError::WrongAttempt);
    }

    if context.sync_token.stream_generation != context.source_epoch.stream_generation {
        return Err(SourceDepthRecoveryError::WrongEpoch);
    }

    Ok(())
}

fn system_time_ns() -> Result<i64, SourceDepthRecoveryError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| SourceDepthRecoveryError::InvalidTimestamp)?;

    i64::try_from(duration.as_nanos()).map_err(|_| SourceDepthRecoveryError::InvalidTimestamp)
}
