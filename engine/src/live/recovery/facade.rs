use crate::live::stream::request::CreateSourceStreamRequest;

use super::{
    coordinator::{RecoveryHandle, RecoveryHandleError, RecoveryTicket},
    model::{
        RecoveryAttemptId, RecoveryOperation, RecoveryRequest, RecoveryRequestId,
        RecoveryRequester, RecoverySourceEpoch,
    },
    scheduler::RecoveryScheduledCancellation,
};

use crate::live::infrastructure::rest::HistoricalTradePage;

// ============================================================
// ERROR
// ============================================================

#[derive(Debug, thiserror::Error)]
pub enum StreamRecoveryError {
    #[error("invalid stream recovery binding: {0}")]
    InvalidBinding(String),

    #[error("invalid recovery request: {0}")]
    InvalidRequest(String),

    #[error(transparent)]
    Manager(#[from] RecoveryHandleError),
}

// ============================================================
// STREAM BINDING
// ============================================================

/// The source identity and configuration version
/// associated with one stream's recovery operations.
///
/// This is immutable after construction.
#[derive(Debug, Clone)]
pub struct StreamRecoveryBinding {
    pub stream_id: String,
    pub source: CreateSourceStreamRequest,
    pub configuration_generation: u64,
    pub epoch: RecoverySourceEpoch,
}

impl StreamRecoveryBinding {
    pub fn new(
        stream_id: impl Into<String>,
        source: CreateSourceStreamRequest,
        configuration_generation: u64,
        epoch: RecoverySourceEpoch,
    ) -> Result<Self, StreamRecoveryError> {
        let stream_id = stream_id.into();

        if stream_id.trim().is_empty() {
            return Err(StreamRecoveryError::InvalidBinding(
                "stream ID cannot be empty".into(),
            ));
        }

        if configuration_generation == 0 {
            return Err(StreamRecoveryError::InvalidBinding(
                "configuration generation cannot be zero".into(),
            ));
        }

        Ok(Self {
            stream_id,
            source,
            configuration_generation,
            epoch,
        })
    }
}

// ============================================================
// STREAM RECOVERY HANDLE
// ============================================================

/// Lightweight recovery interface bound to one
/// source-stream identity and source epoch.
///
/// The shared RecoveryHandle is cloned, not recreated.
///
/// All REST acquisitions are still scheduled by the
/// single shared RecoveryCoordinator.
#[derive(Clone)]
pub struct StreamRecoveryHandle {
    manager: RecoveryHandle,
    binding: StreamRecoveryBinding,
}

impl StreamRecoveryHandle {
    pub fn new(manager: RecoveryHandle, binding: StreamRecoveryBinding) -> Self {
        Self { manager, binding }
    }

    pub fn binding(&self) -> &StreamRecoveryBinding {
        &self.binding
    }

    // ========================================================
    // GENERIC ACQUISITION
    // ========================================================

    pub async fn request(
        &self,
        attempt: RecoveryAttemptId,
        operation: RecoveryOperation,
    ) -> Result<RecoveryTicket, StreamRecoveryError> {
        let requester = RecoveryRequester {
            stream_id: self.binding.stream_id.clone(),
            epoch: self.binding.epoch,
            attempt,
        };

        let request = RecoveryRequest::new(
            requester,
            self.binding.source.clone(),
            self.binding.configuration_generation,
            operation,
        )
        .map_err(|error| StreamRecoveryError::InvalidRequest(format!("{error:?}")))?;

        Ok(self.manager.request(request).await?)
    }

    // ========================================================
    // DEPTH
    // ========================================================

    pub async fn request_depth_snapshot(
        &self,
        attempt: RecoveryAttemptId,
        depth: Option<u64>,
    ) -> Result<RecoveryTicket, StreamRecoveryError> {
        self.request(attempt, RecoveryOperation::DepthSnapshot { depth })
            .await
    }

    // ========================================================
    // RECENT TRADES
    // ========================================================

    pub async fn request_recent_trades(
        &self,
        attempt: RecoveryAttemptId,
        limit: Option<u64>,
    ) -> Result<RecoveryTicket, StreamRecoveryError> {
        self.request(attempt, RecoveryOperation::RecentTrades { limit })
            .await
    }

    // ========================================================
    // HISTORICAL TRADES
    // ========================================================

    pub async fn request_historical_trades(
        &self,
        attempt: RecoveryAttemptId,
        page: HistoricalTradePage,
    ) -> Result<RecoveryTicket, StreamRecoveryError> {
        self.request(attempt, RecoveryOperation::HistoricalTrades { page })
            .await
    }

    // ========================================================
    // CANCELLATION
    // ========================================================
    /// Cancel one request belonging to this exact stream,
    /// epoch, and recovery attempt.
    ///
    /// Requests owned by another stream or attempt
    /// cannot be cancelled through this method.
    pub async fn cancel(
        &self,
        attempt: RecoveryAttemptId,
        request_id: RecoveryRequestId,
    ) -> Result<RecoveryScheduledCancellation, StreamRecoveryError> {
        let requester = RecoveryRequester {
            stream_id: self.binding.stream_id.clone(),
            epoch: self.binding.epoch.clone(),
            attempt,
        };

        Ok(self.manager.cancel_owned(requester, request_id).await?)
    }
    /// Cancel requests from this facade's exact source epoch.
    ///
    /// Does not affect requests created for another connection
    /// or subscription generation.
    pub async fn cancel_epoch(
        &self,
    ) -> Result<Vec<RecoveryScheduledCancellation>, StreamRecoveryError> {
        Ok(self
            .manager
            .cancel_epoch(self.binding.stream_id.clone(), self.binding.epoch.clone())
            .await?)
    }
}
