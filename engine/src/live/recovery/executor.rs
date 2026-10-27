use crate::live::infrastructure::rest::{
    ExchangeRestResponse, ExchangeRestService, RestFacadeError, RestFacadeFactory,
    RestOperationError, StreamRestCapabilities, StreamRestHandle,
};

use super::{model::RecoveryOperation, scheduler::RecoveryScheduledJob};

// ============================================================
// EXECUTION ERRORS
// ============================================================

#[derive(Debug, thiserror::Error)]
pub enum RecoveryExecutionError {
    #[error("invalid recovery acquisition: {0}")]
    InvalidAcquisition(String),

    #[error("recovery REST operation is not supported")]
    UnsupportedOperation,

    #[error(transparent)]
    Facade(#[from] RestFacadeError),

    #[error(transparent)]
    Rest(#[from] RestOperationError),

    #[error("recovery manager shut down before acquisition completed")]
    ManagerShutdown,

    #[error("recovery request was cancelled")]
    Cancelled,
}

// ============================================================
// REST EXECUTOR
// ============================================================

/// Executes recovery acquisitions through the existing
/// stream-bound REST Facade.
///
/// This component does not own:
/// - HTTP clients;
/// - rate limiters;
/// - retry schedulers;
/// - Tokio tasks;
/// - Integrity state;
/// - publication state.
///
/// The future Recovery Coordinator will schedule this
/// executor and deliver its result to registered requesters.
pub struct RecoveryRestExecutor<'service, 'config> {
    factory: RestFacadeFactory<'service, 'config>,
}

impl<'service, 'config> RecoveryRestExecutor<'service, 'config> {
    // ========================================================
    // CONSTRUCTION
    // ========================================================

    pub fn new(service: &'service ExchangeRestService<'config>) -> Self {
        Self {
            factory: RestFacadeFactory::new(service),
        }
    }

    // ========================================================
    // EXECUTION
    // ========================================================

    /// Execute one scheduled recovery acquisition.
    ///
    /// This is a single logical REST operation.
    ///
    /// HTTP requests are performed by the existing REST
    /// service using its shared limiter and transport.
    ///
    /// The returned response remains exchange-native.
    /// No normalization or integrity reconciliation occurs.
    pub async fn execute(
        &self,
        job: &RecoveryScheduledJob,
    ) -> Result<ExchangeRestResponse, RecoveryExecutionError> {
        // The scheduled request originates from the trusted
        // recovery registry.
        //
        // Validate its structural binding again before
        // invoking network infrastructure.
        job.request
            .source
            .validate()
            .map_err(RecoveryExecutionError::InvalidAcquisition)?;

        if job.request.source.channel != job.request.operation.channel() {
            return Err(RecoveryExecutionError::InvalidAcquisition(
                "recovery operation channel does not \
                     match source channel"
                    .into(),
            ));
        }

        let handle = self.factory.create(&job.request.source)?;

        Self::execute_bound(&handle, &job.request.operation).await
    }

    // ========================================================
    // STREAM-BOUND EXECUTION
    // ========================================================

    /// Execute a recovery operation using an already
    /// constructed REST handle.
    ///
    /// This is the common dispatch path for all exchange
    /// profiles.
    pub async fn execute_bound(
        handle: &StreamRestHandle<'_, '_>,
        operation: &RecoveryOperation,
    ) -> Result<ExchangeRestResponse, RecoveryExecutionError> {
        Self::validate_capability(operation, handle.capabilities())?;

        match operation {
            // ================================================
            // DEPTH SNAPSHOT
            // ================================================
            RecoveryOperation::DepthSnapshot { depth: None } => Ok(handle.snapshot().await?),

            RecoveryOperation::DepthSnapshot { depth: Some(depth) } => {
                Ok(handle.snapshot_with_depth(*depth).await?)
            }

            // ================================================
            // RECENT TRADES
            // ================================================
            RecoveryOperation::RecentTrades { limit: None } => Ok(handle.recent_trades().await?),

            RecoveryOperation::RecentTrades { limit: Some(limit) } => {
                Ok(handle.recent_trades_with_limit(*limit).await?)
            }

            // ================================================
            // HISTORICAL TRADES
            // ================================================
            RecoveryOperation::HistoricalTrades { page } => {
                Ok(handle.historical_trades(page.clone()).await?)
            }
        }
    }

    // ========================================================
    // CAPABILITY VALIDATION
    // ========================================================

    /// Reject unsupported operations before HTTP execution.
    ///
    /// The REST Facade performs its own validation too.
    /// This explicit check gives Recovery a predictable
    /// admission error without duplicating endpoint logic.
    pub fn validate_capability(
        operation: &RecoveryOperation,
        capabilities: &StreamRestCapabilities,
    ) -> Result<(), RecoveryExecutionError> {
        if !operation.is_supported(capabilities) {
            return Err(RecoveryExecutionError::UnsupportedOperation);
        }

        Ok(())
    }
}
