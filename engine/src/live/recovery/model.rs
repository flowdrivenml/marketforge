use crate::live::{
    infrastructure::rest::{HistoricalTradePage, StreamRestCapabilities},
    stream::request::{CreateSourceStreamRequest, SourceChannel},
};

// ============================================================
// IDENTIFIERS
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RecoveryRequestId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RecoveryJobId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RecoveryAttemptId(pub u64);

// ============================================================
// TRUSTED SOURCE EPOCH
// ============================================================

/// Acquisition identity assigned by the source-stream runtime.
///
/// This is not derived from exchange-controlled payload fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RecoverySourceEpoch {
    pub stream_generation: u64,
    pub connection_generation: u64,
    pub subscription_generation: u64,
}

// ============================================================
// REQUESTER
// ============================================================

/// Identity of one stream-specific recovery attempt.
///
/// Distinct requesters may share one REST acquisition without
/// sharing Integrity state or synchronization authority.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RecoveryRequester {
    pub stream_id: String,
    pub epoch: RecoverySourceEpoch,
    pub attempt: RecoveryAttemptId,
}

// ============================================================
// RECOVERY OPERATION
// ============================================================

/// Operations supported by the current REST facade.
///
/// A historical trade operation retrieves exactly one page.
/// Pagination and historical coverage reconciliation remain
/// outside the REST facade.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryOperation {
    DepthSnapshot { depth: Option<u64> },

    RecentTrades { limit: Option<u64> },

    HistoricalTrades { page: HistoricalTradePage },
}

impl RecoveryOperation {
    pub fn channel(&self) -> SourceChannel {
        match self {
            Self::DepthSnapshot { .. } => SourceChannel::Depth,

            Self::RecentTrades { .. } | Self::HistoricalTrades { .. } => SourceChannel::Trades,
        }
    }

    pub fn is_supported(&self, capabilities: &StreamRestCapabilities) -> bool {
        match self {
            Self::DepthSnapshot { .. } => capabilities.supports_depth_snapshot(),

            Self::RecentTrades { .. } => capabilities.supports_recent_trades(),

            Self::HistoricalTrades { .. } => capabilities.supports_historical_trades(),
        }
    }
}

// ============================================================
// RECOVERY REQUEST
// ============================================================

/// A request for one exchange-native REST acquisition.
///
/// The source binding must originate from the trusted stream
/// registry. The manager must not construct instrument identity
/// by interpreting arbitrary exchange messages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryRequest {
    pub requester: RecoveryRequester,

    /// Immutable source-stream REST binding.
    pub source: CreateSourceStreamRequest,

    /// Configuration revision associated with this binding.
    ///
    /// The runtime supplies this identifier. It must remain
    /// stable for all handles created from the same config.
    pub configuration_generation: u64,

    pub operation: RecoveryOperation,
}

// ============================================================
// REQUEST VALIDATION
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryRequestError {
    EmptyStreamId,
    InvalidSource(String),
    ChannelMismatch,
    ZeroStreamGeneration,
    ZeroAttempt,
    ZeroLimit,
}

impl RecoveryRequest {
    pub fn new(
        requester: RecoveryRequester,
        source: CreateSourceStreamRequest,
        configuration_generation: u64,
        operation: RecoveryOperation,
    ) -> Result<Self, RecoveryRequestError> {
        if requester.stream_id.trim().is_empty() {
            return Err(RecoveryRequestError::EmptyStreamId);
        }

        source
            .validate()
            .map_err(RecoveryRequestError::InvalidSource)?;

        if source.channel != operation.channel() {
            return Err(RecoveryRequestError::ChannelMismatch);
        }

        if requester.epoch.stream_generation == 0 {
            return Err(RecoveryRequestError::ZeroStreamGeneration);
        }

        if requester.attempt.0 == 0 {
            return Err(RecoveryRequestError::ZeroAttempt);
        }

        let valid_limit = match &operation {
            RecoveryOperation::DepthSnapshot { depth } => depth.is_none_or(|value| value > 0),

            RecoveryOperation::RecentTrades { limit } => limit.is_none_or(|value| value > 0),

            RecoveryOperation::HistoricalTrades { page } => {
                page.limit.is_none_or(|value| value > 0)
            }
        };

        if !valid_limit {
            return Err(RecoveryRequestError::ZeroLimit);
        }

        Ok(Self {
            requester,
            source,
            configuration_generation,
            operation,
        })
    }
}
