//! The WP-06 bounded replay consumer boundary. Implementations validate protection
//! ownership/expiry and pin coverage for the read; IO never creates new cursors.
use crate::live::{
    model::{
        delivery::DeliveryCapacity,
        demand::RetentionSessionId,
        replay::{ProtectionToken, ReplayError, ReplayRange, RetentionPositions},
    },
    ports::routing::{EventBatch, RoutingFuture},
};

#[derive(Debug, Clone)]
pub struct ReplayReadRequest {
    range: ReplayRange,
    limit: DeliveryCapacity,
    owner: RetentionSessionId,
    protection: ProtectionToken,
}
impl ReplayReadRequest {
    pub fn new(
        range: ReplayRange,
        limit: DeliveryCapacity,
        owner: RetentionSessionId,
        protection: ProtectionToken,
    ) -> Result<Self, ReplayError> {
        if limit.max_pending_events == 0 || limit.max_pending_bytes == 0 {
            return Err(ReplayError::Capacity);
        }
        if owner.0.trim().is_empty() {
            return Err(ReplayError::Ownership);
        }
        Ok(Self {
            range,
            limit,
            owner,
            protection,
        })
    }
    pub fn range(&self) -> &ReplayRange {
        &self.range
    }
    pub fn limit(&self) -> DeliveryCapacity {
        self.limit
    }
    pub fn owner(&self) -> &RetentionSessionId {
        &self.owner
    }
    pub fn protection(&self) -> ProtectionToken {
        self.protection
    }
}
#[derive(Debug, Clone)]
pub struct ReplayPage {
    batch: EventBatch,
    continuation: Option<ReplayRange>,
    positions: RetentionPositions,
}
impl ReplayPage {
    pub fn new(
        request: &ReplayReadRequest,
        batch: EventBatch,
        positions: RetentionPositions,
    ) -> Result<Self, ReplayError> {
        use crate::live::model::{memory::RetainedSize, replay::same_namespace};
        request.range.check_page(&batch)?;
        if batch.len() > request.limit.max_pending_events
            || batch
                .retained_size_bytes()
                .map_err(|_| ReplayError::Capacity)?
                > request.limit.max_pending_bytes
        {
            return Err(ReplayError::Capacity);
        }
        if !same_namespace(batch.last_cursor(), positions.accepted())
            || batch.last_cursor().offset > positions.accepted().offset
        {
            return Err(ReplayError::Range);
        }
        let continuation = if request.range.last() == Some(batch.last_cursor())
            || batch.last_cursor().offset == u64::MAX
        {
            None
        } else {
            Some(ReplayRange::resume_after(
                batch.last_cursor().clone(),
                request.range.last().cloned(),
            )?)
        };
        Ok(Self {
            batch,
            continuation,
            positions,
        })
    }
    pub fn batch(&self) -> &EventBatch {
        &self.batch
    }
    pub fn continuation(&self) -> Option<&ReplayRange> {
        self.continuation.as_ref()
    }
    pub fn positions(&self) -> &RetentionPositions {
        &self.positions
    }
}
#[derive(Debug, Clone)]
pub enum ReplayReadOutcome {
    Page(ReplayPage),
    /// Verified current end; never silently skip an unavailable requested cursor.
    AtFrontier(ReplayFrontier),
}
#[derive(Debug, Clone)]
pub struct ReplayFrontier {
    positions: RetentionPositions,
}
impl ReplayFrontier {
    pub fn positions(&self) -> &RetentionPositions {
        &self.positions
    }
}
impl ReplayReadOutcome {
    pub fn at_frontier(
        request: &ReplayReadRequest,
        positions: RetentionPositions,
    ) -> Result<Self, ReplayError> {
        if !crate::live::model::replay::same_namespace(request.range.first(), positions.accepted())
        {
            return Err(ReplayError::Namespace);
        }
        if positions.accepted().offset.checked_add(1) != Some(request.range.first().offset) {
            return Err(ReplayError::Unavailable);
        }
        Ok(Self::AtFrontier(ReplayFrontier { positions }))
    }
}

/// Retain the precise requested namespace, owner and reservation on failure.
#[derive(Debug, Clone, thiserror::Error)]
#[error("retention read failed: {reason}")]
pub struct RetentionReadFailure {
    pub request: ReplayReadRequest,
    pub reason: ReplayError,
}

pub trait RetentionReaderPort: Send + Sync {
    fn read<'a>(
        &'a self,
        request: ReplayReadRequest,
    ) -> RoutingFuture<'a, Result<ReplayReadOutcome, RetentionReadFailure>>;
}

/// Admission pins verified storage coverage atomically with the lease registry.
/// A returned token is scoped to its owner and generation, never a durable
/// post-restart promise. Consumers cannot mutate the owner's ProtectionLease.
pub trait RetentionControlPort: Send + Sync {
    fn protect<'a>(
        &'a self,
        context: &'a super::context::CallContext,
        owner: RetentionSessionId,
        range: ReplayRange,
        capacity: DeliveryCapacity,
        lifetime_ns: u64,
    ) -> RoutingFuture<'a, Result<ProtectionToken, ReplayError>>;
    fn advance<'a>(
        &'a self,
        context: &'a super::context::CallContext,
        owner: RetentionSessionId,
        token: ProtectionToken,
        through: crate::live::model::cursor::StreamCursor,
    ) -> RoutingFuture<'a, Result<(), ReplayError>>;
    fn renew<'a>(
        &'a self,
        context: &'a super::context::CallContext,
        owner: RetentionSessionId,
        token: ProtectionToken,
        lifetime_ns: u64,
    ) -> RoutingFuture<'a, Result<(), ReplayError>>;
    fn release<'a>(
        &'a self,
        context: &'a super::context::CallContext,
        owner: RetentionSessionId,
        token: ProtectionToken,
    ) -> RoutingFuture<'a, Result<(), ReplayError>>;
}

/// Segment storage binds the exact event codec; it preserves cursor identity
/// and original grouped events instead of round-tripping through binary floats.
pub trait SegmentStoragePort: Send + Sync {
    fn append<'a>(
        &'a self,
        context: &'a super::context::CallContext,
        batch: EventBatch,
    ) -> RoutingFuture<'a, Result<(), crate::live::model::error::ControlError>>;
    fn read<'a>(
        &'a self,
        context: &'a super::context::CallContext,
        request: ReplayReadRequest,
    ) -> RoutingFuture<'a, Result<ReplayReadOutcome, RetentionReadFailure>>;
}
