//! Explicit per-call cancellation, deadline and generation fences.
use crate::live::{
    foundation::{CancellationToken, MonotonicClock},
    model::{error::ErrorCode, operation::ResourceVersion},
};
use std::{sync::Arc, time::Instant};
#[derive(Clone)]
pub struct CallContext {
    pub cancellation: CancellationToken,
    pub deadline: Instant,
    pub expected: Option<ResourceVersion>,
}
impl CallContext {
    /// Owners check again immediately before committing a state transition.
    /// Cancellation never revokes work already committed.
    pub fn check(
        &self,
        clock: &dyn MonotonicClock,
        actual: Option<&ResourceVersion>,
    ) -> Result<(), ErrorCode> {
        if self.cancellation.is_cancelled() {
            return Err(ErrorCode::Cancelled);
        }
        if clock.now() >= self.deadline {
            return Err(ErrorCode::Deadline);
        }
        if self.expected.as_ref().is_some_and(|e| Some(e) != actual) {
            return Err(ErrorCode::StaleGeneration);
        }
        Ok(())
    }
}
#[derive(Clone)]
pub struct OwnerContext {
    pub identity: ResourceVersion,
    pub clock: Arc<dyn MonotonicClock>,
    pub cancellation: CancellationToken,
    pub limits: OwnerLimits,
    pub tasks: Arc<dyn super::lifecycle::TaskSpawnerPort>,
}
#[derive(Debug, Clone, Copy)]
pub struct OwnerLimits {
    pub queued_items: usize,
    pub queued_bytes: usize,
    pub in_flight: usize,
    pub shutdown_ns: u64,
}
impl OwnerLimits {
    pub fn validate(self) -> Result<Self, ErrorCode> {
        if self.queued_items == 0
            || self.queued_bytes == 0
            || self.in_flight == 0
            || self.shutdown_ns == 0
        {
            return Err(ErrorCode::InvalidInput);
        }
        Ok(self)
    }
}
