//! Canonical event routing contracts.
//!
//! Defines the boundary between canonical event producers and
//! the MarketForge Live event router.
//!
//! This module does not implement routing, queues, persistence,
//! or publication.

use std::{future::Future, pin::Pin, sync::Arc};

use crate::live::model::{
    cursor::StreamCursor,
    delivery::{
        DeliveryCapacity, DeliveryFailure, DeliveryPolicy, DeliveryStatus, DestinationKind,
    },
    envelope::LiveEvent,
};

/// Shared ownership of one immutable accepted event.
///
/// Consumers must not mutate the underlying event.
pub type SharedLiveEvent = Arc<LiveEvent>;

/// A non-empty collection of accepted events.
///
/// All events must belong to the same stream and log epoch,
/// and must appear in strictly increasing cursor order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventBatch {
    events: Arc<[SharedLiveEvent]>,
}

impl EventBatch {
    pub fn new(events: Vec<SharedLiveEvent>) -> Result<Self, EventBatchError> {
        if events.is_empty() {
            return Err(EventBatchError::Empty);
        }

        for pair in events.windows(2) {
            let previous = &pair[0].cursor;
            let current = &pair[1].cursor;

            if previous.stream_id != current.stream_id || previous.log_epoch != current.log_epoch {
                return Err(EventBatchError::MixedNamespace);
            }

            if previous.offset >= current.offset {
                return Err(EventBatchError::NonIncreasingCursor);
            }
        }

        Ok(Self {
            events: events.into(),
        })
    }

    pub fn events(&self) -> &[SharedLiveEvent] {
        &self.events
    }

    pub fn len(&self) -> usize {
        self.events.len()
    }

    pub fn first_cursor(&self) -> &StreamCursor {
        &self.events[0].cursor
    }

    pub fn last_cursor(&self) -> &StreamCursor {
        &self.events[self.events.len() - 1].cursor
    }
}

/// Validation errors for an accepted-event batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventBatchError {
    Empty,
    MixedNamespace,
    NonIncreasingCursor,
}

/// Identity assigned to one registered destination instance.
///
/// Multiple instances of the same DestinationKind may coexist.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DestinationId(pub u64);

/// Configuration declared by one routing destination.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DestinationDescriptor {
    pub id: DestinationId,
    pub kind: DestinationKind,
    pub policy: DeliveryPolicy,
    pub capacity: DeliveryCapacity,
}

/// The result of submitting a batch to a destination.
///
/// This is an admission result, not a persistence receipt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DestinationAdmission {
    /// The destination accepted responsibility for the batch.
    ///
    /// No durability guarantee is implied.
    Accepted {
        destination: DestinationId,
        through: StreamCursor,
    },

    /// The destination rejected admission.
    Rejected {
        destination: DestinationId,
        reason: DeliveryFailure,
    },
}

/// A subsequent observation about a destination obligation.
///
/// Completion reports are independent of initial admission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DestinationCompletion {
    pub destination: DestinationId,
    pub through: StreamCursor,
    pub status: DeliveryStatus,
}

/// Failure to admit a batch into the canonical router.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoutingAdmissionFailure {
    CapacityExceeded,
    Cancelled,
    DeadlineExceeded,
    ShuttingDown,
    InvalidCursor,
    NoRequiredCapacity,
}

/// The result of canonical router admission.
///
/// This is deliberately distinct from destination completion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoutingAdmission {
    Accepted { through: StreamCursor },

    Rejected { reason: RoutingAdmissionFailure },
}

/// A boxed asynchronous operation.
///
/// The lifetime permits implementations to borrow their owner.
pub type RoutingFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Canonical event admission boundary.
///
/// Implementations must preserve accepted per-stream order,
/// enforce bounded resources, and report admission failures.
///
/// This interface receives already validated events with assigned
/// cursors. It must not allocate replacement cursors.
pub trait EventRouterPort: Send + Sync {
    fn admit<'a>(&'a self, batch: EventBatch) -> RoutingFuture<'a, RoutingAdmission>;
}

/// Destination boundary used by the router.
///
/// A successful admission means that the destination accepted
/// responsibility for the batch under its declared policy.
///
/// It does not establish delivery, database commitment,
/// Parquet commitment, or crash durability.
pub trait EventDestinationPort: Send + Sync {
    fn descriptor(&self) -> DestinationDescriptor;

    fn admit<'a>(&'a self, batch: EventBatch) -> RoutingFuture<'a, DestinationAdmission>;
}
