//! Owned, bounded observations of registered router destinations.
//! Queue admission and capacity occupancy are not delivery or durability receipts.
use super::queue::QueueObservation;
use crate::live::ports::routing::DestinationDescriptor;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DestinationSnapshot {
    pub descriptor: DestinationDescriptor,
    /// None means this adapter has no router-owned queue; it is not an empty queue.
    pub queue: Option<QueueObservation>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouterSnapshot {
    pub revision: u64,
    pub shutting_down: bool,
    pub destination_limit: usize,
    /// Sorted by destination ID; bounded by destination_limit.
    pub destinations: Vec<DestinationSnapshot>,
}
