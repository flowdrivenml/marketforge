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
    /// Active and retired entries together are bounded by destination_limit.
    /// Each list is sorted by destination ID.
    pub destinations: Vec<DestinationSnapshot>,
    /// Removed from routing, still owned until queue/capacity drain and reaping.
    pub retired: Vec<DestinationSnapshot>,
}
