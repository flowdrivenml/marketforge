//! Protected recorded-event decode/cleanup contracts. Backend locking and actual
//! SQL deletion belong to WP-21; this module performs no database mutations.
use super::schema::StoredEvent;
use crate::live::{
    model::replay::{
        ProtectionLease, ProtectionState, ReplayError, ReplayRange, RetentionPositions,
        same_namespace,
    },
    ports::{
        retention::{ReplayPage, ReplayReadRequest},
        routing::EventBatch,
    },
};
use std::{sync::Arc, time::Instant};
pub fn decode_page(
    request: &ReplayReadRequest,
    lease: &mut ProtectionLease,
    rows: &[StoredEvent],
    positions: RetentionPositions,
    now: Instant,
) -> Result<ReplayPage, ReplayError> {
    lease.validate_read(request.owner(), request.protection(), request.range(), now)?;
    let through = rows
        .last()
        .ok_or(ReplayError::Unavailable)?
        .event
        .cursor
        .clone();
    let committed = positions.committed().ok_or(ReplayError::Unavailable)?;
    if !same_namespace(committed, &through) || through.offset > committed.offset {
        return Err(ReplayError::Unavailable);
    }
    let events = rows
        .iter()
        .map(|row| {
            row.decode()
                .map(Arc::new)
                .map_err(|_| ReplayError::SegmentCorrupted)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let batch = EventBatch::new(events).map_err(|_| ReplayError::InvalidEvent)?;
    ReplayPage::new(request, batch, positions)
}
/// Check all independent protections before withdrawing coverage/deleting in one
/// backend-serialized operation. A racing admission must use the same owner lock.
pub fn cleanup_allowed(
    range: &ReplayRange,
    leases: &mut [ProtectionLease],
    now: Instant,
) -> Result<(), ReplayError> {
    let end = range.last().ok_or(ReplayError::Range)?.offset;
    for lease in leases {
        if lease.state(now) != ProtectionState::Active
            || !same_namespace(range.first(), lease.range().first())
        {
            continue;
        }
        let protected_first = match lease.acknowledged() {
            Some(c) => match c.offset.checked_add(1) {
                Some(v) => v,
                None => continue,
            },
            None => lease.range().first().offset,
        };
        let protected_last = lease.range().last().map_or(u64::MAX, |c| c.offset);
        if protected_first <= end && range.first().offset <= protected_last {
            return Err(ReplayError::Ownership);
        }
    }
    Ok(())
}
