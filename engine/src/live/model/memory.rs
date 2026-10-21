//! Logical retained-memory accounting for MarketForge Live.
//!
//! Estimates memory retained by canonical events and event batches.
//!
//! This is a per-destination capacity charge, not a measurement of
//! unique physical process memory.
//!
//! No serialization or heap allocation is performed during accounting.

use std::mem::size_of;

use crate::{
    canonical::{EventEnvelope, L2LevelUpdate, Trade},
    live::{
        model::envelope::{LiveEvent, LivePayload},
        ports::routing::EventBatch,
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryAccountingError {
    Overflow,
}

/// Checked logical retained-memory calculation.
pub trait RetainedSize {
    fn retained_size_bytes(&self) -> Result<u64, MemoryAccountingError>;
}

fn add(a: u64, b: u64) -> Result<u64, MemoryAccountingError> {
    a.checked_add(b).ok_or(MemoryAccountingError::Overflow)
}

fn usize_bytes(value: usize) -> Result<u64, MemoryAccountingError> {
    u64::try_from(value).map_err(|_| MemoryAccountingError::Overflow)
}

fn allocation_bytes<T>(capacity: usize) -> Result<u64, MemoryAccountingError> {
    let bytes = capacity
        .checked_mul(size_of::<T>())
        .ok_or(MemoryAccountingError::Overflow)?;

    usize_bytes(bytes)
}

fn string_bytes(value: &String) -> Result<u64, MemoryAccountingError> {
    usize_bytes(value.capacity())
}

fn optional_string_bytes(value: &Option<String>) -> Result<u64, MemoryAccountingError> {
    match value {
        Some(value) => string_bytes(value),
        None => Ok(0),
    }
}

/// Additional heap allocations owned by a canonical event envelope.
///
/// The inline size of EventEnvelope is accounted for by its parent.
fn envelope_heap_bytes(envelope: &EventEnvelope) -> Result<u64, MemoryAccountingError> {
    add(
        string_bytes(&envelope.symbol)?,
        string_bytes(&envelope.stream_id)?,
    )
}

fn trade_heap_bytes(trade: &Trade) -> Result<u64, MemoryAccountingError> {
    add(
        envelope_heap_bytes(&trade.envelope)?,
        optional_string_bytes(&trade.trade_id)?,
    )
}

fn level_heap_bytes(level: &L2LevelUpdate) -> Result<u64, MemoryAccountingError> {
    envelope_heap_bytes(&level.envelope)
}

impl RetainedSize for LiveEvent {
    fn retained_size_bytes(&self) -> Result<u64, MemoryAccountingError> {
        let mut total = usize_bytes(size_of::<LiveEvent>())?;

        // Cursor stream identity.
        total = add(total, string_bytes(&self.cursor.stream_id.0)?)?;

        // Provenance source identity.
        total = add(total, string_bytes(&self.provenance.source_stream.0)?)?;

        // Optional native trade identifier.
        if let Some(id) = &self.provenance.native_trade_id {
            total = add(total, string_bytes(&id.0)?)?;
        }

        if let super::envelope::EventOrigin::Derived(origin) = &self.provenance.origin {
            total = add(total, string_bytes(&origin.derived_stream.0)?)?;
            total = add(total, string_bytes(&origin.source_cursor.stream_id.0)?)?;
            total = add(
                total,
                allocation_bytes::<super::envelope::ConversionStep>(origin.conversion.capacity())?,
            )?;
            for step in &origin.conversion {
                total = add(total, string_bytes(&step.from_currency)?)?;
                total = add(total, string_bytes(&step.to_currency)?)?;
                total = add(total, string_bytes(&step.reference.stream_id.0)?)?;
            }
        }
        match &self.payload {
            LivePayload::Trade(trade) => {
                total = add(total, trade_heap_bytes(trade)?)?;
            }

            LivePayload::L2Group(group) => {
                // The vector header is already included in LiveEvent.
                // Charge its allocated capacity, not merely its length.
                total = add(
                    total,
                    allocation_bytes::<L2LevelUpdate>(group.grouped_levels.capacity())?,
                )?;

                for level in &group.grouped_levels {
                    total = add(total, level_heap_bytes(level)?)?;
                }
            }
        }

        Ok(total)
    }
}

impl RetainedSize for EventBatch {
    fn retained_size_bytes(&self) -> Result<u64, MemoryAccountingError> {
        // Arc<[Arc<LiveEvent>]> allocation:
        // reference counts plus the allocated slice elements.
        //
        // The EventBatch struct itself is not charged because it is
        // stored inline within QueuedBatch.
        let mut total = allocation_bytes::<usize>(2)?;

        total = add(
            total,
            allocation_bytes::<std::sync::Arc<LiveEvent>>(self.len())?,
        )?;

        for event in self.events() {
            // Charge the logical event allocation and Arc control block.
            //
            // Each destination receives its own logical charge,
            // even when the underlying Arc allocation is shared.
            total = add(total, allocation_bytes::<usize>(2)?)?;
            total = add(total, event.retained_size_bytes()?)?;
        }

        Ok(total)
    }
}
