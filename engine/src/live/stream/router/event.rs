use crate::{
    live::integrity::{
        DepthAcceptedTimestamp, DepthPublicationEvent, DepthSequenceBaseline,
        TradeIntegrityBaseline, TradeIntegrityObservation,
    },
    shared::domain::{book::BookChange, canonical::Trade},
};
use std::{mem::size_of, sync::Arc};

/// Owned form of the existing acknowledged depth publication contract.
#[derive(Debug, Clone)]
pub enum RouterDepthEvent {
    Invalidated {
        stream_generation: u64,
        book_generation: u64,
    },
    Snapshot {
        stream_generation: u64,
        book_generation: u64,
        baseline: DepthSequenceBaseline,
        levels: Arc<[BookChange]>,
        timestamp: DepthAcceptedTimestamp,
    },
    Delta {
        stream_generation: u64,
        book_generation: u64,
        baseline: DepthSequenceBaseline,
        changes: Arc<[BookChange]>,
        timestamp: DepthAcceptedTimestamp,
    },
}
impl RouterDepthEvent {
    pub fn generations(&self) -> (u64, u64) {
        match self {
            Self::Invalidated {
                stream_generation,
                book_generation,
            }
            | Self::Snapshot {
                stream_generation,
                book_generation,
                ..
            }
            | Self::Delta {
                stream_generation,
                book_generation,
                ..
            } => (*stream_generation, *book_generation),
        }
    }
}
impl From<&DepthPublicationEvent<'_>> for RouterDepthEvent {
    fn from(event: &DepthPublicationEvent<'_>) -> Self {
        match *event {
            DepthPublicationEvent::Invalidated {
                stream_generation,
                book_generation,
            } => Self::Invalidated {
                stream_generation,
                book_generation,
            },
            DepthPublicationEvent::Snapshot {
                stream_generation,
                book_generation,
                baseline,
                levels,
                timestamp,
            } => Self::Snapshot {
                stream_generation,
                book_generation,
                baseline,
                levels: levels.into(),
                timestamp,
            },
            DepthPublicationEvent::Delta {
                stream_generation,
                book_generation,
                baseline,
                changes,
                timestamp,
            } => Self::Delta {
                stream_generation,
                book_generation,
                baseline,
                changes: changes.into(),
                timestamp,
            },
        }
    }
}

/// Source integrity metadata, copied without changing its financial semantics.
#[derive(Debug, Clone)]
pub struct RouterTradeIntegrity {
    pub received_records: usize,
    pub duplicate_records: usize,
    pub identity_history_truncated: bool,
    pub coverage_history_unverified: bool,
    pub observations: Arc<[TradeIntegrityObservation]>,
    pub baseline: TradeIntegrityBaseline,
}

/// One indivisible admission unit. Canonical values are never serialized.
#[derive(Debug, Clone)]
pub enum RouterPublication {
    Trades {
        records: Arc<[Trade]>,
        recovered: bool,
        integrity: Option<RouterTradeIntegrity>,
    },
    Depth(Arc<[RouterDepthEvent]>),
}
impl RouterPublication {
    pub fn trades(records: Vec<Trade>) -> Self {
        Self::Trades {
            records: records.into(),
            recovered: false,
            integrity: None,
        }
    }
    pub fn depth(events: &[DepthPublicationEvent<'_>]) -> Self {
        Self::Depth(events.iter().map(RouterDepthEvent::from).collect())
    }
    /// Conservative retained payload charge, including string allocations and Arc headers.
    /// Per-queue entry overhead is also charged. Consumer-owned deliveries are outside Router budgets.
    pub fn retained_bytes(&self) -> Option<usize> {
        let mut bytes = size_of::<Self>().checked_add(128)?;
        match self {
            Self::Trades {
                records, integrity, ..
            } => {
                if let Some(metadata) = integrity {
                    bytes = bytes
                        .checked_add(
                            metadata
                                .observations
                                .len()
                                .checked_mul(size_of::<TradeIntegrityObservation>())?,
                        )?
                        .checked_add(32)?;
                }
                bytes = bytes.checked_add(records.len().checked_mul(size_of::<Trade>())?)?;
                for trade in records.iter() {
                    bytes = bytes
                        .checked_add(trade.envelope.symbol.capacity())?
                        .checked_add(trade.envelope.stream_id.capacity())?
                        .checked_add(trade.trade_id.as_ref().map_or(0, String::capacity))?;
                }
            }
            Self::Depth(events) => {
                bytes = bytes.checked_add(
                    events
                        .len()
                        .checked_mul(size_of::<RouterDepthEvent>() + 32)?,
                )?;
                for event in events.iter() {
                    let count = match event {
                        RouterDepthEvent::Snapshot { levels, .. } => levels.len(),
                        RouterDepthEvent::Delta { changes, .. } => changes.len(),
                        _ => 0,
                    };
                    bytes = bytes.checked_add(count.checked_mul(size_of::<BookChange>())?)?;
                }
            }
        }
        Some(bytes)
    }
    pub(crate) fn empty(&self) -> bool {
        match self {
            Self::Trades { records, .. } => records.is_empty(),
            Self::Depth(events) => events.is_empty(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RouterAcceptance {
    /// Per-binding admission sequence, not a durable log cursor.
    pub sequence: u64,
    pub runtime_generation: crate::live::stream::registry::RuntimeGeneration,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContinuityFailure {
    ConsumerCapacity,
    ProducerRevoked,
    RouterShutdown,
    RouterDropped,
}
#[derive(Debug, Clone)]
pub enum RouterDelivery {
    Publication {
        sequence: u64,
        runtime_generation: crate::live::stream::registry::RuntimeGeneration,
        publication: Arc<RouterPublication>,
        synchronized: bool,
    },
    /// Terminal: queued data is discarded and demand is released.
    ContinuityLost(ContinuityFailure),
}
