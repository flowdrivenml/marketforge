use super::{EventEvidence, RouterAdmission};
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
    pub fn timestamp_ns(&self) -> Option<i64> {
        match self {
            Self::Snapshot { timestamp, .. } | Self::Delta { timestamp, .. } => timestamp.latest_source.filter(|t| t.provenance == crate::live::infrastructure::normalization::DepthTimestampProvenance::Exchange).map(|t| t.timestamp_ns),
            Self::Invalidated { .. } => None,
        }
    }
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

/// Fixed-size provenance: a chain preserves the original source, never a lineage list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventOrigin {
    pub stream_id: String,
    pub generation: crate::live::stream::registry::RuntimeGeneration,
    pub publication_sequence: u64,
    pub record_ordinal: u64,
}
#[derive(Debug, Clone)]
pub enum NativeMarketEvent {
    Trade(Trade),
    Depth(RouterDepthEvent),
}
impl NativeMarketEvent {
    pub fn timestamp_ns(&self) -> Option<i64> {
        match self {
            Self::Trade(t) => Some(t.envelope.event_timestamp_ns),
            Self::Depth(depth) => depth.timestamp_ns(),
        }
    }
}
#[derive(Debug, Clone)]
pub struct EventValuation {
    pub target_quote: String,
    pub evaluation: crate::shared::conversion::evaluation::EvaluatedConversion,
    pub reference_generations: Vec<(String, crate::live::stream::registry::RuntimeGeneration)>,
    pub path: Vec<String>,
    /// One value for a trade; one value per native level for a depth update.
    /// Native level keys, base quantities and contract counts remain unchanged.
    pub values: Arc<[crate::shared::conversion::ConvertedTradeValues]>,
}
#[derive(Debug, Clone)]
pub struct DerivedEvent {
    pub origin: EventOrigin,
    pub upstream: EventOrigin,
    pub input_position: usize,
    pub admission_ordinal: u64,
    pub native: NativeMarketEvent,
    /// Untimed native snapshot: establishes a baseline, carries no valuation.
    pub baseline_barrier: bool,
    pub valuation: Option<EventValuation>,
    pub evidence: EventEvidence,
}
impl DerivedEvent {
    pub fn retained_bytes(&self) -> Option<usize> {
        let native = match &self.native {
            NativeMarketEvent::Trade(t) => RouterPublication::trades(vec![t.clone()]),
            NativeMarketEvent::Depth(d) => RouterPublication::Depth(vec![d.clone()].into()),
        };
        let mut size = native
            .retained_bytes()?
            .checked_add(std::mem::size_of::<Self>())?
            .checked_add(self.origin.stream_id.capacity())?
            .checked_add(self.upstream.stream_id.capacity())?;
        if let Some(v) = &self.valuation {
            for (id, _) in &v.reference_generations {
                size = size
                    .checked_add(std::mem::size_of::<(
                        String,
                        crate::live::stream::registry::RuntimeGeneration,
                    )>())?
                    .checked_add(id.capacity())?;
            }
            for id in &v.path {
                size = size
                    .checked_add(std::mem::size_of::<String>())?
                    .checked_add(id.capacity())?;
            }
            size = size.checked_add(v.target_quote.capacity())?.checked_add(
                v.values.len().checked_mul(std::mem::size_of::<
                    crate::shared::conversion::ConvertedTradeValues,
                >())?,
            )?;
            size = size
                .checked_add(v.evaluation.numerator.to_signed_bytes_be().len())?
                .checked_add(v.evaluation.denominator.to_signed_bytes_be().len())?;
            for r in &v.evaluation.references {
                size = size
                    .checked_add(std::mem::size_of_val(r))?
                    .checked_add(r.stream_id.capacity())?;
            }
        }
        size.checked_add(self.evidence.bytes()?)
    }
}

/// One indivisible admission unit. Canonical values are never serialized.
#[derive(Debug, Clone)]
pub enum RouterPublication {
    /// Explicit finite replay completion; ordinary live producers cannot submit it.
    ReplayComplete,
    Derived(Arc<[DerivedEvent]>),
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
            Self::ReplayComplete => {}
            Self::Derived(events) => {
                for e in events.iter() {
                    bytes = bytes.checked_add(e.retained_bytes()?)?;
                }
            }
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
            Self::ReplayComplete => false,
            Self::Derived(events) => events.is_empty(),
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
        /// Overall subscriber book readiness after this complete publication.
        synchronized: bool,
        /// True when every record is valid for this consumer. During baseline
        /// acquisition this can be true while overall synchronization is false.
        event_valid: bool,
        admission: RouterAdmission,
    },
    /// Terminal: queued data is discarded and demand is released.
    ContinuityLost(ContinuityFailure),
}
