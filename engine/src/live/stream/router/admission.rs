//! Router-issued custody evidence. Records retain only their original source
//! evidence and immediate admission identity, never a recursive lineage chain.
use super::*;
use crate::live::{integrity::DepthAcceptedTimestamp, stream::registry::RuntimeGeneration};
use std::{mem::size_of, sync::Arc};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublicationSemantics {
    Source,
    Derived,
    CanonicalCompatibility,
}

#[derive(Debug)]
pub(crate) struct AdmissionIdentity {
    pub owner: Arc<()>,
    pub binding: u64,
    pub stream_id: Arc<str>,
    pub generation: RuntimeGeneration,
    pub sequence: u64,
    pub semantics: PublicationSemantics,
    pub records: usize,
}
impl AdmissionIdentity {
    fn bytes(&self) -> Option<usize> {
        size_of::<Self>()
            .checked_add(32)?
            .checked_add(self.stream_id.len())
    }
    fn origin(&self, ordinal: u64) -> EventOrigin {
        EventOrigin {
            stream_id: self.stream_id.to_string(),
            generation: self.generation,
            publication_sequence: self.sequence,
            record_ordinal: ordinal,
        }
    }
}
#[derive(Debug)]
enum NativeIdentity {
    Trade {
        timestamp: i64,
        system: Option<i64>,
        sequence: Option<u64>,
        trade_id: Option<Arc<str>>,
    },
    Depth {
        kind: u8,
        generation: u64,
        book: u64,
        sequence: Option<(u64, Option<u64>, bool)>,
        timestamp: Option<DepthAcceptedTimestamp>,
    },
}
impl NativeIdentity {
    fn trade(t: &crate::shared::domain::canonical::Trade) -> Self {
        Self::Trade {
            timestamp: t.envelope.event_timestamp_ns,
            system: t.envelope.system_timestamp_ns,
            sequence: t.sequence,
            trade_id: t.trade_id.as_deref().map(Arc::from),
        }
    }
    fn depth(d: &RouterDepthEvent) -> Self {
        let (generation, book) = d.generations();
        match d {
            RouterDepthEvent::Invalidated { .. } => Self::Depth {
                kind: 0,
                generation,
                book,
                sequence: None,
                timestamp: None,
            },
            RouterDepthEvent::Snapshot {
                baseline,
                timestamp,
                ..
            }
            | RouterDepthEvent::Delta {
                baseline,
                timestamp,
                ..
            } => Self::Depth {
                kind: if matches!(d, RouterDepthEvent::Snapshot { .. }) {
                    1
                } else {
                    2
                },
                generation,
                book,
                sequence: Some((
                    baseline.last_sequence,
                    baseline.cross_sequence,
                    baseline.synchronized,
                )),
                timestamp: Some(*timestamp),
            },
        }
    }
    fn matches(&self, native: &NativeMarketEvent) -> bool {
        match (self, native) {
            (
                Self::Trade {
                    timestamp,
                    system,
                    sequence,
                    trade_id,
                },
                NativeMarketEvent::Trade(t),
            ) => {
                *timestamp == t.envelope.event_timestamp_ns
                    && *system == t.envelope.system_timestamp_ns
                    && *sequence == t.sequence
                    && trade_id.as_deref() == t.trade_id.as_deref()
            }
            (
                Self::Depth {
                    kind,
                    generation,
                    book,
                    sequence,
                    timestamp,
                },
                NativeMarketEvent::Depth(d),
            ) => {
                let Self::Depth {
                    kind: k,
                    generation: g,
                    book: b,
                    sequence: s,
                    timestamp: t,
                } = Self::depth(d)
                else {
                    unreachable!()
                };
                (*kind, *generation, *book, *sequence, *timestamp) == (k, g, b, s, t)
            }
            _ => false,
        }
    }
    fn bytes(&self) -> Option<usize> {
        size_of::<Self>().checked_add(match self {
            Self::Trade {
                trade_id: Some(id), ..
            } => id.len().checked_add(32)?,
            _ => 0,
        })
    }
}
#[derive(Debug)]
struct OriginalEvent {
    admission: Arc<AdmissionIdentity>,
    ordinal: u64,
    native: NativeIdentity,
}

/// Opaque evidence issued for one actual Router delivery record. Applications
/// may clone evidence, but cannot mint or alter its identities or native keys.
#[derive(Debug, Clone)]
pub struct EventEvidence {
    upstream: Arc<AdmissionIdentity>,
    upstream_ordinal: u64,
    original: Arc<OriginalEvent>,
}
impl EventEvidence {
    pub(crate) fn original_identity(&self) -> &AdmissionIdentity {
        &self.original.admission
    }
    pub(crate) fn upstream_identity(&self) -> &AdmissionIdentity {
        &self.upstream
    }
    pub(crate) fn matches(&self, event: &DerivedEvent, owner: &Arc<()>) -> bool {
        Arc::ptr_eq(&self.upstream.owner, owner)
            && Arc::ptr_eq(&self.original.admission.owner, owner)
            && self.upstream_ordinal < self.upstream.records as u64
            && self.original.ordinal < self.original.admission.records as u64
            && event.upstream == self.upstream.origin(self.upstream_ordinal)
            && event.origin == self.original.admission.origin(self.original.ordinal)
            && self.original.native.matches(&event.native)
    }
    pub(crate) fn bytes(&self) -> Option<usize> {
        size_of::<Self>()
            .checked_add(size_of::<OriginalEvent>())?
            .checked_add(self.upstream.bytes()?)?
            .checked_add(self.original.admission.bytes()?)?
            .checked_add(self.original.native.bytes()?)
    }
}

/// Immutable admission metadata travels with custody. It uses the Router's
/// existing sequence/binding, so delayed records need no historical tracker.
#[derive(Debug, Clone)]
pub struct RouterAdmission {
    identity: Arc<AdmissionIdentity>,
    originals: Arc<[Option<Arc<OriginalEvent>>]>,
}
impl RouterAdmission {
    pub fn semantics(&self) -> PublicationSemantics {
        self.identity.semantics
    }
    pub fn stream_id(&self) -> &str {
        &self.identity.stream_id
    }
    pub fn generation(&self) -> RuntimeGeneration {
        self.identity.generation
    }
    pub fn sequence(&self) -> u64 {
        self.identity.sequence
    }
    fn evidence(&self, index: usize) -> Option<EventEvidence> {
        Some(EventEvidence {
            upstream: self.identity.clone(),
            upstream_ordinal: index as u64,
            original: self.originals.get(index)?.as_ref()?.clone(),
        })
    }
    pub(crate) fn issue(
        owner: Arc<()>,
        binding: u64,
        stream: &str,
        generation: RuntimeGeneration,
        sequence: u64,
        semantics: PublicationSemantics,
        publication: &RouterPublication,
    ) -> Self {
        let records = match publication {
            RouterPublication::Trades { records, .. } => records.len(),
            RouterPublication::Depth(events) => events.len(),
            RouterPublication::Derived(events) => events.len(),
            RouterPublication::ReplayComplete => 0,
        };
        let identity = Arc::new(AdmissionIdentity {
            owner,
            binding,
            stream_id: Arc::from(stream),
            generation,
            sequence,
            semantics,
            records,
        });
        let originals = (0..records)
            .map(|index| {
                if semantics == PublicationSemantics::CanonicalCompatibility {
                    return None;
                }
                if let RouterPublication::Derived(events) = publication {
                    return Some(events[index].evidence.original.clone());
                }
                let native = match publication {
                    RouterPublication::Trades { records, .. } => {
                        NativeIdentity::trade(&records[index])
                    }
                    RouterPublication::Depth(events) => NativeIdentity::depth(&events[index]),
                    _ => unreachable!(),
                };
                Some(Arc::new(OriginalEvent {
                    admission: identity.clone(),
                    ordinal: index as u64,
                    native,
                }))
            })
            .collect();
        Self {
            identity,
            originals,
        }
    }
    pub fn retained_bytes(&self) -> Option<usize> {
        let mut bytes = size_of::<Self>()
            .checked_add(self.identity.bytes()?)?
            .checked_add(
                self.originals
                    .len()
                    .checked_mul(size_of::<Option<Arc<OriginalEvent>>>())?,
            )?;
        for root in self.originals.iter().flatten() {
            bytes = bytes
                .checked_add(size_of::<OriginalEvent>())?
                .checked_add(root.native.bytes()?)?
                .checked_add(root.admission.bytes()?)?;
        }
        Some(bytes)
    }
    #[cfg(test)]
    pub(crate) fn fixture(
        stream: &str,
        generation: RuntimeGeneration,
        sequence: u64,
        publication: &RouterPublication,
    ) -> Self {
        Self::issue(
            Arc::new(()),
            1,
            stream,
            generation,
            sequence,
            PublicationSemantics::Source,
            publication,
        )
    }
}
impl DerivedEvent {
    /// Preserve original identity and replace the immediate hop using genuine
    /// Router custody evidence; compatibility publications are not typed data.
    pub fn from_delivery(
        delivery: &RouterDelivery,
        input_position: usize,
        admission_ordinal: u64,
        index: usize,
    ) -> Option<Self> {
        let RouterDelivery::Publication {
            sequence,
            runtime_generation,
            publication,
            admission,
            event_valid: true,
            ..
        } = delivery
        else {
            return None;
        };
        if *sequence != admission.sequence() || *runtime_generation != admission.generation() {
            return None;
        }
        let evidence = admission.evidence(index)?;
        let (native, valuation) = match publication.as_ref() {
            RouterPublication::Trades { records, .. } => {
                (NativeMarketEvent::Trade(records.get(index)?.clone()), None)
            }
            RouterPublication::Depth(events) => {
                (NativeMarketEvent::Depth(events.get(index)?.clone()), None)
            }
            RouterPublication::Derived(events) => {
                let event = events.get(index)?;
                (event.native.clone(), event.valuation.clone())
            }
            _ => return None,
        };
        let baseline_barrier = matches!(
            native,
            NativeMarketEvent::Depth(RouterDepthEvent::Snapshot { .. })
        ) && native.timestamp_ns().is_none();
        Some(Self {
            origin: evidence
                .original
                .admission
                .origin(evidence.original.ordinal),
            upstream: evidence.upstream.origin(evidence.upstream_ordinal),
            input_position,
            admission_ordinal,
            native,
            baseline_barrier,
            valuation,
            evidence,
        })
    }
}
