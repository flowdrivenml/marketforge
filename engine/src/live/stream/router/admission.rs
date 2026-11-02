//! Router-issued custody evidence. Records retain only their original source
//! evidence and immediate admission identity, never a recursive lineage chain.
use super::*;
use crate::live::stream::registry::RuntimeGeneration;
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
    Trade(crate::shared::domain::canonical::Trade),
    Depth(RouterDepthEvent),
}
impl NativeIdentity {
    fn trade(t: &crate::shared::domain::canonical::Trade) -> Self {
        Self::Trade(t.clone())
    }
    fn depth(d: &RouterDepthEvent) -> Self {
        Self::Depth(d.clone())
    }
    fn matches(&self, native: &NativeMarketEvent) -> bool {
        match (self, native) {
            (Self::Trade(a), NativeMarketEvent::Trade(b)) => {
                // Decimal equality is numeric; custody also preserves native scale/bits.
                a == b
                    && [
                        Some(a.price),
                        a.quantity_base,
                        a.quantity_quote,
                        a.quantity_contracts,
                        a.trade_iv,
                        a.mark_iv,
                        a.index_price,
                        a.mark_price,
                    ]
                    .into_iter()
                    .zip([
                        Some(b.price),
                        b.quantity_base,
                        b.quantity_quote,
                        b.quantity_contracts,
                        b.trade_iv,
                        b.mark_iv,
                        b.index_price,
                        b.mark_price,
                    ])
                    .all(|(a, b)| a.map(|d| d.serialize()) == b.map(|d| d.serialize()))
            }
            (Self::Depth(a), NativeMarketEvent::Depth(b)) => match (a, b) {
                (RouterDepthEvent::Invalidated { .. }, RouterDepthEvent::Invalidated { .. }) => {
                    a.generations() == b.generations()
                }
                (
                    RouterDepthEvent::Snapshot {
                        baseline: x,
                        timestamp: t,
                        levels: l,
                        ..
                    },
                    RouterDepthEvent::Snapshot {
                        baseline: y,
                        timestamp: u,
                        levels: m,
                        ..
                    },
                )
                | (
                    RouterDepthEvent::Delta {
                        baseline: x,
                        timestamp: t,
                        changes: l,
                        ..
                    },
                    RouterDepthEvent::Delta {
                        baseline: y,
                        timestamp: u,
                        changes: m,
                        ..
                    },
                ) => a.generations() == b.generations() && x == y && t == u && Arc::ptr_eq(l, m),
                _ => false,
            },
            _ => false,
        }
    }
    fn bytes(&self) -> Option<usize> {
        match self {
            Self::Trade(t) => RouterPublication::trades(vec![t.clone()]).retained_bytes(),
            Self::Depth(d) => RouterPublication::Depth(vec![d.clone()].into()).retained_bytes(),
        }
    }
}
#[derive(Debug)]
struct OriginalEvent {
    admission: Arc<AdmissionIdentity>,
    ordinal: u64,
    native: NativeIdentity,
    integrity: Option<Arc<crate::stream_protocol::DataIntegrityEvidence>>,
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
    pub fn integrity(&self) -> Option<&Arc<crate::stream_protocol::DataIntegrityEvidence>> {
        self.original.integrity.as_ref()
    }
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
            .checked_add(self.original.native.bytes()?)?
            .checked_add(self.original.integrity.as_ref().map_or(0, |e| {
                serde_json::to_vec(e.as_ref()).map_or(usize::MAX, |v| v.len())
            }))
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
    pub fn integrity(
        &self,
        index: usize,
    ) -> Option<&Arc<crate::stream_protocol::DataIntegrityEvidence>> {
        self.originals.get(index)?.as_ref()?.integrity.as_ref()
    }
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
        integrity: Option<Arc<crate::stream_protocol::DataIntegrityEvidence>>,
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
                    integrity: if let RouterPublication::Depth(events) = publication {
                        integrity.as_ref().map(|e| {
                            let mut e = (**e).clone();
                            e.id.record_ordinal = Some(index as u64);
                            let (g, b) = events[index].generations();
                            let same_context =
                                e.source_generation == Some(g) && e.book_generation == Some(b);
                            let same_baseline = match &events[index] {
                                RouterDepthEvent::Snapshot { baseline, .. }
                                | RouterDepthEvent::Delta { baseline, .. } => {
                                    e.depth_sequence == Some(baseline.last_sequence)
                                }
                                _ => false,
                            };
                            if !same_context || !same_baseline {
                                e.depth_coverage = None;
                            }
                            if !same_context {
                                e.diagnostic = None;
                            }
                            e.source_generation = Some(g);
                            e.book_generation = Some(b);
                            match &events[index] {
                                RouterDepthEvent::Snapshot { baseline, .. }
                                | RouterDepthEvent::Delta { baseline, .. } => {
                                    e.depth_sequence = Some(baseline.last_sequence);
                                    e.cross_sequence = baseline.cross_sequence;
                                    e.coverage = "internal_book_baseline".into();
                                }
                                _ => {
                                    e.depth_sequence = None;
                                    e.cross_sequence = None;
                                    e.coverage = "book_invalidated".into();
                                }
                            }
                            Arc::new(e)
                        })
                    } else {
                        integrity.clone()
                    },
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
                .checked_add(root.integrity.as_ref().map_or(0, |e| {
                    serde_json::to_vec(e.as_ref()).map_or(usize::MAX, |v| v.len())
                }))?
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
            None,
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
