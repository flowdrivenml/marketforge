//! Source-owner acceptance, separate from downstream router admission.
//!
//! A valid empty Changes group IS one logical event: it preserves native progress.
//! Rejected canonical input consumes no offset. Deduplication/continuity precede
//! this boundary. A downstream failure never rolls back an accepted identity.
use super::{
    cursor::{LogEpoch, StreamCursor},
    envelope::{EventProvenance, EventTiming, LiveEvent, LiveEventError, LivePayload},
    identity::StreamId,
};
use std::{collections::BTreeMap, sync::Arc};

#[derive(Debug, thiserror::Error)]
pub enum AcceptanceError {
    #[error("invalid accepted event: {0:?}")]
    InvalidEvent(LiveEventError),
    #[error("cursor namespace or offset exhausted")]
    Exhausted,
    #[error("epoch journal is fenced after an uncertain write")]
    Fenced,
    #[error("invalid or unsupported epoch journal")]
    InvalidJournal,
    #[error("epoch journal IO: {0}")]
    Io(#[from] std::io::Error),
}

/// Implementations must persist non-reuse before returning an epoch, serialize
/// competing owners, and fail closed on ambiguity/corruption. Only the stream
/// owner may use the returned namespace. Reconnects do not call this operation.
pub trait EpochStore {
    fn fresh_epoch(&mut self, stream: &StreamId) -> Result<LogEpoch, AcceptanceError>;
}

/// Deterministic witness only; this store makes NO restart durability promise.
#[derive(Default)]
pub struct MemoryEpochStore {
    epochs: BTreeMap<String, u64>,
}
impl EpochStore for MemoryEpochStore {
    fn fresh_epoch(&mut self, stream: &StreamId) -> Result<LogEpoch, AcceptanceError> {
        if stream.0.trim().is_empty() {
            return Err(AcceptanceError::InvalidEvent(LiveEventError::EmptyIdentity));
        }
        let next = self
            .epochs
            .get(&stream.0)
            .copied()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or(AcceptanceError::Exhausted)?;
        self.epochs.insert(stream.0.clone(), next);
        Ok(LogEpoch(next))
    }
}

/// A single source-owner allocator. Not Clone; external synchronization must
/// serialize accepted source order. The persisted namespace claim is sufficient
/// to retain identity ownership after the journal lock is released. The registry
/// must still enforce one authoritative source owner; this is not supervision.
pub struct EventAcceptor {
    stream: StreamId,
    epoch: LogEpoch,
    next: Option<u64>,
}
impl EventAcceptor {
    pub fn start(store: &mut dyn EpochStore, stream: StreamId) -> Result<Self, AcceptanceError> {
        if stream.0.trim().is_empty() {
            return Err(AcceptanceError::InvalidEvent(LiveEventError::EmptyIdentity));
        }
        let epoch = store.fresh_epoch(&stream)?;
        Ok(Self {
            stream,
            epoch,
            next: Some(0),
        })
    }
    pub fn next_cursor(&self) -> Result<StreamCursor, AcceptanceError> {
        Ok(StreamCursor::new(
            self.stream.clone(),
            self.epoch,
            self.next.ok_or(AcceptanceError::Exhausted)?,
        ))
    }
    pub fn accept(
        &mut self,
        payload: LivePayload,
        provenance: EventProvenance,
        timing: EventTiming,
    ) -> Result<Arc<LiveEvent>, AcceptanceError> {
        let cursor = self.next_cursor()?;
        let event = LiveEvent::try_new(cursor, payload, provenance, timing)
            .map_err(AcceptanceError::InvalidEvent)?;
        self.next = event.cursor.offset.checked_add(1);
        Ok(Arc::new(event))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn namespace_exhaustion_does_not_wrap() {
        let mut store = MemoryEpochStore::default();
        store.epochs.insert("s".into(), u64::MAX);
        assert!(matches!(
            store.fresh_epoch(&StreamId("s".into())),
            Err(AcceptanceError::Exhausted)
        ));
    }
    #[test]
    fn last_offset_can_be_accepted_once_and_rejection_preserves_it() {
        use crate::{
            formats::depth::{DepthEventBoundary, DepthSourceEventMetadata, DepthSourceOperation},
            live::model::{
                cursor::BookGeneration,
                envelope::{EventOrigin, LiveL2Group},
            },
        };
        let mut store = MemoryEpochStore::default();
        let mut owner = EventAcceptor::start(&mut store, StreamId("s".into())).unwrap();
        owner.next = Some(u64::MAX);
        let payload = LivePayload::L2Group(LiveL2Group::new(
            DepthEventBoundary::Changes,
            DepthSourceOperation::AbsoluteUpdate,
            vec![],
            DepthSourceEventMetadata::default(),
        ));
        let mut provenance = EventProvenance {
            source_stream: StreamId("wrong".into()),
            origin: EventOrigin::Rest,
            native_sequence: None,
            native_trade_id: None,
            book_generation: Some(BookGeneration(1)),
        };
        let timing = EventTiming {
            exchange_timestamp_ns: None,
            received_timestamp_ns: 0,
        };
        assert!(
            owner
                .accept(payload.clone(), provenance.clone(), timing.clone())
                .is_err()
        );
        assert_eq!(owner.next_cursor().unwrap().offset, u64::MAX);
        provenance.source_stream = StreamId("s".into());
        assert_eq!(
            owner
                .accept(payload, provenance, timing)
                .unwrap()
                .cursor
                .offset,
            u64::MAX
        );
        assert!(matches!(
            owner.next_cursor(),
            Err(AcceptanceError::Exhausted)
        ));
    }
    #[test]
    fn offset_exhaustion_does_not_wrap() {
        let mut store = MemoryEpochStore::default();
        let mut owner = EventAcceptor::start(&mut store, StreamId("s".into())).unwrap();
        owner.next = None;
        assert!(matches!(
            owner.next_cursor(),
            Err(AcceptanceError::Exhausted)
        ));
    }
}
