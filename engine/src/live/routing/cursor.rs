//! Explicit per-stream admission frontier. It never allocates event identities
//! or infers ordering across log namespaces. A router owner supplies the first
//! expected cursor from the authoritative source lifecycle.
use crate::live::{model::cursor::StreamCursor, ports::routing::EventBatch};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CursorOrderError {
    Busy,
    Namespace,
    Gap,
    Exhausted,
    InvalidIdentity,
    Closed,
}
#[derive(Debug)]
struct State {
    namespace: StreamCursor,
    next: Option<u64>,
    pending: Option<u64>,
    closed: bool,
    drain_waiter: Option<Waker>,
}
/// Admission responsibility only; this does not establish downstream completion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmissionOrderSnapshot {
    pub stream_id: crate::live::model::identity::StreamId,
    pub log_epoch: crate::live::model::cursor::LogEpoch,
    /// None means the u64 offset namespace is exhausted.
    pub next: Option<StreamCursor>,
    /// Inclusive end of the sole preparation. While present, next is its start.
    pub pending_last: Option<StreamCursor>,
    pub closed: bool,
}

#[derive(Debug, Clone)]
pub struct StreamAdmissionOrder {
    state: Arc<Mutex<State>>,
}
#[derive(Debug)]
#[must_use = "commit after responsibility transfers, or drop to release admission"]
pub struct CursorOrderPermit {
    state: Arc<Mutex<State>>,
    last: StreamCursor,
    committed: bool,
}
impl StreamAdmissionOrder {
    pub fn new(first: StreamCursor) -> Result<Self, CursorOrderError> {
        if first.stream_id.0.trim().is_empty() || first.stream_id.0.len() > 4096 {
            return Err(CursorOrderError::InvalidIdentity);
        }
        Ok(Self {
            state: Arc::new(Mutex::new(State {
                next: Some(first.offset),
                namespace: first,
                pending: None,
                closed: false,
                drain_waiter: None,
            })),
        })
    }
    /// Nonblocking owner admission. Busy never silently queues another batch;
    /// a caller's bounded scheduling/cancellation policy owns any retry.
    pub fn prepare(&self, batch: &EventBatch) -> Result<CursorOrderPermit, CursorOrderError> {
        let mut state = self.state.try_lock().map_err(|_| CursorOrderError::Busy)?;
        if state.closed {
            return Err(CursorOrderError::Closed);
        }
        if state.pending.is_some() {
            return Err(CursorOrderError::Busy);
        }
        if batch.first_cursor().stream_id != state.namespace.stream_id
            || batch.first_cursor().log_epoch != state.namespace.log_epoch
        {
            return Err(CursorOrderError::Namespace);
        }
        let next = state.next.ok_or(CursorOrderError::Exhausted)?;
        if batch.first_cursor().offset != next {
            return Err(CursorOrderError::Gap);
        }
        for pair in batch.events().windows(2) {
            if pair[0].cursor.offset.checked_add(1) != Some(pair[1].cursor.offset) {
                return Err(CursorOrderError::Gap);
            }
        }
        state.pending = Some(batch.last_cursor().offset);
        Ok(CursorOrderPermit {
            state: self.state.clone(),
            last: batch.last_cursor().clone(),
            committed: false,
        })
    }
    /// Fence all future admissions through every clone. Existing permits retain
    /// their responsibility and may still finish; closing never rewinds a cursor.
    pub fn close(&self) {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).closed = true;
    }

    /// Fence admission and wait for the existing permit to commit or abort.
    /// One shutdown coordinator may poll this owner; a later poll replaces its
    /// bounded waiter. Cancellation retains at most one waker, cleared on drain.
    /// Readiness concerns admission only, never destination delivery.
    pub fn poll_shutdown(&self, cx: &mut Context<'_>) -> Poll<()> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.closed = true;
        if state.pending.is_none() {
            state.drain_waiter = None;
            Poll::Ready(())
        } else {
            if !state
                .drain_waiter
                .as_ref()
                .is_some_and(|w| w.will_wake(cx.waker()))
            {
                state.drain_waiter = Some(cx.waker().clone());
            }
            Poll::Pending
        }
    }

    pub fn snapshot(&self) -> AdmissionOrderSnapshot {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        AdmissionOrderSnapshot {
            stream_id: state.namespace.stream_id.clone(),
            log_epoch: state.namespace.log_epoch,
            next: state.next.map(|offset| {
                StreamCursor::new(
                    state.namespace.stream_id.clone(),
                    state.namespace.log_epoch,
                    offset,
                )
            }),
            pending_last: state.pending.map(|offset| {
                StreamCursor::new(
                    state.namespace.stream_id.clone(),
                    state.namespace.log_epoch,
                    offset,
                )
            }),
            closed: state.closed,
        }
    }

    pub fn next(&self) -> Option<StreamCursor> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.next.map(|offset| {
            StreamCursor::new(
                state.namespace.stream_id.clone(),
                state.namespace.log_epoch,
                offset,
            )
        })
    }
}
impl CursorOrderPermit {
    /// Cancellation after an irreversible queue transfer cannot rewind this
    /// frontier. Failed/partial obligations remain the router owner's concern.
    pub fn commit(mut self) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.next = self.last.offset.checked_add(1);
        state.pending = None;
        self.committed = true;
        let waiter = state.drain_waiter.take();
        drop(state);
        if let Some(waiter) = waiter {
            waiter.wake();
        }
    }
}
impl Drop for CursorOrderPermit {
    fn drop(&mut self) {
        if !self.committed {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            state.pending = None;
            let waiter = state.drain_waiter.take();
            drop(state);
            if let Some(waiter) = waiter {
                waiter.wake();
            }
        }
    }
}
