//! Bounded runtime ownership of cursor namespaces. Initial frontiers must come
//! from the authoritative source/log lifecycle, never from the arriving batch.
use std::{collections::HashMap, sync::Mutex};

use super::cursor::{AdmissionOrderSnapshot, CursorOrderError, StreamAdmissionOrder};
use crate::live::model::{
    cursor::{LogEpoch, StreamCursor},
    identity::StreamId,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NamespaceError {
    InvalidLimit,
    InvalidCursor(CursorOrderError),
    Capacity,
    AlreadyRegistered,
    Unknown,
    Retired,
    Shutdown,
}

/// Bounded owned namespace observation; retired entries remain tombstones.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceSnapshot {
    pub order: AdmissionOrderSnapshot,
    pub retired: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmissionNamespacesSnapshot {
    pub limit: usize,
    pub shutting_down: bool,
    /// Sorted by stream ID then epoch, including retired namespaces.
    pub namespaces: Vec<NamespaceSnapshot>,
}

#[derive(Debug)]
struct Entry {
    order: StreamAdmissionOrder,
    retired: bool,
}

#[derive(Debug)]
struct State {
    entries: HashMap<(StreamId, LogEpoch), Entry>,
    shutdown: bool,
}

/// One registry per router runtime. Retired entries are bounded tombstones:
/// re-registering the same namespace must never reset its accepted frontier.
/// Exhausting this lifetime budget fails closed; reclamation requires an
/// authoritative persisted namespace lifecycle and is not inferred here.
#[derive(Debug)]
pub struct AdmissionNamespaces {
    limit: usize,
    state: Mutex<State>,
}

impl AdmissionNamespaces {
    pub fn new(limit: usize) -> Result<Self, NamespaceError> {
        if limit == 0 {
            return Err(NamespaceError::InvalidLimit);
        }
        Ok(Self {
            limit,
            state: Mutex::new(State {
                entries: HashMap::new(),
                shutdown: false,
            }),
        })
    }

    pub fn register(&self, first: StreamCursor) -> Result<(), NamespaceError> {
        let order =
            StreamAdmissionOrder::new(first.clone()).map_err(NamespaceError::InvalidCursor)?;
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.shutdown {
            return Err(NamespaceError::Shutdown);
        }
        let key = (first.stream_id, first.log_epoch);
        if state.entries.contains_key(&key) {
            return Err(NamespaceError::AlreadyRegistered);
        }
        if state.entries.len() >= self.limit {
            return Err(NamespaceError::Capacity);
        }
        state.entries.insert(
            key,
            Entry {
                order,
                retired: false,
            },
        );
        Ok(())
    }

    /// Clones share the same frontier and closing fence; they cannot create a
    /// second logical admission owner or revive a retired namespace.
    pub fn get(
        &self,
        stream: &StreamId,
        epoch: LogEpoch,
    ) -> Result<StreamAdmissionOrder, NamespaceError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.shutdown {
            return Err(NamespaceError::Shutdown);
        }
        let entry = state
            .entries
            .get(&(stream.clone(), epoch))
            .ok_or(NamespaceError::Unknown)?;
        if entry.retired {
            return Err(NamespaceError::Retired);
        }
        Ok(entry.order.clone())
    }

    pub fn retire(&self, stream: &StreamId, epoch: LogEpoch) -> Result<(), NamespaceError> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.shutdown {
            return Err(NamespaceError::Shutdown);
        }
        let entry = state
            .entries
            .get_mut(&(stream.clone(), epoch))
            .ok_or(NamespaceError::Unknown)?;
        if entry.retired {
            return Err(NamespaceError::Retired);
        }
        entry.order.close();
        entry.retired = true;
        Ok(())
    }

    pub fn shutdown(&self) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.shutdown = true;
        for entry in state.entries.values() {
            entry.order.close();
        }
    }

    /// Close every namespace and register the sole shutdown coordinator on
    /// outstanding permits. Registration remains fenced after cancellation.
    /// Poll every entry so all pending permits can wake the coordinator.
    pub fn poll_shutdown(&self, cx: &mut std::task::Context<'_>) -> std::task::Poll<()> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.shutdown = true;
        let mut drained = true;
        for entry in state.entries.values() {
            if entry.order.poll_shutdown(cx).is_pending() {
                drained = false;
            }
        }
        if drained {
            std::task::Poll::Ready(())
        } else {
            std::task::Poll::Pending
        }
    }

    /// Membership is sampled under the registry lock; each frontier is sampled
    /// under its own owner lock. Concurrent completions may occur between entries.
    /// No adapter callbacks or delivery claims are involved.
    pub fn snapshot(&self) -> AdmissionNamespacesSnapshot {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut namespaces: Vec<_> = state
            .entries
            .values()
            .map(|entry| NamespaceSnapshot {
                order: entry.order.snapshot(),
                retired: entry.retired,
            })
            .collect();
        namespaces.sort_by(|a, b| {
            a.order
                .stream_id
                .0
                .cmp(&b.order.stream_id.0)
                .then_with(|| a.order.log_epoch.0.cmp(&b.order.log_epoch.0))
        });
        AdmissionNamespacesSnapshot {
            limit: self.limit,
            shutting_down: state.shutdown,
            namespaces,
        }
    }

    pub fn registered_count(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entries
            .len()
    }
}
