use std::sync::{Arc, Mutex, MutexGuard};

use thiserror::Error;

use super::StreamRecoveryCoordinator;

#[derive(Debug, Error)]
pub enum SharedRecoveryError {
    #[error("stream recovery authority lock is poisoned")]
    Poisoned,
}

/// Shared access to the authoritative recovery coordinator
/// belonging to one logical WebSocket connection slot.
///
/// Cloning this handle does not create another coordinator.
///
/// All recovery mutations must be serialized through
/// this handle.
#[derive(Debug, Clone)]
pub struct SharedStreamRecovery {
    inner: Arc<Mutex<StreamRecoveryCoordinator>>,
}

impl Default for SharedStreamRecovery {
    fn default() -> Self {
        Self::new()
    }
}

impl SharedStreamRecovery {
    pub fn new() -> Self {
        Self::from_coordinator(StreamRecoveryCoordinator::new())
    }

    pub fn from_coordinator(coordinator: StreamRecoveryCoordinator) -> Self {
        Self {
            inner: Arc::new(Mutex::new(coordinator)),
        }
    }

    pub fn lock(&self) -> Result<MutexGuard<'_, StreamRecoveryCoordinator>, SharedRecoveryError> {
        self.inner.lock().map_err(|_| SharedRecoveryError::Poisoned)
    }

    /// True when both handles share the same recovery
    /// state authority.
    pub fn same_authority(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}
