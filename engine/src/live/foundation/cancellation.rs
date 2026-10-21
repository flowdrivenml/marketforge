//! Cooperative cancellation for MarketForge Live.
//!
//! Properties:
//! - Cancellation is idempotent.
//! - Cloned tokens share cancellation state.
//! - Each pending future owns at most one waiter registration.
//! - Repeated polling updates the existing waker.
//! - Dropped futures unregister their wakers.
//! - Cancellation cannot lose a registered wakeup.
//! - Wakers are invoked outside the registry lock.
//!
//! Cancellation does not revoke already accepted work.

use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Waker},
};

#[derive(Debug, Default)]
struct CancellationState {
    cancelled: AtomicBool,
    waiters: Mutex<WaiterRegistry>,
}

#[derive(Debug)]
struct WaiterRegistry {
    next_id: Option<u64>,
    waiters: HashMap<u64, Option<Waker>>,
    max_waiters: usize,
}

impl Default for WaiterRegistry {
    fn default() -> Self {
        Self {
            next_id: Some(1),
            waiters: HashMap::new(),
            max_waiters: 4096,
        }
    }
}

/// A clonable cooperative cancellation handle.
#[derive(Debug, Clone, Default)]
pub struct CancellationToken {
    state: Arc<CancellationState>,
}

impl CancellationToken {
    pub fn new() -> Self {
        Self::default()
    }

    /// Requests cancellation and wakes registered waiters.
    ///
    /// Cancellation is permanent and idempotent.
    pub fn cancel(&self) {
        if self.state.cancelled.swap(true, Ordering::AcqRel) {
            return;
        }

        let waiters = {
            let mut registry = self
                .state
                .waiters
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());

            std::mem::take(&mut registry.waiters)
        };

        // Never invoke arbitrary executor wake behavior while
        // holding the registry mutex.
        for (_, waker) in waiters {
            if let Some(waker) = waker {
                waker.wake();
            }
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.state.cancelled.load(Ordering::Acquire)
    }

    /// Construct an explicitly bounded cancellation scope.
    pub fn with_waiter_limit(
        max_waiters: usize,
    ) -> Result<Self, crate::live::model::error::ErrorCode> {
        if max_waiters == 0 {
            return Err(crate::live::model::error::ErrorCode::InvalidInput);
        }
        let token = Self::new();
        token
            .state
            .waiters
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .max_waiters = max_waiters;
        Ok(token)
    }
    /// Reserve one bounded waiter before scheduling. Failure never pretends
    /// cancellation occurred. Unpolled and dropped futures release reservations.
    pub fn try_cancelled(&self) -> Result<Cancelled, crate::live::model::error::ErrorCode> {
        use crate::live::model::error::ErrorCode;
        let mut registry = self.state.waiters.lock().unwrap_or_else(|e| e.into_inner());
        let registration = if self.is_cancelled() {
            None
        } else {
            if registry.waiters.len() >= registry.max_waiters {
                return Err(ErrorCode::Capacity);
            }
            let id = registry.next_id.ok_or(ErrorCode::Limit)?;
            registry.next_id = id.checked_add(1);
            registry.waiters.insert(id, None);
            Some(id)
        };
        Ok(Cancelled {
            token: self.clone(),
            registration,
        })
    }
    /// Compatibility convenience for callers with pre-proven waiter capacity.
    /// Production admission should use try_cancelled to propagate exhaustion.
    pub fn cancelled(&self) -> Cancelled {
        self.try_cancelled()
            .expect("cancellation waiter capacity exhausted")
    }
}

/// Future waiting for cancellation.
///
/// A pending future owns at most one waiter registration.
#[derive(Debug)]
pub struct Cancelled {
    token: CancellationToken,
    registration: Option<u64>,
}

impl Cancelled {
    fn unregister(&mut self) {
        let Some(id) = self.registration.take() else {
            return;
        };

        let mut registry = self
            .token
            .state
            .waiters
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        registry.waiters.remove(&id);
    }
}

impl Future for Cancelled {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.token.is_cancelled() {
            self.unregister();
            return Poll::Ready(());
        }

        let state = Arc::clone(&self.token.state);

        let mut registry = state
            .waiters
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        // Recheck while holding the lock.
        //
        // This closes the race between observing the cancellation
        // flag and registering the waiter.
        if state.cancelled.load(Ordering::Acquire) {
            drop(registry);
            self.unregister();
            return Poll::Ready(());
        }

        match self.registration {
            Some(id) => {
                if let Some(existing) = registry.waiters.get_mut(&id) {
                    if existing.as_ref().is_none_or(|w| !w.will_wake(cx.waker())) {
                        *existing = Some(cx.waker().clone());
                    }
                } else {
                    // Cancellation is permanent and checked under this lock;
                    // missing live registrations must never impersonate it.
                    panic!("missing live cancellation registration");
                }
            }

            None => unreachable!("uncancelled future must own a waiter reservation"),
        }

        Poll::Pending
    }
}

impl Drop for Cancelled {
    fn drop(&mut self) {
        self.unregister();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::live::model::error::ErrorCode;
    #[test]
    fn reservations_are_bounded_and_unpolled_drop_releases() {
        let token = CancellationToken::with_waiter_limit(1).unwrap();
        let first = token.try_cancelled().unwrap();
        assert!(matches!(token.try_cancelled(), Err(ErrorCode::Capacity)));
        drop(first);
        let second = token.try_cancelled().unwrap();
        token.cancel();
        assert!(token.try_cancelled().is_ok());
        drop(second);
        assert!(token.state.waiters.lock().unwrap().waiters.is_empty());
    }
}
