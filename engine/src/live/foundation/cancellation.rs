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
    waiters: HashMap<u64, Waker>,
}

impl Default for WaiterRegistry {
    fn default() -> Self {
        Self {
            next_id: Some(1),
            waiters: HashMap::new(),
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
            waker.wake();
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.state.cancelled.load(Ordering::Acquire)
    }

    /// Creates a future that resolves when cancellation is requested.
    pub fn cancelled(&self) -> Cancelled {
        Cancelled {
            token: self.clone(),
            registration: None,
        }
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
                    if !existing.will_wake(cx.waker()) {
                        *existing = cx.waker().clone();
                    }
                } else {
                    // The registration may have been drained by
                    // cancellation. The cancellation flag is checked
                    // above, so this is not an expected live state.
                    //
                    // Avoid silently allocating a replacement ID.
                    drop(registry);

                    self.unregister();

                    return Poll::Ready(());
                }
            }

            None => {
                let Some(id) = registry.next_id else {
                    // Registration IDs must never wrap or be reused.
                    //
                    // Exhaustion is an exceptional condition.
                    // Failing closed is preferable to a pending future
                    // that can never be awakened.
                    drop(registry);

                    panic!("MarketForge cancellation waiter ID exhausted");
                };

                registry.next_id = id.checked_add(1);
                registry.waiters.insert(id, cx.waker().clone());

                self.registration = Some(id);
            }
        }

        Poll::Pending
    }
}

impl Drop for Cancelled {
    fn drop(&mut self) {
        self.unregister();
    }
}
