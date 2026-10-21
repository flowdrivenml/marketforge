//! Bounded destination capacity management.
//!
//! Capacity is measured in events and bytes.
//!
//! Reservations own capacity until dropped or committed.
//! Committed capacity remains occupied until its permit is dropped.
//!
//! This module does not own canonical events or implement queues.

use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};

use crate::live::model::delivery::DeliveryCapacity;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapacityError {
    InvalidWaiterCapacity,
    RequestTooLarge,
    Full,
    TooManyWaiters,
    ShuttingDown,
    WaiterIdExhausted,
    AccountingOverflow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CapacityUsage {
    pub occupied_events: usize,
    pub occupied_bytes: u64,
    pub waiting_operations: usize,
    pub shutting_down: bool,
}

#[derive(Debug)]
struct CapacityState {
    occupied_events: usize,
    occupied_bytes: u64,
    next_waiter_id: Option<u64>,
    waiters: HashMap<u64, Waker>,
    shutting_down: bool,
    shutdown_waker: Option<Waker>,
    release_waker: Option<Waker>,
}

impl Default for CapacityState {
    fn default() -> Self {
        Self {
            occupied_events: 0,
            occupied_bytes: 0,
            next_waiter_id: Some(1),
            waiters: HashMap::new(),
            shutting_down: false,
            shutdown_waker: None,
            release_waker: None,
        }
    }
}

#[derive(Debug)]
struct CapacityInner {
    limits: DeliveryCapacity,
    max_waiters: usize,
    state: Mutex<CapacityState>,
}

/// Shared capacity manager for one destination.
///
/// Clones share the same capacity accounting.
#[derive(Debug, Clone)]
pub struct DestinationCapacity {
    inner: Arc<CapacityInner>,
}

impl DestinationCapacity {
    pub fn new(limits: DeliveryCapacity, max_waiters: usize) -> Result<Self, CapacityError> {
        if max_waiters == 0 {
            return Err(CapacityError::InvalidWaiterCapacity);
        }

        if limits.max_pending_events == 0 || limits.max_pending_bytes == 0 {
            return Err(CapacityError::RequestTooLarge);
        }

        Ok(Self {
            inner: Arc::new(CapacityInner {
                limits,
                max_waiters,
                state: Mutex::new(CapacityState::default()),
            }),
        })
    }

    /// Attempt an immediate atomic reservation.
    ///
    /// Both event and byte limits must be satisfied.
    pub fn try_reserve(
        &self,
        events: usize,
        bytes: u64,
    ) -> Result<CapacityReservation, CapacityError> {
        self.validate_request(events, bytes)?;

        let mut state = self.lock_state();

        if state.shutting_down {
            return Err(CapacityError::ShuttingDown);
        }

        let next_events = state
            .occupied_events
            .checked_add(events)
            .ok_or(CapacityError::AccountingOverflow)?;

        let next_bytes = state
            .occupied_bytes
            .checked_add(bytes)
            .ok_or(CapacityError::AccountingOverflow)?;

        if next_events > self.inner.limits.max_pending_events
            || next_bytes > self.inner.limits.max_pending_bytes
        {
            return Err(CapacityError::Full);
        }

        state.occupied_events = next_events;
        state.occupied_bytes = next_bytes;

        Ok(CapacityReservation {
            inner: Arc::clone(&self.inner),
            events,
            bytes,
        })
    }

    /// Create a future that waits for available capacity.
    ///
    /// Cancellation is performed by dropping the future.
    /// Deadline and cancellation coordination belong to admission.rs.
    pub fn reserve(&self, events: usize, bytes: u64) -> CapacityWait {
        CapacityWait {
            manager: self.clone(),
            events,
            bytes,
            registration: None,
        }
    }

    /// Stop accepting new reservations and wake all waiters.
    ///
    /// Existing reservations and occupied permits remain valid.
    pub fn shutdown(&self) {
        let waiters = {
            let mut state = self.lock_state();

            state.shutting_down = true;

            std::mem::take(&mut state.waiters)
        };

        wake_all(waiters);
    }

    /// One replacement-aware shutdown coordinator; payload owners retain their
    /// permits. Register atomically with the drain check to avoid lost wakeups.
    pub fn poll_shutdown(&self, cx: &mut Context<'_>) -> Poll<()> {
        self.shutdown();
        let mut state = self.lock_state();
        if state.occupied_events == 0 && state.occupied_bytes == 0 {
            state.shutdown_waker = None;
            Poll::Ready(())
        } else {
            if state
                .shutdown_waker
                .as_ref()
                .is_none_or(|w| !w.will_wake(cx.waker()))
            {
                state.shutdown_waker = Some(cx.waker().clone());
            }
            Poll::Pending
        }
    }

    /// One replacement-aware coordinator, armed before inspecting owners.
    /// Releases wake it even after closure without consuming admission waiters.
    pub(crate) fn arm_release_notification(&self, waker: &Waker) {
        let mut state = self.lock_state();
        if state
            .release_waker
            .as_ref()
            .is_none_or(|w| !w.will_wake(waker))
        {
            state.release_waker = Some(waker.clone());
        }
    }

    pub fn usage(&self) -> CapacityUsage {
        let state = self.lock_state();

        CapacityUsage {
            occupied_events: state.occupied_events,
            occupied_bytes: state.occupied_bytes,
            waiting_operations: state.waiters.len(),
            shutting_down: state.shutting_down,
        }
    }

    pub fn limits(&self) -> DeliveryCapacity {
        self.inner.limits
    }

    fn validate_request(&self, events: usize, bytes: u64) -> Result<(), CapacityError> {
        if events == 0
            || bytes == 0
            || events > self.inner.limits.max_pending_events
            || bytes > self.inner.limits.max_pending_bytes
        {
            return Err(CapacityError::RequestTooLarge);
        }

        Ok(())
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, CapacityState> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// Capacity reserved for a future destination admission.
///
/// Dropping this object releases the reserved resources.
#[derive(Debug)]
pub struct CapacityReservation {
    inner: Arc<CapacityInner>,
    events: usize,
    bytes: u64,
}

impl CapacityReservation {
    pub fn events(&self) -> usize {
        self.events
    }

    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Transfer reserved capacity into destination ownership.
    ///
    /// This does not establish delivery or durability.
    pub fn commit(self) -> OccupiedCapacity {
        OccupiedCapacity { reservation: self }
    }
    /// Whether this reservation belongs to the supplied capacity manager.
    pub fn belongs_to(&self, manager: &DestinationCapacity) -> bool {
        Arc::ptr_eq(&self.inner, &manager.inner)
    }
}

impl Drop for CapacityReservation {
    fn drop(&mut self) {
        let (waiters, shutdown_waker, release_waker) = {
            let mut state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());

            state.occupied_events = state
                .occupied_events
                .checked_sub(self.events)
                .expect("capacity event accounting underflow");

            state.occupied_bytes = state
                .occupied_bytes
                .checked_sub(self.bytes)
                .expect("capacity byte accounting underflow");

            // Wake all registered waiters.
            //
            // This deliberately avoids selecting a waiter whose
            // request may not fit the newly available capacity.
            //
            // Waiters compete to reserve capacity when polled.
            let shutdown_waker = if state.occupied_events == 0 && state.occupied_bytes == 0 {
                state.shutdown_waker.take()
            } else {
                None
            };
            (
                std::mem::take(&mut state.waiters),
                shutdown_waker,
                state.release_waker.take(),
            )
        };

        wake_all(waiters);
        if let Some(waker) = release_waker {
            waker.wake();
        }
        if let Some(waker) = shutdown_waker {
            waker.wake();
        }
    }
}

/// Capacity owned by a destination after reservation commitment.
///
/// The destination must retain this permit for as long as the
/// corresponding work occupies its bounded resources.
#[derive(Debug)]
pub struct OccupiedCapacity {
    reservation: CapacityReservation,
}

impl OccupiedCapacity {
    pub fn events(&self) -> usize {
        self.reservation.events()
    }

    pub fn bytes(&self) -> u64 {
        self.reservation.bytes()
    }
}

/// Future waiting for an atomic capacity reservation.
///
/// Each pending future owns at most one waiter registration.
#[derive(Debug)]
pub struct CapacityWait {
    manager: DestinationCapacity,
    events: usize,
    bytes: u64,
    registration: Option<u64>,
}

impl CapacityWait {
    fn unregister(&mut self) {
        let Some(id) = self.registration.take() else {
            return;
        };

        let mut state = self.manager.lock_state();
        state.waiters.remove(&id);
    }
}

impl Future for CapacityWait {
    type Output = Result<CapacityReservation, CapacityError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Validate before registering any waiter.
        if let Err(error) = self.manager.validate_request(self.events, self.bytes) {
            self.unregister();
            return Poll::Ready(Err(error));
        }

        let manager = self.manager.clone();

        let mut state = manager.lock_state();

        if state.shutting_down {
            drop(state);
            self.unregister();
            return Poll::Ready(Err(CapacityError::ShuttingDown));
        }

        let next_events = match state.occupied_events.checked_add(self.events) {
            Some(value) => value,
            None => {
                drop(state);
                self.unregister();
                return Poll::Ready(Err(CapacityError::AccountingOverflow));
            }
        };

        let next_bytes = match state.occupied_bytes.checked_add(self.bytes) {
            Some(value) => value,
            None => {
                drop(state);
                self.unregister();
                return Poll::Ready(Err(CapacityError::AccountingOverflow));
            }
        };

        if next_events <= manager.inner.limits.max_pending_events
            && next_bytes <= manager.inner.limits.max_pending_bytes
        {
            // Capacity acquisition and accounting happen under
            // the same lock. This is the reservation linearization
            // point.
            state.occupied_events = next_events;
            state.occupied_bytes = next_bytes;

            if let Some(id) = self.registration.take() {
                state.waiters.remove(&id);
            }

            drop(state);

            return Poll::Ready(Ok(CapacityReservation {
                inner: Arc::clone(&manager.inner),
                events: self.events,
                bytes: self.bytes,
            }));
        }

        match self.registration {
            Some(id) => {
                if let Some(waker) = state.waiters.get_mut(&id) {
                    if !waker.will_wake(cx.waker()) {
                        *waker = cx.waker().clone();
                    }
                } else {
                    // A release may have drained this registration
                    // and scheduled a wakeup. Re-register below.
                    self.registration = None;
                }
            }
            None => {}
        }

        if self.registration.is_none() {
            if state.waiters.len() >= manager.inner.max_waiters {
                return Poll::Ready(Err(CapacityError::TooManyWaiters));
            }

            let Some(id) = state.next_waiter_id else {
                return Poll::Ready(Err(CapacityError::WaiterIdExhausted));
            };

            state.next_waiter_id = id.checked_add(1);
            state.waiters.insert(id, cx.waker().clone());
            self.registration = Some(id);
        }

        Poll::Pending
    }
}

impl Drop for CapacityWait {
    fn drop(&mut self) {
        self.unregister();
    }
}

fn wake_all(waiters: HashMap<u64, Waker>) {
    // Never invoke executor wake behavior while holding
    // the capacity manager's mutex.
    for (_, waker) in waiters {
        waker.wake();
    }
}
