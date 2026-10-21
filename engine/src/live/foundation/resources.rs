//! Mandatory count/byte accounting, independent of optional measurements.
//! A reservation retains capacity until ownership is dropped, including unwind.
use crate::live::model::error::ErrorCode;
use std::{
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ResourceUsage {
    pub items: usize,
    pub bytes: usize,
}
#[derive(Debug)]
struct State {
    limit: ResourceUsage,
    used: ResourceUsage,
    closed: bool,
    shutdown_waker: Option<Waker>,
}
#[derive(Debug, Clone)]
pub struct ResourceBudget(Arc<Mutex<State>>);
impl ResourceBudget {
    pub fn new(limit: ResourceUsage) -> Result<Self, ErrorCode> {
        if limit.items == 0 || limit.bytes == 0 {
            return Err(ErrorCode::InvalidInput);
        }
        Ok(Self(Arc::new(Mutex::new(State {
            limit,
            used: ResourceUsage::default(),
            closed: false,
            shutdown_waker: None,
        }))))
    }
    /// Immediate, atomic admission in both dimensions. Owners may wait using
    /// their bounded scheduler; no unbounded waiter queue is hidden here.
    pub fn reserve(&self, amount: ResourceUsage) -> Result<ResourceReservation, ErrorCode> {
        if amount.items == 0 {
            return Err(ErrorCode::InvalidInput);
        }
        let mut state = self.0.lock().unwrap_or_else(|e| e.into_inner());
        if state.closed {
            return Err(ErrorCode::Unavailable);
        }
        let next = ResourceUsage {
            items: state
                .used
                .items
                .checked_add(amount.items)
                .ok_or(ErrorCode::Capacity)?,
            bytes: state
                .used
                .bytes
                .checked_add(amount.bytes)
                .ok_or(ErrorCode::Capacity)?,
        };
        if next.items > state.limit.items || next.bytes > state.limit.bytes {
            return Err(ErrorCode::Capacity);
        }
        state.used = next;
        Ok(ResourceReservation {
            budget: self.clone(),
            amount,
        })
    }
    pub fn usage(&self) -> ResourceUsage {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).used
    }
    /// Stop new admission without revoking outstanding obligations.
    pub fn close(&self) {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).closed = true;
    }
    /// Fence admission and register one replaceable drain coordinator. This
    /// does not revoke reservations or allocate a queue of shutdown waiters.
    pub fn poll_shutdown(&self, cx: &mut Context<'_>) -> Poll<()> {
        let mut state = self.0.lock().unwrap_or_else(|e| e.into_inner());
        state.closed = true;
        if state.used == ResourceUsage::default() {
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
}
#[derive(Debug)]
pub struct ResourceReservation {
    budget: ResourceBudget,
    amount: ResourceUsage,
}
impl ResourceReservation {
    pub fn amount(&self) -> ResourceUsage {
        self.amount
    }
}
impl Drop for ResourceReservation {
    fn drop(&mut self) {
        let waker = {
            let mut state = self.budget.0.lock().unwrap_or_else(|e| e.into_inner());
            state.used.items -= self.amount.items;
            state.used.bytes -= self.amount.bytes;
            if state.used == ResourceUsage::default() {
                state.shutdown_waker.take()
            } else {
                None
            }
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn shutdown_wakes_latest_coordinator_only_after_final_charge_outside_lock() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::task::Wake;
        struct Observer {
            budget: ResourceBudget,
            wakes: AtomicUsize,
        }
        impl Wake for Observer {
            fn wake(self: Arc<Self>) {
                assert_eq!(self.budget.usage(), ResourceUsage::default());
                self.wakes.fetch_add(1, Ordering::SeqCst);
            }
        }
        let budget = ResourceBudget::new(ResourceUsage { items: 2, bytes: 2 }).unwrap();
        let first = budget
            .reserve(ResourceUsage { items: 1, bytes: 1 })
            .unwrap();
        let last = budget
            .reserve(ResourceUsage { items: 1, bytes: 1 })
            .unwrap();
        let observer = || {
            Arc::new(Observer {
                budget: budget.clone(),
                wakes: AtomicUsize::new(0),
            })
        };
        let old = observer();
        let latest = observer();
        let old_waker = Waker::from(old.clone());
        let latest_waker = Waker::from(latest.clone());
        assert!(
            budget
                .poll_shutdown(&mut Context::from_waker(&old_waker))
                .is_pending()
        );
        assert!(
            budget
                .poll_shutdown(&mut Context::from_waker(&latest_waker))
                .is_pending()
        );
        assert!(matches!(
            budget.reserve(ResourceUsage { items: 1, bytes: 0 }),
            Err(ErrorCode::Unavailable)
        ));
        drop(first);
        assert_eq!(latest.wakes.load(Ordering::SeqCst), 0);
        drop(last);
        assert_eq!(old.wakes.load(Ordering::SeqCst), 0);
        assert_eq!(latest.wakes.load(Ordering::SeqCst), 1);
        assert!(
            budget
                .poll_shutdown(&mut Context::from_waker(&latest_waker))
                .is_ready()
        );
        assert_eq!(latest.wakes.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn admission_is_atomic_and_drop_releases() {
        let b = ResourceBudget::new(ResourceUsage { items: 2, bytes: 9 }).unwrap();
        let r = b.reserve(ResourceUsage { items: 1, bytes: 8 }).unwrap();
        assert!(matches!(
            b.reserve(ResourceUsage { items: 1, bytes: 2 }),
            Err(ErrorCode::Capacity)
        ));
        assert_eq!(b.usage(), r.amount());
        drop(r);
        assert_eq!(b.usage(), ResourceUsage::default());
        let r = b.reserve(ResourceUsage { items: 2, bytes: 9 }).unwrap();
        b.close();
        assert!(matches!(
            b.reserve(ResourceUsage { items: 1, bytes: 0 }),
            Err(ErrorCode::Unavailable)
        ));
        drop(r);
        assert_eq!(b.usage(), ResourceUsage::default());
    }
    #[test]
    fn concurrent_reservations_never_exceed_capacity() {
        let budget = ResourceBudget::new(ResourceUsage { items: 4, bytes: 4 }).unwrap();
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let b = budget.clone();
                scope.spawn(move || {
                    for _ in 0..100 {
                        if let Ok(r) = b.reserve(ResourceUsage { items: 1, bytes: 1 }) {
                            assert!(b.usage().items <= 4);
                            drop(r);
                        }
                    }
                });
            }
        });
        assert_eq!(budget.usage(), ResourceUsage::default());
    }
    #[test]
    fn arithmetic_overflow_preserves_usage() {
        let b = ResourceBudget::new(ResourceUsage {
            items: usize::MAX,
            bytes: usize::MAX,
        })
        .unwrap();
        let r = b
            .reserve(ResourceUsage {
                items: usize::MAX,
                bytes: usize::MAX,
            })
            .unwrap();
        assert!(matches!(
            b.reserve(ResourceUsage { items: 1, bytes: 1 }),
            Err(ErrorCode::Capacity)
        ));
        assert_eq!(b.usage(), r.amount());
    }
}
