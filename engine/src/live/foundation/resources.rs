//! Mandatory count/byte accounting, independent of optional measurements.
//! A reservation retains capacity until ownership is dropped, including unwind.
use crate::live::model::error::ErrorCode;
use std::sync::{Arc, Mutex};

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
        let mut state = self.budget.0.lock().unwrap_or_else(|e| e.into_inner());
        state.used.items -= self.amount.items;
        state.used.bytes -= self.amount.bytes;
    }
}
#[cfg(test)]
mod tests {
    use super::*;
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
