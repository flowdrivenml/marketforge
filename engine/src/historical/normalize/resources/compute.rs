//! A session-wide compute budget. Parents acquire one permit; children only try
//! spare permits, so parents never wait for children needing parent capacity.
use std::{
    cell::RefCell,
    sync::{Arc, Condvar, Mutex},
};
thread_local! { static CURRENT: RefCell<Option<Arc<ComputeBudget>>> = const { RefCell::new(None) }; }
pub(crate) struct ComputeBudget {
    free: Mutex<usize>,
    changed: Condvar,
}
pub(crate) struct Permit {
    budget: Arc<ComputeBudget>,
}
impl ComputeBudget {
    pub fn new(workers: usize) -> Arc<Self> {
        Arc::new(Self {
            free: Mutex::new(workers),
            changed: Condvar::new(),
        })
    }
    fn acquire(self: &Arc<Self>) -> Permit {
        let mut free = self.free.lock().unwrap();
        while *free == 0 {
            free = self.changed.wait(free).unwrap();
        }
        *free -= 1;
        Permit {
            budget: self.clone(),
        }
    }
    fn try_acquire(self: &Arc<Self>) -> Option<Permit> {
        let mut free = self.free.lock().unwrap();
        if *free == 0 {
            None
        } else {
            *free -= 1;
            Some(Permit {
                budget: self.clone(),
            })
        }
    }
}
impl Drop for Permit {
    fn drop(&mut self) {
        *self.budget.free.lock().unwrap() += 1;
        self.budget.changed.notify_one();
    }
}
pub(crate) fn with_budget<T>(budget: &Arc<ComputeBudget>, run: impl FnOnce() -> T) -> T {
    struct Restore(Option<Arc<ComputeBudget>>);
    impl Drop for Restore {
        fn drop(&mut self) {
            CURRENT.with(|v| *v.borrow_mut() = self.0.take());
        }
    }
    let _permit = budget.acquire();
    let _restore = Restore(CURRENT.with(|v| v.borrow_mut().replace(budget.clone())));
    run()
}
pub(crate) fn spare() -> Option<Permit> {
    CURRENT.with(|v| v.borrow().as_ref().and_then(ComputeBudget::try_acquire))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parent_and_children_share_capacity_and_restore_thread_context() {
        let budget = ComputeBudget::new(2);
        with_budget(&budget, || {
            let child = spare().unwrap();
            assert!(spare().is_none());
            std::thread::spawn(move || drop(child)).join().unwrap();
            assert!(spare().is_some());
        });
        assert!(spare().is_none());
        assert_eq!(*budget.free.lock().unwrap(), 2);
    }
}
