//! Bounded deadline scheduler. A single owned native worker services system
//! deadlines. The deterministic adapter uses the same registration/expiry core.
use super::resources::{ResourceBudget, ResourceReservation, ResourceUsage};
use crate::live::{
    model::error::ErrorCode,
    routing::admission::{DeadlineFuture, DeadlineTimer},
};
use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Waker},
    thread::JoinHandle,
    time::Instant,
};
struct Entry {
    ready: Arc<AtomicBool>,
    waker: Option<Waker>,
}
#[derive(Default)]
struct State {
    entries: BTreeMap<(Instant, u64), Entry>,
    next: u64,
    exhausted: bool,
    closing: bool,
    stopped: bool,
}
struct Core {
    state: Mutex<State>,
    changed: Condvar,
    capacity: ResourceBudget,
}
impl Core {
    fn new(max: usize) -> Result<Arc<Self>, ErrorCode> {
        Ok(Arc::new(Self {
            state: Mutex::new(State::default()),
            changed: Condvar::new(),
            capacity: ResourceBudget::new(ResourceUsage {
                items: max,
                bytes: max,
            })?,
        }))
    }
    fn register(
        self: &Arc<Self>,
        deadline: Instant,
        now: Instant,
    ) -> Result<DeadlineFuture, ErrorCode> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.closing {
            return Err(ErrorCode::Unavailable);
        }
        if deadline <= now {
            return Ok(Box::pin(async {}));
        }
        if state.exhausted {
            return Err(ErrorCode::Limit);
        }
        let reservation = self
            .capacity
            .reserve(ResourceUsage { items: 1, bytes: 1 })?;
        let id = state.next;
        match id.checked_add(1) {
            Some(next) => state.next = next,
            None => state.exhausted = true,
        }
        let ready = Arc::new(AtomicBool::new(false));
        state.entries.insert(
            (deadline, id),
            Entry {
                ready: ready.clone(),
                waker: None,
            },
        );
        drop(state);
        self.changed.notify_all();
        Ok(Box::pin(Sleep {
            core: self.clone(),
            key: (deadline, id),
            ready,
            _reservation: reservation,
        }))
    }
    fn expire(&self, now: Instant) -> Vec<Waker> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut wakers = Vec::new();
        while state
            .entries
            .first_key_value()
            .is_some_and(|(key, _)| key.0 <= now)
        {
            let (_, entry) = state.entries.pop_first().unwrap();
            entry.ready.store(true, Ordering::Release);
            if let Some(waker) = entry.waker {
                wakers.push(waker);
            }
        }
        drop(state);
        wakers
    }
}
struct Sleep {
    core: Arc<Core>,
    key: (Instant, u64),
    ready: Arc<AtomicBool>,
    _reservation: ResourceReservation,
}
impl Future for Sleep {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.ready.load(Ordering::Acquire) {
            return Poll::Ready(());
        }
        let mut state = self.core.state.lock().unwrap_or_else(|e| e.into_inner());
        if self.ready.load(Ordering::Acquire) {
            return Poll::Ready(());
        }
        let entry = state
            .entries
            .get_mut(&self.key)
            .expect("live timer registration");
        if entry
            .waker
            .as_ref()
            .is_none_or(|w| !w.will_wake(cx.waker()))
        {
            entry.waker = Some(cx.waker().clone());
        }
        Poll::Pending
    }
}
impl Drop for Sleep {
    fn drop(&mut self) {
        self.core
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entries
            .remove(&self.key);
        self.core.changed.notify_all();
    }
}
pub struct NativeDeadlineTimer {
    core: Arc<Core>,
    worker: Mutex<Option<JoinHandle<()>>>,
}
impl NativeDeadlineTimer {
    pub fn new(max_timers: usize) -> Result<Self, ErrorCode> {
        let core = Core::new(max_timers)?;
        let owned = core.clone();
        let worker = std::thread::Builder::new()
            .name("live-deadlines".into())
            .spawn(move || {
                loop {
                    for waker in owned.expire(Instant::now()) {
                        waker.wake();
                    }
                    let state = owned.state.lock().unwrap_or_else(|e| e.into_inner());
                    if state.closing && state.entries.is_empty() {
                        drop(state);
                        let mut state = owned.state.lock().unwrap_or_else(|e| e.into_inner());
                        state.stopped = true;
                        owned.changed.notify_all();
                        break;
                    }
                    if let Some((key, _)) = state.entries.first_key_value() {
                        let remaining = key.0.saturating_duration_since(Instant::now());
                        drop(
                            owned
                                .changed
                                .wait_timeout(state, remaining)
                                .unwrap_or_else(|e| e.into_inner()),
                        );
                    } else {
                        drop(owned.changed.wait(state).unwrap_or_else(|e| e.into_inner()));
                    }
                }
            })
            .map_err(|_| ErrorCode::Capacity)?;
        Ok(Self {
            core,
            worker: Mutex::new(Some(worker)),
        })
    }
    /// Blocking administrative shutdown, never a market-data hot-path call.
    /// Does not fabricate expiry or revoke waits. False means outstanding waits
    /// remain owned; drop/cancellation of their futures releases registrations.
    pub fn shutdown(&self, deadline: Instant) -> bool {
        let mut state = self.core.state.lock().unwrap_or_else(|e| e.into_inner());
        state.closing = true;
        self.core.changed.notify_all();
        while !state.stopped {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return false;
            }
            state = self
                .core
                .changed
                .wait_timeout(state, remaining)
                .unwrap_or_else(|e| e.into_inner())
                .0;
        }
        drop(state);
        let worker = self.worker.lock().unwrap_or_else(|e| e.into_inner()).take();
        worker.is_none_or(|w| w.join().is_ok())
    }
}
impl DeadlineTimer for NativeDeadlineTimer {
    fn sleep_until(&self, deadline: Instant) -> DeadlineFuture {
        self.try_sleep_until(deadline)
            .expect("timer capacity must be admitted before infallible sleep")
    }
    fn try_sleep_until(&self, deadline: Instant) -> Result<DeadlineFuture, ErrorCode> {
        self.core.register(deadline, Instant::now())
    }
}
impl Drop for NativeDeadlineTimer {
    fn drop(&mut self) {
        self.core
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .closing = true;
        self.core.changed.notify_all();
        // Explicit shutdown owns joining. If the deadline was exceeded, the
        // bounded worker stays alive until outstanding future owners release.
    }
}
/// Explicit test adapter, never selected as a production fallback.
pub struct ManualDeadlineTimer {
    core: Arc<Core>,
    now: Mutex<Instant>,
}
impl ManualDeadlineTimer {
    pub fn new(now: Instant, max: usize) -> Result<Self, ErrorCode> {
        Ok(Self {
            core: Core::new(max)?,
            now: Mutex::new(now),
        })
    }
    pub fn advance_to(&self, now: Instant) -> Result<(), ErrorCode> {
        let mut current = self.now.lock().unwrap_or_else(|e| e.into_inner());
        if now < *current {
            return Err(ErrorCode::InvalidInput);
        }
        *current = now;
        // Register holds the same clock lock until insertion, preventing a
        // missed expiry if registration races with deterministic advancement.
        let wakers = self.core.expire(now);
        drop(current);
        for waker in wakers {
            waker.wake();
        }
        Ok(())
    }
}
impl DeadlineTimer for ManualDeadlineTimer {
    fn sleep_until(&self, deadline: Instant) -> DeadlineFuture {
        self.try_sleep_until(deadline).expect("test timer capacity")
    }
    fn try_sleep_until(&self, deadline: Instant) -> Result<DeadlineFuture, ErrorCode> {
        let now = self.now.lock().unwrap_or_else(|e| e.into_inner());
        self.core.register(deadline, *now)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::task::Wake;
    struct Noop;
    impl Wake for Noop {
        fn wake(self: Arc<Self>) {}
    }
    #[test]
    fn deterministic_expiry_capacity_drop_and_repoll() {
        let now = Instant::now();
        let timer = ManualDeadlineTimer::new(now, 1).unwrap();
        let waker = Waker::from(Arc::new(Noop));
        let mut cx = Context::from_waker(&waker);
        let mut future = timer
            .try_sleep_until(now + std::time::Duration::from_secs(1))
            .unwrap();
        assert!(future.as_mut().poll(&mut cx).is_pending());
        assert!(future.as_mut().poll(&mut cx).is_pending());
        assert!(matches!(
            timer.try_sleep_until(now + std::time::Duration::from_secs(2)),
            Err(ErrorCode::Capacity)
        ));
        timer
            .advance_to(now + std::time::Duration::from_secs(1))
            .unwrap();
        assert!(future.as_mut().poll(&mut cx).is_ready());
        drop(future);
        let future = timer
            .try_sleep_until(now + std::time::Duration::from_secs(2))
            .unwrap();
        drop(future);
        assert!(timer.core.state.lock().unwrap().entries.is_empty());
    }
    #[test]
    fn native_shutdown_reports_unfinished_without_fabricating_expiry() {
        let timer = NativeDeadlineTimer::new(1).unwrap();
        let mut future = timer
            .try_sleep_until(Instant::now() + std::time::Duration::from_secs(3600))
            .unwrap();
        assert!(!timer.shutdown(Instant::now()));
        let waker = Waker::from(Arc::new(Noop));
        let mut cx = Context::from_waker(&waker);
        assert!(future.as_mut().poll(&mut cx).is_pending());
        drop(future);
        assert!(timer.shutdown(Instant::now() + std::time::Duration::from_secs(1)));
        assert!(matches!(
            timer.try_sleep_until(Instant::now()),
            Err(ErrorCode::Unavailable)
        ));
    }
}
