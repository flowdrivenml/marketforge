//! Deterministic tests for cancellation and monotonic-clock primitives.
//!
//! Tests use only std-library types:
//! - std::sync::AtomicBool / Arc for shared state.
//! - std::task::Poll and safe Arc-backed Wake implementations for polling.
//! - std::pin::Pin, Future for future polling.
//!
//! No Tokio, no external async runtime.

use marketforge_engine::live::foundation::{
    cancellation::CancellationToken,
    clock::{MonotonicClock, SystemMonotonicClock},
};
use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::Wake;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

// ── Test Waker Infrastructure ─────────────────────────────────────────────

/// Data held by the waker: a shared Arc<AtomicBool> used as the "done" flag.
pub struct TestWakerData {
    done: Arc<AtomicBool>,
}

impl Wake for TestWakerData {
    fn wake(self: Arc<Self>) {
        self.done.store(true, Ordering::SeqCst);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.done.store(true, Ordering::SeqCst);
    }
}

struct TestWaker {
    done: Arc<AtomicBool>,
}

impl TestWaker {
    fn new() -> (Self, Waker) {
        let done = Arc::new(AtomicBool::new(false));
        let waker = Waker::from(Arc::new(TestWakerData { done: done.clone() }));
        (Self { done }, waker)
    }

    fn is_done(&self) -> bool {
        self.done.load(Ordering::Acquire)
    }
}

/// Shared state used by cancellation tests that need to observe
/// waker invocations.
struct CancelFixture {
    token: CancellationToken,
    done: Arc<AtomicBool>,
    waker: Waker,
}

impl CancelFixture {
    /// Create a fresh token paired with a TestWaker.
    fn new() -> Self {
        let token = CancellationToken::new();
        let (tw, waker) = TestWaker::new();
        Self {
            token,
            done: tw.done,
            waker,
        }
    }
}

// ── CancellationToken tests ─────────────────────────────────────────

/// 1. A newly created CancellationToken is not cancelled.
#[test]
fn cancel_token_new_is_not_cancelled() {
    let token = CancellationToken::new();
    assert!(!token.is_cancelled());
}

/// 2. Calling cancel() changes is_cancelled() to true.
#[test]
fn cancel_token_cancel_sets_is_cancelled() {
    let token = CancellationToken::new();
    assert!(!token.is_cancelled());
    token.cancel();
    assert!(token.is_cancelled());
}

/// 3. Repeated cancellation is idempotent.
#[test]
fn cancel_token_repeat_cancel_is_idempotent() {
    let token = CancellationToken::new();
    token.cancel();
    token.cancel();
    token.cancel();
    assert!(token.is_cancelled());
}

/// 4. Cloned tokens share cancellation state.
#[test]
fn cancel_token_clone_shares_state() {
    let token = CancellationToken::new();
    let clone = token.clone();
    assert!(!token.is_cancelled());
    assert!(!clone.is_cancelled());

    token.cancel();
    assert!(token.is_cancelled());
    assert!(clone.is_cancelled()); // clone sees cancellation

    let another = clone.clone();
    assert!(another.is_cancelled()); // shares state through Arc

    another.cancel();
    assert!(token.is_cancelled()); // already true, idempotent
}

/// 5. A cancelled() future resolves immediately when its token was
///    cancelled before polling.
#[test]
fn cancel_token_future_ready_when_already_cancelled() {
    let fixture = CancelFixture::new();
    let mut fut = pin!(fixture.token.cancelled());

    // Pre-cancel.
    fixture.token.cancel();

    let cx = &mut Context::from_waker(&fixture.waker);
    let poll_result = fut.as_mut().poll(cx);
    assert!(
        matches!(poll_result, Poll::Ready(())),
        "cancelled() must resolve immediately \
         when token is already cancelled"
    );
}

/// 6. A pending cancelled() future becomes ready after cancellation.
#[test]
fn cancel_token_future_becomes_ready_after_cancel() {
    let fixture = CancelFixture::new();
    let mut fut = pin!(fixture.token.cancelled());

    // Poll before cancellation — should be Pending.
    let cx = &mut Context::from_waker(&fixture.waker);
    let first = fut.as_mut().poll(cx);
    assert!(
        matches!(first, Poll::Pending),
        "cancelled() must return Pending \
         before cancel()"
    );

    // Cancel — the waker stored during poll should be invoked.
    fixture.token.cancel();

    // The test waker's done flag is set by wake.
    assert!(
        fixture.done.load(Ordering::Acquire),
        "waker must have been called \
         by cancel()"
    );

    // Poll again — should now be Ready.
    let second = fut.as_mut().poll(cx);
    assert!(
        matches!(second, Poll::Ready(())),
        "cancelled() must resolve after \
         cancel()"
    );
}

/// 7. Multiple independently registered waiters are awakened.
#[test]
fn cancel_token_multiple_waiters_awakened() {
    let token = CancellationToken::new();

    // Register three independent futures.
    let mut f1 = pin!(token.cancelled());
    let mut f2 = pin!(token.cancelled());
    let mut f3 = pin!(token.cancelled());

    // Poll each — they each store a waker.
    let (tw_a, waker_a) = TestWaker::new();
    let (tw_b, waker_b) = TestWaker::new();
    let (tw_c, waker_c) = TestWaker::new();

    let cx_a = &mut Context::from_waker(&waker_a);
    let cx_b = &mut Context::from_waker(&waker_b);
    let cx_c = &mut Context::from_waker(&waker_c);

    let r1 = f1.as_mut().poll(cx_a);
    let r2 = f2.as_mut().poll(cx_b);
    let r3 = f3.as_mut().poll(cx_c);

    assert!(matches!(r1, Poll::Pending), "f1 should be Pending");
    assert!(matches!(r2, Poll::Pending), "f2 should be Pending");
    assert!(matches!(r3, Poll::Pending), "f3 should be Pending");

    // Cancel — all three waiters must wake.
    token.cancel();

    assert!(tw_a.is_done(), "waker A should have been called");
    assert!(tw_b.is_done(), "waker B should have been called");
    assert!(tw_c.is_done(), "waker C should have been called");

    // Poll again — all should be Ready.
    assert!(
        matches!(f1.as_mut().poll(cx_a), Poll::Ready(())),
        "f1 should be Ready"
    );
    assert!(
        matches!(f2.as_mut().poll(cx_b), Poll::Ready(())),
        "f2 should be Ready"
    );
    assert!(
        matches!(f3.as_mut().poll(cx_c), Poll::Ready(())),
        "f3 should be Ready"
    );
}

/// 9. Cancellation remains observable after all other token clones are
///    dropped.
#[test]
fn cancel_token_remains_observable_after_clones_dropped() {
    let token = CancellationToken::new();

    {
        let _clone1 = token.clone();
        let _clone2 = token.clone();
        let _clone3 = token.clone();
        // All clones dropped here.
    }

    // Token itself should still observe its own cancellation.
    token.cancel();
    assert!(token.is_cancelled());

    // A fresh future must still resolve.
    let mut fut = pin!(token.cancelled());
    let (_, waker) = TestWaker::new();
    let cx = &mut Context::from_waker(&waker);
    let result = fut.as_mut().poll(cx);
    assert!(
        matches!(result, Poll::Ready(())),
        "fresh cancelled() must resolve \
         on already-cancelled token"
    );
}

// ── SystemMonotonicClock tests ────────────────────────────────────────

/// 8. SystemMonotonicClock returns monotonic observations.
#[test]
fn monotonic_clock_returns_monotonic_observations() {
    let clock = SystemMonotonicClock::default();
    let now_1 = clock.now();

    // Spin briefly to ensure time advances.
    let spin_start = std::time::Instant::now();
    while std::time::Instant::now().duration_since(spin_start) < Duration::from_millis(5) {
        std::hint::spin_loop();
    }

    let now_2 = clock.now();
    assert!(
        now_2 >= now_1,
        "monotonic clock must never go backwards: {:?} < {:?}",
        now_2,
        now_1
    );
}

#[derive(Default)]
struct CountingWake {
    wakes: AtomicUsize,
}

impl Wake for CountingWake {
    fn wake(self: Arc<Self>) {
        self.wakes.fetch_add(1, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.wakes.fetch_add(1, Ordering::SeqCst);
    }
}

fn counting_waker() -> (Arc<CountingWake>, Waker) {
    let counter = Arc::new(CountingWake::default());
    let waker = Waker::from(counter.clone());

    (counter, waker)
}

#[test]
fn repeated_polling_registers_only_one_waker() {
    let token = CancellationToken::new();
    let mut future = Box::pin(token.cancelled());

    let (counter, waker) = counting_waker();
    let mut context = Context::from_waker(&waker);

    for _ in 0..100 {
        assert!(matches!(future.as_mut().poll(&mut context), Poll::Pending));
    }

    token.cancel();

    assert_eq!(counter.wakes.load(Ordering::SeqCst), 1);

    assert!(matches!(
        future.as_mut().poll(&mut context),
        Poll::Ready(())
    ));
}

#[test]
fn dropped_future_does_not_receive_cancellation_wakeup() {
    let token = CancellationToken::new();

    let (counter, waker) = counting_waker();
    let mut context = Context::from_waker(&waker);

    {
        let mut future = Box::pin(token.cancelled());

        assert!(matches!(future.as_mut().poll(&mut context), Poll::Pending));
    }

    token.cancel();

    assert_eq!(counter.wakes.load(Ordering::SeqCst), 0);
}

#[test]
fn polling_with_new_waker_replaces_old_waker() {
    let token = CancellationToken::new();
    let mut future = Box::pin(token.cancelled());

    let (old_counter, old_waker) = counting_waker();
    let (new_counter, new_waker) = counting_waker();

    let mut old_context = Context::from_waker(&old_waker);
    let mut new_context = Context::from_waker(&new_waker);

    assert!(matches!(
        future.as_mut().poll(&mut old_context),
        Poll::Pending
    ));

    assert!(matches!(
        future.as_mut().poll(&mut new_context),
        Poll::Pending
    ));

    token.cancel();

    assert_eq!(old_counter.wakes.load(Ordering::SeqCst), 0);

    assert_eq!(new_counter.wakes.load(Ordering::SeqCst), 1);
}

#[test]
fn dropped_waiters_do_not_prevent_active_waiter_wakeup() {
    let token = CancellationToken::new();

    let (counter, waker) = counting_waker();
    let mut context = Context::from_waker(&waker);

    for _ in 0..1000 {
        let mut future = Box::pin(token.cancelled());

        assert!(matches!(future.as_mut().poll(&mut context), Poll::Pending));

        drop(future);
    }

    let mut active = Box::pin(token.cancelled());

    assert!(matches!(active.as_mut().poll(&mut context), Poll::Pending));

    token.cancel();

    assert_eq!(counter.wakes.load(Ordering::SeqCst), 1);

    assert!(matches!(
        active.as_mut().poll(&mut context),
        Poll::Ready(())
    ));
}
