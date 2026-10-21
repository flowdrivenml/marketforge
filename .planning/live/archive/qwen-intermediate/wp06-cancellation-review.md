---
title: WP-06 Cancellation & Clock — Implementation Evidence Report
phase: WP-06
scope: Cancellation primitives, MonotonicClock abstraction
written: 2026-10-20
status: INSPECTION ONLY (no source changes)
---

## 1. Implementation

### 1.1 CancellationToken / Cancelled (`engine/src/live/foundation/cancellation.rs`)

**Types introduced:**

| Type | Visibility | Description |
|------|-----------|-------------|
| `CancellationState` | `struct` (private) | Shared state: `AtomicBool cancelled` + `Mutex<Vec<Waker>> waiters`. Derives `Debug`, `Default`. (Lines 21–25) |
| `CancellationToken` | `pub struct` | Clonable cancellation handle containing `state: Arc<CancellationState>`. Derives `Debug`, `Clone`, `Default`. (Lines 30–33) |
| `Cancelled` | `pub struct` | Zero-sized future that resolves when the token is cancelled. Contains `token: CancellationToken`. (Lines 71–73) |

**Public functions on `CancellationToken`:**

```rust
pub fn new() -> Self                          // Line 36
pub fn cancel(&self)                           // Line 43
pub fn is_cancelled(&self) -> bool             // Line 58
pub fn cancelled(&self) -> Cancelled            // Line 63
```

**Actual behaviour:**

- `new()` — delegates to `Default::default()`, producing an empty `CancellationState`.
- `cancel()` — performs a `swap(true, AcqRel)` on `cancelled`. If the pre-swap value was already `true`, returns immediately (idempotent). Otherwise (first cancel):
  1. Acquires `waiters` mutex.
  2. Drains the waiter list via `std::mem::take` so the locked list disappears from `waiters`.
  3. Calls `wake()` on each drained waker **while still holding the mutex** (the guard is dropped before the loop; guard released immediately before the loop at line 51).
- `is_cancelled()` — loads `cancelled` with `Acquire` ordering.
- `cancelled()` — constructs a `Cancelled` future holding a **clone** of the token's `Arc`. The clone shares the same underlying `CancellationState`.

**`Cancelled::poll()` logic (lines 78–94):**

1. If `self.token.is_cancelled()` → `Poll::Ready(())`.
2. Double-check after mutex: if cancelled while acquiring → return `Ready`.
3. If not cancelled → `waiters.push(cx.waker().clone())` → `Poll::Pending`.

**Error handling:** `Mutex::lock().unwrap()` propagates panics on poisoned mutex. No graceful error path.

**Resource ownership:** `Arc<CancellationState>` is reference-counted across clones. `Vec<Waker>` lives inside the `MutexGuard` scope during `cancel()` and `poll()`.

---

### 1.2 MonotonicClock (`engine/src/live/foundation/clock.rs`)

**Types introduced:**

| Type | Visibility | Description |
|------|-----------|-------------|
| `MonotonicClock` | `pub trait` | Single method: `fn now(&self) -> Instant`. Bounds: `Send + Sync`. (Lines 9–11) |
| `SystemMonotonicClock` | `pub struct` | `Debug + Clone + Copy + Default` unit struct. (Lines 14–15) |

**Behaviour:** `SystemMonotonicClock::now()` calls `Instant::now()`. This is a system monotonic clock (does not go backwards, not affected by wall-clock adjustments).

**No deterministic clock implementation** (no test/mockable clock).

**Deadline functionality:** None. No `deadline()` or `sleep_at()` methods.

---

### 1.3 Module re-exports (`engine/src/live/foundation/mod.rs`)

```rust
pub mod cancellation;    // Line 4
pub mod clock;           // Line 5
pub(crate) mod diagnostics;    // Line 6
pub(crate) mod filesystem;     // Line 7
pub(crate) mod resources;      // Line 8
pub(crate) mod shutdown;       // Line 9
pub use cancellation::{CancellationToken, Cancelled};  // Line 11
pub use clock::{MonotonicClock, SystemMonotonicClock}; // Line 12
```

The scaffolding sub-modules (`diagnostics`, `filesystem`, `resources`, `shutdown`) are declared but **unimplemented** (only module declarations, no pub API).

---

## 2. Cancellation Safety

### 2.1 State sharing

`CancellationToken` clones share the same `Arc<CancellationState>`. Proof: line 32 `state: Arc<CancellationState>`, and `Clone` is derived (line 30), which clones the `Arc` (incrementing ref-count, not the inner data).

### 2.2 Waker registration

Waiters register via `waiters.push(cx.waker().clone())` (line 90) inside a held `MutexGuard`. This is inside `poll()`.

### 2.3 Duplicate waker register on repeated poll

**BUG — duplicate wakers are pushed on every `poll()` call.** There is no `HashSet` or flag to deduplicate. If the future is polled repeatedly by a scheduler before `cancel()` is called, the waiter list accumulates duplicate wakers. Every duplicate will be woken, causing spurious wake-ups (harmless but wasteful).

**Source:** `cancellation.rs` lines 78–94 — no dedup logic.

### 2.4 Dropped futures unregister their wakers

**Not implemented.** When `Cancelled` is dropped, the waker stored in `waiters` is **not** removed. Proof: `Cancelled` has no `Drop` impl (only a struct with a `token` field). The destructor only drops the `Arc`, decrementing its ref-count. The `Vec<Waker>` inside `waiters` is leaked — each abandoned future leaks its waker's internal resources.

**Evidence:** No `impl Drop for Cancelled` exists anywhere in the file.

**Memory impact:** For `N` abandoned cancellation futures, `waiters.len() == N` wakers accumulate. Waker structs contain pointer-sized data; in practice they carry executor-specific state (e.g., task context), so the leak is proportional to the executor's waker size. This is an **unbounded memory growth** concern for long-lived systems with many dropped futures.

### 2.5 Race between cancellation and waiter registration

`cancel()` acquires the mutex, drains waiters, drops the guard, then wakes. `poll()` acquires the same mutex to push a waker. **Mutual exclusion guarantees no race** between these two critical sections.

However, there is a **TOCTOU window** between `cancel()`'s `swap(true)` (line 44) and the mutex acquisition (line 49): a concurrent `poll()` that reads `is_cancelled() == true` (line 79) before `cancel()` acquires the mutex will return `Ready` without registering a waker — this is the **correct** outcome but the code path does not use the mutex to protect the check+register atomically. Instead it relies on the double-check pattern:

```
cancel(): swap(true) → if changed → lock → take → wake
poll(): if cancelled → Return Ready
       lock → if cancelled → Return Ready
       push waker
```

If `poll()` reads `cancelled == false` then `cancel()` sets it to true and wakes, `poll()` will still acquire the mutex, see `cancelled == true` at the double-check (line 86), and return `Ready`. The double-check is safe and the implementation is **race-free**.

### 2.6 Wakes every registered waiter

**Yes.** Line 50: `std::mem::take(&mut *guard)` replaces the `Vec<Waker>` with an empty vec, draining it. Each waker is then woken (line 53–54). Any future that `poll()`ed **before** `cancel()` acquired the mutex had its waker included in the drained set.

### 2.7 Memory growth from abandoned waiters

**Yes.** As noted in 2.4, `Cancelled::Drop` is not implemented. Abandoned futures leave wakers in the list. `cancel()` is the only drain point and it consumes the full list.

---

## 3. Clock Semantics

### 3.1 Trait and implementations

`MonotonicClock` trait (line 9) with a single `now(&self) -> Instant` method.

`SystemMonotonicClock` (line 14) is the sole implementation. It is `Clone + Copy + Default + Debug`.

**No mock/test clock** is available. Tests use the real clock (`SystemMonotonicClock::default()`), which introduces non-determinism.

### 3.2 Monotonic vs wall-clock

`Instant::now()` is a monotonic clock. It does not go backwards, is not affected by NTP adjustments, and measures uptime-relative time. The docs (lines 2–4) correctly state this intent.

### 3.3 Deterministic clock control

**Not implemented.** There is no mockable or adjustable clock. Tests use real wall-clock time via spin loops to verify monotonicity.

### 3.4 Deadline wakeups

**Not implemented.** No `deadline()`, `sleep_at()`, or timeout API exists.

---

## 4. Test Evidence

### 4.1 Tests added (`engine/tests/live/foundations.rs`)

| # | Test name | Scope |
|---|-----------|-------|
| 1 | `cancel_token_new_is_not_cancelled` | New token not cancelled |
| 2 | `cancel_token_cancel_sets_is_cancelled` | `cancel()` flips flag |
| 3 | `cancel_token_repeat_cancel_is_idempotent` | Multiple `cancel()` calls |
| 4 | `cancel_token_clone_shares_state` | Clone propagation |
| 5 | `cancel_token_future_ready_when_already_cancelled` | Pre-cancelled future |
| 6 | `cancel_token_future_becomes_ready_after_cancel` | Pending→ready transition, waker invocation (via RawWaker) |
| 7 | `cancel_token_multiple_waiters_awakened` | Three futures, all woken |
| 8 | `monotonic_clock_returns_monotonic_observations` | Clock monotonicity |
| 9 | `cancel_token_remains_observable_after_clones_dropped` | Post-drop visibility |

### 4.2 Tests executed

**Command:**
```
cargo test --locked --offline --no-default-features --features live \
  --test live_suite foundations
```

**Output:**
```
running 9 tests
test live::foundations::cancel_token_cancel_sets_is_cancelled ... ok
test live::foundations::cancel_token_clone_shares_state ... ok
test live::foundations::cancel_token_future_becomes_ready_after_cancel ... ok
test live::foundations::cancel_token_future_ready_when_already_cancelled ... ok
test live::foundations::cancel_token_new_is_not_cancelled ... ok
test live::foundations::cancel_token_multiple_waiters_awakened ... ok
test live::foundations::cancel_token_remains_observable_after_clones_dropped ... ok
test live::foundations::cancel_token_repeat_cancel_is_idempotent ... ok
test live::foundations::monotonic_clock_returns_monotonic_observations ... ok

test result: ok. 9 passed; 0 failed; 0 ignored; 0 measured
```

**Exit code:** 0

### 4.3 Warnings

- `src/live/model/envelope.rs:116` — dead code `LiveEvent::new()` (pre-existing, not WP-06).
- `tests/live/contracts.rs:7` — unused `Arc` import (pre-existing).
- `tests/live/contracts.rs:19` — unused `EventBatchError`, `EventBatch` imports (pre-existing).
- **No warnings in `cancellation.rs`, `clock.rs`, or `foundations.rs`.**

---

## 5. Contract Gaps

### 5.1 Missing cancellation guarantees

| Gap | Severity | Detail |
|-----|---------|--------|
| Unbounded waiter list | **Medium** | `Mutex<Vec<Waker>>` grows without bound; no `Drop` impl on `Cancelled`. |
| Duplicate wakers on re-poll | Low | Each `poll()` pushes a new waker even if one already exists. No dedup. |
| No timeout API | Low | No way to cancel after a duration. |
| `Mutex::unlock().unwrap()` | Low | Panic on mutex poison rather than graceful recovery. |

### 5.2 Missing deadline functionality

The `MonotonicClock` trait exists but no deadline or timeout API is implemented. Deadlines, alarms, or `sleep_at` are not present.

### 5.3 Unbounded resources

As noted in 5.1, `waiters` is an unbounded `Vec<Waker>`. Each waker carries executor-specific state (likely a task pointer + waker vtable). Long-running producers with many dropped consumers will leak memory proportional to dropped futures.

### 5.4 Missing failure tests

- No test for `Cancelled` drop leaking wakers.
- No test for duplicate waker accumulation on repeated poll.
- No test for `Mutex` poisoning behavior.
- No test for cancellation racing with `poll()`.
- No test for `Clock` monotonicity over longer intervals.

### 5.5 Non-deterministic clock testing

Test 8 (`monotonic_clock_returns_monotonic_observations`) uses a 5 ms spin loop and real time. This could theoretically fail under heavy system load, though the probability is extremely low (monotonic guarantees are kernel-level).

---

## 6. Changed Files

Based on inspection evidence, the following files were modified during WP-06:

1. `engine/src/live/foundation/cancellation.rs` — Full implementation (95 lines).
2. `engine/src/live/foundation/clock.rs` — Full implementation (21 lines).
3. `engine/src/live/foundation/mod.rs` — Added `pub mod` declarations and `pub use` re-exports (12 lines).
4. `engine/tests/live/foundations.rs` — Added 9 WP-06 tests (327 lines).
5. `engine/tests/live_suite.rs` — **Not confirmed modified.** May contain WP-06 test registration or be untouched.

Files **not modified** (confirmed by scope):
- `Cargo.toml` — no dependency changes.
- `STATUS.md` — no changes.
- `docs/decisions.md` — no changes.

**Note:** A complete changed-file inventory cannot be established without Git commands. The above list is inferred from the implementation task scope and file inspection.
