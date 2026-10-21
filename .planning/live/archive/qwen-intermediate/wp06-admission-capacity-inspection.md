# WP-06 Admission Capacity Primitive — Inspection Evidence Report

**Scope:** Audit `admission.rs`, `ports/routing.rs`, `resources.rs`, `destination.rs`, `router.rs`, `Cargo.toml` for capacity reservation prerequisites.
**Mode:** READ-ONLY — no code, test, or config modifications.
**Rust edition:** 2024. Runtime: std-only (no Tokio, no futures-channel).

---

## 1. Bounded Queues & Capacity Reservation Primitives

**Finding: NONE.** No existing bounded queues or capacity reservation types in any inspected file.

| File | Status |
|------|--------|
| `engine/src/live/foundation/resources.rs` | Scaffold only (2 lines, empty body) |
| `engine/src/live/routing/destination.rs` | Scaffold only (2 lines, empty body) |
| `engine/src/live/routing/router.rs` | Scaffold only (2 lines, empty body) |

The `DeliveryCapacity` type is referenced but its definition lives in `model::delivery` (not inspected here). Capacity slots exist only at the type-declaration level.

---

## 2. Asynchronous Channel Dependencies

**Finding: NONE available.**

`engine/Cargo.toml` dependencies (lines 39-61): `clap`, `rust_decimal`, `serde`, `serde_json`, `thiserror`, `sha2`. Optional feature deps: `flate2`, `csv`, `zip`, `tar`, `arrow-*`, `parquet`, `ratatui`, `crossterm`, `num-bigint`, `num-traits`, `num-integer`, `rustix`.

- **Tokio:** Not present.
- **futures-channel:** Not present.
- **async-channel / crossbeam-channel:** Not present.

All capacity primitives must be std-only (condvar + mpsc + manual waker patterns).

---

## 3. Resource Budget & Accounting Interfaces

**Finding: NONE in live context.**

`engine/src/live/foundation/resources.rs` (lines 1-2):
```rust
//! Planned component: src/live/foundation/resources.rs.
//! Scaffold only; no behavior is implemented or verified.
```

A test `resource_file_budget` exists under `tests/processing/resources/` but belongs to the historical pipeline, not live. No `ResourceBudget`, `ResourceAccountant`, or similar type in `live/foundation/`.

---

## 4. Destination Admission Implementations

**Finding: NONE implemented.**

`engine/src/live/routing/destination.rs` (lines 1-2):
```rust
//! Planned component: src/live/routing/destination.rs.
//! Scaffold only; no behavior is implemented or verified.
```

`engine/src/live/routing/router.rs` (lines 1-2):
```rust
//! Planned component: src/live/routing/router.rs.
//! Scaffold only; no behavior is implemented or verified.
```

All destination-side admission and router-side capacity enforcement are unimplemented.

---

## 5. Tokio & futures-channel Dependency Availability

| Dependency | Available? |
|------------|------------|
| `tokio` | No |
| `futures-channel` | No |
| `futures-core` | No |
| `async-trait` | No |
| Any async runtime | No |

All primitives must be runtime-independent (std-only).

---

## 6. Rust Signatures Relevant to Capacity Reservations

### `engine/src/live/ports/routing.rs`

**DestinationDescriptor (line 89-95) — capacity declared per destination:**
```rust
pub struct DestinationDescriptor {
    pub id: DestinationId,
    pub kind: DestinationKind,
    pub policy: DeliveryPolicy,
    pub capacity: DeliveryCapacity,
}
```

**RoutingAdmissionFailure (line 128-136) — CapacityExceeded slot exists:**
```rust
pub enum RoutingAdmissionFailure {
    CapacityExceeded,
    Cancelled,
    DeadlineExceeded,
    ShuttingDown,
    InvalidCursor,
    NoRequiredCapacity,
}
```

**RoutingAdmission (line 141-146):**
```rust
pub enum RoutingAdmission {
    Accepted { through: StreamCursor },
    Rejected { reason: RoutingAdmissionFailure },
}
```

**EventRouterPort (line 160-162) — router admission entry-point:**
```rust
pub trait EventRouterPort: Send + Sync {
    fn admit<'a>(&'a self, batch: EventBatch) -> RoutingFuture<'a, RoutingAdmission>;
}
```

**EventDestinationPort (line 171-175) — destination admission contract:**
```rust
pub trait EventDestinationPort: Send + Sync {
    fn descriptor(&self) -> DestinationDescriptor;
    fn admit<'a>(&'a self, batch: EventBatch) -> RoutingFuture<'a, DestinationAdmission>;
}
```

**RoutingFuture (line 151):**
```rust
pub type RoutingFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
```

**EventBatch (lines 29-72) — validated, non-empty, cursor-ordered:**
```rust
pub struct EventBatch { events: Arc<[SharedLiveEvent]> }
impl EventBatch {
    pub fn new(events: Vec<SharedLiveEvent>) -> Result<Self, EventBatchError>;
    pub fn events(&self) -> &[SharedLiveEvent];
    pub fn len(&self) -> usize;
    pub fn first_cursor(&self) -> &StreamCursor;
    pub fn last_cursor(&self) -> &StreamCursor;
}
```

### `engine/src/live/routing/admission.rs` (95 lines, implemented)

**DeadlineTimer trait (line 35):**
```rust
pub trait DeadlineTimer: Send + Sync {
    fn set(&self, deadline: Instant) -> DeadlineFuture;
}
pub type DeadlineFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
```

**CancellationToken / Cancelled (WP-06, from `cancellation.rs`):**
```rust
pub struct CancellationToken { inner: Arc<CancellationTokenInner> }
impl CancellationToken {
    pub fn new() -> Self;
    pub fn is_cancelled(&self) -> bool;
    pub fn cancel(&self);
    pub fn chain(&self, other: CancellationToken) -> Self;
    pub fn token(&self) -> CancellationToken;  // clone
}
pub struct Cancelled { /* awaits cancellation */ }
impl Future for Cancelled { type Output = (); }
```

---

## 7. Missing Interfaces & Unresolved Dependencies

| Gap | Severity | Detail |
|-----|----------|--------|
| No `DeliveryCapacity` definition inspected | INFO | Referenced in `DestinationDescriptor.capacity` but type not visible here |
| No bounded queue type | **BLOCKING** | Must be built std-only (no Tokio `sync::mpsc`, no `bounded` channel) |
| No capacity semaphore / permit type | **BLOCKING** | No `Semaphore`, `Permit`, or equivalent in std deps |
| No `ResourceBudget` / `ResourceAccountant` | HIGH | `resources.rs` is empty scaffold |
| No admission waiter/queue type | HIGH | `admission.rs` exists but its wait mechanism not fully inspected |
| No deadline-capacity integration | HIGH | `DeadlineTimer` exists; no mechanism to combine deadline + capacity polling |
| No waker deduplication strategy | MEDIUM | `Cancelled` has no `Drop` (WP-06 finding) — leaked wakers if capacity wait times out |

### Blocking Findings

1. **Capacity primitive must be built from scratch** — no bounded queue, semaphore, or permit type exists. Must use `std::sync`(Mutex + Condvar) or `crossbeam` if added to deps (currently not present).

2. **Runtime independence constraint** — no async runtime available. Capacity wait future must self-notify via waker (same pattern as WP-06 `Cancelled` using raw wakers), or return `Poll::Pending` with external trigger.

3. **`Cancelled` future leaks wakers** — WP-06 verification found `Cancelled` has no `Drop` impl; abandoned capacity-wait futures leave unbounded waker lists. Any capacity reservation type must implement `Drop` to clean up waiters.

---

*Report generated in READ-ONLY mode. No files modified.*
