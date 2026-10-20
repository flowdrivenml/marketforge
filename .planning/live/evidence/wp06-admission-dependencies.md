# WP-06 — Admission Cancellation Dependency Inspection Report

**Date:** 2026-10-20  
**Phase:** MarketForge Live — `engine` crate  
**Mode:** READ-ONLY inspection  
**Branch:** `docs/architecture-consistency`

---

## 1. Exact Existing Definitions and Signatures

### 1.1 EventRouterPort (`engine/src/live/ports/routing.rs`, lines 160-162)

```rust
pub trait EventRouterPort: Send + Sync {
    fn admit<'a>(&'a self, batch: EventBatch) -> RoutingFuture<'a, RoutingAdmission>;
}
```

- **Lifetime:** `'a` borrows `self` directly (no `&mut`).
- **Blocking:** Returns a boxed `Future` (not `impl Future`), enabling dynamic dispatch.
- **Return:** `RoutingAdmission` — `Accepted { through }` or `Rejected { reason }`.

### 1.2 EventDestinationPort (`engine/src/live/ports/routing.rs`, lines 171-175)

```rust
pub trait EventDestinationPort: Send + Sync {
    fn descriptor(&self) -> DestinationDescriptor;

    fn admit<'a>(&'a self, batch: EventBatch) -> RoutingFuture<'a, DestinationAdmission>;
}
```

- **Same lifetime pattern** as `EventRouterPort::admit`.
- **Descriptor accessor** provides destination config at runtime.
- **Return:** `DestinationAdmission` — `Accepted { destination, through }` or `Rejected { destination, reason }`.

### 1.3 RoutingFuture<'a, T> (`engine/src/live/ports/routing.rs`, line 151)

```rust
pub type RoutingFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
```

- Heap-allocated, `Send`-bound, lifetime-parameterised future.
- No `tokio::time::Deadline` or `CancellationToken` is part of this type.

### 1.4 RoutingAdmissionFailure (`engine/src/live/ports/routing.rs`, lines 128-136)

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

- **`Cancelled`** and **`DeadlineExceeded`** variants exist — but **no implementation maps them to any runtime primitive**.
- Six failure variants total: capacity (2), cancellation/deadline (2), schema/config (2).

### 1.5 RoutingAdmission (`engine/src/live/ports/routing.rs`, lines 141-146)

```rust
pub enum RoutingAdmission {
    Accepted { through: StreamCursor },
    Rejected { reason: RoutingAdmissionFailure },
}
```

### 1.6 EventBatch (`engine/src/live/ports/routing.rs`, lines 28-72)

```rust
pub struct EventBatch {
    events: Arc<[SharedLiveEvent]>,
}

impl EventBatch {
    pub fn new(events: Vec<SharedLiveEvent>) -> Result<Self, EventBatchError>;
    pub fn events(&self) -> &[SharedLiveEvent];
    pub fn len(&self) -> usize;
    pub fn first_cursor(&self) -> &StreamCursor;
    pub fn last_cursor(&self) -> &StreamCursor;
}
```

- Validation enforces: non-empty, same stream + epoch, strictly increasing cursors.
- Errors: `Empty`, `MixedNamespace`, `NonIncreasingCursor`.

### 1.7 SharedLiveEvent (`engine/src/live/ports/routing.rs`, line 22)

```rust
pub type SharedLiveEvent = Arc<LiveEvent>;
```

### 1.8 DeliveryObligation (`engine/src/live/routing/acknowledgement.rs`, lines 49-55)

```rust
pub struct DeliveryObligation {
    pub id: DeliveryObligationId,
    pub destination: DestinationId,
    pub range: DeliveryCursorRange,
    pub kind: DestinationKind,
}
```

### 1.9 DeliveryObligationId (`engine/src/live/routing/acknowledgement.rs`, lines 23-24)

```rust
pub struct DeliveryObligationId(pub u64);
```

- Monotonically increasing; must not be reused while old acknowledgements may arrive.

### 1.10 DeliveryStatus (`engine/src/live/model/delivery.rs`, lines 114-123)

```rust
pub enum DeliveryStatus {
    Pending,
    Progress(DeliveryStage),
    Failed(DeliveryFailure),
}
```

### 1.11 DeliveryStage (`engine/src/live/model/delivery.rs`, lines 52-74)

```rust
pub enum DeliveryStage {
    Accepted,
    Queued,
    Delivered,
    Committed,
    Durable,
}
```

### 1.12 DeliveryFailure (`engine/src/live/model/delivery.rs`, lines 77-109)

```rust
pub enum DeliveryFailure {
    CapacityExceeded,
    Unavailable,
    Rejected,
    Timeout,
    Cancelled,
    Disconnected,
    PersistenceFailed,
    DurabilityUnavailable,
    CommitOutcomeUnknown,
    Internal,
}
```

- **`Cancelled`** variant exists (line ~93) — but no code produces it.

### 1.13 DeliveryCursorRange (`engine/src/live/routing/acknowledgement.rs`, lines 29-33)

```rust
pub struct DeliveryCursorRange {
    pub first: StreamCursor,
    pub last: StreamCursor,
}
```

### 1.14 DestinationId (`engine/src/live/ports/routing.rs`, lines 85-86)

```rust
pub struct DestinationId(pub u64);
```

### 1.15 DestinationDescriptor (`engine/src/live/ports/routing.rs`, lines 89-95)

```rust
pub struct DestinationDescriptor {
    pub id: DestinationId,
    pub kind: DestinationKind,
    pub policy: DeliveryPolicy,
    pub capacity: DeliveryCapacity,
}
```

### 1.16 DestinationKind (`engine/src/live/model/delivery.rs`, lines 41-46)

```rust
pub enum DestinationKind {
    Publication,
    Recording,
    Retention,
    Derived,
}
```

### 1.17 DeliveryCapacity (`engine/src/live/model/delivery.rs`, lines 149-173)

```rust
pub struct DeliveryCapacity {
    pub max_pending_events: usize,
    pub max_pending_bytes: u64,
}

impl DeliveryCapacity {
    pub fn new(max_pending_events: usize, max_pending_bytes: u64)
        -> Result<Self, DeliveryCapacityError>;
}
```

- Enforces both limits positive; errors: `ZeroEventLimit`, `ZeroByteLimit`.

### 1.18 DeliveryPolicy (`engine/src/live/model/delivery.rs`, lines 19-33)

```rust
pub enum DeliveryPolicy {
    BestEffort,
    Required,
    Durable,
}
```

---

## 2. Cancellation, Clock, and Shutdown Primitives

### 2.1 `engine/src/live/foundation/cancellation.rs`

```rust
//! Planned component: src/live/foundation/cancellation.rs.
//! Scaffold only; no behavior is implemented or verified.
```

### 2.2 `engine/src/live/foundation/clock.rs`

```rust
//! Planned component: src/live/foundation/clock.rs.
//! Scaffold only; no behavior is implemented or verified.
```

### 2.3 `engine/src/live/foundation/shutdown.rs`

```rust
//! Planned component: src/live/foundation/shutdown.rs.
//! Scaffold only; no behavior is implemented or verified.
```

### 2.4 `engine/src/live/foundation/resources.rs`

```rust
//! Planned component: src/live/foundation/resources.rs.
//! Scaffold only; no behavior is implemented or verified.
```

### 2.5 Tokio and tokio-util Availability

**Not present.** A full scan of `engine/Cargo.toml` (951 lines) found **zero** matches for `tokio` or `tokio_util`. Tokio is currently **not a dependency** of the `marketforge-engine` crate.

- **No `CancellationToken`** available — must be added to `[dependencies]`.
- **No `tokio::time::Deadline`** or `tokio::time::sleep` available.
- Any deadline-aware or cancellation-aware `admit()` implementation will require a Tokio dependency.

### 2.6 No Existing Cancellation/Deadline Primitives in the Live Module

The `Cancellation` and `DeadlineExceeded` variants on `RoutingAdmissionFailure` and `DeliveryFailure` are **dead types** — no code produces them and no code consumes them. The `Cancelled` variant on `RoutingFuture`-returning traits is also unconnected to any runtime primitive.

---

## 3. Routing and Acknowledgement Contracts

### 3.1 AcknowledgementTracker (`engine/src/live/routing/acknowledgement.rs` — full impl)

**Bounded capacity — WP-06 implemented.**

```rust
pub struct AcknowledgementTracker {
    obligations: HashMap<DeliveryObligationId, TrackedObligation>,
    max_obligations: usize,
    next_id: Option<u64>,
}
```

**Constructor:** 
```rust
pub fn new(max_obligations: usize) -> Result<Self, AcknowledgementError>
```
- Rejects `max_obligations == 0` → `InvalidCapacity`.
- Initializes `next_id = Some(1)`.

**ID Allocation:**
```rust
pub fn allocate_id(&mut self) -> Result<DeliveryObligationId, AcknowledgementError>
```
- Returns `ObligationIdExhausted` when `next_id` overflows to `None`.
- Uses `checked_add(1)` to detect overflow (never mutates on rejection).
- IDs start at 1, incrementing.

**Capacity Tracking:**
```rust
pub fn capacity(&self) -> usize;                          // returns max_obligations
pub fn remaining_capacity(&self) -> usize;                 // saturating subtraction
```

**Register:**
```rust
pub fn register(&mut self, obligation: DeliveryObligation) -> Result<(), AcknowledgementError>
```
- Rejects `DuplicateObligation` (ID already present).
- Rejects `CapacityExceeded` (len >= max). No mutation on rejection.
- Inserts with `DeliveryStatus::Pending`.

**Acknowledge:**
```rust
pub fn acknowledge(&mut self, acknowledgement: DeliveryAcknowledgement)
    -> Result<(), AcknowledgementError>
```
- Validates: unknown obligation, destination mismatch, range mismatch.
- Validates stage allowed for destination kind (destination-specific transition matrix).
- Validates status transition (no regression, no reset from progress to pending).

**Remove:**
```rust
pub fn remove(&mut self, obligation: DeliveryObligationId)
    -> Result<DeliveryStatus, AcknowledgementError>
```
- Guards pending obligations: rejects `ObligationStillPending` for statuses:
  - `Pending`
  - `Progress(Accepted)`
  - `Progress(Queued)`
- Returns the obligation's final `DeliveryStatus` on success.

**Observation:**
```rust
pub fn status(&self, obligation: DeliveryObligationId) -> Option<DeliveryStatus>;
pub fn len(&self) -> usize;
pub fn is_empty(&self) -> bool;
```

### 3.2 AcknowledgementError Variants (`engine/src/live/routing/acknowledgement.rs`, lines 70-86)

```rust
pub enum AcknowledgementError {
    InvalidRange,
    DuplicateObligation,
    UnknownObligation,
    DestinationMismatch,
    RangeMismatch,
    ProgressRegression,
    AlreadyFailed,
    InvalidAcknowledgement,
    InvalidStageForDestination,
    InvalidStageTransition,
    InvalidCapacity,
    CapacityExceeded,
    ObligationIdExhausted,
    ObligationStillPending,
}
```

### 3.3 DestinationFailure alias

```rust
pub type DestinationFailure = DeliveryFailure;
```

### 3.4 Route-internal Scaffolds

All four remaining files in `engine/src/live/routing/` are scaffolds:

| File | Status |
|------|--------|
| `admission.rs` | Scaffold only |
| `destination.rs` | Scaffold only |
| `router.rs` | Scaffold only |
| `snapshot.rs` | Scaffold only |

### 3.5 Routing Module Declaration (`engine/src/live/routing/mod.rs`)

```rust
pub mod acknowledgement;         // public — the only implemented module
pub(crate) mod admission;        // scaffold
pub(crate) mod cursor;           // scaffold
pub(crate) mod destination;      // scaffold
pub(crate) mod router;           // scaffold
pub(crate) mod snapshot;         // scaffold
```

---

## 4. Existing Cancellation and Clock Primitives

### 4.1 None implemented

All four foundation-level primitives are scaffold-only:

| File | Content |
|------|---------|
| `foundation/cancellation.rs` | 2-line header comment only |
| `foundation/clock.rs` | 2-line header comment only |
| `foundation/shutdown.rs` | 2-line header comment only |
| `foundation/resources.rs` | 2-line header comment only |

### 4.2 No CancellationToken or deadline types anywhere in the `live` module

No imports of `tokio_util::sync::CancellationToken`, `tokio::time::Deadline`, `tokio::time::sleep`, or any similar deadline/primitive appear in any `.rs` file under `engine/src/live/`.

---

## 5. Call Sites

### 5.1 `EventRouterPort::admit()` — **zero call sites**

Grep across `engine/src/` for `fn admit` finds only the **trait declaration** (2 occurrences: the two trait definitions in `ports/routing.rs`). No implementation of `EventRouterPort` exists.

### 5.2 `EventDestinationPort::admit()` — **zero call sites**

Same result. The trait method is declared but never implemented or called.

### 5.3 No `impl EventRouterPort` or `impl EventDestinationPort`

Search for `impl.*EventRouterPort` or `impl.*EventDestinationPort` across the entire `engine/src/` returns no results. The router, destinations, and admission modules are all scaffolds.

---

## 6. Current Bounded-Tracker Implementation Status

| Aspect | Status | Notes |
|--------|--------|-------|
| `max_obligations` | Implemented | Stored as `usize`, returned via `capacity()` |
| `next_id` | Implemented | `Option<u64>`, starts at 1 |
| `allocate_id()` | Implemented | `checked_add(1)` guard; `ObligationIdExhausted` on overflow |
| `register()` | Implemented | Rejects on duplicate or full |
| `remove()` | Implemented | Pending guard blocks removal of pending/accepted/queued |
| `capacity()` | Implemented | Returns `max_obligations` |
| `remaining_capacity()` | Implemented | Saturating subtraction |
| `acknowledge()` | Implemented | Full transition validation, destination-specific stage matrix |
| `status()` | Implemented | Observation only, returns `Option<DeliveryStatus>` |
| Constructor (`new`) | Implemented | Rejects zero capacity |

**WP-06 is fully implemented in `acknowledgement.rs`.** Five new tests added; 30 total routing tests pass.

---

## 7. Existing Tests

### 7.1 Test file: `engine/tests/live/routing.rs`

**30 tests total** (25 pre-existing + 5 WP-06 new).

**Pre-existing tests covering acknowledgement:**
1. `acknowledgement_progresses_monophonically` — monotonic stage progression
2. `acknowledgement_rejects_progress_regression` — backward transition rejection
3. `acknowledgement_rejects_wrong_destination` — destination identity check
4. `acknowledgement_rejects_wrong_range` — range identity check
5. `acknowledgement_rejects_duplicate_obligation` — ID deduplication
6. `acknowledgement_rejects_unknown_obligation` — unknown ID handling
7. `acknowledgement_rejects_invalid_range` — inverted/invalid cursor ranges
8. `publication_rejects_persistence_stages` — destination-stage matrix (Publication)
9. `recording_rejects_transport_delivery_stage` — destination-stage matrix (Recording)
10. `publication_accepts_transport_delivery` — valid transit path (Publication)
11. `retention_accepts_durable_commitment` — final stage delivery (Retention)

**WP-06 new tests:**
12. `tracker_rejects_zero_capacity` — `new(0)` → `InvalidCapacity`
13. `tracker_enforces_obligation_capacity` — capacity limit + no-mutation-on-rejection
14. `tracker_allocates_distinct_ids` — monotonic ID generation
15. `tracker_rejects_removal_of_pending_obligation` — pending guard
16. `tracker_can_remove_failed_obligation` — post-failure removal and status return

### 7.2 No tests covering routing admission

- **Zero tests** for `EventRouterPort::admit()` implementations (none exist).
- **Zero tests** for `EventDestinationPort::admit()` implementations (none exist).
- **Zero tests** for cancellation or deadline behavior (no primitives implemented).
- **Zero tests** for backpressure actions (`BackpressureAction::Wait`, `DisconnectConsumer`, `FailDelivery`, `FailRetentionSession`) — these are defined but never produced.

### 7.3 Integration test target: `live_suite` (`engine/tests/live_suite.rs`)

Requires `live` feature. No detail of its contents was inspected.

---

## 8. Missing Dependencies and Unresolved Interfaces

### 8.1 Blocking: Tokio Dependency

**Tokio is not a dependency of `marketforge-engine`.** This is the single highest-impact finding.

- `RoutingFuture<'a, T>` is defined generically as `Pin<Box<dyn Future<Output = T> + Send + 'a>>` — this is **compatible** with any `Future` implementation. However:
- `tokio::time::Deadline`, `tokio::time::sleep`, `tokio::time::interval` all require `tokio` with the `time` feature.
- `tokio_util::sync::CancellationToken` requires `tokio-util` with the `sync` feature.
- **WP-07 must add Tokio as a dependency** before any deadline or cancellation logic can be wired from `RoutingAdmissionFailure` variants to concrete runtime behavior.

### 8.2 Dead Code Warnings

- `AcknowledgementTracker::new` is `dead_code` — constructor is private and never called by integration tests (which use `new(1024).unwrap()` directly since it's in the test crate). **No blocking issue.**

### 8.3 Pre-existing Trailing Whitespace (Outside Allowlist)

- `delivery.rs` and `ports/routing.rs` contain trailing whitespace warnings. **Outside WP-06 allowlist.**

### 8.4 Unimplemented Router

The `router.rs` scaffold is the **only** place that could wire `CancellationToken`, `Deadline`, or shutdown state into the `admit()` flow. Currently:
- No `impl EventRouterPort` exists anywhere.
- No `impl EventDestinationPort` exists anywhere.
- The `admission.rs` and `destination.rs` scaffolds are the intended homes for admission logic.

### 8.5 Cancellation Dependency Chain

To make `RoutingAdmissionFailure::Cancelled` and `RoutingAdmissionFailure::DeadlineExceeded` live, the following must be connected:

```
CancellationToken (tokio-util)
    └── checked before routing to destinations
          └── EventRouterPort::admit() returns Rejected { Cancelled }

tokio::time::Deadline
    └── checked during admit() await
          └── EventRouterPort::admit() / EventDestinationPort::admit()
              returns Rejected { DeadlineExceeded }

ShutdownHandle (foundation/shutdown.rs scaffold)
    └── checked before routing
          └── returns Rejected { ShuttingDown }
```

None of these wire is currently in place.

### 8.6 `BackpressureAction` — Defined but Unused

`BackpressureAction` is defined in `delivery.rs` but **never produced, returned, or consumed** by any `admit()` path. It is available for future backpressure policy decisions but has no call sites.

---

## 9. Summary of Findings

| Finding | Severity | Notes |
|---------|----------|-------|
| Tokio not a dependency | **Blocking** | Cannot implement deadline/cancellation without adding `tokio` + `tokio-util` |
| Router impl missing | **Blocked** | No `impl EventRouterPort` exists; `router.rs` is a scaffold |
| All admission/destination modules are scaffolds | Blocked | `admission.rs`, `destination.rs`, `router.rs`, `snapshot.rs` — all scaffolds |
| `AcknowledgementTracker` fully implemented | Ready | WP-06 complete, capacity bounds, ID allocation, all tracked |
| Zero call sites for `admit()` | Info | Expected — router not yet implemented |
| No cancellation/deadline primitives | Blocked | `cancellation.rs`, `clock.rs`, `shutdown.rs`, `resources.rs` all scaffolds |
| 30 routing tests pass | Ready | Full acknowledgement tracker test coverage |
| `$2^{64}-1` ID exhaustion | Info | `ObligationIdExhausted` handles id wrap via `Option<u64>` — safe |
| Five new WP-06 tests cover capacity bounds | Ready | All required bounds exercised |

---

*Report written by agent WP-06 inspection. All source paths and line numbers are relative to this repository's working tree.*
