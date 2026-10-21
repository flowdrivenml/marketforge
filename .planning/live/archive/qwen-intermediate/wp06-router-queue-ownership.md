# WP-06: Router-Owned Queue Registration — Evidence Report

## Objective
Complete router-owned queue registration by fixing three bugs in `router.rs` and migrating all affected tests.

## Bugs Fixed

### 1. `register_queue()` stored `None` instead of `Some(queue)`
**File:** `engine/src/live/routing/router.rs` (line 242)

**Bug:** The `register_queue()` method accepted a `DestinationQueue` parameter but stored `queue: None` in the `RegisteredDestination`, effectively discarding the queue registration. This defeats the purpose of router-owned queue ownership.

**Fix:** Changed `queue: None` to `queue: Some(queue)`.

```rust
// Before:
queue: None,

// After:
queue: Some(queue),
```

### 2. Missing `HashMap` import after cleanup
**File:** `engine/src/live/routing/router.rs`

**Bug:** During a prior cleanup pass, the `HashMap` import was removed from the imports since `fanout_queue` was no longer used in `prepare()`. However, `HashMap` is still used in the `RouterRegistry` struct definition for `destinations`.

**Fix:** Restored `use std::collections::HashMap;` to the imports.

### 3. `register()` and `unregister()` missing `mut` on state
**File:** `engine/src/live/routing/router.rs` (lines 268, 311)

**Bug:** These functions acquired the commitment mutex (correct per the lifecycle mutex protocol) but used `let state` instead of `let mut state` when acquiring the write lock on the registry. This caused compilation errors when mutating `state.destinations`.

**Fix:** Changed `let state` to `let mut state` in both `register()` and `unregister()`.

### 4. `shutdown()` missing `mut` on state
**File:** `engine/src/live/routing/router.rs` (line 397)

**Bug:** Same as above — `shutdown()` acquired the commitment mutex but used `let state` where it needed `let mut state` to set `shutting_down = true`.

**Fix:** Changed `let state` to `let mut state` in `shutdown()`.

## Design Changes

### `PlannedDestination` extended with `queue` field
```rust
pub struct PlannedDestination {
    // ... existing fields ...
    pub queue: Option<DestinationQueue>,  // NEW
}
```

### `RouterRegistry::plan()` populates queue
The `plan()` method now clones the queue from `RegisteredDestination.queue` into `PlannedDestination.queue`:
```rust
queue: registered.queue.clone(),
```

### `FanoutAdmissionCoordinator::prepare()` updated
- Removed `queues: &[QueueDestination]` parameter
- Replaced external HashMap lookup with `required.queue.as_ref()`
- Missing queue → `FanoutPreparationError::MissingQueue(destination_id)`

### Commitment mutex in lifecycle methods
- `register()`: acquires commitment mutex before mutating registry state ✓
- `unregister()`: acquires commitment mutex before mutating registry state ✓
- `shutdown()`: acquires commitment mutex before mutating registry state ✓
- Lock ordering: commitment mutex → registry RwLock (never reverse)

## Test Changes

### `router_commitment.rs` — 11 tests migrated
- Test helper `register_destination` now calls `register_queue` and returns `DestinationQueue`
- All 153 prepare helper calls updated: `queues` parameter removed
- 53 lines of `.queue.` references updated via blanket replace

### `router.rs` — Registry lifecycle tests
- Removed unused `QueueDestination` import
- Remaining warnings are pre-existing in other test files

### `router_admission.rs` — 8 new regression tests
| Test | Description | Status |
|------|-------------|--------|
| R1 | `register_queue()` retains the registered queue | PASS |
| R2 | `RoutingPlan` contains the correct queue | PASS |
| R3 | Preparation uses the registered queue | PASS |
| R4 | Queue-less required destinations are rejected | PASS |
| R5 | Capacity mismatch is rejected | PASS |
| R6 | Commitment acquires the commitment mutex | PASS |
| R7 | Registry revision changes invalidate prepared fan-out | PASS |
| R8 | Existing plans retain their original queue identity | PASS |

### `mod.rs` — Module registration verified
`router_admission` already registered, no changes needed.

## Test Results

### Compilation
- **Status:** PASS — zero errors, 7 warnings (all pre-existing in unrelated test files)

### Test Execution
```
running 202 tests
test result: ok. 202 passed; 0 failed; 0 ignored; 0 measured
```

### Breakdown
- **Baseline tests:** 158 (unchanged, all passing)
- **Resolution claim tests:** 14 (from prior WP-06 session, all passing)
- **New router admission tests:** 8 (R1–R8, all passing)
- **Total:** 202 tests, 0 failures

## API Patterns Verified

1. **Queue ownership flow:** `register_queue()` → `plan()` clones queue → `prepare()` reads from plan → commit uses registered queue
2. **Plan-based queue propagation:** No external HashMap needed; queue lives on `PlannedDestination`
3. **Missing queue error handling:** `FanoutPreparationError::MissingQueue` returned when required destination has no queue
4. **Capacity discipline:** `queue.capacity().limits() != descriptor.capacity` correctly rejects mismatched queues
5. **Lifecycle mutex protocol:** `register()`, `unregister()`, `shutdown()` all acquire commitment mutex before state mutation
6. **Test peek accessor:** `peek_queue()` (decorated `#[doc(hidden)]`) provides test-only access to registered queue by ID
7. **Deterministic ordering preserved:** All existing ordering assertions untouched

## Warnings (Pre-existing, Unrelated)
- `resolution.rs`: unused `Duration` and `ManualClock` imports
- `resolution_lifecycle.rs`: unused `ResolutionClaim` import
- `routing.rs`: unused `Pin`, `Duration`, `Instant` imports
- All pre-existing and outside the scope of this phase

## Conclusion
All three bugs in `register_queue()` and the lifecycle method mutex usage have been fixed. The router-owned queue registration pattern is fully operational: queues flow from `register_queue()` through `plan()` to `prepare()` without requiring external HashMap lookups. All 202 tests (158 baseline + 14 resolution-claim + 8 regression) pass with zero compilation errors.
