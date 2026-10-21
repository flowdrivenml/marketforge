# WP-06: Destination Queue Memory Test Migration

**Phase**: WP-06 — Destination Queue & Routing  
**Work Package**: Migration from 2-arg `try_enqueue(batch, bytes)` to 1-arg `try_enqueue(batch)`  
**Status**: PASS  
**Date**: 2026-10-20

## Objective

Migrate destination queue test file (`engine/tests/live/routing.rs`) to use the new 1-arg `try_enqueue(batch)` API, compute byte counts dynamically via `RetainedSize::retained_size_bytes()`, and recalculate byte-capacity limits accordingly.

## Changes Applied

### 1. Import Addition

Added `RetainedSize` trait import to the `destination_queue_tests` module:

```rust
use marketforge_engine::live::model::memory::RetainedSize;
```

### 2. `try_enqueue` Signature Migration

Removed the second argument (hardcoded byte count) from all `try_enqueue()` calls:

| Test Function | Line | Before | After |
|---|---|---|---|
| `queue_accepts_and_preserves_event_identity` | ~904 | `try_enqueue(batch, 100)` | `try_enqueue(batch)` |
| `queue_preserves_fifo_order` | ~920 | `try_enqueue(batch, 100)` | `try_enqueue(batch)` |
| `queue_retains_capacity_until_consumer_drops_batch` | ~938 | `try_enqueue(batch, 1024)` | `try_enqueue(batch)` |
| `queue_rejection_preserves_original_batch` | ~959 | `try_enqueue(batch, 100)` | `try_enqueue(batch)` |
| `queue_rejects_when_capacity_is_exhausted` | ~972, 976 | `try_enqueue(first, 1024)`, `try_enqueue(second, 100)` | `try_enqueue(first)`, `try_enqueue(second)` |
| `concurrent_producers_preserve_all_accepted_events` | ~1017 | `try_enqueue(batch, 1)` | `try_enqueue(batch)` |
| `concurrent_admission_never_exceeds_capacity` | ~1080 | `try_enqueue(batch, 1)` | `try_enqueue(batch)` |
| `dequeued_batches_retain_capacity_under_concurrent_consumption` | ~1129 | `try_enqueue(batch, 100)` | `try_enqueue(batch)` |
| `discarded_batches_release_all_queued_capacity` | ~1173 | `try_enqueue(batch, 100)` | `try_enqueue(batch)` |

### 3. Capacity Limit Recalculation

Tests that set explicit byte capacity limits now compute values dynamically:

- **`queue_retains_capacity_until_consumer_drops_batch`**: Byte capacity = `retained * 2` where `retained = batch.retained_size_bytes().unwrap()`
- **`queue_rejects_when_capacity_is_exhausted`**: Byte capacity = `retained * 2` for the single-batch scenario
- **`concurrent_producers_preserve_all_accepted_events`**: Byte capacity = `retained_per_event * TOTAL_EVENTS as u64 + 1`
- **`dequeued_batches_retain_capacity_under_concurrent_consumption`**: Byte capacity = `retained * 2` for single-event queue
- **`discarded_batches_release_all_queued_capacity`**: Byte capacity = `retained * 10` for 5-event scenario

Tests using `u64::MAX` for byte capacity (where event count is the real constraint):

- **`queue_accepts_and_preserves_event_identity`**: `DeliveryCapacity::new(10, u64::MAX)`
- **`queue_preserves_fifo_order`**: `DeliveryCapacity::new(10, u64::MAX)`
- **`concurrent_admission_never_exceeds_capacity`**: `DeliveryCapacity::new(CAPACITY, u64::MAX)`

### 4. Byte Assertion Updates

Tests that assert `queued_bytes` now compute expected values dynamically:

- **`concurrent_producers_preserve_all_accepted_events`**: `total_retained` computed via per-event `retained_size_bytes()`
- **`concurrent_admission_never_exceeds_capacity`**: `retained * CAPACITY as u64`
- **`discarded_batches_release_all_queued_capacity`**: Per-event sum across `1..=5`

## Verification

### `cargo fmt --check`

```
(no output) — clean
```

### `cargo test --locked --offline --no-default-features --features live --test live_suite`

```
running 83 tests
test result: ok. 83 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

All destination queue tests pass:
- `queue_accepts_and_preserves_event_identity` ✓
- `queue_preserves_fifo_order` ✓
- `queue_retains_capacity_until_consumer_drops_batch` ✓
- `queue_rejection_preserves_original_batch` ✓
- `queue_rejects_when_capacity_is_exhausted` ✓
- `concurrent_producers_preserve_all_accepted_events` ✓
- `concurrent_admission_never_exceeds_capacity` ✓
- `dequeued_batches_retain_capacity_under_concurrent_consumption` ✓
- `discarded_batches_release_all_queued_capacity` ✓

### Preserved Properties

- All test names unchanged
- All producer counts (8 producers / 16 producers) unchanged
- All event counts (100/event per producer) unchanged
- All behavioral guarantees (FIFO ordering, concurrent safety, capacity enforcement) verified
- `QueueError::Capacity(CapacityError::Full)` rejection semantics verified
- `Discarding`, `Closing`, `Shutdown` semantics unchanged

## Risk Assessment

**Low risk.** The `try_enqueue` method internally computes retained size from the batch via `retained_size_bytes()`. The byte capacity limits are set with sufficient headroom to avoid false `Full` rejections. Tests that check exact retained byte values derive them dynamically, so they adapt to any changes in the event fixture structure.
