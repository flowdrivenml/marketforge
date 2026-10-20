# WP-06 — Admission Transaction Test Compilation Fix

## Task

Fix the 15 compilation errors in `engine/tests/live/routing.rs` caused by four
admission-transaction concurrency tests being placed outside the
`admission_transaction_tests` module, making them unable to access
`queue_for()` and the `AdmissionTransaction`/`TransactionError` imports scoped
inside the module.

## Exact File Modified

- `engine/tests/live/routing.rs`

## Module-Boundary Corrections

### Before

```text
destination_queue_tests {                      // opened ~line 817
    …
    mod admission_transaction_tests {          // line 1383
        … 3 existing tests + queue_for() …
    }                                           // line 1537 — closes admission_transaction_tests TOO EARLY
    // now OUTSIDE admission_transaction_tests
    #[test]                                     // line 1539 — at destination_queue_tests level
    fn cancellation_after_commit_does_not_revoke_admission() {
        // uses queue_for — E0425: not found in scope
    }
    #[test]
    fn shutdown_before_commit_rejects_without_losing_batch() {
        // uses queue_for — E0425
    }
    #[test]
    fn concurrent_cancellation_and_commit_have_consistent_outcome() {
        // uses AdmissionTransaction, TransactionError — E0425/E0433
    }
    #[test]
    fn concurrent_shutdown_and_commit_have_consistent_outcome() {
        // uses AdmissionTransaction, TransactionError — E0425/E0433
    }
}                                               // line 1722 — closes destination_queue_tests
```

### After

Removed the premature closing `}` (original line 1537), allowing all four tests
to remain inside `admission_transaction_tests`. Then ran `cargo fmt` to
recursively indent them by +4 spaces to match the nested module level.

```text
destination_queue_tests {                      // opened ~line 817
    …
    mod admission_transaction_tests {          // line 1383
        … 3 existing tests + queue_for() …
        #[test]                                 // now at 8-space indent (inside mod)
        fn cancellation_after_commit_does_not_revoke_admission() {
            // queue_for IS IN SCOPE ✓
        }
        #[test]
        fn shutdown_before_commit_rejects_without_losing_batch() {
        }
        #[test]
        fn concurrent_cancellation_and_commit_have_consistent_outcome() {
            // AdmissionTransaction, TransactionError ARE IN SCOPE ✓
        }
        #[test]
        fn concurrent_shutdown_and_commit_have_consistent_outcome() {
        }
    }                                           // closes admission_transaction_tests
}                                               // closes destination_queue_tests
```

The two extra closing braces added at end of file (original line 1721 was only
one `}`) balance the modules opened but not closed after removing the premature
close brace.

## Import Corrections

No imports were added or removed. The four tests now correctly inherit imports
from the `admission_transaction_tests` module scope:

- `std::sync::Barrier`
- `std::thread`
- `AdmissionTransaction` (from `marketforge_engine::live::routing::transaction`)
- `TransactionError` (from `marketforge_engine::live::routing::transaction`)
- `QueueError` (from `marketforge_engine::live::routing::queue`)
- `CancellationToken`
- `Context`, `Poll`, `Waker`
- `AdmissionContext`, `DestinationCapacity`, `DestinationQueue`
- `EventBatch`
- `RetainedSize`, `DeliveryCapacity`

No unused imports were introduced.

## Compilation Result

```
Finished `test` profile [unoptimized + debuginfo] target(s) in 0.29s
```

Compilation succeeds with zero errors and only 2 pre-existing unused import
warnings (not addressed).

## Complete Test Results

```
running 94 tests
test live::routing::destination_queue_tests::admission_transaction_tests::cancellation_after_commit_does_not_revoke_admission ... ok
test live::routing::destination_queue_tests::admission_transaction_tests::cancellation_before_commit_preserves_batch ... ok
test live::routing::destination_queue_tests::admission_transaction_tests::closed_queue_rejects_prepared_transaction ... ok
test live::routing::destination_queue_tests::admission_transaction_tests::shutdown_before_commit_rejects_without_losing_batch ... ok
test live::routing::destination_queue_tests::admission_transaction_tests::transaction_commit_transfers_ownership ... ok
test live::routing::destination_queue_tests::admission_transaction_tests::transaction_preparation_preserves_batch_ownership ... ok
test live::routing::destination_queue_tests::admission_transaction_tests::concurrent_cancellation_and_commit_have_consistent_outcome ... ok
test live::routing::destination_queue_tests::admission_transaction_tests::concurrent_shutdown_and_commit_have_consistent_outcome ... ok
… (86 additional tests) …

test result: ok. 94 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

All 94 tests pass, including all 8 `admission_transaction_tests` (3 pre-existing
+ 4 concurrency tests + 1 existing `transaction_preparation_preserves_batch_ownership`
that was not affected by the fix).

## Remaining Warnings or Errors

2 pre-existing unused import warnings (not in scope):

```
warning: unused import: `std::pin::Pin`
   --> tests/live/routing.rs:842:9

warning: unused imports: `Duration` and `Instant`
   --> tests/live/routing.rs:843:21
```

## Unresolved Issues

None. All compilation errors resolved. All tests pass. Formatting clean.

## Final Response

- **STATUS**: PASS
- **Evidence report path**: `.planning/live/evidence/wp06-transaction-test-scope-fix.md`
- **Tests passed / failed**: 94 / 0
- **Blocking issues**: None
