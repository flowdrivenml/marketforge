# WP-06 Reporting Retry Test Migration

## 1. Exact Files Modified

- `engine/tests/live/reporting.rs` — reporting module tests only

## 2. Number of Constructors Migrated

3 constructor calls migrated from 2-arg to 3-arg signature.

## 3. Tests Affected

1. **successful_reporting_acknowledges_journal_record** (~line 84)
   - Added `FinalizationRetryPolicy::new(Duration::from_secs(1), Duration::from_secs(30), 5).unwrap()`
   - Updated `FinalizationReportingConsumer::new(reporter, Duration::from_secs(30))` → added `retry_policy`

2. **temporary_reporting_failure_preserves_record** (~line 106)
   - Same migration pattern

3. **empty_journal_returns_idle** (~line 129)
   - Same migration pattern

4. **temporary_failure_does_not_retry_immediately** (~line 147) — already on 3-arg (uses `policy`)
5. **permanent_failure_is_not_retried_automatically** (~line 187) — already on 3-arg (uses `policy`)

## 4. Import Changes

Added `FinalizationRetryPolicy` to the `acknowledgement` import group:

```diff
         acknowledgement::{
             AcknowledgementTracker, DeliveryAcknowledgement, DeliveryCursorRange,
-            DeliveryObligation, DeliveryObligationId, FinalizationJournal,
+            DeliveryObligation, DeliveryObligationId, FinalizationJournal, FinalizationRetryPolicy,
         },
```

## 5. Existing Assertions Preserved

All assertions preserved unchanged:
- `successful_reporting_acknowledges_journal_record`: Reported step, journal empty, call count == 1
- `temporary_reporting_failure_preserves_record`: RetryScheduled step, journal len == 1, call count == 1
- `empty_journal_returns_idle`: Idle step
- `temporary_failure_does_not_retry_immediately`: RetryScheduled → Idle → RetryScheduled, call count progression
- `permanent_failure_is_not_retried_automatically`: PermanentlyFailed, then Idle after clock advance

## 6. Formatting Results

`cargo fmt --check` — PASSED (clean after applying `cargo fmt`)

## 7. Compilation Results

Compilation: PASSED (no errors, no warnings)

## 8. Full Test Summary

**Total tests run: 119**
**Passed: 119**
**Failed: 0**

Reporting tests (5):
1. `successful_reporting_acknowledges_journal_record` — OK
2. `temporary_reporting_failure_preserves_record` — OK
3. `empty_journal_returns_idle` — OK
4. `temporary_failure_does_not_retry_immediately` — OK (already migrated)
5. `permanent_failure_is_not_retried_automatically` — OK (already migrated)

## 9. Unresolved Issues

None.
