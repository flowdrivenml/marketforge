# WP-06 Finalization Lease Test Migration

## 1. Files Modified

- `engine/tests/live/routing.rs` — finalization_journal_tests module only

## 2. Tests Migrated

### Tests requiring signature updates

1. **claiming_does_not_release_journal_capacity** (~line 470)
   - `journal.claim_next().unwrap()` → `journal.claim_next(&clock, Duration::from_secs(30)).unwrap().unwrap()`
   - `claimed.obligation.id` → `claim.record.obligation.id`

2. **acknowledgement_releases_journal_capacity** (~line 488)
   - `journal.claim_next().unwrap()` → captured claim with clock + duration
   - `journal.acknowledge(item.id).unwrap()` → `journal.acknowledge(item.id, claim.token, &clock).unwrap()`

3. **unclaimed_record_cannot_be_acknowledged** (~line 507)
   - `journal.acknowledge(item.id)` → `journal.acknowledge(item.id, FinalizationClaimToken(0), &clock)`
   - Added `ManualClock::new()` for clock parameter required by new signature

4. **failed_reporting_can_retry_claim** (~line 527)
   - `journal.claim_next().unwrap()` → `journal.claim_next(&clock, Duration::from_secs(30)).unwrap().unwrap()`
   - `journal.release_claim(item.id).unwrap()` → `journal.release_claim(item.id, first_claim.token, &clock).unwrap()`
   - `assert_eq!(first_claim, second_claim)` → `assert_eq!(first_claim.record.obligation.id, second_claim.record.obligation.id)`

### Tests already on new API (no changes)

5. `expired_claim_can_be_reclaimed_with_new_token` — already migrated
6. `stale_claim_cannot_acknowledge_reclaimed_record` — already migrated
7. `stale_claim_cannot_release_newer_claim` — already migrated
8. `expired_claim_cannot_acknowledge_without_reclaim` — already migrated

### Tests unaffected (no claim/ack/release calls)

9. `journal_rejects_zero_capacity` — tests constructor validation
10. `failed_obligation_transfers_to_journal` — tests tracker→journal transfer
11. `full_journal_preserves_active_obligation` — tests capacity boundary

## 3. Old vs New API Signatures

### claim_next()
- **Old:** `journal.claim_next() -> Option<FinalizationRecord>`
- **New:** `journal.claim_next(clock: &dyn MonotonicClock, lease_duration: Duration) -> Result<Option<FinalizationClaim>, FinalizationJournalError>`
- **Claim struct:** `FinalizationClaim { record: FinalizationRecord, token: FinalizationClaimToken, expires_at: Instant }`

### acknowledge()
- **Old:** `journal.acknowledge(id: DeliveryObligationId) -> Result<FinalizationRecord, FinalizationJournalError>`
- **New:** `journal.acknowledge(id: DeliveryObligationId, token: FinalizationClaimToken, clock: &dyn MonotonicClock) -> Result<(), FinalizationJournalError>`

### release_claim()
- **Old:** `journal.release_claim(id: DeliveryObligationId) -> Result<(), FinalizationJournalError>`
- **New:** `journal.release_claim(id: DeliveryObligationId, token: FinalizationClaimToken, clock: &dyn MonotonicClock) -> Result<(), FinalizationJournalError>`

## 4. Import Changes

Added:
- `use std::time::Duration;`
- `use crate::live::support::manual_clock::ManualClock;`
- `FinalizationClaimToken` to the acknowledgement imports

```diff
  mod finalization_journal_tests {
+     use std::time::Duration;
+
+     use crate::live::support::manual_clock::ManualClock;
+
      use marketforge_engine::live::{
          ...
          routing::acknowledgement::{
              AcknowledgementTracker, DeliveryAcknowledgement, DeliveryCursorRange,
-             DeliveryObligation, DeliveryObligationId, FinalizationHandoffError,
-             FinalizationJournal, FinalizationJournalError, ObligationFinalization,
+             DeliveryObligation, DeliveryObligationId, FinalizationClaimToken,
+             FinalizationHandoffError, FinalizationJournal, FinalizationJournalError,
+             ObligationFinalization,
          },
      };
```

## 5. Assertions Preserved

All original behavioral assertions preserved:
- Claim returns the correct obligation ID (`record.obligation.id`)
- Journal capacity not released by claim alone (len == 1, remaining_capacity == 0)
- Acknowledgement releases capacity (is_empty() + remaining_capacity == desired capacity)
- Unclaimed record acknowledgment returns `NotClaimed` error
- Failed reporting can retry claim and get same obligation
- Stale claims rejected with `StaleClaim` error
- Expired claims reclaimable with new token
- Zero capacity constructor rejected
- Obligation transfer from tracker to journal
- Journal full capacity boundary

## 6. Formatting Results

`cargo fmt --check` — PASSED (clean, no changes needed)

## 7. Compilation Results

Compilation: PASSED (no errors)

Warnings: 2 pre-existing warnings (unused imports in unrelated code, ~line 1383-1384)

## 8. Complete Test Summary

**Total tests run: 114**
**Passed: 114**
**Failed: 0**

Finalization journal tests all green:
- `expired_claim_can_be_reclaimed_with_new_token` — OK
- `stale_claim_cannot_acknowledge_reclaimed_record` — OK
- `stale_claim_cannot_release_newer_claim` — OK
- `expired_claim_cannot_acknowledge_without_reclaim` — OK
- `journal_rejects_zero_capacity` — OK
- `failed_obligation_transfers_to_journal` — OK
- `full_journal_preserves_active_obligation` — OK
- `claiming_does_not_release_journal_capacity` — OK (migrated)
- `acknowledgement_releases_journal_capacity` — OK (migrated)
- `unclaimed_record_cannot_be_acknowledged` — OK (migrated)
- `failed_reporting_can_retry_claim` — OK (migrated)

## 9. Unresolved Issues

None.
