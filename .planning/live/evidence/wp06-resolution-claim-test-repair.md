# WP-06: Resolution Claim Test Repair — Evidence Report

## Objective
Fix all 23 compilation errors in `engine/tests/live/resolution_claims.rs` introduced by migration to the lease-based claim API, then verify all 14 new resolution-claim tests plus 158 baseline tests pass.

## Changes Made

### File Modified
- `engine/tests/live/resolution_claims.rs` — 14 lease-based recovery and fencing tests

### Key Type Fixes Applied

1. **`DeliveryObligation` is NOT generic** — removed `<DeliveryAcknowledgement>` type parameter from all type annotations and function signatures

2. **`DeliveryPolicy` is an enum** — replaced struct literal `{ timeout, retry_count }` with enum variants: `DeliveryPolicy::Required`, `DeliveryPolicy::BestEffort`, etc.

3. **`FinalizationRecord` fields corrected** — uses `obligation` and `outcome` fields (not `source`/`cursor`)

4. **`ObligationFinalization` is an enum** — used `ObligationFinalization::Succeeded { id, destination }` instead of struct literal patterns

5. **`DeliveryAcknowledgement` import added** — was missing from imports

6. **`ResolutionClaimToken` type corrected** — `wrong_token` in C2 was `EscalationResolutionToken` (wrong type); changed to `ResolutionClaimToken(999)` which is the type expected by `acknowledge()`

7. **`FinalizationJournal::id()` returns `FinalizationJournalId` directly** — no `inner()` wrapper needed

8. **`BoundedResolutionSink::verify(&receipt)`** — called on the sink, not on `ResolutionClaim`

9. **`DeliveryCursorRange::new(first, last).unwrap()`** — correct construction pattern

10. **Token type discipline** — `ResolutionClaimToken` used for `acknowledge()`/`release_claim()`; `EscalationResolutionToken` used for `prepare_resolution()`/`accept()` — never interchanged

## Test Results

### Compilation
- **Status:** PASS — zero errors, 6 warnings (unrelated pre-existing: unused imports in other test files)

### Test Execution
```
running 172 tests
test result: ok. 172 passed; 0 failed; 0 ignored; 0 measured
```

### Breakdown
- **Baseline tests:** 158 (unchanged, all passing)
- **New resolution-claim tests:** 14 (all passing)
- **Total:** 172 tests, 0 failures

### New Test Coverage (C1–C14)
| Test | Description | Status |
|------|-------------|--------|
| C1 | `zero_lease_duration_rejected` | PASS |
| C2 | `acknowledge_wrong_token_rejected` | PASS |
| C3 | `lease_expiration_rejects_acknowledge` | PASS |
| C4 | `release_then_reclaim_produces_new_claim` | PASS |
| C5 | `double_acknowledge_second_is_nop` | PASS |
| C6 | `acknowledge_before_expiration_succeeds` | PASS |
| C7 | `release_unclaimed_record_fails` | PASS |
| C8 | `receipt_identity_preserved_across_reclaim` | PASS |
| C9 | `capacity_released_after_acknowledge` | PASS |
| C10 | `capacity_not_released_after_release_claim` | PASS |
| C11 | `sequential_claims_on_multiple_records_ordered` | PASS |
| C12 | `expired_lease_keeps_record_claimed` | PASS |
| C13 | `verify_fails_after_acknowledge` | PASS |
| C14 | `verify_survives_release_claim` | PASS |

## API Patterns Verified

1. **Lease claim lifecycle:** `try_take(&clock, duration)` → `acknowledge(&receipt, token, &clock)` or `release_claim(&receipt, token, &clock)`
2. **Lease expiration:** `advance()` on ManualClock followed by acknowledge correctly rejects expired claims
3. **Token mismatch:** Wrong `ResolutionClaimToken` correctly rejected by `acknowledge()`
4. **State transitions:** `Accepted → Claimed → JournalCommitted` via release_claim
5. **Receipt identity:** Preserved across re-claim cycles
6. **Capacity management:** Released only on acknowledge, not on release_claim
7. **Zero lease rejection:** `Duration::ZERO` correctly rejected by `try_take()`

## Warnings (Pre-existing, Unrelated)
- `resolution.rs`: unused `Duration` and `ManualClock` imports (pre-existing)
- `resolution_lifecycle.rs`: unused `ResolutionClaim` import (pre-existing)
- `routing.rs`: unused `Pin`, `Duration`, `Instant` imports (pre-existing)
- `resolution_claims.rs`: unused `make_escalation_record` function (new, safe to leave for future use)

## Conclusion
All 23 compilation errors have been fixed. All 172 tests (158 baseline + 14 new) pass. The lease-based claim API is working correctly with proper type discipline, state management, and lease expiration enforcement.
