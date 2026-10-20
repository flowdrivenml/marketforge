# WP-06 Escalation Resolution Compilation Fix

## 1. Exact Files Modified

- `engine/src/live/routing/acknowledgement.rs` — five migration corrections from obsolete `resolution_token` to `prepared_resolution`.
- `engine/tests/live/reporting.rs` — added `EscalationResolutionToken` import (test compiler fix).

## 2. Compilation Errors and Corrections

### Error 1: `entry` not in scope (authorize_retry)
**Line ~397:** `if entry.prepared_resolution.is_some()` — `entry` was referenced before it was bound.

**Fix:** Moved the prepared-resolution guard AFTER the `iter_mut().find()` binding that creates `entry`. Also removed spurious `mut` qualifier (Rust compiler confirmed mutation occurs through the mutable reference, not variable rebinding).

### Error 2: `resolution_token` unknown field (prepare_resolution)
**Line 304:** `entry.resolution_token.is_some()` → Changed to `prepared_resolution.is_some()`.

**Fix:** Used `entries.get(position).prepared_resolution.is_some()` via chained `.get()` calls to avoid holding an immutable borrow across the subsequent mutable write at line 316.

### Error 3: `resolution_token` unknown field (prepare_resolution write)
**Line 316:** `self.entries[position].resolution_token = Some(token)` → Changed to `self.entries[position].prepared_resolution = Some((token, resolution))`.

**Fix:** Store both token AND the requested resolution kind together, replacing the obsolete single-field write.

### Error 4: `resolution_token` unknown field (commit_resolution)
**Lines 342-348:** Two-phase check — first read `resolution_token`, then read `prepared_resolution` separately. Logically inconsistent and field-obsolete.

**Fix:** Replaced the entire block with a single `.get(position).prepared_resolution` retrieval via chained `.get()` calls, destructure `(stored_token, resolution)`, validate token, then proceed to remove the entry.

### Error 5: `resolution_token: None` in test JournalEntry init
**Line 800:** `resolution_token: None` → Changed to `prepared_resolution: None`.

### Error 6: Missing `EscalationResolutionToken` import in test
**tests/live/reporting.rs line 448:** `EscalationResolutionToken(prepared.token.0 + 1)` — type not in scope.

**Fix:** Added `EscalationResolutionToken` to the first `use` import group (acknowledgement module).

## 3. Obsolete `resolution_token` References Eliminated

All references to the obsolete `resolution_token` field have been eliminated except for the structurally correct `next_resolution_token` field of `FinalizationJournal`, which generates new tokens. The `next_resolution_token` uses a different semantic role (token generation, not storage) and was not modified.

## 4. Prepared-Resolution Semantics Verification

**Two-phase protocol preserved:**
- `prepare_resolution()` stores `(token, resolution)` in `prepared_resolution` — does NOT remove the record.
- Duplicate preparation is rejected via `ResolutionAlreadyPrepared` error.
- `commit_resolution(token)` retrieves the prepared `(stored_token, resolution)`, validates `stored_token == token`, then removes the entry and returns the record.
- Stale token cannot commit — returns `StaleResolutionToken`.
- `authorize_retry()` checks `prepared_resolution.is_some()` BEFORE other guards — cannot invalidate a prepared resolution.
- Journal capacity released only in `commit_resolution()` after successful validation.

## 5. Formatting Results

`cargo fmt --check` — PASSED (clean).

## 6. Compilation Results

Library build — PASSED (0 errors, 0 warnings).

## 7. Full Test Results

**Total tests run: 126**
**Passed: 126**
**Failed: 0**

Reporting module tests (10):
1. `authorized_retry_preserves_original_failure` — OK
2. `empty_journal_returns_idle` — OK
3. `non_escalated_record_cannot_be_resolved` — OK
4. `escalated_record_remains_inspectable` — OK
5. `permanent_failure_is_not_retried_automatically` — OK
6. `prepared_resolution_retains_journal_record` — OK
7. `resolution_commit_preserves_prepared_resolution_kind` — OK
8. `resolution_preserves_original_delivery_outcome` — OK
9. `stale_resolution_token_cannot_commit` — OK
10. `successful_reporting_acknowledges_journal_record` — OK
11. `temporary_failure_does_not_retry_immediately` — OK
12. `temporary_reporting_failure_preserves_record` — OK

## 8. Remaining Warnings

Two pre-existing unused import warnings in `tests/live/routing.rs`:
- `Pin` ~line 1384
- `Duration` and `Instant` ~line 1385

These are unrelated to this WP and were noted in WP-05 completion.
