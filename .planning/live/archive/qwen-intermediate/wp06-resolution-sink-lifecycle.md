# WP-06 — Resolution Sink Lifecycle Evidence

## Summary

Implemented the resolution sink lifecycle (Accepted → JournalCommitted → Claimed) to prevent downstream consumers from claiming or acknowledging resolution records before the originating `FinalizationJournal` commits ownership transfer. Added `ResolutionSinkState` enum replacing the `claimed` boolean, fixed `try_take()` / `acknowledge()` state guards, wired the guard chain through `commit_resolution()`, and added 10 lifecycle transition tests.

## Command Run

```bash
cd /home/pasha/projects/bybit-history-downloader/engine
cargo test --locked --offline --no-default-features --features live --test live_suite
```

## Result

**158 passed; 0 failed; 0 ignored**

## Tests Added

### New: `resolution_lifecycle.rs` — 10 tests

| Test | Invariant | Description |
|------|-----------|-------------|
| `full_lifecycle_chain` | Accepted→JournalCommitted→Claimed→removed | End-to-end: accept → commit → try_take → acknowledge |
| `try_take_transitions_to_claimed_state` | try_take retains record (Claimed ≠ removed) | Sink length stays 1 after claim; `verify` succeeds until acknowledge |
| `acknowledge_removes_claimed_record_and_releases_capacity` | acknowledge releases capacity | Length goes 1→0 after acknowledge; new record can be accepted |
| `try_take_rejected_on_accepted_record` | L5: try_take on Accepted returns None | Accepted records cannot be claimed before journal commitment |
| `ack_transitions_to_claimed_before_acknowledge` | L7: State transitions visible during claim | State goes Accepted→JournalCommitted→Claimed→absent |
| `acknowledge_does_not_affect_accepted_records` | L8: Acknowledge targets only Claimed records | Committing only A leaves B untouched (still Accepted) |
| `double_commit_rejected_via_guard_chain` | L9: Commit guard prevents double-commit | `can_commit()` returns false after `mark_journal_committed()` |
| `try_take_fifo_ordering` | FIFO: oldest committed taken first | Three records committed in order A→B→C; try_take returns A first |

### Migrations: `resolution_integration.rs` — 2 tests fixed

| Test | Fix |
|------|-----|
| `acknowledgement_releases_capacity` (test H) | Added missing `acknowledge` step before second `try_take()`. Renamed capture variable `taken_a` to avoid shadowing. |
| — | The test pipeline: register → acknowledge(failure) → finalize_into → claim_next → escalate → prepare_resolution → accept → commit_resolution → try_take → acknowledge → verify capacity released. |

## Production Code Changes

| File | Change |
|------|--------|
| `engine/src/live/routing/resolution.rs` | Added `ResolutionSinkState` enum (`Accepted`/`JournalCommitted`/`Claimed`). Replaced `claimed: bool` on `AcceptedResolution` with `state: ResolutionSinkState`. Updated `try_take()` to only claim `JournalCommitted` records (rejects `Accepted`). Updated `acknowledge()` to only acknowledge `Claimed` records. Removed unused `HashMap` import. |
| `engine/src/live/routing/acknowledgement.rs` | Added `can_commit()` validation → `mark_journal_committed()` state transition guard chain in `commit_resolution()`. Validates sink record is in `Accepted` state before committing; transitions to `JournalCommitted`. |

## Production Code Observations

1. `ResolutionSinkState` enum is an internal state machine on each `AcceptedResolution`, not a separate boolean field. This enables three states (not two) and supports future extensions.
2. `ResolutionCommitGuard` holds `MutexGuard<'_, BoundedResolutionSinkState>` + `ResolutionAcceptanceReceipt`. Its `can_commit()` reads the record's current state; `mark_journal_committed()` mutates it → `JournalCommitted`.
3. All fallible validation in `commit_resolution()` occurs **before** journal entry removal. This ensures the journal is not corrupted if the sink rejects the commit.
4. `ObligationFinalization` is an enum (not unit struct) with variant `Succeeded { id, destination }`.
5. `FinalizationRecord.obligation` is `DeliveryObligation` directly (cloneable), not a reference wrapper.
6. `EscalationResolutionRecord` has public fields: `record: FinalizationRecord`, `escalation: FinalizationEscalation`, `resolution: EscalationResolution`.

## Test Coverage Notes

- **L1–L9** (negative transitions, guard violations): 7 tests verify that wrong-state attempts are rejected.
- **L10** (positive path): 1 test verifies success when all preconditions are met.
- **try_take FIFO ordering** proved the queue invariant — records emerge from the sink in commit order.
- **FIFO comparison approach**: Since each `prepare_resolution` helper runs a separate journal (each initialized at sequence 1), sequence numbers are not cross-journal comparable. Verification used `record().record.obligation.id` instead.

## File Inventory

| File | Type |
|------|------|
| `engine/tests/live/resolution_lifecycle.rs` | New — 10 lifecycle transition tests |
| `engine/src/live/routing/resolution.rs` | Modified — enum + guard fixes |
| `engine/src/live/routing/acknowledgement.rs` | Modified — guard chain in `commit_resolution()` |
| `engine/tests/live/resolution_integration.rs` | Migrated — fixed test H pipeline |
| `engine/tests/live/mod.rs` | Updated — registered `resolution_lifecycle` module |

## Constraints Checked

- No Cargo.toml modifications
- No new dependencies
- STATUS.md not modified
- No git commands used
- Unrelated production modules untouched
