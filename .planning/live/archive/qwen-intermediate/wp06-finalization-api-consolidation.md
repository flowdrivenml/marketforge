# WP-06 — Finalization API Consolidation: Evidence Report

## Objective
Remove unsafe finalization bypass APIs (`finalize()`, `remove()`, `resolve_escalation()`) and migrate all tests to use the authoritative handoff lifecycle through `FinalizationJournal`.

## Changes Made

### `engine/src/live/routing/acknowledgement.rs`
Removed three legacy methods from production code:

| Method | Line (before removal) | Description |
|--------|----------------------|-------------|
| `AcknowledgementTracker::finalize()` | ~863 | Direct finalization that bypassed bounded journal |
| `AcknowledgementTracker::remove()` | ~983 | Direct removal that internally called `finalize()` |
| `FinalizationJournal::resolve_escalation()` | ~472 | Single-step escalation resolution (replaced by two-phase chain) |

All three methods contained doc comments noting they were "retained temporarily for existing contract tests."

### `engine/tests/live/routing.rs`
Migrated / renamed five test functions:

| Old Test Name | New Test Name | Migration |
|---------------|---------------|-----------|
| `durable_recording_cannot_finalize_at_committed` | (kept name) | `finalize()` → `finalize_into()` + journal |
| `durable_recording_finalizes_after_durable_stage` | (kept name) | `finalize()` → `finalize_into()` + journal |
| `required_publication_finalizes_at_delivered` | (kept name) | `finalize()` → `finalize_into()` + journal |
| `required_recording_cannot_finalize_while_queued` | (kept name) | `finalize()` → `finalize_into()` + journal |
| `failed_obligation_returns_explicit_failure_outcome` | (kept name) | `finalize()` → `finalize_into()` + journal |
| `tracker_rejects_removal_of_pending_obligation` | `tracker_rejects_finalization_of_pending_obligation` | `remove()` → `finalize_into()` pattern |
| `tracker_can_remove_failed_obligation` | `tracker_finalizes_failed_obligation` | `remove()` → `finalize_into()` pattern |

Added imports: `FinalizationJournal`, `FinalizationHandoffError`.

### `engine/tests/live/reporting.rs`
Migrated two test functions:

| Old Test Name | New Test Name | Migration |
|---------------|---------------|-----------|
| `resolution_preserves_original_delivery_outcome` | (kept name) | `resolve_escalation()` → `prepare_resolution()` + `sink.accept()` + `commit_resolution()` |
| `non_escalated_record_cannot_be_resolved` | `non_escalated_record_cannot_prepare_resolution` | `resolve_escalation()` → `prepare_resolution()` (verifies `NotEscalated` error) |

## Test Results

```
running 148 tests
test result: ok. 148 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

- 148 tests passed (no regressions).
- All 5 routing.rs finalization tests use `finalize_into()` pattern.
- All 2 routing.rs remove tests renamed and use `finalize_into()` pattern.
- All 2 reporting.rs resolve_escalation tests use `prepare_resolution()` + sink pattern.

## Verification

- `cargo fmt` — applied successfully.
- `cargo test --locked --offline --no-default-features --features live --test live_suite` — 148/148 passed.
- No calls to `finalize()`, `remove()`, or `resolve_escalation()` remain in the codebase (verified by grep).
