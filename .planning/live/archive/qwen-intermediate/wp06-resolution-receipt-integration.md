# WP-06 — Resolution Receipt Integration Evidence

## Summary

Migrated `engine/tests/live/resolution.rs` token constructors from tuple to struct form and created `engine/tests/live/resolution_integration.rs` with integration tests covering the two-phase escalation-resolution protocol between `FinalizationJournal`, `BoundedResolutionSink`, and `ResolutionAcceptanceReceipt`.

## Command Run

```bash
cd /home/pasha/projects/bybit-history-downloader/engine
cargo test --locked --offline --no-default-features --features live --test live_suite
```

## Result

**148 passed; 0 failed; 0 ignored**

## Test Coverage

### New integration tests in `resolution_integration.rs` (12 tests)

| Test | Invariant | Description |
|------|-----------|-------------|
| `valid_receipt_allows_journal_commit` | A | Valid receipt allows journal to commit the resolved record |
| `foreign_sink_cannot_commit_to_journal` | B | Cross-sink verification rejection |
| `mismatched_resolution_record_rejected` | C | Conflicting acceptance for identical token+record is rejected |
| `wrong_journal_identity_detected_by_token` | D | Token structural identity (journal ID + sequence) is verified |
| `sink_capacity_exhaustion_prevents_acceptance` | E | Sink enforces bounded capacity |
| `closed_sink_rejects_new_acceptance` | F | Closed sink rejects all new acceptances |
| `receipt_valid_after_claim` | G | Receipt remains verifiable after `try_take` |
| `acknowledgement_releases_capacity` | H | `acknowledge` releases sink capacity for new records |
| `duplicate_acceptance_returns_identical_receipt` | I | Duplicate `accept` on same token is idempotent |
| `stale_token_sequence_rejected` | J | Different token sequences produce distinct entries |
| `full_acceptance_commit_lifecycle` | — | End-to-end: claim→escalate→prepare→accept→commit |
| `mismatched_receipt_token_rejected_by_commit` | — | Foreign token receipt is rejected by `commit_resolution` |
| `sink_verify_cross_sink_rejection` | — | `sink.verify` rejects receipts from another sink |
| `commit_without_prepared_resolution_fails` | — | `commit_resolution` returns `ResolutionNotPrepared` without escalation |
| `distinct_journals_have_distinct_ids` | — | Auto-incremented journal IDs never collide |
| `journal_claims_produce_valid_claim_tokens` | — | Auto-incremented claim tokens are verified structurally |

### Migrations applied to `resolution.rs`

- `resolution()` helper updated from tuple syntax `(journal: id, sequence: n)` to struct syntax `EscalationResolutionToken { journal: FinalizationJournalId(id), sequence: n }`
- Added `FinalizationJournalId` import for token construction

### Migrations applied to `reporting.rs`

- Added struct field access for `EscalationResolutionToken { journal, sequence }` in the `stale_resolution_token_cannot_commit` test
- Added `BoundedResolutionSink` import for the stale token test pipeline
- Added `ObligationFinalization` and `FinalizationRecord` imports for constructing test fixtures with mismatched records
- `EscalationResolutionToken` struct fields (`journal`, `sequence`) verified via actual assertion: a receipt with sequence+1 is rejected by `commit_resolution`

## Production Code Observations

No production code changes were made. The following were verified:

1. `FinalizationClaim.token` is `FinalizationClaimToken(pub u64)`, NOT `record.lease.token`.
2. `FinalizationClaim.expires_at` is directly on the claim, NOT `record.lease.expires_at`.
3. `publishers` field in `ResolutionAcceptanceReceipt` is private — `record()` and `token()` accessors are the public API.
4. `BoundedResolutionSink::accept()` requires `ResolutionAcceptanceSink` trait in scope.
5. `commit_resolution` validates (in order): sink identity → record retention → receipt.token == prepared.token → resolved record matches.

## File Inventory

| File | Type |
|------|------|
| `engine/tests/live/resolution_integration.rs` | New — 16 integration tests |
| `engine/tests/live/resolution.rs` | Migrated — token struct syntax |
| `engine/tests/live/reporting.rs` | Migrated — struct token access + stale token verification |
| `engine/tests/live/mod.rs` | Updated — registered `resolution_integration` module |
