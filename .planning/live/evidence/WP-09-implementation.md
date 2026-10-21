# WP-09 — Configuration candidates, dependency validation and local journal

Status: Partial. Date: 2026-10-21. No acceptance claim.

Inspected roadmap WP-09, Documents 03/13 persistence, validation and revision
requirements; existing frozen configuration ports/models; current draft global,
source and derived TOML shapes (keys/types only, no secret values printed). No
ignored configuration was modified. Existing inputs differ from the management
DesiredConfiguration DTO and must be explicitly compiled, not deserialized as
though they were already canonical resolved plans.

Implemented:
- Pure candidate changes for every existing ConfigChange variant, optimistic
  revision checking, explicit overrides/inheritance, restore contents without
  rewinding accepted identity, bounded changes and no base/runtime mutation.
- Stable iterative source/derived dependency order, unknown-reference rejection,
  merge duplicate rejection, cycle detection and bounded nodes/edges. Disabled
  persistent acquisition does not erase dependent demand.
- Synchronous cold-path FileRevisionJournal for a dedicated configuration worker:
  exclusive stable sidecar lock, bounded checksummed snapshot/history, immutable
  monotonic revisions, no-op preservation, CAS, targeted history reads, complete
  snapshot recovery, temp-file sync/rename/directory sync before publication,
  cancellation/deadline/generation checks before effects and explicit uncertain
  outcomes after rename. No runtime application is performed by this owner.
- At most one pending artifact; no truncation, promotion or automatic deletion
  of a pre-rename interrupted write. Such state explicitly fails closed pending
  operator recovery. A complete post-rename snapshot reopens as one revision;
  the failed in-process owner remains fenced.
- Shared sanitized desired-state fixture reused by earlier contract tests.

Verification:
`cargo test --locked --offline --no-default-features --features live --test
live_suite wp09_configuration` (engine/): exit 0, 5 passed, 368 filtered.
`cargo test --locked --offline --no-default-features --features live --lib
config::store`: exit 0, initially 4 passed; final full suite includes 6 journal
tests covering all nine persistence fault boundaries, contention, restart,
corruption, missing snapshot, history bounds, no-op, cancellation/deadline, stale
generation, size rejection and checked revision exhaustion.
`./scripts/verify-live.sh full`: exit 0, all five gates pass, 129 library,
371 integration passed / 2 opt-in ignored, 1 doctest. Raw receipts:
`engine/target/live-verification/20261021T031924Z-2/`.

Remaining: full TOML/static-spec loading and versioned schema compilation, C01
authoritative catalog/economic metadata provisioning and verified capability
compilation, semantic input compatibility, complete diff/impact planning,
management/history integration through ConfigurationStorePort on a bounded
worker, observation binding, and runtime application integration. The candidate
constructor deliberately does not implement ConfigurationCompilerPort and
cannot certify economic validity. FileRevisionJournal is a real durable building
block, not a claimed complete configuration service or async fallback adapter.
C12 concrete network/SQL adapter choices also remain open for later consumers.

No dependencies, lockfile, Python, historical behavior, database schemas, real
data, or ignored profile contents changed. Changed: config compiler/validation/
store and exports; shared fixture and configuration tests. Checkpoint: commit
carrying this report. Next independent authorized component: WP-10 (its explicit
prerequisites are event/delivery contracts and observability); WP-09 remains open.
