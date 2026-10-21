# WP-10 — Recovery-aware fan-out repair and ordered admission primitive

Status: Partial. Date: 2026-10-21. No production router readiness claimed.

Requirements inspected: roadmap WP-10; current routing ports, registry, queues,
acknowledgement tracker, recovery capacity, finalization and their consumers.
Existing implementation is preserved rather than replaced.

Implemented:
- `commit_tracked_with_recovery` now performs real queue commitment. Previously
  every otherwise-valid call aborted prepared obligations and returned rejection.
- Full mapping, registry state and recovery reservations are checked before
  activation. Acknowledgement templates are built before irreversible queue
  writes. Queue outcomes flow through the existing recovery owner.
- Public owned RecoveryPending outcomes preserve failed tracking state and
  reservations even on defensive mapping/count errors. Successful queueing is
  Queued, never Committed or Durable. Partial required failures remain explicit.
- Registry registration, removal and shutdown share the commitment fence, so
  registry mutation cannot interleave between validation and queue commitment.
- Destination registration is bounded (configurable limit; 1024 compatibility
  default) and capacity rejection does not change the registry revision.
- StreamAdmissionOrder has an explicit authoritative starting frontier, one
  active admission per stream/log namespace, contiguous offset checks, RAII
  release on uncommitted drop, independent namespaces and checked exhaustion.
  This primitive does not allocate event identities or infer epoch ordering.

Verification:
Focused `cargo test --locked --offline --no-default-features --features live
--test live_suite recovery_aware` (engine/): exit 0, 5 passed.
Focused `... --test live_suite router_obligations`: exit 0, 52 passed before
adding the registration-bound witness (included in final full verification).
Final `./scripts/verify-live.sh full`: exit 0, all five gates pass, 130 library,
376 integration passed / 2 opt-in ignored, 1 doctest. Raw receipts:
`engine/target/live-verification/20261021T032801Z-2/`.
All existing assertions preserved. Historical execution and Python unchanged.

Remaining acceptance work:
- Production EventRouterPort::admit composition and authoritative runtime owner.
- Bind the order permits, cancellation/deadlines and global admission budgets
  into that entry point; the standalone frontier is not end-to-end ordering.
- Real destination worker/completion correlation, best-effort isolation and
  no-subscriber handling, policy-specific delivery/recovery and shutdown.
- Retain required failed delivery payloads and recovery results through real
  owner lifetime and finalization. Returned ownership alone is not a running
  recovery service.
- Router native metrics/control snapshots and end-to-end concurrency/fault tests.
- Consolidate transitional APIs so production cannot bypass obligation tracking.

Changed: routing router/cursor/exports and router_obligations integration tests.
No package accepted. Checkpoint: commit carrying this report. Next authorized
work remains WP-09/WP-10 completion; WP-11 follows satisfied prerequisites.

Final cross-feature receipts: `engine/target/live-verification/campaign-final-1792553404735277127/results.json`. Both compilation
gates exit 0; the two opt-in test commands each execute and pass one test.

## Continuation — ordered tracked admission (2026-10-21)

Status: Partial. Existing checkpoint `01f378f` preserved. Changed production
paths: `routing/ordered.rs`, `routing/router.rs`, `routing/mod.rs`; consumer
witnesses: `tests/live/router_obligations.rs`. No historical, Python, dependency,
ignored configuration, database or external exchange changes.

Implemented acceptance ledger:
- Required/durable fan-out capacity preparation holds the authoritative
  per-namespace order permit until synchronous tracked commitment.
- The prepared operation is bound to its originating registry. No tracker
  obligations are allocated while waiting, so dropping the future or prepared
  operation releases resources without orphaning pending obligations.
- Cancellation and injected-clock deadlines are checked before and after waiting,
  before tracker preparation, and under the registry commitment lock immediately
  before activation. Context rejection aborts obligations and releases recovery
  reservations; exact obligation errors remain distinguishable.
- Fully rejected precommit operations preserve the frontier. Complete, partial,
  and owned recovery outcomes advance once, preserving immutable batch identity.
- Seven deterministic witnesses exercise successful real queue readback,
  overlapping admission, duplicate cursor rejection, cancellation, shutdown,
  expired entry/final deadline, tracker pressure, dropping preparations and a
  pending multi-sink reservation, and partial queue closure with payload retention.
  No sleeps, exchanges or writable database fixtures are used.

Remaining: production EventRouterPort owner and bounded global scheduling,
namespace lifecycle registration, best-effort/no-subscriber policy composition,
destination workers and completion correlation, owner-lifetime payload/recovery
retention, native snapshots/metrics and deadline shutdown. No downstream package
is asserted executable solely because this coordinator exists. Earlier WP-05/06
and WP-09 unresolved gates remain as recorded in their own ledgers.

Verification (from `engine/`, locked/offline):
- `cargo fmt --all -- --check`: exit 0.
- `cargo check --locked --offline --no-default-features --features live --lib`: exit 0.
- `cargo test --locked --offline --no-default-features --features live --lib`:
  exit 0, 130 passed.
- `cargo test --locked --offline --no-default-features --features live --test live_suite`:
  exit 0, 383 passed / 2 opt-in ignored. Includes all seven new witnesses.
- `cargo test --locked --offline --no-default-features --features live --doc`:
  exit 0, 1 passed.
Full wrapper `./scripts/verify-live.sh full`: exit 0, five gates. Raw receipts:
`engine/target/live-verification/20261021T034918Z-2/`.
- `cargo check --locked --offline --no-default-features --features live,historical`:
  exit 0. Existing dead-code warnings remain; historical tests and opt-in
  PostgreSQL/performance fixtures were not rerun in this continuation.
- `git diff --check`: exit 0.

Checkpoint: local commit carrying this continuation; exact identifier recorded
in STATUS.md after commitment. No push. Next authorized: WP-10 completion.
