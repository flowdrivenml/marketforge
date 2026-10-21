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

Checkpoint: `133b72e` (ordered tracked fan-out and verification evidence). No push. Next authorized: WP-10 completion.


## Continuation — bounded namespace ownership (2026-10-21)

Status: Partial. Next executable package remains WP-10. Baseline branch
`codex/wp06-completion`, HEAD `5607784`; only pre-existing untracked
`scripts/run-autonomous-codex.sh`, preserved. Earlier implementations were
inspected and retained. No package acceptance is asserted.

Implemented:
- `AdmissionNamespaces` owns bounded explicit `(StreamId, LogEpoch)` frontiers;
  arriving batches cannot establish or reset them. Concurrent registration has
  one winner. Independent epochs share no frontier state.
- Retirement and shutdown close all cloned admission handles. An existing permit
  may finish or drop; retirement never rolls back responsibility. No replacement
  owner is created for that namespace. Retired entries remain bounded tombstones
  to prevent same-epoch offset reuse. Lifetime capacity exhaustion fails closed.
- `OrderedFanout::prepare_registered` resolves the authoritative registered
  frontier and uses the existing real ordered/tracked queue commitment path.
- Three deterministic consumer witnesses exercise real two-destination queue
  admission/readback, shared-frontier exclusion, unknown namespace rejection,
  duplicate registration, stale handles, invalid identities/limits, bounded
  tombstones, independent epochs, inflight retirement, idempotent shutdown and
  barrier-controlled concurrent registration. No sleeps or external services.

Verification (all exit 0):
- From engine/: `cargo test --locked --offline --no-default-features --features
  live --test live_suite registered_namespace`: 3 passed / 385 filtered out.
- From repository root: `./scripts/verify-live.sh full`: five gates;
  `cargo fmt --all -- --check`, `cargo check --locked --offline
  --no-default-features --features live --lib`, library tests (130 passed),
  live_suite (386 passed / 2 opt-in ignored), doctests (1 passed).
  Raw receipts: `engine/target/live-verification/20261021T035235Z-2/`.
- From engine/: `cargo check --locked --offline --no-default-features --features
  live,historical`: exit 0; raw receipt
  `engine/target/live-verification/namespace-combined-check.log`.
- `git diff --check`: exit 0. Existing dead-code warnings remain.

Changed paths: routing/{cursor,mod,namespaces,ordered}.rs,
engine/tests/live/router_obligations.rs, STATUS.md and this ledger.
No dependencies, lockfile, Python, historical execution, ignored configuration,
production data or external exchange changes. No SQL fixtures were required or
run; the opt-in PostgreSQL/performance tests were not rerun.

Limitations: runtime-local ownership only, not a restart journal or persisted
namespace reclamation policy. Integration with the production EventRouterPort
owner is still pending. Required worker/completion correlation, bounded global
scheduling, owner-lifetime recovery/payload retention, best-effort/no-subscriber
policies, native metrics/snapshots and deadline shutdown remain incomplete.
Earlier WP-05/06/07/09 gates remain open. Further independent WP-10 work is safe;
WP-11 prerequisites are not declared satisfied. Checkpoint: `045d860` (namespace ownership, witnesses and package evidence).
