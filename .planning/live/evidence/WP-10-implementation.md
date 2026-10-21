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


## Continuation — bounded global preparation (2026-10-21)

Status: Partial. Next authorized/executable package: WP-10 completion.
Baseline HEAD `b9b3a94`, branch `codex/wp06-completion`; pre-existing untracked
`scripts/run-autonomous-codex.sh` preserved. Current source, history, status,
roadmap WP-10 and recorded decisions inspected; no completed work restarted.

Implemented:
- `OrderedAdmissionBudget` composes the existing WP-08 atomic resource budget
  with registered ordered tracked fan-out. Event count and canonical retained
  memory are charged before any destination capacity wait; no caller-supplied
  byte estimate can undercount the batch. Checked conversion fails closed.
- Global exhaustion rejects explicitly without allocating a waiter queue.
  Charges survive preparation and release on dropped futures/preparations,
  cancellation, errors and synchronous commitment. Queue/obligation capacity
  remains owned by existing downstream mechanisms after responsibility transfer.
- Closing the budget rejects new preparation without revoking in-flight work.
  Owner shutdown must also close namespaces/cancel waits; close alone is not
  advertised as a complete router shutdown.
- Two deterministic real-consumer witnesses cover count and independent byte
  exhaustion, exact usage, prepared drop, slow second destination, pending-future
  drop, cancellation wakeup, closure, tracked commitment and queue readback.

Verification (all exit 0):
- engine/: `cargo test --locked --offline --no-default-features --features live
  --test live_suite bounded_ordered_admission`: 2 passed, 388 filtered out.
- root: `./scripts/verify-live.sh full`: five gates pass. Exact commands:
  `cargo fmt --all -- --check`; `cargo check --locked --offline
  --no-default-features --features live --lib`; `cargo test --locked --offline
  --no-default-features --features live --lib` (130 passed); same test command
  with `--test live_suite` (388 passed / 2 opt-in ignored); same with `--doc`
  (1 passed). Commands execute in engine/. Receipts:
  `engine/target/live-verification/20261021T035519Z-2/`.
- engine/: `cargo check --locked --offline --no-default-features --features
  live,historical`: exit 0; receipt
  `engine/target/live-verification/admission-budget-combined-check.log`.
- `git diff --check`: exit 0. Existing dead-code warning remains.

Changed: `engine/src/live/routing/ordered.rs`,
`engine/tests/live/router_obligations.rs`, STATUS.md and this ledger.
No SQL or exchange requests; opt-in tests and historical baseline tests were
not rerun. No Python, historical execution, dependencies, lockfile, ignored
configuration or user launcher changes. No push.

Limitations: bounded preparation is available through this coordinator; existing
lower-level compatibility APIs are preserved. Production EventRouterPort owner
integration is pending and must not bypass this bound. Production workers,
completion/recovery ownership, best-effort/no-subscriber policies, native
observations and deadline shutdown remain incomplete. No package acceptance
asserted. Earlier foundational blockers remain unchanged. Checkpoint: commit
carrying this report; further independent WP-10 work can safely continue.


## Continuation — isolated optional queue fan-out (2026-10-21)

Status: Partial. Baseline `00d9990`, branch `codex/wp06-completion`; only
pre-existing untracked `scripts/run-autonomous-codex.sh`, preserved. Inspected
AGENTS, authorized plan, decisions, current status, roadmap WP-10 and current
routing/queue/port implementations and consumers. WP-10 remains next executable.

Implemented:
- Nonwaiting optional queue fan-out validates cursor range, registry revision,
  destination identity/policy and duplicate IDs before any write, under the
  existing registry commitment fence. Writes use authoritative registered queues.
- Each full/closed optional destination reports its own queue failure; other
  destinations still run. No asynchronous capacity waiter or unbounded retry
  buffer is created. Immutable batch/event sharing and queue count/byte limits
  remain authoritative. Results mean queued, never durable/consumer-processed.
- Ordered commitment can compose this fan-out after required responsibility
  transfer, retaining the namespace permit and global preparation charge through
  optional writes. Required rejection skips optional writes; optional failure
  cannot roll back accepted required cursors. Registry mutation between phases
  explicitly rejects the optional plan rather than writing to stale owners.
- Three real consumer witnesses cover full/closed/healthy queues together,
  required queue readback, shared Arc event identity, zero optional waiters,
  stale/duplicate/shutdown plan rejection with no writes, and cancellation plus
  same-frontier retry. Fixtures use deterministic local queues, no sleeps.

Verification (engine/ unless noted; all final commands exit 0):
- Focused `cargo test --locked --offline --no-default-features --features live
  --test live_suite optional_fanout`: initial two witnesses passed; final third
  witness included in complete suite below. An initial test append used the wrong
  relative path and failed before writing; corrected before final verification.
  The accompanying initial zero-match test invocation is not counted as evidence.
- Root `./scripts/verify-live.sh full`: five passing gates, exact commands
  `cargo fmt --all -- --check`; `cargo check --locked --offline
  --no-default-features --features live --lib`; `cargo test --locked --offline
  --no-default-features --features live --lib` (130 passed); same with
  `--test live_suite` (391 passed, 2 opt-in ignored); same with `--doc` (1 passed).
  Final receipts: `engine/target/live-verification/20261021T035844Z-2/`.
- `cargo check --locked --offline --no-default-features --features live,historical`:
  exit 0, receipt `engine/target/live-verification/optional-combined-check.log`.
- Root `git diff --check`: exit 0. Existing dead-code warning remains.

Changed: routing/{router,ordered}.rs, tests/live/router_obligations.rs, STATUS.md
and this ledger. No SQL, external exchange, Python, historical execution,
dependency/lockfile, ignored configuration or user-launcher changes. Opt-in SQL,
performance and historical baseline tests were not rerun. No push.

Limitations: this composes queue policy through the existing ordered coordinator;
production EventRouterPort owner and workers are still pending. Optional-only
and no-subscriber admission remain unsupported by the required coordinator.
Completion correlation, owner-lifetime recovery/payload retention, observations
and deadline shutdown remain incomplete; earlier foundational gates are unchanged.
No package accepted. Next authorized/executable work: WP-10 completion. Further
autonomous implementation is safe. Checkpoint: `2fba098` (optional fan-out, witnesses and evidence).

## Continuation — bounded optional-only ordered attempts (2026-10-21)

Status: Partial. Baseline `1fcd282`, branch `codex/wp06-completion`;
pre-existing untracked `scripts/run-autonomous-codex.sh` preserved. Read campaign
instructions, decisions, status and roadmap; inspected current ordered admission,
registry, cursor frontier and consumer tests. WP-10 remains next executable.

Implemented:
- `OrderedAdmissionBudget::try_optional_only` bounds retained count/bytes and
  takes the authoritative registered namespace permit. No capacity waiters or
  retry buffer are allocated. Required destinations explicitly reject this path.
- Optional registry commitment validates its plan, then checks cancellation and
  deadline under the commitment fence immediately before queue writes.
- Empty registries reject without consuming cursors. Completed optional attempts
  advance order and return every queue failure, including total overload. This
  records an attempted best-effort delivery, never durability or processing.
- Rejections return the immutable original batch; all paths release global
  reservations and uncommitted order permits. Tests cover no subscribers,
  cancellation, required-sink bypass prevention, byte exhaustion, healthy queue
  readback/shared identity, total overload, stale frontier and closed budget.

Verification: commands from engine/ unless stated. Focused
`cargo test --locked --offline --no-default-features --features live --test
live_suite optional_only_ordered`: exit 0, 1 passed at first witness revision;
second witness verified in final complete suite. Initial edit used a wrong
relative test path, then compile attempts exposed an incorrectly placed enum
variant and fixture helper assumptions; corrected without weakening assertions.
A complete gate run also caught formatting after restoring enum order; formatted
and reran final gates. These failed attempts are not pass evidence.

Final root `./scripts/verify-live.sh full` runs `cargo fmt --all -- --check`,
`cargo check --locked --offline --no-default-features --features live --lib`,
`cargo test --locked --offline --no-default-features --features live --lib`,
same test command with `--test live_suite`, and same with `--doc`.
Final results: all five gates exit 0; 130 library, 393 integration (2 opt-in
ignored), 1 doctest. Receipts:
`engine/target/live-verification/20261021T040223Z-3/`.
Combined `cargo check --locked --offline --no-default-features --features
live,historical`: exit 0; receipt
`engine/target/live-verification/optional-only-combined-check.log`.
`git diff --check`: exit 0. Existing dead-code warning remains.

Changed: routing/{ordered,router}.rs, tests/live/router_obligations.rs, STATUS.md
and this ledger. No SQL/exchange access, Python/historical execution changes,
dependency/lockfile changes, ignored configuration edits or push. Opt-in SQL,
performance and historical baseline tests were not rerun.

Remaining: production EventRouterPort owner, worker/completion/recovery lifetime,
observations and deadline shutdown composition. No-subscriber discard policy is
not introduced. Earlier foundational gates remain open. No package accepted.
Next authorized executable work: WP-10 completion; safe to continue autonomously.


## Continuation — authoritative owned destination observations (2026-10-21)

Status: Partial. Baseline `fa272e7`, branch `codex/wp06-completion`.
Read AGENTS, autonomous plan, decisions, STATUS and WP-10 roadmap; inspected real
registry, queues, capacity owner, ordering and consumer witnesses. WP-10 remains
the next executable package. Pre-existing untracked launcher preserved.

Implemented:
- Public owned RouterSnapshot/DestinationSnapshot from the actual registry,
  consistent membership/revision under its read lock, bounded by its configured
  destination limit, deterministic ID ordering, explicit registry shutdown.
- QueueObservation samples queue state and capacity in queue-to-capacity lock
  order, excluding insertion/dequeue during the sample. Occupancy includes
  reserved and consumer-held resources and is explicitly distinct from queue
  depth. Adapter-only registrations report None, never fabricated zero depth.
- No adapter callbacks or second mutable resource owner. Registry observations
  are not a global transaction across independent destination consumers; they
  cannot establish delivery, durability, readiness or historical coverage.
- Two deterministic production-interface witnesses exercise reserved capacity,
  enqueue/dequeue, retained consumer capacity, shutdown/drain, owned snapshots,
  empty/bounded/sorted membership, rejection, removal and revision changes.

Verification (engine/ unless stated):
- Focused `cargo test --locked --offline --no-default-features --features live
  --test live_suite router_snapshot`: exit 0, 2 passed. Initial test append used
  a root-relative path from engine/ and failed before writing; accompanying
  zero-match run is not pass evidence. Corrected and executed both witnesses.
- Root `./scripts/verify-live.sh full`: exit 0. Exact commands:
  `cargo fmt --all -- --check`; `cargo check --locked --offline
  --no-default-features --features live --lib`; `cargo test --locked --offline
  --no-default-features --features live --lib` (130 passed); same test flags with
  `--test live_suite` (395 passed, 2 opt-in ignored); with `--doc` (1 passed).
  Receipts: `engine/target/live-verification/20261021T040447Z-2/`.
- `cargo check --locked --offline --no-default-features --features live,historical`:
  exit 0. Existing dead-code warnings remain.
- Root `git diff --check`: exit 0.

Changed: routing/{mod,queue,router,snapshot}.rs, tests/live/router_obligations.rs,
STATUS.md and this ledger. No SQL, external exchanges, Python/historical execution,
dependencies/lockfile or ignored configuration changes. No push. Opt-in SQL,
performance and historical baseline tests not rerun.

Remaining acceptance ledger: ordered bounded tracked fan-out and optional queue
attempts retained; concrete destination observations now implemented. Production
EventRouterPort owner, bounded worker scheduling, completion/recovery lifetime,
native metrics/control-port projection and deadline shutdown composition remain
unverified/unimplemented. These observations alone do not satisfy the full
control snapshot/metrics criterion. Earlier foundational gates are unchanged.
WP-10 is not ReadyForReview or Accepted. WP-11–29 remain pending. Next authorized
executable work is WP-10 completion; further autonomous implementation is safe.
Checkpoint: local commit containing this continuation, implementation and tests.


## Continuation — owned namespace admission observations (2026-10-21)

Status: Partial. Baseline `4b6d7c7`, branch `codex/wp06-completion`, only
pre-existing untracked `scripts/run-autonomous-codex.sh`, preserved. Inspected
instructions, autonomous plan, decisions, status, roadmap WP-10, actual admission,
namespace ownership, destination observations, reporting and router port consumers.
Next executable package remains WP-10; completed work retained.

Implemented:
- `StreamAdmissionOrder::snapshot` samples authoritative namespace, next cursor,
  optional inclusive pending end cursor and closed state under its owner lock.
  Pending state stores an offset, adding no allocation per admission. Drop
  clears preparation; commitment advances once, including explicit exhaustion.
- `AdmissionNamespaces::snapshot` owns bounded, sorted membership observations
  with registry limit, shutdown and retirement tombstones. Existing clones
  cannot reset retired frontiers. Each entry is consistent, while independent
  frontier completions can occur between samples; no global transaction claimed.
- Two deterministic production-interface witnesses cover real ordered tracked
  preparation, namespace shutdown while prepared, commitment and queue readback,
  pending obligations after admission quiescence, owned snapshot independence,
  rollback, sorted epochs, bounded membership, retirement and u64 exhaustion.
  No sleeps or external services. These are admission observations, not
  durability, delivery, consumer progress or completed runtime shutdown.

Verification (engine/ unless stated):
- `cargo test --locked --offline --no-default-features --features live --test
  live_suite namespace_snapshot`: exit 0, 2 passed / 397 filtered out.
- Root `./scripts/verify-live.sh full`: exit 0, five gates. Exact commands:
  `cargo fmt --all -- --check`; `cargo check --locked --offline
  --no-default-features --features live --lib`; `cargo test --locked --offline
  --no-default-features --features live --lib` (130 passed); same test flags
  with `--test live_suite` (397 passed, 2 opt-in ignored); with `--doc`
  (1 passed). Receipts: `engine/target/live-verification/20261021T040735Z-2/`.
- `cargo check --locked --offline --no-default-features --features live,historical`:
  exit 0. Existing dead-code warnings remain.
- Initial edit attempt used a root-relative path from engine/ and failed before
  writing; corrected before verification. No failed test is counted as passing.

Changed: `engine/src/live/routing/{cursor,namespaces}.rs`,
`engine/tests/live/router_obligations.rs`, STATUS.md and this ledger.
No SQL/exchange requests, historical/Python/dependency/lockfile or ignored
configuration changes; no push. Opt-in SQL/performance and historical baseline
tests were not rerun; no such verification claimed.

Remaining acceptance ledger: production EventRouterPort owner, bounded workers,
completion/recovery and payload lifetime, metrics/control projection and finite
deadline shutdown composition remain. Snapshots expose pending admission but do
not retain outstanding delivery responsibility for a future runtime owner.
Earlier foundational gates unchanged. No package accepted; WP-11–29 pending.
Next authorized executable work: WP-10 completion. Safe to continue autonomously.
Checkpoint: local commit carrying this implementation and evidence.


## Continuation — authoritative destination commitment fence (2026-10-21)

Status: Partial. Implementation checkpoint `3155b01`; baseline `0f914d4`,
branch `codex/wp06-completion`. Initial inventory contained only pre-existing
untracked `scripts/run-autonomous-codex.sh`, preserved. Inspected AGENTS.md,
autonomous plan, decisions, STATUS, roadmap WP-10 and actual plan, admission,
queue, cursor, snapshot and tracker consumers. WP-10 remains next executable.

Requirements and outcomes:
- Every registered required/durable destination must be represented exactly
  once before queue writes or obligation activation. Public plan mutations
  previously allowed omission, duplicate membership and unrelated queues.
  Shared precommit validation now checks complete required membership, descriptor
  equality and actual queue ownership identity under the registry fence.
- A prepared destination retains its descriptor. Obligation validation compares
  policy/kind to both the public plan and retained descriptor, preventing a
  mutually consistent forged plan/obligation pair after preparation.
- Optional fan-out validates complete optional membership before the first write;
  every selected subscriber receives an explicit attempt outcome. Existing
  revision, shutdown, cancellation and capacity behavior remains tested.
- Rejection returns the original batch and rolls back prepared obligations and
  reservations. Deterministic production-interface witnesses cover omitted and
  duplicate required owners, substituted queues, forged policy/kind, mutation
  after preparation, untracked commitment and omitted optional membership.
  Untouched optional plans demonstrate shared Arc identity in real queue reads.

Verification (engine/ unless noted):
- `cargo test --locked --offline --no-default-features --features live --test
  live_suite commitment_owner_fence`: exit 0, 3 passed / 399 filtered out.
  Initial compile failed (exit 101) because the new test used `used_events`;
  corrected to the actual `occupied_events` field, with assertions preserved.
- Root `./scripts/verify-live.sh full`: exit 0, all five gates. Exact commands:
  `cargo fmt --all -- --check`; `cargo check --locked --offline
  --no-default-features --features live --lib`; `cargo test --locked --offline
  --no-default-features --features live --lib` (130 passed); same test flags
  with `--test live_suite` (400 passed, 2 opt-in ignored); with `--doc`
  (1 passed). Final receipts:
  `engine/target/live-verification/20261021T041111Z-2/`.
- `cargo check --locked --offline --no-default-features --features live,historical`:
  exit 0; existing dead-code warnings remain. No historical execution changes.
- Root `git diff --check`: exit 0 before implementation checkpoint.

Changed implementation: `engine/src/live/routing/{queue,router}.rs`,
`engine/tests/live/router_obligations.rs`. Documentation: STATUS.md and this
ledger. No SQL, production data, exchange requests, Python/historical execution,
lockfile/dependency or ignored configuration changes; no push. Opt-in SQL,
performance and historical baseline tests were not rerun; no such claims made.

Remaining acceptance ledger: ordered tracked fan-out, budgets and owned
observations remain implemented; required membership/queue owner validation now
covers concrete commitment entry points. Production EventRouterPort service,
bounded workers, completion/recovery payload lifetime, metrics/control projection
and deadline shutdown composition remain incomplete. No package is marked
Accepted or ReadyForReview by this continuation. Earlier WP-05/06/07/09 gates
remain open; WP-11–29 remain pending. Next authorized executable package is
WP-10 completion. Further autonomous implementation can safely continue.


## Continuation — atomic obligation preparation (2026-10-21)

Status: Partial. Implementation checkpoint `b18da9c`; baseline `7181bf3`,
branch `codex/wp06-completion`. Initial inventory: only pre-existing untracked
`scripts/run-autonomous-codex.sh`, preserved. Inspected instructions, plan,
decisions, status, roadmap WP-10/11 and current router/tracker consumers.
Next executable package remains WP-10; completed work was retained.

Implemented in `engine/src/live/routing/acknowledgement.rs`:
- Prevalidate duplicate IDs across an entire obligation preparation. Previously
  duplicate IDs could panic after the first obligation was registered.
- Preflight complete specification ID allocation, transaction identity and
  collisions with existing obligations before consuming allocator state.
- Register empty transaction membership explicitly, permitting abort/activation
  rather than returning an unusable prepared handle.
- Two deterministic tests exercise duplicate rejection without mutation,
  exhaustion at u64 boundary, manually registered ID collision and empty
  transaction abort/activation through production tracker operations.

Verification from engine/ (all exit 0):
- `cargo test --locked --offline --no-default-features --features live --lib
  transaction_integrity_tests`: 5 passed, 127 filtered out.
- Root `./scripts/verify-live.sh full`: five gates pass. Exact commands:
  `cargo fmt --all -- --check`; `cargo check --locked --offline
  --no-default-features --features live --lib`; `cargo test --locked --offline
  --no-default-features --features live --lib` (132 passed); same test flags
  with `--test live_suite` (400 passed, 2 opt-in ignored); with `--doc`
  (1 passed). Receipts: `engine/target/live-verification/20261021T041259Z-2/`.
- `cargo check --locked --offline --no-default-features --features live,historical`:
  exit 0; existing dead-code warnings remain.
- Root `git diff --check`: exit 0.

Changed files: acknowledgement.rs, STATUS.md and this ledger. No SQL, exchange,
Python/historical execution, dependency/lockfile, ignored configuration changes
or push. Opt-in SQL/performance and historical baseline tests were not rerun.

Remaining acceptance ledger unchanged: production EventRouterPort owner, bounded
worker scheduling, completion/recovery payload lifetime, metrics/control binding
and deadline shutdown composition remain incomplete. Earlier WP-05/06/07/09
gates persist; WP-11–29 pending. No acceptance claimed. Further autonomous
WP-10 implementation can safely continue.


## Continuation — bounded recovery batch allocation (2026-10-21)

Status: Partial. Baseline `e86acf7`, branch `codex/wp06-completion`.
Initial inventory: pre-existing untracked `scripts/run-autonomous-codex.sh` only,
preserved. Instructions, autonomous plan, decisions, current status/evidence,
roadmap WP-10/11, bounded distribution requirements and actual recovery consumers
were inspected. Next executable package remains WP-10.

`TrackingRecovery::reserve_batch` previously allocated a Vec directly from an
unchecked public count, permitting layout-overflow panic before capacity checks.
It now preflights available bounded slots before allocation, uses fallible
allocation and retains per-reservation enforcement/rollback. No acknowledgement,
financial, durability or accepted identity semantics change. One deterministic
consumer witness covers usize::MAX, over-limit/occupied rejection without state
mutation, zero slots, exact capacity and reservation release. Existing fanout
consumers use this same production method through prepare_batch.

Verification (engine/ unless noted):
- `cargo test --locked --offline --no-default-features --features live --test
  live_suite recovery_batch_rejects_unbounded_counts_before_allocation`: exit 0,
  1 passed / 402 filtered out.
- Root `./scripts/verify-live.sh full`: exit 0, all five gates. Commands:
  `cargo fmt --all -- --check`; `cargo check --locked --offline
  --no-default-features --features live --lib`; `cargo test --locked --offline
  --no-default-features --features live --lib` (132 passed); same test flags
  with `--test live_suite` (401 passed, 2 opt-in ignored); with `--doc`
  (1 passed). Receipts: `engine/target/live-verification/20261021T041427Z-2/`.
- `cargo check --locked --offline --no-default-features --features live,historical`:
  exit 0, existing dead-code warnings remain.
- Root `git diff --check`: exit 0.
- An initial append used an engine-relative path from engine/ and failed before
  writing; corrected path immediately. No source/test assertion was weakened.

Changed files: `engine/src/live/routing/tracking_recovery.rs`,
`engine/tests/live/tracking_recovery.rs`, STATUS.md and this ledger.
No SQL/exchange traffic, Python/historical execution, dependencies/lockfile,
ignored configuration changes or push. Opt-in SQL/performance/historical
baseline tests not rerun. Checkpoint: commit containing this continuation.

Remaining acceptance ledger: production EventRouterPort owner, bounded workers,
completion/recovery payload lifetime, metrics/control integration and deadline
shutdown remain incomplete. Earlier WP-05/06/07/09 gates persist; WP-11–29
pending. No acceptance claimed. Further autonomous WP-10 work can safely continue.


## Continuation — uncorrelated outcome reconciliation (2026-10-21)

Status: Partial. Baseline `7583e10`, branch `codex/wp06-completion`.
Initial inventory: only pre-existing untracked `scripts/run-autonomous-codex.sh`,
preserved. Read repository instructions, autonomous plan, decisions, status,
roadmap WP-10/11, existing recovery/fanout and acknowledgement consumers.
Next executable package remains WP-10; no completed work restarted.

Implemented `TrackingRecovery::next_unresolved` for borrowed inspection and
`reconcile_next_unresolved` for authoritative tracker-validated reconciliation.
The oldest outcome retains its original destination, full cursor namespace/range,
status and reservation on rejection. Success removes only that outcome and
releases its recovery capacity; it does not finalize the delivery obligation or
claim processing/durability. No caller can substitute status or cursor range.
Two production-interface tests cover unknown ID, wrong destination, wrong range,
FIFO capacity release, empty queue, idempotent queued progress, regression,
pending-update independence and continued delivery-obligation ownership.

Verification (engine/ unless noted), all exit 0:
- Focused `cargo test --locked --offline --no-default-features --features live
  --test live_suite unresolved_reconciliation`: initial witness 1 passed,
  403 filtered out; both final witnesses execute in the full suite below.
- Root `./scripts/verify-live.sh full`: all five gates pass. Exact commands:
  `cargo fmt --all -- --check`; `cargo check --locked --offline
  --no-default-features --features live --lib`; `cargo test --locked --offline
  --no-default-features --features live --lib` (132 passed); same test flags
  with `--test live_suite` (403 passed, 2 opt-in ignored); with `--doc`
  (1 passed). Final receipts:
  `engine/target/live-verification/20261021T041624Z-3/`.
- `cargo check --locked --offline --no-default-features --features live,historical`:
  exit 0, existing dead-code warnings remain.
- Root `git diff --check`: exit 0.

Changed paths: `engine/src/live/routing/tracking_recovery.rs`,
`engine/tests/live/tracking_recovery.rs`, STATUS.md and this evidence ledger.
Checkpoint: local commit containing this continuation. No SQL/exchange traffic,
Python/historical execution changes, dependencies/lockfile, ignored configuration
changes or push. Opt-in SQL/performance and historical baseline tests not rerun.

Remaining acceptance ledger: production EventRouterPort owner, bounded workers,
completion/recovery payload lifetime, metrics/control integration and deadline
shutdown composition remain incomplete. Earlier WP-05/06/07/09 gates persist;
WP-11–29 pending. No acceptance claimed. Further autonomous WP-10 work is safe.
