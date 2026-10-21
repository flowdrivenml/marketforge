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


## Continuation — complete recovery drain observation (2026-10-21)

Status: Partial. Baseline `8d2d631`, branch `codex/wp06-completion`.
Initial inventory: only pre-existing untracked `scripts/run-autonomous-codex.sh`,
preserved. Reviewed AGENTS, autonomous plan, decisions, current status, roadmap
WP-10/11 and actual recovery/tracker/fanout consumers. Next executable work
remains WP-10; completed implementations were preserved.

Implemented `TrackingRecoverySnapshot` and `TrackingRecovery::snapshot` with
separate correlated pending, unresolved, in-flight reserved and available counts.
`is_drained` requires all three responsibility categories to clear; it does not
claim downstream completion or obligation finalization. Existing `len` and
`is_empty` retain their correlated-queue semantics, now explicitly documented.
The owned snapshot allocates no collections and borrows no mutable state.
The deterministic production-owner witness exercises all categories together,
failed retry without mutation, successful retry/reconciliation, final reservation
drop and continuing authoritative delivery-obligation ownership. Earlier snapshots
remain unchanged after progress.

Verification (engine/ unless stated), all exit 0:
- Root `./scripts/verify-live.sh full`: `cargo fmt --all -- --check`;
  `cargo check --locked --offline --no-default-features --features live --lib`;
  `cargo test --locked --offline --no-default-features --features live --lib`
  (132 passed); same test flags with `--test live_suite` (404 passed,
  2 opt-in ignored); with `--doc` (1 passed). Receipts:
  `engine/target/live-verification/20261021T041749Z-2/`.
- `cargo check --locked --offline --no-default-features --features live,historical`:
  exit 0, existing dead-code warnings remain.
- Root `git diff --check`: exit 0.

Changed files: `engine/src/live/routing/tracking_recovery.rs`,
`engine/tests/live/tracking_recovery.rs`, STATUS.md and this ledger.
Checkpoint: local commit carrying this continuation. No SQL/exchange traffic,
Python/historical execution, dependency/lockfile or ignored configuration changes;
no push. Opt-in SQL/performance and historical baseline tests not rerun.

Remaining acceptance ledger: production EventRouterPort owner, bounded workers,
completion/recovery payload lifetime, metrics/control integration and deadline
shutdown composition remain incomplete. These observations support that work;
they do not constitute a shutdown implementation. Earlier WP-05/06/07/09 gates
persist; WP-11–29 pending. No acceptance claimed. Further autonomous WP-10
implementation can safely continue.


## Continuation — exclusive destination admission worker (2026-10-21)

Status: Partial. Baseline `d4099b4`, branch `codex/wp06-completion`; only
pre-existing untracked `scripts/run-autonomous-codex.sh`, preserved. Inspected
AGENTS, autonomous plan, decisions, status, roadmap WP-10/11, distribution ownership
requirements and actual queue, router, tracker, reporting and destination ports.
Next executable package remains WP-10.

Implemented `DestinationWorker` over the real `EventDestinationPort`. One exclusive
queue lease prevents competing workers/raw dequeues. At most one adapter call is
polled; pending calls retain their exact future, immutable payload and count/byte
charge when callers stop polling. Outcomes block further dequeue until explicitly
transferred with the charged payload. Valid acceptance is only admission, including
for Durable destinations. Wrong destination/through cursor is an explicit retained
invalid response; rejected outcomes retain their original payload. Shutdown closes
admission without destroying queued or in-flight work. Drain observations include
transferred payload charges and reservations, not just queue depth. No background
scheduler, automatic retry, delivery completion or crash durability is claimed.

Four deterministic production-interface witnesses cover FIFO and shared Arc
identity, adapter wakeups, repeated polling without resubmission, failed/invalid
responses, constructor rejection, exclusive ownership/release, slow destination
isolation, shutdown with unfinished work, and capacity retained through handoff.
No sleeps or external services.

Verification (all exit 0):
- engine/: `cargo test --locked --offline --no-default-features --features live
  --test live_suite destination_worker`: 4 passed, 406 filtered out.
- Root `./scripts/verify-live.sh full`: `cargo fmt --all -- --check`;
  `cargo check --locked --offline --no-default-features --features live --lib`;
  `cargo test --locked --offline --no-default-features --features live --lib`
  (132 passed); same test flags with `--test live_suite` (408 passed, 2 opt-in
  ignored); with `--doc` (1 passed). Receipts:
  `engine/target/live-verification/20261021T042138Z-2/`.
- engine/: `cargo check --locked --offline --no-default-features --features
  live,historical`: exit 0, existing dead-code warnings remain.

Changed: routing/{mod,queue,worker}.rs, tests/live/{mod,destination_worker}.rs,
STATUS.md and this ledger. Checkpoint: local commit carrying this continuation.
No SQL/exchange traffic, Python/historical execution, dependencies/lockfile,
ignored configuration changes or push. Opt-in SQL/performance/historical baseline
not rerun. Earlier foundational gates unchanged. Remaining: production router
owner, registry-bound worker construction/scheduling, completion/recovery payload
integration, metrics/control binding and deadline shutdown composition. WP-10 is
not ReadyForReview or Accepted; WP-11–29 pending. Safe to continue autonomously.


## Continuation — registry-bound destination workers (2026-10-21)

Status: Partial. Baseline/worker checkpoint `bd48878`; branch
`codex/wp06-completion`. Existing untracked launcher preserved. WP-10 remains
next executable; no later package prerequisites have been bypassed.

`RouterRegistry::claim_worker` now binds the actual registered adapter, queue and
frozen descriptor under its membership read fence. Adapter code is not invoked
while holding that fence. Missing destination/queue and duplicate consumer claims
are explicit errors; closed registries permit retained queue draining. Retired
workers retain their original queue, and cannot close or dequeue a replacement's
queue. Struct destruction drops the adapter future before releasing payload
capacity, and releases exclusive dequeue authority last.

Four additional deterministic witnesses (eight total worker tests) exercise:
- Real bounded ordered admission, obligation preparation/commitment, registered
  worker admission, immutable payload readback and shutdown. Adapter acceptance
  does not advance the tracker beyond Queued or finalize its Durable obligation.
- Old/new same-ID registration isolation, wrong/missing membership and closed
  registry draining.
- Barrier-coordinated concurrent claims: exactly one consumer, with capacity and
  membership authority retained until release.
- Adapter future destruction observes both the original payload charge and
  exclusive consumer lease still held; queued successor survives owner drop.

Verification (all exit 0):
- engine/: `cargo test --locked --offline --no-default-features --features live
  --test live_suite destination_worker`: 6 passed initially, 7 passed after
  concurrent claim coverage; all 8 execute in the final full suite.
- Root `./scripts/verify-live.sh full`: `cargo fmt --all -- --check`;
  `cargo check --locked --offline --no-default-features --features live --lib`;
  `cargo test --locked --offline --no-default-features --features live --lib`
  (132 passed); same test flags with `--test live_suite` (412 passed, 2 opt-in
  ignored); with `--doc` (1 passed). Final receipts:
  `engine/target/live-verification/20261021T042417Z-2/`.
- engine/: `cargo check --locked --offline --no-default-features --features
  live,historical`: exit 0, existing dead-code warnings remain.
- Root `git diff --check`: exit 0 before checkpoint.

Changed: `engine/src/live/routing/{router,worker}.rs`,
`engine/tests/live/destination_worker.rs`, STATUS.md and this ledger.
Checkpoint: local commit carrying this continuation. No SQL/exchange traffic,
Python/historical execution, dependencies/lockfile or ignored configuration
changes; no push. Opt-in SQL/performance and historical baseline not rerun.

Remaining acceptance ledger: production EventRouterPort owner, runtime scheduling,
completion/recovery outcome correlation and payload ownership, native metrics/
control binding and finite deadline shutdown composition remain. Worker close/
snapshot is not complete runtime shutdown; dropping a pending owner is abandonment,
not a successful delivery or drain. Earlier WP-05/06/07/09 gates remain unchanged;
WP-11–29 pending. No package accepted. Further autonomous WP-10 work is safe.


## Continuation — wake-driven destination queue consumption (2026-10-21)

Status: Partial. Baseline `c9e817f`, branch `codex/wp06-completion`.
Initial inventory: only pre-existing untracked `scripts/run-autonomous-codex.sh`,
preserved. Read instructions, autonomous plan, decisions, status, WP-10/11 roadmap
and actual queue/worker/port/tracker consumers. Next executable remains WP-10.

Implemented `DestinationWorker::poll_next` for awaiting queue activity and adapter
progress without an idle busy loop. Queue inspection and single-waker registration
share the insertion lock, preventing lost notifications. Both reserved and direct
insertion wake after releasing the queue lock; closure wakes an idle worker and
preserves queued drain. Lease destruction clears its retained waiter before a new
owner claims the queue. Existing nonwaiting poll_admission behavior is preserved.
Two deterministic production-interface witnesses cover waker replacement, both
insertion paths, closed empty queues, lease replacement, continued adapter calls
and payload charges retained through outcome handoff. No background task or full
runtime scheduling/shutdown implementation is claimed.

Verification (all final commands exit 0):
- Root `./scripts/verify-live.sh full`: `cargo fmt --all -- --check`;
  `cargo check --locked --offline --no-default-features --features live --lib`;
  `cargo test --locked --offline --no-default-features --features live --lib`
  (132 passed); same flags with `--test live_suite` (414 passed, 2 opt-in ignored);
  with `--doc` (1 passed). Receipts:
  `engine/target/live-verification/20261021T042629Z-7/`.
- engine/: `cargo check --locked --offline --no-default-features --features
  live,historical`: exit 0; existing warnings remain.
- Root `git diff --check`: exit 0.
- Initial edit command used an incorrect working directory and failed before
  writing; corrected immediately. No test/assertion weakening.

Changed: routing/{queue,worker}.rs, tests/live/destination_worker.rs, STATUS.md
and this ledger. Checkpoint: local commit containing this continuation.
No SQL/exchange traffic, historical/Python execution changes, dependency/lockfile
or ignored configuration changes; no push. SQL/performance/historical baseline
execution not rerun; no new claims for those gates.

Remaining: production EventRouterPort owner, runtime scheduling composition,
completion/recovery correlation and payload ownership, native metrics/control
binding and finite deadline shutdown. Earlier WP-05/06/07/09 gates unchanged;
WP-11–29 pending. No acceptance claimed. Further autonomous WP-10 work is safe.


## Continuation — bounded fair destination worker scheduling (2026-10-21)

Status: Partial. Baseline `8a16109`, branch `codex/wp06-completion`.
Initial inventory: only pre-existing untracked `scripts/run-autonomous-codex.sh`,
preserved. Inspected campaign instructions/plan, decisions, status, roadmap WP-10,
Document 08 ownership/shutdown boundaries, worker/queue/snapshot and frozen ports.
WP-10 remains the next executable package; completed work was retained.

Implemented `DestinationWorkers`, a bounded runtime scheduling owner of actual
exclusive destination workers. Each poll visits at most the configured worker
limit and transfers at most one charged outcome. Round-robin progression avoids
starvation; pending adapters retain calls and payloads while other workers run.
The existing queue and adapter wakers schedule progress; no background tasks or
hidden outcome queues were added. Duplicate/full/closed insertion returns the
entire rejected worker. Close fences every owned queue. Retirement requires a
closed worker with no queued/in-flight/outcome/external payload charges. Removal
preserves the next polling position. Owned bounded snapshots expose worker states.

Two deterministic production-interface witnesses exercise two real queues and
adapters, slow-adapter isolation, FIFO successor handling, fairness, duplicate and
capacity rejection, idle enqueue wakeup, shutdown admission fencing, invalid
response preservation, pending-call supervisor handoff without resubmission,
external outcome charges preventing false drain, closed insertion and empty-set
polling. Adapter acceptance still does not update delivery/durability evidence.

Verification (all exit 0):
- engine/: `cargo test --locked --offline --no-default-features --features live
  --test live_suite destination_worker_scheduler`: 2 passed, 416 filtered out.
- Root `./scripts/verify-live.sh full`: `cargo fmt --all -- --check`;
  `cargo check --locked --offline --no-default-features --features live --lib`;
  `cargo test --locked --offline --no-default-features --features live --lib`
  (132 passed); same flags with `--test live_suite` (416 passed, 2 opt-in ignored);
  with `--doc` (1 passed). Receipts:
  `engine/target/live-verification/20261021T042852Z-2/`.
- engine/: `cargo check --locked --offline --no-default-features --features
  live,historical`: exit 0; log `engine/target/wp10-scheduler-combined.log`.
  Existing warnings retained.
- Root `git diff --check`: exit 0.

Changed: `engine/src/live/routing/worker.rs`,
`engine/tests/live/destination_worker.rs`, STATUS.md and this ledger.
Checkpoint: local commit carrying this entry. No SQL/exchange traffic,
Python/historical execution changes, dependencies/lockfile or ignored configuration
changes; no push. Opt-in SQL/performance and historical baseline not rerun.

Remaining acceptance ledger: production EventRouterPort owner, completion/recovery
correlation and payload retention, runtime task supervision, native metrics/control
binding and finite deadline shutdown. Scheduler close is an admission fence, not
a complete shutdown; polling for outcomes remains Pending when no outcome exists,
and the supervisor must inspect drain observations independently. Independent
tracker/recovery obligations are outside the worker snapshot's drain claim.
A supervisor must retain owners and outcomes at deadlines rather than drop them.
Earlier WP-05/06/07/09 gates remain unchanged; WP-11–29 pending. No acceptance
claimed. Further autonomous WP-10 work is safe.


## Continuation — deadline-driven destination worker shutdown (2026-10-21)

Status: Partial. Baseline `18dfe31`, branch `codex/wp06-completion`.
Initial working tree: only pre-existing untracked `scripts/run-autonomous-codex.sh`,
preserved. Read repository/campaign instructions, decisions, current status,
WP-10/11 roadmap and real worker/queue/deadline consumers. Next executable WP-10.

Implemented `DestinationWorkers::poll_shutdown` through the existing bounded
DeadlineTimer future contract. First poll fences every queue; each poll visits a
bounded worker set and returns at most one charged outcome. Pending operations
remain owned across polling cancellation or deadline expiry. An expired deadline
prevents starting or consuming adapter calls; a fresh deadline permits later
resumption without resubmission. Fully drained closed worker resources report
Drained, including an empty scheduler, even if the deadline has expired.
External payload charges prevent false drain. Callers must retain the deadline
future across polls and poll after processing/releasing an outcome; external
charge release alone need not wake the scheduler, while deadline expiry does.
Independent acknowledgement/recovery obligations still require supervisor checks.
No full router shutdown or delivery/durability guarantee is claimed.

Two deterministic production-interface tests cover timer wakeup, expired deadline
before dequeue, pending adapter retention, queued successor preservation, admission
fencing, expired deadline with a now-ready response, resumed drain without repeated
adapter admission, externally retained outcomes and empty closed drain.

Verification, final commands all exit 0:
- engine/: `cargo test --locked --offline --no-default-features --features live
  --test live_suite destination_worker_shutdown`: 2 passed, 418 filtered out.
- Root `./scripts/verify-live.sh full`: `cargo fmt --all -- --check`;
  `cargo check --locked --offline --no-default-features --features live --lib`;
  `cargo test --locked --offline --no-default-features --features live --lib`
  (132 passed); same flags with `--test live_suite` (418 passed, 2 opt-in ignored);
  with `--doc` (1 passed). Receipts:
  `engine/target/live-verification/20261021T043100Z-2/`.
- engine/: `cargo check --locked --offline --no-default-features --features
  live,historical`: exit 0; `engine/target/wp10-shutdown-combined.log`.
- Root `git diff --check`: exit 0.
- Initial test append used the wrong working directory and failed before writing;
  corrected immediately. Existing unused recovery-reservation warning remains.

Changed: routing/worker.rs, tests/live/destination_worker.rs, STATUS.md and this
ledger. Checkpoint: local commit carrying this entry. No SQL/exchange access,
Python/historical execution changes, dependencies/lockfile or ignored config
changes; no push. Opt-in SQL/performance and historical baseline execution were
not rerun; no new claims for those gates.

Remaining: production EventRouterPort owner, completion/recovery correlation and
payload retention, runtime supervision, native metrics/control integration and
whole-router deadline shutdown composition. Earlier WP-05/06/07/09 gates remain;
WP-11–29 pending. No acceptance claimed. Safe autonomous work can continue on WP-10.


## Continuation — charged payload completion and journal handoff (2026-10-21)

Status: Partial. Baseline `7107948`, branch `codex/wp06-completion`; only initial
untracked `scripts/run-autonomous-codex.sh` preserved. Inspected instructions,
autonomous plan, decisions, current status/evidence, WP-10/11 roadmap and actual
ordered/worker/tracker/journal consumers. Relevant Document 08 sections 2 and 17
require independent completion semantics and distinct queue/delivery guarantees.
Next executable package remains WP-10; completed components were retained.

Implemented `routing::completion::RetainedDelivery`, owning the original charged
worker payload and frozen obligation metadata. Binding checks the authoritative
tracker registration's destination/kind/policy and exact batch cursor range, then
revalidates the admission response even if a caller forges the public Valid label.
Failure returns the entire WorkerOutcome. Immutable tracker metadata access avoids
a second registration owner. Accepted admission invents no delivery stage; actual
rejection can be applied as failure while retaining recovery payload. Separately
observed completions must match destination and full through cursor and pass the
existing authoritative progress rules. Every operation verifies the registration
still matches. Terminal handoff uses the existing atomic tracker-to-journal path;
incomplete guarantees/full journals return the entire owner. Successful handoff
returns the charged payload for explicit release or recovery transfer. There is
no hidden collection, retry, task, replacement cursor or changed durability policy.

Consumer evidence: extended the real registered ordered fan-out/worker witness
through queued admission, insufficient queued/committed durability, durable
completion and journal transfer. Three new deterministic tests cover unknown and
wrong-descriptor binding, wrong destination/partial cursor completion, progress
regression, invalid admission responses relabeled Valid, rejection handling,
completion refusal after rejection, full journal preservation and supervisor
handoff without adapter resubmission. Worker drain stays false until the returned
terminal payload is released, while tracker/journal reporting ownership remains
separate. Actual queue, adapter, tracker and journal implementations are exercised.

Verification (all final commands exit 0):
- engine/: `cargo test --locked --offline --no-default-features --features live
  --test live_suite retained_delivery`: 3 passed, 420 filtered out.
- Root `./scripts/verify-live.sh full`: `cargo fmt --all -- --check`;
  `cargo check --locked --offline --no-default-features --features live --lib`;
  same flags with `cargo test ... --lib` (132 passed), `--test live_suite`
  (421 passed, 2 opt-in ignored), `--doc` (1 passed). Final receipts:
  `engine/target/live-verification/20261021T043439Z-2/`.
- engine/: `cargo check --locked --offline --no-default-features --features
  live,historical`: exit 0; log `engine/target/wp10-completion-combined.log`.
- Root `git diff --check`: exit 0.
Existing unused recovery-reservation warning retained. Opt-in SQL/performance and
historical baseline execution not rerun; no new claims for those gates.

Changed: `engine/src/live/routing/completion.rs`, `acknowledgement.rs`, `mod.rs`,
`engine/tests/live/destination_worker.rs`, STATUS.md and this ledger. Checkpoint:
local commit carrying this entry. No SQL/exchange traffic, Python/historical
execution changes, dependencies/lockfile or ignored configuration changes; no push.

Remaining acceptance ledger: production EventRouterPort owner, automatic bounded
outcome/obligation lookup and completion ingestion, recovery scheduling, runtime
supervision, native metrics/control integration and whole-router deadline shutdown.
This explicit single-payload owner does not supply a runtime supervisor or complete
WP-10 acceptance. The caller still supplies an existing obligation ID and authentic
destination observations. Earlier WP-05/06/07/09 gates persist; WP-11–29 pending.
Further autonomous WP-10 implementation can safely continue. No acceptance claimed.


## Continuation — bounded automatic delivery correlation (2026-10-21)

Status: Partial. Baseline `88a87e2`, branch `codex/wp06-completion`. Initial
inventory: only pre-existing untracked `scripts/run-autonomous-codex.sh`, preserved.
Read AGENTS, autonomous plan, status/evidence, decisions, roadmap WP-10/11 and
Document 08 distribution requirements; inspected tracker, worker and real ordered
fan-out consumers. Next executable package remains WP-10.

Implemented `PendingDeliveries`, a bounded owner of actual `RetainedDelivery`
payloads. It correlates exact destination descriptor and full cursor range against
the authoritative bounded tracker without caller-supplied obligation IDs or a
second registration index. Unknown/ambiguous/duplicate matches and capacity or
allocation failure return the original charged outcome. Binding still validates
actual adapter admission. Ingestion invents no progress. Independently observed
completions resolve by destination/full through cursor, reject ambiguity and use
the original progress rules. Failed terminal journal handoff restores the charged
owner without reallocation; success transfers the payload to the caller. Explicit
recovery transfer preserves the independent authoritative obligation. Scans are
bounded by configured tracker/collection capacities; queue byte/event charges
remain held throughout. Dropping an owner is abandonment, not successful drain.

Three deterministic production-interface tests exercise automatic successful
correlation, full collection and preserved payloads, unknown/ambiguous/duplicate
registrations, wrong completion destination, progress regression, insufficient
durability, rejected admission, full journal retry and explicit recovery transfer.
The existing real registered ordered fan-out/worker witness now uses this owner
through admission, committed versus durable completion and terminal journal
handoff. Queue drain remains false until terminal payload release. No sleeps,
external services or test-only alternative state machines.

Verification (final commands all exit 0):
- engine/: `cargo test --locked --offline --no-default-features --features live
  --test live_suite pending_deliveries`: 3 passed, 423 filtered out.
- Root `./scripts/verify-live.sh full`: `cargo fmt --all -- --check`;
  `cargo check --locked --offline --no-default-features --features live --lib`;
  same flags with `cargo test ... --lib` (132 passed), `--test live_suite`
  (424 passed, 2 opt-in ignored), `--doc` (1 passed). Final receipts:
  `engine/target/live-verification/20261021T043756Z-2/`.
- engine/: `cargo check --locked --offline --no-default-features --features
  live,historical`: exit 0; log `engine/target/wp10-pending-combined.log`.
- Root `git diff --check`: exit 0 before checkpoint.
- Initial edit script assertion failed before writing; corrected the insertion
  anchor. No compiler/test failures or weakened assertions.
Existing unused recovery-reservation warning remains. Opt-in SQL/performance and
historical baseline execution not rerun; no new claims for these gates.

Changed: `engine/src/live/routing/{acknowledgement,completion}.rs`,
`engine/tests/live/destination_worker.rs`, STATUS.md and this ledger. Checkpoint:
local commit carrying this continuation. No SQL/exchange traffic, Python or
historical execution changes, dependencies/lockfile or ignored configuration
changes; no push.

Remaining: production EventRouterPort owner, recovery scheduling, runtime
supervision, native metrics/control integration and whole-router shutdown. The
collection does not spawn a runtime or source completion evidence; authentic
observations still arrive through destination adapters/supervision. Earlier
WP-05/06/07/09 gates persist, WP-11–29 pending. No package acceptance claimed.
Further autonomous WP-10 work can safely continue.

## Continuation — bounded terminal discovery (2026-10-21)

Status: Partial. Baseline `37d94c6`, branch `codex/wp06-completion`; only
pre-existing untracked `scripts/run-autonomous-codex.sh`, preserved. Read the
campaign instructions, decisions, current status/evidence and WP-10/11 roadmap;
inspected actual tracker finalization and worker consumers. WP-10 remains next.

Implemented `PendingDeliveries::finalize_next_into`: bounded scan through actual
retained deliveries, skips incomplete obligations, transfers at most one terminal
payload per invocation. Existing tracker policy remains authoritative. No extra
index, payload allocation, invented completion, task or retry. Tracking/journal
errors preserve all payloads; full journal can be retried. `None` means no terminal
payload, not successful drain. Successful transfer preserves remaining order.

New deterministic production-interface witness covers two charged workers,
incomplete-before-terminal selection, no-terminal reporting, terminal transfer,
independent tracker ownership and capacity release only after payload drop.
Existing rejection/full-journal recovery witness now exercises automatic handoff.

Verification:
- engine/: `cargo test --locked --offline --no-default-features --features live
  --test live_suite pending_deliveries`: exit 0, 4 passed, 423 filtered out.
- Root `./scripts/verify-live.sh full`: exit 0. Exact gates: `cargo fmt --all --
  --check`; `cargo check --locked --offline --no-default-features --features live
  --lib`; equivalent test flags with `--lib` (132 passed), `--test live_suite`
  (425 passed, 2 opt-in ignored), `--doc` (1 passed). Receipts:
  `engine/target/live-verification/20261021T043959Z-2/`.
- engine/: `cargo check --locked --offline --no-default-features --features
  live,historical`: exit 0; existing dead-code warnings.
- Root `git diff --check`: exit 0.
- Initial test append used a root-relative path from engine/ and failed before
  writing; corrected path and reran with the new test executing. No test failures.

Changed: `engine/src/live/routing/completion.rs`,
`engine/tests/live/destination_worker.rs`, STATUS.md and this ledger. Checkpoint:
local commit carrying this entry. No database/exchange access, Python/historical
code edits, dependencies, ignored configuration edits or push. SQL/performance
opt-in gates and historical baseline tests not rerun; no claims for those gates.

Remaining: production EventRouterPort owner, recovery scheduling, supervision,
native metrics/control binding and whole-router shutdown. Earlier WP-05/06/07/09
completion gates persist; WP-11–29 pending. No package acceptance claimed.
Further autonomous implementation can safely continue in WP-10.

## Continuation — bounded delivery supervision (2026-10-21)

Status: Partial. Baseline `0d24ffa`, branch `codex/wp06-completion`; initial
inventory contained only pre-existing untracked `scripts/run-autonomous-codex.sh`,
preserved. Read repository/campaign instructions, decisions, status and latest
WP-10 evidence, roadmap requirements and Document 08 distribution requirements;
inspected actual admission, worker, tracker and retained-completion consumers.
WP-10 remains the next executable package; completed work was preserved.

Implemented `DeliverySupervisor`, composing the actual exclusive worker scheduler
with bounded retained-delivery tracking. Each poll transfers at most one worker
outcome or terminal payload. Tracker and journal supplied by the admission owner
remain authoritative. Rejections apply their actual failure; acceptance creates
no completion progress. Completion uses the existing exact destination/cursor
correlation. Invalid responses retain one charged original outcome for explicit
recovery transfer; capacity/correlation/journal faults are explicit backpressure,
not hidden retries. The supervisor owns all pending adapter futures through
shutdown expiry. Expiry fences new calls; resuming uses the same owner and a new
deadline registration. Resource drain includes externally transferred charges but
does not claim tracker/recovery/journal drain. No spawned task or runtime claimed.

Three deterministic production-interface tests cover real worker pending/resume,
no resubmission, automatic correlation, independent durable completion, retained
capacity through terminal handoff, bounded pending pressure, actual rejection,
invalid adapter response recovery, closed queue admission and deadline expiry
before first adapter call followed by resumed drain. Existing full suite exercises
journal-full retained handoff and worker fairness; no sleeps or external services.

Verification from engine/ unless indicated:
- `cargo test --locked --offline --no-default-features --features live --test
  live_suite delivery_supervisor`: final exit 0, 3 passed / 427 filtered out.
  Initial run after adding shutdown test: exit 101, 2 passed / 1 failed because
  the test polled std::future::Ready after completion. Corrected the fixture to
  supply a fresh deadline when resuming, and documented that requirement.
- Root `./scripts/verify-live.sh full`: exit 0. Gates: `cargo fmt --all --
  --check`; `cargo check --locked --offline --no-default-features --features live
  --lib`; equivalent test flags with `--lib` (132 passed), `--test live_suite`
  (428 passed, 2 opt-in ignored), `--doc` (1 passed). Final receipts:
  `engine/target/live-verification/20261021T044309Z-2/`.
- `cargo check --locked --offline --no-default-features --features
  live,historical`: exit 0; `engine/target/wp10-supervisor-combined.log`.
- Root `git diff --check`: exit 0 before checkpoint.
Existing dead-code warnings persist. SQL/performance opt-in and historical
baseline execution not rerun; no new claims for these gates.

Changed: `engine/src/live/routing/{mod,supervisor}.rs`,
`engine/tests/live/destination_worker.rs`, STATUS.md and this ledger. Checkpoint:
local commit carrying this entry. No SQL/exchange requests, Python/historical
execution edits, dependency/lockfile or ignored configuration edits; no push.

Remaining acceptance ledger: production EventRouterPort owner and ordered
admission composition, recovery scheduling/lifecycle, native metrics/control
binding and complete router shutdown remain unverified/unimplemented. Invalid
outcome retention intentionally stops additional worker ingestion until recovery
transfer; the enclosing runtime must handle the explicit fault rather than spin.
Blocked steps require repolling after external progress. This component does not
certify router-wide fairness under exhausted supervision capacity, restart
durability or full service operation. Earlier WP-05/06/07/09 gates persist;
WP-11–29 pending. Next authorized executable work: WP-10. Further autonomous
implementation can safely continue; no package acceptance claimed.


## Continuation — bounded prepared ownership and supervision observations (2026-10-21)

Status: Partial. Baseline `3a4a9b6`, branch `codex/wp06-completion`; only initial
untracked file was `scripts/run-autonomous-codex.sh`, preserved. Read repository
and authorized campaign instructions, decisions, status, roadmap WP-10/11,
latest package evidence and relevant Document 08 delivery/resource/shutdown
requirements. Inspected actual admission, tracker, supervisor, worker and recovery
owners. WP-10 remains next; completed implementation was preserved.

Implemented:
- Prepared transactions share the configured tracker count ceiling independently
  of obligation occupancy. Empty transactions retain real commitment tokens and
  previously bypassed that limit entirely. Both preparation entry points now
  reject at the ceiling before allocation/ID mutation. Abort and commitment free
  transaction capacity; direct registration behavior remains unchanged.
- `DeliverySupervisionSnapshot` reads actual worker resources, retained capacity,
  blocked outcome error, authoritative obligation/prepared counts, recovery
  reservation/queue counts, journal occupancy/capacity and escalated count.
  It owns bounded observations and creates no new owner or canonical state.
- Its delivery drain predicate requires worker closure/resource release and no
  retained, blocked, tracked, prepared, recovery or reporting responsibility.
  Resource-only `resources_drained` retains its documented semantics. A full
  journal or transferred payload cannot become a false successful delivery drain.

Deterministic witnesses use production tracker/worker/supervisor interfaces:
empty and nonempty spec preparations reject without consuming IDs at the limit;
abort/commit restore capacity; open versus closed empty workers; empty prepared
transaction; live recovery reservation; immutable prior observations; actual
worker completion and terminal handoff retaining payload charges, followed by
payload release while independent journal reporting remains outstanding.

Verification (engine/ unless stated):
- `cargo test --locked --offline --no-default-features --features live --lib
  empty_preparations`: final exit 0, 1 passed / 132 filtered out.
- `cargo test --locked --offline --no-default-features --features live --test
  live_suite delivery_supervisor`: final exit 0, 4 passed / 427 filtered out.
- Root `./scripts/verify-live.sh full`: exit 0. Exact gates: `cargo fmt --all --
  --check`; `cargo check --locked --offline --no-default-features --features live
  --lib`; equivalent test flags with `--lib` (133 passed), `--test live_suite`
  (429 passed, 2 opt-in ignored), `--doc` (1 passed). Receipts:
  `engine/target/live-verification/20261021T044555Z-2/`.
- `cargo check --locked --offline --no-default-features --features
  live,historical`: exit 0; log `engine/target/wp10-supervision-observation-combined.log`.
- Root `git diff --check`: exit 0 before checkpoint.
- Initial focused command exited 101: a patch matched the wrong repeated method
  context, placing an accessor on PreparedObligations; corrected its owner.
  Subsequent focused run exited 101 with 1 regression failing because a capacity
  guard matched register_batch instead of prepare_obligations. Moved the guard,
  preserving direct registration semantics, and reran focused/full gates above.
  Assertions were not weakened. Existing dead-code warnings remain.

Changed: `engine/src/live/routing/{acknowledgement,supervisor}.rs`,
`engine/tests/live/destination_worker.rs`, STATUS.md and this ledger. Checkpoint:
local commit carrying this entry. No SQL/exchange traffic, Python/historical
execution changes, dependency/lockfile or ignored configuration edits; no push.
SQL/performance opt-in tests and historical baseline execution not rerun; no
claims for those gates.

Acceptance ledger remains Partial: production EventRouterPort owner/ordered
composition, recovery scheduling/lifecycle, native metrics/control binding and
complete router shutdown remain. The snapshot is a delivery-owner observation,
not a control transport or metrics collector; admission namespaces and outer
runtime lifecycle are outside its drain predicate. Caller must supply the same
authoritative tracker/recovery/journal under the enclosing owner fence. Earlier
WP-05/06/07/09 gates persist; WP-11–29 pending. Next authorized executable work:
WP-10. Further autonomous implementation can safely continue. No acceptance
or entire-project completion claimed.


## Continuation — independent bounded tracking recovery pass (2026-10-21)

Status: Partial. Baseline `4647155`, branch `codex/wp06-completion`; initial
working tree contained only untracked `scripts/run-autonomous-codex.sh`, preserved.
Reconciled campaign status, roadmap WP-10 prerequisites/acceptance, latest
checkpoint and actual router, ordered admission, supervisor and recovery owners.
WP-10 remains next executable; no completed component restarted.

Implemented `TrackingRecovery::retry_pass` and owned `TrackingRecoveryPass`
observations. Each initially pending update is attempted at most once. Failed
entries keep their original order and reservations. An earlier failed update
fences later updates for that obligation; independent obligations can progress.
Success releases only its recovery reservation, not delivery/finalization
responsibility. Uncorrelated outcomes and in-flight reservations remain untouched.
The pass performs no IO, sleep, retry loop or allocation. Work is bounded by
configured capacity, with a prefix scan per retained entry (quadratic worst case).
Existing single-entry FIFO retry API remains available without changed semantics.

Deterministic production witnesses: empty pass; unknown obligation followed by a
valid independent obligation and blocked successor; registration then successful
retry of retained updates; in-flight reservation still preventing recovery drain;
invalid destination followed by an otherwise valid update for the same obligation;
repeated failed passes preserve charges and do not bypass that predecessor.

Verification (engine/ except root script/diff):
- `cargo test --locked --offline --no-default-features --features live --test
  live_suite recovery_pass`: exit 0, 1 passed before the second witness was added;
  `engine/target/wp10-recovery-pass-focused.log`.
- Root `./scripts/verify-live.sh full`: final exit 0, formatting, Live compilation,
  133 library tests, 431 integration tests passed / 2 opt-in ignored, 1 doctest.
  Exact commands and complete receipts:
  `engine/target/live-verification/20261021T044817Z-7/`.
  Earlier full run also passed (430 integration), receipts
  `engine/target/live-verification/20261021T044752Z-3/`.
- `cargo check --locked --offline --no-default-features --features
  live,historical`: exit 0; `engine/target/wp10-recovery-pass-combined.log`.
- Root `git diff --check`: exit 0 before checkpoint. Existing dead-code warnings
  remain. SQL/performance opt-in tests and historical test execution not rerun.

Changed: `engine/src/live/routing/tracking_recovery.rs`,
`engine/tests/live/tracking_recovery.rs`, STATUS.md and this evidence ledger.
Local checkpoint: commit carrying this entry; no push. No database access,
exchange traffic, Python/historical code edits, dependencies/lockfile changes,
or ignored configuration edits.

Remaining acceptance ledger: production EventRouterPort owner and ordered
composition, automatic recovery scheduling/lifecycle, native metrics/control
binding and complete router shutdown remain. The pass is a production recovery
primitive exercised through the real tracker, not an automatically scheduled
runtime or crash-durable recovery mechanism. Callers must schedule it without
busy polling. Earlier WP-05/06/07/09 gates persist; WP-11–29 pending. Next
authorized executable package remains WP-10; safe autonomous work can continue.
No package acceptance or overall completion claimed.


## Continuation — owned delivery driver and complete delivery drain (2026-10-21)

Status: Partial. Starting checkpoint `51f21c8`, branch `codex/wp06-completion`.
Initial working tree contained only untracked `scripts/run-autonomous-codex.sh`,
preserved. Read campaign policy, decisions, status, WP-10 roadmap requirements
and current ordered admission, worker, supervisor, completion and recovery code.
WP-10 remains next executable; completed work was preserved.

Implemented `DeliveryDriver`, an exclusive mutable composition owner for the
existing supervisor, acknowledgement tracker, tracking recovery and terminal
journal. Admission borrows the same tracker/recovery pair; reporting borrows the
same journal. Each drive turn performs one capacity-bounded recovery pass and at
most one worker/finalization step. Failed recovery observations remain explicit
while independent delivery work progresses. Correlated completion and charged
recovery transfers use the existing supervisor. Shutdown fences worker queues,
checks all delivery responsibilities, and observes deadline expiry before applying
recovery or calling adapters. Deadline expiry retains the owner for resumption.
Worker drain alone cannot hide preparation tokens, recovery reservations, retained
or transferred payload charges, or unreported terminal outcomes.

Three deterministic production witnesses cover recovery enabling terminal handoff
in the same turn; deadline expiry preserving an otherwise recoverable update;
reporting success while a transferred payload is still charged; later full drain;
unknown-obligation recovery retaining its capacity without blocking an independent
worker rejection; and empty prepared transactions plus external recovery
reservations preventing false successful shutdown. Reporting uses ManualClock;
no arbitrary sleeps or external services.

Verification:
- From engine/: `cargo test --locked --offline --no-default-features --features
  live --test live_suite delivery_driver`: final exit 0, 2 passed before adding
  the third witness. Initial run exited 101 (0 passed, 2 failed): test reused a
  completed `std::future::Ready` deadline and confused in-flight reservation count
  with charged pending-update count. Replaced deadline before repolling and asserted
  pending count plus exhausted available capacity. Assertions retain the intended
  ownership guarantees. Third witness included in the full suite below.
- Root `./scripts/verify-live.sh full`: exit 0. Exact gates: `cargo fmt --all --
  --check`; `cargo check --locked --offline --no-default-features --features live
  --lib`; same feature flags with `cargo test --lib` (133 passed), `--test
  live_suite` (434 passed, 2 opt-in ignored), `--doc` (1 passed). Full receipts:
  `engine/target/live-verification/20261021T045121Z-3/`.
- From engine/: `cargo check --locked --offline --no-default-features --features
  live,historical`: exit 0; `engine/target/wp10-delivery-driver-combined.log`.
- Root `git diff --check`: exit 0 before checkpoint. Existing dead-code warning
  for the owned unresolved-recovery reservation remains. SQL/performance opt-in
  tests and historical baseline test execution not rerun; no claim for those gates.

Changed: `engine/src/live/routing/{driver,mod}.rs`,
`engine/tests/live/destination_worker.rs`, STATUS.md and this package ledger.
Checkpoint: local commit carrying this entry. No push or database/exchange traffic;
no Python/historical execution, dependency/lockfile or ignored configuration edits.

Remaining acceptance ledger: production EventRouterPort owner and ordered
admission integration, automatic runtime scheduling/wakeup/retry policy, native
metrics/control binding, and whole-router shutdown including admission namespaces.
DeliveryDriver integrates delivery components but does not spawn an executor,
certify durability, reconcile uncorrelated outcomes automatically, or establish
namespace closure. The enclosing runtime must schedule bounded turns on wakeup,
completion/reporting changes and retry deadlines; failed retries must not busy
poll. Its mutable admission access is internal composition, not a public control
API or an admission-closure fence. Earlier WP-05/06/07/09 gates persist; WP-11–29
remain pending. Further autonomous WP-10 work can safely continue. No package
acceptance or whole-project completion claimed.


## Continuation — delivery admission shutdown fence (2026-10-21)

Status: Partial. Starting checkpoint `fda3323`, branch `codex/wp06-completion`.
Only pre-existing untracked `scripts/run-autonomous-codex.sh`; preserved. Read
AGENTS.md, autonomous plan, decision register, status, WP-10/11 roadmap entries,
Document 08 routing removal/shutdown boundaries and current delivery-driver,
ordered admission, namespace, supervisor and consumer test code. WP-10 remains
the next executable package; no completed implementation restarted.

Implemented a monotonic driver admission fence. `close` and `drive_shutdown`
reject subsequent admission-owner borrows with `DeliveryAdmissionError::Closed`.
Mutable borrows prevent close racing an existing borrow. Explicit `abort_prepared`
permits cleanup of already-owned preparations after closure without exposing the
tracker for new admission. Recovery, completion, reporting and resumed draining
continue through existing restricted operations. No external contract or financial
semantics changed; the driver has no EventRouterPort implementation yet.

Production witnesses cover open admission, repeated explicit close, already-empty
drain, deadline expiry with retained work, refusal after eventual successful drain,
and post-close abort allowing full drain while recovery reservations remain charged.
One new deterministic test and strengthened existing tests use real owners and
controlled futures, without sleeps or external services.

Verification:
- Root `./scripts/verify-live.sh full`: exit 0. Exact commands: `cargo fmt --all
  -- --check`; `cargo check --locked --offline --no-default-features --features live
  --lib`; same Cargo feature flags with `cargo test --lib` (133 passed), `--test
  live_suite` (435 passed, 2 opt-in ignored), `--doc` (1 passed). Receipts:
  `engine/target/live-verification/20261021T045336Z-3/`. Preliminary full run also
  passed before final witness (434 integration), receipts
  `engine/target/live-verification/20261021T045312Z-7/`.
- From engine/: `cargo check --locked --offline --no-default-features --features
  live,historical`: exit 0; `engine/target/wp10-admission-fence-combined.log`.
- Root `git diff --check`: exit 0. Existing dead-code warnings remain.
  Opt-in SQL/performance tests and historical test execution not rerun; no claim
  for those gates. No database access was needed.

Changed: `engine/src/live/routing/driver.rs`,
`engine/tests/live/destination_worker.rs`, STATUS.md and this evidence ledger.
Checkpoint: local commit carrying this entry. No push, SQL/exchange requests,
Python/historical execution code, dependencies/lockfile or ignored config edits.

Remaining acceptance ledger: production EventRouterPort owner and ordered
admission integration, automatic runtime wakeup/retry scheduling, native
metrics/control binding and whole-router admission namespace closure. This fence
only covers the delivery driver's admission access; it does not close independently
owned namespaces, budgets or registry. It does not certify a complete router or
external durability. Earlier WP-05/06/07/09 gates remain; WP-11–29 pending. Safe
independent autonomous WP-10 implementation can continue. No acceptance or overall
completion claimed.


## Continuation — wake-driven namespace drain (2026-10-21)

Status: Partial. Starting checkpoint `83a2106`, branch `codex/wp06-completion`.
Inspected working tree/history, required campaign documents, WP-10/11 roadmap,
current cursor, namespace, driver, ordered admission and consumer witnesses.
Only pre-existing untracked `scripts/run-autonomous-codex.sh`; preserved.
WP-10 remains next executable; no completed package restarted.

Implemented `StreamAdmissionOrder::poll_shutdown` and
`AdmissionNamespaces::poll_shutdown`. Both fence new admission and distinguish
closed state from outstanding preparation responsibility. Each namespace holds at
most one shutdown waker; replacement updates the coordinator, and permit commit
or abort clears and wakes it outside the cursor lock. Registry polling visits
all bounded entries, including retired namespaces. Cancellation does not reopen
admission or release permits; a later coordinator can resume. Commit advances
the frontier; abort preserves it. Readiness proves admission drain only.

A deterministic production-owner witness covers commit/abort, retired namespace
responsibility, replacement wakers, repeated drain, closed admission/registration,
frontier preservation and empty registry. No sleeps or external services.

Verification (actual exit codes):
- From engine/: `cargo test --locked --offline --no-default-features --features
  live --test live_suite namespace_shutdown`: exit 0, 1 passed.
- Root `./scripts/verify-live.sh full`: exit 0. Commands: `cargo fmt --all --
  --check`; `cargo check --locked --offline --no-default-features --features live
  --lib`; same flags with `cargo test --lib` (133 passed), `--test live_suite`
  (436 passed, 2 opt-in ignored), `--doc` (1 passed). Receipts:
  `engine/target/live-verification/20261021T045558Z-2/`.
- From engine/: `cargo check --locked --offline --no-default-features --features
  live,historical`: exit 0. Existing dead-code warnings remain.
- Root `git diff --check`: exit 0. SQL/performance opt-ins and historical test
  execution not rerun; no claim for those gates. An initial test append used a
  root-relative path from engine/ and failed before writing; corrected path,
  then focused and full checks passed.

Changed: `engine/src/live/routing/{cursor,namespaces}.rs`,
`engine/tests/live/router_obligations.rs`, STATUS.md and this ledger.
Checkpoint: local commit carrying this entry. No push, database/exchange access,
Python/historical execution changes, dependencies or ignored configuration edits.

Remaining: production EventRouterPort ordered owner, runtime wakeup/retry
scheduling, metrics/control integration and composition of admission namespace,
budget, destination registry and delivery shutdown. One coordinator per namespace
is supported; this is a bounded poll primitive, not a spawned runtime or complete
router. Earlier WP-05/06/07/09 gates persist; WP-11–29 pending. Further autonomous
implementation can safely continue. No acceptance or whole-project completion.

## Continuation — composed cooperative router shutdown (2026-10-21)

Status: Partial. Starting checkpoint `04ff888`, branch `codex/wp06-completion`.
Inspected required campaign documents, current history/working tree, WP-10/11
roadmap, registry, ordered admission/budget, namespaces, driver, supervisor and
consumer witnesses. WP-10 remains next executable; completed work preserved.
Only pre-existing untracked launcher `scripts/run-autonomous-codex.sh` preserved.

Implemented registry shutdown queue/capacity closure after fencing membership
and releasing both registry locks. Executor wakeups can re-enter observations
and planning without deadlock. Already queued/consumer-owned payloads and capacity
reservations remain charged; nothing is discarded or declared delivered.

Added `RouterShutdownOwners` over the existing authoritative owners. Each call
closes budget/namespaces/registry/delivery admission, observes all in-process
responsibilities, checks deadline, then performs at most one bounded delivery
turn. Expiry makes no adapter call or tracking retry. Repeated/resumed shutdown
uses the same owners. `RouterDrainSnapshot` covers retired namespaces, pending
cursor permits, global admission usage, registered queue/consumer-held capacity,
waiters and the complete delivery drain predicate. It certifies no external
consumer processing or crash durability. The runtime must still schedule turns
for completions, reporting, transferred responsibility and bounded retries.

Deterministic production-path witnesses cover reentrant shutdown wakeups,
blocked capacity failure, queue closure, repeated closure, payload/charge
preservation, deadline expiry/resume, cursor preparation rollback, empty tracker
preparation ownership and consumer-held payloads after delivery drain.

Verification (actual exit codes):
- From engine/: `cargo test --locked --offline --no-default-features --features
  live --test live_suite registry_shutdown_wakes`: exit 0, 1 passed.
- Same command filtered by `router_shutdown`: final exit 0, 2 passed (new
  composition witness and existing prepared commitment shutdown witness).
- Root `./scripts/verify-live.sh full`: final exit 0; executes `cargo fmt --all
  -- --check`, `cargo check --locked --offline --no-default-features --features
  live --lib`, same flags with `cargo test --lib` (133 passed), `--test live_suite`
  (438 passed, 2 opt-in ignored), `--doc` (1 passed). Final receipts:
  `engine/target/live-verification/20261021T050018Z-2/`.
- From engine/: `cargo check --locked --offline --no-default-features --features
  live,historical`: final exit 0. Existing dead-code warnings remain.
- `git diff --check`: exit 0. Initial root Cargo formatting invocation failed
  (no Cargo.toml); a later root-relative append invoked from engine failed before
  writes. Corrected working directories. First composition-test compilation
  exited 101 due to private CancellationToken import; fixed to foundation export.
  All final checks above pass. No assertions weakened.

Changed: `engine/src/live/routing/{router,shutdown,mod}.rs`,
`engine/tests/live/router.rs`, STATUS.md and this ledger. Checkpoint: local commit
carrying this entry. No push, database/exchange traffic, dependency/lockfile,
ignored configuration or Python/historical execution changes. SQL/performance
opt-ins and historical-only test execution unavailable/not rerun; no claims for
those gates or the historical zero-stream baseline.

Remaining acceptance ledger: production EventRouterPort ordered composition,
runtime scheduling/retry integration, native metrics/control binding. Shutdown
composition now exists but needs binding in that owner and ultimately WP-24.
Earlier WP-05/06/07/09 gates persist. WP-11–29 remain pending; no package accepted
or overall completion claimed. Further autonomous WP-10 work can safely continue.


## Continuation — wake shutdown after final accounting release (2026-10-21)

Status: Partial. Starting checkpoint `ab7dac5`, branch `codex/wp06-completion`.
Read required campaign instructions, decisions/status and WP-10/11 roadmap;
inspected current ordered admission, registry, delivery driver, shutdown and real
consumer tests. Next executable remains WP-10. Preserved pre-existing untracked
`scripts/run-autonomous-codex.sh`; no competing writer or completed-work restart.

Implemented one bounded replacement-aware shutdown waiter on ResourceBudget and
DestinationCapacity. Closure and drain registration are atomic with accounting;
final release extracts the waiter and invokes it outside the lock. Partial release
retains it. Existing reservations/payloads remain charged; no delivery or durability
is inferred. OrderedAdmissionBudget and RouterRegistry expose poll_shutdown;
RouterShutdownOwners registers the coordinator on both real owners. Registry
releases its locks before capacity shutdown/wake callbacks. Foundation change is
required by WP-10's existing global preparation accounting, with no financial or
historical contract changes.

Deterministic tests exercise real production reservations and registered queue
payloads, final/partial release, latest-waker replacement, reentrant snapshots,
closed admission and repeat drain. No arbitrary sleeps or external dependencies.

Verification (actual commands/exit codes):
- From engine/: `cargo test --locked --offline --no-default-features --features
  live --test live_suite registry_shutdown`: exit 0, 2 passed (capacity change).
- Root `./scripts/verify-live.sh full`: final exit 0. Executes `cargo fmt --all
  -- --check`; `cargo check --locked --offline --no-default-features --features
  live --lib`; same flags with `cargo test --lib` (134 passed), `--test live_suite`
  (439 passed, 2 opt-in ignored), `--doc` (1 passed). Final receipts:
  `engine/target/live-verification/20261021T050256Z-2/`.
- From engine/: `cargo check --locked --offline --no-default-features --features
  live,historical`: final exit 0; existing dead-code warnings remain.
- Root `git diff --check`: exit 0 before checkpoint. SQL/performance opt-ins and
  historical-only execution not run; no claim on those gates or zero-stream baseline.

Changed paths: `engine/src/live/foundation/resources.rs`,
`engine/src/live/routing/{destination,ordered,router,shutdown}.rs`,
`engine/tests/live/router.rs`, STATUS.md and this ledger.
Checkpoint: local commit carrying this entry. No push, SQL/exchange traffic,
Python/historical execution edits, dependency/lockfile or ignored-config changes.

Remaining acceptance ledger: production EventRouterPort ordered composition,
runtime retry scheduling and native metrics/control binding; shutdown primitives
still need binding in that owner and WP-24. One shutdown coordinator per accounting
owner is supported. Reporting/transferred recovery responsibility still needs runtime
scheduling; these wakeups establish no external consumer processing or durability.
WP-05/06/07/09 gates persist; WP-11–29 pending. Safe autonomous continuation remains
possible; no package acceptance or whole-project completion claimed.


## Continuation — retain removed destination drain ownership (2026-10-21)

Status: Partial. Starting checkpoint `57171a8`, branch `codex/wp06-completion`.
Inspected campaign instructions, decisions, current status/evidence, roadmap WP-10,
registry, queue, worker, admission and shutdown consumers. Next executable: WP-10.
The only initial untracked file was `scripts/run-autonomous-codex.sh`; preserved.

Fixed a production gap: unregister previously erased occupied queues from registry
and composed shutdown observations. Retired entries now retain original adapter
and queue ownership, close outside both registry locks, and support exclusive
oldest-unclaimed worker acquisition. Same-ID active replacements and multiple old
instances keep independent queues. Active plus retired entries consume the same
bounded destination slots. Explicit reaping requires closed queues, zero queue
payloads, zero occupied count/bytes and zero capacity waiters; adapters drop after
registry locks release. Reaping establishes no delivery completion or durability.
Snapshots and both shutdown paths include retired queues. Runtime must schedule
retired worker claims, reporting, recovery and explicit reaping.

Tests exercise original Arc payload readback, worker exclusivity, queue closure,
replacement independence, repeated same-ID retirement at the resource limit,
partial/final charge release, held reservations, reentrant retirement wakeups,
stale queue admission rejection and composed shutdown across retired ownership.
No sleeps, SQL or exchange dependencies. Existing replacement assertions unchanged.

Verification (actual exits):
- From engine/: `cargo test --locked --offline --no-default-features --features
  live --test live_suite registry_retirement`: exit 0, 3 passed.
- Same command filtered by `router_shutdown`: exit 0, 2 passed; filtered by
  `destination_worker_registration_replacement`: final exit 0, 1 passed.
- Root `./scripts/verify-live.sh full`: final exit 0; formatting; Live library
  check; 134 library / 442 integration (2 opt-in ignored) / 1 doctest passed.
  Exact Cargo commands/logs: `engine/target/live-verification/20261021T050714Z-2/`.
- From engine/: `cargo check --locked --offline --no-default-features --features
  live,historical`: exit 0, existing dead-code warnings only.
- `git diff --check`: exit 0 before checkpoint.
Initial replacement test failed because the first draft blocked same-ID reuse;
corrected retirement storage to preserve established independent replacement
behavior, without changing that test. One edit script failed before writing;
corrected its match. No final failing verification. SQL/performance opt-ins and
historical-only test execution not rerun; no new claim on those gates/baselines.

Changed: `engine/src/live/routing/{router,shutdown,snapshot}.rs`,
`engine/tests/live/router.rs`, STATUS.md and this ledger. Checkpoint: `b658d5e`
(bounded retired-destination ownership and shutdown drain). No push, dependency/lockfile, ignored config,
Python/historical execution changes or production data access.

Remaining acceptance ledger: production EventRouterPort ordered composition,
runtime retries/retired-drain/reaping integration, native metrics/control binding.
WP-05/06/07/09 gates remain; WP-11–29 pending. No package accepted and no overall
completion claim. Further autonomous WP-10 work can safely continue.


## Continuation — schedule retired and replacement instances together (2026-10-21)

Status: Partial. Starting checkpoint `00d3db1`, branch `codex/wp06-completion`.
Read repository/campaign instructions, decision register, status, WP-10/11 roadmap
and latest evidence; inspected actual registry, queue, scheduler and consumer tests.
Next executable remains WP-10. Preserved untracked `scripts/run-autonomous-codex.sh`.

Fixed scheduler integration with registry retirement: duplicate destination IDs are
permitted when at least one independent queue is closed. Two open instances remain
rejected, queue identity and exclusive leases protect dequeue ownership, and all
instances still consume scheduler slots. Registration errors return the full worker.
Existing closed/drained-only removal preserves charged retired owners and open
replacements. No external delivery/durability is inferred from draining.

New deterministic production registry/scheduler witness covers active-first capacity
rejection, returned retired ownership, retired-first successful coexistence, both
outcome transfers, payload-charge fencing of removal/reaping, independent replacement
preservation and final closed scheduler drain. Existing duplicate-open, fairness,
slow-adapter and resource-bound tests remain unchanged and pass in the full suite.

Verification (actual commands/exit codes):
- From engine/: `cargo test --locked --offline --no-default-features --features live
  --test live_suite registry_retired_and_replacement_workers_share_bounded_scheduler`:
  exit 0, 1 passed.
- Root `./scripts/verify-live.sh full`: exit 0. Runs `cargo fmt --all -- --check`,
  `cargo check --locked --offline --no-default-features --features live --lib`,
  `cargo test` with the same locked/offline/feature flags and `--lib` (134 passed),
  `--test live_suite` (443 passed, 2 opt-in ignored), `--doc` (1 passed).
  Receipts: `engine/target/live-verification/20261021T050911Z-2/`.
- From engine/: `cargo check --locked --offline --no-default-features --features
  live,historical`: exit 0; existing dead-code warnings only.
- Root `git diff --check`: exit 0 before checkpoint.
SQL/performance opt-ins and historical-only test execution not run; no claim on
those gates or the historical zero-stream baseline. No failed behavioral checks.

Changed: `engine/src/live/routing/worker.rs`, `engine/tests/live/router.rs`,
STATUS.md and this ledger. Checkpoint: local commit carrying this entry.
No push, SQL/exchange traffic, Python/historical execution edits, dependency/lockfile
or ignored-configuration changes. This is an incremental verified WP-10 checkpoint,
not package acceptance. Remaining: production EventRouterPort ordered composition,
runtime retry/retired-drain/reaping scheduling and native metrics/control binding.
WP-05/06/07/09 gates persist; WP-11–29 pending. Further autonomous work is safe.


## Continuation — dynamic supervised worker membership (2026-10-21)

Status: Partial. Starting checkpoint `23db1c3`, branch `codex/wp06-completion`.
Read AGENTS, campaign plan, decisions, status and roadmap WP-08–11; inspected real
registry, worker, supervisor, driver, shutdown and consumer tests. Next executable
remains WP-10. Preserved initial untracked `scripts/run-autonomous-codex.sh`.

The composed delivery owner previously exposed no way to attach replacement or
retired workers after construction, or release drained scheduler slots. Added
owner-preserving `insert_worker` through DeliverySupervisor/DeliveryDriver and a
capacity-bounded `reap_drained` scheduler pass exposed through both owners. Reaping
requires closed queues and zero queued, pending-call, outcome and payload charges;
open replacement instances survive. Registration retains existing duplicate/capacity
checks and closed scheduler fence; errors return the complete worker. Scheduling
position is adjusted when removed slots precede it. Registry reaping remains
explicit and independent; neither reaping path finalizes delivery obligations.

New deterministic real registry/driver witness: same-ID retired/replacement queues,
externally held payloads, scheduler capacity rejection with intact returned worker,
charge-fenced reaping, retry into released slot, open replacement preservation,
idempotent reaping, closed scheduler drain and rejected post-close registration.
No sleeps, SQL or exchanges. Existing worker fan-out/fairness tests remain passing.
Initial witness attempted external dequeue after exclusive worker acquisition;
corrected fixture order to hold payloads before acquisition, preserving production
queue exclusivity. An initial edit script used the wrong working directory and
made no changes; corrected path. No final failing checks.

Verification (actual exits):
- From engine/: `cargo test --locked --offline --no-default-features --features live
  --test live_suite delivery_driver_reconciles_retired`: final exit 0, 1 passed.
- Root `./scripts/verify-live.sh full`: final exit 0. Executes `cargo fmt --all
  -- --check`; `cargo check --locked --offline --no-default-features --features
  live --lib`; tests with same flags and `--lib` (134 passed), `--test live_suite`
  (444 passed, 2 opt-in ignored), `--doc` (1 passed). Final receipts:
  `engine/target/live-verification/20261021T051151Z-2/`.
- From engine/: `cargo check --locked --offline --no-default-features --features
  live,historical`: exit 0; existing dead-code warnings only.
- Root `git diff --check`: exit 0 before checkpoint.
SQL/performance opt-ins and historical-only execution not run; no claim on those
gates or historical zero-stream baseline. Full suite rerun after strengthening
capacity rejection/retry assertions; no production edits after combined check.

Changed: `engine/src/live/routing/{worker,supervisor,driver}.rs`,
`engine/tests/live/router.rs`, STATUS.md and this ledger. Checkpoint: local commit
carrying this entry. No push, SQL/exchange traffic, Python/historical execution,
dependency/lockfile or ignored configuration edits.

Remaining: production EventRouterPort ordered composition, runtime wakeup/retry
and registry-worker synchronization/retired reaping, native metrics/control binding.
These lifecycle APIs are real composition primitives, not a running router daemon.
Earlier WP-05/06/07/09 gates persist; WP-11–29 pending. No acceptance or entire
project completion claimed. Further autonomous WP-10 implementation is safe.


## Continuation — registry reconciliation and membership wakeups (2026-10-21)

Status: Partial. Starting checkpoint `6e07aae`, branch `codex/wp06-completion`.
Inspected AGENTS, autonomous plan, decisions, status, roadmap WP-10/11, distribution
requirements, existing ordered admission, registry, driver/supervisor, worker,
shutdown and real consumer witnesses. WP-10 remains next executable; completed
work preserved. Initial untracked `scripts/run-autonomous-codex.sh` untouched.

Implemented:
- `RouterRegistry::claim_next_worker`: destination-limit-bounded scan, retired
  instances before sorted active IDs, skipping direct adapters and existing
  exclusive leases. Frozen descriptors avoid adapter callbacks under locks.
- `DeliveryDriver::sync_registry`: reap closed/drained workers and retired registry
  entries, attach at most one fresh worker, report counts and explicit failures.
  Failed scheduler insertion releases only the unpolled fresh lease: no dequeue,
  adapter operation or payload/charge loss. Existing workers remain owned.
  Closed driver permits reaping and rejects attachment.
- `RouterRegistry::poll_membership_change`: revision comparison and one replaceable
  coordinator waiter share the membership fence. Successful registration/removal
  and shutdown extract the waiter and wake outside both locks. Failed changes
  leave it armed; retired-slot reaping does not change routing revision. Repeated
  shutdown is idempotent. Notification establishes no delivery/durability evidence.

Three deterministic witnesses exercise actual registry/driver/queues: retired
charge fences, same-ID replacement, full scheduler and fresh-lease retry, queued
payload preservation, deterministic active claims, existing lease exclusion,
direct adapters, closed-driver attachment rejection, replacement wakers, repeated
polls, failed mutation without wake, register_queue, removal, unchanged-revision
shutdown and reentrant read/commitment callbacks. No arbitrary sleeps/services.

Verification (actual exits):
- From engine/: `cargo test --locked --offline --no-default-features --features
  live --test live_suite registry_`: final exit 0, 15 passed. Initial compile exit
  101: DestinationId lacks Ord; sorted its existing u64 value, no contract change.
- Same flags with filter `registry_membership_notifications`: exit 0, 1 passed;
  subsequent full suite includes strengthened commitment reentrancy assertions.
- Root `./scripts/verify-live.sh full`: final exit 0. Executes `cargo fmt --all
  -- --check`; `cargo check --locked --offline --no-default-features --features
  live --lib`; `cargo test` with those same flags and `--lib` (134 passed),
  `--test live_suite` (447 passed, 2 opt-in ignored), `--doc` (1 passed).
  Final receipts: `engine/target/live-verification/20261021T051603Z-2/`.
  Earlier reconciliation-only full run also passed (446 integration tests):
  `engine/target/live-verification/20261021T051417Z-2/`.
- From engine/: `cargo check --locked --offline --no-default-features --features
  live,historical`: exit 0 after each implementation increment; existing dead-code
  warnings remain. Root `git diff --check`: exit 0 before checkpoints.
- SQL/performance opt-ins and historical-only test execution not run. No claim
  on those gates or the historical zero-stream baseline.

Changed: `engine/src/live/routing/{driver,router}.rs`,
`engine/tests/live/router.rs`, STATUS.md and this ledger.
Implementation checkpoints: `51bd142` (bounded registry/driver reconciliation),
`b12bf59` (membership notification). Evidence checkpoint: commit carrying this
entry. No push, database/exchange access, Python/historical execution edits,
dependency/lockfile or ignored configuration changes.

Remaining acceptance ledger: production EventRouterPort ordered composition,
runtime orchestration of worker/recovery/completion/reporting wakeups and bounded
retries, native metrics/control binding. Runtime must synchronize workers before
closing the driver; closed-driver maintenance deliberately cannot attach new
workers. Membership notification supports one coordinator and does not signal
external payload release or completion; those owners retain their own wakeups.
These are integrated production composition primitives, not a running daemon.
Earlier WP-05/06/07/09 gates persist; WP-11–29 pending. Next authorized executable
package: WP-10. Further autonomous implementation is safe. No acceptance claimed.

## Continuation — bounded registered delivery turns (2026-10-21)

Status: Partial. Implementation checkpoint: `cd7f74f`; starting checkpoint:
`b9ebf5b`, branch `codex/wp06-completion`. Inspected campaign instructions, Git
inventory/history, decisions, status, roadmap WP-08–11, distribution requirements,
actual registry/ordered admission/driver/supervisor/worker owners and tests.
WP-10 remains next executable. Preserved untracked user launcher.

Implemented `DeliveryDriver::drive_registered` and its explicit result: one
registry reconciliation (at most one fresh attachment), one existing bounded
recovery/supervision pass, and membership observation against the revision captured
before maintenance/adapter polling. Successful attachment/retention/finalization or
an observed concurrent membership revision schedules one further cooperative
turn. Capacity/correlation failures and shutdown do not self-wake repeatedly.
No second tracker, task, executor, retry queue or delivery policy was introduced.

Acceptance ledger and witnesses:
- Bounded reconciliation and retirement: one attachment per turn, scheduler-full
  rejection releases only the fresh lease, retired drain reaps one slot and
  attaches its replacement, closed driver fences attachment.
- Wake safety: actual membership mutation during adapter future polling is
  observed; the next turn arms the current revision. Capacity/correlation/shutdown
  observations do not create an immediate retry loop.
- Fault ownership: deliberately untracked queued payload produces a retained
  correlation failure, is not resubmitted, and transfers intact to recovery.
- Real consumer path: existing OrderedFanout prepares/commits through the same
  driver tracker/recovery, adapter admission retains the delivery, explicit
  Delivered completion permits finalization, returned terminal record retains
  capacity until dropped, and the independent journal record remains visible.

Verification (actual final exits):
- From engine/: `cargo test --locked --offline --no-default-features --features
  live --test live_suite registered_driver`: exit 0, 3 passed.
- Root `./scripts/verify-live.sh full`: exit 0; `cargo fmt --all -- --check`,
  `cargo check --locked --offline --no-default-features --features live --lib`,
  tests with the same locked/offline/feature flags and `--lib` (134 passed),
  `--test live_suite` (450 passed, 2 ignored), `--doc` (1 passed).
  Receipts: `engine/target/live-verification/20261021T052023Z-2/`.
- From engine/: `cargo check --locked --offline --no-default-features --features
  live,historical`: exit 0; existing dead-code warnings remain.
- Root `git diff --check`: exit 0 before implementation checkpoint.
- Initial test append used a root-relative path from engine/ and wrote no file;
  corrected path. Initial success-witness compile exit 101 used the wrong
  ManualClock import; corrected to foundation::clock. Its next run exit 101
  exposed the expected retained-capacity backpressure (CapacityBlocked); corrected
  fixture expectation to assert that explicit state and absence of a self-wake.
  No production assertions or delivery guarantees weakened.

Changed: `engine/src/live/routing/driver.rs`, `engine/tests/live/router.rs`,
STATUS.md and this ledger. No SQL/exchange traffic, production data, dependencies,
ignored configuration, Python or historical execution edits. SQL/performance
opt-ins and historical-only tests not run; no claim on those gates or historical
zero-stream baseline. No push. Documentation checkpoint is the commit carrying
this entry.

Remaining: production EventRouterPort ordered composition; runtime completion,
reporting and bounded retry timer scheduling; native metrics/control binding.
External payload release and completion/reporting still require runtime notification
or a bounded retry timer; failed tracking passes must not be busy-polled. Synchronize
all queues before closing the driver; this method is not deadline shutdown.
Earlier WP-05/06/07/09 gates remain open and WP-11–29 remain pending. Further
independent autonomous WP-10 work is safe. No package or project acceptance claimed.


## Continuation — completion coordinator notification (2026-10-21)

Status: Partial. Starting checkpoint `912fb94`, branch `codex/wp06-completion`.
Inspected repository instructions, autonomous plan, decision register, current
status/history/evidence, roadmap WP-10/11 and actual delivery/admission consumers.
Next executable remains WP-10. Preserved untracked scripts/run-autonomous-codex.sh.

Implementation checkpoint: `8f7a2d5`. DeliveryDriver now retains at most one
replaceable coordinator waker, armed before delivery polling. Successful correlated
completion takes and wakes it; failed correlation leaves it armed. Completions
coalesce until another drive. Closing admission preserves completion progress.
No new task, queue, tracker, durability guarantee or financial policy introduced.
The existing real ordered-fanout consumer witness now also exercises invalid
correlation without wake, coordinator replacement, completion after close, correct
coordinator wake and subsequent terminal finalization/charged payload release.

Verification (actual exits, commands from engine/ unless noted):
- cargo test --locked --offline --no-default-features --features live --test
  live_suite registered_driver: exit 0, 3 passed.
- Root ./scripts/verify-live.sh full: exit 0. Runs cargo fmt --all -- --check;
  cargo check --locked --offline --no-default-features --features live --lib;
  cargo test with the same flags and --lib (134 passed), --test live_suite
  (450 passed, 2 opt-in ignored), --doc (1 passed). Receipts:
  engine/target/live-verification/20261021T052247Z-2/.
- cargo check --locked --offline --no-default-features --features live,historical:
  exit 0; existing dead-code warnings remain.
- Root git diff --check: exit 0. No behavioral test failures. A reconnaissance
  rg for rust-version returned no match (exit 1), not a verification failure.
  Only a doc comment changed after full checks.

Changed: engine/src/live/routing/driver.rs, engine/tests/live/router.rs,
STATUS.md and this ledger. No SQL/exchange traffic, push, dependency/lockfile,
ignored configuration or Python/historical execution edits. SQL/performance
opt-ins and historical-only tests not run; historical zero-stream baseline not
rechecked. Documentation checkpoint is the commit carrying this entry.

Remaining ledger: production EventRouterPort ordered composition, runtime
reporting and bounded retry timer scheduling, native metrics/control binding.
External payload release and reporting still need notifications or retry timers;
completion notification is an in-process coordinator primitive, not a daemon.
Earlier WP-05/06/07/09 gates persist; WP-11–29 remain pending. Further autonomous
implementation can safely continue. No acceptance or project completion claimed.


## Continuation — terminal reporting capacity notification (2026-10-21)

Status: Partial. Starting checkpoint `8b77401`; branch `codex/wp06-completion`.
Inspected AGENTS.md, autonomous plan, decisions, status, roadmap WP-10/11,
current history and actual driver/journal/reporting/resolution consumers.
WP-10 remains next executable; did not restart completed work. Preserved
pre-existing untracked scripts/run-autonomous-codex.sh.

Implementation checkpoint: `077a78a`. FinalizationJournal now owns at most one
replaceable coordinator waker. DeliveryDriver arms it before recovery/supervision.
Successful reporting acknowledgement or committed escalation resolution consumes
and wakes it. Failed operations leave it armed; reporting remains available after
admission closes. Resolution wakes after releasing the resolution-sink commitment
guard so a callback can safely observe that sink. No new queue, executor, tracker,
reporting policy, event identity or durability promise.

Acceptance ledger: real ordered-fanout/retained-delivery witness now checks
reporting wake after close, failed acknowledgement without wake, successful record
removal and journal count. New resolution lifecycle test checks coordinator
replacement, failed resolution retaining the waiter/record, successful transfer,
reentrant sink observation (lock released), rejected double commit, and downstream
claimability. This is in-process scheduling integration, not a running daemon.

Verification (commands from engine/ unless marked root; all final exits 0):
- cargo test --locked --offline --no-default-features --features live --test
  live_suite registered_driver: 3 passed.
- cargo test --locked --offline --no-default-features --features live --test
  live_suite journal_capacity_notification: 1 passed.
- Root ./scripts/verify-live.sh full: cargo fmt --all -- --check; cargo check
  --locked --offline --no-default-features --features live --lib; cargo test
  with the same flags and --lib (134 passed), --test live_suite (451 passed,
  2 opt-in ignored), --doc (1 passed). Final receipts:
  engine/target/live-verification/20261021T052449Z-2/.
- cargo check --locked --offline --no-default-features --features live,historical:
  exit 0; existing dead-code warnings remain.
- Root git diff --check: exit 0 before checkpoint.
- Earlier full check also passed (450 integration tests) before the new resolution
  test was added; final full suite above supersedes it. Exploratory rg paths for
  nonexistent acknowledgement/journal test files returned errors; corrected by
  locating existing reporting/resolution tests. No compiler or test failures.

Changed: engine/src/live/routing/acknowledgement.rs, routing/driver.rs,
engine/tests/live/router.rs, resolution_lifecycle.rs, STATUS.md and this ledger.
No database/exchange traffic, push, dependency/lockfile, ignored configuration,
Python or historical execution changes. SQL/performance opt-ins and historical-only
suite not run; historical zero-stream baseline not rechecked. Documentation
checkpoint is the commit carrying this entry.

Remaining: production EventRouterPort ordered composition; bounded retry timer
scheduling (failed tracking/reporting and external payload release); native
metrics/control integration. Actual reporting retries still require runtime
scheduling. Earlier WP-05/06/07/09 gates persist; WP-11–29 pending. Further safe
autonomous work remains authorized. No package acceptance/project completion claimed.


## Continuation — bounded delivery maintenance timer (2026-10-21)

Status: Partial. Starting checkpoint `d7e38f6`, branch `codex/wp06-completion`.
Reconciled instructions, autonomous plan, decisions, status, roadmap WP-10/11,
latest evidence and actual ordered admission/delivery/queue/timer consumers.
WP-10 remains next executable. Preserved untracked scripts/run-autonomous-codex.sh.

Implementation checkpoint: `001633c`. The existing DeliveryDriver now offers
`drive_registered_with_retry`, performing one real bounded registered turn while
owning at most one timer through the existing DeadlineTimer/MonotonicClock ports.
Unrelated turns do not move its deadline; expiration releases its reservation
before rearming. There is no missed-tick catch-up or adapter resubmission. Timer
registration errors and invalid zero intervals are explicit without suppressing
independent delivery progress. Closed drivers retain scheduling while payload
charges or other responsibilities remain, then cancel the timer on complete drain.

Acceptance witness: a real registered destination queue transfers a charged batch
to an external consumer. Deterministic clock/timer exercise zero interval, exhausted
timer capacity, continued worker attachment, repeated turns without busy wakes or
deadline postponement, timer expiry/rearm with capacity one, closed-driver charge
retention, and eventual worker reaping/drain after the consumer releases its batch.
No arbitrary sleeps, network requests or parallel test-only delivery algorithm.

Verification (commands from engine/ unless marked root):
- cargo test --locked --offline --no-default-features --features live --test
  live_suite registered_driver_retry_timer: exit 0, 1 passed.
- Root ./scripts/verify-live.sh full: exit 0. Runs cargo fmt --all -- --check;
  cargo check --locked --offline --no-default-features --features live --lib;
  cargo test with those same feature flags and --lib (134 passed), --test live_suite
  (452 passed, 2 opt-in ignored), --doc (1 passed). Exact receipts:
  engine/target/live-verification/20261021T052731Z-2/.
- cargo check --locked --offline --no-default-features --features live,historical:
  exit 0; pre-existing dead-code warnings remain.
- Root git diff --check: exit 0 before implementation checkpoint.
- Initial test-file append used a root-relative path while already in engine/ and
  failed (no file changed). Its following filtered command ran zero tests and is
  not counted as verification. Corrected append and the 1-test/full receipts above
  supersede that attempt. No compiler or behavioral test failures.

Changed: engine/src/live/routing/driver.rs, engine/tests/live/router.rs, STATUS.md
and this package ledger. No SQL/exchange access, push, dependencies/lockfile,
ignored configuration or Python/historical execution changes. Opt-in SQL/performance
and historical-only tests not run; historical zero-stream baseline not rechecked.
Documentation checkpoint is the commit carrying this entry.

Remaining: production EventRouterPort ordered admission/runtime composition,
actual external reporting retries, native metrics/control integration. The runtime
must consistently poll the timed entry point, surface timer admission failure, and
supply a functioning timer. This is in-process maintenance scheduling, not daemon
composition or durable recovery. Earlier WP-05/06/07/09 gates remain; WP-11–29
pending. Next authorized package: WP-10 completion. Further autonomous work can
safely continue. No package acceptance or project completion claimed.

## Continuation — terminal reporting in timed delivery turns (2026-10-21)

Status: Partial. Starting checkpoint `08b02d0`, branch `codex/wp06-completion`.
Inspected instructions, autonomous plan, decisions, status, WP-10 prerequisites
and acceptance, current delivery/reporting/worker/journal consumers and tests.
Next executable package remains WP-10. Existing untracked launcher preserved.

Implementation checkpoint: `358c34e`. `DeliveryDriver::drive_registered_reporting`
composes existing timed registered maintenance with the existing reporting
consumer against the same owned journal. Each turn processes at most one record.
Deferred temporary failures do not self-wake/retry early; permanent failures stay
escalated. Reporting is independent of timer registration success and continues
while closed. Successful reporting schedules continued progress; complete closed
owner drain cancels its timer immediately. No second tracker/journal or reporter
policy was introduced. Reporter calls must remain bounded/nonblocking/idempotent;
this synchronous port cannot preempt a handler or prove external durability.

Acceptance ledger for this continuation:
- Implemented: bounded reporting/delivery composition, explicit independent timer
  errors, retry eligibility, escalation retention, closed-owner reporting/drain.
- Witnesses: real finalization journal populated through tracker failure and
  finalization; manual timer/clock; temporary failure then success after deadline;
  no premature handler calls or busy wakes; permanent failure under exhausted
  timer capacity remains inspectable and prevents false shutdown success.
- Still missing for package acceptance: production EventRouterPort ordered
  admission composition, concrete reporter/runtime integration, native metrics
  and control snapshot integration. This change does not certify the daemon.

Verification (all final exits 0):
- From engine/: cargo test --locked --offline --no-default-features --features live
  --test live_suite registered_reporting: initial witness 1 passed. It initially
  emitted two unused-Result warnings; both fixed by asserting clock/timer success.
- Root ./scripts/verify-live.sh full: cargo fmt --all -- --check; cargo check
  --locked --offline --no-default-features --features live --lib; cargo test with
  those same flags and --lib (134 passed), --test live_suite (454 passed, 2 opt-in
  ignored), --doc (1 passed). Final receipts:
  engine/target/live-verification/20261021T052956Z-2/.
- From engine/: cargo check --locked --offline --no-default-features --features
  live,historical: exit 0, existing dead-code warnings only.
- Root git diff --check: exit 0 before implementation checkpoint.
- Earlier full run passed 453 integration tests before the second witness; final
  receipts supersede that run. No compiler or behavioral test failures.

Changed: engine/src/live/routing/driver.rs, engine/tests/live/reporting.rs,
STATUS.md and this ledger. No SQL/exchange access, remote push, dependency/lockfile,
ignored configuration or Python/historical execution changes. Opt-in SQL/performance
and historical-only suites not run; historical zero-stream baseline not rechecked.
Documentation checkpoint is the commit carrying this entry.

Earlier WP-05/06/07/09 gates remain; WP-11–29 pending. Next authorized executable
package: WP-10 completion. Further autonomous implementation can safely continue.
No package acceptance or entire-project completion claimed.


## Continuation — ordered commitment through delivery owner (2026-10-21)

Status: Partial. Starting HEAD `2e3dec0`, branch `codex/wp06-completion`;
pre-existing untracked `scripts/run-autonomous-codex.sh` preserved. Reviewed
repository instructions, autonomous plan, decisions, current status, roadmap
WP-05–11 and existing ordered/driver/registry/consumer implementations.
Next executable package remains WP-10; completed work was preserved.

Implementation checkpoint: `0b0de81`. `DeliveryDriver::commit_ordered` connects
ordered commitment and optional fan-out to its existing authoritative tracker and
recovery owner without exposing mutable ownership. Closure returns both the typed
error and original preparation before mutation. The caller retains capacity and
ordering responsibility until it drops that preparation. Existing ordered checks,
tracking reservations and partial outcomes are preserved rather than remapped to
success. This is synchronous commitment composition, not EventRouterPort or an
executor; existing preparation/context/resource limits remain mandatory.

Acceptance ledger:
- Implemented: ordered commitment through delivery owner, optional outcome
  propagation, shutdown fencing with ownership-preserving rejection.
- Consumer witness: existing registered driver test now commits through this
  method, then supervises the actual queue, correlates completion, finalizes and
  reports using the same driver owners.
- Negative witness: preparation acquired before driver close is returned intact;
  no obligations appear, frontier stays unchanged, pending order responsibility
  remains until returned preparation is dropped, then releases.
- Missing: production EventRouterPort admission, concrete runtime/reporting and
  native metrics/control integration. No package acceptance claimed.

Verification (commands from engine/ unless marked root):
- cargo test --locked --offline --no-default-features --features live --test
  live_suite driver_: exit 0, 11 passed, 446 filtered out.
- Root ./scripts/verify-live.sh full: exit 0. cargo fmt --all -- --check;
  cargo check --locked --offline --no-default-features --features live --lib;
  cargo test with same flags --lib (134 passed), --test live_suite (455 passed,
  2 opt-in ignored), --doc (1 passed). Receipts:
  engine/target/live-verification/20261021T053147Z-2/.
- cargo check --locked --offline --no-default-features --features live,historical:
  exit 0; existing dead-code warnings remain.
- Root git diff --check: exit 0 before implementation checkpoint.
- Initial test append used a root-relative path while in engine/ and failed without
  modifying a file. The subsequent filtered run passed 10 existing tests; the
  corrected append and final receipts above include the new witness. No compiler
  or behavioral failures. No SQL tests or exchange traffic. Historical-only suite
  and historical zero-stream baseline were not rechecked.

Changed files: engine/src/live/routing/driver.rs, engine/tests/live/router.rs,
.planning/live/STATUS.md and this evidence ledger. No Python/historical execution,
dependency/lockfile, ignored configuration, database or system changes. No push.
Documentation checkpoint is the commit containing this entry.

Remaining packages: WP-10 Partial; earlier WP-05/06/07/09 gates unchanged;
WP-11–29 pending. Next authorized executable package: WP-10 completion.
Further autonomous implementation can safely continue. The service and project
are not complete.


## Continuation — admission closure and read-only router observation (2026-10-21)

Status: Partial. Starting HEAD `3b59ef9`, branch `codex/wp06-completion`.
Pre-existing untracked `scripts/run-autonomous-codex.sh` preserved. Inspected
instructions, campaign plan, decision register, status, roadmap WP-08–11, current
router, ordered admission, shutdown, supervision and relevant consumer tests.
WP-10 remains next executable; no completed functionality restarted.

Implementation checkpoint: `a2ccb14`.

Acceptance ledger:
- Implemented: atomic mandatory budget observation of limits, usage and closure;
  ordered-budget exposure; shared-reference `RouterDrainSnapshot::observe`;
  shutdown delegates to that same observation; drain now requires admission
  closure as well as zero usage and other owners' drain.
- Regression: all other owners closed/empty while admission remains open does
  not report complete drain. Closing admission then permits drain. Observation
  does not close owners or poll adapters. Snapshots remain owned/stable.
- Accounting witness: closing a charged budget preserves its charge and limit,
  rejects further reservations and releases usage only on reservation drop.
- Still missing: production EventRouterPort composition, concrete runtime and
  reporter integration, native metric/control binding. This supplies observation
  to future Control consumers; it does not implement the Control service.
- Observation is consistent within each owner, not a cross-owner transaction.
  No external durability or end-consumer processing claim follows from drain.

Verification (from engine/ unless stated; final exits all 0):
- cargo test --locked --offline --no-default-features --features live --test
  live_suite router_observation: 1 passed, 457 filtered out.
- Root ./scripts/verify-live.sh full: cargo fmt --all -- --check; cargo check
  --locked --offline --no-default-features --features live --lib; cargo test
  with the same flags --lib (135 passed), --test live_suite (456 passed,
  2 opt-in ignored), --doc (1 passed). Receipts:
  engine/target/live-verification/20261021T053402Z-2/.
- cargo check --locked --offline --no-default-features --features live,historical:
  exit 0; existing dead-code warnings only.
- Root git diff --check: exit 0 before checkpoint.
- Initial cargo fmt --all from repository root exited 1 (no Cargo.toml), then
  corrected to engine/ and succeeded. No compiler or behavioral test failures.
- SQL/performance opt-in tests and historical-only suite not run; historical
  zero-stream baseline not rechecked. No database or exchange traffic.

Changed: engine/src/live/foundation/resources.rs,
engine/src/live/routing/ordered.rs, engine/src/live/routing/shutdown.rs,
engine/tests/live/router.rs, STATUS.md and this ledger. No Python/historical
execution, dependency/lockfile, ignored configuration or system changes. No push.
Documentation checkpoint is the commit carrying this entry.

Earlier WP-05/06/07/09 gates persist; WP-10 Partial; WP-11–29 pending.
Next authorized executable package: WP-10 completion. Further autonomous
implementation can safely continue. No acceptance or project completion claimed.

## Continuation — native router metric binding (2026-10-21)

Status: Partial. Starting HEAD `42bc459`, branch `codex/wp06-completion`.
Preserved pre-existing untracked `scripts/run-autonomous-codex.sh`. Read campaign
instructions, plan, decisions, status, WP-10/11 roadmap, latest evidence, actual
router/admission/delivery/shutdown and native observation consumers. WP-10 remains
the next executable package; no completed package restarted.

Implementation checkpoint: `5775ada`.

Acceptance ledger:
- Implemented: six finite native gauge IDs with correct count/byte descriptors;
  RouterDrainSnapshot::record_metrics uses the prebound ScopedMetricsPort and
  returns each result independently in a fixed array without allocations/retries.
- Admission occupancy, retained deliveries, tracked obligations, terminal reports
  and escalated reports remain separate overlapping responsibilities. Samples do
  not establish durability, distinct event totals or consumer processing.
- Consumer witness: extended existing production-owner observation test uses the
  actual NativeMetrics registry. Covers zero observations, distinct projection
  fixture values (mapping only), correct units/timestamps, monotonic staleness,
  disabled measurements, replacement generation fencing, missing bindings with
  independent successful sampling, and unchanged authoritative router owners.
- Missing: EventRouterPort production admission composition, concrete runtime and
  reporter integration, scheduled sampling/full router instrumentation and Control
  binding. This API does not autonomously schedule collection. No daemon claimed.
- Existing earlier contract gates remain; no architecture decision was changed.

Verification (engine/ unless marked root):
- cargo test --locked --offline --no-default-features --features live --test
  live_suite router_observation: exit 0, 1 passed, 457 filtered out.
- Root ./scripts/verify-live.sh full: exit 0, all five gates passed. Commands:
  cargo fmt --all -- --check; cargo check --locked --offline --no-default-features
  --features live --lib; cargo test with the same flags --lib (135 passed),
  --test live_suite (456 passed, 2 opt-in ignored), --doc (1 passed).
  Receipts: engine/target/live-verification/20261021T053737Z-2/.
- cargo check --locked --offline --no-default-features --features live,historical:
  exit 0, existing dead-code warnings remain.
- Root git diff --check: exit 0 before implementation checkpoint.
- Two initial working-directory mistakes: creating the new module with a doubled
  engine/ prefix failed without writing it; a root-level cargo fmt invocation
  failed for missing Cargo.toml. Corrected paths and ran final gates above. No
  compiler or behavioral test failures. SQL/performance opt-in tests and historical
  zero-stream baseline not rechecked; no database or exchange traffic.

Changed: engine/src/live/model/observation.rs, engine/src/live/routing/metrics.rs,
engine/src/live/routing/mod.rs, engine/tests/live/router.rs, STATUS.md, this ledger.
No Python/historical execution, dependency/lockfile, ignored configuration or
system changes; no push. Documentation checkpoint is the commit carrying this
entry. Remaining: WP-05/06/07/09 open gates, WP-10 Partial, WP-11–29 pending.
Next authorized executable package: WP-10 completion. Further autonomous work can
safely continue; no package acceptance or project completion claimed.
