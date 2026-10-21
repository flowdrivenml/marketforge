# MarketForge Live — Implementation Status

## Work Package Status

| Package | Scope | Status | Evidence |
|---|---|---|---|
| WP-00 | Establish the actual baseline | Accepted | WP-00A-baseline.md, WP-00B-feature-isolation.md |
| WP-01 | Create the module skeleton and working records | Superseded | — |
| WP-02 | Make shared depth/domain models available safely | Accepted | WP-02A-depth-event-sharing.md, WP-02B-trade-sharing.md |
| WP-03 | Extract compatible storage, arithmetic and UI primitives | Accepted | WP-03A-DWP-03B-parquet-contract.md, WP-03D-shared-parquet-primitives.md, WP-03E-shared-trade-builder.md |
| WP-04 | Finalize dependency and feature wiring | Accepted | WP-04D.md — feature/build verification |
| WP-05 | Implement domain identity, lifecycle and policy types | Partial — reconciliation underway | WP-04A, WP-04A.1–A.3, WP-04B, WP-04B.1 — identity, validation, lifecycle, demand |
| WP-06 | Freeze and implement event/delivery/replay contracts | Partial — basic real C06 witness passes; signed timestamp and fault gates remain | [Independent contracts](evidence/wp06-independent-contracts.md), [Observed C06 mapping and blockers](evidence/wp06-c06-connected.md) |
| WP-07 | Freeze service ports, management and observation surfaces | Contract implementation review pending; C12 adapter decision OPEN | [Contract implementation and witnesses](evidence/wp07-contracts.md) |
| WP-08 | Implement foundations and real native observability | ReadyForReview | [Implementation ledger](evidence/WP-08-implementation.md) |
| WP-09 | Implement the Configuration System | Partial — candidates/DAG/journal verified; semantic compiler and service pending | [Implementation ledger](evidence/WP-09-implementation.md) |
| WP-10 | Implement the canonical event router | Partial — ordered tracked fan-out integrated; production owner pending | [Implementation ledger](evidence/WP-10-implementation.md) |
| WP-11 | Implement source registry and dependency ownership | Pending | — |
| WP-12 | Implement exchange-profile and format compilation | Pending | — |
| WP-13 | Implement WebSocket Connection Manager | Pending | — |
| WP-14 | Implement REST client and shared request scheduling | Pending | — |
| WP-15 | Implement Subscription Manager | Pending | — |
| WP-16 | Implement trade processing and continuity | Pending | — |
| WP-17 | Implement L2 synchronization and processing | Pending | — |
| WP-18 | Implement Recovery Coordinator | Pending | — |
| WP-19 | Verify the complete acquisition slice | Pending | — |
| WP-20 | Implement Live Retention completely | Pending | — |
| WP-21 | Implement PostgreSQL Recording completely | Pending | — |
| WP-22 | Implement Unix market-data publication completely | Pending | — |
| WP-23 | Implement derived streams completely | Pending | — |
| WP-24 | Finalize the production runtime and reconciler | Pending | — |
| WP-25 | Implement Control server and shared Control client | Pending | — |
| WP-26 | Implement the independent Ratatui Live client | Pending | — |
| WP-27 | Verify internal metrics, diagnostics, and observability integration | Pending | No Prometheus/Grafana |
| WP-28 | Verify the complete intended exchange matrix | Pending | — |
| WP-29 | Hardening, reproducibility and release | Pending | — |

## Current Development Position

### Autonomous continuation — 2026-10-21

Latest continuation adds bounded explicit cursor namespace ownership and
`OrderedFanout::prepare_registered`, preserving the existing real tracked
fan-out. Retirement/shutdown fence cloned handles; bounded tombstones prevent
same-namespace frontier resets. Three new deterministic witnesses pass, including
concurrent registration and real destination queue readback. Full verification
exits 0: 130 library / 386 integration (2 opt-in ignored) / 1 doctest, formatting
and Live check; Live+historical compilation also exits 0. Receipts:
`engine/target/live-verification/20261021T035235Z-2/`.
WP-10 remains Partial; production owner, global scheduling, destination workers,
completion/recovery ownership, best-effort policies and snapshots remain. This
runtime-local registry does not claim restart durability. WP-05/06/07/09 open
gates remain. Next authorized work: WP-10 completion; WP-11–29 remain pending.
No SQL/exchange requests, historical execution, Python, dependencies, ignored
configuration or user launcher changes. Further autonomous WP-10 work is safe.
See [WP-10 ledger](evidence/WP-10-implementation.md).


Checkpoint `133b72e` adds WP-10 `OrderedFanout`: the real required-destination capacity,
order permit, obligation activation and recovery-aware queue commitment now
compose through one cancellation/deadline-fenced operation. Dropping a prepared
operation or waiting future releases reservations and the stream permit without
creating obligations. The final deadline check runs inside the registry
commitment fence; partial outcomes retain their payload and advance the frontier.
WP-10 remains Partial: this is a required-sink coordinator, not the production
EventRouterPort service or worker/recovery runtime. Next executable work remains
WP-10 production owner, bounded scheduling and downstream completion integration.
Final verification exits 0: formatting, Live check, 130 library / 383 integration
(2 opt-in ignored) / 1 doctest, and Live+historical compilation. Receipts:
`engine/target/live-verification/20261021T034918Z-2/`. Seven new deterministic
coordinator witnesses pass. Further autonomous WP-10 implementation is safe;
this continuation does not close its remaining acceptance requirements.
Pre-existing untracked `scripts/run-autonomous-codex.sh` was preserved.


Final cross-feature / opt-in receipts are in
`engine/target/live-verification/campaign-final-1792553404735277127/results.json`.
All four commands exit 0: base library check, Live+historical+TUI all-target check,
isolated PostgreSQL witness (1 test), release measurement baseline (1 test).
Historical library retains its independently rechecked 151/1 baseline result.
No production data, Python, historical execution, dependency/lockfile or ignored
exchange configuration changes; no remote push.


Latest verified milestones: `50f1009` (WP-08 ReadyForReview), `ba5ad39`
(WP-09 candidates/DAG and durable journal, Partial). WP-10 recovery-aware fan-out
now commits queues and preserves tracking responsibility, with explicit partial
outcomes, bounded registry and checked stream-order permits. WP-10 remains
Partial: production admit/worker/recovery/snapshot/shutdown composition is not
implemented. Final Live full run exits 0: 130 library / 376 integration / 1 doc;
2 opt-in tests are separate. Source identity validation additionally rejects a
definite derivative-versus-Spot mismatch without resolving OPEN-001 by guess.


Checkpoint `50f1009` completes WP-08 primitives as ReadyForReview. WP-09 now has
verified candidate changes, iterative bounded DAG validation and a real bounded
checksummed local revision journal, including all nine persistence fault points.
Full verification exits 0: 129 library / 371 integration / 1 doctest. WP-09 remains
Partial; authoritative metadata/capability compilation, TOML projection, worker
port and management integration are not certified. Independent WP-10 work may
continue against its explicit WP-06/WP-08 prerequisites.


WP-08 completion verification: full Live exits 0 (123 library, 366 integration,
1 doctest; two opt-in tests run separately). TUI and combined Live+historical
checks pass. Historical library baseline rechecked: 151 passed / 1 unchanged
zero-stream failure. Native metrics/health/diagnostics and deterministic
composition use real public ports. See WP-08 evidence for exact receipts and
release primitive overhead results. No package marked Accepted.


Checkpoints: `6c58c37` (WP-05 checked generations/cursors), `5b97f54`
(WP-06 real isolated PostgreSQL grouped transaction/readback/rollback witness).
WP-08 now has tested clocks, count/byte budgets, bounded task admission, scoped
native metrics, histogram, aggregation, capability health and redacted local
diagnostics. Latest full verification: 113 library, 363 integration, 1 doctest;
all gates exit 0. PostgreSQL opt-in witness separately passed 1/1. WP-08 primitives have since completed their package verification and are
ReadyForReview; see its current evidence ledger. WP-09 candidate/DAG work is
underway independently of the unresolved metadata/capability adapter gates.


Authorized scope is now WP-05 through WP-29, sequentially, one writer, local
commits permitted and no remote pushes. Initial branch `codex/wp06-completion`
was clean. WP-05 reconciliation adds checked advancement/exhaustion and exact
generation fences, plus namespace-aware cursor position comparison. Full Live
verification passes: 95 library, 363 integration, 1 doctest; all five gates exit 0.
See [WP-05 reconciliation](evidence/WP-05-implementation.md).

The isolated PostgreSQL identity was confirmed as database and authenticated
role `marketforge_live_test`. Although SQL CONNECT privilege on `marketforge` is
true, an actual connection with the test role is rejected by pg_hba.conf for both
SSL and non-SSL, establishing the server-side production connection fence. No
production credentials were used and no writes were made. C06 transaction
witnesses can now proceed in the isolated database. Signed timestamp/schema
compatibility and C12 adapter selection remain unresolved; prior reports
describing read-only authorization are historical.


### Accepted Foundations

The following work packages have been formally accepted:

- WP-00 — Baseline and feature isolation.
- WP-02 — Shared depth and trade models.
- WP-03 — Shared storage, arithmetic, and supporting primitives.
- WP-04 — Dependency and Cargo feature wiring.

WP-01 was superseded.

Acceptance of these packages is based on their existing evidence records.

### WP-05 — Domain Identity, Lifecycle and Policy Types

Status: In Progress

Implemented:

- Live source identities.
- Stream identity and cursor types.
- Log epoch and generation identity.
- SourceStreamKey validation.
- Lifecycle models.
- Integrity models.
- Demand ownership models.

Remaining:

- Reconcile implemented types against the authoritative WP-05 acceptance criteria.
- Identify any missing domain and policy contracts.
- Complete required verification.
- Record acceptance evidence.

Do not mark WP-05 Accepted until its formal criteria have been verified.

### WP-06 — Event, Delivery and Replay Contracts

Status: Partial. Independent contracts and the isolated real grouped transaction/
readback/rollback witness pass. Signed timestamp representability and broader
commit-fault/protected-cleanup verification remain open.

The immutable events, typed provenance, checked source cursor acceptance,
restart-safe epoch journal, exact versioned DTOs, protected replay and bounded
replay/follow/book-view cutover contracts remain implemented. The separate
filesystem journal audit and deterministic failure tests are retained.

On 2026-10-21 PostgreSQL connectivity succeeded using local libpq authentication.
All five live_canonical tables exist and are SELECT-accessible. Actual catalog
inspection replaces the former assumption that only historical schema-v9 tables
are available. No credential file was opened by the agent; no DDL or DML ran.

Implemented C06: exact relational row mapping and observed schema checks,
ordered grouped-level identity, defensive canonical decode, transaction write
plans, ambiguous outcome/retry contracts, bounded contiguous committed coverage,
protected reads and cleanup guards. Twelve new contract tests pass. The observed
SQL unsigned timestamp CHECKs cannot represent negative canonical timestamps;
the mapping explicitly rejects those without changing canonical validity.

The former read-only authorization gate is superseded for the isolated test DB.
Basic real transactions, exact fractional readback, duplicate rejection, atomic
rollback and fixture cleanup now pass. Production-driver commit faults and
concurrent protected cleanup remain unverified; no migration option is assumed
approved. The old schema proposal is retained as historical evidence.

Evidence: [independent contracts](evidence/wp06-independent-contracts.md),
[filesystem journal audit](evidence/wp06-epoch-journal-audit.md),
[connected C06 requirements, implementation and approval gates](evidence/wp06-c06-connected.md).

### WP-07 — Service, Management and Observation Contracts

Status: Contract implementation review pending; C12 production adapter decision OPEN.
Not Accepted; production adapter integration is not certified.

Started sequentially after C06 mapping/contract tests passed. Implemented narrow
ports, owner-specific dependency bundles, task/shutdown/update boundaries,
versioned JSON control framing/codec, trusted authorization, bounded idempotency,
operation completion evidence, resolved configuration/revisions/history/restore,
closed metrics/availability/health models, strategy/capability descriptors and
owned snapshots for all five views. Twenty-eight deterministic management,
configuration, strategy, resource and presentation tests pass.

C12 driver/runtime/TLS adapter choice is not inferred from illustrative library
lists or a successful psql connection. No new dependencies were added. The
implemented public ports remain independent of those concrete adapter types;
review/approve the remaining choice before the first real adapter consumer.

Final Live library 93/93, Live integration 363/363, doctest 1/1; combined
historical+Live integration 363/363. Default, historical, Live and combined
Live+historical+TUI all-target checks pass. Historical library remains 151 passed /
1 unchanged baseline failure: historical::merge::resources::tests::rejects_empty_streams.
Historical zero-stream behavior/test and existing WP-10 code were not modified.

Evidence: [contract ledger, constructor table, tests and limitations](evidence/wp07-contracts.md),
[exact final commands/logs](evidence/wp06-c06-wp07-verification/results-final.json),
[exact modified-file manifest](evidence/wp06-c06-wp07-verification/files.json).

### WP-10 — Canonical Event Router

Status: In Progress — Early Implementation

WP-10 implementation began ahead of the formal roadmap sequence.

This work is not evidence that WP-05 through WP-09 have been completed.

Implemented and tested independently:

- RouterRegistry.
- Destination registration and removal.
- Registry revision tracking.
- Destination queue management.
- Event and byte capacity accounting.
- Capacity reservations and ownership transfer.
- Asynchronous admission preparation.
- Required-destination fan-out preparation.
- Queue commitment.
- Complete and partial fan-out outcomes.
- AcknowledgementTracker.
- Delivery obligation preparation and activation.
- Transaction identity and membership validation.
- Transaction abort handling.
- FinalizationJournal.
- TrackingRecoveryCapacity.
- TrackingRecoveryReservation.
- TrackingRecoveryBatch.
- TrackingRecovery.
- Failed acknowledgement retention.
- Deterministic acknowledgement retry.
- Unresolved recovery outcome retention.
- Recovery-aware acknowledgement helper functions.

Existing tests have passed following the latest changes.

Implementation remains incomplete.

Known outstanding work:

- Integrate the repaired operational commit_tracked_with_recovery() into the production owner.
- Retain owned tracking failures and required delivery payloads through real recovery lifetime.
- Consolidate transitional commitment APIs.
- Prevent production routing from bypassing obligation tracking.
- Wire the tested StreamAdmissionOrder into production per-stream admission.
- Implement the canonical production EventRouterPort::admit() path.
- Integrate the router with its production runtime owner.
- Complete end-to-end routing and recovery tests.
- Verify concurrency, cancellation, shutdown, and backpressure behavior.
- Verify all WP-10 acceptance criteria.

Important:

Passing unit and integration tests demonstrates that tested components behave as expected.

It does not establish that the complete router is operational or production-ready.

## Implementation Architecture

### Existing Routing Components

Primary implementation location:

`engine/src/live/routing/`

Relevant modules include:

- router.rs
- queue.rs
- destination.rs
- admission.rs
- acknowledgement.rs
- tracking_recovery.rs

Additional routing-related contracts and ports exist elsewhere under:

`engine/src/live/`

### Routing Lifecycle

The intended production lifecycle is:

1. Receive an EventBatch.
2. Resolve required destinations.
3. Prepare destination capacity.
4. Prepare delivery obligations.
5. Validate registry and transaction identity.
6. Reserve tracking recovery capacity.
7. Activate delivery obligations.
8. Commit destination queues.
9. Apply or retain delivery acknowledgements.
10. Preserve unresolved delivery responsibility.
11. Finalize completed or failed obligations.

The complete lifecycle is not yet connected through one authoritative production entry point.

### Delivery Guarantees

The implementation is designed around:

- No silent loss of required market events.
- Bounded resource consumption.
- Explicit backpressure.
- Destination-specific delivery obligations.
- Independent tracking of partial fan-out outcomes.
- Ownership-preserving capacity reservations.
- Explicit acknowledgement progression.
- Recovery responsibility for failed tracking updates.
- Per-stream ordering.

Not all guarantees have been verified end to end.

### Persistence Boundaries

PostgreSQL is reserved for optional market-data recording.

Application logs and operational event history remain local.

TrackingRecovery and FinalizationJournal currently provide in-process tracking and recovery mechanisms.

They must not be described as crash-durable without a verified persistence implementation.

## Implementation Workflow

### Development Responsibilities

ChatGPT:

- Architecture interpretation.
- Implementation planning.
- Contract and invariant review.
- Acceptance criteria analysis.
- Independent code review.

Codex:

- Repository inspection.
- Rust implementation.
- Targeted refactoring.
- Compilation and test execution.
- Compiler-error resolution.
- Missing acceptance-test implementation.
- Reporting evidence and blockers.

Qwen/OpenCode:

- Optional repository inspection.
- Exact source extraction.
- Small mechanical modifications.

The user:

- Reviews implementation results.
- Approves architectural changes.
- Controls Git commits and merges.
- Confirms work-package acceptance.

### Autonomous Coding Policy

Codex may autonomously:

- Inspect relevant repository files.
- Implement approved work-package requirements.
- Modify files within the authorized scope.
- Run relevant formatting, compilation, and tests.
- Fix compilation errors.
- Fix failing tests without weakening assertions.
- Add tests required by acceptance criteria.

Codex must not:

- Change accepted architectural decisions without approval.
- Redesign unrelated modules.
- Remove required fault-tolerance guarantees.
- Introduce placeholder implementations as completed functionality.
- Delete or weaken tests merely to obtain a passing result.
- Modify historical behavior without explicit authorization.
- Run destructive Git operations.
- Commit, merge, or push without explicit authorization.
- Mark work packages Accepted without acceptance evidence.

Prefer completing the functionality already specified for a work package rather than repeatedly revisiting the same module to add known requirements.

### Verification Policy

Use the existing Cargo feature boundaries.

The Live test commands currently used are:

```bash
cargo test --locked --offline --no-default-features --features live --lib
