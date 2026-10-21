# MarketForge Live — Autonomous Implementation Plan

**Status:** Active execution policy
**Execution model:** One Codex agent, sequential work packages
**Project:** MarketForge Live
**Implementation language:** Rust
**Target:** Complete the approved MarketForge Live v1 implementation

---

## 1. Mission 
Implement MarketForge Live according to the accepted architecture,
implementation roadmap, and recorded decisions.

The goal is a fully integrated, continuously operating Rust service
that provides:

- Multi-exchange trade and L2 acquisition.
- Generic protocol decoding and canonical normalization.
- Connection, subscription, and stream lifecycle management.
- Integrity validation and recovery.
- Canonical event distribution.
- Unix-domain socket publication.
- RAM/Parquet retention and cursor-based replay.
- Optional PostgreSQL recording.
- Derived stream merging and currency conversion.
- Native metrics, health, and operational diagnostics.
- Versioned runtime configuration and control.
- Independent Ratatui management interface.
- Deterministic testing and fault tolerance.

Complete the approved v1 requirements rather than producing partial
demonstrations or placeholder implementations.

Do not implement Python orchestration, historical processing,
Bootstrap orchestration, feature engineering, trading strategies,
or unrelated systems.

---

## 2. Authoritative Sources

Read and follow:

1. `AGENTS.md`
2. `.planning/live/decisions.md`
3. `docs/live/17 — Implementation Roadmap.md`
4. Relevant subsystem specifications under `docs/live/`
5. `.planning/live/STATUS.md`
6. Current Rust implementation and tests
7. Relevant implementation evidence

Existing code establishes what is implemented.

Documentation and approved decisions establish what must be implemented.

A previous status report is not proof that its corresponding
implementation still satisfies the requirements.

Do not treat historical evidence as a replacement for inspecting
the current source.

If a document is missing, locate the authoritative equivalent.
Do not fabricate its contents.

---

## 3. Standing Execution Authorization

The user authorizes autonomous implementation of the remaining
MarketForge Live work packages in the sequence defined below.

Codex may independently:

- Inspect source code and documentation.
- Design implementation details within approved contracts.
- Implement production Rust functionality.
- Create and modify tests.
- Add justified dependencies.
- Refactor existing Live components when required.
- Integrate dependent subsystems.
- Run compilation, formatting, tests, and benchmarks.
- Diagnose and repair implementation failures.
- Create local Git branches, commits, and checkpoints.
- Maintain implementation status and evidence.
- Proceed to subsequent authorized work packages.

Do not request user approval for ordinary implementation choices,
compiler errors, borrow-checker problems, test failures, or
routine integration work.

Architectural changes require approval when they materially alter
an accepted contract, financial semantics, ownership, persistence
guarantee, event identity, or external protocol.

Standing authorization does not include:

- Remote Git pushes or merging into protected branches.
- Changes to Python or historical Rust execution.
- Destructive operations against the existing database.
- Access to unapproved production credentials or private data.
- Destructive modification of user-owned files.
- Unapproved real-exchange traffic.
- Installation or modification of system-wide infrastructure.

Use available permissions; do not bypass sandbox restrictions.

---

## 4. Current Implementation Baseline

The repository already contains substantial MarketForge Live work.

### Current WP-10 disposition

**ReadyForReview — Not Accepted**, implementation/audit checkpoint `d631b4d`.
The authoritative component evidence is
[evidence/WP-10-implementation.md](evidence/WP-10-implementation.md).
Its external integrations remain unverified work of their original owning
packages below. Do not reopen WP-10 merely because a concrete sink, daemon or
Control server does not exist; reopen only for a demonstrated router-owned defect.
Only the user/integration owner may mark Accepted.

### Historical baseline (superseded by STATUS.md and current evidence)

Previously implemented or partially implemented:

- WP-00 through WP-04: foundational work.
- WP-05: domain models and related contracts.
- WP-06: event, delivery, recording, and replay contracts.
- WP-07: management and observation contracts.
- WP-10: early canonical-router infrastructure.

WP-06 and WP-07 have been undergoing active Codex implementation.

The exact completion state must be determined from the current
working tree, tests, decisions, evidence, and STATUS.md.

This section is historical orientation, not an acceptance claim.

### Initial reconciliation

Before executing the next package:

1. Inspect the current Git branch and working tree.
2. Identify work already in progress.
3. Read STATUS.md and the latest package evidence.
4. Verify the actual state of WP-05, WP-06, and WP-07.
5. Identify incomplete requirements or blocked prerequisites.
6. Preserve existing WP-10 implementation.
7. Determine the first executable work package.

Do not restart completed work from scratch.

Do not duplicate existing implementations.

Do not overwrite an active Codex session's work.

If another writer is active, do not start a competing writer.

---

## 5. Authorized Work-Package Sequence

The authoritative requirements and acceptance criteria are defined
in Document 17.

Execute in dependency order.

### Initial completion gates

| Package | Responsibility |
|---------|----------------|
| WP-05 | Verify and complete domain contracts |
| WP-06 | Complete event, delivery, recording, and replay contracts |
| WP-07 | Complete management and observation contracts |

Preserve completed functionality.

Resolve outstanding requirements before treating their dependent
production implementations as verified.

### Production implementation

| Order | Package | Responsibility |
|-------|---------|----------------|
| 08 | Foundations and native observability | Clocks, cancellation, budgets, metrics, health |
| 09 | Configuration system | Compilation, validation, revisions, persistence |
| 10 | Canonical event router | Ordered admission, delivery, acknowledgements |
| 11 | Source registry | Deduplication, demand ownership, lifecycle |
| 12 | Exchange protocols | Profiles, decoders, normalization plans |
| 13 | WebSocket manager | Connections, supervision, heartbeats |
| 14 | REST infrastructure | Requests, quotas, scheduling, retries |
| 15 | Subscription manager | Activation, acknowledgements, restoration |
| 16 | Trade processing | Normalization, deduplication, continuity |
| 17 | L2 processing | Book synchronization, sequencing, recovery |
| 18 | Recovery coordination | Incidents, recovery scheduling, escalation |
| 19 | Acquisition integration | Complete simulated acquisition pipeline |
| 20 | Live retention | RAM/Parquet, protection, replay, recovery |
| 21 | PostgreSQL recording | Transactions, idempotency, coverage, reads |
| 22 | Unix publisher | Binary protocol, sessions, replay/handoff |
| 23 | Derived streams | Merging, ordering, currency conversion |
| 24 | Runtime composition | Daemon, supervision, reconciliation |
| 25 | Control server/client | Versioned management protocol |
| 26 | Ratatui client | Five management views |
| 27 | Native observability integration | Internal metrics, diagnostics, health, snapshots, and read-only API integration |
| 28 | Exchange certification | Verification of supported exchange families |
| 29 | Hardening and release | Fault testing, performance, deployment |

The sequence is authorized.

However, authorization to attempt a package is not evidence that
its prerequisites are satisfied.

Do not bypass unresolved foundational contracts.

An already partially implemented package must be inspected and
completed rather than replaced unnecessarily.

---

### WP-10 consumer integration obligations

This is the action ledger for the seven external boundaries identified by the
WP-10 audit. It supplements, and never replaces or weakens, each complete
Document 17 package entry. Source/owner/interface/test traceability and existing
witnesses are in
[evidence/wp10-cross-package-reconciliation.md](evidence/wp10-cross-package-reconciliation.md).
All integrations below remain outstanding; an available interface is not an
implemented consumer. Working Rust code stays with its current owner/module.

#### WP-09 — Configuration acceptance

- **Behavior/reuse:** Complete validated, durably accepted desired revisions and
  impact/boundary plans through `ConfigurationCompilerPort`,
  `ConfigurationStorePort` and `ConfigurationApplyPort`. Reuse WP-10's registry
  revision/instance fencing and measurement boundary as application targets
  **through the WP-24 reconciler**, not direct pre-persistence router mutation.
- **Remaining/prerequisites:** Finish the semantic compiler, static-spec loading,
  revision service and bounded worker integration already missing in WP-09. Use
  WP-05/06/07 contracts, verified metadata/capability inputs and WP-08 foundations;
  keep unresolved C01/economic validation fail-closed. A candidate/local journal
  alone is not a complete configuration service.
- **Acceptance/boundary:** Test invalid proposals, failed/uncertain persistence,
  conflicting revisions and restore-as-new-revision with zero premature router
  effects; verify affected-resource plans and admission/measurement boundaries
  through a deterministic apply-port consumer. WP-09 owns acceptance/plan output;
  WP-24 owns actual destination execution and applied/adopted observations.

#### WP-18 — Recovery coordination

- **Behavior/reuse:** Consume typed delivery failures without reassigning cursors
  or guessing ambiguous obligations. Reuse `IngressReporter`,
  `FinalizationReporter`, charged `IngressRecord`/`RetainedDelivery`/
  `FinalizedDelivery`/`WorkerOutcome` and `RouterRuntime` transfer, restore and
  tracking-reconciliation methods. Keep `RecoveryPort` orchestration distinct
  from ownership of those payloads. WP-24 binds/supervises report consumers;
  WP-25 authorizes supported manual actions, not automatic integrity decisions.
- **Remaining/prerequisites:** Implement incidents, bounded scheduling/retries,
  generation/demand fences, restored-guarantee results and persistent-gap
  coordination using WP-05/06/07/08/10 and the WP-11/14/16/17 execution ports.
  WP-20/21 own their segment/transaction evidence; later bindings provide actual
  storage recovery. Operational history remains local (DEC-031).
- **Acceptance/boundary:** Exercise real router-to-recovery transfers, full and
  foreign-owner rejection with charges intact, post-deadline restoration,
  ambiguous outcomes, retry/escalation and partial/unrecoverable coverage.
  Reuse the existing return/receipt witnesses. Component tests may use explicit
  backend ports; actual storage repair requires later real-adapter evidence.
  Failure acknowledgement/current readiness must never imply restored history.

#### WP-20 — Live retention

- **Behavior/reuse:** Bind a retention adapter behind `EventDestinationPort`
  using the immutable `EventBatch`, `DestinationKind::Retention` and exact
  `DestinationCompletion` cursor/stage correlation via
  `RouterRuntime::apply_completion`. Keep RAM acceptance, segment commitment
  and configured crash durability separate. Reuse router queues/permits rather
  than introducing a competing router or cursor allocator.
- **Remaining/prerequisites:** Complete RAM/spill/manifest/protection/replay,
  restart and coverage behavior in Document 17 WP-20. Consume WP-05/06/07/08/10
  contracts, shared Arrow/Parquet primitives and the approved epoch/checkpoint
  contract; bind `SegmentStoragePort`/`RetentionReaderPort`/`RetentionControlPort`.
- **Acceptance/boundary:** Feed the real router into the real retention owner;
  verify grouped-event identity on Parquet/replay, full buffers/disk, faults at
  file/manifest boundaries, protected readers, restart and replay-to-live handoff.
  Withhold Committed/Durable on failed persistence and retain required ownership
  through deadline shutdown. Retention owns storage evidence, WP-18 recovery
  orchestration; neither database recording nor the final daemon is a prerequisite
  for retention component verification.

#### WP-21 — PostgreSQL recording

- **Behavior/reuse:** Implement the recorder behind `EventDestinationPort` and
  `RecordingBackendPort`; return separately correlated `DestinationCompletion`
  through `RouterRuntime::apply_completion`. Reuse `EventBatch` and existing
  explicit policy/stage validation. Register recording demand through WP-11;
  WP-24 applies accepted enable/disable boundaries and registers the adapter.
- **Remaining/prerequisites:** Complete real SQL mapping/transactions, batching,
  ambiguous-commit reconciliation, contiguous coverage, protected reads/cleanup
  and retention catch-up. Depend on WP-05/06/07/10, configuration policy,
  WP-11 demand, WP-18 recovery and WP-20 retention interfaces. Resolve the C06
  mapping/timestamp/fault and C12 adapter gates as applicable; any migration still
  needs its specific authorization. Writable tests require verified isolated
  database/role permissions that cannot mutate the real `marketforge` database.
- **Acceptance/boundary:** Exercise the composed router/recorder against isolated
  real PostgreSQL: exact values, grouped-depth atomicity, duplicate/out-of-order
  batches, lost commit acknowledgement, restart/outage/catch-up and protected
  cleanup. Committed alone cannot settle Durable; unknown commits stay unknown.
  Test disabled recording creates no connection and leaves publication working.
  Mock contracts cannot complete SQL correctness or coverage acceptance.

#### WP-22 — Unix publication

- **Behavior/reuse:** Bind the publisher behind `EventDestinationPort` with
  Publication policy and correlated Delivered evidence; consume the existing
  router optional-outcome/retirement/queue contracts. The publisher owns its
  session/subscriber routing and bounded queues; that does not duplicate the
  canonical router or create independent acquisition ownership.
- **Remaining/prerequisites:** Complete binary framing, negotiation, local socket
  lifecycle, session-generation fencing, source demand, slow-client policy,
  replay/book handoff and finite publication shutdown. Use WP-05/06/07/10/11/17
  and WP-20 replay/protection/book-cut interfaces; PostgreSQL is optional.
- **Acceptance/boundary:** Drive the real router into local Unix sessions with
  fragmented/invalid frames, slow/disconnected readers and healthy peer/storage
  sinks; verify bounds, stale completions, grouped L2 identity, protected replay
  and snapshot-to-live handoff. Report precise publication gaps and unfinished
  shutdown obligations. Socket write/Delivered is not consumer processing.

#### WP-24 — Runtime composition and reconciliation

- **Behavior/reuse:** Construct one validated `RouterRuntime` using
  `RouterResources::validate` and `RouterServices`; pass `producer()` as the
  owned `EventRouterPort`, schedule bounded `drive` turns, route actual adapter
  completions to `apply_completion`, and drive `shutdown` to an explicit result.
  Reuse native `snapshot`, namespace registration/retirement, destination
  register/unregister, metric scheduling and charged recovery/optional transfers.
- **Remaining/prerequisites:** Bind real bounded clocks/timers/tasks, local
  reporting/operational acceptance consumers, metrics and available sinks;
  implement startup, signals and generation/revision-safe configuration execution
  behind `ConfigurationApplyPort` and lifecycle ports. Consume WP-09 committed
  plans plus the earlier owner contracts/implementations through WP-23; preserve
  independent transient demands and report partial desired/applied/adopted state.
- **Acceptance/boundary:** Exercise headless startup, restart reconstruction,
  accepted-versus-applied revisions, replacement races, owner/report-consumer
  failure, bounded retry scheduling and finite shutdown with real owners/sinks
  where available. Disabled recording creates no DB connection; Control clients
  need not be connected for local progress. A dropped/disabled diagnostic sample
  is not accepted market-data recovery responsibility. Keep transferred charges
  until accepted by their next owner; map DeadlineReached/unfinished work into
  the service ShutdownReport rather than claiming complete merely on task join.
  This package owns real daemon wiring, not storage transactions or a second router.

#### WP-25 — Control server/client

- **Behavior/reuse:** Dispatch approved `ManagementPort`/`ManagementClientPort`
  commands to authoritative owners; project `RouterRuntime::snapshot`/native
  metrics without exposing mutable locks. Supported retry/resolution operations
  delegate to `retry_ingress`, `retry_terminal_report`,
  `prepare_terminal_resolution` and `commit_terminal_resolution`, using original
  journal/sink tokens. Native methods do not automatically become wire commands.
- **Remaining/prerequisites:** Complete bounded JSON transport/correlation,
  authorization, audit, operations/idempotency and fresh snapshots using WP-07
  schemas and WP-24/09/18 authoritative owner services. No control handler owns
  desired configuration, recovery policy, delivery tracking or task lifecycle.
- **Acceptance/boundary:** Test read-only denial/no effects, stale revisions,
  disconnect-after-acceptance retries, foreign/stale resolution receipts,
  operation objectives, snapshot freshness and slow clients alongside ongoing
  market delivery. Any unsupported external command returns an explicit error.
  Accepted failure resolution preserves the original failure and never certifies
  payload repair. Control readiness is a WP-25 gate, not a WP-10 prerequisite.

### Sequence and completion boundary check

Document 17 order is unchanged. WP-09 produces committed plans through the
apply port; it need not import a concrete WP-10/WP-24 owner to validate them.
WP-18 may test later storage backends through their existing ports before
WP-20/21 implement them; it cannot certify real storage recovery from those tests.
WP-20/21/22 test their actual adapters against the existing router without waiting
for the final daemon. WP-24 binds the concrete owners, then WP-25 adds management
transport. Composition witnesses grow incrementally under Document 17 §13.2;
WP-19's collector destinations are intentional and do not replace WP-20/21/22.
No backward requirement for the complete daemon/Control server is imposed on
WP-10 or its earlier consumers. Existing foundational/capability gates remain;
no package was reordered or declared accepted by this reconciliation.
The pre-existing WP-27 exporter-title inconsistency is recorded in the report;
DEC-032 governs and no external monitoring requirement is added here.

---

## 6. Autonomous Work-Package Procedure

For every work package, perform the following procedure.

### Phase A — Inspect

Read:

- The package definition in Document 17.
- Relevant requirements from Documents 00–16.
- Accepted architectural decisions.
- Existing implementation.
- Existing tests.
- Downstream consumer requirements.

Identify:

1. Implemented requirements.
2. Missing requirements.
3. Incomplete integrations.
4. Existing limitations.
5. Required dependencies.
6. Acceptance criteria.
7. Genuine architectural blockers.

Avoid broad repository-wide analysis when focused inspection is
sufficient.

### Phase B — Plan

Create a concise internal implementation plan.

Identify the authoritative owner of mutable state.

Determine the necessary interfaces, errors, resources,
cancellation behavior, observability, and integration points.

Examine downstream consumers before freezing public interfaces.

Do not implement hypothetical future requirements.

Do include all currently approved v1 requirements.

### Phase C — Implement

Implement production behavior.

Requirements include, where applicable:

- Correct domain behavior.
- Validation and error handling.
- Resource limits and accounting.
- Cancellation and shutdown.
- Retry and recovery semantics.
- Generation and revision fencing.
- Configuration integration.
- Metrics and health observations.
- Operational diagnostics.
- State persistence.
- Replay and continuity guarantees.
- Integration with existing owners.

Avoid temporary production substitutes.

A fake adapter may support deterministic testing, but it must not
silently replace required production functionality.

### Phase D — Test

Add deterministic tests covering:

- Normal operation.
- Invalid inputs.
- Boundary conditions.
- Failure handling.
- Resource exhaustion.
- Cancellation.
- Concurrent operations.
- Recovery and restart.
- Relevant consumer integration.
- Serialization compatibility.
- Historical compatibility when shared contracts are affected.

Use actual production logic in tests.

Do not duplicate algorithms solely to generate expected results.

Do not weaken existing assertions to obtain a passing result.

### Phase E — Verify

Run relevant checks.

From `engine/`, the standard Live verification targets are:

    cargo fmt --check

    cargo check --locked --offline \
      --no-default-features --features live

    cargo test --locked --offline \
      --no-default-features --features live --lib

    cargo test --locked --offline \
      --no-default-features --features live \
      --test live_suite

    cargo test --locked --offline \
      --no-default-features --features live --doc

Include additional feature combinations and subsystem-specific
integration tests as required.

Offline mode is preferred when dependencies are already cached.
If an authorized dependency change requires network resolution,
record the reason and preserve reproducible dependency versions.

Do not claim test success from a command executing zero relevant
tests.

Capture actual command exit codes.

Distinguish existing unrelated failures from regressions introduced
by the current implementation.

### Phase F — Integrate

Verify that the component works through its real interfaces.

Use deterministic test implementations for dependencies that
have not yet been implemented.

Do not replace actual component behavior with a demonstration
implementation merely to pass integration tests.

Inspect effects on downstream consumers.

Resolve ordinary integration defects independently.

### Phase G — Checkpoint

After meaningful verification:

1. Inspect the Git diff.
2. Check for unintended modifications.
3. Stage relevant implementation files.
4. Create a descriptive local commit.
5. Record the checkpoint identifier.
6. Update implementation evidence.

Commits are authorized on local development branches.

Do not push automatically.

A Git commit is a recovery checkpoint, not formal acceptance.

### Phase H — Report and Continue

Update:

    .planning/live/STATUS.md

Create or update one concise package evidence report.

Record:

- Implemented requirements.
- Relevant changed files.
- Tests executed and results.
- Integration evidence.
- Remaining limitations.
- Architectural blockers.
- Git checkpoint.
- Recommended completion status.

Allowed implementation statuses:

- InProgress
- ReadyForReview
- Partial
- Blocked

Only the user or designated integration owner may mark a package
Accepted.

After reporting, continue to the next authorized package if its
technical prerequisites are satisfied.

Formal user review need not interrupt independent, authorized
implementation when the next package can proceed safely.

---

## 7. Dependency and Integration Policy

Implement components using the already approved architecture.

The preferred order is:

    Domain contracts
         |
         v
    Infrastructure ports
         |
         v
    Concrete component implementations
         |
         v
    Runtime composition
         |
         v
    External clients

A component may use a deterministic fake implementation of a
not-yet-implemented dependency.

The fake must implement the same authoritative contract.

When a real dependency becomes available, integrate it through
that contract.

If integration reveals a defect in an earlier component:

1. Identify the failing invariant.
2. Add a regression test.
3. Make the smallest correct change.
4. Verify affected consumers.
5. Document the reason for reopening the component.

Do not redesign completed components merely for stylistic reasons.

Do not introduce a second authoritative owner for the same state.

---

## 8. Architecture and Decision Handling

Codex may independently decide ordinary implementation details:

- Private Rust structure layout.
- Local algorithm organization.
- Error propagation mechanics.
- Internal helper abstractions.
- Test arrangement.
- Appropriate standard-library usage.
- Performance improvements preserving semantics.

Codex must not independently redefine:

- Canonical financial values.
- Event identity or grouping.
- Generation and epoch semantics.
- Ownership boundaries.
- Accepted delivery guarantees.
- Persistence durability.
- Recovery correctness requirements.
- External wire compatibility.
- Supported exchange economic semantics.
- Approved product scope.

When an implementation-critical conflict is found:

1. Identify the conflicting requirements.
2. Quote the relevant authoritative passages.
3. Explain the behavioral consequence.
4. Describe the affected components.
5. Propose one or more solutions.
6. Record the blocker in STATUS.md.

Do not silently choose a conflicting architectural interpretation.

Continue independent authorized work where possible.

Do not repeatedly ask for approval for questions already resolved
in decisions.md or the implementation roadmap.

---

## 9. Handling Failures Without Supervision

### Compiler or test failure

Investigate and fix independently.

Do not abandon work after one failed command.

### Repeated identical failure

Avoid endlessly repeating substantially identical commands.

Change diagnostic strategy.

Inspect source, assumptions, dependencies, and failure output.

If progress genuinely requires external intervention, record the
blocker and continue independent authorized work.

### Missing dependency

Determine whether an approved dependency already exists.

If a new dependency is justified, add it reproducibly within
the permitted Rust scope.

Do not introduce redundant libraries or overlapping owners.

### Missing external infrastructure

Do not fabricate successful integration results.

Use deterministic mocks where appropriate.

Mark actual external verification unavailable.

Continue work that does not depend on the missing infrastructure.

### Unavailable exchange capability

Do not invent protocol fields, quantity units, API limits,
authentication behavior, or sequencing rules.

Record the unverified capability.

Fail configuration validation for capabilities that cannot be
implemented safely.

Continue with independently verified capabilities.

### Exhausted resources or permissions

Preserve current work and evidence where possible.

Do not bypass sandbox restrictions, disable protections, or
perform unsafe cleanup.

Record the precise blocker.

---

## 10. PostgreSQL Rules

The existing PostgreSQL database named `marketforge` contains
real project data and is not disposable.

The approved canonical recording direction uses the additive
`live_canonical` schema without destroying the legacy schema.

Codex may inspect the existing database read-only when authorized.

Do not:

- Drop existing schemas.
- Truncate existing tables.
- Delete existing records.
- Modify historical schema ownership.
- Run destructive migrations.
- Treat production data as disposable test fixtures.

Writable PostgreSQL integration tests must use an explicitly
authorized isolated test database with appropriately restricted
credentials.

Before executing such tests, verify that the active connection
targets the authorized test database.

Tests must not rely solely on a database name supplied by
untrusted configuration.

If isolated writable access is unavailable:

1. Complete the independent recording logic and contract tests.
2. Record which database integration tests remain blocked.
3. Do not claim full PostgreSQL transaction verification.
4. Continue other authorized work packages.

The production PostgreSQL recorder belongs to WP-21.

Do not implement competing recorder ownership in earlier packages.

---

## 11. Exchange and Network Testing

Ordinary correctness tests must use deterministic fixtures and
controlled fake transports.

Actual exchange API behavior must be verified against current
primary exchange documentation before certification.

Real-exchange smoke testing requires explicit authorization.

Do not perform unrestricted network requests, high-volume
subscriptions, or potentially rate-limited recovery operations
merely to demonstrate success.

The intended exchange matrix includes:

- Binance: Spot, Linear, Inverse.
- Bybit: Spot, Linear, Inverse.
- OKX: Spot, Linear, Inverse.
- Bitget: Spot, Linear, Inverse.
- Gate.io: Spot, Linear, Inverse.

Trade and L2 capabilities must be verified independently.

Unsupported or unverified combinations must remain visible.

Do not reduce the intended v1 scope without approval.

---

## 12. Git Checkpoint Strategy

Maintain a recoverable local implementation history.

Before beginning a package:

    Inspect branch
    Inspect dirty state
    Inspect untracked files
    Identify previous checkpoints

During implementation:

    Make focused changes
    Run relevant tests
    Create meaningful local checkpoints

At package completion:

    Verify scope
    Verify tests
    Record commit identifier
    Update evidence
    Continue

Codex may autonomously manage its own local development branches,
commits, merges, cherry-picks, and unshared history according to
AGENTS.md.

Never destroy unrelated work.

Never push to a remote without explicit authorization.

Avoid including credentials, private database contents, temporary
build logs, or large generated artifacts in commits.

---

## 13. Evidence and Documentation Policy

Documentation should make implementation easier to understand,
not become a substitute for implementation.

Produce one authoritative implementation report per package.

Prefer:

    .planning/live/evidence/WP-08-implementation.md
    .planning/live/evidence/WP-09-implementation.md
    .planning/live/evidence/WP-10-implementation.md

Existing evidence filenames may be retained when already in use.

Do not create dozens of near-identical reports for routine
compiler fixes.

Keep full diagnostic logs outside the compact evidence index
unless they are necessary to reproduce a particular failure.

Every package report should identify:

- Requirements satisfied.
- Code implementing the requirements.
- Tests proving the behavior.
- Important failure cases.
- Integration verification.
- Remaining blockers.
- Commit/checkpoint identifier.

Maintain STATUS.md as the current progress ledger.

Do not mark a package complete merely because its source files
exist or its implementation compiles.

---

## 14. Performance and Quality

MarketForge Live is a high-throughput market-data system.

Prioritize:

1. Correct canonical semantics.
2. Explicit ownership and ordering.
3. Bounded resource consumption.
4. Failure isolation and recovery.
5. Predictable latency.
6. Efficient memory and CPU usage.

Avoid unnecessary hot-path:

- Allocations.
- Serialization.
- Configuration parsing.
- Dynamic lookups.
- Global locks.
- Synchronous filesystem operations.
- Database queries.
- Excessive dynamic dispatch.

Do not prematurely optimize at the expense of correctness.

For performance-critical components, measure relevant behavior
and retain reproducible benchmark inputs.

Do not invent performance claims without measurements.

---

## 15. Unattended Execution Rules

The user may leave Codex executing for extended periods without
interactive supervision.

Operate independently within the authorized scope.

Do not stop merely because:

- A task requires multiple implementation steps.
- Compiler errors occur.
- Additional tests are needed.
- Integration exposes ordinary defects.
- A work package requires many files.
- The conversation or execution session becomes lengthy.

Use the repository's persistent planning files to preserve
the state required for continuation.

If context is compacted or a new session begins, reconstruct
the task from current repository evidence rather than assuming
that prior conversational state was retained.

At meaningful milestones, record progress in STATUS.md.

Prefer completing coherent implementation work to generating
frequent progress documentation.

Do not perform unbounded retry loops.

Do not conceal unresolved failures.

Do not claim completion when critical verification is unavailable.

---

## 16. Stop Conditions

Stop the affected work when:

1. An unresolved architectural decision prevents correct implementation.
2. Continuing would require destructive or unauthorized operations.
3. Required financial or protocol semantics cannot be verified.
4. A shared contract would need an unapproved incompatible change.
5. Required infrastructure is unavailable and no independent work remains.
6. Persistent failures cannot be resolved safely.
7. The authorized implementation sequence has been exhausted.

A localized blocker does not automatically stop the entire
development campaign.

Continue other authorized work only when its prerequisites
are genuinely independent.

Never bypass a required acceptance or safety guarantee solely
to continue the schedule.

---

## 17. Special Gates for WP-28 and WP-29

WP-28 may perform offline capability verification, fixture testing,
configuration validation, and documentation-based protocol research.

Real-exchange certification requires separately authorized smoke
tests and suitable verified evidence.

WP-29 may perform deterministic fault testing, profiling,
benchmarks, and deployment preparation.

Do not publish releases, deploy services, install system-wide
daemons, alter external infrastructure, or claim production
certification without explicit authorization.

If release acceptance requires unavailable external verification,
record the outstanding gate truthfully.

---

## 18. Final Objective

The implementation campaign succeeds when the approved
MarketForge Live v1 functionality has been implemented,
integrated, tested, and documented.

The final deliverable should include:

- Production Rust Live implementation.
- Working headless daemon.
- Working independent Ratatui management interface.
- Supported exchange integrations.
- Canonical event publication.
- Recovery and integrity handling.
- Optional PostgreSQL recording.
- RAM/Parquet retention and replay.
- Derived streams and currency conversion.
- Runtime configuration and management.
- Native metrics and operational health.
- Deterministic integration and fault tests.
- Performance verification.
- Reproducible build and deployment instructions.
- A clear record of unsupported or unverified capabilities.

Codex reports implementation readiness.

The user approves final acceptance.

**Implement completely. Verify rigorously. Preserve architectural
decisions. Maintain recoverable progress. Continue autonomously
while safe, authorized work remains.**
