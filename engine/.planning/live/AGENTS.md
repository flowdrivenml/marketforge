# MarketForge — Repository Instructions for Codex

## Mission and scope

Implement **MarketForge Live**, the independently operating Rust market-data service, against its approved v1 architecture. Favor correct, integrated, tested functionality over generated line counts, scaffolds, or passing tests that do not exercise required behavior.

- Main Rust crate: `engine/` (`marketforge_engine`); Live implementation: `engine/src/live/`.
- The historical engine, Python orchestration, Bootstrap application, and existing historical workflows are separate systems. **Do not modify their code or behavior without explicit task authorization.** Reuse shared primitives through existing approved interfaces.
- One writing agent works on the checkout at a time. Execute authorized work packages **sequentially**. An assignment to one package does not authorize later packages unless an execution plan or user explicitly does so.
- Inspect actual source and current Git state before edits. Many Live files are scaffolds; others already contain substantial working code. Neither a file's existence nor old evidence proves its current implementation status.

## Authority and navigation

1. `.planning/live/decisions.md`: accepted architectural decisions and recorded resolutions.
2. `docs/live/17 — Implementation Roadmap.md`: work-package scope, prerequisites, execution rules, acceptance criteria.
3. `docs/live/00 — Master Plan.md` through `16 — External Libraries and Code Reuse.md`: detailed subsystem requirements, interpreted using the roadmap's later, more specific resolutions.
4. Current Rust code, tests, and observable database schema: authoritative **evidence of what exists**, not permission to weaken accepted requirements.
5. `.planning/live/STATUS.md` and current evidence: progress and verification claims; confirm against code and actual test receipts.

When sources materially disagree about identity, exact financial values, ownership, durability, cancellation, persistence, or wire compatibility, record the conflict and **block only the affected requirement** pending an approved decision. Continue genuinely independent, authorized work. Do not silently invent a policy, rewrite accepted architecture, or declare a blocked package accepted. Current approved canonical PostgreSQL direction is an additive `live_canonical` schema alongside legacy tables; its precise write, mapping, and coverage guarantees still require verification.

Read only the document sections relevant to the current work package and its actual consumers. Do not inject the entire architecture collection into every implementation task.

## Non-negotiable architectural invariants

- Canonical trade, L2, exact-decimal, instrument, normalization, `BookStore`, and shared Arrow/Parquet semantics remain authoritative. Do not duplicate or narrow financial types. Do not fabricate unavailable native fields, economic metadata, sequence gaps, timestamps, or provenance.
- A canonical event cursor is `(StreamId, LogEpoch, offset)`. Generations for connection, subscription, book, derived output, and log namespace are independent; stale operations cannot mutate newer owners. An accepted logical L2 group has **one** cursor, with its ordered levels and original initialization/change boundary.
- One authoritative logical owner per mutable resource. Equivalent acquisitions share a source; independent demand registrations cannot be silently removed. Source-runtime activity, connection state, subscription state, integrity, readiness, recovery, historical coverage, and health are distinct.
- Preserve required per-stream ordering. Every queue/buffer has bounded event/byte resources, cancellation/deadline behavior, and a defined overflow policy. Required market events and delivery obligations cannot silently disappear. Report partial outcomes and retain recovery responsibility.
- Distinguish accepted, queued, transmitted, committed, durable, and consumer-processed stages. A queue enqueue, socket write, WebSocket reconnect, or current valid book is **not** proof of durability, end-consumer processing, or recovered historical coverage.
- Configuration is validated and durably accepted before runtime effects; desired versus applied/actual revisions remain distinguishable. Control and market-data Unix sockets are separate. Ratatui is an independent client and must not own the daemon or internal mutable state.
- Metrics and operational incidents are native, bounded, and observational; they must never decide market-data integrity. Logs and operational history remain local. PostgreSQL recording is optional. The Prometheus exporter, if implemented, is optional and disabled by default; no external monitoring stack is required.
- Bootstrap orchestration, trading, feature generation, historical job processing, and Python refactoring are outside MarketForge Live's initial scope.

## Work-package execution loop

For each **authorized** package:

1. Read its roadmap entry, relevant architecture sections, recorded decisions, and latest evidence; inspect callers and downstream consumers before freezing an interface.
2. Identify which requirements already work, which are missing, and which are genuinely blocked. Keep the package's acceptance ledger explicit.
3. Implement the **complete approved v1 surface** within scope: domain behavior, ownership, configuration, monitoring, resource bounds, cancellation, lifecycle, shutdown, error/recovery handling, persistence/replay where applicable, and real consumer integration. Do not create placeholder success paths or promise known work as a later retrofit.
4. Reuse and integrate existing types and implementations. Traits belong primarily at replaceable infrastructure boundaries; avoid duplicate state machines, global service locators, unnecessary abstraction layers, speculative features, and hot-path dynamic parsing or allocation.
5. Add deterministic success, negative, boundary, concurrency/fault, and relevant consumer-witness tests. Test actual production behavior, not a parallel test-only algorithm. Use fake clocks/transports and controlled failure injection, not arbitrary sleeps or live-exchange dependencies for ordinary tests.
6. Run focused checks during development and relevant complete package/feature checks before proposing acceptance. Fix routine Rust/compiler/test issues autonomously; never silence errors or weaken assertions just to turn a suite green.
7. Produce **one concise evidence report per package**: requirements and outcomes, changed paths, exact commands/exit codes/test counts, exercised fixtures, limitations, baseline failures, and required decisions. Keep bulky raw logs outside the evidence index unless needed for a concrete incident.
8. Report `Ready for Review`, `Partial`, or `Blocked` truthfully. Only the user/integration owner marks `Accepted`. Advance to the **next explicitly authorized** package when its prerequisites are met; a localized blocker may be documented while independent permitted work continues.

When a difficult repair repeats without new information, change diagnostic strategy and record the blocker instead of retrying substantially identical failing commands indefinitely.

## Verification

The authoritative MarketForge Live verification entry point is:

    scripts/verify-live.sh

Use the following profiles:

- `quick`: During implementation and debugging.
- `full`: Before declaring a work package ReadyForReview.
- `tui`: When modifying Live TUI functionality.
- `historical`: When shared-core compatibility requires regression testing.

Run additional targeted tests, integration tests, property tests,
fault-injection tests, and benchmarks required by the relevant
work-package acceptance criteria.

The `full` profile is the standard Live regression baseline.
It does not substitute for package-specific acceptance tests.

Verification requirements:

- All commands must preserve actual exit codes.
- Tests must execute the intended cases, not merely compile.
- Report actual executed test counts.
- Distinguish known baseline failures from new regressions.
- Do not remove or weaken tests to manufacture success.
- Do not claim production integration verification from fake adapters.
- Store detailed logs under `engine/target/live-verification/`.
- Include the verification log location and relevant results
  in each work-package evidence report.

When dependencies are cached, prefer offline Cargo execution.

Dependency changes requiring network resolution are permitted
within the authorized Rust implementation scope.

Do not run multiple competing Cargo builds against the same
target directory.

## Integration testing and subsystem completion

Integration tests must verify production subsystem composition, not merely individual components or test-only approximations of production behavior.

### Test organization

Preserve the existing `engine/tests/live/` organization:

- `exchanges/`: Exchange-specific fixtures, behavior, and configuration contracts.
- `integration/`: Cross-component and subsystem integration tests.
- `network/`: Network-facing and opt-in external-service tests.
- `support/`: Reusable test infrastructure, fixtures, and controlled servers.

New subsystem integration tests belong under the appropriate existing hierarchy. Inspect Cargo test target configuration and existing module conventions before creating test entrypoints.

Reuse existing HTTP servers, WebSocket servers, native exchange fixtures, configuration loaders, and assertion helpers. Extend shared infrastructure when appropriate rather than creating duplicate test frameworks.

### Production integration requirements

Integration tests must exercise actual production implementations of the components under examination.

For market-data subsystems, this includes applicable combinations of:

- Source construction and validated configuration.
- WebSocket connection management and subscription acknowledgment.
- Native payload normalization.
- Canonical financial data construction.
- Integrity processing and continuity enforcement.
- REST acquisition, recovery scheduling, and reconciliation.
- Publication acknowledgment and failure handling.
- Readiness and lifecycle transitions.
- Cancellation, shutdown, and resource cleanup.

Mock external dependencies at controlled infrastructure boundaries. Do not replace the subsystem being tested with a parallel test-only implementation.

### Deterministic and external testing

Deterministic integration tests must run without public exchange connectivity or access to production databases.

Use controlled local servers, recorded native payloads, deterministic scheduling, and explicit failure injection.

External network smoke tests must be separately identifiable, explicitly authorized, time-bounded, and excluded from ordinary deterministic verification.

Database-backed integration tests requiring writes must use an isolated test database with restricted credentials.

Never weaken deterministic tests to accommodate transient external-service behavior.

### Capability-driven coverage

Exchange and market coverage must follow actual validated configuration and supported capabilities.

Do not assume all exchanges provide identical:

- WebSocket channels.
- Depth synchronization mechanisms.
- Trade identity semantics.
- REST recovery operations.
- Historical coverage guarantees.
- Subscription limits.
- Rate-limit policies.

Unsupported capabilities must be reported explicitly. Do not fabricate support, silently skip required coverage, or weaken integrity requirements.

### Resource and concurrency verification

Where applicable, integration tests must verify:

- Bounded event, record, and byte buffers.
- Explicit overflow behavior.
- Publication backpressure.
- Shared rate-limit enforcement.
- Concurrent source isolation.
- Recovery request ownership.
- Stale completion rejection.
- Reconnection generation fencing.
- Cancellation and deadline behavior.
- Graceful shutdown and resource release.

Use deterministic assertions for logical resource bounds. Process memory and throughput measurements may supplement these assertions but do not replace them.

### Observable test execution

Integration tests should support readable execution diagnostics when run with `--nocapture`.

Diagnostics should identify the scenario, exchange, market, channel, important lifecycle transitions, recovery operations, publication results, and final outcome.

Tests must remain correct when output is captured or disabled.

Use assertions for correctness. Printed success messages are not evidence of successful behavior by themselves.

Avoid excessive per-message logging in stress tests. Prefer periodic summaries and final metrics.

### Verification evidence

For each completed subsystem integration campaign, report:

- Actual test targets and executed commands.
- Exit codes and executed test counts.
- Covered exchange, market, and channel combinations.
- Passed, failed, blocked, and intentionally unsupported scenarios.
- Recovery and failure-injection coverage.
- Resource-bound and rate-limit results.
- Relevant performance observations.
- Remaining limitations and unresolved architectural decisions.

A passing unit-test suite does not establish subsystem integration completeness.

A passing deterministic integration suite does not establish live exchange compatibility.

A passing live smoke test does not establish fault tolerance or historical completeness.

Declare subsystem completion only when its authorized acceptance criteria have supporting verification evidence.

## Database, filesystem, and external-service safety

- The existing `marketforge` PostgreSQL database is **not disposable**. Inspect it read-only when permitted. Do not issue writes, DDL, destructive queries, cleanup, or migrations against it without specific authorization for that action.
- Prefer a separate restricted test database/role on the same PostgreSQL server for unattended writable integration tests. Verify that credentials cannot mutate the real `marketforge` database; prompt restrictions alone are not isolation. Never print, copy, commit, or log passwords, tokens, `.pgpass`, or other secrets.
- Database fixtures must test real PostgreSQL transactions when required; mock contracts are not substitutes for claiming SQL correctness. Mark environment-dependent verification unavailable if proper isolated access is missing.
- Do not silently rewrite or delete persistent epoch journals, configuration revisions, retention data, user files, or existing database records. Respect documented fail-closed and crash-recovery limitations.
- No external exchange requests, credentials, or smoke tests unless separately authorized. Local deterministic mocks are the default.

## Git and unattended safety

- Codex is authorized to autonomously manage Git throughout implementation, including creating branches, staging changes, committing, amending, rebasing, cherry-picking, merging, and maintaining checkpoints.
- Create meaningful local commits after completing and verifying each work package or significant implementation milestone. Use descriptive commit messages referencing the relevant work package.
- Codex may reorganize or rewrite its own unshared development history when necessary. Never destroy unrelated user work or previously existing changes.
- Preserve pre-existing dirty and untracked files unless they are explicitly part of the assigned implementation. Inspect the working tree before destructive operations.
- Maintain a recoverable Git history. Establish checkpoints before risky refactoring, major integration changes, or potentially destructive operations.
- Resolve ordinary merge conflicts independently when the correct resolution follows established contracts. Escalate genuine architectural conflicts.
- Codex may freely manage local development branches, but must not push to remotes, force-push, or modify protected branches without explicit authorization.
- Stay within the authorized implementation scope. Cross-module changes are permitted when necessary for correct integration, provided existing architectural decisions and compatibility guarantees are preserved.
- Do not access or modify unapproved production data, secrets, external paths, or system settings merely to make tests pass.
- Report significant Git operations, completed checkpoints, unresolved conflicts, and any remaining risks in the work-package evidence.

## Reporting and stopping

At every package boundary, give a compact report: `status`, `implemented`, `verification`, `remaining blockers`, `changed files`, `evidence path`, and `next authorized package`. Stop for an unresolved decision with material consequences, unsafe/destructive access, exhausted permitted resources, or completion of the authorized sequence. Routine code decisions and compiler errors are not reasons to request user intervention.

`AGENTS.md` defines durable repository behavior; `.planning/live/AUTONOMOUS_PLAN.md` (if present and explicitly authorized) defines the **current sequence, permitted operations, and stopping point**. Neither file grants permission to change accepted decisions, access production databases, or claim acceptance without evidence.

