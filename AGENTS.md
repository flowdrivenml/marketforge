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

## Rust verification

Run Cargo commands from `engine/`. Prefer locked, offline checks when dependencies are already cached. Capture actual exit status and executed-test count; zero matching tests is not a pass.

```bash
cargo fmt --check
cargo check --locked --offline --no-default-features --features live
cargo test --locked --offline --no-default-features --features live --lib
cargo test --locked --offline --no-default-features --features live --test live_suite
cargo test --locked --offline --no-default-features --features live --doc
cargo check --locked --offline --no-default-features --features live,historical
```

Add package-specific checks and Live+TUI / historical-only / base-library checks when changes affect those boundaries. Target the registered integration test targets explicitly: a blanket `cargo test --no-default-features --features live` may try to compile historical-only tests. A previously observed unrelated historical zero-stream test mismatch is a **baseline finding to recheck and report**, not authorization to edit historical behavior or ignore new regressions. Do not alter `Cargo.lock` or dependencies without an explicit justified scope.

## Database, filesystem, and external-service safety

- The existing `marketforge` PostgreSQL database is **not disposable**. Inspect it read-only when permitted. Do not issue writes, DDL, destructive queries, cleanup, or migrations against it without specific authorization for that action.
- Prefer a separate restricted test database/role on the same PostgreSQL server for unattended writable integration tests. Verify that credentials cannot mutate the real `marketforge` database; prompt restrictions alone are not isolation. Never print, copy, commit, or log passwords, tokens, `.pgpass`, or other secrets.
- Database fixtures must test real PostgreSQL transactions when required; mock contracts are not substitutes for claiming SQL correctness. Mark environment-dependent verification unavailable if proper isolated access is missing.
- Do not silently rewrite or delete persistent epoch journals, configuration revisions, retention data, user files, or existing database records. Respect documented fail-closed and crash-recovery limitations.
- No external exchange requests, credentials, or smoke tests unless separately authorized. Local deterministic mocks are the default.

## Git and unattended safety

- Preserve pre-existing dirty/untracked changes. Before editing, record branch and working-tree inventory; do not assume the checkout is clean.
- Do not push, merge, rebase, reset, clean, remove another owner's files, or rewrite history. **No commits unless explicitly authorized** by the current execution plan/user. If local checkpoint commits are authorized later, keep them on the designated development branch and never push automatically.
- Stay within the current work-package scope. When a dependency requires editing outside that scope, document the precise reason and seek approval if it changes an accepted interface or another subsystem's semantics.
- Do not access or modify unapproved production data, secrets, external paths, or system settings just to make a task pass.
- Be candid about permissions, missing dependencies, unavailable tests, unsupported exchange behavior, and architectural blockers. Do not manufacture success evidence.

## Reporting and stopping

At every package boundary, give a compact report: `status`, `implemented`, `verification`, `remaining blockers`, `changed files`, `evidence path`, and `next authorized package`. Stop for an unresolved decision with material consequences, unsafe/destructive access, exhausted permitted resources, or completion of the authorized sequence. Routine code decisions and compiler errors are not reasons to request user intervention.

`AGENTS.md` defines durable repository behavior; `.planning/live/AUTONOMOUS_PLAN.md` (if present and explicitly authorized) defines the **current sequence, permitted operations, and stopping point**. Neither file grants permission to change accepted decisions, access production databases, or claim acceptance without evidence.
