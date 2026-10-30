# MarketForge Live — Autonomous Source Stream Completion Plan

## Authorization

Codex is authorized to complete the MarketForge Live Source Stream
subsystem, implement its integration tests, correct discovered defects,
and produce verification evidence.

Work autonomously through all phases of this plan.

Follow AGENTS.md, accepted architectural decisions, and the Live
implementation roadmap. Inspect current code before making changes.

Do not implement the generic router, full MarketForge runtime,
trading engine, Python orchestration, or unrelated subsystems.

Do not claim acceptance. Final acceptance belongs to the user.

## 1. Git Safety

Before editing:

- Inspect Git status and the current branch.
- Confirm that a pre-Codex checkpoint exists.
- Work on the dedicated Source Stream development branch.
- Preserve unrelated changes and untracked files.
- Create meaningful commits after verified milestones.
- Never force-push, rewrite protected branches, or push without permission.

## 2. Source Stream Completion

Inspect and complete the existing production implementations:

- SourceStreamFactory
- SourceStreamInstance
- SourceProcessor
- SourceStreamRunner
- SourceStreamExecutor
- SourceRuntimeSupervisor
- SourceTradeAttempt
- SourceTradeRecoveryPolicy
- SourceTradePageCollector
- SourceTradePublication
- Integrity facade and recovery integration

Required functionality:

- Validated source construction.
- Standalone initialization without the full MarketForge runtime.
- Correct WebSocket acquisition and subscription handling.
- Native normalization and canonical output.
- Trade and depth integrity.
- REST depth synchronization.
- Supported trade recovery and historical pagination.
- Recovery ownership and epoch fencing.
- Publication acknowledgment before integrity commit.
- Correct readiness transitions.
- Cancellation, reconnection, and shutdown.
- Bounded resource usage and explicit failure behavior.

Reuse existing components. Do not introduce competing controllers,
registries, recovery coordinators, or state machines.

## 3. Standalone Source Initialization

Implement a minimal, production-usable Source Stream construction API.

The standalone builder must:

- Accept validated source definitions.
- Resolve existing exchange configuration.
- Assemble existing production dependencies.
- Construct the runner and executor.
- Integrate with the existing supervisor.
- Support deterministic integration testing.
- Work without requiring PostgreSQL.

Keep persistence and runtime construction as separate responsibilities.

## 4. PostgreSQL Persistence

Inspect the existing live.streams schema, registry implementation,
migrations, and accepted architecture.

Implement missing persistence functionality only where required by
the authorized Source Stream scope.

Requirements:

- Persist source definitions when accepted.
- Persist desired state when changed.
- Distinguish desired state from actual runtime state.
- Restore valid definitions after restart.
- Establish fresh acquisition and integrity authority.
- Handle duplicate identities and concurrent registration.
- Support controlled deletion and dependent-record cleanup.
- Preserve transactional correctness.

Never persist ephemeral task handles or treat old integrity state
as currently synchronized.

## 5. PostgreSQL Authorization

Codex may read secrets/postgres.env locally for database access.

For this assignment, Codex is authorized to:

- Inspect schemas and records.
- Query catalog instruments and specifications.
- Create test-owned Source Stream records.
- Update test-owned Source Stream records.
- Delete test-owned Source Stream records.
- Modify dependent records exclusively owned by those test streams.
- Test persistence, restoration, transactions, and cleanup.

The existing database is not disposable.

Restrictions:

- Preserve all pre-existing application records.
- Never use unrestricted DELETE or TRUNCATE.
- Never perform destructive schema changes.
- Never delete shared records belonging to other streams.
- Inspect foreign-key relationships before cleanup.
- Use identifiable test ownership.
- Prefer transactional isolation where possible.
- Never expose or commit credentials.
- Report incomplete cleanup.

Database writes outside these permissions require separate approval.

## 6. Instrument Test Matrix

Use:

engine/tests/live/support/data/instruments/btc_sources.json

Test all 14 BTC instrument profiles:

| Exchange | Market | Instrument ID |
|---|---|---:|
| Bybit | Spot | 1 |
| Bybit | Linear | 688 |
| Bybit | Inverse | 1427 |
| Binance | Spot | 6163 |
| Binance | Linear | 9872 |
| Binance | Inverse | 10792 |
| OKX | Spot | 3336 |
| OKX | Linear | 4063 |
| OKX | Inverse | 4048 |
| Bitget | Spot | 11545 |
| Bitget | Linear | 14215 |
| Gate.io | Spot | 15850 |
| Gate.io | Linear | 17397 |
| Gate.io | Inverse | 18256 |

Options and dated futures are excluded.

Exercise trades and depth according to actual profile capabilities.

Do not invent unsupported exchange functionality.

Preserve exact decimal quantities and contract economics.

## 7. Integration Test Architecture

Preserve the existing engine/tests/live structure.

Add Source Stream integration tests under:

engine/tests/live/integration/source/

Inspect existing Cargo test targets before creating entrypoints.

Reuse existing infrastructure:

- tests/live/support/http_server.rs
- tests/live/support/websocket_server.rs
- Existing native exchange fixtures.
- Existing normalization and integrity test utilities.
- Existing REST and recovery test infrastructure.

Test the actual production pipeline:

Local WebSocket Server
    -> WebSocket Manager
    -> Source Attachment
    -> Source Executor
    -> Source Runner
    -> Normalization
    -> Integrity
    -> Publication Sink

REST recovery tests must exercise the real recovery coordinator,
request construction, HTTP execution, normalization, reconciliation,
and integrity completion.

Mock external exchanges, not MarketForge's internal behavior.

Deterministic tests must not require public exchange connectivity.

## 8. Required Scenario Coverage

### Normal Operation

- Source creation and initialization.
- Successful WebSocket connection.
- Subscription acknowledgment.
- Native trade normalization.
- Native depth normalization.
- Accepted publication.
- Correct ordering.
- Correct readiness.
- Multiple independent sources.

### Successful Recovery

- Initial depth synchronization.
- Depth recovery after a gap.
- Single-page trade recovery.
- Multi-page historical trade recovery.
- Live trades arriving during REST recovery.
- REST/WebSocket overlap.
- Retryable HTTP failure followed by success.
- Recovery after reconnect.
- Successful publication followed by integrity commit.

### Partial Recovery

- Incomplete REST trade coverage.
- First page succeeds, second page fails.
- Pagination reaches configured limits.
- Missing or duplicate trade identities.
- REST snapshot cannot bridge buffered depth updates.
- Empty REST response.
- REST/WS identity conflict.
- Recovery buffer reaches capacity.
- Publication failure after successful reconciliation.
- Reconnect interrupts recovery.

Partial recovery must never produce false readiness or premature
identity commits.

### Failed Recovery

- HTTP unavailable.
- HTTP timeout.
- Malformed REST payload.
- Wrong symbol or profile.
- Obsolete acquisition response.
- Invalid pagination cursor.
- Retry budget exhaustion.
- Buffer overflow.
- Shutdown during recovery.

### Publication

- Ordinary publication succeeds.
- Ordinary publication fails.
- Recovery publication succeeds.
- Recovery publication fails.
- Depth snapshot publication fails.
- Depth invalidation failure.
- Slow downstream consumer.
- Publication capacity exhaustion.
- Ambiguous downstream handoff.
- Reconnect during publication.

### Lifecycle

- Start and stop.
- Repeated start/stop.
- Unexpected disconnect.
- Successful reconnect.
- Stale WebSocket messages.
- Stale REST completions.
- Rapid reconnect cycles.
- Cancellation during recovery.
- Shutdown during publication.
- Multi-source isolation.

### Resource Limits

- Shared REST rate limiting.
- WebSocket subscription rate limiting.
- Concurrent recovery scheduling.
- Retry and deadline enforcement.
- Publication backpressure.
- Bounded trade buffers.
- Bounded depth buffers.
- Bounded pagination.
- Bounded pending requests.
- Sustained throughput.
- Resource release after shutdown.

### PostgreSQL

- Create source definition.
- Retrieve persisted configuration.
- Duplicate identity handling.
- Update desired state.
- Graceful restart restoration.
- Simulated crash restoration.
- Fresh integrity authority after restart.
- Test-owned source deletion.
- Transaction rollback.
- Concurrent registration.
- Database failure handling.

## 9. Test Correctness Rules

Every scenario must contain real assertions.

A printed PASS is not proof of correctness.

Use deterministic local servers, controlled clocks where practical,
and explicit failure injection.

Avoid arbitrary sleeps.

Assert ordering, identities, readiness, resource bounds,
publication outcomes, and cleanup.

A recovery failure may be a passing test when the source responds
correctly and safely.

Do not weaken assertions to manufacture success.

## 10. Observable Test Output

Integration tests must support --nocapture.

Print meaningful scenario milestones:

[EXCHANGE/MARKET/CHANNEL] Source initialized
[EXCHANGE/MARKET/CHANNEL] WebSocket connected
[EXCHANGE/MARKET/CHANNEL] Subscription acknowledged
[EXCHANGE/MARKET/CHANNEL] Normalization completed
[EXCHANGE/MARKET/CHANNEL] Integrity evaluated
[EXCHANGE/MARKET/CHANNEL] Recovery started
[EXCHANGE/MARKET/CHANNEL] Recovery completed or rejected
[EXCHANGE/MARKET/CHANNEL] Publication result
[EXCHANGE/MARKET/CHANNEL] Final readiness
[EXCHANGE/MARKET/CHANNEL] Shutdown completed

Report actual metrics where available:

- Received WebSocket messages.
- Canonical records.
- Integrity rejections.
- Recovery attempts and pages.
- Publication failures.
- Peak buffer occupancy.
- Rate-limit observations.
- Final readiness.
- Test duration.

Avoid excessive per-message logging during stress tests.

## 11. Verification

Source Stream development must use focused verification to avoid repeatedly executing unrelated, expensive MarketForge Live integration suites.

### Verification entrypoints

The dedicated Source Stream verification entrypoint is:

`scripts/verify-live-source.sh`

The authoritative full MarketForge Live verification entrypoint remains:

`scripts/verify-live.sh`

Codex is authorized to create or improve `scripts/verify-live-source.sh` without modifying the behavior of the existing full verification script.

### Dedicated Source Stream integration target

Create or reuse the Cargo integration-test target:

`live_source`

Entrypoint:

`engine/tests/live/integration/source/main.rs`

Register it in `engine/Cargo.toml` using the existing integration-test conventions.

Preserve all existing Live test targets.

### Verification profiles

The Source Stream verification script must support four profiles.

**quick**

- Run `cargo check` with the appropriate Live features.
- Run focused Source Stream unit tests.
- Use during ordinary implementation and debugging.
- Do not execute unrelated integration suites.

**integration**

- Run the dedicated `live_source` integration-test target.
- Include readable scenario diagnostics using `--nocapture`.
- Exercise the actual Source Stream pipeline.
- Use after Source Stream behavior changes.

**extended**

- Run Source Stream unit tests.
- Run Source Stream integration tests.
- Run relevant existing integration targets:
  - `live_websocket`
  - `live_rest`
  - `live_normalization`
  - `live_integrity`
  - `live_recovery`
- Use after major implementation milestones.
- Avoid repeatedly executing slow unrelated suites.

**full**

- Delegate to `scripts/verify-live.sh full`.
- Run when the implementation is stable.
- Required before declaring Source Stream Ready for Review.
- Repeat when subsequent changes invalidate the verification evidence.

### Individual scenario execution

Codex should run individual integration scenarios when diagnosing failures rather than repeatedly executing the entire integration suite.

For example:

```bash
cargo test \
    --no-default-features \
    --features live,postgres \
    --test live_source \
    multi_page_trade_recovery \
    -- --nocapture
```

Use the actual Cargo test targets and feature combinations defined by the repository.

### Existing integration-test failures

Codex is authorized to investigate and correct existing integration-test failures directly related to Source Stream or introduced by its implementation changes.

Before modifying existing tests, identify whether the failure results from:

- A production regression.
- Incorrect test expectations.
- Outdated fixtures.
- Nondeterministic scheduling.
- Missing external infrastructure.
- A pre-existing unrelated failure.

Correct production defects rather than weakening tests.

Changes to existing test expectations must be justified by authoritative contracts or approved architecture.

Do not:

- Remove failing tests to obtain a passing suite.
- Weaken assertions without evidence.
- Introduce broad `#[ignore]` annotations.
- Silently skip required scenarios.
- Change unrelated subsystem behavior without authorization.
- Claim that a test passed when it was not executed.

Unrelated baseline failures must be documented separately.

### Slow and stress tests

Long-running stress tests should be independently selectable.

Where useful, introduce dedicated test targets such as:

- `live_source_stress`
- `live_source_network`

Stress tests must examine concurrency, resource bounds, rate limiting, backpressure, and cleanup.

External network tests remain opt-in and require separate authorization.

Do not include real exchange connectivity in ordinary deterministic verification.

### Execution requirements

- Preserve actual command exit codes.
- Report actual executed test counts.
- Do not run competing Cargo builds against the same target directory.
- Avoid unnecessary full recompilation.
- Prefer cached dependencies when available.
- Use deterministic local test servers.
- Avoid arbitrary sleeps and uncontrolled timing assumptions.
- Do not weaken tests to manufacture success.

### Verification evidence

Store detailed verification logs under:

`engine/target/live-verification/`

The final evidence report must include:

- Exact commands executed.
- Exit codes.
- Executed test counts.
- Source Stream scenario coverage.
- Exchange, market, and channel coverage.
- Successful, partial, and failed recovery results.
- Rate-limit and backpressure results.
- Resource-bound verification.
- PostgreSQL integration results.
- Existing regression failures.
- Blocked or unsupported scenarios.
- Relevant log locations.

### Final acceptance

Focused verification is sufficient during iterative implementation but not for final acceptance.

Before declaring Source Stream Ready for Review:

1. Complete all required Source Stream integration scenarios.
2. Run the relevant extended regression suite.
3. Run `scripts/verify-live.sh full`.
4. Produce the final verification evidence report.

A passing focused suite does not establish complete MarketForge Live regression coverage.

A passing deterministic integration suite does not establish live exchange compatibility.

Only the user may mark Source Stream Accepted.

## 12. Execution Phases

Phase 1: Audit current implementation and acceptance requirements.

Phase 2: Finish recovery, publication, and lifecycle correctness.

Phase 3: Implement standalone source initialization.

Phase 4: Complete authorized persistent source management.

Phase 5: Build reusable Source Stream integration harness.

Phase 6: Implement normal-operation scenarios.

Phase 7: Implement successful, partial, and failed recovery scenarios.

Phase 8: Implement publication and lifecycle failure scenarios.

Phase 9: Implement rate-limit, backpressure, and resource tests.

Phase 10: Implement PostgreSQL persistence and restoration tests.

Phase 11: Run all applicable 14-profile scenarios.

Phase 12: Run full regression verification and produce evidence.

Codex is authorized to execute these phases sequentially without
requesting approval for routine implementation decisions.

Stop only for genuine architectural conflicts, unsafe operations,
missing required authorization, or material unresolved requirements.

## 13. Completion Criteria

Source Stream is Ready for Review only when:

- Standalone initialization works.
- Supported source profiles initialize correctly.
- WebSocket integration works.
- Normalization and integrity operate correctly together.
- Supported REST recovery works.
- Partial recovery fails safely.
- Publication acknowledgment precedes integrity commit.
- Stale acquisitions cannot mutate current state.
- Rate limits and resource bounds are enforced.
- Multiple sources operate independently.
- Lifecycle and cleanup behavior is verified.
- Authorized persistence requirements are satisfied.
- Required deterministic integration tests pass.
- Full relevant regression verification passes.
- Actual evidence is recorded.

Unsupported capabilities must be reported explicitly.

Do not mark blocked or untested scenarios as passing.

## 14. Evidence and Final Report

Produce a Source Stream completion report containing:

- Implemented functionality.
- Changed files.
- Git commits.
- Exact verification commands.
- Exit codes and test counts.
- Exchange/market/channel coverage matrix.
- Successful recovery scenarios.
- Partial and failed recovery scenarios.
- Rate-limit results.
- Backpressure and resource-bound results.
- PostgreSQL tables accessed.
- Test-owned records created and removed.
- Remaining limitations.
- Unsupported capabilities.
- Unresolved architectural decisions.

Use PASS, FAIL, BLOCKED, or UNSUPPORTED for scenario outcomes.

Final status must be one of:

- Ready for Review
- Partial
- Blocked

Only the user may mark the work Accepted.

Do not begin implementing the generic router or full MarketForge
runtime after completing this assignment.


