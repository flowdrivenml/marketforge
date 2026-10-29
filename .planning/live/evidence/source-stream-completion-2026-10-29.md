# Source Stream completion verification — 2026-10-29

Status: **Partial**. Full executed verification passes. Gate.io acquisition remains incomplete at an explicitly rejected transport boundary; this report does not claim completion or acceptance of all authorized requirements.

## Authority and unresolved scope

The current authorities are `.planning/live/AGENTS.md`, `.planning/live/objective.md`, the updated autonomous plan, and production code/tests/schema. Retired documents are not required. The previous report's missing-document blocker is resolved.

PostgreSQL is mandatory Live infrastructure. `live` now enables `postgres`; standalone means independence from the future top-level runtime. No alternative registry, persistence backend, generic canonical router, full runtime, trading engine, or Python orchestration was implemented.

**Unresolved requirement:** the existing `StreamSourceResolver` explicitly rejects Gate.io's `channel` mode because it lacks instrument-aware payload routing. Normalization supports all three Gate.io BTC profiles, but acquisition cannot be reported as working. A decision was requested about extending this existing resolver within the generic-router exclusion. No response was assumed. The affected tests assert fail-closed rejection and print UNSUPPORTED; they do not claim successful Gate.io acquisition. In particular, handling mixed-instrument native batches needs a source-routing contract rather than arbitrary destination selection.

## Implemented and repaired

- Validated standalone `SourceRuntimeBuilder` loads accepted PostgreSQL definitions, resolves existing exchange/format configuration, constructs fresh integrity, binder, runner, and executor, and attaches through the existing WebSocket manager and recovery coordinator. The resulting executor runs under the existing supervisor.
- Pure preparation is separated from PostgreSQL acceptance and transport effects. Deterministic tests supply controlled accepted-definition fixtures; production `load` reads PostgreSQL. No in-memory production persistence backend was added.
- Ordinary trade publication now precedes identity and ordering-baseline commits. A failed handoff leaves the batch retryable at the integrity boundary. Recovery publication similarly commits only after acknowledgment.
- Completed paginated trade recovery now reaches reconciliation, publication, integrity commit, and readiness; the old executor retained a completed bridge indefinitely. Removed duplicated cursor/operation calculations and reused the existing normalization helper.
- The executor selects transport/recovery work fairly. The shared recovery coordinator stores Send futures and can run as a normal Tokio task with the real REST executor.
- Trade recovery live buffers have validated configurable batch/record/byte limits. Readiness is revoked on processing failure. Supervisor startup rejects an executor belonging to another stream; dropped attachments request cleanup through the existing manager as a fallback. Explicit awaited detach remains the shutdown contract.
- Atomic source registration persists `live.streams` registration and desired definition in one transaction. Catalog identity, assets, instrument classification, quantity semantics, and contract economics are validated before acceptance. Duplicate registration, optimistic desired-state revisions, restoration, deletion, and rollback are exercised against PostgreSQL.
- Fixed existing Binance raw subscription resolution and configured combined-envelope extraction. Financial normalization still uses the authoritative canonical types and exact Decimal policy.
- Added the `live_source` entrypoint and reusable local-server harness under the existing integration hierarchy. Tracked all 14 instrument fixtures and controlled native trade envelopes. The fixture README documents their provenance and explicit finite-decimal test prices.
- Fixed a real REST fixture race by adding a process-local atomic discriminator to temporary directory names. Existing clock-only names could collide across concurrent tests.
- Replaced stale aggregate REST endpoint expectations with an exact per-profile public endpoint matrix. All 35 endpoints are required; the two authenticated Binance futures history endpoints remain excluded. No missing public endpoint is silently ignored.
- Replaced seven accidentally executable pseudocode blocks with seven genuine executable documentation tests using production APIs and assertions. No doctest was removed or ignored.

## Git milestones

Branch: `codex/source-stream-completion`. No pushes, resets, rebases, or protected-branch changes.

| Commit | Milestone |
|---|---|
| `9b10989` | Preserved pre-Codex checkpoint |
| `2128be4` | Preserved verified callback repair |
| `bd56a77` | Historical audit evidence; its missing-document blocker is superseded |
| `c011573` | Publication-before-commit, completed recovery handoff, feature/test entrypoint and tracked instrument fixture |
| `570741e` | Standalone source assembly |
| `a8b6f07` | Atomic PostgreSQL registration and restoration tests |
| `435eafe` | Production pipeline harness, fault/lifecycle/resource verification and related repairs |
| `445b256` | REST fixture race, exact endpoint contracts, executable docs, depth-gap and publication-shutdown scenarios |

User changes to `.planning/live/AGENTS.md`, `.planning/live/AUTONOMOUS_PLAN.md`, and untracked `engine/.planning/` are preserved and excluded from these implementation commits.

## Final verification receipt

Final command: `SOURCE_POSTGRES_TESTS=1 scripts/verify-live-source.sh full`, with `MARKETFORGE_DATABASE_URL` supplied locally from `.secrets/postgres.env` through the process environment without printing or committing it. Exit **0**.

Combined log: `engine/target/live-verification/source-audit/full-final.log`.

Authoritative full Live logs: `engine/target/live-verification/20261029T204304Z-252288/`.

The focused wrapper uses `cargo test --locked --offline --no-default-features --features live,postgres`. Its exact executed test suffixes and counts are:

| Target / suffix | Passed | Ignored | Exit |
|---|---:|---:|---:|
| `--lib live::stream::source` | 124 | 0 | 0 |
| `--test live_source -- --nocapture` | 34 | 1 PostgreSQL opt-in | 0 |
| `--test live_source database:: -- --ignored --nocapture` | 1 | 0 | 0 |
| `--test live_websocket` | 69 | 0 | 0 |
| `--test live_rest` | 48 | 0 | 0 |
| `--test live_normalization` | 9 | 0 | 0 |
| `--test live_integrity` | 83 | 0 | 0 |
| `--test live_recovery` | 63 | 0 | 0 |

The wrapper then ran `scripts/verify-live.sh full` (exit **0**), whose 12 gates all passed. That script uses `--locked --offline --no-default-features --features live`; `live` includes mandatory PostgreSQL.

| Authoritative gate | Executed result |
|---|---|
| formatting | PASS: `cargo fmt --all -- --check` |
| live-check | PASS: `cargo check --locked --offline --no-default-features --features live --lib` |
| live-unit | PASS: 2,010 passed, 4 existing recording-DB tests ignored |
| live_websocket | PASS: 69 |
| live_rest | PASS: 48 |
| live_normalization | PASS: 9 |
| live_integrity | PASS: 83 |
| live_config | PASS: 14 |
| live_metrics | PASS: 3 |
| live_exchange_contracts | PASS: 149 |
| live-network-compile | PASS: compilation only, no external network tests executed |
| live-doctests | PASS: 7 |

There were **2,823 passing test invocations** in the final wrapper run, including intentionally repeated focused/full regression gates. This is not a claim of 2,823 distinct tests. The focused 124 source unit cases are part of the full library suite. The PostgreSQL test ignored by the ordinary deterministic invocation was explicitly executed successfully immediately afterward. The four remaining ignored library cases concern optional market-data recording and require their own isolated PostgreSQL test database; they were not repurposed against this non-disposable database.

Earlier milestones:

- `scripts/verify-live-source.sh quick`: PASS, 124 source units; `source-audit/final-quick.log`.
- `scripts/verify-live-source.sh integration`: final focused campaign PASS, 34 deterministic cases; `source-audit/gap-publication-gates.log`.
- `scripts/verify-live-source.sh extended`: PASS at the major harness milestone, 124 source units, then 18 source integration cases, and 69/48/9/83/63 related cases; `source-audit/extended-milestone.log`.
- Initial full campaign: exit 1, identifying the fixture race, stale endpoint totals, and seven invalid documentation examples; `source-audit/full-verification.log`, authoritative logs `20261029T203906Z-243199/`. These failures were repaired, not hidden.
- Focused repair checks: two REST binding cases, one exact endpoint resolution case, one endpoint/bootstrap case, and seven doctests all passed. Receipts: `rest-race-repair.log`, `endpoint-matrix-repair.log`, `endpoint-bootstrap-repair.log`, `doctest-repair.log`.
- `git diff --check`: exit 0 at implementation milestones.

No public exchange smoke tests were run. Deterministic results do not establish live exchange compatibility.

## Exchange / market / channel coverage

Construction and fresh integrity preparation exercise both channels for all 14 profiles. Native trade normalization also verifies exact original quantity representations for all 14. PostgreSQL registration/restoration exercises all 14 instruments. Transport results below are actual local-server production-pipeline results.

| Exchange | Market | Instrument | Trade pipeline | Depth pipeline | Automatic Source trade recovery |
|---|---|---:|---|---|---|
| Bybit | Spot | 1 | PASS | PASS, WS snapshot | UNSUPPORTED |
| Bybit | Linear | 688 | PASS | PASS, WS snapshot | UNSUPPORTED |
| Bybit | Inverse | 1427 | PASS | PASS, WS snapshot | UNSUPPORTED |
| Binance | Spot | 6163 | PASS | PASS, REST synchronization | PASS, historical numeric-ID pagination |
| Binance | Linear | 9872 | PASS | PASS, REST synchronization | UNSUPPORTED |
| Binance | Inverse | 10792 | PASS | PASS, REST synchronization | UNSUPPORTED |
| OKX | Spot | 3336 | PASS | PASS, WS snapshot | UNSUPPORTED |
| OKX | Linear | 4063 | PASS | PASS, WS snapshot | UNSUPPORTED |
| OKX | Inverse | 4048 | PASS | PASS, WS snapshot | UNSUPPORTED |
| Bitget | Spot | 11545 | PASS | PASS, staged snapshot + bridge | UNSUPPORTED |
| Bitget | Linear | 14215 | PASS | PASS, staged snapshot + bridge | UNSUPPORTED |
| Gate.io | Spot | 15850 | UNSUPPORTED transport boundary | UNSUPPORTED transport boundary | UNSUPPORTED |
| Gate.io | Linear | 17397 | UNSUPPORTED transport boundary | UNSUPPORTED transport boundary | UNSUPPORTED |
| Gate.io | Inverse | 18256 | UNSUPPORTED transport boundary | UNSUPPORTED transport boundary | UNSUPPORTED |

UNSUPPORTED here describes the current Source implementation, not absence of exchange APIs. Automatic trade recovery remains deliberately limited to the existing conservative Binance Spot policy; REST capability tests do not establish complete recovery for other profiles. Bitget Linear requires an explicitly supplied catalog-resolved `native_ws_inst_type`; its test supplies `usdt-futures` through the existing validated subscription parameter mechanism. Canonical `perpetual` alone does not resolve that native product classification.

## Scenario results and evidence layers

The 34 Source deterministic cases use actual WebSocket manager/attachment/executor/runner/normalization/integrity/publication composition. REST scenarios use the real coordinator, request resolution, HTTP client, normalization, and integrity completion. Subscription-quota verification exercises the production connection limiter directly; it is not a claim of six real socket subscriptions. Additional ownership/cursor/fault cases run in the existing production component suites.

| Scenario | Result / witness |
|---|---|
| Validated construction; fresh initialization | PASS: both channels, 14 profiles; invalid inputs also covered by source units |
| WebSocket connect / ACK / canonical normalization / accepted publication / readiness | PASS: 11 supported transport profiles, both channels |
| Exact decimal native quantities and contract economics | PASS: all 14 native trade profiles; all 14 catalog/specification comparisons |
| Initial REST depth synchronization | PASS: Binance Spot, Linear, Inverse |
| Gap after a published depth book | PASS: invalidation, second REST attempt, buffered bridge, acknowledged replacement snapshot, Ready |
| Single-page trade recovery | PASS: IDs 11–13 after accepted baseline 10 |
| Multi-page history, REST/WS overlap, live during recovery | PASS: gated HTTP guarantees live buffering while recovery is outstanding; ordered unique IDs and subsequent ordinary publication asserted |
| Retryable HTTP failure then depth recovery | PASS: 503 then valid snapshot, exactly two HTTP attempts, fresh live bridge |
| Incomplete / missing intermediate / duplicate IDs; empty response | PASS: recovery rejects safely; no recovered publication or false readiness |
| First historical page succeeds, second fails | PASS: terminal failure, original recovery responsibility never treated as completed |
| Pagination / retry budget exhaustion | PASS: at most three acquisition attempts/pages; no false completion |
| Malformed REST payload | PASS: trade and depth fail safely |
| HTTP timeout | PASS: real trade HTTP timeout with a gated response, followed by resource release |
| Repeated HTTP server failure | PASS: depth recovery exhausts its bounded three-attempt budget |
| REST snapshot cannot bridge depth buffer | PASS: unsynchronized throughout, bounded retry exhaustion |
| REST/WS identity conflict | PASS: conflicting canonical contents reject reconciliation |
| Recovery publication failure | PASS: terminal source failure; existing recovery commit tests preserve baseline/hold on callback failure |
| Ordinary publication capacity exhaustion | PASS: bounded full sink rejects, source fails and releases attachment; integrity regression tests verify retryable uncommitted identities/baseline |
| Uncertain depth snapshot handoff | PASS: source does not become Ready; invalidation/reset protocol applied |
| Depth invalidation failure | PASS: shutdown reports failure rather than clean Stopped; attachment still released |
| Shutdown during publication | PASS: readiness stays non-ready while callback is outstanding; shutdown completes after its bounded acknowledged boundary |
| Repeated start/stop and reconnect | PASS: three lifetimes, nine reconnects, readiness revoked/re-established; retired supervisor generation cannot resurrect |
| Reconnect / cancellation during REST recovery | PASS: pending old acquisition revoked; new generation publishes IDs 20 and 21 without stale recovery publication |
| Multi-source isolation | PASS: trade sink failure on a shared connection does not destroy the surviving depth source; replacement connection resynchronizes it |
| Shared REST rate limiting | PASS: exhausted shared budget causes zero HTTP requests, one pending waiter, and explicit waiter release on cancellation |
| WebSocket subscription rate limiting | PASS at production limiter boundary: five admissions; sixth deadline fails; pending count returns to zero; independent connection admits |
| Trade buffer capacity | PASS: two retained batches/records configured; additional live input fails closed, without recovery publication |
| Depth buffer capacity | PASS: two-message limit; third delta revokes old snapshot authority, exactly a replacement recovery attempt is observed; stale snapshot not published |
| Bounded publication and sustained ordering | PASS: 512 records, ordered IDs, bounded 64-batch sink; final run 139.189 ms (diagnostic throughput observation, not a benchmark) |
| Pending recovery requests, concurrent scheduling, invalid cursors, foreign requester/profile/epoch | PASS in existing source unit, REST, WebSocket, integrity and recovery suites; these are component-boundary proofs, not invented external responses |
| Stale WebSocket frames and connection/epoch fencing | PASS in source transport/acquisition units and existing WebSocket fenced-ingress/reconnect suites |
| PostgreSQL create/retrieve/desired update/duplicate/concurrency/rollback/failure/deletion/restoration | PASS: explicitly authorized database case |
| Gate.io acquisition, recovery and lifecycle pipeline | UNSUPPORTED: fail-closed rejection verified; successful pipeline remains incomplete |

No process RSS/allocator peak was measured. Logical buffer bounds are tested through explicit overflow and replacement/failure outcomes; inferred buffer limits are not advertised as measured peak occupancy. Synchronous publication callbacks must honor the bounded handoff contract; the executor cannot preempt an arbitrary blocking callback. Existing transport/fence tests cover concurrent revocation at their production boundaries; this campaign does not claim every cross-product of every fault on every exchange.

## PostgreSQL access and cleanup

Read catalog tables: `catalog.instruments`, `catalog.instrument_specs`, `catalog.exchanges`. Read schema metadata through `information_schema` and `pg_constraint`. Inspected foreign keys before any cleanup. All relevant Live foreign keys use restrictive deletion; definitions reference stream registrations, and dependencies reference definitions in both directions.

Writes were limited to uniquely owned `codex-source-<pid>-<timestamp>-<index>` records in `live.streams` and `live.stream_definitions`. No shared instrument/specification rows, dependency edges, recorded trades, L2, or recording coverage rows were modified. No DDL, migrations, unrestricted DELETE, TRUNCATE, or sequence reset was performed. Normal identity allocation advanced the registration sequence.

| Campaign | Ownership prefix | Registrations created / removed | Definitions created / removed |
|---|---|---|---|
| Focused PostgreSQL | `codex-source-224335-1793305671711655209` | 15 / 15 | 15 / 15 |
| First full | `codex-source-241506-1793306336817498840` | 15 / 15 | 15 / 15 (one via repository deletion, 14 final cleanup) |
| Final full | `codex-source-250882-1793306578079129815` | 15 / 15 | 15 / 15 (one via repository deletion, 14 final cleanup) |

Total accepted test-owned registrations: **45**, all removed. Total accepted test-owned definitions: **45**, all removed. Additional transaction-rollback/missing-catalog/wrong-symbol attempts leave no accepted rows. Concurrent registration has exactly one winner. Cleanup executes for the exact generated ownership list even if an assertion panics; no shared records are selected for deletion.

Final read-only audit: **0 registrations, 0 definitions, 0 remaining `codex-source-%` registrations**, matching the pre-test empty application baseline. Receipt: `engine/target/live-verification/source-audit/final-database-audit.log`. No incomplete cleanup remains.

Restoration tests discard old runtime bookkeeping and reload durable definitions into fresh Stopped registries with unsynchronized source processing authority. These are library-level graceful/crash-state restoration simulations, not child-process kill tests or crash-lossless recording claims.

## Phase disposition and remaining limitations

Phases 1–10 and 12 have the implementation/verification evidence described above for the supported transport surface. Phase 11 ran all 14 profile capability checks, with 11 successful acquisition profiles and three explicit Gate.io transport exclusions. The full authorized completion criteria therefore remain **Partial**, despite passing regression gates.

Remaining material requirement: resolve and implement/verify Gate.io instrument-aware acquisition within the explicitly bounded routing scope, or explicitly approve its exclusion from this completion assignment. No test result substitutes for that missing implementation. Native product parameters must be supplied when the current canonical definition cannot uniquely select exchange-specific classification. Public-exchange compatibility, optional recording DB tests, process-wide memory benchmarking, and the future full runtime/router were not claimed.

Only the user may mark this work Accepted.

## Changed paths

- `.planning/live/evidence/source-stream-verification-2026-10-29.md`
- `engine/Cargo.toml`
- `engine/src/live/infrastructure/backoff.rs`
- `engine/src/live/infrastructure/normalization/facade.rs`
- `engine/src/live/infrastructure/rest/retry.rs`
- `engine/src/live/infrastructure/websocket/streams/resolver.rs`
- `engine/src/live/integrity/depth/continuity.rs`
- `engine/src/live/integrity/depth/controller.rs`
- `engine/src/live/integrity/facade/processing.rs`
- `engine/src/live/integrity/trades/controller.rs`
- `engine/src/live/integrity/trades/ingress.rs`
- `engine/src/live/recovery/coordinator.rs`
- `engine/src/live/stream/persistence/repository.rs`
- `engine/src/live/stream/source/attachment.rs`
- `engine/src/live/stream/source/builder.rs`
- `engine/src/live/stream/source/executor.rs`
- `engine/src/live/stream/source/mod.rs`
- `engine/src/live/stream/source/processor.rs`
- `engine/src/live/stream/source/runner.rs`
- `engine/src/live/stream/source/supervisor.rs`
- `engine/tests/live/exchanges/contracts/rate_limits.rs`
- `engine/tests/live/exchanges/contracts/rest/mod.rs`
- `engine/tests/live/exchanges/contracts/rest/resolution.rs`
- `engine/tests/live/integration/integrity/facade/trades.rs`
- `engine/tests/live/integration/rest/support.rs`
- `engine/tests/live/integration/source/database.rs`
- `engine/tests/live/integration/source/depth.rs`
- `engine/tests/live/integration/source/harness.rs`
- `engine/tests/live/integration/source/main.rs`
- `engine/tests/live/integration/source/pipeline.rs`
- `engine/tests/live/integration/source/profiles.rs`
- `engine/tests/live/integration/source/recovery.rs`
- `engine/tests/live/integration/source/resources.rs`
- `engine/tests/live/support/data/instruments/btc_sources.json`
- `engine/tests/live/support/data/source/README.md`
- `engine/tests/live/support/data/source/trades.json`
- `engine/tests/live/support/http_server.rs`
- `scripts/verify-live-source.sh`
