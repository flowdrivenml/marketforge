# Source Stream completion verification — 2026-10-29

Status: **Ready for Review**. The authorized Source Stream plan and the subsequently authorized Gate.io acquisition routing are implemented and verified. All 14 BTC profiles pass actual local-server trade and depth pipelines. This report does not confer acceptance.

## Authority and scope

The current authorities are `.planning/live/AGENTS.md`, `.planning/live/objective.md`, the updated autonomous plan, and production code/tests/schema. Retired documents are not required. The previous report's missing-document blocker is resolved.

PostgreSQL is mandatory Live infrastructure. `live` now enables `postgres`; standalone means independence from the future top-level runtime. No alternative registry, persistence backend, generic canonical router, full runtime, trading engine, or Python orchestration was implemented.

The user explicitly authorized Gate.io instrument-aware WebSocket acquisition routing within the existing transport resolver and Source attachment infrastructure. The previous Gate.io scope blocker is resolved. The implementation associates native messages with existing subscription owners; it does not route canonical events or implement the future generic router.

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

Gate.io completion adds:

- Source preparation carries the same immutable normalization format store into attachment. The existing resolver validates exchange/profile/channel/event/record contracts and compiles the configured instrument field (`currency_pair`, `contract`, or `s`), rather than duplicating a separate exchange routing configuration.
- Spot trades use native object payloads, futures trades use native arrays, and all three depth profiles use instrument-tagged native objects. Mixed futures arrays are validated completely before delivery, then partitioned by subscribed instrument while preserving each instrument's record order, envelope metadata, native values, and text/binary frame type. A single destination retains the original frame.
- Unknown instruments, missing/malformed instrument fields, unsupported record shapes, and overlapping/ambiguous selectors fail closed. Invalid mixed batches cannot publish a valid prefix. Transport failure revokes readiness and retires affected ownership explicitly.
- Every native fragment retains its physical connection stamp. Subscription confirmation gates each destination independently; bounded pre-ACK fragments are discarded when their connection generation is revoked. A second fence check at dispatch prevents fragments classified before revocation from crossing into a newer connection.
- Routing bounds are 16 MiB per input frame, 1,024 array records, 64 destinations, and 64 KiB of envelope metadata for mixed fanout. The complete payload is removed before cloning envelopes, preventing multiplication of the original batch per destination. Overflows fail explicitly. Existing bounded Source channels and shared early-frame capacity remain in force.
- Deterministic production-pipeline tests now include BTC plus actual catalog ETH Spot/Linear instruments sharing one connection, mixed BTC/ETH/BTC futures batches, exact contracts/base quantities, independent shutdown and surviving resubscription, separate REST depth snapshots and instrument-isolated updates, nine malformed/unknown/shape scenarios, and pre-ACK stale data plus two reconnects for each of three Gate.io profiles. All 14-profile acquisition tests now require success; no Gate.io transport exclusion remains.

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
| `ce233a1` | Previous Partial evidence; its Gate.io scope blocker is superseded by explicit authorization and this campaign |
| `828d88f` | Instrument-aware Gate.io native routing, production Source attachment, shared/mixed/fault/lifecycle/depth tests |

User changes to `.planning/live/AGENTS.md`, `.planning/live/AUTONOMOUS_PLAN.md`, and untracked `engine/.planning/` are preserved and excluded from these implementation commits.

## Final verification receipt

Final command: `SOURCE_POSTGRES_TESTS=1 scripts/verify-live-source.sh full`, with `MARKETFORGE_DATABASE_URL` supplied locally from `.secrets/postgres.env` through the process environment without printing or committing it. Exit **0**.

Combined log: `engine/target/live-verification/gateio/full-final.log`.

Authoritative full Live logs: `engine/target/live-verification/20261029T211839Z-291259/`.

The focused wrapper uses `cargo test --locked --offline --no-default-features --features live,postgres`. Its exact executed test suffixes and counts are:

| Target / suffix | Passed | Ignored | Exit |
|---|---:|---:|---:|
| `--lib live::stream::source` | 124 | 0 | 0 |
| `--test live_source -- --nocapture` | 38 | 1 PostgreSQL opt-in | 0 |
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
| live-unit | PASS: 2,017 passed, 4 existing recording-DB tests ignored |
| live_websocket | PASS: 69 |
| live_rest | PASS: 48 |
| live_normalization | PASS: 9 |
| live_integrity | PASS: 83 |
| live_config | PASS: 14 |
| live_metrics | PASS: 3 |
| live_exchange_contracts | PASS: 149 |
| live-network-compile | PASS: compilation only, no external network tests executed |
| live-doctests | PASS: 7 |

There were **2,834 passing test invocations** in the final wrapper run, including intentionally repeated focused/full regression gates. This is not a claim of 2,834 distinct tests. The focused 124 source unit cases are part of the full library suite. The PostgreSQL test ignored by the ordinary deterministic invocation was explicitly executed successfully immediately afterward. The four remaining ignored library cases concern optional market-data recording and require their own isolated PostgreSQL test database; they were not repurposed against this non-disposable database.

Gate.io milestone verification (all exit **0**):

- `scripts/verify-live-source.sh quick`: 124 source units plus compilation; `engine/target/live-verification/gateio/quick-final.log`.
- `cargo test --manifest-path engine/Cargo.toml --locked --offline --no-default-features --features live --lib live::infrastructure::websocket`: 235 tests, including five batch-routing tests, revoked-fragment fencing, and six profile/channel resolver contracts; `gateio/websocket-units.log`.
- `scripts/verify-live-source.sh integration`: 34 original Source cases, now requiring all Gate.io trade/depth acquisitions; `gateio/initial-matrix.log`.
- `scripts/verify-live-source.sh extended`: 124 source units, 38 deterministic Source cases, and 69/48/9/83/63 related cases; `gateio/extended.log`.
- `cargo test --manifest-path engine/Cargo.toml --locked --offline --no-default-features --features live --test live_source gateio::shared_depth -- --nocapture`: final focused shared-depth scenario passed both Spot and Linear; `gateio/depth-debug.log`. Full and extended runs subsequently passed all four Gate.io test functions.
- Development failures identified test-fixture errors: Spot identity uses `id_market`; subscription demand changes can replay old ACKs; extending depth demand requires fresh synchronization; an unchanged depth level correctly yields no changes. Fixtures now update native identities consistently, match ACKs to their instrument, gate current snapshot responses, and send a changed level for delta assertions. Production assertions were retained. The final extended/full receipts supersede diagnostic failure logs.

Earlier Source milestones (historical receipts, before Gate.io completion):

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
| Gate.io | Spot | 15850 | PASS | PASS, REST synchronization | UNSUPPORTED |
| Gate.io | Linear | 17397 | PASS | PASS, REST synchronization | UNSUPPORTED |
| Gate.io | Inverse | 18256 | PASS | PASS, REST synchronization | UNSUPPORTED |

UNSUPPORTED here describes the current Source implementation, not absence of exchange APIs. Automatic trade recovery remains deliberately limited to the existing conservative Binance Spot policy; REST capability tests do not establish complete recovery for other profiles. Bitget Linear requires an explicitly supplied catalog-resolved `native_ws_inst_type`; its test supplies `usdt-futures` through the existing validated subscription parameter mechanism. Canonical `perpetual` alone does not resolve that native product classification.

## Scenario results and evidence layers

The 38 Source deterministic cases use actual WebSocket manager/attachment/executor/runner/normalization/integrity/publication composition. REST scenarios use the real coordinator, request resolution, HTTP client, normalization, and integrity completion. Subscription-quota verification exercises the production connection limiter directly; it is not a claim of six real socket subscriptions. Additional ownership/cursor/fault cases run in the existing production component suites.

| Scenario | Result / witness |
|---|---|
| Validated construction; fresh initialization | PASS: both channels, 14 profiles; invalid inputs also covered by source units |
| WebSocket connect / ACK / canonical normalization / accepted publication / readiness | PASS: all 14 profiles, both channels |
| Exact decimal native quantities and contract economics | PASS: all 14 native trade profiles; all 14 catalog/specification comparisons |
| Initial REST depth synchronization | PASS: Binance and Gate.io Spot, Linear, Inverse |
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
| Bounded publication and sustained ordering | PASS: 512 records, ordered IDs, bounded 64-batch sink; final run 131.502 ms (diagnostic throughput observation, not a benchmark) |
| Pending recovery requests, concurrent scheduling, invalid cursors, foreign requester/profile/epoch | PASS in existing source unit, REST, WebSocket, integrity and recovery suites; these are component-boundary proofs, not invented external responses |
| Stale WebSocket frames and connection/epoch fencing | PASS in source transport/acquisition units and existing WebSocket fenced-ingress/reconnect suites |
| PostgreSQL create/retrieve/desired update/duplicate/concurrency/rollback/failure/deletion/restoration | PASS: explicitly authorized database case |
| Gate.io BTC trade/depth acquisition | PASS: all three profiles, publication/readiness/shutdown; real REST depth synchronization |
| Gate.io shared trade instruments / mixed native arrays | PASS: Spot BTC/ETH objects and Linear BTC/ETH/BTC array on one connection; source identity, order and exact contract economics asserted |
| Gate.io shared depth instruments | PASS: Spot and Linear per-instrument REST snapshots; ETH-only change leaves BTC message count unchanged |
| Gate.io malformed/unknown payloads | PASS: three fault types × three profiles; complete-frame rejection, no prefix publication, Failed status, zero remaining attachments |
| Gate.io stale data / reconnect | PASS: pre-ACK old-generation trade never published; two reconnects per profile revoke/re-establish readiness; direct production dispatch test rejects both stale partitioned fragments after physical revocation |
| Gate.io independent source shutdown | PASS: BTC removal leaves ETH subscribed/publishing with its own identity on the rebuilt connection; stopped BTC stays Stopped |
| Gate.io ambiguous selectors and routing bounds | PASS: overlapping selectors reject; 1,025 records, 65 destinations, or oversized mixed envelope fail; binary/text metadata and per-source ordering preserved |
| Gate.io automatic historical trade recovery | UNSUPPORTED by current conservative Source policy; no consecutive-ID gap guarantee is fabricated |

No process RSS/allocator peak was measured. Logical buffer bounds are tested through explicit overflow and replacement/failure outcomes; inferred buffer limits are not advertised as measured peak occupancy. Synchronous publication callbacks must honor the bounded handoff contract; the executor cannot preempt an arbitrary blocking callback. Existing transport/fence tests cover concurrent revocation at their production boundaries; this campaign does not claim every cross-product of every fault on every exchange.

## PostgreSQL access and cleanup

Read catalog tables: `catalog.instruments`, `catalog.instrument_specs`, `catalog.exchanges`. Read schema metadata through `information_schema` and `pg_constraint`. Inspected foreign keys before any cleanup. All relevant Live foreign keys use restrictive deletion; definitions reference stream registrations, and dependencies reference definitions in both directions.

Writes were limited to uniquely owned `codex-source-<pid>-<timestamp>-<index>` records in `live.streams` and `live.stream_definitions`. No shared instrument/specification rows, dependency edges, recorded trades, L2, or recording coverage rows were modified. No DDL, migrations, unrestricted DELETE, TRUNCATE, or sequence reset was performed. Normal identity allocation advanced the registration sequence.

| Campaign | Ownership prefix | Registrations created / removed | Definitions created / removed |
|---|---|---|---|
| Focused PostgreSQL | `codex-source-224335-1793305671711655209` | 15 / 15 | 15 / 15 |
| First full | `codex-source-241506-1793306336817498840` | 15 / 15 | 15 / 15 (one via repository deletion, 14 final cleanup) |
| Previous final full | `codex-source-250882-1793306578079129815` | 15 / 15 | 15 / 15 (one via repository deletion, 14 final cleanup) |
| Gate.io final full | `codex-source-290791-1793308715034211307` | 15 / 15 | 15 / 15 (one via repository deletion, 14 final cleanup) |

Total accepted test-owned registrations: **60**, all removed. Total accepted test-owned definitions: **60**, all removed. Additional transaction-rollback/missing-catalog/wrong-symbol attempts leave no accepted rows. Concurrent registration has exactly one winner. Cleanup executes for the exact generated ownership list even if an assertion panics; no shared records are selected for deletion.

Final read-only audit: **0 registrations, 0 definitions, 0 remaining `codex-source-%` registrations**, matching the pre-test empty application baseline. Receipts: `engine/target/live-verification/gateio/database-before.log` and `database-after.log`; exit **0**, both counts identical. The earlier campaign receipt remains `engine/target/live-verification/source-audit/final-database-audit.log`. No incomplete cleanup remains.

Restoration tests discard old runtime bookkeeping and reload durable definitions into fresh Stopped registries with unsynchronized source processing authority. These are library-level graceful/crash-state restoration simulations, not child-process kill tests or crash-lossless recording claims.

## Phase disposition and remaining limitations

Phases 1–12 have supporting implementation and verification evidence. Phase 11 now requires and passes successful trade/depth acquisition for all 14 BTC profiles. The previously unresolved Gate.io routing requirement is implemented under the user's explicit bounded authorization. There are no remaining architectural blockers within this assignment.

Remaining limitations: automatic Source trade history recovery is limited to the existing Binance Spot numeric-ID policy; readiness for other profiles does not assert proven historical coverage. Native product parameters must be supplied when canonical classification alone does not select a unique exchange product. Tests are controlled local-server integration, not public-exchange compatibility smoke tests. Four optional recording-DB unit cases remain intentionally ignored; mandatory Source persistence is explicitly tested. No process-wide memory benchmark or child-process crash test is claimed. Synchronous publication callbacks must honor their bounded handoff contract. The future generic router/full runtime and unrelated systems remain outside the assignment.

Only the user may mark this work Accepted.

## Changed paths

- `.planning/live/evidence/source-stream-verification-2026-10-29.md`
- `engine/Cargo.toml`
- `engine/src/live/infrastructure/backoff.rs`
- `engine/src/live/infrastructure/normalization/facade.rs`
- `engine/src/live/infrastructure/rest/retry.rs`
- `engine/src/live/infrastructure/websocket/manager/facade.rs`
- `engine/src/live/infrastructure/websocket/routing/frame_dispatcher.rs`
- `engine/src/live/infrastructure/websocket/routing/message_router.rs`
- `engine/src/live/infrastructure/websocket/routing/payload_tests.rs`
- `engine/src/live/infrastructure/websocket/slots/coordinator.rs`
- `engine/src/live/infrastructure/websocket/streams/source.rs`
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
- `engine/tests/live/integration/source/gateio.rs`
- `engine/tests/live/integration/source/harness.rs`
- `engine/tests/live/integration/source/main.rs`
- `engine/tests/live/integration/source/pipeline.rs`
- `engine/tests/live/integration/source/profiles.rs`
- `engine/tests/live/integration/source/recovery.rs`
- `engine/tests/live/integration/source/resources.rs`
- `engine/tests/live/support/data/instruments/btc_sources.json`
- `engine/tests/live/support/data/source/gateio_secondary.json`
- `engine/tests/live/support/data/source/README.md`
- `engine/tests/live/support/data/source/trades.json`
- `engine/tests/live/support/http_server.rs`
- `scripts/verify-live-source.sh`
