# Live application v1 implementation and verification

## Inspection baseline and ownership map

Starting commit: `974187dbeacffd771b4f2f93741e3ec903efb0c1`, main. Existing streaming and hardening edits, including audit files, are uncommitted and must be preserved. Starting status/diff and focused test logs are recorded separately during implementation.

The immutable StreamGraph supplies the only dependency graph. DemandRegistry owns ephemeral consumer demand; LifecyclePlanner supplies start/stop order. StreamRuntimeRegistry is the sole lifecycle and publication-authority owner. SourceRuntimeSupervisor currently owns it by value, whereas ProcessingService shares an Arc/Mutex; integration must adapt the former to the shared owner without mirrored state. Source executors own attachments, normalization, integrity and acquisition/recovery acknowledgment. ProcessingService owns its existing CPU pool and internal subscriptions. Router owns market publication, subscription queues and demand. Unix transport owns socket/session cleanup. Application ownership will coordinate these services rather than repeat their transitions.

A single application metrics sampler will call MetricsSnapshotCollector.collect; readers/exporters must use latest completed snapshots. Handles retain metrics only, never executors or publications. Application handles are command/read boundaries, not mutable registries. Persistence remains separate from market recording. There is no existing faithful heterogeneous Router recorder/identity allocator; recording-enabled application configuration must be rejected rather than guessing durable positions.

## Instrumentation matrix (pre-edit audit)

| Boundary | Existing authority / instrumentation | Required integration |
|---|---|---|
| Native message / normalization | Source executor, SourceMetrics | Audit JSON-failure placement and processing-time timestamp wording |
| Accepted Source market output | Source executor callbacks | Count successful market acceptance, not metadata-only outcomes |
| Reacquisition | Source executor after acquisition | Clarify successful reacquisition versus attempts |
| Derived input / calculated output / Router acceptance | Processor, output mailbox, service | Distinct generation-scoped handles, no retry double counting |
| Router publication / queue | Router status | Copy occupancy; cumulative acceptance/failure boundaries |
| Unix queue / write | Session worker / coordinated writer | Admissions versus completed writes and occupancy |
| Recording | Recorder control and worker | Pending queued + in-flight, confirmed commit versus failed attempt |
| Lifecycle | Shared registry and application reconciliation | Bounded counters and failure classifications |
| Sampling / process | MetricsSnapshotCollector / sampler | One sampling authority; freshness and optional process observations |
| HTTP | Latest snapshot only | Bounded exposition and encoding cache; no collection on scrape |

This document records implementation contracts and verification, not an autonomous plan.

## Executed verification (this application pass)

Baseline commit remained `974187dbeacffd771b4f2f93741e3ec903efb0c1` on main. The starting status had 43 entries, including the prior uncommitted streaming/hardening implementation and audit artifacts. No existing edits were reset/discarded. Starting status, tracked diff and execution logs were captured under `/tmp/marketforge-application-baseline/` (local evidence, not a tracked build artifact). Baseline focused tests were compiled first and executed: live_config 25, live_metrics 3, live_processing 19, live_source 38 + 1 database skip, stream_transport 22; all passed (107 passes). Existing general configuration was inspected at `data/.config/live/general.toml`; no deployment config was overwritten.

Final locked checks passed for no features, live, historical, stream-client, and the client-only stream-consumer example. The client normal/build/feature dependency tree contains no SQLx/PostgreSQL, reqwest, tungstenite, Hyper or Live runtime dependencies. The executable was built and static validation run with the launch command above, reporting `definitions_loaded=false`.

After final code changes, the compiled normal parallel-harness integration results were:

| Features | Target | Passed | Explicitly skipped |
|---|---|---:|---:|
| live stream-client | live_application | 15 | 1 opt-in measurement (executed separately) |
| live stream-client | live_config | 25 | 0 |
| live stream-client | live_metrics | 3 | 0 |
| live stream-client | live_integrity | 83 | 0 |
| live stream-client | live_source | 38 | 1 PostgreSQL write |
| live stream-client | live_router | 32 | 0 |
| live stream-client | live_processing | 19 | 0 |
| live stream-client | live_derived | 40 | 1 opt-in throughput |
| live stream-client | stream_protocol / stream_client / stream_transport | 3 / 2 / 22 | 0 |
| stream-client only | stream_protocol / stream_client | 3 / 2 | 0 |
| live historical | live_derived | 40 | 1 opt-in throughput |
| live | live_recovery | 63 | 0 |

Relevant library filters were executed, not just compiled: metrics 46; registry 51; Source supervisor 4; Source executor 6; Router 22; stream processing 6 + 1 optional benchmark skip; infrastructure processing 52; recording 6 + 4 disposable-PostgreSQL skips. There is no existing control unit-test target: an exploratory `live::control` filter selected zero and is **not counted** as verification. The new application tests exercise the existing control models, supported Restart and unsupported Reconnect behavior.

The final ten-round campaign ran live_application (15), live_metrics (3), and metrics library tests (46) in each round with normal harness parallelism: 640 passing executions, no failures/timeouts, and the measurement remained opt-in. Earlier diagnostic campaigns are not added to these final totals. Test failures during development were investigated: an exchange fixture only handled one connection and sent premature acquisition messages; it now supports multiple connections and waits original owner readiness. A subsequent repetition exposed the AlwaysOn fixture still using subscribe ACKs before Source acquisition (round 3: 14 passed, 1 timed out). It now gates the initial inputs on both owner AwaitingData states, and every retry input on the replacement generation's AwaitingData state; retries no longer rely on one-shot subscribe-time data. A later round-ten timeout was a second fixture defect: its receive loop ignored terminal End/EOF and spun without yielding. Terminal paths now fail immediately. Diagnostic repeats exposed the originating reason: the fixture fed arrival-order Merge into event-time Convert with zero lateness, so cross-source arrival order could regress the timestamp of the single Merge input. Convert correctly failed closed. The fixture now declares the existing event-time Merge contract and supplies genuine later native observations on both inputs to advance each strict frontier; no financial algorithm, lateness policy or assertion was weakened. The final campaign follows these corrections. Cancellation coverage uses a current-thread poll to admit a retry then drop its caller, a FIFO command acknowledgment, and a new-generation readiness assertion. A discovered Source deadline hole across Recovering was closed rather than extending test sleeps. Retirement is dependent-first rather than racing Source revocation with Derived cleanup.

The application primary test uses the public owner, actual Source normalization/integrity, three Source dependencies, production Merge→Convert, actual Unix transport/client, collector and HTTP. It retains incomplete HTTP requests while market data progresses, verifies valuations/evidence, and removes one root while an independent shared-ancestor consumer remains ready. Additional tests cover cold/late depth baselines, AlwaysOn/OnDemand, delayed readiness/deadline, originating errors/retry isolation, drop and cancelled commands, explicit/idempotent shutdown, a partially written blocked frame with incomplete then completed shutdown, metrics-disabled mode, unavailable measurements, response bounds, recorder commit outcomes, and subprocess offline validation/SIGTERM cleanup.

Commands used (from repository root; no Clippy, public exchanges, production writes or datasets):

```sh
cargo check --manifest-path engine/Cargo.toml --locked --no-default-features
cargo check --manifest-path engine/Cargo.toml --locked --no-default-features --features live
cargo check --manifest-path engine/Cargo.toml --locked --no-default-features --features historical
cargo check --manifest-path engine/Cargo.toml --locked --no-default-features --features stream-client
cargo check --manifest-path engine/Cargo.toml --locked --no-default-features --features stream-client --example stream-consumer
cargo tree --manifest-path engine/Cargo.toml --locked --no-default-features --features stream-client -e normal,build,features

cargo test --manifest-path engine/Cargo.toml --locked --no-default-features \
  --features 'live stream-client' --lib --test live_application --test live_router \
  --test live_config --test live_metrics --test live_integrity --test live_source \
  --test live_processing --test live_derived --test stream_protocol \
  --test stream_client --test stream_transport --no-run
cargo test --manifest-path engine/Cargo.toml --locked --no-default-features \
  --features stream-client --test stream_protocol --test stream_client --no-run
cargo test --manifest-path engine/Cargo.toml --locked --no-default-features \
  --features 'live historical' --test live_derived --no-run
cargo test --manifest-path engine/Cargo.toml --locked --no-default-features \
  --features live --test live_recovery --no-run
```

Each binary path printed by these successful compilations was then run as `timeout 90 PATH`, not timed out during compilation. New application waits additionally have explicit eight-second bounds. Relevant library runs used `timeout 90 engine/target/debug/deps/marketforge_engine-1599fea79e24efa5 FILTER` with the filters `live::metrics`, `live::stream::registry`, `live::stream::source::supervisor`, `live::stream::source::executor`, `live::stream::router`, `live::stream::processing`, `live::infrastructure::processing`, and `live::recording`. The final campaign used:

```sh
for round in $(seq 1 10); do
  timeout 90 engine/target/debug/deps/live_application-a2d916cef0b84f10 || exit $?
  timeout 90 engine/target/debug/deps/live_metrics-37fcd4d1393c964d || exit $?
  timeout 90 engine/target/debug/deps/marketforge_engine-1599fea79e24efa5 live::metrics || exit $?
done
```

Hashes are this build's outputs; resolve them again after compiling in another checkout. No zero-test run is included in the counts. `git diff --check` passed. Real PostgreSQL transactions, public exchange behavior and unsupported-platform resource sampling remain unverified; local mocks replace only exchange/network/database writer seams, not application orchestration or financial processors.

## Small instrumentation/sampling assessment

Opt-in harness: `application_sampling_measurement`, debug test profile (unoptimized), Linux 7.0.0-38-generic x86_64, AMD Ryzen 9 9950X / 32 logical CPUs. Each mode starts one actual application, one local mock WebSocket Source, one Unix client/subscription, then sequentially sends/receives 100 one-trade publications. Sampling-enabled mode collects every 10ms; disabled mode does not collect. Both modes retain the same lightweight authoritative counters. This measures end-to-end native Source/Router/Unix/SDK delivery with **sampling overhead**, not total instrumentation overhead versus a former application (none existed), isolated serialization speed, or financial conversion throughput. Acquisition's first publication is included. It is intentionally short and not a timing-sensitive assertion.

```sh
timeout 90 engine/target/debug/deps/live_application-a2d916cef0b84f10 \
  application_sampling_measurement --ignored --nocapture
```

Final-code run:

| Periodic sampling | Publications/s | Records/s | Median ns | p95 ns | p99 ns |
|---|---:|---:|---:|---:|---:|
| disabled | 1200 | 1200 | 421997 | 540413 | 40928140 |
| enabled (10ms) | 1192 | 1192 | 405597 | 537383 | 41680982 |

Enabled throughput was ~0.7% lower, median latency ~3.9% lower, and p99 ~1.8% higher. These small, conflicting changes are noise rather than a stable overhead estimate. Earlier diagnostic runs ranged from ~7.5% lower enabled throughput to ~1.4% higher. The ~42ms cold tail and small sample dominate this campaign. No stable overhead estimate or throughput target is claimed, and no correctness guarantee was changed for performance. Larger controlled measurement is a remaining limitation, not a reason to introduce batching or another scheduler.

## Changed files and final review state

Application-pass new files: `src/live/application/{mod,runtime,infrastructure}.rs`, `src/live/metrics/{process,prometheus}.rs`, `tests/live_application.rs`, `examples/live/general.toml`, and this document. Existing files extended in this pass:

- Cargo.toml/Cargo.lock; `src/bin/marketforge-live.rs`; `src/live/mod.rs`.
- `src/live/config/general/{mod,model,validation}.rs`, `config/store.rs`; limiter manager and WebSocket module construction exports.
- Integrity depth timestamps/facade handle for authoritative current-book handoff; recovery facade for accepted-request observation.
- Metrics source/derived/runtime/publication/recording/snapshot/module; recorder control/worker/module; persistence repository.
- Processing module/operations/pipeline/service; registry demand/reconcile/runtime; Router event/facade; Source builder/executor/module/processor/supervisor.
- Existing untracked transport session/streaming modules for actual queue/write accounting and current-book marker projection; streaming-v1 docs cross-link the new owner.
- Existing Derived harness, processing fixture RuntimeConfig literals and Router contracts for compatibility and capacity assertions.

Direct live-only dependencies use Hyper 1.12 HTTP/1, hyper-util Tokio, http-body-util, libc, tracing and tracing-subscriber JSON. Hyper/libc/tracing were already transitively locked; the deliberate lock update adds tracing-subscriber and six support packages (nu-ansi-term, sharded-slab, thread_local, tracing-log, tracing-serde, valuable). CBOR v1, exact decimals, native custody, SDK feature boundary and snapshot omission remain unchanged.

Final branch/commit are unchanged. The working tree remains dirty and uncommitted (74 status entries at final review, including the original 43). No files were staged, no commit/merge/push occurred. The original audit files and earlier implementation edits remain present. `streaming-v1-verification.md` describes the earlier milestone; its original totals are not newly executed application verification.

Remaining boundaries: no automatic market archive, no executed disposable PostgreSQL test, static-only CLI validation does not read the persisted graph, fixed bounded recovery/REST service capacities, no generalized application hot reload/graph CRUD, and no administrative API/TUI. Incomplete shutdown remains owned in-library but cannot guarantee cleanup after executable process exit. Supported Source subscription profiles must contain enough native arguments to pass preflight. Tests demonstrate bounded observed ownership/gauges and specific cleanup paths, not proof of absence of every possible leak or production exchange failure.
