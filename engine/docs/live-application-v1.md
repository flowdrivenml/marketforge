# Live application v1

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

## Implemented ownership and public API

`live::application::LiveApplication` is the Unix headless service owner. The library does not initialize global logging. `LiveApplicationOptions` accepts validated `GeneralConfig`, resolved `PersistedStreamDefinition`/`StreamDependency` collections, immutable exchange/format stores, shared `ExchangeLimiters`, and a compiled `WebSocketManagerRuntime`. `compile_limiters` and `compile_websocket` are the executable/embedding construction helpers; REST and WebSocket must share the same limiter manager. REST exchange weights must be supplied from a verified operator quota; missing weights are rejected, not guessed from a stream.

```rust,ignore
LiveApplication::validate(&options)?; // no exchange acquisition
let mut application = LiveApplication::start(options).await?;
let handle = application.handle();
let status = handle.status();
let metrics = handle.latest_metrics(); // completed snapshot; never collects
handle.demand("btc_merged", DemandOwner::Manual {
    session_id: "embedded-consumer".into(),
}, true).await?;
handle.retry_stream("btc_merged").await?;
let report = application.shutdown().await;
```

`status()` is a bounded copied read, including lifecycle generations, original Source readiness, endpoint address, and final shutdown report. It does not expose mutable registries. `demand`, `retry_stream`, and `request_shutdown` return **admission** acknowledgments, not market readiness or completed retirement. Unsupported `StreamAction` variants are rejected; only Restart maps to retry. Manual demand owners require a 1–256 byte session ID; command stream IDs are bounded at 4096 bytes. Accepted operations belong to the application even if the caller cancels. A full command queue returns an explicit capacity/closed error. Shutdown closes admission and rejects queued, unaccepted operations.

The Source supervisor now shares the same `Arc<Mutex<StreamRuntimeRegistry>>` as ProcessingService. The supervisor remains responsible for Source transitions and fencing; ProcessingService retains its CPU pool, subscriptions and lifecycle transitions. The immutable graph and existing LifecyclePlanner determine demand closure and retirement order. Application reconciliation is a coalescing periodic inspection, not a second planner. `DemandOwner::Processing` explicitly identifies dependency ownership and is excluded as an independent demand root. AlwaysOn definitions and external Unix/Manual/Direct owners remain roots. Shared ancestors retire only after their last root disappears.

Derived consumers prepare/attach before cold Source activation. A late internal depth attachment requests an untimed `CurrentBookHandoff` snapshot from the existing synchronized Source integrity/book owner. Registration precedes notification under the Router cut. The Source actor serializes handoff with native mutation, preserves book/runtime identity, and uses ordinary publication/custody. Existing consumers do not reconnect. This is a current authoritative baseline, not historical bootstrap. Handoff controls do not increment canonical market-event totals. External Unix projection still emits only an omission marker, never book arrays. An unavailable baseline remains an explicit readiness failure; no synchronization is assumed.

A returned application means configured services are bound and serving, not that all streams are Running. Quiet Sources remain AwaitingData until authoritative usable output; the initial deadline also covers Recovering/Degraded before first readiness. Failed streams remain occupied and are not blindly retried. Explicit retry retires affected descendants in existing dependency order, respects demand, allocates fresh generations, and leaves independent owners intact. Generation-fenced old completions cannot update replacements. Processing terminal reports are explicitly discarded after bounded accounting; retained work is never force-flushed as finite event-time completion.

## Limits, startup and shutdown

Existing general configuration remains the loader and authority. New optional `[runtime.application]` defaults:

| Setting | Default | Validation |
|---|---:|---|
| command_capacity | 64 | 1–1024 |
| max_definitions | 256 | 1–1024 |
| max_lifecycle_operations | 260 | at least definitions + Source concurrency, at most 2048 |
| source_start_concurrency | 4 | 1–64 |
| reconcile_interval_ms | 25 | 1–60000 |
| source_readiness_timeout_ms | 30000 | 1–3600000 |

The graph is immutable for this owner lifetime. Derived definitions require `runtime.processing`; without it a Source-only application uses one default idle ProcessingService/pool for a uniform shutdown owner. Router subscription capacity is preflighted from configured external slots plus declared dependency inputs, capped at 16384. Producer count is capped by max_definitions. Manual demand registration is bounded at 4096 owners; the shared DemandRegistry has a hard 65536-entry ceiling. Existing processing, publication, queue, frame and deadline settings retain their meanings. REST recovery currently uses fixed bounded limits: 128 commands/requests, 64 jobs, 8 concurrent jobs; REST has 8 concurrent requests and a 4 MiB response bound.

Graph, schemas, Source subscription parameters, shared limiter identity, recording support, and processing capacities are checked before acquisition. Binding failures roll back previously bound services and owners. Native catalog parameters not derivable from definitions/configuration (for example an ambiguous exchange-specific market argument) cause preflight failure; the application does not guess them. `StreamDefinitionRepository::snapshot_bounded` reads definitions/dependencies in one read-only repeatable-read transaction, with LIMIT+1 checks. Restored runtime state is fresh/stopped, with no persisted sockets, demand or authorities. The executable bounds database connect and coherent restore separately by `runtime.shutdown_timeout_seconds`; errors redact connection/query details.

Shutdown stops admissions/reconciliation, closes Unix sessions, retires processing and Source ownership, then shuts down WebSocket/recovery infrastructure. CPU retirement is awaited asynchronously by existing owners, never joined on an async executor thread. Executing CPU code is not forcibly cancellable. Final collection occurs after cleanup and before exporter retirement. Handles can read the retained final snapshot/report. `ShutdownReport.complete` requires completed cleanup without errors. `discarded_publications` counts retained work disposed in the final report drain, not records committed or earlier retired work. Errors are capped at 64 entries of 512 bytes.

A timed-out `shutdown()` returns `complete=false, cleanup_continues=true`, increments the incomplete-wait counter, and retains the task; another wait can observe completion. Drop requests owned cleanup without blocking. The executable exits nonzero for incomplete/fatal shutdown; its Tokio runtime has a one-second shutdown wait, so process exit may abandon still-incomplete work. This is reported, not described as a successful drain. SIGINT and SIGTERM follow the same explicit protocol.

## Metrics contracts and instrumentation inventory

Exactly one application task calls `MetricsSnapshotCollector::collect()` periodically and once at shutdown. The collector assigns revision/time and consumes latency intervals. Snapshot handles are captured together under a short Router lock and collected outside lifecycle/Router locks. Readers, HTTP requests and status reads use `latest()`. No asynchronous metrics message is sent per market event. Sampling uses monotonic elapsed time; first observations and generation changes have unavailable rates. The latest immutable snapshot and collector's current-generation rate state are bounded; retired metric handles are not an incident/time-series archive.

In the table, **application** means cumulative for one application owner (one owner per executable process), reset on new application construction; removal of streams does not reduce these totals. **Generation** counters reset on replacement and disappear from the next snapshot after retirement. Gauges are observations, not cumulative outcomes. The Prometheus prefix is `marketforge_`; native snapshots expose additional optional rate/latency fields even when no corresponding exporter family exists.

| Measurements / unit | Authoritative site and success boundary | Type / lifetime / availability | Regression coverage |
|---|---|---|---|
| uptime seconds; lifecycle starts/stops/failures; reconciliations; incomplete waits | Registry begin_start/begin_stop/new Failed transition; application inspection/deadline | uptime gauge; outcome counters/application; failure counted once per transition | application AlwaysOn/retry, readiness deadline, partial-write shutdown; registry units |
| native_messages_total, normalization_errors_total, recovery_requests_total, source_reacquisitions_process_total | Source Text/Binary entry **before JSON decode**; failed decode/normalization; recovery registration accepted; confirmed Source reacquisition | counters/application; control frames excluded; reacquisition is **success**, not attempted reconnect | application malformed payload/retry; Source recovery/executor tests |
| source_messages_total, source_normalization_errors_total, source_reacquisitions_total | same observations, same generation metrics handle installed before executor run | counters/generation; no duplicate instrumentation | Source units and application generation-isolation |
| source_events_total / derived_events_total; stream_accepted_events_total | successful Router admission, not calculation or temporary Busy | counters/application and generation respectively; one trade=one event, one complete depth snapshot/delta=one event | 10 native trades/3 clients test; Source/Derived suites |
| depth_events_total / depth_levels_total | Router accepted native market shape | counters/generation; levels counted separately; invalidation, integrity-only, completion and current-book handoff count zero | application cold/late depth: 1 depth update, 2 levels; Router contracts |
| derived_input_events_total | work taken by CPU processor, including conversion reference inputs | counter/generation; references do not become converted output | actual Merge→Convert application test; processing/Derived suites |
| derived_calculated_events_total, derived_mailbox_events_total | non-reference financial result calculated once; output mailbox successful admission | counters/generation, distinct from Router acceptance; Busy retry does not recalculate/count again | processing output/backpressure units and Derived retry coverage |
| processing interval mean/max/last seconds and sample count | Source processing / CPU processing turns using Instant | explicit interval gauges/generation; no sample means mean/max absent; last may be older; no invented histogram/percentiles | metrics latency/collector units; repeated application scrapes |
| conversion_failures_total, ordering_failures_total | actual failed valuation call; emission-frontier regression | counters/generation; bounded classifications, no arbitrary error labels | processing/Derived error suites |
| derived_retained_bytes; native input health, queue and retained-event fields | existing ProcessingService status captured by sampler | gauges/generation; no publication retained by metrics | metrics units; application retirement |
| router_accepted_publications_total, router ingress/consumer publications and bytes; router_capacity_failures_total | existing Router status; ingress rejection and consumer queue-capacity termination | acceptance/capacity counters/application, occupancy gauges; publications include controls, market totals do not | Router slow-consumer capacity regression, existing Router suites |
| publication connections/subscriptions; queued_frames/queued_bytes; events_queued_total; frames/bytes_written_total; queue/frame-capacity/write failures | RAII connection/subscription/frame reservations; queue admission; whole-frame write_all completion including prefix | occupancy gauges/application; outcomes counters/application; event fanout increases admissions only; partially failed bytes not claimed as completed | application 10→30 fanout and stalled writer; transport cancellation/overflow suites |
| source_delay_seconds; native last-message/event timestamps | local **Source-processing entry** minus current native exchange event time | signed optional gauge/generation; not network latency or original transport receipt; absent/overflow preserved; depth delay unavailable | Source metric optional-delay units; runtime normalizer tests |
| limiter pending/budgets; websocket slots/streams and availability | existing shared infrastructure APIs during sampling; WS observation bounded at 50ms | gauges/application; failed observation is unavailable, not zero | application pipeline and metrics units |
| recording_available; committed records, failed attempts, pending records | recorder control queue admission + RAII in-flight retention; complete writer confirmation; failed/ambiguous attempt | availability gauge; counters/recorder owner; pending occupancy includes queue and in-flight; absent recorder omits outcomes | controlled real worker writer seam: pending/confirmed/failure/release |
| snapshot revision/time, age, collector duration, sampling duration/failures | collector completed snapshot; application sampling service | gauges plus failure counter/application; repeated reads do not reset/refresh anything; age includes encoding-cache staleness | exporter limits/no-collection test, collector units |
| process resident bytes / CPU cores and availability | periodic Linux procfs sampler | optional gauges; first CPU unavailable; native 100%=one logical core, exporter divides by 100; unsupported platforms omit observations | runtime optional-value units, exporter unavailable regression |

Legacy `events_produced_total` fields mean **Router-accepted market output in production**, not merely successful calculation. The Derived fields explicitly distinguish calculation and mailbox admission. `queued_bytes_total` remains a legacy native occupancy field; its exporter name is `publication_queued_bytes` and TYPE gauge. Metrics do not add repeated integrity references as gap incidents. Integrity, custody, financial values and publication acknowledgments remain governed by their existing owners, never metrics.

Stream labels are restricted to configured definitions (max_definitions), with fixed `kind` values. Generation is a gauge value, never a label. No connection, operation, trade, sequence or arbitrary reason labels are exported. Generation-specific counters may reset on replacement; retired series are removed rather than cached indefinitely. Process/application totals are updated at actual boundaries rather than summing cumulative per-stream snapshots repeatedly.

Metrics disabled means no collector/export service and `latest_metrics() == None`; lightweight owner counters still exist. Prometheus enabled with metrics disabled is rejected. Disabled recording is unavailable, not a fabricated successful zero-commit recorder. Unsupported process observations remain explicitly unavailable.

## Prometheus HTTP

Hyper HTTP/1 serves only GET at `metrics.prometheus.path`; this is not an administrative API. It reads completed snapshots and never queries the database, traverses books or calls collect. Correct HELP/TYPE declarations, escaping, optional omission and base seconds/bytes are used. There are no synthesized percentiles/histograms.

The existing enabled/listen_address/path/max_concurrent_requests/max_response_bytes/cache_interval_ms settings are implemented. At most 128 concurrent **connections**, including incomplete headers and blocked writes; path <=256 bytes, response <=16 MiB, concurrency×response <=64 MiB, cache interval <=60s. HTTP has 32 headers, 8192-byte parser buffer, two-second header deadline, five-second whole-connection deadline, and no keepalive. Excess connections are closed. Invalid methods/paths return 404, request bodies 400, missing snapshots or oversized exposition 503. A too-large body never becomes a truncated successful exposition. Cache encoding is independent of sampling and bounded to one body plus bounded active response copies. Snapshot timestamps are retained on cache reuse. The age gauge is evaluated at encoding time and is also cached; use the snapshot wall-clock timestamp to assess current age between re-encodings. A cached age gauge can lag by up to cache_interval_ms and must not be interpreted as a newly measured age on each scrape.

The listener binds exactly the configured address. The sanitized example uses 127.0.0.1:9091; do not broaden exposure unintentionally. The application does not start the retained configuration's administrative control endpoint or a TUI.

## Recording and database boundary

No faithful Router→recorder adapter or durable recording identity allocator exists for heterogeneous Derived data. `recording.enabled=true` is therefore rejected **before acquisition**, with an explicit explanation. Definition restoration still requires PostgreSQL in normal executable mode. Recording is not silently ignored and Router generation/sequence are not reused as durable positions.

The existing PostgresRecorder remains available independently. `RecordingBatchWriter` is the narrow production-worker seam; Postgres writer and controlled integration writer use the same queue/worker/reservation code. Only confirmed complete commits increment written totals. Failed/ambiguous writes terminate the worker and release pending reservations without claiming persistence. Real SQL commit/migration behavior requires a disposable database and has not been executed here.

Existing migrations are `src/shared/persistence/postgres/migrations/0001_create_live_tables.sql` and `0002_create_stream_definitions.sql`. Have an operator provision the intended database/schema through the repository's existing migration path and review the persisted graph before launch. The executable never automatically resets or migrates a database. Supply `MARKETFORGE_DATABASE_URL` through the environment; do not print it in commands/logs. The repeatable-read restoration path is source-inspected and compiled, not database-verified in this run.

## Launch and inspect

From the repository root, build and perform static offline validation for the example Binance profile (the quota below must be replaced with a verified deployment quota):

```sh
cargo build --manifest-path engine/Cargo.toml --locked --no-default-features \
  --features live --bin marketforge-live
engine/target/debug/marketforge-live --validate-only \
  --config-dir data/.config/live --general engine/examples/live/general.toml \
  --profiles binance_spot --rest-weight binance_spot=1200/60
```

Validation checks static configuration/formats/limiter/WS compilation only; it deliberately says persisted definitions were **not loaded**, and performs no DB/network access. To run the restored graph, export the database variable securely and then:

```sh
install -d -m 700 data/.live
engine/target/debug/marketforge-live \
  --config-dir data/.config/live --general engine/examples/live/general.toml \
  --rest-weight binance_spot=1200/60
```

In another terminal:

```sh
curl --fail http://127.0.0.1:9091/metrics
cargo run --manifest-path engine/Cargo.toml --locked --no-default-features \
  --features stream-client --example stream-consumer -- \
  data/.live/marketforge-data.sock EXISTING_STREAM_ID
```

The socket parent must satisfy the existing Unix server's ownership/permission checks; inspect `publication.socket_path` in the example and create a private directory before launch. `--profiles` only restricts static/empty diagnostics; normal profiles come from enabled persisted Sources. Give all selected weighted profiles explicit quotas. `--empty-definitions` is a deliberate empty resolved-graph diagnostics mode: it starts the same application/export/Unix services without restoring a DB graph or acquiring exchange demand. It is useful for signal/binding checks, not a persisted-data mode or alternate backend. Logging is structured stderr at the existing configured level; lifecycle logs include stream/generation/reason, not each market event.

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
