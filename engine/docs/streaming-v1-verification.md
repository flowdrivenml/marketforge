# Streaming v1 implementation and verification record

The original milestone record below predates the focused hardening pass. Its totals
are historical, not new hardening results. See the final section for this pass.

## Baseline and working-tree ownership

Baseline: `974187dbeacffd771b4f2f93741e3ec903efb0c1`, branch `main`, clean working tree. There were no pre-existing edits to preserve. The commit and branch remain unchanged; all implementation changes are uncommitted. No branch, commit, reset, rebase, merge or push was performed.

Before editing, the six requested live integration targets were compiled successfully with `--no-run`. A full pre-edit execution baseline was **not** recorded for this commit. Post-edit execution results below are not presented as baseline results.

Implementation inspection covered the existing integrity/recovery owners, Source processor/publication/runner, Router, production processing and registry, shared domain/conversion, general configuration, library exports, live binary and integration tests. There was no reusable streaming protocol/client/server in this checkout. The existing general publication configuration contained `socket_path`, `max_connections` and `subscriber_queue_bytes`; it remains the configuration entry point. No deployment TOML file was added or changed, and no alternate loader was introduced.

## Implemented behavior

See [streaming-v1.md](streaming-v1.md) for the public APIs, complete wire contract, exact numeric encoding, limits and guarantees. The runnable [consumer](../examples/stream-consumer.rs) handles market publications, integrity snapshots/updates and termination.

* Authoritative Source trade observations, including an initial coverage-review gap with no accepted records, reach live observers independently from financial publication. Rejected financial admission still fails and does not commit Source acknowledgments. Integrity-only input does not establish readiness, advance watermarks or supply a conversion reference.
* Bounded snapshots and updates use a Router-lock registration cut. Immutable original evidence survives Source → production Merge → production Convert. Splitting a batch preserves one observation identity. Valuations retain the exact evidence of references actually used, independently from subsequent reference updates.
* Source-generation, runtime-generation, book, native sequence, Router position, integrity revision and subscription sequence remain separate. Unknown authority/positions/counts are null. Truncation and unverified history remain explicit. Recovered-record provenance does not assert complete recovery.
* Original financial custody is checked against the retained complete native trade (including decimal scale) or immutable depth allocations. Reusing genuine custody after financial mutation is rejected. Retained allocations are charged conservatively.
* Portable bounded CBOR v1 models and framing support canonical trades, atomic incremental depth operations, derived provenance/valuations, integrity, discovery and subscription boundaries. Every full snapshot is replaced by an ordered omission marker, including embedded derived snapshots; its native levels and valuation arrays never cross the socket.
* The generic session owns existing Router subscriptions with `DemandOwner::Unix`. The Unix adapter supplies filesystem/socket ownership and connection lifecycle. Multiplexing uses one writer, bounded queues and deadlines. Oversize publications fail atomically; unsubscribe provides a completion boundary. Disconnect and shutdown release demand.
* `MarketForgeStreamClient` provides concurrent bounded requests, multiple subscriptions, cancellation-safe receives, deterministic close and nonblocking bounded Drop cleanup. Unexpected connection loss is an error; reconnect/resume is not implemented.

The authority inventory is intentionally limited to owners that exist: the trade integrity controller owns duplicates, bounded numeric identity history, sequence observations and coverage-review conclusions; depth integrity/recovery owns book baseline, recovery reasons and coverage; Source recovery callbacks establish recovered-record provenance; production processors own their ordering/conversion continuity failures. The standalone legacy GapTracker is not connected to Source in this checkout. No transport-side exchange checker or inferred recovery-completeness claim was added.

Terminal cleanup retains a bounded explanation. Stress testing exposed a race between a processing failure fence and publication cleanup; the processor now captures its data conclusion before returning failure. A nonblocking Router test also incorrectly assumed contention could never occur; it now retries only `Busy` under its existing bounded deadline, retaining exact rejection and accepted-count assertions. Source test helpers were updated to observe metadata-only outcomes without counting them as financial events. Configuration roundtrip verification exposed default derived-section serialization; default sections are now omitted consistently.

## Dependencies and configuration

Added `ciborium = 0.2.2`; the lockfile adds ciborium, ciborium-io and ciborium-ll. No other codec or transport was added. The `stream-client` feature enables Tokio without `live` or PostgreSQL. The executed feature tree confirms SQLx/PostgreSQL, reqwest and tokio-tungstenite are absent from the client-only graph. Existing always-on shared dependencies remain unchanged.

Existing publication byte-budget semantics are preserved. Optional `[publication.streaming]` defaults: 64 subscriptions, 1 MiB frames, 32 pending commands, 5 s handshake, 300 s partial-frame assembly, 5 s writes and 3 s shutdown. Validation enforces positive values, hard ceilings and frame/queue consistency. Idle live sessions do not expire for lack of commands. See the protocol document for all allocation, collection, observer, terminal-reserve and socket-permission limits.

## Executed verification

All commands were run from the repository root with the lockfile enforced. Final checks passed:

```sh
cargo check --manifest-path engine/Cargo.toml --locked --no-default-features
cargo check --manifest-path engine/Cargo.toml --locked --no-default-features --features live
cargo check --manifest-path engine/Cargo.toml --locked --no-default-features --features historical
cargo check --manifest-path engine/Cargo.toml --locked --no-default-features --features stream-client
cargo check --manifest-path engine/Cargo.toml --locked --no-default-features --features stream-client --example stream-consumer
cargo tree --manifest-path engine/Cargo.toml --locked --no-default-features --features stream-client -e features
```

The historical check emits five existing dead-code warnings in `marketforge-convert`; it succeeds. No Clippy was run.

Executed integration commands:

```sh
cargo test --manifest-path engine/Cargo.toml --locked --no-default-features --features live --test live_integrity --test live_source --test live_router --test live_processing --test live_derived --test live_config
cargo test --manifest-path engine/Cargo.toml --locked --no-default-features --features "live historical" --test live_derived
cargo test --manifest-path engine/Cargo.toml --locked --no-default-features --test shared_merge --test shared_conversion
cargo test --manifest-path engine/Cargo.toml --locked --no-default-features --features "live stream-client" --test stream_protocol --test stream_client --test stream_transport
cargo test --manifest-path engine/Cargo.toml --locked --no-default-features --features stream-client --test stream_protocol --test stream_client
```

| Target | Passed | Intentionally ignored |
|---|---:|---:|
| live_integrity | 83 | 0 |
| live_source | 38 | 1 |
| live_router | 31 | 0 |
| live_processing | 19 | 0 |
| live_derived, live | 40 | 1 |
| live_derived, live + historical | 40 | 1 |
| live_config | 25 | 0 |
| shared_conversion | 67 | 0 |
| shared_merge | 8 | 0 |
| stream_protocol, live + stream-client | 3 | 0 |
| stream_client, live + stream-client | 2 | 0 |
| stream_transport | 18 | 0 |
| stream_protocol, client only | 3 | 0 |
| stream_client, client only | 2 | 0 |

The Source skip requires explicit PostgreSQL write authorization; the Derived skip is an explicitly opt-in throughput measurement, repeated in two feature builds. Neither was run.

Live library unit filters were compiled and executed: `live::stream::router` **21 passed**, `live::stream::registry` **51 passed**, `live::infrastructure::processing` **52 passed**, `live::stream::source::processor` **16 passed**. No zero-test filter counted toward verification.

Every integration and selected unit executable was compiled before external deadlines. Individually executing those tests (including feature-build duplicates) with a 15-second per-test limit and eight parallel processes produced **519 passed, 3 intentionally ignored, 0 failed, 0 timed out** across 522 selections. Whole integration commands also passed with normal harness parallelism under 90-second suite limits; unit filter executions had 60-second limits.

Ten additional rounds executed all four binaries concurrently, each retaining normal harness parallelism: `stream_transport`, `live_processing`, `live_derived` with live, and `live_derived` with live + historical. **All 40 suite runs passed**, with 30-second per-binary deadlines and no timeouts. These rounds include the final native provenance fields and terminal-race fix. No permanent serialization, arbitrary long sleep or weakened financial assertion was used.

The 18 dedicated transport tests cover real Unix sockets; actual SourceProcessor/Router/production Merge/Convert; late integrity snapshots and subsequent gaps; reference evidence and as-of valuation; depth operation atomicity, replacement omission markers and truthful late tails; mixed derived snapshots with large native/value arrays; recovery without invented completeness; bounded history; generation replacement; metadata-only readiness/watermarks; cancellation, multiplexing, duplicate IDs and unsubscribe boundaries; malformed schemas and partial-frame deadlines; oversize publications; slow local readers and nonreading sockets with an independent progressing client; terminal cleanup; rejected market acknowledgment; finite-replay rejection; configuration limits. Protocol/client tests additionally exercise fragmented reads/writes, multiple frames, mid-frame EOF, exact large numbers, hostile collection bounds, malformed peers and cancelled Subscribe cleanup.

`git diff --check` passed. No public exchange connections, production database writes, historical workloads, slow Historical suites or unrelated benchmarks were executed. Source inspection claims above are distinguished from these executed tests; no cross-platform execution is claimed.

## Remaining boundaries

This is a bounded live tail, without initialized downstream books, full snapshots, historical datasets, bootstrap, durable replay, automatic reconnect, downstream processing acknowledgment or exactly-once claims. Missing authoritative information is explicitly unavailable. Finite replay ancestors are rejected for external live subscriptions.

The existing `marketforge-live` binary is still a placeholder. Applications embed the public owned server constructor around their existing Router; real integration tests exercise that constructor and explicit asynchronous shutdown. No management runtime was created. Unix is the only connector; platform-gated code was tested on Linux, not on other operating systems.

## Changed files and final state

All files below belong to this task. The final tree remains modified/untracked on `main` at the baseline commit, ready for independent review; no changes were committed.

- `engine/Cargo.lock`
- `engine/Cargo.toml`
- `engine/docs/streaming-v1-verification.md`
- `engine/docs/streaming-v1.md`
- `engine/examples/stream-consumer.rs`
- `engine/src/client/mod.rs`
- `engine/src/client/streaming/connection.rs`
- `engine/src/client/streaming/mod.rs`
- `engine/src/client/streaming/subscription.rs`
- `engine/src/lib.rs`
- `engine/src/live/config/general/mod.rs`
- `engine/src/live/config/general/model.rs`
- `engine/src/live/config/general/validation.rs`
- `engine/src/live/integrity/trades/controller.rs`
- `engine/src/live/mod.rs`
- `engine/src/live/stream/processing/conversion.rs`
- `engine/src/live/stream/processing/operations.rs`
- `engine/src/live/stream/processing/pipeline.rs`
- `engine/src/live/stream/processing/service.rs`
- `engine/src/live/stream/registry/graph.rs`
- `engine/src/live/stream/router/admission.rs`
- `engine/src/live/stream/router/event.rs`
- `engine/src/live/stream/router/facade.rs`
- `engine/src/live/stream/router/integrity.rs`
- `engine/src/live/stream/router/source.rs`
- `engine/src/live/stream/router/tests.rs`
- `engine/src/live/stream/source/executor.rs`
- `engine/src/live/stream/source/processor.rs`
- `engine/src/live/stream/source/runner.rs`
- `engine/src/live/stream/source/supervisor.rs`
- `engine/src/live/stream/source/trade_publication.rs`
- `engine/src/live/transport/mod.rs`
- `engine/src/live/transport/session.rs`
- `engine/src/live/transport/streaming.rs`
- `engine/src/live/transport/unix/connection.rs`
- `engine/src/live/transport/unix/mod.rs`
- `engine/src/live/transport/unix/server.rs`
- `engine/src/stream_protocol/codec.rs`
- `engine/src/stream_protocol/integrity.rs`
- `engine/src/stream_protocol/market.rs`
- `engine/src/stream_protocol/message.rs`
- `engine/src/stream_protocol/mod.rs`
- `engine/src/stream_protocol/validation.rs`
- `engine/tests/live/integration/derived/hardening.rs`
- `engine/tests/live/integration/processing/main.rs`
- `engine/tests/live/integration/router/integrity_publication.rs`
- `engine/tests/live/integration/router/main.rs`
- `engine/tests/live/integration/source/pipeline.rs`
- `engine/tests/live/integration/source/recovery.rs`
- `engine/tests/live/integration/source/resources.rs`
- `engine/tests/stream_client.rs`
- `engine/tests/stream_protocol.rs`
- `engine/tests/stream_transport.rs`

## Focused hardening pass — 2026-11-01

### Starting state and scope

Started on `main` at `974187dbeacffd771b4f2f93741e3ec903efb0c1`, with the original streaming implementation already uncommitted. The starting status and tracked binary diff were captured before editing. All existing work, including the user-provided `marketforge-streaming-audit.patch` and `marketforge-streaming-changes.txt`, was preserved. No branch/commit/push or autonomous planning document was used. The placeholder application executable was not changed.

This pass changes only these files relative to that starting implementation:

* `engine/Cargo.toml` — opt-in measurement target; no dependency or lockfile change.
* `engine/src/stream_protocol/mod.rs`, `accounting.rs` — explicit owned-payload charge.
* `engine/src/client/streaming/connection.rs`, `subscription.rs` — reservation lifecycle and observable queued payload counter.
* `engine/src/live/stream/router/integrity.rs`, `facade.rs`, `tests.rs` — nonallocating snapshot sizing, linear selection, bounded receive fairness and unit fixture update.
* `engine/tests/live/integration/router/integrity_publication.rs` — controlled priority/evidence test and old-generation boundary assertions.
* `engine/tests/stream_transport.rs` — delayed production conversion evidence, cancellation, unread subscription churn and partial-write shutdown tests.
* `engine/tests/stream_performance.rs` — two opt-in local measurements.
* `engine/docs/streaming-v1.md`, `streaming-v1-verification.md` — accounting/ordering/cleanup contract and this report.

### Findings classified

**Confirmed correctness/lifecycle accounting issue:** manual payload-byte reservations were not released when an unread subscription receiver was dropped. This left a stale observable counter, even though dropping the receiver freed its payloads; it was not evidence of an allocation leak. Reservations now travel with each queued message and release exactly once on receive or destruction. Failed insertion, overflow, cancelled receives, receiver Drop, explicit close and connection-failure draining are covered. Exact-limit admission, over-limit rejection, checked arithmetic overflow and full cleanup-channel fallback have direct tests.

**Confirmed fairness defect under a serviceable controlled workload:** a fresh integrity update before each receive indefinitely prevented queued market publications from being delivered. The regression failed with no market deliveries across 24 receives. Active observers now service one queued market publication after eight consecutive ordinary integrity updates. Initial/final snapshots retain priority. Separate queues, event ordering and the coherent subscription cut are preserved.

**Measured performance issue:** an oversized 128-Source snapshot repeatedly serialized the full view while removing individual history/source details. The replacement counts JSON bytes without allocating output and selects omissions in linear passes, with one prefix drain instead of repeated front-removal. Update charging also counts without allocating a JSON buffer. The 256 KiB conservative JSON-size contract and explicit omission flags remain; this is not a structural-Rust-size substitution. Coherent snapshot construction remains inside the original Router lock; no await, callback or socket operation was moved into it. Remaining bounded cloning and counting work is intentional.

**Intentional behavior verified:** newer integrity can overtake older queued market data. Controlled Source and production Merge/Convert tests retain original event and actually-used reference evidence, preserve market order, identify repeated batch evidence and do not assert scope-contiguous Router-global revisions. Generation replacement does not attach old evidence to a new runtime; terminal explanation retention remains covered by the existing tests.

**Cleanup verified, without claiming universal leak freedom:** a finite campaign uses 12 rounds × three disposable clients × two subscriptions (72 disposable subscriptions), with unread messages, explicit close and Drop, while an independent client receives the next market position each round. Router subscription ownership returns to its baseline after every round and zero at shutdown. Existing Router ownership couples subscription removal to demand release; no RSS/FD/allocator-leak claim follows from those counts. Controlled cancellation covers before response (fake peer), after real attachment and after acceptance with initial messages pending. Partial publication shutdown races explicit client close/EOF. Full client command-channel cleanup triggers connection shutdown without spawning an unbounded cleanup backlog. Existing unsubscribe completion and identifier-reuse tests remain passing.

### Retention contract

The client now charges the inline StreamMessage once, String capacities, Vec capacity × inline element size, and nested owned allocations in populated elements. It accounts for optional decimals, native trade fields, depth operations, provenance, reference evidence, rational coefficient strings and conversion paths. Arithmetic is checked; overflow rejects admission. The charge never serializes JSON/CBOR. The accounting tests include 1,024 depth operations in a 2,048-capacity vector, excess reference/evidence/path/value capacity, many short strings and a permitted 4,096-byte string.

The charge covers the documented application-owned payload representation, excluding allocator metadata/rounding, channel blocks and messages handed to the application. Queue metadata/slot overhead is separately count-bounded. `queued_payload_bytes()` measures reservations, not RSS. The reader-to-session decoded-envelope channel has a separate count/collection/frame bound; its coarse representation ceiling and active-frame allowance are documented in streaming-v1.md. Per-subscription queue bytes do not cover every client allocation. The server encoded-frame budget is unchanged.

### Reproducible local measurement

Machine: AMD Ryzen 9 9950X (16 cores / 32 logical CPUs), Linux 7.0.0-38-generic, rustc 1.94.1, Cargo **test/dev unoptimized** profile, four Tokio runtime threads, no CPU pinning. These are short local samples, not production capacity claims. Both variants use the same workload, configuration and profile, executed serially with `--test-threads=1`; normal test parallelism is used for correctness campaigns.

The temporary reference copy restores the original snapshot trimming/JSON-buffer update charge and original JSON×2+256 client charging expression, while keeping the same harness and new cleanup reservation mechanics. It is a comparison of the inspected hot paths, not a byte-identical archival checkout of every pre-hardening file. No tracked/untracked source was reset. A shared-target artifact collision was detected because an allegedly optimized measurement still emitted the reference-only unused-counter warning; that comparison was discarded. Project build artifacts were cleaned and the current source rebuilt before the retained after-results and final verification.

The first oversized measurement attempted 100 large attachments and exceeded its 60-second execution deadline: a failed measurement, not a passing result. The bounded comparable workload was reduced to five large attachments in both variants. The initial smoke harness also rejected a one-input Merge and an excessive CBOR-item depth case; the retained comparison uses valid inputs. No timeout is hidden by a later test pass.

Workload: two and 128 simultaneously bound Sources, each initially containing 64 authoritative sequence details; bounded history is populated. Twenty small-scope and five maximum-scope attachments each register and receive their initial snapshot, concurrent with 100 market admissions. Every produced snapshot is checked against 256 KiB JSON. The 128-Source case exercises the supported source-count boundary. Timings include registration/graph traversal/snapshot construction/receive and the harness JSON-bound assertion, **not direct lock-hold time**. Source timing includes fixture publication construction, Busy retry and scheduling, and is **end-to-end admission latency**, not an isolated mutex or financial-processing measurement.

| Snapshot workload | Reference median / p95 / p99 ms | After median / p95 / p99 ms |
|---|---|---|
| 2 Sources, attach + initial receive | 0.953 / 1.500 / 1.500 | 0.829 / 1.311 / 1.311 |
| 128 Sources, attach + initial receive | 1813.088 / 1869.450 / 1869.450 | 42.912 / 43.143 / 43.143 |
| Source admission concurrent with 2-Source attachment | 0.196 / 0.729 / 0.972 | 0.182 / 0.557 / 0.757 |
| Source admission concurrent with 128-Source attachment | 0.221 / 1798.291 / 1864.527 | 0.204 / 0.396 / 37.509 |

Only five large-scope samples were collected; p95 and p99 both select the maximum. This demonstrates the avoidable trimming cost, not a stable tail-latency distribution. Large snapshot assembly still takes roughly 43 ms in this debug build and remains a limitation under contention.

Transport-only codec work preconstructs payloads, then encodes/decodes 200 complete publications per workload. It does not calculate valuations, merge records or normalize data. A depth publication is one record containing many operations. The valuation/evidence workload has 32 preconstructed derived records. Bytes below are encoded CBOR payload bytes, excluding length prefixes.

| Workload | Reference publications/s / records/s / bytes/s | After publications/s / records/s / bytes/s |
|---|---|---|
| Trade, 1 record | 20,792 / 20,792 / 23,598,543 | 20,330 / 20,330 / 23,074,157 |
| Trade, 64 records | 428 / 27,391 / 24,941,199 | 432 / 27,650 / 25,177,530 |
| Atomic depth, 256 operations | 469 / 469 / 21,239,702 | 480 / 480 / 21,762,398 |
| Atomic depth, 1,500 operations | 83 / 83 / 21,720,615 | 80 / 80 / 20,736,943 |
| Derived valuation/evidence, 32 records | 443 / 14,177 / 24,371,841 | 409 / 13,102 / 22,524,527 |

Real Unix delivery: 200 sequential single-trade publications with a healthy client and a second client that deliberately does not drain its four-message application queue. The slow subscription overflows independently. This measurement includes fixture publication construction, Router admission, projection, socket delivery, client decode, receive, and an extra market encoding for byte counting; no financial calculation is included. It is not pure socket throughput.

| Unix sample | Reference | After |
|---|---:|---:|
| Publications/s = records/s | 1,803 | 1,589 |
| Market CBOR bytes/s (integrity traffic excluded) | 1,980,207 | 1,744,931 |
| Publish → recv median / p95 / p99 ms | 0.541 / 0.694 / 0.764 | 0.594 / 0.905 / 1.229 |
| Maximum sampled healthy queue charge | 2,810 | 1,763 |
| Maximum sampled slow queue charge | 8,572 | 4,476 |

The queue-charge units changed from a JSON heuristic to structural capacity charge; those numbers are **not directly comparable memory measurements**. Sampling can miss a higher instantaneous occupancy. Codec results are mixed; Unix throughput declined in this short sample. No general transport throughput improvement is claimed and no correctness limit was relaxed to chase throughput. The separate nonreading-socket deadline test continues to verify connection-level failure and independent-client progress.

Run the opt-in measurements (compile before the external 60-second execution limit):

```sh
cargo test --manifest-path engine/Cargo.toml --locked --no-default-features --features "live stream-client" --test stream_performance --no-run
cargo test --manifest-path engine/Cargo.toml --locked --no-default-features --features "live stream-client" --test stream_performance -- --ignored --nocapture --test-threads=1
```

The comparison hot-path reference can be reproduced in a separate copy by restoring the previously documented `serde_json::to_vec` snapshot trimming loop (`remove(0)`, then clear details/pop sources), JSON-buffer update charging (`len + 128`), and JSON×2+256 client charge. Use a separate Cargo target directory for reference builds to avoid artifact identity collisions. Neither measurement is a timing-sensitive correctness gate.

### Newly executed final verification

The following results were newly executed for this pass; original milestone totals above are not counted again. The focused starting compile covered stream_protocol, stream_client, stream_transport and live_router. The first focused suite execution occurred after the initial accounting edit, so it is not claimed as a pristine pre-edit execution baseline.

All five requested checks passed, using the exact commands below. Client-only feature-tree inspection again excludes SQLx/PostgreSQL, reqwest and tokio-tungstenite. Existing dead-code warnings remain; no Clippy was run.

```sh
cargo check --manifest-path engine/Cargo.toml --locked --no-default-features
cargo check --manifest-path engine/Cargo.toml --locked --no-default-features --features live
cargo check --manifest-path engine/Cargo.toml --locked --no-default-features --features historical
cargo check --manifest-path engine/Cargo.toml --locked --no-default-features --features stream-client
cargo check --manifest-path engine/Cargo.toml --locked --no-default-features --features stream-client --example stream-consumer
cargo tree --manifest-path engine/Cargo.toml --locked --no-default-features --features stream-client -e features
cargo test --manifest-path engine/Cargo.toml --locked --no-default-features --features stream-client --test stream_protocol --test stream_client
cargo test --manifest-path engine/Cargo.toml --locked --no-default-features --features "live stream-client" --test stream_protocol --test stream_client --test stream_transport
cargo test --manifest-path engine/Cargo.toml --locked --no-default-features --features live --test live_integrity --test live_source --test live_router --test live_processing --test live_derived --test live_config
cargo test --manifest-path engine/Cargo.toml --locked --no-default-features --features "live historical" --test live_derived
```

| Newly executed target | Passed | Intentional skips |
|---|---:|---:|
| stream_protocol, client only | 3 | 0 |
| stream_client, client only | 2 | 0 |
| stream_protocol, live + client | 3 | 0 |
| stream_client, live + client | 2 | 0 |
| stream_transport | 22 | 0 |
| live_integrity | 83 | 0 |
| live_source | 38 | 1 |
| live_router | 32 | 0 |
| live_processing | 19 | 0 |
| live_derived, live | 40 | 1 |
| live_config | 25 | 0 |
| live_derived, live + historical | 40 | 1 |

The Source PostgreSQL write test and both feature-build instances of the Derived throughput test remain intentionally skipped.

Explicit library filters used `cargo test --manifest-path engine/Cargo.toml --locked --no-default-features --features "live stream-client" --lib FILTER`:

| FILTER | Passed | Intentional skips |
|---|---:|---:|
| stream_protocol::accounting | 3 | 0 |
| client::streaming::connection::accounting_tests | 2 | 0 |
| client::streaming::connection::lifecycle_accounting_tests | 2 | 0 |
| live::stream::router | 22 | 0 |
| live::stream::registry | 51 | 0 |
| live::infrastructure::processing | 52 | 0 |
| live::stream::processing | 6 | 1 |

The additional unit skip is `operations::selection_measurement::selector_cpu_benchmark`, an unrelated opt-in selection measurement. It was not run. All accounting filters select actual tests.

Every executable was built with `--no-run --message-format=json` before applying external execution limits. Integration suites used 90-second limits; explicit unit filters used 60 seconds. Individual exact-name runs across all these integration and selected unit binaries, including feature duplicates, used 15 seconds per test and eight parallel worker processes: **451 selections, 447 passed, four intentional skips, zero failures or timeouts**. No zero-test result was accepted.

One final ten-round campaign used normal harness parallelism within each binary and up to four concurrent binary invocations. Each round covered seven integration binaries: both client feature builds, transport, Router, processing and both Derived feature builds. Five additional library invocations per round covered accounting, client connection lifecycle, Router, stream processing and infrastructure processing. **All 120 invocations passed** (70 integration + 50 library-filter runs), with 30-second per-invocation limits. There were no final intermittent failures or timeouts. The earlier shared-build-artifact diagnostic interrupted preparation before this retained final campaign; it was not a second successful campaign waved in as a correction.

The two opt-in measurement tests also executed successfully. `git diff --check` passed. No production database, public exchange, historical dataset, unrelated benchmark, application startup or extra transport was exercised.

Final state: `main`, unchanged commit `974187dbeacffd771b4f2f93741e3ec903efb0c1`, original implementation and this pass both uncommitted. Starting modifications and user audit files remain present. No commit, push, reset, rebase or merge occurred.

Remaining limitations: snapshot cloning/serialization counting still performs bounded work under the Router lock; the stress-scope debug sample remains about 43 ms. Reader-stage retention has a separate coarse count/collection bound, not the subscription-byte budget. Accounting excludes allocator/RSS costs. Queue high-water samples are not continuous instrumentation. The performance comparison is short, unpinned and hot-path-reference based; no production throughput promise follows. Existing live-tail/no-bootstrap/no-replay guarantees are unchanged.

Application integration is now implemented separately in [Live application v1](live-application-v1.md).
That report records its own baseline, executed tests, metrics and lifecycle guarantees.
The original streaming totals above remain historical results from the streaming milestone.
