# Streaming v1 checkout review and verification — 2026-11-02

## Baseline and ownership

Started on `main` at `91f4e42fe386b0fd28f72f872363ac623b657a72`, with a clean working tree. There were no pre-existing edits to preserve. This checkout already contained the integrity publication contract, portable CBOR protocol, Unix streaming server, Rust client, consumer example, application integration, and dedicated integration targets. Those implementations were reused, not recreated. The older implementation records describe earlier checkouts; their test totals are not this review's results.

Before production edits, compiled and executed these nine targets with `--locked --no-default-features --features "live stream-client"`: `live_config` (25 passed), `live_derived` (40 passed, 1 ignored), `live_integrity` (83 passed), `live_processing` (19 passed), `live_router` (32 passed), `live_source` (38 passed, 1 ignored), `stream_client` (2 passed), `stream_protocol` (3 passed), and `stream_transport` (22 passed). Total: **264 passed, 2 intentionally ignored, no failures**. Executables were compiled before applying 60-second suite deadlines.

Inspected the integrity and recovery owners, Source processor/publication adapters, Router integrity/admission/custody paths, production Merge/Convert, registry demand, shared canonical book/trade and conversion types, general configuration/loading, library exports, application binary, portable protocol, session/Unix adapter, client, and relevant tests. No autonomous planning documents were consulted or changed.

Existing configuration is loaded through `GeneralConfig::load`, with a sanitized example at `engine/examples/live/general.toml`. It specifies `publication.socket_path = "data/.live/marketforge-data.sock"`, `max_connections = 64`, and `subscriber_queue_bytes = 16777216`. Optional streaming limits already default to 64 subscriptions, 1 MiB frames, 32 pending commands, 5-second handshake/write deadlines, 300-second partial-frame assembly, and 3-second shutdown. The derived data-stall default was 5 seconds. No deployed configuration file, codec dependency, feature declaration, or lockfile was changed.

## Existing behavior inspected and exercised

The public API and wire format are documented in [streaming-v1.md](streaming-v1.md). The runnable Rust [consumer](../examples/stream-consumer.rs) handles both market data and integrity information.

* The trade controller owns numeric discontinuity/regression observations, duplicates, and bounded identity-window coverage. The depth controller owns continuity decisions, book baseline/coverage and recovery-required conclusions. Source recovery establishes recovered-record provenance without asserting complete exchange history.
* Source forwards first coverage-review observations and empty accepted metadata through an integrity-only Router path. Pending holds, obsolete generations and rejected validation outcomes are not fabricated into market publications. Integrity-only input does not establish market readiness, advance watermarks, or supply a conversion reference; failed financial admission remains failed.
* Router snapshots and observer registration share a lock cut. Source attribution, runtime/source/book generations, native sequences, publication sequence, integrity revision and subscription sequence remain distinct. Unknown authority and missing-record counts remain unavailable. Truncation is explicit.
* Original immutable evidence survives production Merge and Convert, including the actual as-of conversion reference evidence. Repeated records from one publication retain the same evidence identity. Native custody already checks altered prices, sides, quantities and depth allocation identity; the negative custody tests executed successfully.
* CBOR messages use explicit version/schema addressing and bounded big-endian length framing. Exact decimal coefficient/scale and rational witness values are preserved. Internal snapshots become ordered omission markers with no native or converted level arrays. Late subscribers receive a live tail with `client_book_initialized = false`.
* The transport uses existing Router subscriptions and `DemandOwner::Unix`, a single coordinated connection writer, bounded queues and deadlines, explicit unsubscribe boundaries, and owned shutdown. The client has bounded request/subscription state and cancellation-safe receiving. Socket disconnects are errors, without automatic replay/reconnection.

These are source-inspection findings backed by executed tests, not claims that this review newly implemented all of those components.

## Changes in this review

1. `engine/src/live/transport/unix/server.rs`: validate every parent-path prefix, rejecting indirect ancestor symlinks and `..` traversal. Previously only the immediate parent was checked. The new lifecycle regression failed on an indirect symlink before the correction. Existing endpoint rejection, 0600 socket permissions and device/inode cleanup remain in use.
2. `engine/tests/stream_transport.rs`: real-socket lifecycle coverage for indirect symlink rejection, preservation of stale sockets, symlink endpoints and regular files, active endpoint rejection, restrictive permissions, continued service of the original endpoint, preservation of a replacement file during shutdown, and ordinary socket cleanup.
3. `engine/src/live/config/general/model.rs` and `engine/src/live/stream/processing/service.rs`: separate processor startup readiness from the data-stall deadline. Added `DerivedProcessingConfig::startup_timeout_ms`, default 5000, validated in 1..=86,400,000. Previously startup reused `stall_timeout_ms`; short data-stall settings therefore also shortened construction/readiness. Default behavior remains five seconds; existing configurations specifying only a custom stall deadline now inherit a separate five-second startup budget. Financial processing, watermark/stall policy, and the scheduler algorithms are unchanged.
4. `engine/tests/live/integration/config/processing.rs`: roundtrip and reject invalid startup budgets.
5. `engine/tests/live/integration/derived/hardening.rs`: hold readiness explicitly behind a channel beyond a short data-stall budget, then release it and verify successful startup and cleanup.
6. `engine/docs/streaming-v1.md` and this report: document the actual path policy, separate startup budget, findings and verification.

No dependency, lockfile, publication queue-budget interpretation, transport family, management API, runtime-health publication, historical access, bootstrap or financial algorithm was added.

## Failures investigated

The initial ten-round stress run found one pre-existing intermittent failure: `hardening::idle_identity_conversion_survives_computation_timeout` failed in round 7 with `required input readiness deadline expired`. Its fixture uses a 50 ms data-stall limit. Inspection showed that the same limit incorrectly governed asynchronous startup. The separate startup budget corrects that coupling while preserving the short computation-stall assertions. The new gated test exercises the distinction directly; subsequent successful runs alone were not treated as a correction.

The first version of the new gated test accidentally declared a single-input Merge and was rejected by graph validation. The fixture was corrected to two declared Source inputs. No assertion was removed or weakened. Intermediate failed runs are retained in the raw logs and excluded from the final passing totals.

## Executed final verification

All commands ran from the repository root, with the lockfile enforced. Checks passed:

```sh
cargo check --manifest-path engine/Cargo.toml --locked --no-default-features
cargo check --manifest-path engine/Cargo.toml --locked --no-default-features --features live
cargo check --manifest-path engine/Cargo.toml --locked --no-default-features --features historical
cargo check --manifest-path engine/Cargo.toml --locked --no-default-features --features stream-client
cargo check --manifest-path engine/Cargo.toml --locked --no-default-features --features stream-client --example stream-consumer
cargo tree --manifest-path engine/Cargo.toml --locked --no-default-features --features stream-client -e features
```

Inspection of the executed client-only dependency tree found no SQLx/PostgreSQL, reqwest, or tokio-tungstenite. Existing shared dependencies remain present. Historical checking emits five existing dead-code warnings in `marketforge-convert`; checking succeeds.

Each integration configuration below was compiled with `--no-run` before executing with an external deadline. Exact requested integration commands passed:

```sh
cargo test --manifest-path engine/Cargo.toml --locked --no-default-features --features live --test live_integrity --test live_source --test live_router --test live_processing --test live_derived --test live_config
cargo test --manifest-path engine/Cargo.toml --locked --no-default-features --features "live historical" --test live_derived
cargo test --manifest-path engine/Cargo.toml --locked --no-default-features --test shared_merge --test shared_conversion
cargo test --manifest-path engine/Cargo.toml --locked --no-default-features --features stream-client --test stream_protocol --test stream_client
cargo test --manifest-path engine/Cargo.toml --locked --no-default-features --features "live stream-client" --test stream_transport --test stream_protocol --test stream_client
```

| Target / features | Passed | Ignored |
|---|---:|---:|
| live_integrity / live | 83 | 0 |
| live_source / live | 38 | 1 |
| live_router / live | 32 | 0 |
| live_processing / live | 19 | 0 |
| live_derived / live | 41 | 1 |
| live_config / live | 25 | 0 |
| live_derived / live historical | 41 | 1 |
| shared_merge / no defaults | 8 | 0 |
| shared_conversion / no defaults | 67 | 0 |
| stream_protocol / stream-client | 3 | 0 |
| stream_client / stream-client | 2 | 0 |
| stream_protocol / live stream-client | 3 | 0 |
| stream_client / live stream-client | 2 | 0 |
| stream_transport / live stream-client | 23 | 0 |

Library tests were compiled with `cargo test --manifest-path engine/Cargo.toml --locked --no-default-features --features live --lib --no-run`. Executed filters: `live::stream::router` (22 passed), `live::stream::registry` (51 passed), and `live::infrastructure::processing` (52 passed). The individual-test pass additionally executed the 16 `live::stream::source::processor` unit tests. No zero-test selection counted as verification.

A separate individual-test pass selected tests from the compiled integration feature configurations and these four unit groups, with eight parallel processes and a **15-second deadline for each test**: **613 passed, 4 intentionally ignored, zero failed, zero timed out**, across 617 selections. Counts include repeated tests from distinct feature configurations; they are not unique-test counts.

After the corrections, ten rounds each of `stream_transport`, `live_processing`, `live_derived` with live, and `live_derived` with live + historical all passed: **40 successful suite runs**. Each retained normal harness parallelism with a 30-second suite deadline. Transport, processing and live Derived were also run concurrently in each round. No permanent serialization or arbitrary completion sleeps were added.

The intentionally ignored cases require scoped PostgreSQL write authorization or are an opt-in throughput measurement; neither was run. No public exchange connection, production database write, large historical workload, slow Historical suite, Clippy, or unrelated benchmark was run. `git diff --check` passes.

Raw logs, dependency tree and per-test results are in `/tmp/marketforge-stream-v1-review/` for this session.

## Final state and boundaries

The eight changed files are the seven source/test/documentation paths listed above plus this report. All changes remain uncommitted on `main`, at the original baseline commit. No branch switch, reset, rebase, merge, commit, discard or push occurred.

The interface remains a bounded live tail: no initialized downstream order book, full book snapshots, historical datasets, bootstrap, durable cursor, exactly-once delivery, automatic reconnect/resume, or downstream processing acknowledgment. Unix is the sole implemented transport. Execution was on Linux; no cross-platform verification is claimed. The existing owned server/application integration and Rust consumer remain the runnable public entry points.
