# Management v1 implementation and verification

## Baseline and working tree

Baseline commit: `c8627e390cb30e27da64e7c4650876f2655bdfa4`, branch `main`. Working tree was clean when this assignment began. No branch change, commit, reset, rebase, merge, staging or push was performed.

Concurrent external repository reorganization appeared during this work, including root documentation, planning, Python/assets and legacy root Rust deletions/moves, and changes under `engine/.planning`. These were not read for instructions, edited, restored or included in this implementation. All task changes remain uncommitted. Review the final working tree with this distinction in mind.

Existing configuration uses `data/.config/live/general.toml`, `exchanges/` and `formats/`, with the existing GeneralConfig loader. `--config`/`--general` may select another general file. The sanitized runnable example remains `engine/examples/live/general.toml`.

Baseline compiled with `live stream-client`, then bounded execution produced:

| Target | Passed | Failed | Ignored |
|---|---:|---:|---:|
| live_application | 12 | 3 | 1 |
| live_config | 25 | 0 | 0 |
| live_derived | 41 | 0 | 1 |
| live_processing | 19 | 0 | 0 |
| live_router | 32 | 0 | 0 |
| stream_transport | 23 | 0 | 0 |

The three baseline application failures used Derived definitions without configuring the required processing runtime. The fixture now supplies processing configuration; production admission was not relaxed.

## Implemented and exercised

The tests exercise real existing Source supervision, runtime registry, Router, dedicated CPU pool, Merge, Convert, Unix transport and Rust streaming client. Exchange inputs are synthetic local sockets. The management API and its client use real ephemeral TCP loopback listeners. The TUI intent path uses that same HTTP client. A child process exercises the real headless binary, authentication, live resize, atomic configuration save and administrator shutdown. Another test proves missing required definition persistence fails rather than becoming an empty catalogue.

Coverage includes graph validation/cycles/dependencies, revision conflicts and concurrent edits, unrelated graph additions without service reconstruction, saved-versus-ready outcomes, injected persistence failure, accepted work after lost HTTP responses, shutdown during gated storage commit, fresh restoration without old generations/leases, dynamic Unix demand invalidating a preview, owner-specific holds/renewal/expiry, idempotency conflicts, explicit privileged forced release and its audit event, bounded plans/operations/event history, malformed and oversized requests, SSE cursor expiry/new-daemon resync, a nonreading observer while an independent client progresses, metric readers preserving collector windows, exact large identifiers, UI selection/confirmation/narrow/disconnected/control-character behavior and UI exit independent of daemon shutdown.

The integrated financial scenario creates Source, Merge and Convert definitions through management; consumes actual provenance, conversion values and integrity over Unix; measures settled worker targets 4 → 8 → 4 → 2 (requested/live equal, starting/retiring zero, resize no longer pending) while output continues without generation changes; adds/deletes an unrelated Source through the same live graph; previews/restarts a Merge with its dependent Convert; observes the explicit old-consumer continuity boundary and new generation output; and performs a controlled semantic Derived edit.

Worker tests cover staged partial-creation rollback, repeated grow/shrink, idle retirement, retirement during blocked turns, state/lease return, non-returning-until-released turns and incomplete shutdown ownership, conflicting resizes, shutdown racing growth, and scheduler wake preservation. Blocked test workers are released during cleanup.

## Commands and results

All Cargo invocations used `--manifest-path engine/Cargo.toml --locked --no-default-features` from the repository root. Final `cargo check` passed for each feature selection:

```text
(no features)
live
historical
management-client
management-api
management-tui
management-api management-tui stream-client
```

Dependency inspection used `cargo tree ... -e features` separately for `management-client` and `management-tui`. Neither tree contains SQLx, PostgreSQL, Live execution or tokio-tungstenite. The client and TUI tests were also compiled/run independently of Live.

Integration compilation:

```sh
cargo test --manifest-path engine/Cargo.toml --locked --no-default-features \
  --features 'management-api management-tui stream-client' \
  --test live_integrity --test live_source --test live_router \
  --test live_processing --test live_derived --test live_config \
  --test live_metrics --test live_application \
  --test stream_transport --test stream_client --test stream_protocol \
  --test management --test management_client --test management_tui \
  --lib --no-run
```

Primary selected execution: **659 passed, 0 failed, 3 existing ignores** (before additional feature/parity runs and repetitions). Compiled binaries were then executed with a 90-second external deadline, with normal libtest parallel execution and existing internal synchronization/deadlines. Independent suites were also run concurrently. Filters were checked to select nonzero tests.

| Integration target | Passed | Ignored |
|---|---:|---:|
| management | 15 | 0 |
| management_client | 3 | 0 |
| management_tui | 5 | 0 |
| live_application | 15 | 1 |
| live_integrity | 83 | 0 |
| live_source | 38 | 1 |
| live_router | 32 | 0 |
| live_processing | 19 | 0 |
| live_derived | 41 | 1 |
| live_config | 25 | 0 |
| live_metrics | 3 | 0 |
| stream_transport | 23 | 0 |
| stream_client | 2 | 0 |
| stream_protocol | 3 | 0 |

| Library filter | Passed |
|---|---:|
| `live::infrastructure::processing` | 58 |
| `live::stream::registry` | 52 |
| `live::stream::router` | 22 |
| `management_protocol::definitions` | 98 |
| `live::management` | 2 |
| `live::config::general` | 120 |

Additional compile-before-execution checks:

```sh
cargo test --manifest-path engine/Cargo.toml --locked --no-default-features \
  --features 'live historical' --test live_derived --no-run
cargo test --manifest-path engine/Cargo.toml --locked --no-default-features \
  --test shared_merge --test shared_conversion --no-run
cargo test --manifest-path engine/Cargo.toml --locked --no-default-features \
  --features management-client --test management_client --no-run
cargo test --manifest-path engine/Cargo.toml --locked --no-default-features \
  --features management-tui --test management_tui --no-run
```

Bounded execution: Live/Historical Derived 41 passed/1 ignored; shared_merge 8 passed; shared_conversion 67 passed; client-only 3 passed; TUI-only 5 passed.

Final repeated-run gate: ten normal parallel executions each of management (15), application lifecycle (15), Derived (41), Unix transport (23), processing library (58), registry library (52). All ten passed for each after diagnosed corrections. Existing opt-in throughput/measurement tests and the authorized database fixture test stayed ignored; ignores are not counted as passes.

### Failures diagnosed during implementation

Earlier runs are not represented as successful final verification:

- The new resize supervisor originally waited on the CPU workers' condition variable and could consume `notify_one`, stranding a queued activation. Parallel Derived/transport repetitions exposed this. A separate supervisor wait channel fixes it; a 128-activation regression and repeated full suites verify the correction.
- A worker-failure unit test initiated shutdown before observing its injected failure; staged startup could correctly retire first. It now synchronizes on the actual unexpected-failure counter before requesting planned shutdown.
- A new tombstone test expected an error for an obsolete completion, but the existing lifecycle contract returns `Ok(false)`. The assertion now checks that result and unchanged Starting state; the established contract was preserved.
- An old publication-config unit fixture expected a one-byte queue to be valid, contradicting the existing production 4 KiB minimum frame/queue contract. It now tests the actual minimum; no transport budget was weakened.
- Child daemon tests required credentials after authentication was wired. They now provide test-only environment credentials and an ephemeral management address. Repetitions additionally exposed the signal-installation race; SIGTERM/SIGINT handlers now install before readiness is advertised.
- An explicit retry could race ongoing failure cleanup and be rejected as pending. One bounded deferred retry now waits for owned lifecycle cleanup and revalidates current graph/demand before execution; conflicting retries remain bounded.
- The new integrated market test initially supplied insufficient genuine event-time frontiers for its two-stage Merge/Convert chain. It now supplies real subsequent Source/reference observations. No fake progress, readiness, watermark, conversion rule or arbitrary long sleep was introduced.

Initial CPU-pool construction/partial-start rollback also runs on a blocking task, keeping possible joins off Tokio executor threads. Management readiness reasons identify disabled/no-demand states and waiting data/reference dependencies, and Source readiness observations are matched to their runtime generation.

No final external test timeout is counted as success. Intermediate failed compiles/runs and investigative repetitions were retained in `/tmp/marketforge-management/`; later green runs alone were not treated as a diagnosis.

## Dependencies and configuration

Added Axum 0.8.9 and optional tokio-stream. Enabled streaming on the existing reqwest dependency and display on existing pinned toml_edit 0.25.17. Cargo.lock deliberately adds Axum's small dependency set and reqwest streaming support; no unrelated dependency upgrade was requested/performed.

Extended the existing `control.management` configuration with validated admission/history/lease/client/body/response/deadline/worker limits. Existing files inherit defaults. Existing publication configuration and Unix protocol are preserved. Loopback control port zero is deliberately supported for ephemeral ownership/testing; non-loopback remains rejected. Live-editable settings are dedicated processing worker target and management observation interval. Save-and-apply uses the existing general path, preserves unrelated TOML/comments and rejects external-content conflicts. Other settings are explicitly restart-only/unsupported. See `management-v1.md` for defaults, API and operator commands.

## Limitations and unexecuted verification

No isolated `MARKETFORGE_TEST_DATABASE_URL` was available. PostgreSQL transactional definition changes/template lookup and restart restoration through a real PostgreSQL instance were implemented and source-inspected, but **not executed**. Memory storage contracts, gated commits, restoration and injected failure tests do not independently establish PostgreSQL behavior. No production database writes occurred.

No public exchange connections, large historical workloads, slow Historical suites, unrelated benchmarks or Clippy were run. No missing captured exchange fixture was fabricated, skipped or counted as a pass. New integration fixtures are local synthetic exchange inputs. Existing ignored measurements/database tests remain explicit exclusions.

The application still has no faithful Router recording adapter: recording-enabled startup is rejected and management marks recording unsupported. Management does not fabricate missing metrics/last-data age. Other constraints include process-local operation/idempotency history, bounded non-durable SSE history, no automatic external config reload or live credential reload, single-authoritative-daemon persistence, and a TUI full-overview response budget. Detailed Derived edits use the existing validated JSON request form. These limitations are documented rather than presented as implemented capabilities.

Final status: HEAD remains the baseline commit on `main`; 59 task paths are modified/added/relocated, with 544 concurrent external status entries preserved. `git diff --check -- engine` passes. No task changes are staged or committed. The implementation file inventory follows.

```text
 M engine/Cargo.lock
 M engine/Cargo.toml
 M engine/examples/live/general.toml
 M engine/src/bin/marketforge-live-tui.rs
 M engine/src/bin/marketforge-live.rs
 M engine/src/client/mod.rs
 M engine/src/lib.rs
 M engine/src/live/application/mod.rs
 M engine/src/live/application/runtime.rs
 M engine/src/live/config/exchange/identity.rs
 M engine/src/live/config/general/mod.rs
 M engine/src/live/config/general/model.rs
 M engine/src/live/config/general/validation.rs
 M engine/src/live/control/mod.rs
 M engine/src/live/infrastructure/processing/pool.rs
 M engine/src/live/mod.rs
 M engine/src/live/stream/persistence/model.rs
 M engine/src/live/stream/persistence/repository.rs
 M engine/src/live/stream/processing/service.rs
 M engine/src/live/stream/registry/demand.rs
 M engine/src/live/stream/registry/graph.rs
 M engine/src/live/stream/registry/runtime.rs
 D engine/src/live/stream/request/derived.rs
 M engine/src/live/stream/request/mod.rs
 D engine/src/live/stream/request/model.rs
 D engine/src/live/stream/request/validation.rs
 M engine/src/live/stream/router/facade.rs
 M engine/src/live/stream/router/integrity.rs
 M engine/src/live/stream/router/tests.rs
 M engine/src/tui/mod.rs
 M engine/tests/live_application.rs
?? engine/docs/management-v1-verification.md
?? engine/docs/management-v1.md
?? engine/src/bin/marketforge-tui.rs
?? engine/src/client/management/mod.rs
?? engine/src/live/application/managed.rs
?? engine/src/live/config/general/management.rs
?? engine/src/live/control/http/connection.rs
?? engine/src/live/control/http/mod.rs
?? engine/src/live/infrastructure/processing/pool/resize.rs
?? engine/src/live/management/auth.rs
?? engine/src/live/management/config_file.rs
?? engine/src/live/management/mod.rs
?? engine/src/live/management/planning.rs
?? engine/src/live/management/storage.rs
?? engine/src/live/management/views.rs
?? engine/src/management_protocol/definitions/derived.rs
?? engine/src/management_protocol/definitions/identity.rs
?? engine/src/management_protocol/definitions/mod.rs
?? engine/src/management_protocol/definitions/model.rs
?? engine/src/management_protocol/definitions/persisted.rs
?? engine/src/management_protocol/definitions/validation.rs
?? engine/src/management_protocol/mod.rs
?? engine/src/tui/management/backend.rs
?? engine/src/tui/management/mod.rs
?? engine/tests/management.rs
?? engine/tests/management/support.rs
?? engine/tests/management_client.rs
?? engine/tests/management_tui.rs
```
