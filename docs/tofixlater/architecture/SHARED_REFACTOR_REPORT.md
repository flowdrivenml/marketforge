# Shared refactor implementation and verification report

Date: 2026-10-27. All paths below are relative to the Git root unless explicitly described as package-relative.

## Baseline and safety

- Git root: `/home/pasha/projects/bybit-history-downloader`; Rust package: `engine/Cargo.toml`, `marketforge-engine` 0.1.0, edition 2024.
- Branch: `refactor/shared-architecture`; HEAD: `f24f1eddac97cc0f0472f3b0ed9501ae2d77a686`. Initial working tree was clean. No user edits needed deferral.
- Toolchain: rustc 1.94.1 (`e408947bf`, 2026-03-25), Cargo 1.94.1 (`29ea6fb6a`, 2026-03-24), x86_64-unknown-linux-gnu. No tool/dependency installation, upgrade, edition change, or lockfile change.
- No applicable `AGENTS.md` was present in the repository/package or ancestor directories. The separate `.planning/live/AGENTS.md` does not govern these source paths. Actual manifests, modules, tests, and conventions were inspected; the Repomix snapshot was not used as implementation authority.
- No staging, commits, branch changes, worktrees, Git configuration changes, services, exchange operations, or dataset generation outside existing tests' temporary directories. Protected directories and credentials were untouched. Changes remain uncommitted.
- The reported Historical production compilation failure did **not** reproduce: baseline `cargo check --locked --offline --features historical` and baseline Historical test compilation both passed. Baseline warnings: 12 library dead-code warnings in the existing private trade CSV implementation, plus five headless conversion-binary dead-code warnings. No Historical algorithm repair was justified.
- Baseline executable discovery recorded 1,079 Historical cases, including five existing ignored cases. Live executable discovery was not captured before migration; its source registrations were inventoried in the baseline metadata/source inventory. No baseline Live pass count is claimed.

Baseline evidence: `engine/target/refactor-validation/baseline-{historical.log,tests.jsonl,tests.log,metadata.txt,manifest.toml,toolchain.txt,status.txt}`, `baseline-tests-source.json`, and `baseline-discovery/`. Final toolchain details are in `toolchain-final.txt`.

## Final ownership and layout

```text
engine/src/
  bin/                              # Same executable names and CLIs
  shared/
    domain/
      book/                         # Book primitives, sequence/segments, boundary state
        boundary/                   # State, fingerprints, explicit boundary metadata
      canonical/                    # Events, envelopes, numerics, depth batches/source metadata
      instrument/                   # Identity, currency, specification, catalog validation
    formats/                        # Trade/depth parsing and transformation contracts
    normalization/                  # Exact quantity, decimal, timestamp, side normalization
    validation/                     # Canonical validation and shared integrity categories
    error/                          # Error model and local unit tests
    persistence/
      config.rs                     # Explicit Parquet resource targets
      parquet/                      # Schemas, decimal encoding, builders, file writers, segments
    io/
      container.rs                  # Source container description
      instrument.rs                 # Catalog filesystem loading adapter
      source/                       # Archive/stream reading
      decode/                       # CSV/XLSX decoding
    terminal/                       # Palette, terminal guard, progress display formatting
  historical/
    job/                            # Historical configuration, loading, validation, task adapters
    normalize/                      # Processing orchestration, boundary tracking, sink adapters
    convert/                        # Nested planning/execution/output/reference modules
    merge/                          # Nested depth/trade/core/execution/materialization modules
  live/                             # Existing runtime subsystems retained
  tui/                              # Application dashboards/screens and Live scaffolds retained
  storage/mod.rs                    # Narrow legacy persistence facade only
  lib.rs                            # Authoritative modules and compatibility exports
```

```text
engine/tests/
  shared/
    contracts/
      book/ depth/ decode/ instrument/
    integration/
      parquet/ source/
  historical/
    normalize/                      # Pipelines, scheduling, resources, dataset manifests
    convert/                        # Existing scenario groups and local support
    merge/                          # Existing scenario groups and merge-local dataset support
  live/
    exchanges/ integration/ network/ support/  # Existing ownership and target names retained
engine/scripts/
  bounded_tests.py
  README.md
```

Complete file inventories: `source-tree.txt` and `test-tree.txt` under the validation directory. No empty utility directories, universal repository abstraction, PostgreSQL scaffolding, or new runtime were introduced.

### Module migration map

| Original package-relative path | Authoritative path / ownership |
|---|---|
| `src/book`, `src/canonical`, `src/instrument` | `src/shared/domain/{book,canonical,instrument}` |
| `src/error` | `src/shared/error`; implementation/tests in `model.rs`, declarations in `mod.rs` |
| `src/formats` | `src/shared/formats` |
| `src/normalize` | `src/shared/normalization` |
| `src/validate` | `src/shared/validation` |
| `src/source`, `src/decode` | `src/shared/io/{source,decode}` |
| `src/job` | `src/historical/job`, with shared value types extracted below |
| Job instrument kinds/specification | `shared::domain::instrument::{ContractKind, InstrumentKind, InstrumentSpec}` |
| Job normalization descriptions | `shared::normalization::{NormalizationConfig, QuantityEncoding, TargetSchema, TimestampEncoding}` |
| Job integrity category | `shared::validation::IntegrityCategory`; policy/actions/windows remain Historical |
| Job Parquet resources | `shared::persistence::config::ParquetResourceConfig`; scheduler/resource budgets remain Historical |
| Job source container | `shared::io::container::SourceContainer`; task/order/orchestration types remain Historical |
| Historical boundary state/manifest/fingerprint | `shared::domain::book::boundary`; boundary tracker remains Historical |
| Format depth outcome/source metadata | `shared::domain::canonical`; formats retains explicit compatibility exports |
| Instrument catalog file loading | `shared::io::instrument`; domain retains catalog validation/lookup |
| `src/storage/parquet` | `src/shared/persistence/parquet` |
| Historical Parquet depth/events/segments/writers | Same shared Parquet subsystem; sink implementations in `historical/normalize/parquet/sinks.rs` |
| Historical palette constants | `shared::terminal::palette` |
| Identical processing/merge/demo terminal guards | `shared::terminal::guard::TerminalGuard` |
| Identical processing/merge number/speed/duration formatting | `shared::terminal::format` |
| Live general config `mod.rs` implementation | `live/config/general/loading.rs` |
| Live exchange config `mod.rs` implementation | `live/config/exchange/model.rs` |

Merge and conversion's flat `#[path]` declarations were replaced with real nested modules and explicit subsystem facades. Repeated filename prefixes were removed where ownership is already clear, e.g. `merge/depth/depth_cursor.rs` → `merge/depth/cursor.rs`, `merge/trades/trade_cursor.rs` → `merge/trades/cursor.rs`, and conversion execution `combined/combined_worker.rs` → `combined/worker.rs`. The exhaustive nested module/file map is `module-map.json`; `source-path-audit.json` records filesystem changes. Workflow-local `shared` directories remain local where they hold workflow-specific decimal, planning, execution, and materialization contracts.

### Dependency boundaries and deliberately retained code

Shared imports no Historical, Live, application TUI, binary, root `job`, or `process` implementation, directly or through compatibility facades. Shared domain imports no persistence, terminal UI, or network transport. The source audit is recorded in `shared-dependency-audit.json`; direct dependency trees for minimal, Live, Parquet, and terminal configurations are in `dependencies-*.txt`.

Historical jobs remain responsible for task configuration, schedulers, integrity policy, orchestration, historical boundary tracking/provenance, workflow publication policy, merge/convert planning, and transformed schemas. Live retains REST/WebSocket management, Integrity, Recovery, lifecycle machinery, and existing public integration boundaries. A task-taking `InstrumentMatcher::from_task` compatibility method is implemented in the Historical adapter; shared identity policy accepts explicit exchange/instrument-kind inputs through `resolve_identity_policy`. The old `formats::trades::identity_policy(&WorkTask)` interface delegates from Historical and remains available through its legacy facade.

The existing lightweight job configuration API was unconditional before this change. It remains compiled unconditionally at `historical::job`, with the old root `job` export preserved. This does not enable Historical execution/normalize/merge/convert modules or make shared persistence depend on jobs. Standalone Parquet callers need no `ProcessingJob`, scheduler, Live handle, transport, or database connection.

Archive source/decode features retain their existing Historical optional-dependency gate. This assignment introduced an independent capability specifically for canonical Parquet; it did not introduce a second archive feature framework. Generic-looking conversion publication code retains its Linux no-replace semantics, conversion-specific errors, and workflow owner. Merge/materialization publication and normalization final publication also retain their existing policy and behavior.

## Canonical Parquet API and preserved contracts

Enable `parquet` to use `marketforge_engine::shared::persistence::parquet` independently of `historical`. `historical` enables `parquet` and its original other dependencies. The feature activates Arrow array/schema/buffer and Parquet dependencies. Conversion-only bigint/traits/integer dependencies remain Historical. Minimal and Live-only builds do not acquire Arrow/Parquet or terminal dependencies.

Public entry points:

- `trade_schema`, `depth_schema`, `depth_event_schema`, `DECIMAL_PRECISION`, `DECIMAL_SCALE`.
- `decimal_to_i256`, `i256_to_decimal` for exact decimal conversion.
- `TradeBatchBuilder`, `DepthBatchBuilder`, `DepthEventBatchBuilder`, `IndexedDepthEvent`.
- `ParquetTradeWriter::{new, write_trade, finish, metrics, is_failed, is_finished}`.
- `ParquetDepthWriter::{new, write_outcome, write_boundary, finish, metrics, segments, next_event_offset, events_indexed, is_failed, is_finished}`.
- `DepthSegmentManifest`, `read_depth_segments`, `write_depth_segments`.
- Explicit resource input: `shared::persistence::config::ParquetResourceConfig`. Depth canonical batches/source metadata live under `shared::domain::canonical`; explicit boundary state/metadata lives under `shared::domain::book::boundary`.

Writers now expose inherent methods containing the original implementations. Historical `TradeSink` and `DepthSink` implementations delegate to those methods, preserving callbacks, signatures, failure state, idempotence, and finalization behavior. No sink trait was moved into a universal storage framework. No provenance/sequence information is synthesized; the same source ordinal, event boundaries, initialization rules, and boundary metadata requirements remain enforced, including zero-level source events.

The schema source is byte-identical to baseline. A textual function-body audit found no differences across 78 extracted schema/decimal/builder/writer/segment functions after import-path/whitespace normalization (`parquet-body-audit.json`); this is supporting evidence rather than a substitute for execution. Existing reader/assertion contracts passed in **41 standalone-Parquet cases without Historical**, covering exact decimals, batches, event indexes, row offsets, segments, and trade/depth output. These 41 identities also passed in the combined run and are counted once in the unique-case totals below.

Field names/order, Arrow/Parquet types, Decimal256 precision 76/scale 28, nullability, timestamp units, enum encodings, source boundaries, segment versions, naming, compression, row-group/file rotation, flush/finalization, manifests, and error/publication semantics were preserved. Merge timeline and converted dataset schemas remain distinct and workflow-owned. No byte-identical-file assertion was added for nondeterministic encoder metadata.

## Terminal reuse and behavior

`shared::terminal` is gated by the unchanged `monitor-tui` feature. It contains one Tokyo Night palette, the reusable processing/merge/demo RAII guard, and shared processing/merge count/speed/duration formatting. Processing, merging, conversion, and the standalone process demo consume the palette. Processing/merge/the demo consume the guard. All screens, tables, gauges, progress state, routing, controls, labels, geometry, refresh cadence, and domain-specific view logic remain in their application owners. No competing TUI framework or new UI product behavior was introduced.

Intentional differences retained:

- Conversion restores raw mode before leaving the alternate screen; processing/merge/demo leave the alternate screen first. Conversion keeps its existing guard so this ordering is unchanged.
- Conversion uses two decimal places for compact thousands; processing/merge use one. The demo has its own existing compact precision. These formats were not forced into one API.
- The Live application-facing TUI tree and binary are scaffolds in this checkout; `live/tui` contains existing application state/models rather than a completed Ratatui renderer. Their state flow and feature accessibility remain unchanged. No placeholder was substituted for a completed implementation.

Historical+TUI, Live+TUI, and terminal-only production checks passed. Combined test compilation includes the TUI feature; existing application/state tests remain registered. No baseline visual golden files or terminal lifecycle integration suite existed to relocate. No interactive terminal/panic restoration or pixel-level rendering test was executed, so those behaviors are preserved by source extraction, not claimed as runtime-verified.

## Compilation fixes, facades, visibility, and removals

1. **Historical production/test compilation:** no pre-existing compiler failure reproduced, so no Historical algorithm or API behavior repair was made.
2. **Existing test feature-registration mismatch:** non-Historical test compilation exposed E0432 in `source_compatibility`: the test imported source readers while the source module was Historical-gated. The original source reader gate and ungated auto-discovered test registration confirm this pre-existed by inspection; the observed diagnostic was captured after relocation in `tests-{minimal,parquet,live}.jsonl`. The test now has an explicit, name-preserving registration with `required-features = ["historical"]`. Historical-owned targets likewise explicitly require Historical; shared Parquet targets require Parquet; the catalog contract no longer unnecessarily requires Historical. No target, assertion, or test was disabled to make a build pass.
3. **Structural compiler adjustments:** module imports/re-exports, ancestor-qualified references, sink adapters, and narrow module visibility were updated to match real nesting. Transient migration-only import/scoped-re-export diagnostics are retained in slice logs and are not characterized as baseline failures.

Compatibility exports preserve root `book`, `canonical`, `instrument`, `normalize`, `validate`, `error`, gated `source`/`decode`, `job`, `formats`, and `storage::parquet`/`storage::historical_compat`. Root `process` still aliases Historical normalization; its existing merge/convert re-exports remain valid. Historical Parquet and boundary facades explicitly export their shared implementations. No duplicated implementations or production `#[path]` declarations remain.

Visibility changes are limited to the necessary inherent public persistence methods, crate-private catalog validation constructor (`from_file`), and nested module/function visibility restricted to the owning Historical subsystem. Private implementation modules remain hidden behind the existing public facades. No blanket warning suppression or ownership-field deletion was added.

Removed code: three redundant Historical decimal/schema/trade-builder shim files were replaced by the explicit Parquet facade. An unregistered `src/formats/depth/depth_snapshot_assembler.rs` was confirmed to be a whitespace-equivalent duplicate of the registered snapshot-assembler contract suite and removed from production source; all three tests remain registered exactly once under `tests/shared/contracts/depth/depth_snapshot_assembler.rs`. Evidence is in `removed-code-evidence.json`. No unfinished public Live integration boundary was removed.

Test `#[path]` attributes remain only for fixture/support sharing across independently registered test binaries and existing Live scenario modules. Their relative paths were updated, preserving process isolation. Merge-only dataset support moved from root `tests/common` to `tests/historical/merge/support`, rather than claiming cross-domain ownership. Conversion and Live support remain near their consumers.

## Verification commands and compilation outcomes

All Cargo commands below were run from `engine/` with the existing cache and pinned lockfile. Compilation/linking/lock waits were not subjected to the individual-test deadline.

Baseline:

```sh
cargo check --locked --offline --features historical
cargo test --locked --offline --features historical --no-run --message-format=json
python3 scripts/bounded_tests.py target/refactor-validation/baseline-tests.jsonl target/refactor-validation/baseline-discovery
```

Final production checks (each exit 0):

| Configuration | Command suffix after `cargo check --locked --offline` |
|---|---|
| Supported default | none |
| Minimal library | `--no-default-features --lib` |
| Historical | `--features historical` |
| Live | `--features live` |
| Historical + Live | `--features historical,live` |
| Historical TUI | `--features historical,monitor-tui` |
| Live TUI | `--features live,monitor-tui` |
| Terminal primitives/demo | `--features monitor-tui` |
| Standalone Parquet | `--features parquet` |

Final test compilation (each exit 0):

```sh
cargo test --locked --offline --no-run --message-format=json
cargo test --locked --offline --features parquet --no-run --message-format=json
cargo test --locked --offline --features historical --no-run --message-format=json
cargo test --locked --offline --features live --no-run --message-format=json
cargo test --locked --offline --features historical,live,monitor-tui --no-run --message-format=json
```

Exact commands, statuses and logs: `audit-build-matrix.json`, `audit-check-*.log`, `audit-test-build-matrix.json`, `audit-tests-*.{jsonl,log}`. The original feature-only build checks and intermediate no-run results are also retained; failed intermediate commands have not been recast as successful. Final test compilation covers all 205 target registrations (195 integration-test targets, nine binaries and the library), including ignored/network code and benchmark-binary test harness compilation. Benchmark mains were never executed.

Final formatting: `cargo fmt --manifest-path engine/Cargo.toml -- --check` and `git diff --check` from the Git root passed. Final warning inspection preserves the baseline 12 library CSV warnings and five headless conversion-binary warnings in relevant configurations; Live test builds additionally expose the unchanged `received_at` field / `throttled` helper warnings. Newly unused sink imports and migration-only unused re-exports/imports were removed. No blanket `allow` was added; ownership-bearing fields were retained.

### Automatic per-case execution

The checked-in runner discovers executable paths from Cargo JSON artifacts and lists exact libtest names/ignored status. Each selected case runs sequentially as:

```text
<artifact executable> --exact <fully-qualified-name> --nocapture --test-threads=1
```

A monotonic deadline starts before process creation, including fixture setup. At 30 seconds it signals the owned process group, waits 0.2 seconds, then kills remaining owned processes. It preserves package cwd, Cargo manifest environment, inherited environment and dynamically discovered library directories. Raw exit codes, timings, logs and failure status remain available. A zero-test or ignored-only invocation cannot produce PASS, and `should_panic` uses the normal harness. Empty discovery and incomplete selective verification return non-success.

Executed commands (JSON discovery/results and individual logs reside inside each output directory):

```sh
python3 scripts/bounded_tests.py target/refactor-validation/final-all-tests-v2.jsonl target/refactor-validation/offline-run --run --targets 'marketforge_engine|book_.*|parquet_(decimal|depth_batch|depth_events|depth_segments|depth_writer|trade_batch|writer)|depth_(json|relative|scalar|spec|transforms|snapshot_assembler|snapshot_differencing|source_metadata|outcome|processor)|instrument_catalog|live_(normalization|integrity|recovery)'
python3 scripts/bounded_tests.py target/refactor-validation/final-tests-parquet.jsonl target/refactor-validation/standalone-parquet-run --run --targets 'parquet_(decimal|depth_batch|depth_events|depth_segments|depth_writer|trade_batch|writer)'
python3 scripts/bounded_tests.py target/refactor-validation/complete-tests.jsonl target/refactor-validation/additional-run --run --targets 'depth_(csv_adapter|xlsx_decoder|sink|csv_pipeline)|source_compatibility|dataset_manifest|live_config'
python3 scripts/bounded_tests.py target/refactor-validation/complete-tests.jsonl target/refactor-validation/expanded-offline-run --run --targets 'materialize_.*|live_(rest|websocket|metrics|exchange_contracts)|merge_(job|kway|kway_event|resource_estimation)|convert_(decimal|graph|catalog|planner|job|reference_state)'
```

Each command returned 1 because the overall inventory was incomplete and/or contained failures. Standalone Parquet's selected 41 cases passed, while its other 155 discovered cases were explicitly NOT_RUN. No timeout or failure exit was masked. Two synthetic runner self-checks verified PASS, zero-test rejection, a 30-second timeout, group-child cleanup, continuation after timeout, and non-success exit; a later empty-artifact check returned BLOCKED_ENVIRONMENT. The timeout check was repeated only after correcting the deadline to subtract process setup time. Synthetic self-check identities are separate from MarketForge test counts (`runner-selfcheck*/`, `runner-empty-selfcheck/`). No timed-out MarketForge case was rerun.

## Discovery reconciliation and execution results

Baseline Historical discovery and final Historical discovery both contain **1,079** cases, with identical scenarios/ignore states. All are mapped one-to-one in `test-identity-map.json`; 125 fully qualified local-unit names changed with module ownership/nesting. Integration target names are unchanged. Final all-feature discovery contains **3,265 unique cases**; there are no lost or double-registered identities. Final executable discovery is in `final-discovery/` and `final-historical-discovery/`; `discovery-reconciliation.json` records the reconciliation.

Unique final-code case outcomes, combining the three disjoint selected-target runs (the standalone Parquet repeats are not double-counted):

| Status | Cases |
|---|---:|
| `PASS` | 2,693 |
| `FAIL` | 8 |
| `TIMED_OUT_30S` | 0 |
| `IGNORED_EXISTING` | 16 |
| `SKIPPED_NETWORK` | 0 |
| `BLOCKED_MISSING_FIXTURE` | 0 |
| `BLOCKED_ENVIRONMENT` | 67 |
| `NOT_RUN` | 481 |

Exact identities for **every** status, including all passing and unexecuted cases, are in `result-status-groups.json`; `consolidated-results.json` records each selected invocation's log, executable, elapsed time, raw exit code and any reviewed environment classification. Raw `offline-run/results.json`, `additional-run/results.json`, and `expanded-offline-run/results.json` remain intact. `reviewed-results.json` preserves the first run's raw FAIL status separately when annotating socket-denied cases as blocked.

### Assertion failures retained

- `depth_spec::compiles_all_depth_normalization_specs` — `target/refactor-validation/offline-run/00006.log`.
- `dataset_manifest::creates_valid_dataset_manifest` — `target/refactor-validation/additional-run/02684.log`.
- `convert_job::loads_combined_conversion_job` — `target/refactor-validation/expanded-offline-run/02376.log`.
- `convert_job::loads_depth_conversion_job` — `target/refactor-validation/expanded-offline-run/02377.log`.
- `convert_job::loads_trade_conversion_job` — `target/refactor-validation/expanded-offline-run/02378.log`.
- `marketforge_engine::historical::merge::execution::shared::resources::tests::rejects_empty_streams` — `target/refactor-validation/offline-run/00792.log`.
- `live_exchange_contracts::contracts::rate_limits::bootstraps_rest_for_all_real_exchange_profiles` — `target/refactor-validation/expanded-offline-run/02217.log`.
- `live_exchange_contracts::contracts::rest::resolution::resolves_all_real_exchange_rest_endpoints` — `target/refactor-validation/expanded-offline-run/02223.log`.

The first six failures reproduce in untouched baseline executables: `baseline-failure-check/` (depth spec and merge resources), `baseline-manifest-check/`, and `baseline-convert-check/`. The two Live endpoint-count failures have no captured pre-migration executable run; they resolve all 35 configured endpoints successfully before failing the expected count of 37. The test/configuration contracts were not edited, and this is consistent with an existing configuration-count expectation mismatch, but it remains a failed validation result rather than an asserted baseline pass or repaired behavior.

No assertion was weakened, dataset shrunk, fixture regenerated to match production, early return inserted, or new `#[ignore]` added. Historical metrics/resource semantics and fixture expectations are deferred to dedicated Historical repair. Live endpoint-count expectations require a separate configuration/contract review.

### Blocked and intentionally unexecuted work

All **67 BLOCKED_ENVIRONMENT** cases failed while binding local HTTP/WebSocket mock listeners with `Os { code: 1, kind: PermissionDenied, message: "Operation not permitted" }`. Each was inspected against the listener source; the raw failure/exit is retained. Their exact identities and logs are in the status groups. These were local mock tests, not permission to use external endpoints or weaken the sandbox.

There are **16 IGNORED_EXISTING** cases:

- `merge_full_equivalence::validates_full_parallel_merge_against_sequential`.
- `trade_all_archives::processes_all_trade_archives`.
- `process_parallel_consistency::large_parallel_consistency`.
- `merge_executor::executes_real_merge_job`.
- `merge_cursor_benchmark::compares_indexed_and_sequential_cursors`.
- `live_network::rest::bulk::verifies_public_rest_endpoints_in_bulk`.
- `live_network::rest::service::production_rest_service_bulk_live_acceptance`.
- `live_network::rest::service::production_rest_service_executes_live_bybit_trades`.
- `live_network::rest::smoke::bybit_spot_depth_snapshot_live`.
- `live_network::rest::smoke::bybit_spot_recent_trades_live`.
- `live_network::websocket::bulk::all_exchange_profiles_live`.
- `live_network::websocket::manager::websocket_manager_backpressure_isolated_between_slots`.
- `live_network::websocket::manager::websocket_manager_backpressure_must_close_stream`.
- `live_network::websocket::manager::websocket_manager_routing_conflict_retires_entire_slot`.
- `live_network::websocket::manager::websocket_manager_shared_slot_lifecycle_live`.
- `live_network::websocket::slot::binance_spot_live_trades`.

Eleven belong to the external `live_network` suite. They retain IGNORED_EXISTING as their primary status, so SKIPPED_NETWORK is zero rather than double-counting them. Ignored full-data/performance cases and benchmark execution were not opted into.

The **481 NOT_RUN** cases are listed exactly in the status groups. They are the remaining Historical merge/convert/normalization workflows, larger data/reconstruction scenarios and extended fixture-dependent contracts outside the audited execution selection. Several existing tests use persistent `data/.work/convert-validation`, `data/converted`, or job-derived output locations; these were not launched or redirected, preserving their fixtures and behavior. Not every unexecuted case is unsafe: the remaining safe scenarios also remain explicitly unverified. No fixture-dependent test was falsely reported missing merely because it failed an assertion. Selected fixture preflight found the actual catalog/job/multihop manifests, canonical dataset roots, and raw source fixture root available; BLOCKED_MISSING_FIXTURE is therefore zero.

Doctests are explicitly **NOT_RUN** under this per-case policy. No fenced Rust source examples or custom `harness = false` registrations were found (`doctest-inventory.json`); no unbounded doctest/custom harness was launched. This is recorded separately from the 3,265 libtest cases. Interactive terminal/visual verification is also unexecuted.

Rerun after resolving a failure/environment condition, retaining supervision and a fresh output directory, for example:

```sh
cargo test --locked --offline --features historical,live,monitor-tui --no-run --message-format=json > target/refactor-validation/rerun-artifacts.jsonl 2> target/refactor-validation/rerun-build.log
python3 scripts/bounded_tests.py target/refactor-validation/rerun-artifacts.jsonl target/refactor-validation/rerun-depth-spec --run --targets depth_spec --cases compiles_all_depth_normalization_specs
python3 scripts/bounded_tests.py target/refactor-validation/rerun-artifacts.jsonl target/refactor-validation/rerun-recovery --run --targets live_recovery --cases 'executor_http::recovery_executor_fetches_recent_trades'
```

Use the same target/name pairs from the status groups for other failures/blocks. Audit persistent fixture/output ownership before opting into the remaining workflows. A successful selective rerun still reports other unexecuted identities and returns an explicit incomplete result.

## Future infrastructure and review limits

The intended future PostgreSQL infrastructure home is `shared/persistence/postgres`. It is **not implemented or scaffolded** here. An eventual database-to-canonical-Parquet exporter can consume the shared canonical records/builders/writers and explicit persistence metadata. Reusing shared code does not create one connection pool across separate OS processes; each application runtime must own its eventual pool and lifecycle.

No Source Stream Runtime, PostgreSQL exporter, new recovery behavior, exchange configuration edits, historical arithmetic/ordering changes, or broad Historical repair was made. Compilation is verified across the configurations above, but runtime validation remains incomplete because of retained assertion failures, sandbox listener denials and the explicitly unexecuted scenarios. This report does not claim readiness to merge.

## Exact test-target migration map

All 195 integration-test target names remain unchanged, including Live commands. These paths are package-relative; `—` means no required feature. Executable names/CLI flags and serialized contracts were not renamed. Library/binary registrations and exact before/after feature requirements for all 205 targets are in `target-map.json`; file-only moves/support changes are in `test-file-map.json`.

| Target | Old path | New path | Final required feature |
|---|---|---|---|
| `book_batch` | `tests/processing/depth/book_batch.rs` | `tests/shared/contracts/book/book_batch.rs` | — |
| `book_emit` | `tests/processing/depth/book_emit.rs` | `tests/shared/contracts/book/book_emit.rs` | — |
| `book_segment` | `tests/processing/depth/book_segment.rs` | `tests/shared/contracts/book/book_segment.rs` | — |
| `book_sequence` | `tests/processing/depth/book_sequence.rs` | `tests/shared/contracts/book/book_sequence.rs` | — |
| `book_store` | `tests/processing/depth/book_store.rs` | `tests/shared/contracts/book/book_store.rs` | — |
| `convert_atomic_publication` | `tests/convert/output/atomic_publication.rs` | `tests/historical/convert/output/atomic_publication.rs` | `historical` |
| `convert_catalog` | `tests/convert/planning/catalog.rs` | `tests/historical/convert/planning/catalog.rs` | `historical` |
| `convert_checkpoints` | `tests/convert/execution/checkpoints.rs` | `tests/historical/convert/execution/checkpoints.rs` | `historical` |
| `convert_combined_alignment` | `tests/convert/combined/combined_alignment.rs` | `tests/historical/convert/combined/combined_alignment.rs` | `historical` |
| `convert_combined_checkpoint_ordering` | `tests/convert/combined/combined_checkpoint_ordering.rs` | `tests/historical/convert/combined/combined_checkpoint_ordering.rs` | `historical` |
| `convert_combined_checkpoints` | `tests/convert/combined/combined_checkpoints.rs` | `tests/historical/convert/combined/combined_checkpoints.rs` | `historical` |
| `convert_combined_conversion` | `tests/convert/combined/combined_conversion.rs` | `tests/historical/convert/combined/combined_conversion.rs` | `historical` |
| `convert_combined_depth_adapter` | `tests/convert/combined/combined_depth_adapter.rs` | `tests/historical/convert/combined/combined_depth_adapter.rs` | `historical` |
| `convert_combined_equivalence` | `tests/convert/combined/combined_equivalence.rs` | `tests/historical/convert/combined/combined_equivalence.rs` | `historical` |
| `convert_combined_materialize` | `tests/convert/combined/combined_materialize.rs` | `tests/historical/convert/combined/combined_materialize.rs` | `historical` |
| `convert_combined_parallel` | `tests/convert/combined/combined_parallel.rs` | `tests/historical/convert/combined/combined_parallel.rs` | `historical` |
| `convert_combined_publication` | `tests/convert/combined/combined_publication.rs` | `tests/historical/convert/combined/combined_publication.rs` | `historical` |
| `convert_combined_references` | `tests/convert/combined/combined_references.rs` | `tests/historical/convert/combined/combined_references.rs` | `historical` |
| `convert_combined_restoration` | `tests/convert/combined/combined_restoration.rs` | `tests/historical/convert/combined/combined_restoration.rs` | `historical` |
| `convert_combined_schema` | `tests/convert/combined/combined_schema.rs` | `tests/historical/convert/combined/combined_schema.rs` | `historical` |
| `convert_combined_validation` | `tests/convert/combined/combined_validation.rs` | `tests/historical/convert/combined/combined_validation.rs` | `historical` |
| `convert_combined_worker` | `tests/convert/combined/combined_worker.rs` | `tests/historical/convert/combined/combined_worker.rs` | `historical` |
| `convert_combined_writer` | `tests/convert/combined/combined_writer.rs` | `tests/historical/convert/combined/combined_writer.rs` | `historical` |
| `convert_decimal` | `tests/convert/planning/decimal.rs` | `tests/historical/convert/planning/decimal.rs` | `historical` |
| `convert_depth_alignment` | `tests/convert/depth/depth_alignment.rs` | `tests/historical/convert/depth/depth_alignment.rs` | `historical` |
| `convert_depth_arithmetic` | `tests/convert/depth/depth_arithmetic.rs` | `tests/historical/convert/depth/depth_arithmetic.rs` | `historical` |
| `convert_depth_builder` | `tests/convert/depth/depth_builder.rs` | `tests/historical/convert/depth/depth_builder.rs` | `historical` |
| `convert_depth_checkpoints` | `tests/convert/depth/depth_checkpoints.rs` | `tests/historical/convert/depth/depth_checkpoints.rs` | `historical` |
| `convert_depth_equivalence` | `tests/convert/depth/depth_equivalence.rs` | `tests/historical/convert/depth/depth_equivalence.rs` | `historical` |
| `convert_depth_materialize` | `tests/convert/depth/depth_materialize.rs` | `tests/historical/convert/depth/depth_materialize.rs` | `historical` |
| `convert_depth_parallel` | `tests/convert/depth/depth_parallel.rs` | `tests/historical/convert/depth/depth_parallel.rs` | `historical` |
| `convert_depth_planner` | `tests/convert/depth/depth_planner.rs` | `tests/historical/convert/depth/depth_planner.rs` | `historical` |
| `convert_depth_preservation` | `tests/convert/depth/depth_preservation.rs` | `tests/historical/convert/depth/depth_preservation.rs` | `historical` |
| `convert_depth_publication` | `tests/convert/depth/depth_publication.rs` | `tests/historical/convert/depth/depth_publication.rs` | `historical` |
| `convert_depth_schema` | `tests/convert/depth/depth_schema.rs` | `tests/historical/convert/depth/depth_schema.rs` | `historical` |
| `convert_depth_validation` | `tests/convert/depth/depth_validation.rs` | `tests/historical/convert/depth/depth_validation.rs` | `historical` |
| `convert_execution_reader` | `tests/convert/execution/execution_reader.rs` | `tests/historical/convert/execution/execution_reader.rs` | `historical` |
| `convert_executor` | `tests/convert/execution/executor.rs` | `tests/historical/convert/execution/executor.rs` | `historical` |
| `convert_external_reference` | `tests/convert/reference/external_reference.rs` | `tests/historical/convert/reference/external_reference.rs` | `historical` |
| `convert_graph` | `tests/convert/planning/graph.rs` | `tests/historical/convert/planning/graph.rs` | `historical` |
| `convert_job` | `tests/convert/planning/job.rs` | `tests/historical/convert/planning/job.rs` | `historical` |
| `convert_materialize` | `tests/convert/execution/materialize.rs` | `tests/historical/convert/execution/materialize.rs` | `historical` |
| `convert_materialize_multihop` | `tests/convert/execution/materialize_multihop.rs` | `tests/historical/convert/execution/materialize_multihop.rs` | `historical` |
| `convert_output_manifest` | `tests/convert/output/output_manifest.rs` | `tests/historical/convert/output/output_manifest.rs` | `historical` |
| `convert_output_schema` | `tests/convert/output/output_schema.rs` | `tests/historical/convert/output/output_schema.rs` | `historical` |
| `convert_output_trade` | `tests/convert/output/output_trade.rs` | `tests/historical/convert/output/output_trade.rs` | `historical` |
| `convert_output_validation` | `tests/convert/output/output_validation.rs` | `tests/historical/convert/output/output_validation.rs` | `historical` |
| `convert_output_writer` | `tests/convert/output/output_writer.rs` | `tests/historical/convert/output/output_writer.rs` | `historical` |
| `convert_parallel` | `tests/convert/execution/parallel.rs` | `tests/historical/convert/execution/parallel.rs` | `historical` |
| `convert_parallel_publication` | `tests/convert/execution/parallel_publication.rs` | `tests/historical/convert/execution/parallel_publication.rs` | `historical` |
| `convert_parallel_validation` | `tests/convert/execution/parallel_validation.rs` | `tests/historical/convert/execution/parallel_validation.rs` | `historical` |
| `convert_partition_reader` | `tests/convert/execution/partition_reader.rs` | `tests/historical/convert/execution/partition_reader.rs` | `historical` |
| `convert_partitions` | `tests/convert/execution/partitions.rs` | `tests/historical/convert/execution/partitions.rs` | `historical` |
| `convert_persistent_fixture` | `tests/convert/trades/persistent_fixture.rs` | `tests/historical/convert/trades/persistent_fixture.rs` | `historical` |
| `convert_planner` | `tests/convert/planning/planner.rs` | `tests/historical/convert/planning/planner.rs` | `historical` |
| `convert_prepass` | `tests/convert/execution/prepass.rs` | `tests/historical/convert/execution/prepass.rs` | `historical` |
| `convert_publication` | `tests/convert/output/publication.rs` | `tests/historical/convert/output/publication.rs` | `historical` |
| `convert_receipt` | `tests/convert/output/receipt.rs` | `tests/historical/convert/output/receipt.rs` | `historical` |
| `convert_reference_evaluation` | `tests/convert/reference/reference_evaluation.rs` | `tests/historical/convert/reference/reference_evaluation.rs` | `historical` |
| `convert_reference_reader` | `tests/convert/reference/reference_reader.rs` | `tests/historical/convert/reference/reference_reader.rs` | `historical` |
| `convert_reference_seek` | `tests/convert/reference/reference_seek.rs` | `tests/historical/convert/reference/reference_seek.rs` | `historical` |
| `convert_reference_selection` | `tests/convert/reference/reference_selection.rs` | `tests/historical/convert/reference/reference_selection.rs` | `historical` |
| `convert_reference_state` | `tests/convert/reference/reference_state.rs` | `tests/historical/convert/reference/reference_state.rs` | `historical` |
| `convert_source_preservation` | `tests/convert/output/source_preservation.rs` | `tests/historical/convert/output/source_preservation.rs` | `historical` |
| `convert_trade` | `tests/convert/trades/trade.rs` | `tests/historical/convert/trades/trade.rs` | `historical` |
| `convert_worker` | `tests/convert/execution/worker.rs` | `tests/historical/convert/execution/worker.rs` | `historical` |
| `dataset_manifest` | `tests/dataset_manifest.rs` | `tests/historical/normalize/dataset_manifest.rs` | `historical` |
| `depth_archive_continuity` | `tests/processing/depth/depth_archive_continuity.rs` | `tests/historical/normalize/depth/depth_archive_continuity.rs` | `historical` |
| `depth_boundary` | `tests/processing/depth/depth_boundary.rs` | `tests/historical/normalize/depth/depth_boundary.rs` | `historical` |
| `depth_boundary_recovery` | `tests/processing/depth/depth_boundary_recovery.rs` | `tests/historical/normalize/depth/depth_boundary_recovery.rs` | `historical` |
| `depth_csv_adapter` | `tests/processing/depth/depth_csv_adapter.rs` | `tests/shared/contracts/decode/depth_csv_adapter.rs` | `historical` |
| `depth_csv_pipeline` | `tests/processing/depth/depth_csv_pipeline.rs` | `tests/historical/normalize/depth/depth_csv_pipeline.rs` | `historical` |
| `depth_formats` | `tests/processing/depth/depth_formats.rs` | `tests/historical/normalize/depth/depth_formats.rs` | `historical` |
| `depth_json` | `tests/processing/depth/depth_json.rs` | `tests/shared/contracts/depth/depth_json.rs` | — |
| `depth_outcome` | `tests/processing/depth/depth_outcome.rs` | `tests/shared/contracts/depth/depth_outcome.rs` | — |
| `depth_parquet_replay` | `tests/processing/depth/depth_parquet_replay.rs` | `tests/historical/normalize/depth/depth_parquet_replay.rs` | `historical` |
| `depth_pipeline_recovery` | `tests/processing/depth/depth_pipeline_recovery.rs` | `tests/historical/normalize/depth/depth_pipeline_recovery.rs` | `historical` |
| `depth_processor` | `tests/processing/depth/depth_processor.rs` | `tests/shared/contracts/depth/depth_processor.rs` | — |
| `depth_real_archive` | `tests/processing/depth/depth_real_archive.rs` | `tests/historical/normalize/depth/depth_real_archive.rs` | `historical` |
| `depth_relative` | `tests/processing/depth/depth_relative.rs` | `tests/shared/contracts/depth/depth_relative.rs` | — |
| `depth_scalar` | `tests/processing/depth/depth_scalar.rs` | `tests/shared/contracts/depth/depth_scalar.rs` | — |
| `depth_sequence_ranges` | `tests/processing/depth/depth_sequence_ranges.rs` | `tests/historical/normalize/depth/depth_sequence_ranges.rs` | `historical` |
| `depth_sink` | `tests/processing/depth/depth_sink.rs` | `tests/historical/normalize/depth/depth_sink.rs` | `historical` |
| `depth_snapshot_assembler` | `tests/processing/depth/depth_snapshot_assembler.rs` | `tests/shared/contracts/depth/depth_snapshot_assembler.rs` | — |
| `depth_snapshot_differencing` | `tests/processing/depth/depth_snapshot_differencing.rs` | `tests/shared/contracts/depth/depth_snapshot_differencing.rs` | — |
| `depth_source_metadata` | `tests/processing/depth/depth_source_metadata.rs` | `tests/shared/contracts/depth/depth_source_metadata.rs` | — |
| `depth_spec` | `tests/processing/depth/depth_spec.rs` | `tests/shared/contracts/depth/depth_spec.rs` | — |
| `depth_transforms` | `tests/processing/depth/depth_transforms.rs` | `tests/shared/contracts/depth/depth_transforms.rs` | — |
| `depth_xlsx_decoder` | `tests/processing/depth/depth_xlsx_decoder.rs` | `tests/shared/contracts/decode/depth_xlsx_decoder.rs` | `historical` |
| `depth_xlsx_pipeline` | `tests/processing/depth/depth_xlsx_pipeline.rs` | `tests/historical/normalize/depth/depth_xlsx_pipeline.rs` | `historical` |
| `instrument_catalog` | `tests/instrument/catalog.rs` | `tests/shared/contracts/instrument/catalog.rs` | — |
| `integrity_policy` | `tests/processing/resources/integrity_policy.rs` | `tests/historical/normalize/resources/integrity_policy.rs` | `historical` |
| `live_config` | `tests/live/integration/config/main.rs` | `tests/live/integration/config/main.rs` | `live` |
| `live_exchange_contracts` | `tests/live/exchanges/main.rs` | `tests/live/exchanges/main.rs` | `live` |
| `live_integrity` | `tests/live/integration/integrity/main.rs` | `tests/live/integration/integrity/main.rs` | `live` |
| `live_metrics` | `tests/live/integration/metrics/main.rs` | `tests/live/integration/metrics/main.rs` | `live` |
| `live_network` | `tests/live/network/main.rs` | `tests/live/network/main.rs` | `live` |
| `live_normalization` | `tests/live/integration/normalization/main.rs` | `tests/live/integration/normalization/main.rs` | `live` |
| `live_recovery` | `tests/live/integration/recovery/main.rs` | `tests/live/integration/recovery/main.rs` | `live` |
| `live_rest` | `tests/live/integration/rest/main.rs` | `tests/live/integration/rest/main.rs` | `live` |
| `live_websocket` | `tests/live/integration/websocket/main.rs` | `tests/live/integration/websocket/main.rs` | `live` |
| `materialize_cache` | `tests/merge/materialize/cache.rs` | `tests/historical/merge/materialize/cache.rs` | `historical` |
| `materialize_cached_reader` | `tests/merge/materialize/cached_reader.rs` | `tests/historical/merge/materialize/cached_reader.rs` | `historical` |
| `materialize_checkpoints` | `tests/merge/materialize/checkpoints.rs` | `tests/historical/merge/materialize/checkpoints.rs` | `historical` |
| `materialize_combined_builder` | `tests/merge/materialize/combined_builder.rs` | `tests/historical/merge/materialize/combined_builder.rs` | `historical` |
| `materialize_depth_builder` | `tests/merge/materialize/depth_builder.rs` | `tests/historical/merge/materialize/depth_builder.rs` | `historical` |
| `materialize_depth_metadata` | `tests/merge/materialize/depth_metadata.rs` | `tests/historical/merge/materialize/depth_metadata.rs` | `historical` |
| `materialize_manifest` | `tests/merge/materialize/manifest.rs` | `tests/historical/merge/materialize/manifest.rs` | `historical` |
| `materialize_partition` | `tests/merge/materialize/partition.rs` | `tests/historical/merge/materialize/partition.rs` | `historical` |
| `materialize_planner` | `tests/merge/materialize/planner.rs` | `tests/historical/merge/materialize/planner.rs` | `historical` |
| `materialize_publish` | `tests/merge/materialize/publish.rs` | `tests/historical/merge/materialize/publish.rs` | `historical` |
| `materialize_reader` | `tests/merge/materialize/reader.rs` | `tests/historical/merge/materialize/reader.rs` | `historical` |
| `materialize_reconstruction` | `tests/merge/materialize/reconstruction.rs` | `tests/historical/merge/materialize/reconstruction.rs` | `historical` |
| `materialize_resolver` | `tests/merge/materialize/resolver.rs` | `tests/historical/merge/materialize/resolver.rs` | `historical` |
| `materialize_resources` | `tests/merge/materialize/resources.rs` | `tests/historical/merge/materialize/resources.rs` | `historical` |
| `materialize_row_groups` | `tests/merge/materialize/row_groups.rs` | `tests/historical/merge/materialize/row_groups.rs` | `historical` |
| `materialize_timeline_reader` | `tests/merge/materialize/timeline_reader.rs` | `tests/historical/merge/materialize/timeline_reader.rs` | `historical` |
| `materialize_trade_builder` | `tests/merge/materialize/trade_builder.rs` | `tests/historical/merge/materialize/trade_builder.rs` | `historical` |
| `materialize_window` | `tests/merge/materialize/window.rs` | `tests/historical/merge/materialize/window.rs` | `historical` |
| `materialize_writer` | `tests/merge/materialize/writer.rs` | `tests/historical/merge/materialize/writer.rs` | `historical` |
| `merge_boundary_summary` | `tests/merge/depth/boundary_summary.rs` | `tests/historical/merge/depth/boundary_summary.rs` | `historical` |
| `merge_checkpoint` | `tests/merge/depth/merge_checkpoint.rs` | `tests/historical/merge/depth/merge_checkpoint.rs` | `historical` |
| `merge_combined_real` | `tests/merge/combined/merge_combined_real.rs` | `tests/historical/merge/combined/merge_combined_real.rs` | `historical` |
| `merge_commit` | `tests/merge/execution/merge_commit.rs` | `tests/historical/merge/execution/merge_commit.rs` | `historical` |
| `merge_cursor_benchmark` | `tests/merge/benchmarks/merge_cursor_benchmark.rs` | `tests/historical/merge/benchmarks/merge_cursor_benchmark.rs` | `historical` |
| `merge_depth_cursor` | `tests/merge/depth/merge_depth_cursor.rs` | `tests/historical/merge/depth/merge_depth_cursor.rs` | `historical` |
| `merge_depth_cursor_range` | `tests/merge/depth/merge_depth_cursor_range.rs` | `tests/historical/merge/depth/merge_depth_cursor_range.rs` | `historical` |
| `merge_depth_stream` | `tests/merge/depth/merge_depth_stream.rs` | `tests/historical/merge/depth/merge_depth_stream.rs` | `historical` |
| `merge_event_validation` | `tests/merge/depth/merge_event_validation.rs` | `tests/historical/merge/depth/merge_event_validation.rs` | `historical` |
| `merge_executor` | `tests/merge/execution/merge_executor.rs` | `tests/historical/merge/execution/merge_executor.rs` | `historical` |
| `merge_full_equivalence` | `tests/merge/partition/merge_full_equivalence.rs` | `tests/historical/merge/partition/merge_full_equivalence.rs` | `historical` |
| `merge_indexed_cursor` | `tests/merge/depth/merge_indexed_cursor.rs` | `tests/historical/merge/depth/merge_indexed_cursor.rs` | `historical` |
| `merge_inventory` | `tests/merge/depth/merge_inventory.rs` | `tests/historical/merge/depth/merge_inventory.rs` | `historical` |
| `merge_job` | `tests/merge/core/merge_job.rs` | `tests/historical/merge/core/merge_job.rs` | `historical` |
| `merge_kway` | `tests/merge/core/merge_kway.rs` | `tests/historical/merge/core/merge_kway.rs` | `historical` |
| `merge_kway_event` | `tests/merge/core/merge_kway_event.rs` | `tests/historical/merge/core/merge_kway_event.rs` | `historical` |
| `merge_manifest_builder` | `tests/merge/execution/merge_manifest_builder.rs` | `tests/historical/merge/execution/merge_manifest_builder.rs` | `historical` |
| `merge_output_validation` | `tests/merge/execution/merge_output_validation.rs` | `tests/historical/merge/execution/merge_output_validation.rs` | `historical` |
| `merge_parallel` | `tests/merge/execution/merge_parallel.rs` | `tests/historical/merge/execution/merge_parallel.rs` | `historical` |
| `merge_parallel_commit` | `tests/merge/execution/merge_parallel_commit.rs` | `tests/historical/merge/execution/merge_parallel_commit.rs` | `historical` |
| `merge_partition_coverage` | `tests/merge/partition/merge_partition_coverage.rs` | `tests/historical/merge/partition/merge_partition_coverage.rs` | `historical` |
| `merge_partition_equivalence` | `tests/merge/partition/merge_partition_equivalence.rs` | `tests/historical/merge/partition/merge_partition_equivalence.rs` | `historical` |
| `merge_partition_executor` | `tests/merge/partition/merge_partition_executor.rs` | `tests/historical/merge/partition/merge_partition_executor.rs` | `historical` |
| `merge_partition_input` | `tests/merge/partition/merge_partition_input.rs` | `tests/historical/merge/partition/merge_partition_input.rs` | `historical` |
| `merge_resolver` | `tests/merge/execution/merge_resolver.rs` | `tests/historical/merge/execution/merge_resolver.rs` | `historical` |
| `merge_resource_estimation` | `tests/merge/execution/merge_resource_estimation.rs` | `tests/historical/merge/execution/merge_resource_estimation.rs` | `historical` |
| `merge_timeline_writer` | `tests/merge/execution/merge_timeline_writer.rs` | `tests/historical/merge/execution/merge_timeline_writer.rs` | `historical` |
| `merge_trade_cursor` | `tests/merge/trades/merge_trade_cursor.rs` | `tests/historical/merge/trades/merge_trade_cursor.rs` | `historical` |
| `merge_trade_cursor_indexed` | `tests/merge/trades/merge_trade_cursor_indexed.rs` | `tests/historical/merge/trades/merge_trade_cursor_indexed.rs` | `historical` |
| `merge_trade_kway_real` | `tests/merge/trades/merge_trade_kway_real.rs` | `tests/historical/merge/trades/merge_trade_kway_real.rs` | `historical` |
| `merge_trade_stream` | `tests/merge/trades/merge_trade_stream.rs` | `tests/historical/merge/trades/merge_trade_stream.rs` | `historical` |
| `merge_unified_boundary_stress` | `tests/merge/partition/merge_unified_boundary_stress.rs` | `tests/historical/merge/partition/merge_unified_boundary_stress.rs` | `historical` |
| `merge_unified_commit` | `tests/merge/execution/merge_unified_commit.rs` | `tests/historical/merge/execution/merge_unified_commit.rs` | `historical` |
| `merge_unified_failure` | `tests/merge/execution/merge_unified_failure.rs` | `tests/historical/merge/execution/merge_unified_failure.rs` | `historical` |
| `merge_unified_file_rotation` | `tests/merge/output/merge_unified_file_rotation.rs` | `tests/historical/merge/output/merge_unified_file_rotation.rs` | `historical` |
| `merge_unified_manifest` | `tests/merge/execution/merge_unified_manifest.rs` | `tests/historical/merge/execution/merge_unified_manifest.rs` | `historical` |
| `merge_unified_manifest_builder` | `tests/merge/execution/merge_unified_manifest_builder.rs` | `tests/historical/merge/execution/merge_unified_manifest_builder.rs` | `historical` |
| `merge_unified_output_validation` | `tests/merge/execution/merge_unified_output_validation.rs` | `tests/historical/merge/execution/merge_unified_output_validation.rs` | `historical` |
| `merge_unified_parallel_equivalence` | `tests/merge/partition/merge_unified_parallel_equivalence.rs` | `tests/historical/merge/partition/merge_unified_parallel_equivalence.rs` | `historical` |
| `merge_unified_partition_executor` | `tests/merge/partition/merge_unified_partition_executor.rs` | `tests/historical/merge/partition/merge_unified_partition_executor.rs` | `historical` |
| `merge_unified_partition_input` | `tests/merge/partition/merge_unified_partition_input.rs` | `tests/historical/merge/partition/merge_unified_partition_input.rs` | `historical` |
| `merge_unified_production_rotation` | `tests/merge/output/merge_unified_production_rotation.rs` | `tests/historical/merge/output/merge_unified_production_rotation.rs` | `historical` |
| `merge_unified_reference_integrity` | `tests/merge/execution/merge_unified_reference_integrity.rs` | `tests/historical/merge/execution/merge_unified_reference_integrity.rs` | `historical` |
| `merge_unified_stream_resolver` | `tests/merge/execution/merge_unified_stream_resolver.rs` | `tests/historical/merge/execution/merge_unified_stream_resolver.rs` | `historical` |
| `merge_unified_timeline` | `tests/merge/combined/merge_unified_timeline.rs` | `tests/historical/merge/combined/merge_unified_timeline.rs` | `historical` |
| `merge_unified_writer` | `tests/merge/combined/merge_unified_writer.rs` | `tests/historical/merge/combined/merge_unified_writer.rs` | `historical` |
| `parquet_decimal` | `tests/processing/parquet/parquet_decimal.rs` | `tests/shared/integration/parquet/parquet_decimal.rs` | `parquet` |
| `parquet_depth_batch` | `tests/processing/parquet/parquet_depth_batch.rs` | `tests/shared/integration/parquet/parquet_depth_batch.rs` | `parquet` |
| `parquet_depth_events` | `tests/processing/parquet/parquet_depth_events.rs` | `tests/shared/integration/parquet/parquet_depth_events.rs` | `parquet` |
| `parquet_depth_real_archive` | `tests/processing/parquet/parquet_depth_real_archive.rs` | `tests/historical/normalize/parquet/parquet_depth_real_archive.rs` | `historical` |
| `parquet_depth_segments` | `tests/processing/parquet/parquet_depth_segments.rs` | `tests/shared/integration/parquet/parquet_depth_segments.rs` | `parquet` |
| `parquet_depth_writer` | `tests/processing/parquet/parquet_depth_writer.rs` | `tests/shared/integration/parquet/parquet_depth_writer.rs` | `parquet` |
| `parquet_discovery` | `tests/processing/parquet/parquet_discovery.rs` | `tests/historical/normalize/parquet/parquet_discovery.rs` | `historical` |
| `parquet_trade_batch` | `tests/processing/parquet/parquet_trade_batch.rs` | `tests/shared/integration/parquet/parquet_trade_batch.rs` | `parquet` |
| `parquet_writer` | `tests/processing/parquet/parquet_writer.rs` | `tests/shared/integration/parquet/parquet_writer.rs` | `parquet` |
| `process_executor` | `tests/processing/execution/process_executor.rs` | `tests/historical/normalize/execution/process_executor.rs` | `historical` |
| `process_executor_failure` | `tests/processing/execution/process_executor_failure.rs` | `tests/historical/normalize/execution/process_executor_failure.rs` | `historical` |
| `process_failure` | `tests/processing/execution/process_failure.rs` | `tests/historical/normalize/execution/process_failure.rs` | `historical` |
| `process_metrics` | `tests/processing/execution/process_metrics.rs` | `tests/historical/normalize/execution/process_metrics.rs` | `historical` |
| `process_parallel_consistency` | `tests/processing/execution/process_parallel_consistency.rs` | `tests/historical/normalize/execution/process_parallel_consistency.rs` | `historical` |
| `process_scheduler` | `tests/processing/execution/process_scheduler.rs` | `tests/historical/normalize/execution/process_scheduler.rs` | `historical` |
| `process_session` | `tests/processing/execution/process_session.rs` | `tests/historical/normalize/execution/process_session.rs` | `historical` |
| `process_session_finalization` | `tests/processing/execution/process_session_finalization.rs` | `tests/historical/normalize/execution/process_session_finalization.rs` | `historical` |
| `process_session_validation` | `tests/processing/execution/process_session_validation.rs` | `tests/historical/normalize/execution/process_session_validation.rs` | `historical` |
| `process_task` | `tests/processing/execution/process_task.rs` | `tests/historical/normalize/execution/process_task.rs` | `historical` |
| `processing_config` | `tests/processing/execution/processing_config.rs` | `tests/historical/normalize/execution/processing_config.rs` | `historical` |
| `processing_metrics_aggregation` | `tests/processing/execution/processing_metrics_aggregation.rs` | `tests/historical/normalize/execution/processing_metrics_aggregation.rs` | `historical` |
| `resource_file_budget` | `tests/processing/resources/resource_file_budget.rs` | `tests/historical/normalize/resources/resource_file_budget.rs` | `historical` |
| `source_compatibility` | `tests/source_compatibility.rs` | `tests/shared/integration/source/compatibility.rs` | `historical` |
| `trade_all_archives` | `tests/processing/trades/trade_all_archives.rs` | `tests/historical/normalize/trades/trade_all_archives.rs` | `historical` |
| `trade_formats` | `tests/processing/trades/trade_formats.rs` | `tests/historical/normalize/trades/trade_formats.rs` | `historical` |
| `trade_integrity_enforcement` | `tests/processing/trades/trade_integrity_enforcement.rs` | `tests/historical/normalize/trades/trade_integrity_enforcement.rs` | `historical` |
| `trade_real_archives` | `tests/processing/trades/trade_real_archives.rs` | `tests/historical/normalize/trades/trade_real_archives.rs` | `historical` |
| `trade_worker` | `tests/processing/trades/trade_worker.rs` | `tests/historical/normalize/trades/trade_worker.rs` | `historical` |
| `trade_worker_metrics` | `tests/processing/trades/trade_worker_metrics.rs` | `tests/historical/normalize/trades/trade_worker_metrics.rs` | `historical` |
