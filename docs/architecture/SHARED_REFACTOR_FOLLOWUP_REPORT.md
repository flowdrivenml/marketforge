# Shared refactor focused follow-up

Date: 2026-10-28. Paths are repository-relative unless described as package-relative. Changes are uncommitted.

## Baseline and scope

Repository: `/home/pasha/projects/bybit-history-downloader`. Package: `engine/Cargo.toml`, `marketforge-engine`, edition 2024. Branch: `refactor/shared-architecture`; HEAD: `f24f1eddac97cc0f0472f3b0ed9501ae2d77a686`. Branch and HEAD were checked again at completion and are unchanged. No applicable ancestor/package `AGENTS.md` exists; `.planning/live/AGENTS.md` governs separate planning work. The first-pass [report](SHARED_REFACTOR_REPORT.md), current manifest, explicit test registrations, source tree, external tests, and existing bounded runner were inspected.

The starting tree already contained substantial tracked and untracked first-pass changes. They were protected, including the original report and validation evidence. No Git index mutation, commit, branch change, configuration change, worktree, stash, reset, cleanup, dependency installation/upgrade, lockfile removal, authenticated operation, exchange configuration edit, or dataset regeneration was performed. No unrelated Python/assets/credentials were edited. New evidence lives under `engine/target/refactor-followup-validation/` (abbreviated **V** below); first-pass `target/refactor-validation/` is untouched.

Baseline evidence: `V/baseline-status.txt`, `baseline.diff`, `baseline-manifest.toml`, `baseline-source.json`, and `baseline-tests-source.json`. The source snapshot includes untracked Rust files, so `V/incremental.diff` compares this follow-up against the actual first-pass tree, rather than against HEAD. `changed-files.json` lists its 271 affected Rust paths, predominantly authoritative import migrations. The manifest is unchanged by this follow-up; `target-map.json` records identical before/after features and explicit targets. Cargo metadata is in `metadata.json` (205 targets, including 195 explicit integration suites).

Before source edits, this command passed from the repository root:

```sh
cargo test --manifest-path engine/Cargo.toml --locked --offline --features historical,live,monitor-tui --no-run --message-format=json > engine/target/refactor-followup-validation/baseline-tests.jsonl 2> engine/target/refactor-followup-validation/baseline-tests.log
```

From `engine/`, baseline executable discovery used:

```sh
python3 scripts/bounded_tests.py target/refactor-followup-validation/baseline-tests.jsonl target/refactor-followup-validation/baseline-discovery
```

It discovered 3,265 cases, including 16 existing ignored cases. Compilation produced the existing private trade-CSV dead-code warnings; no baseline compiler error reproduced. This is compilation/discovery evidence, not a claim that baseline execution passed. First-pass assertion/environment failures were explicitly retained as validation concerns.

## Physical layout, compatibility, and ownership

The final physical source root is exactly:

```text
engine/src/
  bin/
  historical/
  live/
  shared/
  tui/
  lib.rs
```

`src/storage/mod.rs` and its empty directory are removed. Its compatibility contents now appear inline at the library boundary in `src/lib.rs`. The outer gate remains `any(live, historical, parquet)`. Both `storage::parquet` and `storage::historical_compat` remain gated by `parquet`; the latter retains all seven schema/decimal exports. Live without Parquet therefore retains the same empty compatibility namespace. No deprecation warnings, duplicate writers, replacement `storage.rs`, backend scaffolds, or speculative PostgreSQL module were introduced.

Implementation and ordinary tests now use `shared::persistence::parquet` directly, including consumers formerly navigating through Historical's Parquet facade. Historical's `TradeSink` and `DepthSink` implementations remain in `historical/normalize/parquet/sinks.rs`. The facade remains available externally. Root domain/error/format/normalization/validation/source/job compatibility exports also remain available.

Root `process::convert` and `process::merge` callers now use `historical::{convert,merge}`; processing callers use `historical::normalize`. Existing callers of the normalization facade's merge/convert exports also use the actual sibling owner. Grouped imports and binary/integration-test crate prefixes were resolved individually; `std::process`, CLI names, fields and dataset strings were preserved. The task-taking `formats::trades::identity_policy(&WorkTask)` adapter is owned by `historical::job`; shared format parsing/policy APIs remain under `shared::formats`. Internal adapter callers use their actual owner. Library compatibility exports were preserved, with no public re-export lost in the source audit.

Complete repository searches included hidden source/documentation paths, grouped uses, qualified calls, aliases and explicit Cargo targets. No examples/benches directory or custom harness registration exists in this package. Final `storage::` implementation usage is confined to the deliberate compatibility integration test. `std::process` uses remain. Configuration fields named storage were not changed. Search evidence: `path-search.txt`, `storage-full-search.txt`, `final-path-search.txt`.

## Nested modules and file changes

`historical/convert/execution/mod.rs`, `historical/convert/output/mod.rs`, and `historical/merge/mod.rs` now contain declarations and deliberate direct nested exports. Their private aliases recreating the old flat module layout were removed. Implementation consumers reference the nested implementation paths directly. `V/private-alias-map.json` records every old parent alias and its actual nested owner. Module visibility was not broadened; no production `#[path]` attributes or replacement facade layers were added.

All these paths are package-relative. Every old file below was removed by an ordinary filesystem move, preserving its implementation at the destination:

| Old path beneath `src/` | New path beneath `src/` |
|---|---|
| `historical/convert/output/writers/combined_numeric.rs` | `historical/convert/output/validation/combined_numeric.rs` |
| `historical/convert/output/writers/depth_numeric.rs` | `historical/convert/output/validation/depth_numeric.rs` |
| `historical/convert/output/validation/combined_parallel_validation.rs` | `historical/convert/output/validation/combined_parallel.rs` |
| `historical/convert/output/validation/combined_validation.rs` | `historical/convert/output/validation/combined.rs` |
| `historical/convert/output/validation/depth_parallel_validation.rs` | `historical/convert/output/validation/depth_parallel.rs` |
| `historical/convert/output/validation/depth_validation.rs` | `historical/convert/output/validation/depth.rs` |
| `historical/convert/output/validation/source_validation.rs` | `historical/convert/output/validation/source.rs` |
| `historical/convert/output/validation/validation.rs` | `historical/convert/output/validation/trades.rs` |
| `shared/persistence/parquet/writer.rs` | `shared/persistence/parquet/trade_writer.rs` |

Contents confirmed the responsibility of each validator before moving. Public subsystem exports, validation algorithms, error handling, schemas and writer behavior remain the same. Declarations/consumers use the new names directly. `file-map.json` contains the machine-readable map. `non-import-audit.json` compares non-import source text after normalizing path/whitespace changes: its remaining differences are inline compatibility relocation, a rustfmt trailing call comma, the preserved inventory ordering assertion, and the one added compatibility test. No Historical arithmetic, ordering implementation, schema, nullability, or state machine changed.

## Stray-file reachability and preserved coverage

The complete Rust/Cargo search checked declarations, explicit targets, `include!`, test registrations and external suites. None of the four questioned files was registered or included. Removal evidence, body comparison and original trade assertions are retained in `V/orphan-evidence.json`; full originals are also in `baseline-source.json`.

| Removed package-relative file | Reason and retained coverage |
|---|---|
| `src/storage/mod.rs` | Compatibility glue moved verbatim into the inline library module; tested by `parquet_writer::legacy_storage_imports_remain_available`. |
| `src/historical/convert/depth_equivalence.rs` | Its sole `checkpointed_depth_matches_sequential` function body is whitespace-equivalent to the registered `tests/historical/convert/depth/depth_equivalence.rs` case. That suite also retains its additional concurrent-worker scenario and helpers. No unique assertion/fixture was discarded. |
| `src/historical/merge/trades/merge_trade_inventory.rs` | Same dataset 120, manifest source-task assertion, active task IDs and 162-row assertion already exist in `tests/historical/merge/trades/merge_trade_stream.rs::accepts_dataset_with_empty_source_task`. The stray additionally checked the returned task-ID order; the registered test now checks the unsorted IDs, preserving that stronger assertion rather than losing it. No production ordering change. |
| `src/historical/convert/provenance.rs` | Unregistered superseded copy of `output/publication/provenance.rs`. All content after the import section is whitespace-equivalent; differences were imports, including an unused `EvaluatedReference` import. Active provenance behavior and existing conversion publication/output suites remain registered. |
| `src/historical/normalize/runs/mod.rs` | Zero-byte unregistered file, with no declared module, target, include or supported public boundary. Its empty directory was removed. No tests or implementation existed there. |

The nine renamed implementation files are accounted for in the preceding move table; their contents and coverage were retained. No other source/test deletion was made. No orphan test was newly registered: both scenarios already had discovered integration identities and remain registered exactly once.

## Compiler adjustments

No pre-existing Historical compiler repair was needed. All follow-up compiler changes were import/module/re-export wiring:

- Removing private aliases initially produced E0433 for `merge::materialize_cache`, `merge::source`, and `convert::output::files` at qualified call/type sites. They now resolve respectively to `materialize::readers::cache`, `core::source`, and `output::shared::files`.
- Renaming the trade validator initially produced E0432 for `output::validation::{ConversionValidationMetrics, validate_converted_trades_with_progress}` in publication manifest/receipt/publication consumers. These now import `output::validation::trades` directly; the stable public output facade remains unchanged.
- E0282 at `File::open` and related closures in manifest/trade validation was cascading from the unresolved discovery import. Correcting the import resolved inference without adding type annotations or changing logic.

The first two intermediate `cargo check --manifest-path engine/Cargo.toml --locked --offline --features historical,live,monitor-tui --all-targets` attempts failed; raw diagnostics remain in `first-check.log` and `second-check.log`. The third passed (`third-check.log`), followed by the final feature matrix below. A temporary migration-script parse failure was corrected before the final import pass; it did not require a source workaround. No code was disabled or compiler errors suppressed.

## Build matrix and formatting

All commands below run from `engine/`, with no installs or network access. Every matrix row exited 0. Exact argument arrays, durations and outcomes are in `V/build-matrix.json`; logs are `check-<name>.log` and `tests-<name>.{jsonl,log}`.

Production/test/example target checks use `cargo check --locked --offline --no-default-features --all-targets`, with these suffixes:

| Name | Feature suffix |
|---|---|
| minimal (also the supported empty default feature set) | none |
| parquet | `--features parquet` |
| live | `--features live` |
| historical | `--features historical` |
| terminal | `--features monitor-tui` |
| live-tui | `--features live,monitor-tui` |
| historical-tui | `--features historical,monitor-tui` |
| historical-live | `--features historical,live` |
| combined | `--features historical,live,monitor-tui` |
| arrow-select | `--features arrow-select` |

Test executables were separately compiled with `cargo test --locked --offline --no-default-features --no-run --message-format=json`, for minimal, `--features parquet`, `--features live`, and `--features historical`. Final combined compilation also passed:

```sh
cargo test --manifest-path engine/Cargo.toml --locked --offline --features historical,live,monitor-tui --no-run --message-format=json > engine/target/refactor-followup-validation/final-tests-combined-v2.jsonl 2> engine/target/refactor-followup-validation/final-tests-combined-v2.log
```

That command is expressed from the repository root, as actually invoked. The earlier passing combined build is preserved separately as `final-tests-combined.{jsonl,log}`. Library, binaries (including benchmark test harness compilation), and every applicable explicit test target compiled. Benchmark mains were not executed. No examples/custom harness targets are registered. This checks independent feature boundaries rather than relying only on a combined build; it does not exhaust the powerset of optional features, platforms or toolchains.

`cargo fmt --manifest-path engine/Cargo.toml --check` and `git diff --check` passed from the repository root (`fmt-check.log`, `diff-check.log`). Metadata initially invoked from the repository root without `--manifest-path` failed to locate a manifest; the corrected offline command with `--manifest-path engine/Cargo.toml` produced `metadata.json`. This was a command-location error, not a package diagnostic.

## Test identities and bounded execution

Baseline and final combined executable discovery contain 3,265 and 3,266 cases respectively. All 3,265 old target/name pairs map identically to themselves, with identical ignore states; none was lost, renamed or registered twice. The only addition is `parquet_writer::legacy_storage_imports_remain_available`, under the existing Parquet-gated target. It checks both legacy namespaces, trade/depth writer types, three schema exports, decimal constants and the decimal conversion roundtrip. Full old-to-new identities are in `test-identity-map.json`; source identities and explicit feature/target registrations are in `baseline-tests-source.json` and `target-map.json`.

Existing `scripts/bounded_tests.py` was reused unchanged. Cargo compilation/linking was separate. Each selected fully qualified case ran in its own process from `engine/` with Cargo environment/library paths and a 30-second monotonic deadline including setup. PASS requires output confirming exactly one passed case. On timeout the runner terminates the owned process group, waits 0.2 seconds and kills survivors; no unrelated processes are targeted. The wrappers in V only record command sequences, not a competing supervision implementation.

Exact execution commands and regex selections are in `V/execution-commands.json`; `run-selected.py` records their argument construction. They invoke the existing runner against `final-tests-combined-v2.jsonl`, writing fresh `offline-run/` and `orphan-coverage-run/` directories. The broad selection reuses the first-pass audited offline target set: library, book/shared Parquet/depth contracts, selected local normalization pipelines, materialization, planning, and offline Live suites. Persistent-output workflows were not opted into. The affected orphan scenarios were selected separately by exact target/name filters.

Standalone Parquet execution, from `engine/`, was:

```sh
python3 scripts/bounded_tests.py target/refactor-followup-validation/tests-parquet.jsonl target/refactor-followup-validation/standalone-parquet-run --run --targets 'parquet_(decimal|depth_batch|depth_events|depth_segments|depth_writer|trade_batch|writer)'
```

Its 42 selected cases passed without Historical (41 existing plus the compatibility check); 155 other discovered cases remained NOT_RUN. This runner invocation exited 1 because selection does not cover its entire discovered inventory, not because any selected Parquet case failed. Its counts/log are in `standalone-parquet-run.log` and `standalone-parquet-run/`. These repeats are not double-counted below.

Unique final combined outcomes, merging the two disjoint selections:

| Status | Cases |
|---|---:|
| `PASS` | 2695 |
| `FAIL` | 8 |
| `TIMED_OUT_30S` | 1 |
| `IGNORED_EXISTING` | 16 |
| `SKIPPED_NETWORK` | 0 |
| `BLOCKED_MISSING_FIXTURE` | 0 |
| `BLOCKED_ENVIRONMENT` | 67 |
| `NOT_RUN` | 479 |

The broad run's raw outcomes were 2,694 PASS, 75 FAIL, 16 IGNORED_EXISTING and 481 NOT_RUN. Review classified 67 of its raw failures as BLOCKED_ENVIRONMENT because their logs show local HTTP/WebSocket mock listener binding denied with `PermissionDenied` / `Operation not permitted`. Those raw failures remain intact. The separate orphan run added one PASS and one timeout, replacing two NOT_RUN entries. All three selective execution commands exited 1, accurately preserving incomplete/failed outcomes.

Every identity, executable, raw exit, elapsed time and log path is retained in `consolidated-raw-results.json`, `reviewed-results.json`, and `result-status-groups.json`. Raw per-run `results.json`, `results.jsonl`, `discovery.json` and numbered logs remain available. No inferred missing-fixture classification or ignored/network double counting was used. The external network target's eleven tests were already ignored; all 16 existing ignored cases retain IGNORED_EXISTING. No network suite, ignored heavy-data case or benchmark main was launched.

Retained assertion failures:

- `marketforge_engine::historical::merge::execution::shared::resources::tests::rejects_empty_streams` — `engine/target/refactor-followup-validation/offline-run/00121.log`.
- `dataset_manifest::creates_valid_dataset_manifest` — `engine/target/refactor-followup-validation/offline-run/02088.log`.
- `convert_job::loads_combined_conversion_job` — `engine/target/refactor-followup-validation/offline-run/02256.log`.
- `convert_job::loads_depth_conversion_job` — `engine/target/refactor-followup-validation/offline-run/02257.log`.
- `convert_job::loads_trade_conversion_job` — `engine/target/refactor-followup-validation/offline-run/02258.log`.
- `live_exchange_contracts::contracts::rate_limits::bootstraps_rest_for_all_real_exchange_profiles` — `engine/target/refactor-followup-validation/offline-run/02501.log`.
- `live_exchange_contracts::contracts::rest::resolution::resolves_all_real_exchange_rest_endpoints` — `engine/target/refactor-followup-validation/offline-run/02507.log`.
- `depth_spec::compiles_all_depth_normalization_specs` — `engine/target/refactor-followup-validation/offline-run/02941.log`.

These are the same eight assertion identities recorded in the first-pass report: depth spec, dataset manifest, three conversion job expectations, empty-stream resource expectations, and two Live endpoint-count expectations. Fresh baseline runtime reproduction was not performed in this follow-up; the prior report documents its own reproduction evidence. Current logs are retained rather than treating prior knowledge as a pass. No assertions/configuration/algorithms were changed to repair these failures.

Affected orphan-case execution:

- `merge_trade_stream::accepts_dataset_with_empty_source_task`: **PASS**, 4.047s; `engine/target/refactor-followup-validation/orphan-coverage-run/00101.log`.
- `convert_depth_equivalence::checkpointed_depth_matches_sequential`: **TIMED_OUT_30S**, 30.200s; `engine/target/refactor-followup-validation/orphan-coverage-run/02948.log`.

The depth checkpoint equivalence case reached its 30-second deadline and was terminated, including the bounded termination grace period. Its fixture paths were available, and its body is unchanged from the already registered scenario. Timeout is incomplete runtime evidence, not a pass or proof of regression; it was not retried with a longer limit. The inventory case passed with its preserved ordering assertion.

The 479 NOT_RUN identities remain explicitly listed in the status inventory. They include remaining Historical conversion/merge/normalization and heavy fixture scenarios outside the audited safe selection. Some write persistent job-derived output, so they were not launched or redirected. Doctests/custom harnesses and interactive terminal rendering/lifecycle checks are separately NOT_RUN: the package has no custom harness registrations or Rust fenced source examples, and the existing runner supervises libtest cases only. No unbounded alternate harness was launched.

## Deferred architecture and remaining limits

- `live/tui/` owns the currently Live-available application state/models; `tui/live/` contains planned renderer scaffolding. A future consolidated presentation home under `tui/live/` should consume Live state through a stable application API. That requires an explicit migration plan: current state must remain accessible with Live alone, while terminal renderers stay behind `monitor-tui`. No screens, scaffold behavior, precision/suffix formatting, feature gates or terminal dependencies were redesigned here.
- Shared terminal palette, guard and reusable format functions remain under `shared/terminal`; application screens remain outside Shared. Similar formatting names were not treated as proof of identical behavior.
- `shared/io/mod.rs` owns reusable reader/decoder sources but still gates them through Historical. Shared source ownership does not imply an independently selectable I/O capability. That feature decision remains deferred; existing semantics are unchanged.
- `shared/persistence/config.rs` currently holds `ParquetResourceConfig`. Revisit backend-specific placement when another backend actually exists, without narrowing its current supported availability now. Future PostgreSQL implementation belongs under `shared/persistence/postgres/`; none was created.
- Historical merge planners, conversion engines, workflow-specific Parquet representations, Source Stream Runtime and substantive Historical repairs remain out of scope. The known assertion failures, sandbox mock-listener blocks, checkpoint timeout, unexecuted cases and untested platform/feature combinations prevent a claim that all tests pass or the branch is merge-ready.

The requested structural cleanup is complete. The first-pass report remains unchanged; the bounded-runner README links to this follow-up. All work remains uncommitted on the original branch.
