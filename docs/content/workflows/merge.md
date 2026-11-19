# Merge registered normalized files
Check [supported layouts](#supported-layouts) before choosing inputs. Use dataset inspection to confirm one compatible stream, its publication identity and the required primary parts.

Install the managed merge binary with `mf engine build merge`. Use the [configured testing PostgreSQL registry](../getting-started/installation.md#postgresql-credentials) for discovery and preparation.

## Start from a normalization recipe

```bash
mf merge new first_merge --from-normalize engine_test --testing
mf merge validate first_merge
mf merge prepare first_merge --testing
```

`new` reads the existing managed normalization recipe and creates `data/.config/historical/merge/first_merge.toml`. A TOML path also works as the source. Creation is exclusive and reports matching registered files and known blockers. `validate` checks recipe structure without a database connection. `prepare` resolves the live registry and validates the resulting job with Rust. Both creation and preparation stop before processing.

```toml
version = 1

[interval]
start = 2026-10-27T00:00:00Z
end = 2026-10-30T00:00:00Z

[selection]
source = "engine_test"
operation = "normalize"
mode = "all"
```

The source determines the eligible requests. `mode = "all"` considers all eligible files for those requests. Narrow the interval or add filters under `[selection]`. All filters must match the same stream membership.

```toml
data_types = ["trades"]
instruments = ["bybit.spot.spot.BTCUSDT"]
```

| Selection field | Contract |
| --- | --- |
| `source` | Required normalization recipe. |
| `operation`, `mode` | Default to `normalize` and `all`. These are the only accepted values. |
| `data_types` | Optional nonempty subset of `trades` and `order_book_l2`. |
| `exchanges`, `instruments` | Optional nonempty lists of lowercase registered venue codes and canonical `exchange.instrument_type.market_category.symbol` identities. |
| `instrument_types` | Optional nonempty subset of `spot`, `perpetual`, `future`, `option`, `equity`. |
| `market_categories` | Optional nonempty subset of `spot`, `linear`, `inverse`, `option`, `equity`. |
| `publication_ids` | Optional nonempty list of distinct positive PostgreSQL publication IDs. Get these from the discovery report or `mf --json datasets show FILE_ID --testing` under `publication.publication_id`. |

Omitting `data_types` keeps the types requested by the source. The resolver uses resolved normalization tasks to establish historical family membership and checks the applicable Gate.io feed and registered source key.

Forge discovers eligible files in `historical.datasets` and their shared evidence in `historical.dataset_publications`. A retained execution intent links a source configuration to a submission when the prepared requests and interval agree with the current source and the submitted batch fingerprint agrees with the recorded publication. A single matching verified submission can supply multiple publications. If multiple verified submissions match, or the matching files lack a verified submission link, add `publication_ids = [ID]` to approve specific registry publications. Use the publication IDs in the preparation report, which differ from file dataset IDs and engine IDs. Approval still requires a matching stream and interval. For legacy normalization requests with explicitly listed archive IDs, use explicit file selection.

The interval is half-open and must fit the engine’s signed 64-bit nanosecond range. Forge converts the recipe's timezone-aware datetime values to integer Unix nanoseconds. The observed maximum timestamp of a file is inclusive, so a file with a maximum equal to the merge start is eligible, while a file starting at the merge end is outside the range. The report lists observed envelopes, uncovered parts of the requested interval and files extending beyond it. Observed envelopes provide no proof of continuity inside the interval. Use explicit nanosecond bounds for precision finer than the datetime parser's microsecond resolution.

Missing requests, incomplete inputs and unresolved publication choices can block registry preparation. A blocked preparation returns exit 1 and retains `prepare-report.json`. If registry preparation can resolve the selection but the engine rejects it, the job and report are retained with status `generated` and compatibility `not_validated`. Execution requires validation of the exact submitted job.

## Select file dataset IDs explicitly

```bash
mf merge new manual_merge
```

Edit `data/.config/historical/merge/manual_merge.toml`. Use either this explicit format or the source-derived format for each recipe. Replace the illustrative IDs with file dataset IDs returned by normalization registration or `mf datasets list --testing` and set the requested half-open interval in integer nanoseconds.

```toml
dataset_ids = [1, 2]
start_ns = 1727683200000000000
end_ns = 1727769600000000000
```

```bash
mf merge validate manual_merge
mf merge prepare manual_merge --testing
mf merge run manual_merge --testing
mf merge run manual_merge --testing --tui
```

Choose one run command. Both modes prepare one `MergeJob`, submit it, verify the committed physical timeline and register the output in PostgreSQL. TUI mode requires an interactive terminal. For machine output, use `mf --json merge run manual_merge --testing`.

## Selection and runtime

Explicit recipes preserve their listed file order. Source-derived recipes use a stable market-identity ordering. The public JSON contains `job_id`, `operation`, `inputs`, `time_range` and `output`. Each entry in `inputs` has the PostgreSQL file `dataset_id`, its absolute `path` and `sha256`, and a `publication` object with `engine_dataset_id` (a decimal string), `manifest_path` and `manifest_sha256`. The manifest describes source ownership and auxiliary artifacts. Its presence does not guarantee that a single selected depth file has enough reconstruction state.

Set execution options in `data/.config/historical/general.toml`. The Rust CLI uses `--config PATH` to override that location. The shared configuration supplies worker count, memory budget, open-file limit, and Parquet sizes. Merge requires whole-MiB byte budgets. The optional merge settings default to the values below.

```toml
[merge]
partitions_per_worker = 4
compression_level = 3
```

Rust verifies the selected files and their owning manifests. Forge retains registry and source-recipe evidence in the preparation report. To submit the same job later, use `mf merge run data/runs/PREPARE_ID/merge-job.json --testing`, replacing `PREPARE_ID` with the preparation directory. The job JSON carries no runtime overrides.

## Supported layouts

Managed merge accepts v3 single-stream inputs and can select one concrete trade stream from a timestamp-first v4/v5 publication with contiguous source ownership and no repeated source spans. Supported trade selections may contain one part, several parts of the same stream, or parts from different publications. The current engine accepts one selected stream per publication. Native-order or interleaved trade layouts and already-combined inputs are unsupported.

Depth currently requires a compatible single-stream v3 publication and explicit selection of all its primary files for reconstruction. Rust reads the shared auxiliary state from the publication. Requests missing required primary parts are rejected before processing.

### Merge tests

The ordinary `historical_merge` target runs under the `historical` feature using source-controlled and generated fixtures. It requires no local datasets, PostgreSQL, or prior normalization.

```bash
cargo test --manifest-path engine/Cargo.toml --features historical --test historical_merge -- --list
cargo test --manifest-path engine/Cargo.toml --features historical --test historical_merge -- --nocapture
```

Reference bounds, event kinds, duplicate identities and zero-change events use typed inventories. Stream metadata, provenance, partitioning, checkpoint reconstruction, publication and corruption cases use test-owned fixtures. Their primary files use production Arrow schemas and their processing-job checksums are local to each temporary directory. The production combined rotation case uses four primary files and 16,390 total events to cross one materialization window and rotate at the minimum configured 1 MiB file target.

### Optional recording compatibility

Opt-in tests derive their selections from `engine/tests/historical/fixtures/merge.json`. They require the pinned normalized manifests, selected Parquet files, and applicable processing provenance and sidecars. To use an identical relocated copy, set `MARKETFORGE_MERGE_FIXTURE_FROM` and `MARKETFORGE_MERGE_FIXTURE_TO` to absolute path prefixes. Both primary and manifest paths relocate together. The tests verify the existing checksums and require no PostgreSQL connection.

```bash
cargo test --manifest-path engine/Cargo.toml --features historical --test historical_merge bounded_recorded:: -- --ignored --nocapture
cargo test --manifest-path engine/Cargo.toml --features historical --test historical_merge validates_real_ -- --ignored --nocapture
cargo test --manifest-path engine/Cargo.toml --features historical --test historical_merge merges_two_bounded_recorded_trade_streams -- --ignored --nocapture
```

Explicit requests fail with the missing prerequisite when a pinned artifact is absent. The checks select their inputs before opening primary Parquet or resolving the runtime job.

The workload guard caps runtime resolution at four selected files. Trade compatibility reads primaries of up to 256 rows and 1 MiB each. The K-way case compares trades from two concrete streams. The v5 trade commit case compares physical values with the selected source Parquet using one and two workers. The depth reference check compares 16 index events starting at an initialization. It caps the selected primary at 8 MiB and also caps sidecar bytes, row-group size and snapshot levels. The miniature fixtures exercise depth reconstruction and combined publication.

Separate bounded probes verify the guards for missing depth reconstruction dependencies and replay of multiple concrete ranks from one publication. The cursor performance comparison remains ignored in the ordinary target. Bounded partition comparisons and publication tests replace the former full-day equivalence and full canonical execution cases.

Launch a **validated** prepared job with the TUI by substituting its path for `PATH_TO_VALIDATED_JOB`. The direct Rust command publishes files. Forge is responsible for PostgreSQL registration.

```bash
cargo run --manifest-path engine/Cargo.toml --locked --features historical,monitor-tui --bin marketforge-merge -- --job PATH_TO_VALIDATED_JOB --project-root . --tui
```

## Publication and recovery

A recipe uses a fresh output under `data/datasets/merged/<engine-job-id>` relative to the configured data root. Rust publishes the validated physical timeline, manifest, resolved job, input manifest checksums and physical validation evidence atomically. Forge revalidates the committed physical Parquet and byte fingerprints and registers each primary output in `historical.datasets` under one shared publication. Selected input file IDs are retained as publication-scope lineage.

The report under `data/runs/merge-execute-*/execution-result.json` distinguishes execution, verification and database registration. A complete report includes each file dataset ID and its path. Failed or interrupted runs return nonzero. One merge job publishes one output.

After a power loss or database error, retry registration by passing the execution intent, committed directory, its `manifest.json`, or a particular primary file path.

```bash
mf merge register data/runs/MERGE_EXECUTION_ID/execution-intent.json --testing
mf merge register data/datasets/merged/ENGINE_JOB_ID --testing
```

Registration-only recovery checks the output and required input lineage again. Identical retries reuse the database ID.

### Empty processing partitions

A job can pass preparation and still fail with `merge stream resource estimation requires at least one active stream` when the requested interval is much wider than the selected observations. This occurred in the one-trade synthetic example with a full-day interval. The same input successfully merged over its one-nanosecond observed interval. Inspect the preparation report's observed bounds and narrow the interval when that matches your intent.
