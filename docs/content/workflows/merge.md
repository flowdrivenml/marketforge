# Merge registered normalized files
Check [supported layouts](#supported-layouts) before choosing inputs. Use dataset inspection to confirm concrete stream ownership, publication identities, and required primary parts.

Install the managed merge binary with `mf engine build merge 2>&1 | cpout`. Use the [configured testing PostgreSQL registry](../getting-started/installation.md#postgresql-credentials) for discovery and preparation.

## Start from a normalization recipe

```bash
mf merge new first_merge --from-normalize engine_test --testing 2>&1 | cpout
mf merge validate first_merge 2>&1 | cpout
mf merge prepare first_merge --testing 2>&1 | cpout
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
mf merge new manual_merge 2>&1 | cpout
```

Edit `data/.config/historical/merge/manual_merge.toml`. Use either this explicit format or the source-derived format for each recipe. Replace the illustrative IDs with file dataset IDs returned by normalization registration or `mf datasets list --testing` and set the requested half-open interval in integer nanoseconds.

```toml
dataset_ids = [1, 2]
start_ns = 1727683200000000000
end_ns = 1727769600000000000
```

```bash
mf merge validate manual_merge 2>&1 | cpout
mf merge prepare manual_merge --testing 2>&1 | cpout
mf merge run manual_merge --testing 2>&1 | cpout
mf merge run manual_merge --testing --tui
```

Choose one run command. Both modes prepare one `MergeJob`, submit it, verify the committed physical timeline and register the output in PostgreSQL. TUI mode requires an interactive terminal. For machine output, use `mf --json merge run manual_merge --testing`.

## Selection and runtime

Explicit-file recipes assign priority in order of the first file listed for each stream. Source-derived recipes use the first matching normalization request, with expanded members ordered by canonical stream ID. Streams without a matching request use ascending canonical stream ID. The preparation report shows stream IDs, content types, ranks, and file counts alongside separate totals for files, streams, and publications.

Either recipe may override selected priorities using a top-level TOML table. Quote canonical IDs to keep punctuation literal. Unspecified streams take the lowest unused ranks in their default order.

```toml
[stream_ranks]
"binance.inverse.perpetual.BTCUSD_PERP.trades" = 3
"binance.linear.future.BTCUSDT_261225.trades" = 7
```

Forge uses the committed manifest's file ownership and content type. Older task-layout publications supply their single stream through the checksummed processing job. The public JSON carries each selected file's `stream_id`, `stream_rank`, and `content_type` with its existing file ID, SHA-256, path, and publication pin. Depth appears as `depth` in the job and as `order_book_l2` in recipe discovery filters. Use the exact reported stream ID in overrides. For the full schema, see the [prepared job reference](../reference/prepared.md#merge-jobs).

Rust verifies every selected primary and its owning stream against the pinned publication. Files in one stream repeat its assigned rank. Lower ranks receive priority at equal timestamps across streams. This priority expresses a merge policy. Original source order and task-local row references are preserved within a stream. Removing files to run a trades-only or depth-only selection can leave gaps in the ranks. Preserve those ranks when preparing a filtered job under a new output path.

Normalization, merge, and conversion resolve the default runtime from `data/.config/historical/general.toml` under the selected project root. Direct merge and conversion accept `--config PATH` and resolve relative overrides against `--project-root`. Merge uses workers, memory, open files, partitioning, and Parquet settings. The optional merge table defaults to the values below.

```toml
[merge]
partitions_per_worker = 4
compression_level = 3
```

Forge copies and checksums the TOML at preparation. Execution pins a copy in its execution directory, rejects configuration drift for a prepared job, and passes that same copy to Rust for validation and execution. The intent records the TOML checksum and resolved settings. The committed `merge-runtime.json` records effective settings and its SHA-256 is in `publication-checksums.json`. A direct job-file run without a preparation report pins the runtime at launch. Re-prepare after changes to ranks, binary, or runtime. Older submissions require regeneration with `mf merge prepare RECIPE --testing`.

## Supported layouts

A timestamp-ordered v4/v5 trade publication may contribute multiple concrete streams and multiple parts per stream. Each selected stream must have contiguous source spans with no repeated task spans. Compatible consecutive trade publications can share one rank when the exact stream ID and instrument namespace agree and the committed task metadata describes the same instrument. Publications without a verified namespace remain distinct. Equal or overlapping timestamps at a cross-publication boundary for the same stream are ambiguous and rejected. Each row retains its original publication, task, and task-local offset.

Depth-only and combined execution support complete concrete streams from segmented v4/v5 depth publications in causal mode. Select all primary parts of each requested depth stream. A single publication may supply several independently ranked streams. Merge verifies the pinned event index, source boundaries and segments for those streams, then replays their committed event sequences. Each initialization, reset, update and zero-row event retains its original meaning. Independent source tasks start at their committed initialization. Depth streams spanning multiple publications require a supported continuity contract and are currently rejected.

### Timeline ordering

Forge prepares an explicit `timeline_ordering` for each job. Set `timeline_ordering = "chronological"` or `timeline_ordering = "causal"` at the top level of an explicit or source-derived recipe to override the resolved mode. Forge defaults segmented depth selections to causal and other selections to chronological. Older prepared jobs that omit the field use chronological ordering. The resolved field participates in the prepared job fingerprint and is recorded in the committed manifest.

- **Chronological** emits nondecreasing event timestamps and uses the submitted rank at equal timestamps. This retains timestamp partitions and the existing physical schema. Segmented depth requires the causal contract.
- **Causal** chooses the next available stream head by original timestamp, then submitted rank at equality. Only that stream advances. A timestamp regression is retained in source order. This deterministic policy establishes a replay sequence across independent streams.

Causal physical schema version 2 stores a zero-based `timeline_sequence` across all output files. Read files in manifest order and rows in sequence order, including any timestamp regressions. A single forward logical merge produces contiguous sequence partitions for budgeted physical materialization. Timestamp statistics report the actual minimum and maximum written timestamps. The timestamp-based conversion workflow still requires chronological input.

The half-open request interval selects observations. If a depth event needs earlier initialization or intervening updates outside the interval, merge includes the necessary committed segment prefix and marks those events `reconstruction_only`. These rows are replay dependencies and are excluded from ordinary observation membership in Forge. The manifest records their count separately. Missing primary parts, missing initialization and corrupt metadata produce dependency or integrity errors before commit.

### Merge tests

The ordinary `historical_merge` target runs under the `historical` feature using source-controlled and generated fixtures. It requires no local datasets, PostgreSQL, or prior normalization.

```bash
cargo test --manifest-path engine/Cargo.toml --features historical --test historical_merge -- --list 2>&1 | cpout
cargo test --manifest-path engine/Cargo.toml --features historical --test historical_merge -- --nocapture 2>&1 | cpout
```

Reference bounds, event kinds, duplicate identities and zero-change events use typed inventories. Stream metadata, provenance, partitioning, checkpoint reconstruction, publication and corruption cases use test-owned fixtures. Their primary files use production Arrow schemas and their processing-job checksums are local to each temporary directory. The production combined rotation case uses four primary files and 16,390 total events to cross one materialization window and rotate at the minimum configured 1 MiB file target.

### Optional recording compatibility

Opt-in tests derive their selections from `engine/tests/historical/fixtures/merge.json`. They require the pinned normalized manifests, selected Parquet files, and applicable processing provenance and sidecars. To use an identical relocated copy, set `MARKETFORGE_MERGE_FIXTURE_FROM` and `MARKETFORGE_MERGE_FIXTURE_TO` to absolute path prefixes. Both primary and manifest paths relocate together. The tests verify the existing checksums and require no PostgreSQL connection.

```bash
cargo test --manifest-path engine/Cargo.toml --features historical --test historical_merge bounded_recorded:: -- --ignored --nocapture 2>&1 | cpout
cargo test --manifest-path engine/Cargo.toml --features historical --test historical_merge validates_real_ -- --ignored --nocapture 2>&1 | cpout
cargo test --manifest-path engine/Cargo.toml --features historical --test historical_merge merges_two_bounded_recorded_trade_streams -- --ignored --nocapture 2>&1 | cpout
```

Explicit requests fail with the missing prerequisite when a pinned artifact is absent. The checks select their inputs before opening primary Parquet or resolving the runtime job.

The workload guard caps runtime resolution at four selected files. Trade compatibility reads primaries of up to 256 rows and 1 MiB each. The K-way case compares trades from two concrete streams. The v5 trade commit case compares physical values with the selected source Parquet using one and two workers. The depth reference check compares 16 index events starting at an initialization. It caps the selected primary at 8 MiB and also caps sidecar bytes, row-group size and snapshot levels. The miniature fixtures exercise depth reconstruction and combined publication.

The bounded trade probe commits two ranks from one v5 publication. The recorded depth-only and mixed probes commit a Gate.io segment with a real timestamp regression and compare every emitted reference and depth payload to the normalized data. The ordinary suite exercises sparse ranks, multiple depth streams in one publication, dependencies, zero-row events, input permutations and sequence rotation across worker counts. The cursor performance comparison remains opt-in.

Launch a **validated** prepared job with the TUI by substituting its path for `PATH_TO_VALIDATED_JOB`. The direct Rust command publishes files. Forge is responsible for PostgreSQL registration.

```bash
cargo run --manifest-path engine/Cargo.toml --locked --features historical,monitor-tui --bin marketforge-merge -- --job PATH_TO_VALIDATED_JOB --project-root . --tui
```

## Reading the merge dashboard

Run with `--tui` in an interactive terminal. The header shows the requested selection, UTC interval with an exclusive end, and timeline ordering. The center shows the active phase, elapsed time, current rate when sampled, partition work, and recent activity. Press `D` for details, use the arrow keys to scroll, and press `Ctrl+C` to interrupt. Details include exact nanoseconds, the full job ID, configuration paths and SHA-256, effective runtime limits, the output path, and the report location.

- **Checking inputs** counts selected primary files whose integrity was verified.
- **Building timeline** counts logical event references. Its total is unknown until the timeline has been built. Individual partitions can process many references before any partition finishes.
- **Writing Parquet** counts written output events against the completed timeline reference count. **Checking timeline** validates the logical references. **Verifying output** counts physical output events against the expected output and reports checked files. Each check remains pending until it starts.
- **Active / worker cap** compares current running partitions with the effective configured maximum. A causal timeline uses one forward logical pass. Parquet work can use multiple admitted workers. Queued work counts planned partitions that have yet to start. The dashboard shows a queue of work, with live memory and open-file consumption unavailable.
- The output breakdown distinguishes trade and depth observations from reconstruction context events. Canonical payload rows are a separate count available in Details. Throughput uses samples from the current phase. ETA is omitted because these samples lack a stable estimate of remaining work.

The default runtime is `data/.config/historical/general.toml` under the project root. The direct Rust `--config` override resolves relative paths from the same root. Forge pins a copy for validation and execution, records both its original location and loaded copy in the execution intent, and checks the prepared fingerprint before launching. The dashboard shows the effective values already handed to the engine. The source TOML SHA-256 identifies the loaded configuration bytes. The separately checksummed `merge-runtime.json` records the effective values used for the committed publication.

**Engine committed** indicates the Rust publication is ready for Forge verification. The persistent Forge summary distinguishes execution, verification, and registration and lists the registered file IDs when available. Direct Rust execution prints the engine outcome and manifest path after restoring the terminal.

## Publication and recovery

A recipe uses a fresh output under `data/datasets/merged/<engine-job-id>` relative to the configured data root. Rust publishes the validated physical timeline, manifest, resolved job, effective runtime, input manifest checksums and physical validation evidence atomically. Forge revalidates the committed physical Parquet and byte fingerprints and registers each primary output in `historical.datasets` under one shared publication. Selected input file IDs are retained as publication-scope lineage.

The report under `data/runs/merge-execute-*/execution-result.json` distinguishes execution, verification and database registration. A complete report includes each file dataset ID and its path. Failed or interrupted runs return nonzero. One merge job publishes one output.

After a power loss or database error, retry registration by passing the execution intent, committed directory, its `manifest.json`, or a particular primary file path.

```bash
mf merge register data/runs/MERGE_EXECUTION_ID/execution-intent.json --testing 2>&1 | cpout
mf merge register data/datasets/merged/ENGINE_JOB_ID --testing 2>&1 | cpout
```

Registration-only recovery checks the output and required input lineage again. Identical retries reuse the database ID.
