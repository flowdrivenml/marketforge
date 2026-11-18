# Merge registered normalized files

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

Missing requests, incomplete or degraded inputs, incompatible layouts and unresolved publication choices block preparation. A blocked preparation returns exit 1 and retains the report under `data/runs/merge-prepare-*/prepare-report.json`. Global `--json` includes the candidate file IDs, stream memberships, engine publication identities and reasons. Narrow the recipe explicitly to pursue a compatible subset. For example, a source containing mixed v5 trades and depth may support a recipe for one timestamp-first trade stream. The full recipe remains blocked.

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

Files from each publication must appear consecutively in an explicit recipe, and the listed order defines precedence. Source-derived selection groups publications by the earliest member in canonical venue, instrument type, category, symbol, content type, observed start, registry publication ID and primary path order. Within each publication, the same key orders the files. Forge verifies each owning publication and records the ordered manifest-relative files, their checksums and the shared manifest checksum in `primary_files`. Rust verifies membership and consumes events from the selected files. Publication precedence follows the recipe. Within a publication, canonical source order resolves ties across selected parts. Forge reports gaps in the observed extents of selected streams.

Both operations read `data/.config/historical/general.toml`. Managed merge preparation uses the shared Rust runtime loader for worker count, memory budget, open-file limit and Parquet sizes. The byte budgets sent to the current merge engine must be whole MiB. Optional merge settings have these defaults.

```toml
[merge]
partitions_per_worker = 4
compression_level = 3
```

These settings are resolved into the current Rust JSON contract as `resources`, `partitioning` and `parquet`. A managed preparation freezes the ordered file dataset IDs, engine IDs, absolute registered paths, manifest-relative primary files, file checksums, manifest fingerprints, namespaces, selected streams and requested range. A source-derived report also retains the source recipe fingerprint and discovery evidence. Later registry additions do not change the prepared job's input selection. Execution rechecks the registered inputs and fingerprints. To replay exactly that job, use `mf merge run data/runs/PREPARE_ID/merge-job.json --testing`. A standalone `MergeJob` JSON supplies its own embedded execution settings and must include `primary_files`, absolute publication paths, and matching registered fingerprints. Prepare a new job if you have an old whole-publication request.

## Supported layouts

Managed merge accepts v3 single-stream inputs and can select one concrete trade stream from a timestamp-first v4/v5 publication with contiguous source ownership and no repeated source spans. Supported trade selections may contain one part, several parts of the same stream, or parts from different publications. The current engine accepts one selected stream per publication. Native-order or interleaved trade layouts and already-combined inputs are unsupported.

Depth currently requires a compatible single-stream v3 publication and explicit selection of all its primary files for reconstruction. Rust reads the shared auxiliary state from the publication. Requests missing required primary parts are rejected before processing.

## Publication and recovery

A recipe uses a fresh output under `data/datasets/merged/<engine-job-id>` relative to the configured data root. Rust publishes the validated physical timeline, manifest, resolved job, input manifest checksums and physical validation evidence atomically. Forge revalidates the committed physical Parquet and byte fingerprints and registers each primary output in `historical.datasets` under one shared publication. Selected input file IDs are retained as publication-scope lineage.

The report under `data/runs/merge-execute-*/execution-result.json` distinguishes execution, verification and database registration. A complete report includes each file dataset ID and its path. Failed or interrupted runs return nonzero. One merge job publishes one output.

After a power loss or database error, retry registration by passing the execution intent, committed directory, its `manifest.json`, or a particular primary file path.

```bash
mf merge register data/runs/MERGE_EXECUTION_ID/execution-intent.json --testing
mf merge register data/datasets/merged/ENGINE_JOB_ID --testing
```

Registration-only recovery checks the output and required input lineage again. Identical retries reuse the database ID.
