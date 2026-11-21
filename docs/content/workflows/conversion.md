# Prepare historical conversion

Forge converts the quote currency of a committed merge. The recipe describes source intent and references. The generated Rust job points to a complete publication directory, including its manifest and all primary files.

## Select a source

Create a recipe from one source TOML. Forge uses a merge recipe's retained submission and registered normalized parents. For a normalization recipe, it matches the recorded requests and retained normalization submission before following the merge lineage. For an acquisition recipe, it checks the raw scope, archive interval and digest against the normalized file's registered raw contributions, then follows the merge lineage. These routes reuse existing outputs. Legacy normalization requests based on archive IDs are unsupported as source recipes.

```bash
mf convert new example --from-merge upstream_merge --target-quote USDT --testing | cpout
mf convert new from_normalization --from-normalize upstream_normalization --target-quote USDT --testing | cpout
mf convert new from_acquisition --from-acquisition upstream_acquisition --target-quote USDT --testing | cpout
nvim data/.config/historical/conversion/example.toml
```

The managed recipe records `version = 1`, a `[source]` table with kind, reference, path and SHA-256, and a half-open `[interval]` with integer `start_ns` and `end_ns`. Edit the recipe to choose references and review settings. If the source TOML changes, review the changes and create a new conversion recipe.

- `[selection].file_dataset_ids` pins registered **merge file** IDs. Forge expands each to its parent and deduplicates the publications.
- `[selection].publication_ids` pins **registry publication primary keys**. Use this or `file_dataset_ids`.
- `[selection].expected_engine_publication_ids` optionally checks the resolved engine publication IDs using exact decimal strings.
- `[selection].whole_publication = true` explicitly approves executing all streams and rows when the requested interval is narrower. Approval still requires verified lineage and an overlapping interval.
- `[conversion]` sets `target_quote`, `price_source = "last_trade"`, positive `max_staleness_ns`, `max_hops` from 1 to 4, `path_policy` of `"all"` or `"primary"`, and optional `instrument_kinds`. The Forge preparation path still rejects plans with multiple routes.
- `[resources]` sets positive `workers`, `memory_limit_mb`, and `max_open_files`. The engine caps these with its historical runtime configuration.
- Top-level `reference_stream_ids` lists exact trade stream IDs. Top-level `reference_publication_id`, when given, denotes a separate registered **trade-only merge**. Combined and depth sources can use verified external trade merges. The converter's trade-only execution does not replay external references.

If verified outputs overlap and compete for a stream scope, prepare reports the candidate publication IDs. Pin the intended revision explicitly. Distinct noncompeting publications get separate jobs in one Forge collection. Each job needs its configured reference trades to resolve in the source or the specified external publication. File, stream, and interval selections never trims the physical input. Causal reconstruction rows remain part of the effective publication.

## Prepare the Bybit combined merge for USDT

In the workspace with the existing `engine_test_conversation` merge, create the managed conversion recipe.

```bash
mf convert new engine_test_conversion --from-merge engine_test_conversation --target-quote USDT --testing | cpout
nvim data/.config/historical/conversion/engine_test_conversion.toml
```

Keep the generated source fingerprint and interval. Set these top-level references and update the corresponding tables.

```toml
reference_stream_ids = ["bybit.spot.spot.BTCUSDT.trades", "bybit.spot.spot.BTCUSDC.trades"]

[selection]
file_dataset_ids = [360]
expected_engine_publication_ids = ["2079541492669036539"]
whole_publication = true

[conversion]
target_quote = "USDT"
price_source = "last_trade"
max_staleness_ns = 60000000000
max_hops = 2
path_policy = "all"
```

The 60-second staleness limit is a walkthrough setting. Run these commands in a shell with `pipefail` enabled to retain the Forge exit status through the pipelines.

```bash
set -o pipefail
mf convert validate engine_test_conversion | cpout
mf convert prepare engine_test_conversion --testing --dry-run | cpout
mf --json convert prepare engine_test_conversion --testing | jq '{status, compatibility, job_path, report_path, compatibility_blockers}' | cpout
```

On the verified registry, file `360` belongs to registry publication `60`, engine publication `2079541492669036539`. Its complete inventory contains one primary file with **12,477,919** events, including **5,233,312** trades and **7,244,607** depth events. The requested interval is `[2026-10-27T00:00:00Z, 2026-10-30T00:00:00Z)`. Inclusive observed endpoints are `2026-10-27T00:00:00.197000000Z` and `2026-10-29T23:59:59.991000000Z`. Endpoints describe observed extent only.

| Bybit spot instrument | Instrument ID | Native quote | Metadata route |
| --- | ---: | --- | --- |
| BTCUSDT | 1 | USDT | Identity |
| ETHUSDT | 2 | USDT | Identity |
| ETHBTC | 4 | BTC | BTC → USDT through BTCUSDT trades |
| BTCUSDC | 65 | USDC | USDC → BTC through inverse BTCUSDC trades, then BTC → USDT through BTCUSDT trades |

All four instruments have trades and depth in this publication. The reference trades are a separate selection from the eight source streams. Reachability establishes a currency route from historical base and quote metadata. The engine evaluates prior observations and staleness during event execution.

## Inspect the artifacts and compatibility

The run directory `data/runs/convert-prepare-*/` contains `prepare-report.json`, a Forge-owned `convert.json`, exact `job-*.json` files, source and conversion TOML snapshots, a historical instrument catalog snapshot, and a copy of the runtime settings. The report lists the artifact hashes, source and reference roots, registry and engine IDs, instrument namespaces, whole-publication scope, lineage, and execution blockers.

The current binary takes one `--job` document. Only `job-*.json` uses the Rust `ConvertJob` shape. It contains `job_id`, `operation = "convert"`, `input_merge`, `conversion`, `resources`, and `output`. The collection has a Forge version and indexes the jobs and evidence. Forge accepts either retained path in `mf convert run` and submits the original job bytes.

```bash
mf convert run /absolute/path/to/convert.json --testing | cpout
```

For the walkthrough source, Forge preparation reaches `generated`. Its current orchestration still expects a fixed catalog and applies its existing compatibility checks. The Rust CLI can validate the retained single job and verified catalog directly. Passing the retained catalog and adopting the engine check in Forge are separate integration work. Registration also needs a conversion operation migration and operation-aware lineage verification.

## Validate and run a single engine job

`--validate` loads the production job and runtime settings, checks the source and any external reference manifest against their published Parquet inventories and footer schemas, resolves committed normalization metadata, and plans the currency routes. It does not scan all market rows or create staging or output. Full physical and source-preservation checks run during publication.

From the project root, point `catalog` at the verified `instruments.json` retained by the preparation run for this publication. The default catalog remains `data/catalog/instruments.json`. Both paths use the same catalog loader. `--config` defaults to `data/.config/historical/general.toml`. Paths supplied to these flags are relative to `--project-root` unless absolute paths are supplied.

```bash
set -o pipefail
catalog=/absolute/path/to/verified/instruments.json
engine/target/debug/marketforge-convert \
  --job engine/tests/historical/fixtures/convert.json --catalog "$catalog" --validate 2>&1 | cpout
```

On another machine, the production job still requires absolute input and output paths. Edit a local copy of the job with `nvim /absolute/path/to/local-job.json`. For relocated provenance, `--normalization-root /absolute/path/to/normalized` selects committed directories named by the engine normalization dataset IDs in the merge manifest. Conversion checks their identity, stream metadata, processing-job checksum, and available catalog authority. Keep the relevant normalization manifests and committed processing-job snapshots with their checksums.

The combined engine prefers configured internal trade streams and uses the external trade-only publication for configured instruments absent from the internal trades. Matching depth streams remain sources. Depth-only conversion uses external trade references. Trade-only conversion uses internal trades. `all` tries planned routes in deterministic graph order and emits a single valuation from the first successful route. `primary` tries the first planned route.

Causal conversion keeps physical row order, event timestamps, and reconstruction markers. Physical row totals include reconstruction-only depth rows, and the converted manifest reports the reconstruction count separately. Internal references must precede the event in causal traversal and have a strictly earlier event-time key. At equal timestamps the stream rank, task ID, and ordinal break ties. External trades must have a strictly earlier timestamp and arrive in chronological order. The engine uses the latest eligible observation. If the most recent encountered observation is too far ahead in event time, it looks for an earlier eligible observation. Reference age equal to the staleness limit is eligible. A timestamp-only scan bounds the retained history and checkpoints preserve it across partitions. The memory and open-file values shown in the dashboard are configured caps.

Build the engine with `historical,monitor-tui` for the dashboard. `D` toggles job details, the arrow keys scroll, and `Ctrl+C` requests interruption. Set `MARKETFORGE_TUI_HOLD=1` to keep the final screen open. Close it with `q` or `Esc`. The process joins workers before returning and exits with status 130 when interrupted before completion. If an atomic commit has already begun, the final status reports its actual outcome. `--materialize` leaves validated job and runtime context with staged Parquet and the execution receipt and shows **Materialized (unpublished)**. `--publish` validates and commits that staging directory. **Published** appears after the atomic rename. The dashboard also reports **Failed** and **Interrupted**, and prints the outcome and artifact paths after restoring the terminal. Interrupted materialization can leave staging that must be reviewed before reuse.

```bash
engine/target/debug/marketforge-convert --job /absolute/path/to/local-job.json --catalog "$catalog" --materialize --tui
```

## Verify conversion changes

`engine/tests/historical/support/conversion_fixture.rs` loads the canonical single job with the production parser. Contract tests use that job unchanged or derive invalid fields explicitly. Portable execution tests derive jobs with locally declared overrides. They build complete trade, depth, combined, or external-reference inputs using production physical schemas and checksummed normalization job snapshots. Every writing case owns a temporary directory. The equivalence checks follow committed manifest inventories and compare logical Arrow rows and reference provenance across file and row-group boundaries.

Use the bounded runner from `engine/`. Compiling separately keeps each case within the runner's 30-second limit. The monitor feature adds dashboard and PTY coverage. Omit it for a portable non-TUI check.

```bash
set -o pipefail
run_dir=$(mktemp -d)
cargo test --locked --offline --features historical,monitor-tui \
  --test historical_convert --bin marketforge-convert --no-run --message-format=json > "$run_dir/artifacts.jsonl" && \
python3 scripts/bounded_tests.py "$run_dir/artifacts.jsonl" "$run_dir/results" \
  --run --targets 'historical_convert|marketforge-convert' 2>&1 | cpout
```

The recorded target requires `historical-real-tests` and an explicit catalog. It verifies engine publication `2079541492669036539` from registry publication 60 and primary file dataset 360. Set `MARKETFORGE_RECORDED_CONVERT_INPUT` to an absolute path for an exact copy of that publication if the canonical input path is unavailable. Set `MARKETFORGE_RECORDED_CONVERT_NORMALIZED` to the relocated normalization provenance root when needed. The full test also accepts `MARKETFORGE_RECORDED_CONVERT_CONFIG` for an absolute runtime config path. Preserve the merge manifest, its publication checksums, and all timeline files. No registry or network connection is required. Missing inputs or an incompatible publication fail the selected test.

The prefix case checks the first 4,096 causal rows with their preceding in-publication reference history. It stays in temporary staging and is never published.

```bash
set -o pipefail
export MARKETFORGE_RECORDED_CONVERT_CATALOG=/absolute/path/to/verified/instruments.json
cargo test --locked --offline --features historical-real-tests --test historical_recorded_convert \
  -- validates_canonical_recorded_prefix_with_causal_prehistory --exact 2>&1 | cpout
```

Select the full recorded conversion deliberately. It reads and validates the entire 12,477,919-row publication and needs temporary disk space for the conversion and validation. The test publishes only under its temporary directory.

```bash
set -o pipefail
cargo test --locked --offline --features historical-real-tests --test historical_recorded_convert \
  -- publishes_canonical_recorded_conversion --exact --ignored 2>&1 | cpout
```
