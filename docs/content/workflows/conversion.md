# Prepare historical conversion

Forge converts the quote currency of a supported committed [merge publication](../reference/publication-merge.md#verification-and-handoff). The recipe describes source intent and references. The generated Rust job points to a complete publication directory, including its manifest and all primary files.

## Select a source

### Start from a registered merge

List merged publications in the registry you intend to use. Copy the registry publication ID and inspect it before creating the conversion recipe.

```bash
mf datasets list --testing --view publications --operation merge
mf datasets show --publication-id MERGED_PUBLICATION_ID --testing
mf convert new merged_usdt --from-publication MERGED_PUBLICATION_ID --target-quote USDT --testing
mf convert show merged_usdt
```

The recipe binds that exact publication and uses its committed half-open time range. It approves the whole publication, including retained state reconstruction. Review the reference trade selection in the recipe before preparing. The target quote alone does not establish available reference observations or their freshness.

```bash
nvim data/.config/workflows/data_engineering/conversion/merged_usdt.toml
mf convert prepare merged_usdt --testing --dry-run
```

### Start from a recipe

Create a recipe from one source TOML. Forge uses a merge recipe's retained submission and registered normalized parents. For a normalization recipe, it matches the recorded requests and retained normalization submission before following the merge lineage. For an acquisition recipe, it checks the raw scope, archive interval and digest against the normalized file's registered raw contributions, then follows the merge lineage. These routes reuse existing outputs. Legacy normalization requests based on archive IDs are unsupported as source recipes.

```bash
set -o pipefail
mf convert new example --from-merge upstream_merge --target-quote USDT --testing
mf convert new from_normalization --from-normalize upstream_normalization --target-quote USDT --testing
mf convert new from_acquisition --from-acquisition upstream_acquisition --target-quote USDT --testing
nvim data/.config/workflows/data_engineering/conversion/example.toml
```

A recipe-based source records `version = 1`, a `[source]` table with kind, reference, path and SHA-256, and a half-open `[interval]` with integer `start_ns` and `end_ns`. Edit the recipe to choose references and review settings. If the source TOML changes, review the changes and create a new conversion recipe.

For recipe-based sources, use file IDs when you started from registered primary files and publication IDs when you already know the owning merge. Forge expands the selected files to their complete publications. If your requested interval is narrower than a source publication, approve whole-publication processing deliberately. Confirm the approved publication’s stream and row scope before submitting it.

### Review reference trades

Choose reference trade streams that can support the target quote at the event times you need. A separate trade-only merge can supply references for combined and depth inputs. Check staleness limits and any currency route gaps before accepting the result. The [recipe reference](../configuration/conversion.md) lists exact selector names, defaults, and constraints. The [prepared job reference](../reference/prepared-conversion.md) describes the resolved bindings Forge hands to the Engine.

If verified outputs overlap and compete for a stream scope, prepare reports the candidate publication IDs. Pin the intended revision explicitly. Distinct noncompeting publications get separate jobs in one Forge collection. Each job needs its configured reference trades to resolve in the source or the specified external publication. File, stream, and interval selections never trim the physical input. Causal reconstruction rows remain part of the effective publication.

## Recorded case: Bybit combined merge to USDT

This recorded case uses an existing registered three-day Bybit spot merge. The recipe name and numeric IDs were assigned in that workspace. For a new run, normalize and merge the markets you need, then obtain your file IDs with [dataset inspection](datasets.md). Use a whole publication with the required trade streams.

```bash
set -o pipefail
mf convert new engine_test_conversion --from-merge engine_test_conversation --target-quote USDT --testing
nvim data/.config/workflows/data_engineering/conversion/engine_test_conversion.toml
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

The 60-second staleness limit is a walkthrough setting. The JSON report can be saved for inspection without mixing stderr into the document.

```bash
set -o pipefail
mf engine build convert
mf convert validate engine_test_conversion
mf convert prepare engine_test_conversion --testing --dry-run
mf --json convert prepare engine_test_conversion --testing > conversion-prepare.json
jq '{status, compatibility, job_path, report_path, compatibility_blockers}' conversion-prepare.json
```

On the verified registry, file `360` belongs to registry publication `60`, engine publication `2079541492669036539`. Its complete inventory contains one primary file with **12,477,919** events, including **5,233,312** trades and **7,244,607** depth events. The requested interval is `[2026-10-27T00:00:00Z, 2026-10-30T00:00:00Z)`. Inclusive observed endpoints are `2026-10-27T00:00:00.197000000Z` and `2026-10-29T23:59:59.991000000Z`. Endpoints describe observed extent only.

| Bybit spot instrument | Instrument ID | Native quote | Metadata route |
| --- | ---: | --- | --- |
| BTCUSDT | 1 | USDT | Identity |
| ETHUSDT | 2 | USDT | Identity |
| ETHBTC | 4 | BTC | BTC → USDT through BTCUSDT trades |
| BTCUSDC | 65 | USDC | USDC → BTC through inverse BTCUSDC trades, then BTC → USDT through BTCUSDT trades |

All four instruments have trades and depth in this publication. The reference trades are a separate selection from the eight source streams. Reachability establishes a currency route from historical base and quote metadata. The engine evaluates prior observations and staleness during event execution.

## Inspect the prepared job

Forge saves a collection named `convert.json`, a `prepare-report.json`, and one
`job-*.json` per physical publication. The collection indexes jobs and evidence.
The report distinguishes job generation from successful production Engine
validation. A dry run reports intended paths without creating files or running
the Engine. The retained catalog and TOML snapshots document preparation.

The individual job includes the stream bindings, scoped instrument metadata,
publication pins, and effective runtime. See the
[prepared conversion job](../reference/prepared-conversion.md) for the
fields and their authority. Re-prepare older jobs from their Forge recipes.
Forge accepts retained job and collection paths for `run`. Conversion
registration is unavailable.

```bash
set -o pipefail
mf convert validate /absolute/path/to/job-123.json
mf convert run /absolute/path/to/convert.json --testing
```

## Run a single prepared job

The Engine needs the declared physical merge publications and their regular
commit metadata. The paths in the job are absolute, so it can run from any
working directory.

```bash
set -o pipefail
cargo build --release --manifest-path engine/Cargo.toml --features historical,monitor-tui --bin marketforge-convert
./engine/target/release/marketforge-convert --job /absolute/path/to/job-123.json --validate
./engine/target/release/marketforge-convert --job /absolute/path/to/job-123.json --run --tui
```

`--validate` checks the schema, publication IDs and hashes, Parquet inventory,
stream bindings, references, and plan. It reads input bytes for checksums
without executing conversion or creating staging or output. The same checks
precede materialization and publication.

Combined conversion prefers configured internal trade streams and uses external
trades for configured instruments absent from the internal trades. Matching depth
streams remain sources. Depth-only conversion uses a trade-only external merge.
Trade-only conversion uses internal trades. `all` tries planned routes in
deterministic order and emits one valuation from the first successful route.
`primary` tries the first planned route.

Causal conversion preserves physical order, timestamps, and reconstruction
markers. Internal references must precede the event in causal traversal and have
a strictly earlier event-time key. At equal timestamps, stream rank, task ID,
and ordinal break ties. External trades require a strictly earlier timestamp.
Reference age equal to the staleness limit is eligible.

`--materialize` writes staged Parquet and execution evidence. `--publish` checks
the job and effective settings against the staged specification, validates the
output, and commits it. The dashboard shows physical rows and throughput for
the active phase. The conversion ETA covers the physical work only. The
partition table retains completed and failed partitions, and unavailable
valuations have their own counts and reasons.

Use `D` for details, arrows or Page Up and Page Down to navigate, and `?` for
help. `Ctrl+C` requests interruption. The Engine joins the worker and restores
the terminal before returning. `MARKETFORGE_TUI_HOLD=1` keeps the final screen
open for review. Press `Q` to close it.

To preview the dashboard without accessing recorded inputs, generate a complete
12-row job in a fresh temporary directory after building the release binary.

```bash
set -o pipefail
preview=$(mktemp -d)
MARKETFORGE_CONVERSION_PREVIEW_DIR="$preview" \
  cargo test --manifest-path engine/Cargo.toml --features historical \
  --test historical_convert portable::export_conversion_dashboard_preview \
  -- --ignored --exact --nocapture &&
  job=$(find "$preview" -name job.json -print -quit) &&
  MARKETFORGE_TUI_HOLD=1 ./engine/target/release/marketforge-convert --job "$job" --run --tui
```

For recorded and portable regression checks, see [conversion verification](../development/testing.md#conversion-verification).

### Verify the portable result without a terminal UI

Run from the checkout with Rust and the converter built as above. After exporting the preview job into a fresh directory, run these commands in place of the dashboard command.

```bash
./engine/target/release/marketforge-convert --job "$job" --validate
./engine/target/release/marketforge-convert --job "$job" --run
jq '{statistics, integrity}' "$(dirname "$job")/converted/manifest.json"
```

The synthetic fixture has 12 physical rows, including one reconstruction-only input. The published counts are six identity, five converted, and one unavailable. The report records `source_preservation = true` and `conversion_validation = true`. An unavailable valuation is retained when the conversion policy cannot use a reference. See the [conversion publication contract](../reference/publication-converted.md) for the output schema.
