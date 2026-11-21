# Prepare historical conversion

Forge converts the quote currency of a committed merge. The recipe describes source intent and references. The generated Rust job points to a complete publication directory, including its manifest and all primary files.

## Select a source

Create a recipe from one source TOML. Forge uses a merge recipe's retained submission and registered normalized parents. For a normalization recipe, it matches the recorded requests and retained normalization submission before following the merge lineage. For an acquisition recipe, it checks the raw scope, archive interval and digest against the normalized file's registered raw contributions, then follows the merge lineage. These routes reuse existing outputs. Legacy normalization requests based on archive IDs are unsupported as source recipes.

```bash
set -o pipefail
mf convert new example --from-merge upstream_merge --target-quote USDT --testing 2>&1 | cpout
mf convert new from_normalization --from-normalize upstream_normalization --target-quote USDT --testing 2>&1 | cpout
mf convert new from_acquisition --from-acquisition upstream_acquisition --target-quote USDT --testing 2>&1 | cpout
nvim data/.config/historical/conversion/example.toml
```

The managed recipe records `version = 1`, a `[source]` table with kind, reference, path and SHA-256, and a half-open `[interval]` with integer `start_ns` and `end_ns`. Edit the recipe to choose references and review settings. If the source TOML changes, review the changes and create a new conversion recipe.

- `[selection].file_dataset_ids` pins registered **merge file** IDs. Forge expands each to its parent and deduplicates the publications.
- `[selection].publication_ids` pins **registry publication primary keys**. Use this or `file_dataset_ids`.
- `[selection].expected_engine_publication_ids` optionally checks the resolved engine publication IDs using exact decimal strings.
- `[selection].whole_publication = true` explicitly approves executing all streams and rows when the requested interval is narrower. Approval still requires verified lineage and an overlapping interval.
- `[conversion]` sets `target_quote`, `price_source = "last_trade"`, positive `max_staleness_ns`, `max_hops` from 1 to 4, `path_policy` of `"all"` or `"primary"`, and optional `instrument_kinds`. The Engine plans and validates route eligibility.
- `[resources]` sets positive `workers`, `memory_limit_mb`, and `max_open_files`. Forge resolves these against the historical runtime at preparation.
- Top-level `reference_stream_ids` lists exact trade stream IDs. Top-level `reference_publication_id`, when given, denotes a separate registered **trade-only merge**. Combined and depth sources can use verified external trade merges. The converter's trade-only execution does not replay external references.

If verified outputs overlap and compete for a stream scope, prepare reports the candidate publication IDs. Pin the intended revision explicitly. Distinct noncompeting publications get separate jobs in one Forge collection. Each job needs its configured reference trades to resolve in the source or the specified external publication. File, stream, and interval selections never trim the physical input. Causal reconstruction rows remain part of the effective publication.

## Prepare the Bybit combined merge for USDT

In the workspace with the existing `engine_test_conversation` merge, create the managed conversion recipe.

```bash
set -o pipefail
mf convert new engine_test_conversion --from-merge engine_test_conversation --target-quote USDT --testing 2>&1 | cpout
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
mf engine build convert 2>&1 | cpout
mf convert validate engine_test_conversion 2>&1 | cpout
mf convert prepare engine_test_conversion --testing --dry-run 2>&1 | cpout
mf --json convert prepare engine_test_conversion --testing | jq '{status, compatibility, job_path, report_path, compatibility_blockers}' 2>&1 | cpout
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
[prepared conversion configuration](../configuration/conversion.md) for the
fields and their authority. Re-prepare older jobs from their Forge recipes.
Forge accepts retained job and collection paths for `run`. Conversion
registration is unavailable.

```bash
set -o pipefail
mf convert validate /absolute/path/to/job-123.json 2>&1 | cpout
mf convert run /absolute/path/to/convert.json --testing 2>&1 | cpout
```

## Run a single prepared job

The Engine needs the declared physical merge publications and their regular
commit metadata. The paths in the job are absolute, so it can run from any
working directory.

```bash
set -o pipefail
cargo build --release --manifest-path engine/Cargo.toml --features historical,monitor-tui --bin marketforge-convert 2>&1 | cpout
./engine/target/release/marketforge-convert --job /absolute/path/to/job-123.json --validate 2>&1 | cpout
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
output, and commits it. In the dashboard, `D` toggles details, arrow keys scroll,
and `Ctrl+C` requests interruption. The Engine joins the worker and restores
the terminal before returning. `MARKETFORGE_TUI_HOLD=1` keeps the final screen
open for review.

## Verify conversion changes

Portable tests generate complete tiny publications and run the production binary
from unrelated directories after removing test-generated preparation inputs.

```bash
set -o pipefail
cargo test --manifest-path engine/Cargo.toml --features historical,monitor-tui --test historical_convert --bin marketforge-convert 2>&1 | cpout
cargo test --manifest-path engine/Cargo.toml --features historical --test historical_convert --bin marketforge-convert 2>&1 | cpout
cargo test --manifest-path engine/Cargo.toml --features historical-real-tests --test historical_recorded_convert \
  validates_canonical_recorded_prefix_with_causal_prehistory -- --exact --nocapture 2>&1 | cpout
```

The smoke test binds the canonical job to merge `2079541492669036539`, checks the
pinned metadata, and stages its first 4,096 causal events. It retains snapshots
and internal reference observations and checks eight streams and three routes.
Set `MARKETFORGE_RECORDED_CONVERT_INPUT` to the absolute location of an exact
copy if the publication has moved. Full recorded publication is a separate
ignored test, `publishes_canonical_recorded_conversion`.
