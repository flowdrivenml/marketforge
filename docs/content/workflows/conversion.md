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
- `[conversion]` sets `target_quote`, `price_source = "last_trade"`, positive `max_staleness_ns`, `max_hops` from 1 to 4, `path_policy` of `"all"` or `"primary"`, and optional `instrument_kinds`. The installed engine rejects plans with multiple routes even with `"primary"`.
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

For the walkthrough source, preparation reaches `generated` and `run` blocks launch. The source uses causal physical schema 2, while conversion validates chronological timestamp/rank ordering. Its catalog lookup assumes the first normalization task owns each stream, and its identity-only reference lookup rejects an instrument when a depth stream shares the selected trade instrument's identity. The binary also loads the catalog from `data/catalog/instruments.json` under the project root, with no catalog argument. Forge retains a verified historical snapshot and reports a missing or conflicting fixed catalog. It never installs the snapshot at that location.

The engine follow-up is to support causal physical reads, resolve exact normalized stream provenance, resolve reference trades independently of depth, and expose nonexecuting validation and a pinned catalog input. A separate Forge registration phase would need compatible committed outputs, a new operation migration, and operation-aware registry lineage verification.
