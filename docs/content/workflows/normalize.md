<a id="normalize-registered-data"></a>

# Normalize historical data

Normalization converts acquired or registered historical archives into canonical trade and order-book datasets. MarketForge resolves the registered files, applies source-format profiles, runs the Rust Engine, and verifies and registers the resulting Parquet files.

| Input or result | Role |
| --- | --- |
| Raw archive | Preserves the original bytes selected through acquisition or external registration. |
| Normalization profile | Describes the file format, timestamps, raw fields, and trade or order-book operations. |
| Normalization recipe | Selects registered inputs and a time interval. |
| Canonical dataset | Contains normalized Parquet and the evidence needed to inspect it or use it in later processing. |

<a id="select-the-raw-inputs"></a>

## 1. Check prerequisites

- [Acquire historical archives](acquisition/historical-data.md) or [register external data](acquisition/external-data.md), then check the [managed copies](acquisition/managing-acquisitions.md). A file on disk becomes eligible for managed selection through its registry record.
- Check your installed [normalization profiles](../reference/profiles.md) and the [instrument and historical specifications](catalogue/specifications.md) applicable over the full archive intervals. Preparation and Engine validation check the evidence and the selected source interpretation.
- Use a configured [normalization Engine](../getting-started/installation.md#verify-and-troubleshoot) and the database that owns the raw records. The commands below use `--testing` throughout.

For the example below, look for a complete Bybit BTCUSDT linear perpetual trades archive for 27 October 2026 in testing storage. If the source format has no applicable profile, use the [profile authoring guide](catalogue/profiles.md) before preparing the recipe.

## 2. Create a normalization recipe

```bash
mf normalize new my_dataset
```

This creates a managed TOML draft, opens your editor, and checks the saved recipe when the editor exits successfully. Choose an unused recipe name. For an independent copy of an existing acquisition recipe, use the following command in place of plain `new`. The copy can be edited without affecting the acquisition recipe, and later changes to the original do not propagate.

```bash
mf normalize new my_dataset --from-acquisition getting_started
```

For this walkthrough, save one UTC day of Bybit BTCUSDT linear perpetual trades:

```toml
version = 1

[interval]
start = 2026-10-27T00:00:00Z
end = 2026-10-28T00:00:00Z

[[requests]]
exchange = "bybit"
instrument_type = "perpetual"
market_category = "linear"
data_type = "trades"
scope = { kind = "instrument", identifier = "BTCUSDT" }
```

The interval includes its start and excludes its end. It selects whole overlapping archives, which can contain records outside the requested dates. See the [configuration reference](../configuration/normalization.md) for source selection and the older explicit archive-ID recipe format.

## 3. Validate the recipe

```bash
mf normalize validate my_dataset | cpout
```

Validation checks TOML syntax, required fields, and local recipe constraints. Use preparation to resolve registered archives and instrument metadata, and persisted preparation to check the batch with Rust.

<a id="prepare-and-check-the-batch"></a>

## 4. Preview preparation

```bash
mf normalize prepare my_dataset --testing --dry-run | cpout
```

Check the selected archives, job, task and stream counts, ranking resolution, and any input, specification, profile, or coverage diagnostics. A dry run showing `prepared` means Forge resolved the plan. The displayed artifact paths are prospective, and compiled batch validation happens during persisted preparation. Resolve blockers before continuing. If an archive appears among the matches but is missing from the selected inputs, look for its local archive ID in the blockers. Nominal archive intervals describe file boundaries and leave event-level completeness unknown.

<a id="run-and-verify"></a>

## 5. Execute normalization

```bash
mf normalize execute my_dataset --testing
```

This resolves inputs, writes and validates the prepared batch, runs the Rust Engine, verifies committed output, and registers eligible primary files. Check the final execution, verification, and registration statuses. A run can commit Parquet and still need registration recovery before those files appear in the dataset list. For the same workflow with an interactive progress monitor, run:

```bash
mf normalize execute my_dataset --testing --tui
```

## 6. Inspect normalized datasets

```bash
mf datasets list --testing | cpout
mf datasets show DATASET_ID --testing | cpout
mf datasets verify DATASET_ID --testing | cpout
```

Use a registry primary-file ID from the list or the normalization result. Engine dataset IDs identify Engine output, publication IDs identify registered groups of files, and execution IDs identify retained attempts. See the [dataset guide](datasets.md) for schema, row previews, and lineage.

## Optional advanced execution

Use [manual batch submission](../reference/prepared-normalization.md#manual-batch-submission) when preparation and execution need to happen separately. The [CLI reference](../reference/cli/normalize.md) compares the execution modes.

<a id="recover-from-a-failure"></a>

## Troubleshooting

| Problem | First check |
| --- | --- |
| No selected inputs | Recipe scope, interval, and registered archive status. |
| Missing specification or incompatible profile | Applicability across the archive and the registered source interpretation. |
| Rust validation or execution failed | Preparation diagnostics or retained execution report. |
| Output committed but registration failed | Retained execution intent and [registration recovery](../reference/cli/normalize.md#registration-and-recovery). |

## Next steps

Use registered primary files in [dataset inspection](datasets.md) or [merge](merge.md). See [runtime configuration](../configuration/normalization-runtime.md) for resource and integrity settings. See the [publication reference](../reference/publication-normalized.md) for manifests and integrity evidence.
