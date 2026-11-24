# Acquire historical archives

Acquisition finds historical market-data archives and preserves the original provider bytes in managed raw storage. Once the files are available locally, normalization reads them and builds canonical datasets.

**Create → Edit → Validate → Discover → Download → Inspect → Normalize**

Run the commands from your MarketForge workspace. The examples use the testing registry, where downloads still write real files to the configured data root. See the [acquisition prerequisites](index.md#prerequisites) for setup.

## 1. Create a recipe

In **Workflows → Acquisition → New recipe**, supply the half-open UTC
interval and at least one archive request. Choose the venue, instrument type
and market explicitly. **Find** in an instrument scope searches the registered
catalogue for that classification. Add another request for an additional
instrument or data kind. Validation checks the adapter's supported request
combinations. **Map** discovers whether the historical archives are available.

```bash
mf acquisition new september | cpout
mf acquisition edit september
```

`new` creates a TOML recipe in the workspace acquisition library and refuses to overwrite an existing file. `edit` opens that file in your editor and checks its structure when the editor exits successfully.

A recipe has one time interval shared by all its requests. Each request names an exchange, instrument type, market category, data type, and archive scope. This example selects one UTC day of Bybit BTCUSDT linear perpetual trades. The interval includes its start and excludes its end. Use the example as a starting point and replace the dates and request details with the data you intend to acquire.

```toml
version = 1

[interval]
start = 2026-09-01T00:00:00Z
end = 2026-09-02T00:00:00Z

[[requests]]
exchange = "bybit"
instrument_type = "perpetual"
market_category = "linear"
data_type = "trades"
scope = { kind = "instrument", identifier = "BTCUSDT" }
```

Choose combinations with `mf acquisition sources` or the [supported historical sources](../../reference/acquisition-sources.md). See the [configuration reference](../../reference/configuration/acquisition.md) for every recipe field.

## 2. Validate

```bash
mf acquisition validate september | cpout
```

Validation reads the saved TOML and checks fields, types, and interval order. A structurally valid recipe can still specify an unsupported source combination or a date with no published files. Mapping evaluates the combination and checks for archives.

## 3. Discover archives

```bash
mf acquisition map september --testing
```

Mapping finds archive identities and records their nominal time intervals without downloading archive bodies. The request table distinguishes cached discovery from discovery performed during this run. A successful search can be empty. Unsupported requests and discovery failures have separate outcomes. Use the run ID or report path in the summary to inspect every request.

## 4. Download

```bash
mf acquisition download september --testing
```

Download uses fresh discovery when available and discovers archives as needed. It verifies the size and SHA-256 of matching local copies before reusing them. New upstream revisions get separate immutable managed copies. Acquisition retrieves whole archives that overlap your interval, so a file may contain records beyond the requested dates.

## 5. Inspect

```bash
mf storage list --testing --exchange bybit --instrument BTCUSDT --data-type trades | cpout
mf runs list --testing --operation acquisition --limit 10 | cpout
mf runs report RUN_ID --testing | cpout
```

Storage shows registered files. Use the run ID from the acquisition summary to inspect discovery and file outcomes. Nominal archive intervals describe the advertised file boundaries. Event-level completeness remains unknown even when those intervals span the entire request.

## Optional operations

| Goal | Command or option |
| --- | --- |
| Inspect fresh cached discovery | `mf acquisition plan september --testing --cache-only` |
| Hash a bounded sample of registered files locally | Add `--verify-local` to the cache-only plan. |
| Check discovery again | Add `--refresh` to `map` or `download`. |
| Lower HTTP request pacing | Add `--requests-per-second 0.5` to `map` or `download`. |
| Capture machine-readable results | Put global `--json` before `acquisition`. Add `--silent` to `map` or `download` to suppress progress in human mode. |

## Troubleshooting

| Symptom | What to do |
| --- | --- |
| No archives found | Check the native identifier, scope and dates. Use `--refresh` if cached results need an upstream check. |
| Unsupported source or scope | Check the [historical source matrix](../../reference/acquisition-sources.md) and update the request. |
| Partial or failed download | Inspect the retained report and events, then follow the [recovery instructions](managing-acquisitions.md#recover-partial-or-failed-runs). |
| Invalid recipe | Correct the TOML location or field reported by `validate`. See [common validation errors](../../reference/configuration/acquisition.md#common-validation-errors). |

## Next step

[Normalize managed archives](../normalization/index.md) into canonical datasets.
