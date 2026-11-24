# Acquisition and raw import recipes

Save acquisition recipes in `data/.config/workflows/data_engineering/acquisition/`. Commands accept the managed name with an optional `.toml` suffix. Each `[[requests]]` entry selects one archive collection for the shared interval. Unknown fields are rejected.

## Scope and provider options

| Field | Accepted values and constraints |
| --- | --- |
| `version` | Integer `1`, the default. |
| `interval.start`, `interval.end` | Required timezone-aware datetimes. Start must precede end. The interval includes start and excludes end. |
| `requests` | Required array of tables with at least one entry. |
| `requests[].exchange` | Required. Historical adapters use `binance`, `bybit`, `bitget`, `okx`, or `gateio`. Structural validation accepts any string matching `^[a-z][a-z0-9_]*$`. |
| `requests[].instrument_type` | Required. `spot`, `perpetual`, `future`, or `option`. |
| `requests[].market_category` | Required. `spot` for spot, `linear` or `inverse` for perpetuals and futures, `option` for options. Adapter support varies. |
| `requests[].data_type` | Required. `trades` or `order_book_l2`. |
| `requests[].scope.kind` | Required. `instrument`, `family`, or `base_coin`, as required by the adapter. |
| `requests[].scope.identifier` | Required nonblank provider-native identifier. Adapters accept letters, digits, underscore, dot, colon, slash and hyphen. Reserved identifiers `.`, `..`, and `ANY` are rejected. |
| `requests[].source_options.feed` | Gate.io L2 only. `incremental` (default) or `snapshot`. Omit `source_options` for other requests. |

Choose supported combinations from [historical sources](../acquisition-sources.md). Gate.io's feed changes archive selection and source identity. Choose the recording format required for later processing. Discovery applies each provider's archive calendar to select overlapping whole files. See [time, selection and coverage](../../concepts/time.md).

## Complete example

This OKX request selects inverse dated-future trades for the `BTC-USD` family over one UTC+8 calendar day.

```toml
version = 1

[interval]
start = 2026-09-01T00:00:00+08:00
end = 2026-09-02T00:00:00+08:00

[[requests]]
exchange = "okx"
instrument_type = "future"
market_category = "inverse"
data_type = "trades"
scope = { kind = "family", identifier = "BTC-USD" }
```

For an instrument-scoped Bybit recipe, see the [acquisition tutorial](../../workflows/acquisition/historical-data.md). For Bybit options, set both `instrument_type` and `market_category` to `option` and use `scope = { kind = "base_coin", identifier = "BTC" }`.

## Common validation errors

| Diagnostic | Correction |
| --- | --- |
| Invalid TOML or unexpected field | Fix the reported line or field. Use `[[requests]]` for each request and put its fields under that entry. |
| Missing timezone or unordered interval | Add `Z` or an explicit offset and set `end` later than `start`. |
| Invalid source options | Keep `source_options` only for a `gateio` request with `data_type = "order_book_l2"`. |
| Unsupported combination during mapping | Match the exchange, instrument type, market category, data type and scope against the historical source matrix. Structural validation leaves this check to the adapter. |

## Application HTTP/storage settings

Application settings govern the raw storage root, HTTP concurrency, retries and request pacing. Per-run `--requests-per-second` is capped by provider budgets. See [application settings](application.md) and [acquisition commands](../cli/acquisition.md).

## External import configuration

| Field | Required | Accepted values / default |
|---|---|---|
| `version` | No | Integer `1`; defaults to `1` |
| `source_key` | Yes | Nonempty source identifier |
| `source_kind` | No | `recording`, `purchased`, `other`; default `recording` |
| `root` | Yes | Nonempty source directory path |
| `files` | One selection method required | List of root-relative filenames |
| `patterns` | One selection method required | Root-relative file patterns |
| `max_files` | No | Default `1000`; accepted `1–10000` |
| `data_type` | Yes | `trades` or `order_book_l2` |
| `format_profile` | No | Optional profile reference |
| `[identity]` | Yes | Explicit execution venue and instrument identity |
| `[interval]` | No | Optional timezone-aware half-open interval |
| `[specifications]` | Conditional | Optional locally; required when the instrument is unknown to the catalogue during raw-only import |

At least one nonempty file or pattern selection is required.

Unknown fields are rejected.

The source root must resolve to a directory during import input selection.

The `format_profile` field does not independently establish profile validity or normalization readiness.

---

## Import Identity Fields

| Field | Required | Accepted values |
|---|---|---|
| `identity.exchange` | Yes | Lowercase registered execution-venue code |
| `identity.market_category` | Yes | `spot`, `linear`, `inverse`, `equity`, `option` |
| `identity.instrument_type` | Yes | `spot`, `perpetual`, `future`, `equity`, `option` |
| `identity.symbol` | Yes | Nonempty native symbol |

### Important rules

The market category and instrument type must be compatible.

The symbol cannot be:

```text
UNSPECIFIED
```

The exchange cannot be:

```text
external
```

The actual execution venue must be declared.

The provider or source origin is separate provenance.

---

## Import Specification Fields

The supported specification declaration can include:

```toml
[specifications]
as_of = 2026-09-01T00:00:00Z

base_asset = "BTC"
quote_asset = "USDT"
quantity_type = "base"

tick_size = "0.01"
qty_step = "0.000001"
```

The required fields depend on whether specifications are supplied and the selected quantity type.

### Required relationships

If specifications are supplied, both base and quote assets are required by the import configuration.

At least one supported observation timestamp or historical validity interval must establish the specification's time context.

If quantity type is `contracts`, an explicit positive contract value and its denomination are required.

Historical validity requires both start and end boundaries.

Exact decimal fields must use supported exact representations.

### Validation constraints

For example:

- Tick size must be positive when provided.
- Quantity step must be positive when provided.
- Minimum and maximum quantity values must be nonnegative when provided.
- Minimum quantity cannot exceed maximum quantity.
- Contract value must be a positive finite decimal string.
- Historical validity must use an ordered nonempty interval.

### What if a value is unknown?

Do not fabricate it.

For raw-only import, omit genuinely optional information where the schema permits it.

For full normalization readiness, obtain the required historical specifications from trustworthy source evidence.

---

## Selecting Files with Patterns

When a directory contains many files, writing every filename manually can be inconvenient.

MarketForge supports file patterns.

For example:

```toml
root = "./recordings"
patterns = ["*.csv"]
```

This selects matching CSV files in the root directory.

### Why use patterns?

Suppose your recording directory contains:

```text
recordings/
├── trades-2026-09-01.csv
├── trades-2026-09-02.csv
├── trades-2026-09-03.csv
└── notes.txt
```

A pattern such as:

```toml
patterns = ["*.csv"]
```

selects the matching CSV recordings.

It does not select `notes.txt`.

### Recursive patterns

MarketForge also supports recursive `**` path matching.

For example:

```toml
patterns = ["**/*.csv"]
```

This can select CSV files from nested directories.

### Why be careful with patterns?

A broad pattern may include files that do not share the same source format or instrument identity.

For example, a recording directory might contain:

```text
BTCUSDT/
ETHUSDT/
```

If your import recipe declares only BTCUSDT but selects files from both directories, the source identity would be incorrect for some inputs.

Use narrow patterns and inspect the resolved selection.

### Are absolute file entries supported?

No.

The entries inside `files` and `patterns` must be relative to the source root.

For example:

```toml
files = ["/home/user/recordings/trades.csv"]
```

is not accepted as a root-relative file selection.

Instead, declare the root separately:

```toml
root = "/home/user/recordings"
files = ["trades.csv"]
```

### Can file selections traverse outside the root?

No.

Parent-directory traversal is rejected.

For example:

```toml
files = ["../other-data/trades.csv"]
```

is not accepted.

This helps keep the selected input files within the declared source collection.

---

## Understanding Source Root Resolution

This is an important difference between external import and some other MarketForge configuration types.

### External import root

For external import recipes, a relative root such as:

```toml
root = "./recordings"
```

is resolved relative to the **current command working directory**.

For example, if you run MarketForge from the repository root:

```text
/home/user/projects/marketforge/
```

then:

```toml
root = "./recordings"
```

resolves to:

```text
/home/user/projects/marketforge/recordings/
```

### What if I invoke MarketForge elsewhere?

The same relative root can resolve to a different directory.

This is why the command's working directory matters for external import.

### How can I avoid ambiguity?

Use an absolute source root when you want the recipe to identify one fixed location independently of the current working directory.

For example:

```toml
root = "/mnt/market-data/recordings"
```

### What does `import new` do?

When you create an import recipe from an existing file or directory, MarketForge resolves the supplied source path.

The generated draft stores that resolved root.

This helps avoid ambiguity in the initial declaration.

### How is this different from registration recipes?

The full raw-data registration workflow has its own source-root resolution rules.

Those declarations can resolve repository-relative roots independently of the command invocation directory.

Do not assume that every MarketForge configuration type interprets relative paths identically.

---

## Import File Limits

An import pattern could potentially match thousands of files.

To avoid unexpectedly large selections, MarketForge limits file enumeration.

### Maximum selected files

The relevant field is:

```toml
max_files = 1000
```

The default is 1000.

The accepted range is:

```text
1–10000
```

### Why is there a limit?

Imagine accidentally selecting:

```text
*
```

inside a directory containing many unrelated files.

Without a limit, the import operation could attempt to preserve far more data than intended.

The file limit provides a safeguard.

### Pattern traversal limit

Pattern expansion also stops after inspecting more than 100,000 filesystem entries.

This prevents unbounded directory traversal.

### Does `max_files` limit file size?

No.

It limits the number of selected files.

A single large archive can still require substantial storage.

### Does the importer select files randomly?

No.

The selected file paths are resolved into a deterministic sorted order.

This helps make the import operation reproducible.

### What if no files match?

The import selection fails.

MarketForge does not treat an empty selection as a successful import of an unspecified dataset.

---

## Symlinks and Source Safety

External imports must preserve the correct source files without escaping the declared root.

MarketForge therefore checks source paths.

### Source files must stay inside the root

The resolved file paths must remain within the declared source directory.

### Selected symlink files

Symlinked source files are not accepted as managed-copy inputs.

### Directory traversal

The selection rejects paths that escape the source root.

Recursive pattern traversal does not follow symlinked directories.

### Why enforce these rules?

Without path checks, a broad pattern or unexpected symlink could cause MarketForge to import files outside the intended source collection.

The importer must know exactly which physical files belong to the declared input.

### Are source directories modified?

The import workflow preserves the original source files.

It copies or reuses the selected bytes in managed raw storage.

The source files remain separate from the managed copies.

### What about files changing during import?

MarketForge checks source-file stability.

It does not accept an input as stable merely because the filename remains the same.

The importer compares relevant filesystem information before and after hashing or copying.

If the file changes during the operation, import can fail.

This prevents a source file that is still being written from being accepted as one immutable recording.

---
