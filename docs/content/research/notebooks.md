# Explore datasets in Jupyter

The research API reads local Parquet manifests and footers. It can discover files without PostgreSQL, inspect their stored metadata, and read explicitly selected rows.

From the repository root, install the research group and launch JupyterLab.

```bash
uv sync --project forge --group lab
uv run --project forge --group lab --with jupyterlab jupyter lab
```

Open the [example notebook](../examples/notebooks/dataset_inspection.ipynb) with its adjacent [fixture builder](../examples/notebooks/dataset_fixtures.py). The notebook creates tiny synthetic normalized and combined files in a temporary directory. It needs no database, credentials, network access, or processing engine once the environment is installed.

## Discover files and narrow the catalog

```python
from forge.datasets import MarketData
from forge.lab.inspection import inspect

market = MarketData()
catalog = market.discover(scope="committed", metadata_only=True)
display(catalog)

matches = catalog.find(
    exchange=["bybit", "binance", "okx"],
    instrument_type="perpetual",
    market_category="linear",
    symbol="BTCUSDT",
    data_type=["trades", "depth"],
    start="2026-10-27T00:00:00Z",
    end="2026-10-30T00:00:00Z",
)
display(matches)
matches.summary()
```

Set `MarketData(root=..., data_root=...)` when working outside the repository or with a separate data directory. With an omitted root, Forge uses `MARKETFORGE_PROJECT_ROOT` or searches upward from the working directory. Discover the test trees with `scope="tests"`, or include them alongside production output with `scope="all"`. Add `include_staging=True` to see recognized staging directories as uncommitted. Standalone Parquet encountered in a search tree appears as unverified with unknown market identity. Discover again to refresh the filesystem metadata.

Catalog search returns a collection for every result size. Use iteration or indexing for several matches, `if not matches` for an empty result, and `matches.one()` when exactly one file is expected. Symbol matching is exact and case-sensitive. Use `catalog.find(symbol="BTC", symbol_match="contains")` to search for a substring. Filters on venue, symbol, instrument type, and category apply to the same concrete stream within each file.

Catalog diagnostics preserve affected paths and reasons while successful files remain available. Inspect `catalog.diagnostics` for unreadable files, malformed manifests, unknown versions, conflicts, or incomplete identity.

## Inspect one file or concrete stream

```python
if matches:
    dataset = matches[0]
    display(dataset)
    display(dataset.streams())

    report = inspect(dataset)
    display(report)
    print(report.schema())
    display(report.columns(), report.row_groups(), report.timestamps())
```

Choose a stream in a mixed file using `dataset.select(exchange="okx", kind="depth")` when it matches exactly one available membership. If the file contains Binance ETH and OKX BTC, one file appears in the catalog. A query for OKX ETH has no match. A matching view keeps all known file memberships as context and highlights the matching memberships. File totals always describe the physical file. Stream selections use actual row predicates, including rank and the combined event-kind discriminator when present.

Timestamps in the catalog are observed envelopes. The maximum is inclusive. Search uses overlap with `[start, end)`; the envelope can overlap when no row in the range exists, and says nothing about continuity. Use `interval="envelope_contained"` to require the entire known file envelope inside the query range. Inspection separates recorded requested bounds, observed file or stream bounds, and physical footer statistics. Footer statistics for a selected stream may include other streams in the same file.

## Read records explicitly

```python
if matches:
    dataset = matches[0]
    display(dataset.head(5, columns=["event_timestamp_ns", "symbol"], input_rows=10_000))
    for batch in dataset.batches(columns=["event_timestamp_ns", "symbol"], input_rows=1000):
        print(batch.num_rows)
    query = dataset.scan(columns=["event_timestamp_ns", "symbol"], start="2026-10-27T00:00:00Z")
    display(query.limit(5).collect())
```

These methods also work on a selected stream. Set `time_field="system_timestamp_ns"` explicitly to filter on the stored system time instead of event time. Its meaning depends on the source profile. A preview preserves physical order and may read more than its returned row limit, especially when filtering sparse streams. `input_rows` limits the physical prefix evaluated before predicates. Parquet readers may still decode whole pages or dictionaries.

Arrow batches retain exact Decimal256 prices and quantities. Previews represent Decimal256 as exact strings, including nested combined payloads. A Polars lazy query requires a supported projection. Exclude Decimal256 fields or use Arrow batches for those fields. Inspection applies no scaling or contract multipliers.

Use `report.profile(["event_timestamp_ns"], input_rows=1000)` to summarize a bounded physical prefix and `compare(file_a, file_b)` to compare stored schemas, envelopes and sizes. These operations report their evidence and scope. See the [Python API reference](python-api.md).

## Inspect causal streams and bounded reads

### AND and OR Semantics

This is one of the most important properties of the search API.

### OR within one filter

For example:

```python
exchange=["bybit", "okx"]
```

means:

```text
Bybit OR OKX
```

### AND between filters

For example:

```python
catalog.find(
    exchange="bybit",
    symbol="BTCUSDT",
    data_type="trades",
)
```

means:

```text
Exchange is Bybit
AND
Symbol is BTCUSDT
AND
Content is trades
```

### Why does membership consistency matter?

Suppose one merged file contains:

```text
Bybit BTCUSDT Trades
OKX ETHUSDT Trades
```

Now consider:

```python
catalog.find(
    exchange="bybit",
    symbol="ETHUSDT",
)
```

The file should not match merely because it contains:

```text
Bybit
```

somewhere and:

```text
ETHUSDT
```

somewhere else.

The conditions must identify the same established stream membership.

### Why is this important?

Otherwise, a combined file could produce false matches for markets it does not actually represent.

MarketForge preserves this relationship.

---

### Composable Searches

A search returns another `DatasetCatalog`.

This means you can apply additional filters later.

For example:

```python
bybit = catalog.find(
    exchange="bybit",
)

perpetuals = bybit.find(
    instrument_type="perpetual",
)

trades = perpetuals.find(
    data_type="trades",
)

display(trades.summary())
```

### Why is this useful?

You can build reusable research selections.

For example:

```python
btc = catalog.find(
    symbol="BTCUSDT",
)

btc_trades = btc.find(
    data_type="trades",
)
```

### Does each search start from every original file?

No.

The next search operates on the current matching collection.

### Are previous membership filters retained?

Yes.

Composable searches retain matching memberships.

This prevents later filters from unexpectedly switching to unrelated streams in the same physical file.

### Does filtering modify the underlying datasets?

No.

The searches return new metadata collections.

They do not alter Parquet files.

---

## Publication and File Filters

### Search by Processing Operation

The API supports an operation filter.

For example:

```python
normalized = catalog.find(
    operation="normalize",
)
```

Or:

```python
merged = catalog.find(
    operation="merge",
)
```

### Why distinguish operations?

Normalization and merging produce different physical publication formats.

For example:

**Normalization**

```text
Raw Archives
    |
    v
Canonical Trades / Depth
```

**Merge**

```text
Normalized Streams
    |
    v
Combined Timeline
```

The appropriate inspection strategy can depend on the selected operation.

---
