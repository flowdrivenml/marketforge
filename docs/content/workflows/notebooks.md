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
catalog = market.discover(scope="committed")
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

Use `report.profile(["event_timestamp_ns"], input_rows=1000)` to summarize a bounded physical prefix and `compare(file_a, file_b)` to compare stored schemas, envelopes and sizes. These operations report their evidence and scope. See the [Python API reference](../reference/python-datasets.md).
