# Discover and open datasets

The public entry point is `MarketData` from `forge.datasets`. Filesystem discovery returns one handle per primary file. Registry search returns one record per publication or indexed conversion. A publication can appear on several timelines without any physical copies being made.

## Filesystem discovery

```python
from forge.datasets import MarketData

data = MarketData()
catalog = data.discover(scope="committed", metadata_only=True)
files = catalog.find(exchange="bybit", symbol="BTCUSDT")
files.summary()
```

Use `scope="tests"` for test fixture roots or `scope="all"` to include both. Opt in to staging with `include_staging=True`. `metadata_only=True` uses manifests and footers. The default additionally checks physical provenance for supported causal merges, which can read projected event columns to establish exact stream membership or counts. `summary()` uses the metadata already loaded on the handles.

A summary begins with an actionable reference and its domain. A file ID opens a registered primary file. A filesystem path opens that specific artifact through `data.open(path)`. A supplied output reference identifies an indexed conversion. Multiple known instruments are shown explicitly. Unknown fields stay empty. See the [Python API](python-api.md#datasetcatalog) for exact filters.

`find` uses exact symbol matching by default, with `symbol_match="contains"` for substring matching. It matches instrument filters against one membership. Results normally use observed start descending with a path tie-break. Explicit publication lookups retain physical path order unless you request a sort. Use `sort="time"` or `sort="path"` and `order="asc"` or `order="desc"` to choose explicitly.

## Read-only registry discovery

In a notebook, choose the environment explicitly and open a returned reference.

```python
page = await data.find_registered(
    testing=True, operation="merge", data_type="combined",
    exchange="bybit", instrument="BTCUSDT", limit=10
)
publications = page["publications"]
if publications:
    merged = await data.open_registered(
        testing=True, publication_id=publications[0]["publication_id"]
    )
    print(merged.discovery_fields())
```

`find_registered` returns the same structured publication page as the [Datasets CLI](../reference/cli/datasets.md), with `operation`, `exchange`, `instrument`, `instrument_type`, `market_category`, `data_type`, `start`, `end`, `status`, `target_quote`, `search`, `sort`, `order`, `limit`, and `offset` available as filters. Pass integer nanoseconds for registry time bounds. Use exactly one of `publication_id`, `file_id`, or `output_ref` to open a result. Use `asyncio.run` to execute the coroutine from a synchronous script.

Normalized and merged publications use numeric registry publication IDs. Converted outputs use a complete durable work reference. Registry IDs are scoped to the selected database. Engine IDs and filesystem paths have their own domains. See [identity](../concepts/identity.md#inspection-identifiers) and [coverage](../concepts/time.md).
