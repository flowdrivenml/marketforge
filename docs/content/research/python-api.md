# Python dataset API

The public classes are exported from `forge.datasets`. Open a known filesystem artifact or discover a catalog of primary files. Select a logical member before reading a single stream from a combined file. Start with [Python setup](getting-started.md) and the [notebooks](notebooks.md) for complete workflows. The partition and query handles below are returned by these interfaces.

## MarketData

```python
MarketData(*, root: str | Path | None = None, data_root: str | Path | None = None)
open(path: str | Path, *, allow_staging: bool = False,
     metadata_only: bool = False, strict: bool = False) -> Dataset
discover(*, scope: str = "committed", include_staging: bool = False,
         registry_records: Iterable[Mapping[str, Any]] = (),
         registry_context: str | None = None, metadata_only: bool = False) -> DatasetCatalog
datasets(*, include_tests: bool = False, include_staging: bool = False) -> pl.DataFrame
```

`root` and `data_root` use Forge path resolution. A relative `open` path resolves against the project root. Opening a publication directory groups its manifest-backed files under one handle. Open a primary Parquet path, or select a catalog entry, for file-scoped inspection. A missing path raises `FileNotFoundError`. A non-primary or unreadable manifest member raises `ValueError` when Forge can classify the failure. Staging paths require explicit opt-in.

`discover` supports `committed`, `tests`, and `all` and orders entries by path. Manifest validation, Parquet footers, and, where required, projected provenance columns establish file and stream metadata. Unreadable artifacts and unsupported versions appear in `catalog.diagnostics`. Bare Parquet can be discovered with missing-identity diagnostics. To include registry IDs, supply file summaries with distinct absolute `storage_path` values and a nonempty `registry_context`. Discovery does not connect to PostgreSQL. Invalid scope or registry arguments raise `ValueError`. `datasets` returns the corresponding discovery summary table.

```python
from forge.datasets import MarketData

catalog = MarketData().discover()
print(catalog.overview())
```

### Registry access

```python
await market.find_registered(*, testing=False, **filters) -> dict[str, Any]
await market.open_registered(*, publication_id: int | None = None,
                             file_id: int | None = None, output_ref: str | None = None,
                             testing: bool = False) -> Dataset
```

These asynchronous methods require a configured PostgreSQL registry. The default selects production. Browse returns a metadata page containing `publications`, `offset`, `limit`, and `has_more`. Available filters include `operation`, `exchange`, `instrument` (native symbol), `instrument_type`, `market_category`, `data_type`, `start`, `end`, `status`, `target_quote`, and `search`. Registry time bounds are integer Unix nanoseconds. Use `sort` and `order` as described in the [CLI reference](../reference/cli/datasets.md). The page defaults to 50 entries with offset zero.

Opening requires exactly one identity. A publication or indexed conversion can resolve several physical files. A primary-file ID selects one file. An invalid identity combination, unavailable record, missing file, or inconsistent evidence can raise a Forge error. A connection or credential failure also prevents browsing or opening. Use the environment that produced the reference. For a complete example, see [registry discovery](discovery.md#browse-a-registry).

## DatasetCatalog

A catalog supports iteration, length, integer indexing, and slicing. An integer index yields a `Dataset`. Slicing retains catalog context. `first() -> Dataset` returns the first entry or raises `ValueError` on an empty catalog. `get(reference: int | str | Path) -> Dataset` matches a registry primary-file ID, exact file path, or supplied output reference and requires a unique match. `one() -> Dataset` raises `ValueError` unless exactly one file matches. `records() -> list[dict[str, object]]` returns file metadata records. `summary() -> pl.DataFrame` returns a display table, and `overview() -> dict[str, Any]` includes aggregate metadata and diagnostics.

```python
Text = str | Sequence[str]
IDs = int | Sequence[int]
Moment = int | str | datetime
Paths = str | Path | Sequence[str | Path]

find(
    *,
    exchange: Text | None = None, symbol: Text | None = None,
    symbol_match: Literal["exact", "contains"] = "exact",
    instrument_id: IDs | None = None, instrument_namespace: Text | None = None,
    instrument_type: Text | None = None, market_category: Text | None = None,
    data_type: Text | None = None, stream_id: Text | None = None,
    fingerprint: Text | None = None, operation: Text | None = None,
    publication_id: IDs | None = None, publication_path: Paths | None = None,
    engine_publication_id: int | Text | None = None, dataset_id: IDs | None = None,
    path: Paths | None = None, min_rows: int | None = None, max_rows: int | None = None,
    min_bytes: int | None = None, max_bytes: int | None = None,
    start: Moment | None = None, end: Moment | None = None,
    interval: Literal["overlap", "envelope_contained"] = "overlap",
    status: Text | None = None, integrity: Text | None = None,
    output_ref: Text | None = None, target_quote: Text | None = None,
    sort: Literal["time", "path"] | None = None,
    order: Literal["asc", "desc"] | None = None,
) -> DatasetCatalog
```

Filters compose with AND. Alternatives within one filter compose with OR. Market selectors must match the same membership inside a candidate file. File-size and row-count filters apply to complete primary files. Time filters use observed metadata envelopes with a half-open query interval. Unknown endpoints cannot establish a match. Neither interval mode establishes continuous coverage. A file can match several logical streams. Finding a file does not change which rows a `Dataset` reads. Registry publication, Engine publication and file dataset IDs have separate namespaces.

```python
files = catalog.find(exchange="bybit", symbol="BTCUSDT",
                     instrument_type="perpetual", data_type="trades")
```

## Dataset and Stream

```python
Dataset.files -> tuple[Path, ...]
Dataset.info() -> dict[str, Any]
Dataset.discovery_fields() -> dict[str, Any]
Dataset.lineage() -> dict[str, Any]
Dataset.streams() -> pl.DataFrame
Dataset.select(*, stream_id: str | None = None, exchange: str | None = None,
               symbol: str | None = None, kind: str | None = None,
               fingerprint: str | None = None, instrument_id: int | None = None,
               instrument_type: str | None = None,
               market_category: str | None = None) -> Stream
Dataset.trades(**filters: Any) -> Stream
Dataset.depth(**filters: Any) -> Stream
Stream.info() -> dict[str, Any]
Stream.membership() -> dict[str, object]
```

`info` reports metadata, including observed extents and their evidence. `streams` lists verified memberships. `select` requires exactly one logical stream and raises `ValueError` for zero or competing matches. `trades` calls the trade-kind selector. `depth` calls the depth-kind selector. `files` lists paths in physical order. `discovery_fields` provides the compact identity-first representation used in the catalog. `lineage` reads recorded manifest provenance and raises `ValueError` if an expected manifest hash changed. It reports unavailable lineage for a standalone file. `Stream.membership()` exposes identity and selection evidence. Logical stream counters may be unavailable when evidence does not establish them. An observed envelope or row count does not imply source continuity.

```python
trade_stream = publication.select(stream_id=stream_id)
print(trade_stream.info())
```

## Record readers

The following methods are shared by `Dataset`, `Stream`, physical partitions, and query selections.

```python
schema() -> pa.Schema
head(n: int = 10, *, columns: Sequence[str] | None = None,
     start: Moment | None = None, end: Moment | None = None,
     time_field: str = "event_timestamp_ns",
     input_rows: int | None = None) -> pl.DataFrame
batches(*, columns: Sequence[str] | None = None, batch_size: int = 65_536,
        start: Moment | None = None, end: Moment | None = None,
        time_field: str = "event_timestamp_ns",
        input_rows: int | None = None, bounded: bool = False) -> Iterator[pa.RecordBatch]
scan(*, columns: Sequence[str] | None = None, start: Moment | None = None,
     end: Moment | None = None,
     time_field: str = "event_timestamp_ns") -> pl.LazyFrame
```

Use Arrow batches when preserving typed Decimal256 and nanosecond values. `head` represents Decimal256 as exact strings, including in nested data. `scan` requires an explicitly compatible projection when the schema includes unsupported Decimal256 columns. Time bounds are inclusive start and exclusive end, and accept Unix nanosecond integers, ISO strings and aware datetimes. Use integer or ISO nanosecond bounds for sub-microsecond precision. Projected columns must exist. Timestamp and column constraints raise `ValueError` when rejected by Forge. Parquet readers can also raise I/O or backend errors.

`n` limits matching preview rows delivered to the caller. `input_rows` limits the physical prefix inspected before row predicates. `batch_size` is an Arrow batch row limit. These limits do not cap byte I/O. Readers traverse physical order, including causal order where applicable. `bounded=True` selects sequential batch iteration for filtered Arrow reads. To cap the pre-filter row prefix, also set `input_rows`. Explicit row-group selections support `head` and `batches`. Calling `scan` on them raises `ValueError`. Lazy collection uses Polars and follows the requested query plan. A selected stream applies its membership predicate. A plain file handle reads all member streams.

```python
for batch in trade_stream.batches(columns=["event_timestamp_ns"], batch_size=512, input_rows=10_000):
    print(batch.schema)
    break
```

## Inspection helpers

```python
from forge.lab.inspection import compare, inspect
# inspect(target: Dataset | Stream | str | Path) -> Inspection
# compare(*targets: Dataset | Stream) -> dict[str, object]
```

`Inspection.schema()` returns the Arrow schema. `columns()`, `row_groups()`, and `timestamps()` return lists of metadata records. `summary() -> dict[str, object]` combines inspection evidence. `profile(columns: Sequence[str], *, input_rows: int = 1000) -> dict[str, object]` reads a physical prefix and reports sampled values. Sampling limits do not establish properties of unread rows. `compare` compares artifact metadata, including schema and time evidence, without a row-by-row diff.

```python
view = inspect(trade_stream)
print(view.timestamps())
```

## PhysicalPartition

```python
Dataset.partitions() -> tuple[PhysicalPartition, ...]
Dataset.partition(reference: str) -> PhysicalPartition
PhysicalPartition.reference: str
PhysicalPartition.info() -> dict[str, object]
PhysicalPartition.extent() -> tuple[int | None, int | None]
PhysicalPartition.row_groups() -> list[dict[str, object]]
PhysicalPartition.row_group(index: int, *, file: int = 1) -> Selection
PhysicalPartition.select_row_groups(indices: Sequence[int], *, file: int = 1) -> Selection
```

Use a locator returned by the current handle. File positions start at one within a partition. Row-group indices start at zero. An unknown partition, invalid file or group, or empty group selection raises `ValueError`. Counts, sizes, and any observed timestamp bounds are footer metadata. `extent()` can return unknown bounds.

```python
part = publication.partition(partition_reference)
display(part.row_group(0).head(5, columns=["event_timestamp_ns"]))
```

See [partitions and row groups](partitions.md) for locator and scope examples.

## Query selections

```python
Dataset.query(*, data_type: str | None = None, instrument_id: int | None = None,
              stream_id: str | None = None,
              partition: str | Sequence[str] | None = None) -> Selection
Selection.explain(*, start: Moment | None = None, end: Moment | None = None,
                  columns: Sequence[str] | None = None) -> dict[str, object]
```

A query retains physical order and can filter multiple streams. Use `trades` or `depth` for an event-kind predicate. Use a positive signed-64-bit historical instrument ID or exact stream ID from the stream table. A partition list retains manifest file order. Invalid locators, unsupported event-kind selection, or missing predicate columns raise `ValueError`.

A kind-specific Arrow read omits the other kind's nested payload by default. A lazy query defaults to compatible columns. `explain()` shows predicates, projection, and candidate row groups using event time for its bounds. It does not execute a read or measure I/O. See [querying market data](querying.md) and [precision and projections](arrow-polars.md).

```python
query = publication.query(data_type="depth", partition=partition_reference)
display(query.explain(columns=["event_timestamp_ns"]))
```
