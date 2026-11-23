# Python dataset API

The public classes are exported from `forge.datasets`. Open a known filesystem artifact or discover a catalog of primary files. Select a logical member before reading a single stream from a combined file. See the [notebook guide](../workflows/notebooks.md) for installation, setup, and complete workflows.

## MarketData

```python
MarketData(*, root: str | Path | None = None, data_root: str | Path | None = None)
open(path: str | Path, *, allow_staging: bool = False) -> Dataset
discover(*, scope: str = "committed", include_staging: bool = False,
         registry_records: Iterable[Mapping[str, Any]] = (),
         registry_context: str | None = None) -> DatasetCatalog
datasets(*, include_tests: bool = False, include_staging: bool = False) -> pl.DataFrame
```

`root` and `data_root` use Forge path resolution. A relative `open` path resolves against the project root. Opening a publication directory groups its manifest-backed files under one handle. Open a primary Parquet path, or select a catalog entry, for file-scoped inspection. A missing path raises `FileNotFoundError`. A non-primary or unreadable manifest member raises `ValueError` when Forge can classify the failure. Staging paths require explicit opt-in.

`discover` supports `committed`, `tests`, and `all` and orders entries by path. Manifest validation, Parquet footers, and, where required, projected provenance columns establish file and stream metadata. Unreadable artifacts and unsupported versions appear in `catalog.diagnostics`. Bare Parquet can be discovered with missing-identity diagnostics. To include registry IDs, supply file summaries with distinct absolute `storage_path` values and a nonempty `registry_context`. Discovery does not connect to PostgreSQL. Invalid scope or registry arguments raise `ValueError`. `datasets` returns the corresponding discovery summary table.

```python
from forge.datasets import MarketData

catalog = MarketData().discover()
print(catalog.overview())
```

## DatasetCatalog

A catalog supports iteration, length, integer indexing, and slicing. An integer index yields a `Dataset`. Slicing retains catalog context. `one() -> Dataset` raises `ValueError` unless exactly one file matches. `records() -> list[dict[str, object]]` returns file metadata records. `summary() -> pl.DataFrame` returns a display table, and `overview() -> dict[str, Any]` includes aggregate metadata and diagnostics.

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
) -> DatasetCatalog
```

Filters compose with AND. Alternatives within one filter compose with OR. Market selectors must match the same membership inside a candidate file. File-size and row-count filters apply to complete primary files. Time filters use observed metadata envelopes with a half-open query interval. Unknown endpoints cannot establish a match. Neither interval mode establishes continuous coverage. A file can match several logical streams. Finding a file does not change which rows a `Dataset` reads. Registry publication, Engine publication and file dataset IDs have separate namespaces.

```python
files = catalog.find(exchange="bybit", symbol="BTCUSDT",
                     instrument_type="perpetual", data_type="trades")
```

## Dataset and Stream

```python
Dataset.info() -> dict[str, Any]
Dataset.streams() -> pl.DataFrame
Dataset.select(*, stream_id: str | None = None, exchange: str | None = None,
               symbol: str | None = None, kind: str | None = None,
               fingerprint: str | None = None, instrument_id: int | None = None,
               instrument_type: str | None = None,
               market_category: str | None = None) -> Stream
Dataset.trades(**filters: Any) -> Stream
Stream.info() -> dict[str, Any]
Stream.membership() -> dict[str, object]
```

`info` reports metadata, including observed extents and their evidence. `streams` lists verified memberships. `select` requires exactly one logical stream and raises `ValueError` for zero or competing matches. `trades` calls the trade-kind selector. Logical stream counters may be unavailable when evidence does not establish them. An observed envelope or row count does not imply source continuity.

```python
trade_stream = files[0].trades(exchange="bybit", symbol="BTCUSDT")
print(trade_stream.info())
```

## Record readers

The following methods are shared by `Dataset` and `Stream`.

```python
schema() -> pa.Schema
head(n: int = 10, *, columns: Sequence[str] | None = None,
     start: Moment | None = None, end: Moment | None = None,
     time_field: str = "event_timestamp_ns",
     input_rows: int | None = None) -> pl.DataFrame
batches(*, columns: Sequence[str] | None = None, batch_size: int = 65_536,
        start: Moment | None = None, end: Moment | None = None,
        time_field: str = "event_timestamp_ns",
        input_rows: int | None = None) -> Iterator[pa.RecordBatch]
scan(*, columns: Sequence[str] | None = None, start: Moment | None = None,
     end: Moment | None = None,
     time_field: str = "event_timestamp_ns") -> pl.LazyFrame
```

Use Arrow batches when preserving typed Decimal256 and nanosecond values. `head` represents Decimal256 as exact strings, including in nested data. `scan` requires an explicitly compatible projection when the schema includes unsupported Decimal256 columns. Time bounds are inclusive start and exclusive end, and accept Unix nanosecond integers, ISO strings and aware datetimes. Use integer or ISO nanosecond bounds for sub-microsecond precision. Projected columns must exist. Timestamp and column constraints raise `ValueError` when rejected by Forge. Parquet readers can also raise I/O or backend errors.

`n` limits matching preview rows delivered to the caller. `input_rows` limits the physical prefix inspected before row predicates. `batch_size` is an Arrow batch row limit. These limits do not cap byte I/O. Readers traverse physical order, including causal order where applicable. Lazy collection uses Polars and follows the requested query plan. A selected stream applies its membership predicate. A plain file handle reads all member streams.

```python
for batch in trade_stream.batches(columns=["event_timestamp_ns"], batch_size=512):
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
