# Python dataset API

Install the `lab` dependency group for PyArrow and Polars. IPython/Jupyter can display the rich representations. The objects also work as ordinary Python objects, without widgets or a visualization server.

## Discovery and selection

| API | Returns |
| --- | --- |
| `MarketData(root=None, data_root=None)` | A local project and data-root accessor. |
| `market.discover(scope="committed", include_staging=False, registry_records=(), registry_context=None)` | `DatasetCatalog`, one entry per discovered physical primary file, plus `diagnostics`. Scopes are `committed`, `tests`, and `all`. |
| `market.open(path, allow_staging=False)` | `Dataset` for a file. Opening a publication directory returns an explicitly labeled grouping and carries no file dataset ID. Opening staging requires opt-in. |
| `market.datasets(include_tests=False, include_staging=False)` | Compatibility table equivalent to discovery followed by `summary()`. The table now has one row per file. |
| `catalog.find(...)` | A new collection with zero or more files. Searches compose and retain matching memberships. |
| `catalog.one()` | One `Dataset`. Raises `ValueError` for any other cardinality. |
| `catalog.summary()`, `catalog.records()`, `catalog.overview()` | A Polars table, Python record list, and aggregate dictionary. These describe physical file counts, sizes and rows. |
| `dataset.streams()` | Polars table of established memberships. Its row counts describe exact matched rows only when known. |
| `dataset.select(stream_id=None, exchange=None, symbol=None, kind=None, fingerprint=None, instrument_id=None, instrument_type=None, market_category=None)` | One `Stream` among the current matching memberships, if a catalog filter selected any. Raises `ValueError` for zero or multiple matches. `dataset.trades(**filters)` selects the trade kind. |

`find` takes keyword filters:

- `exchange`, `symbol`, `instrument_id`, `instrument_namespace`, `instrument_type`, `market_category`, `stream_id`, `fingerprint`, and `data_type`.
- `operation`, `publication_id`, `publication_path`, `engine_publication_id`, `dataset_id`, `path`, `status`, and `integrity`.
- `min_rows`, `max_rows`, `min_bytes`, and `max_bytes` use inclusive bounds on physical totals.
- `start`, `end`, and `interval="overlap"` compare the half-open query to inclusive observed file envelopes. `interval="envelope_contained"` checks full envelope containment. Bounds are signed integer Unix nanoseconds, timezone-aware Python datetimes, or ISO timestamp strings with an offset and up to nine fractional digits. Python datetime inputs carry microsecond precision.
- `symbol_match="exact"` is case-sensitive. `symbol_match="contains"` enables a case-sensitive substring search.

Use a sequence for OR within a filter. Different filters combine with AND. Market filters apply to a single membership. Type aliases are `trade` / `trades`, `l2` / `depth` / `order_book_l2`, and `trade_l2` / `combined`. Searching `trades` or `depth` can match a membership in a combined file. Search never applies row-level time predicates.

A file dataset ID and a publication ID identify records in one PostgreSQL registry. The engine publication ID identifies the committed output in its publication context. A stream identity or rank identifies market content within that output. Filesystem discovery leaves database IDs unknown. To attach existing read-only registry evidence, pass the list from the `datasets` key of `mf --json datasets list --testing` as `registry_records`, along with a descriptive `registry_context`. Records are joined to local primary paths and checked against the manifest. This Python method makes no database calls. Large engine IDs are shown as strings in the Polars summary to preserve their full value.

Filesystem-only membership comes from normalized manifests and footer identities, or from merged manifests and file-specific footer rank evidence. Where bounds prove only some ranks, discovery returns those memberships and records a diagnostic. Rank ranges alone cannot prove intervening ranks. Missing parent instrument metadata leaves market category or instrument type unknown. Standalone files support physical inspection and explicit reads with unknown market identity. Canonical namespace filtering uses stream identity evidence when available. A merged publication can carry a different registry namespace from those of its input instruments. Recorded integrity retains its original scope.

## Inspection and row access

| API | Evidence and return value |
| --- | --- |
| `inspect(dataset_or_stream_or_path)` | `Inspection`, using cached physical footers. `schema()` returns an Arrow schema, or raises on incompatible schemas. |
| `report.summary()`, `report.columns()`, `report.row_groups()`, `report.timestamps()` | Python dictionary and lists of records. Column stats are Parquet leaf values. Stored byte-array bounds can be outward-truncated. Nested/repeated leaves can have different counts from physical rows. Absent stats are unknown. |
| `report.profile(columns, input_rows=1000)` | Explicit physical-prefix profile with input and matching row counts, nulls, and supported exact min/max. The prefix is not a random sample. Unsupported exact calculations report a limitation. |
| `compare(*datasets_or_streams)` | Metadata summaries and pairwise Arrow schema compatibility. |
| `handle.head(n=10, *, columns=None, start=None, end=None, time_field="event_timestamp_ns", input_rows=None)` | Bounded Polars preview in physical order. Decimal256 displays as exact text. |
| `handle.batches(*, columns=None, batch_size=65536, start=None, end=None, time_field="event_timestamp_ns", input_rows=None)` | Iterator of exact Arrow record batches. |
| `handle.scan(*, columns=None, start=None, end=None, time_field="event_timestamp_ns")` | Lazy Polars query. Unsupported Decimal256 projections raise `ValueError`. |

Here `handle` is a file `Dataset` or a selected `Stream`. Row predicates apply to a named nanosecond timestamp or int64 Unix-nanosecond column. The interval is half-open. Unconstrained batches preserve the stored payload and combined discriminators. Metadata, rich display and lazy-query construction do not read event rows. Explicit previews, profiling, batch iteration and lazy materialization do read records. A return-row or prefix limit does not bound actual Parquet bytes decoded.

[Notebook workflow](../workflows/notebooks.md)
