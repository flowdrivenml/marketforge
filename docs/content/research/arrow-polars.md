# Arrow and Polars

MarketForge preserves financial values in exact Arrow decimal types, including Decimal256. Nanosecond event and system times are stored as signed Unix nanosecond integers or supported Arrow nanosecond timestamps, depending on the field. Read the actual Arrow schema before choosing columns. [Time semantics](../concepts/time.md) describes the distinction between event and system time.

## Preserve exact values

`head()` returns a small Polars table for display. Decimal256 values appear as exact strings, including values nested inside a combined event payload. For typed decimals, iterate Arrow record batches. This example reads a bounded physical prefix and displays only batch metadata:

```python
trades = publication.query(data_type="trades")
for batch in trades.batches(batch_size=1024, input_rows=20_000):
    print(batch.schema)
    print(batch.num_rows)
    break
```

Use Arrow for arithmetic when the selected values require Decimal256. A display string is a representation of the stored value. Keep the Arrow decimal type when exporting exact amounts.

## Project a lazy Polars scan

Polars cannot represent Arrow Decimal256, including nested columns containing that type. Direct `Dataset.scan()`, `Stream.scan()`, and partition scans need an explicit compatible projection when unsupported fields are present. A `Dataset.query()` selection defaults to compatible fields in `scan()`. For predictable results, specify the columns you intend to use:

```python
lazy = trades.scan(
    columns=["event_timestamp_ns", "instrument_id", "stream_id"],
    start="2026-10-27T10:00:00Z",
    end="2026-10-27T11:00:00Z",
)
print(lazy.explain())
display(lazy.limit(10).collect())
```

A lazy query reads rows when you collect it. Projection reduces the columns needed for decoding. Predicates and a final row limit can reduce work, though Parquet pages or groups may have to be read before the first match. See [bounded reading](querying.md#exact-values-and-bounded-execution) and [record readers](python-api.md#record-readers).
