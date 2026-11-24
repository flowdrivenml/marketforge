# Query combined market data

Use the [discovery workflow](discovery.md) to open a merged or converted **combined** publication. Inspect its stream identities and available physical locators before selecting events.

```python
print(merged.streams())
print(merged.schema())
part = merged.partitions()[0]
trades = merged.query(data_type="trades", partition=part.reference)
depth = merged.query(data_type="depth", partition=part.reference)
print(trades.head(20))
print(depth.head(5))
```

Choose an authoritative numeric `instrument_id` from `streams()`, or an exact `stream_id`. They can be combined with the event-kind selector. Specify a projection to reduce decoding and memory use.

```python
instrument_id = merged.streams()["instrument_id"][0]
query = merged.query(data_type="trades", instrument_id=instrument_id)
columns = ["event_timestamp_ns", "instrument_id", "stream_id"]
start = "2026-10-27T10:00:00.000000001Z"
end = "2026-10-27T11:00:00Z"
lazy = query.scan(columns=columns, start=start, end=end)
print(lazy.explain())
print(lazy.limit(20).collect())
print(query.explain(columns=columns, start=start, end=end))
print(merged.lineage())
```

Use bounds appropriate for your selected data. The event-time query interval is half-open in exact Unix nanoseconds. Physical bounds come from footer statistics. They say nothing about complete time coverage. A filtered causal timeline preserves the matching events' physical order. Event timestamps can regress, so sort explicitly if your analysis needs timestamp order. For book reconstruction, retain the preceding state and reconstruction rows described by the publication. A narrow depth preview alone does not establish a starting book.

## Exact values and bounded execution

- `head(n)` stops after up to `n` matches and represents Decimal256 values as exact strings. The default kind-specific Arrow projection drops the opposite nested payload, even when both kinds appear in every row group.
- `batches(columns=..., batch_size=...)` streams exact Arrow values in source order. Use it for Decimal256 arithmetic or export. Iterate batches individually to bound retained memory.
- `scan(columns=...)` returns a Polars LazyFrame. A query handle defaults to Polars-compatible fields. Use Arrow batches for Decimal256. Use `limit(...).collect()` for a preview and collect a full query only after reviewing the projection and expected result size.
- `explain` lists requested predicates, projection, and footer-eligible row groups. Those are candidates, not measured reads. Polars optimizes its own plans, visible through `LazyFrame.explain()`.

Predicate statistics can eliminate groups when the bounds prove no match. In a combined causal file, every group can contain both trades and depth. In that case event kind selects records inside each group. Reading only the selected kind's payload and stopping early can still reduce decoding and memory sharply. Explicit partition selection also reduces the file scope. MarketForge uses existing Parquet metadata for this path and creates no duplicate cache or secondary publication.

See the [market-data ordering contract](../concepts/ordering.md) and [schema references](../reference/data-formats/publications.md).

## Measured example

A read-only test on a registered Bybit BTCUSDT causal merge used one 614 MiB Parquet file with 1,937,808 physical rows, 1,074,411 trades, and 65 row groups. The benchmark ran each reader in a fresh process, alternated run order, and reports two-run medians on a mostly warm filesystem cache. Peak RSS covers the entire Python process. The full scan streamed and counted batches rather than retaining the entire result.

| Query | Earlier reader | Selective reader | Earlier RSS | Selective RSS |
| --- | ---: | ---: | ---: | ---: |
| First 20 trades | 9.57 s | 83 ms | 1,391 MiB | 122 MiB |
| First 20 depth records | 10.40 s | 85 ms | 1,419 MiB | 140 MiB |
| Trade count from the discriminator alone | 49 ms | 49 ms | 105 MiB | 109 MiB |
| Stream all trade results | 4.52 s | 276 ms | 1,465 MiB | 357 MiB |
| Trades in a one-hour interval | 327 ms | 63 ms | 556 MiB | 182 MiB |

The earlier payload reader materialized all stored columns. The selective reader projected event time, instrument, stream, discriminator, and the selected kind's payload. Both returned the same row counts. The 1-hour query returned 74,940 trades from 5 footer-eligible groups. All 65 groups were eligible for a trade-kind predicate alone. The trade count already projected one column and remained essentially unchanged.

The repository's `research/benchmarks/selective_parquet.py` runner takes an explicit combined publication directory and a trusted copy of the previous `parquet.py` supplied via `--baseline`. It prints runtimes, peak RSS, counts, and Linux storage-read counters where available. Most measured reads were served from cache, so these timings do not establish cold-storage throughput.
