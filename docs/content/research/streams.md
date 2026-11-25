# Streams and event selection

Open a merged publication using the [discovery guide](discovery.md). Inspect `streams()` for its exchange, instrument, kind, and stream IDs. The same Parquet file can hold several logical streams. File paths and partitions select storage. Stream and event-kind selectors determine which rows are returned.

```python
display(publication.streams())
trades = publication.query(data_type="trades")
depth = publication.query(data_type="depth")
display(trades.head(5, input_rows=100_000))
display(depth.head(5, input_rows=100_000))
```

For a particular stream, copy its ID from the stream table:

```python
stream_id = input("Exact stream ID: ").strip()
stream = publication.select(stream_id=stream_id)
display(stream.membership())
display(stream.head(5, columns=["event_timestamp_ns"], input_rows=100_000))
```

`publication.trades(exchange="bybit", symbol="BTCUSDT")` and `publication.depth(exchange="bybit", symbol="BTCUSDT")` select one logical stream each. They raise `ValueError` if the filters select zero or several streams. Add the exact `stream_id` from the table in that case. `query()` can select many rows across streams by an event kind, a numeric historical instrument ID from the same table, a stream ID, or a combination of predicates.

```python
instrument_id = int(input("Instrument ID from the stream table: "))
query = publication.query(data_type="trades", instrument_id=instrument_id)
display(query.head(
    10, columns=["event_timestamp_ns", "instrument_id", "stream_id"],
    start="2026-10-27T10:00:00Z", end="2026-10-27T11:00:00Z",
    input_rows=500_000,
))
```

Use time bounds appropriate to your publication. A physical prefix cap may yield an empty preview if the requested hour starts later in the files. Use [physical partitions](partitions.md) and [query inspection](querying.md) to narrow the input when the event range is sparse.
