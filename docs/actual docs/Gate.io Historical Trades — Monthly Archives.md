**Observation:** Gate.io distributes historical trade data in monthly archives rather than individual daily files.

When MarketForge requests trades for a specific date range, the downloaded archive may contain trades for the entire month.

Example:

```text
BTC_USDT-202609.csv.gz
```

This archive contains trades from September 1 through September 30, 2026.

### Verified Archive Sizes

| Market | Instrument | Trades |
|---|---|---:|
| Spot | BTC_USDT | 3,554,747 |
| Linear perpetual | BTC_USDT | 20,458,327 |
| Inverse perpetual | BTC_USD | 215,494 |
| **Total** | | **24,228,568** |

All three archives were successfully decoded and normalized during full-archive integration testing.

### Processing Implications

The archive's time coverage may exceed the requested processing interval.

```text
Gate.io monthly archive
        ↓
Decode all records
        ↓
Normalize timestamps
        ↓
Filter requested time range
        ↓
Write matching trades
```

**Required behavior:** MarketForge must distinguish between archive coverage and the requested dataset interval.

For example, requesting September 1–4 should not produce a dataset containing trades from the entire month.

### Current Implementation

The Rust trade worker successfully processes complete monthly archives, but it does not yet apply processing time-range filtering.

Consequently, all records from the selected Gate.io archive are normalized, regardless of the requested interval.

### Decision

Preserve the existing monthly acquisition workflow.

Implement timestamp-based filtering during Rust processing using the requested processing interval.

Use normalized UTC nanosecond timestamps and half-open intervals:

```text
start_timestamp_ns <= event_timestamp_ns < end_timestamp_ns
```

This approach avoids changing the acquisition architecture while ensuring that output datasets contain only the requested time interval.

Monthly archives may still require reading records outside the requested interval. Archive-level optimizations can be considered later.