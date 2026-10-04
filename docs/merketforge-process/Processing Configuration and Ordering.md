Processing-specific decisions belong to the processing job rather than permanent catalog metadata.

## Quick Navigation

- [Ordering Policy](#ordering-policy)
- [Stream Rank](#stream-rank)
- [ProcessingJob](#processingjob)
- [Configuration Flow](#configuration-flow)
- [Manifest Reproducibility](#manifest-reproducibility)
- [Responsibility Boundary](#responsibility-boundary)

## Ordering Policy

Ordering is separated into two domains.

### Within a Stream

Preserve authoritative source order:

```text
native sequence when meaningful
    ↓
otherwise original source-event order
```

Sequence continuity, duplicates, and timestamp regressions are validated according to the source format.

Stateful depth is never reordered merely to make timestamps appear monotonic.

### Across Streams

Globally merge using:

```text
event_timestamp_ns
    ↓
stable stream_rank
```

`stream_rank` resolves otherwise indistinguishable equal-timestamp events.

It provides deterministic ordering only and does not imply real-world causality between independent exchanges.

## Stream Rank

`stream_rank` is a **processing-job property**, not permanent instrument metadata.

It should therefore not be stored as an intrinsic property in PostgreSQL.

Ranks are generated deterministically by the planner from stable stream properties such as:

```text
exchange
instrument_id
data type
stream variant
```

Example:

```text
0 → Binance BTCUSDT trades
1 → Binance BTCUSDT depth
2 → Bybit BTCUSDT trades
3 → Bybit BTCUSDT depth
```

The same effective job configuration must produce the same ranks.

Never derive ordering from:

```text
worker ID
worker completion order
thread scheduling
temporary-file creation order
```

## ProcessingJob

Python constructs a complete `ProcessingJob` before starting Rust.

Conceptually:

```json
{
  "dataset_id": 42,
  "ordering": {
    "primary": "event_timestamp_ns",
    "tie_break": "stream_rank"
  },
  "streams": [
    {
      "stream_id": "binance:BTCUSDT:trades",
      "stream_rank": 0
    },
    {
      "stream_id": "binance:BTCUSDT:depth",
      "stream_rank": 1
    },
    {
      "stream_id": "bybit:BTCUSDT:trades",
      "stream_rank": 2
    },
    {
      "stream_id": "bybit:BTCUSDT:depth",
      "stream_rank": 3
    }
  ]
}
```

The full job may also contain:

```text
input files
raw formats
instrument specs
normalization configuration

workers
memory budget
scratch configuration

Arrow batch target
Parquet row-group target
Parquet file target

transformations
monitoring configuration
```

Rust receives a frozen execution specification and does not need to query PostgreSQL.

## Configuration Flow

Users should not normally configure internal details such as `stream_rank` manually.

```text
CLI / user request
        +
PostgreSQL catalog metadata
        +
MarketForge defaults
        ↓
Python planner
        ↓
resolve inputs
        ↓
derive streams and ranks
        ↓
derive processing settings
        ↓
ProcessingJob
        ↓
JSON
        ↓
marketforge-process
```

For example:

```bash
marketforge process \
    --dataset 42 \
    --workers 12 \
    --memory 8G
```

Python expands this into the complete internal processing configuration.

## Manifest Reproducibility

The effective ordering configuration is preserved in the finished dataset manifest.

Example:

```json
{
  "ordering": {
    "primary": "event_timestamp_ns",
    "tie_break": "stream_rank",
    "streams": [
      {
        "stream_id": "binance:BTCUSDT:trades",
        "rank": 0
      },
      {
        "stream_id": "binance:BTCUSDT:depth",
        "rank": 1
      },
      {
        "stream_id": "bybit:BTCUSDT:trades",
        "rank": 2
      }
    ]
  }
}
```

This makes equal-timestamp ordering reproducible and explainable without access to the original processing process.

## Responsibility Boundary

```text
PostgreSQL
    → persistent catalog
    → instruments
    → raw formats
    → normalization metadata
    → datasets
    → lineage

ProcessingJob
    → frozen execution configuration
    → selected inputs
    → streams
    → stream ranks
    → resources
    → transformations
    → output settings

manifest.json
    → effective configuration used
    → ordering policy
    → stream ranks
    → physical files
    → integrity results
    → processing/schema versions
```

The core rule is:

> Persistent market facts belong in the catalog. Per-run execution decisions belong in `ProcessingJob`. Reproducibility-critical effective settings are preserved in `manifest.json`.