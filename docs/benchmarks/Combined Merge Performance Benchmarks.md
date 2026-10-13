
Production benchmark of MarketForge's Rust engine for physically merging historical tick-level trades and order-book depth events into a unified chronological dataset.

## Hardware

| Component | Specification |
|---|---|
| CPU | AMD Ryzen 9 9950X |
| Cores / Threads | 16 / 32 |
| RAM | 128 GB |
| Storage | NVMe SSD |
| OS | Ubuntu Linux |
| Engine | Rust (release build) |

## Configuration

| Parameter | Value |
|---|---|
| Merge mode | Combined (trades + depth) |
| Benchmark interval | September 1–4, 2026 (72 hours) |
| Workers | 24 |
| Partitions | 48 |
| Repetitions | 1 |
| Parquet row-group target | 128 MiB |
| Parquet file target | 512 MiB |
| Compression | ZSTD level 3 |
| Memory budget | 60 GiB |
| Maximum open files | 1,024 |
| Physical materialization | Enabled |
| Physical validation | Enabled |
| Transactional commits | Enabled |

The benchmark measures the production pipeline: dataset discovery, inventory construction, depth-boundary reconciliation, partition planning, chronological event merging, canonical row resolution, physical Parquet writing, validation, and transactional publication.

Raw archive processing, normalization, and order-book reconstruction are excluded.

Resource budgets use reservation-based scheduling rather than strict operating-system limits.

## Dataset

| Metric | Value |
|---|---:|
| Trade exchanges | Binance, Bitget, Bybit, Gate.io, OKX |
| Depth exchanges | Bitget, Bybit, Gate.io, OKX |
| Trade datasets | 21 |
| Depth datasets | 15 |
| **Total datasets** | **36** |
| Trade events | 50,106,717 |
| Depth events | 405,581,426 |
| **Total merged events** | **455,688,143** |
| Benchmark duration | 72 hours |

The workload includes spot, perpetual, and futures instruments across multiple exchanges.

Trade and depth events are interleaved into a single deterministic chronological timeline.

Depth events may reference multiple canonical price-level rows, making physical depth materialization more computationally demanding than trade materialization.

## Performance

### Benchmark Result

| Metric | Result |
|---|---:|
| Workers | 24 |
| Partitions | 48 |
| Trade events | 50,106,717 |
| Depth events | 405,581,426 |
| **Total events** | **455,688,143** |
| Execution time | ~611.8 s |
| Execution time | ~10.2 minutes |
| **Throughput** | **744,822 events/s** |
| Status | **PASS** |

Execution time is approximately reconstructed from the reported throughput.

### Comparison Across Merge Modes

All configurations use 24 workers, 48 partitions, and the same 72-hour interval.

| Mode | Events | Execution Time | Throughput |
|---|---:|---:|---:|
| Trades | 50,106,717 | 46.8 s | 1,071,207/s |
| Depth | 405,581,426 | 586.6 s | 691,372/s |
| **Combined** | **455,688,143** | **611.8 s** | **744,822/s** |

The combined merge completed approximately **3.4% faster** than executing the trade-only and depth-only benchmarks separately.

This suggests that unified processing introduces no substantial additional overhead for the tested workload.

However, the difference is based on single executions and may reflect normal benchmark variability.

### Performance Characteristics

The combined merge performs:

- Deterministic chronological merging across 36 datasets.
- Canonical trade and depth-level resolution.
- Persistent forward-only Parquet reading.
- Bounded canonical cursor management.
- Physical Arrow record construction.
- ZSTD-compressed Parquet writing.
- Physical output validation.
- Transactional dataset publication.

Potential performance limitations include Parquet decoding, Arrow construction, compression, memory bandwidth, storage throughput, and partition scheduling overhead.

## Validation

The production benchmark completed successfully.

| Check | Result |
|---|---|
| Physical materialization | PASS |
| Trade event accounting | PASS |
| Depth event accounting | PASS |
| Total event count | PASS |
| Physical Parquet validation | PASS |
| Manifest verification | PASS |
| Transactional publication | PASS |
| Execution status | PASS |

Event accounting:

```text
Trade events :  50,106,717
Depth events : 405,581,426
--------------------------
Total events : 455,688,143
```

MarketForge uses deterministic chronological ordering:

```text
event_timestamp_ns
    → stream_rank
    → task_id
    → event_ordinal
```

The physical output preserves canonical provenance and supports validation against source datasets.

The successful benchmark establishes event-count consistency and validated physical publication. It does not independently establish byte-for-byte equivalence with separately executed trade and depth merges.

## Conclusion

MarketForge successfully materialized **455.7 million historical trade and depth events** from 36 canonical datasets across five cryptocurrency exchanges.

Using **24 workers and 48 partitions**, the complete production merge finished in approximately **10.2 minutes**, achieving **744,822 events per second**.

The benchmark demonstrates large-scale chronological merging, canonical data resolution, physical Parquet materialization, output validation, and transactional publication on a single workstation.

The result establishes a production-scale combined physical merge throughput of approximately **745,000 events per second** on the tested hardware.