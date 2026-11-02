
Production benchmarks of MarketForge's Rust engine for physically materializing historical tick-level trades across multiple cryptocurrency exchanges.

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
| Merge mode | Trade-only |
| Benchmark interval | September 1–4, 2026 (72 hours) |
| Repetitions | 1 per configuration |
| Total executions | 9 |
| Parquet row-group target | 128 MiB |
| Parquet file target | 512 MiB |
| Compression | ZSTD level 3 |
| Memory budget | 60 GiB |
| Maximum open files | 1,024 |
| Physical materialization | Enabled |
| Physical validation | Enabled |
| Transactional commits | Enabled |

The benchmark measures the production merge pipeline: dataset discovery, inventory construction, partition planning, chronological merging, canonical trade resolution, physical Parquet writing, validation, and transactional publication.

Raw archive processing and normalization are excluded.

Resource budgets use reservation-based scheduling rather than strict operating-system limits.

## Dataset

| Metric | Value |
|---|---|
| Exchanges | Binance, Bitget, Bybit, Gate.io, OKX |
| Canonical datasets | 21 |
| Available canonical trades | 71,874,226 |
| Merged trade events | 50,106,717 |
| Benchmark duration | 72 hours |
| Successful executions | 9 |
| Failed executions | 0 |

The datasets include spot, perpetual linear, perpetual inverse, and dated futures instruments.

All configurations process the same **50,106,717 trade events**.

## Performance

### Benchmark Results

| Workers | Partitions | Execution Time | Throughput | Speedup |
|---:|---:|---:|---:|---:|
| 4 | 8 | 74.01 s | 677,044 events/s | 1.00× |
| 8 | 8 | 53.84 s | 930,666 events/s | 1.37× |
| 8 | 16 | 55.93 s | 895,942 events/s | 1.32× |
| 12 | 12 | 57.25 s | 875,207 events/s | 1.29× |
| 16 | 16 | 48.51 s | 1,032,950 events/s | 1.53× |
| 16 | 32 | 47.86 s | 1,047,026 events/s | 1.55× |
| 24 | 24 | 49.45 s | 1,013,316 events/s | 1.50× |
| **24** | **48** | **46.78 s** | **1,071,207 events/s** | **1.58×** |
| 32 | 32 | 47.49 s | 1,055,146 events/s | 1.56× |

Execution times are approximate, reconstructed from reported throughput.

### Scaling

Increasing concurrency from 4 to 16 workers substantially improves throughput.

Performance gains diminish beyond 16 workers, with the highest observed throughput at **24 workers and 48 partitions**.

Partition granularity also affects performance:

- 8 workers: 8 partitions outperform 16.
- 16 workers: 32 partitions slightly outperform 16.
- 24 workers: 48 partitions outperform 24.

The results indicate that optimal partitioning depends on concurrency and workload size.

Potential limiting factors include Parquet decoding and encoding, memory bandwidth, storage throughput, and partition scheduling overhead.

Because each configuration was executed once, small performance differences require repeated measurements to establish a definitive optimum.

## Validation

All nine configurations completed successfully.

| Check | Result |
|---|---|
| Physical materialization | PASS |
| Event count consistency | PASS |
| Physical Parquet validation | PASS |
| Canonical reference validation | PASS |
| Manifest verification | PASS |
| Transactional publication | PASS |
| Failed executions | 0 |

Every execution produced exactly **50,106,717 trade events**.

MarketForge uses deterministic chronological ordering:

```text
event_timestamp_ns
    → stream_rank
    → task_id
    → event_ordinal
```

The physical merge engine uses persistent forward-only Parquet readers, bounded cursor pools, resource-aware scheduling, and transactional output publication.

Identical event counts across configurations do not independently establish byte-for-byte output equivalence.

## Conclusion

MarketForge physically materialized **50.1 million historical trade events** across 21 datasets and five exchanges.

The highest observed throughput was **1,071,207 events/s**, achieved with **24 workers and 48 partitions**.

The complete merge finished in approximately **46.8 seconds**, including canonical data resolution, physical Parquet materialization, validation, and transactional publication.

Performance improvements diminish beyond 16 workers, with configurations using 16–32 workers achieving approximately one million events per second.

The results establish a production-scale physical trade-merging throughput of approximately **0.9–1.1 million events per second** on the tested hardware.