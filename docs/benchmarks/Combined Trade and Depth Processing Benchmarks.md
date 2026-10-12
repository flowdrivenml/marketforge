Performance benchmarks of MarketForge's Rust engine for concurrent historical tick-level trade and order-book depth processing.

## Hardware

| Component       | Specification        |
| --------------- | -------------------- |
| CPU             | AMD Ryzen 9 9950X    |
| Physical cores  | 16                   |
| Logical threads | 32                   |
| RAM             | 128 GB (123 GiB)     |
| Storage         | NVMe SSD             |
| OS              | Ubuntu Linux         |
| Engine          | Rust (release build) |

## Processing Configuration

| Parameter                | Value                |
| ------------------------ | -------------------- |
| Scheduler                | Global multithreaded |
| Worker configurations    | 4, 8, 16, 24, 32     |
| Repetitions              | 2 per configuration  |
| Parquet row-group target | 128 MiB              |
| Parquet file target      | 512 MiB              |
| Integrity enforcement    | Enabled              |
| Memory budget            | 60 GiB               |
| Scratch budget           | 200 GiB              |

Memory-aware task admission uses estimated resource reservations. Actual process memory and scratch disk consumption are not strictly enforced.

Processing includes archive decompression, parsing, canonical normalization, order-book reconstruction, integrity validation, Parquet writing, reconstruction metadata generation, and transactional dataset commits.

Trades and depth tasks execute concurrently through a shared global worker pool.

No chronological dataset merging is performed during this benchmark.

## Combined Dataset

| Metric               | Value                  |
| -------------------- | ---------------------- |
| Data types           | Trades and L2 depth    |
| Exchanges            | Binance, Bitget, Bybit, Gate.io, OKX |
| Processing jobs      | 30                     |
| Benchmark sessions   | 10                     |
| Total job executions | 300                    |
| Failed executions    | 0                      |
| Integrity failures   | 0                      |

The benchmark combines historical trade and order-book depth processing workloads across multiple cryptocurrency exchanges.

Trade processing includes exchange-specific decoding, quantity normalization, timestamp normalization, integrity validation, and canonical Parquet generation.

Depth processing additionally performs stateful order-book reconstruction, sequence validation where available, snapshot differencing, and reconstruction metadata generation.

The global scheduler distributes independent tasks from both processing pipelines across the same worker pool.

## Global Scheduler Performance

| Workers | Average time | Throughput          | Speedup vs. 4 workers |
| ------- | ------------ | ------------------- | --------------------- |
| 4       | 181.306 s    | 318,662 records/s   | 1.00×                 |
| 8       | 121.902 s    | 473,951 records/s   | 1.49×                 |
| **16**  | **92.679 s** | **623,393 records/s** | **1.96×**           |
| 24      | 95.458 s     | 605,245 records/s   | 1.90×                 |
| 32      | 95.263 s     | 606,480 records/s   | 1.90×                 |

Results represent two repetitions per worker configuration.

The 16-worker configuration achieved the highest measured throughput.

Increasing concurrency from 4 to 16 workers improved throughput by approximately 96% and reduced average execution time by approximately 49%.

Performance decreased slightly beyond 16 workers.

## Parallel Scaling

| Workers | Speedup vs. 4 | Parallel efficiency |
| ------- | ------------- | ------------------- |
| 4       | 1.00×         | 100%                |
| 8       | 1.49×         | 74.5%               |
| 16      | 1.96×         | 49.0%               |
| 24      | 1.90×         | 31.7%               |
| 32      | 1.90×         | 23.8%               |

Parallel efficiency is calculated relative to the four-worker baseline.

### Scaling Characteristics

Throughput increased substantially between 4 and 16 workers.

Beyond 16 workers, additional concurrency did not improve overall performance.

Potential contributing factors include:

- CPU resource contention.
- Memory bandwidth saturation.
- NVMe storage throughput.
- Archive decompression overhead.
- Parquet compression and serialization.
- Uneven task sizes and processing complexity.

The benchmark does not isolate individual bottlenecks.

The observed performance plateau is consistent with the hardware's 16 physical CPU cores, although this alone does not establish the cause.

## Processing Validation

All benchmark executions completed successfully.

| Metric                | Result |
| --------------------- | ------ |
| Processing jobs       | 30     |
| Benchmark sessions    | 10     |
| Total job executions  | 300    |
| Successful executions | 300    |
| Failed executions     | 0      |

The benchmark validates concurrent execution of independent trade and depth tasks through a shared scheduler.

### Task Isolation

Each processing task independently owns its decoding, normalization, integrity, and Parquet-writing state.

Depth tasks additionally maintain independent order-book reconstruction and sequence-tracking state.

The global scheduler preserves deterministic task-result aggregation regardless of execution completion order.

### Dataset Output

Trade processing produces canonical Parquet datasets.

Depth processing produces canonical Parquet datasets together with reconstruction metadata.

| Output           | Purpose                                                 |
| ---------------- | ------------------------------------------------------- |
| `part-*.parquet` | Canonical trade or L2 depth records                     |
| `events.parquet` | Depth event indexing and canonical row offsets          |
| `segments.json`  | Depth reconstruction segment boundaries                 |
| `boundary.json`  | Initial/final order-book states and continuity metadata |
| `manifest.json`  | Dataset-level metadata and processing results           |

Job-level integrity evaluation and transactional dataset commits remain enabled during concurrent execution.

Each processing job is finalized independently.

## Comparison with Individual Processing Benchmarks

MarketForge was also benchmarked separately for trade-only and depth-only processing.

| Workload | Best workers | Peak throughput |
| -------- | ------------ | --------------- |
| Trades only | 16 | 4.52M records/s |
| Depth only | 16 | 122,425 records/s |
| **Trades + Depth** | **16** | **623,393 records/s** |

All three benchmarks achieved their highest measured throughput at 16 workers.

### Throughput Interpretation

Trade records generally correspond to individual canonical trade events.

Depth source records may represent complete order-book snapshots, incremental updates, or multiple price-level changes.

Consequently, source-record throughput is not directly equivalent to canonical output throughput.

The combined benchmark reports total source records processed per second across both pipelines.

Its throughput should not be interpreted as a direct measure of canonical depth levels generated per second.

The combined benchmark also processes a different workload from either standalone benchmark, so throughput differences cannot be attributed solely to resource contention.

## Resource Management

MarketForge uses resource-aware task admission to control concurrency.

The scheduler reserves estimated memory requirements before executing tasks and releases those reservations after task completion.

| Resource                  | Implementation |
| ------------------------- | -------------- |
| Worker concurrency        | Enforced       |
| Estimated memory budget   | Reservation-based admission |
| Estimated scratch budget  | Reservation infrastructure |
| Actual process RAM limit  | Not enforced   |
| Actual scratch disk limit | Not enforced   |
| Task resource isolation   | Implemented    |

Memory estimates account for source characteristics and Parquet buffering requirements.

Memory-intensive formats, particularly Bitget XLSX depth archives, receive higher estimated memory reservations.

The same resource budget is shared across trade and depth tasks during global scheduling.

Resource reservations provide admission control rather than strict operating-system memory limits.

## Conclusion

MarketForge achieved **623,393 source records per second** while processing historical trades and order-book depth concurrently using 16 workers on an AMD Ryzen 9 9950X.

The global scheduler completed **300 processing job executions with zero failures** across 10 benchmark sessions.

Compared with four workers, the 16-worker configuration delivered:

- **1.96× higher throughput**
- **Approximately 49% lower processing time**
- **623K source records per second**
- **Zero failed job executions**

The combined benchmark confirms that MarketForge can execute trade and depth processing workloads concurrently through a shared multithreaded scheduler.

Across the trade-only, depth-only, and combined benchmarks, **16 workers consistently provided the highest measured throughput**.

Additional concurrency at 24 and 32 workers did not improve performance for the tested workloads.

These results validate the shared processing architecture, task isolation, integrity evaluation, and transactional dataset commits under concurrent execution.

Benchmarks measure end-to-end processing using real historical exchange archives.