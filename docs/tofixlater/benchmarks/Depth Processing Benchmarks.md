Performance benchmarks of MarketForge's Rust engine for historical tick-level order-book depth processing.
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
| Worker configurations    | 4, 8, 16             |
| Repetitions              | 2 per configuration  |
| Parquet row-group target | 128 MiB              |
| Parquet file target      | 512 MiB              |
| Integrity enforcement    | Enabled              |
| Memory budget            | 60 GiB               |
| Scratch budget           | 200 GiB              |

Memory-aware task admission uses estimated resource reservations. Actual process memory is not strictly limited, and scratch disk usage is not yet enforced at the write boundary.

Processing includes archive decompression, parsing, normalization, order-book reconstruction, sequence validation where available, integrity evaluation, Parquet writing, reconstruction metadata generation, and transactional dataset commits.

## Depth Dataset

| Metric            | Value                       |
| ----------------- | --------------------------- |
| Exchanges         | Binance excluded; Bitget, Bybit, Gate.io, OKX |
| Processing jobs   | 12                          |
| Source records    | ~10.05 million              |
| Benchmark sessions| 6                           |
| Job executions    | 72                          |
| Failed executions | 0                           |
| Integrity         | Clean                       |

The benchmark used real historical L2 order-book archives from four exchanges.

Supported processing formats include:

- Bybit snapshot and incremental depth updates.
- Gate.io hourly snapshots and incremental updates.
- OKX periodic snapshots and incremental updates.
- Bitget standalone 500-level order-book snapshots.

The processing engine normalizes these different source formats into a unified canonical depth representation.

For snapshot-based sources, MarketForge derives canonical level changes by comparing consecutive reconstructed book states.

For sequenced sources, continuity validation is performed according to the source's sequence semantics.

The benchmark selected 12 processing jobs. A separate workload inventory identified 15 available depth jobs containing 252 archives and approximately 4.49 GiB of compressed source data. The three-job difference was not investigated, so those inventory totals should not be treated as the exact benchmark workload.

## Global Scheduler Performance

| Workers | Average time | Throughput         | Speedup vs. 4 workers |
| ------- | ------------ | ------------------ | --------------------- |
| 4       | 164.122 s    | 61,209 records/s   | 1.00×                 |
| 8       | 109.977 s    | 91,344 records/s   | 1.49×                 |
| **16**  | **82.057 s** | **122,425 records/s** | **2.00×**          |

Results represent two repetitions per configuration.

The 16-worker configuration achieved the highest measured throughput.

Increasing concurrency from 4 to 8 workers improved throughput by approximately 49%.

Increasing concurrency from 4 to 16 workers doubled throughput and reduced average processing time by approximately 50%.

### Scaling Efficiency

| Workers | Speedup | Parallel efficiency |
| ------- | ------- | ------------------- |
| 4       | 1.00×   | 100%                |
| 8       | 1.49×   | 74.6%               |
| 16      | 2.00×   | 50.0%               |

Parallel efficiency is measured relative to the four-worker baseline.

Scaling becomes less efficient at higher worker counts, potentially reflecting storage bandwidth, memory bandwidth, decompression overhead, Parquet compression, or uneven task workloads.

The benchmark does not isolate these bottlenecks.

## Processing Validation

All benchmark executions completed successfully.

| Metric               | Result |
| -------------------- | ------ |
| Processing jobs      | 12     |
| Benchmark sessions   | 6      |
| Total job executions | 72     |
| Successful executions| 72     |
| Failed executions    | 0      |
| Integrity failures   | 0      |

The global scheduler distributes independent depth-processing tasks across workers while preserving task isolation.

Each task independently maintains its reconstructed order book, sequence tracker, integrity metrics, and Parquet writer.

Completed datasets include:

| Output | Purpose |
| ------ | ------- |
| `part-*.parquet` | Canonical L2 level updates |
| `events.parquet` | Event timestamps, sequence metadata, and canonical row offsets |
| `segments.json` | Reconstruction segment boundaries |
| `boundary.json` | Initial/final book states, continuity metadata, and fingerprints |
| `manifest.json` | Dataset-level metadata, file inventory, and processing results |

Dataset manifests distinguish canonical Parquet files from auxiliary reconstruction metadata.

Job-level integrity evaluation and transactional dataset commits remain enabled during parallel execution.

### Independent Parallel Processing Validation

A separate Gate.io BTC_USDT spot test processed three consecutive hourly archives using two workers.

| Metric            | Result     |
| ----------------- | ---------- |
| Source archives   | 3          |
| Workers           | 2          |
| Source records    | 2,160,734  |
| Canonical levels  | 2,160,734  |
| Rejected records  | 0          |
| Parquet files     | 3          |
| Integrity         | Clean      |
| Dataset commit    | Successful |
| Execution time    | 5.615 s    |
| Throughput        | ~385K/s    |

All three tasks completed successfully, and the resulting dataset was committed with its canonical data and reconstruction metadata.

## Resource Management

MarketForge uses a shared resource-budget manager for parallel task admission.

The scheduler considers estimated memory requirements before admitting additional tasks.

Resource reservations are released automatically when tasks finish.

| Resource                  | Implementation |
| ------------------------- | -------------- |
| Worker concurrency        | Enforced       |
| Estimated memory budget   | Reservation-based admission |
| Estimated scratch budget  | Reservation infrastructure |
| Actual process RAM limit  | Not enforced   |
| Actual scratch disk limit | Not enforced   |
| Task resource isolation   | Implemented    |

Memory estimates account for source characteristics and Parquet buffering.

Bitget XLSX processing receives higher estimated memory requirements because its decoder buffers workbook data, parses worksheet contents, materializes snapshots, and sorts them chronologically.

Resource reservations are scheduling estimates rather than hard operating-system limits.

The current scratch workspace supports usage inspection and capacity checks, but strict write-time disk enforcement remains unimplemented.

## Conclusion

MarketForge achieved **122,425 source depth records per second** using 16 workers on an AMD Ryzen 9 9950X.

The global scheduler successfully completed **72 depth-processing job executions with zero failures**.

Compared with four workers, the 16-worker configuration delivered:

- **2.00× higher throughput**
- **50% lower processing time**
- **122K source records per second**

For the tested depth workload, **16 workers provided the best measured performance** among the evaluated configurations.

Unlike trade processing, depth processing includes stateful order-book reconstruction, sequence validation, snapshot differencing, and reconstruction metadata generation.

These additional operations contribute to its lower throughput relative to the trade-processing benchmark.

Benchmarks measure end-to-end processing using real historical exchange archives, including normalization, integrity evaluation, Parquet persistence, and dataset commits.