Performance benchmarks of MarketForge's Rust engine for historical tick-level trade processing.

## Hardware

| Component | Specification |
|---|---|
| CPU | AMD Ryzen 9 9950X |
| Physical cores | 16 |
| Logical threads | 32 |
| RAM | 128 GB (123 GiB) |
| Storage | NVMe SSD |
| OS | Ubuntu Linux |
| Engine | Rust (release build) |

## Processing Configuration

| Parameter | Value |
|---|---|
| Scheduler | Global multithreaded |
| Worker configurations | 4, 8, 16, 24, 32 |
| Repetitions | 2 per configuration |
| Parquet row-group target | 128 MiB |
| Parquet file target | 512 MiB |
| Integrity enforcement | Enabled |
| Memory budget | 60 GiB |
| Scratch budget | 200 GiB |

Memory and scratch budgets are configured but not yet strictly enforced.

Processing includes archive decompression, parsing, normalization, integrity validation, Parquet writing, and dataset commits.

## Trade Dataset

| Metric | Value |
|---|---:|
| Exchanges | Binance, Bitget, Bybit, OKX |
| Processing jobs | 18 |
| Source archives | 90 |
| Source records | ~47.7 million |
| Failed executions | 0 |

Gate.io was excluded from the scaling benchmark because its monthly trade archives are represented by single processing tasks.

## Global Scheduler Performance

| Workers | Average time | Throughput | Speedup vs. 4 workers |
|---|---:|---:|---:|
| 4 | 19.914 s | 2.40M records/s | 1.00× |
| 8 | 13.514 s | 3.53M records/s | 1.47× |
| **16** | **10.557 s** | **4.52M records/s** | **1.89×** |
| 24 | 10.984 s | 4.35M records/s | 1.81× |
| 32 | 11.371 s | 4.20M records/s | 1.75× |

Results represent two repetitions per configuration. The 16-worker configuration achieved the highest measured throughput.

An independent 16-worker benchmark measured 10.772 seconds and 4.43 million records/s.

## Parallel Consistency

A separate Bitget benchmark verified identical processing results using 1 and 16 workers.

| Metric | 1 worker | 16 workers |
|---|---:|---:|
| Tasks | 34 | 34 |
| Trades processed | 3,300,439 | 3,300,439 |
| Execution time | 4.249 s | 0.655 s |
| Throughput | 0.78M/s | 5.04M/s |
| Integrity | Clean | Clean |
| Output fingerprint | Identical | Identical |

**Parallel speedup: 6.49×**

The SHA-256 comparison uses Arrow RecordBatch representations under identical processing configurations.

## Full Trade Processing Validation

The complete five-exchange integration test produced:

| Metric | Result |
|---|---:|
| Processing jobs | 21 |
| Referenced archives | 93 |
| Source records | 71,958,169 |
| Canonical trades | 71,874,226 |
| Rejected records | 0 |
| Failed jobs | 0 |
| Integrity status | Clean |

All 21 processing jobs completed successfully.

## Conclusion

MarketForge achieved **4.52 million source records per second** using 16 workers on an AMD Ryzen 9 9950X.

Global scheduling distributes independent trade-processing tasks across exchanges while preserving per-job integrity evaluation and transactional dataset commits.

For the tested trade workload, **16 workers provided the best performance**, with throughput decreasing slightly at 24 and 32 workers.

Benchmarks measure end-to-end processing using real historical exchange archives.

## Note

**Gate.io was excluded from the parallel scaling benchmark** because it provides an entire month of historical trades in a single archive file. Each archive is processed as one task, preventing multiple workers from processing that archive concurrently. Including these large, single-task workloads would obscure the scaling benefits of global task scheduling across independent archives.

Gate.io remains supported by MarketForge and was successfully validated in the full five-exchange trade-processing test.