
Production benchmarks of MarketForge's Rust engine for physically materializing historical order-book depth events across multiple cryptocurrency exchanges.

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
| Merge mode | Depth-only |
| Benchmark interval | September 1–4, 2026 (72 hours) |
| Repetitions | 1 per configuration |
| Parquet row-group target | 128 MiB |
| Parquet file target | 512 MiB |
| Compression | ZSTD level 3 |
| Memory budget | 60 GiB |
| Maximum open files | 1,024 |
| Timestamp regression policy | Drop |
| Physical materialization | Enabled |
| Physical validation | Enabled |
| Transactional commits | Enabled |

The benchmark measures the complete production merge pipeline: dataset discovery, inventory construction, boundary reconciliation, partition planning, chronological merging, canonical depth-level resolution, physical Parquet writing, validation, and transactional publication.

Raw archive processing and order-book reconstruction are excluded.

## Dataset

| Metric | Value |
|---|---|
| Exchanges | Bitget, Bybit, Gate.io, OKX |
| Canonical datasets | 15 |
| Available canonical depth-level rows | 815,146,563 |
| Merged depth events | 405,581,426 |
| Benchmark duration | 72 hours |
| Successful executions | 3 |
| Failed executions | 0 |

The datasets contain spot, perpetual, and futures order-book data.

Canonical depth-level rows and logical depth events represent different quantities. A single depth event can reference multiple price-level changes.

## Performance

### Benchmark Results

| Workers | Partitions | Execution Time | Throughput | Speedup |
|---:|---:|---:|---:|---:|
| 8 | 8 | 651.2 s | 622,844 events/s | 1.00× |
| 16 | 16 | 597.9 s | 678,302 events/s | 1.09× |
| **24** | **48** | **586.6 s** | **691,372 events/s** | **1.11×** |

Execution times are approximate, reconstructed from reported throughput.

### Scaling

Increasing workers from 8 to 16 improves throughput by **8.9%**.

Increasing from 16 to 24 workers improves throughput by only **1.9%**.

Performance approaches a plateau beyond 16 workers, suggesting increasing resource contention or shared processing bottlenecks.

Worker and partition counts change together, so these measurements do not isolate worker scaling from partition overhead.

Potential limiting factors include Parquet decoding, canonical depth-level resolution, Arrow construction, compression, memory bandwidth, and storage throughput.

## Validation

All three configurations completed successfully.

| Check | Result |
|---|---|
| Physical materialization | PASS |
| Event count consistency | PASS |
| Chronological ordering validation | PASS |
| Physical Parquet validation | PASS |
| Manifest verification | PASS |
| Transactional publication | PASS |
| Failed executions | 0 |

Every execution produced exactly **405,581,426 depth events**.

MarketForge preserves canonical provenance, reconstruction metadata, and deterministic chronological ordering.

Timestamp-regressing events are excluded according to the configured drop policy without modifying canonical source datasets.

Identical event counts across configurations do not independently establish byte-for-byte output equivalence.

## Conclusion

MarketForge physically materialized **405.6 million historical depth events** across 15 datasets and four exchanges.

The highest observed throughput was **691,372 events/s**, achieved with **24 workers and 48 partitions**.

The complete merge finished in approximately **9.8 minutes**, including canonical data resolution, physical Parquet materialization, validation, and transactional publication.

Performance improvements diminish beyond 16 workers, indicating that further optimization should focus on profiling decoding, materialization, memory bandwidth, and I/O rather than increasing concurrency alone.

The results establish a production-scale physical depth-merging throughput of approximately **0.6–0.7 million events per second** on the tested hardware.