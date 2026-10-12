Performance benchmarks of MarketForge's Rust engine for historical tick-level trade merging across multiple cryptocurrency exchanges and instruments.

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

## Merge Configuration

| Parameter                | Value                      |
| ------------------------ | -------------------------- |
| Execution                | Parallel partition merging |
| Worker configurations    | 4, 8, 16, 24, 32           |
| Repetitions              | 2 per configuration        |
| Partitions per worker    | 4                          |
| Parquet row-group target | 128 MiB                    |
| Parquet file target      | 512 MiB                    |
| Compression              | ZSTD level 3               |
| Physical validation      | Enabled                    |
| Canonical validation     | Enabled                    |
| Memory budget            | 60 GiB                     |
| Maximum open files       | 1,024                      |
| Transactional commits    | Enabled                    |

Memory and file budgets use resource-aware scheduling rather than strict operating-system limits.

The benchmark measures the complete production merge operation, including:

- Canonical dataset discovery and metadata resolution.
- Trade source inventory construction.
- Timestamp-range partition planning.
- Parallel chronological event merging.
- Deterministic event ordering.
- Unified Arrow and Parquet writing.
- Physical output validation.
- Canonical source-reference validation.
- Manifest generation.
- Transactional dataset publication.

Unlike trade processing, merging operates on already normalized canonical Parquet datasets.

Archive decompression, raw-data parsing, and normalization are therefore excluded.

## Trade Dataset

| Metric                   | Value                                        |
| ------------------------ | -------------------------------------------- |
| Exchanges                | Binance, Bitget, Bybit, Gate.io, OKX          |
| Canonical input datasets | 21                                           |
| Available canonical rows | 71,874,226                                   |
| Merged trade events      | 50,106,717                                   |
| Benchmark interval       | September 1–4, 2026 (UTC)                    |
| Benchmark sessions       | 10                                           |
| Failed executions        | 0                                            |
| Merge mode               | Trade-only                                   |
| Manifest version         | 2                                            |

The benchmark uses real canonical trade datasets produced by MarketForge's processing engine.

Inputs include spot, perpetual linear, perpetual inverse, and dated futures instruments.

Unlike the earlier trade-processing scaling benchmark, Gate.io is included.

The merge engine partitions canonical data by timestamp rather than by the original source archive boundaries. Consequently, monthly archives do not inherently prevent parallel merging.

### Dataset Coverage

The selected datasets contain 71,874,226 canonical trades.

Of these, 50,106,717 events fall within the benchmark's requested timestamp interval and are included in the merged output.

Every worker configuration processes the same input datasets and timestamp interval.

The benchmark includes multiple instruments from the same exchange, allowing MarketForge to merge independent chronological streams into a deterministic unified timeline.

## Parallel Merge Performance

| Workers | Partitions | Average time | Throughput       | Speedup vs. 4 workers |
| ------- | ---------- | ------------ | ---------------- | --------------------- |
| 4       | 16         | 5.791 s      | 8.65M events/s   | 1.00×                 |
| 8       | 32         | 5.007 s      | 10.01M events/s  | 1.16×                 |
| **16**  | **64**     | **4.884 s**  | **10.26M events/s** | **1.19×**          |
| 24      | 96         | 5.125 s      | 9.78M events/s   | 1.13×                 |
| 32      | 128        | 5.431 s      | 9.23M events/s   | 1.07×                 |

Results represent two repetitions per worker configuration.

Throughput is calculated using the total merged event count divided by the total execution time across both repetitions.

The **16-worker configuration achieved the highest measured throughput**, processing approximately 10.26 million events per second.

Increasing concurrency from 4 to 16 workers improved throughput by approximately 19%.

Increasing concurrency beyond 16 workers reduced throughput.

### Individual Benchmark Runs

| Workers | Repetition 1 | Repetition 2 | Average |
| ------- | ------------ | ------------ | ------- |
| 4       | 5.775 s      | 5.806 s      | 5.791 s |
| 8       | 5.045 s      | 4.968 s      | 5.007 s |
| 16      | 4.892 s      | 4.875 s      | 4.884 s |
| 24      | 5.114 s      | 5.135 s      | 5.125 s |
| 32      | 5.392 s      | 5.470 s      | 5.431 s |

The benchmark alternates worker execution order between repetitions to reduce systematic cache and execution-order bias.

All ten executions completed successfully.

## Scaling Efficiency

| Workers | Speedup | Parallel efficiency |
| ------- | ------- | ------------------- |
| 4       | 1.00×   | 100.0%              |
| 8       | 1.16×   | 57.8%               |
| 16      | 1.19×   | 29.6%               |
| 24      | 1.13×   | 18.8%               |
| 32      | 1.07×   | 13.3%               |

Parallel efficiency is measured relative to the four-worker baseline.

The results indicate diminishing returns as concurrency increases.

Unlike raw trade processing, the merge engine reads already normalized Parquet data and performs comparatively lightweight event transformations.

Potential limiting factors include:

- Parquet decoding and encoding throughput.
- Memory bandwidth.
- Storage throughput.
- Partition initialization overhead.
- Per-partition file handling.
- Physical output validation.
- Increasing scheduling overhead at higher partition counts.

The benchmark does not independently isolate these factors.

### Partition Scaling Considerations

The benchmark uses four partitions per worker.

Consequently, increasing worker counts also increases the number of partitions:

| Workers | Partitions |
| ------- | ---------- |
| 4       | 16         |
| 8       | 32         |
| 16      | 64         |
| 24      | 96         |
| 32      | 128        |

These measurements therefore represent combined worker-concurrency and partition-granularity scaling.

They should not be interpreted as isolated worker-scaling measurements.

A future fixed-partition benchmark can separate the effects of worker concurrency from partition overhead.

## Merge Validation

All benchmark executions completed successfully.

| Metric                  | Result     |
| ----------------------- | ---------- |
| Canonical input datasets| 21         |
| Benchmark sessions      | 10         |
| Successful executions   | 10         |
| Failed executions       | 0          |
| Events per execution    | 50,106,717 |
| Event count consistency | Verified   |
| Manifest verification   | Passed     |
| Transactional commits   | Successful |

Every execution produced exactly **50,106,717 merged trade events**, independently of worker count or partition configuration.

This verifies event-count consistency across the tested parallel configurations.

The benchmark validates each committed dataset manifest against the execution result.

Validation includes:

- Job identity.
- Total event count.
- Parquet file inventory.
- File existence and size.
- Canonical row-count accounting.
- Safe manifest-relative file paths.

The production unified merge executor additionally performs physical Parquet validation and canonical source-reference validation before publication.

### Deterministic Ordering

MarketForge uses deterministic chronological ordering based on:

```text
event_timestamp_ns
    → stream_rank
    → task_id
    → event_ordinal
```

This ordering provides consistent tie-breaking when events from different exchanges or instruments share identical timestamps.

The benchmark confirms identical event counts across worker configurations.

It does not independently establish byte-for-byte or row-by-row output equivalence between all benchmark executions.

Sequential and parallel equivalence are covered by separate integration tests.

### Transactional Publication

Every benchmark execution publishes its output only after successful merge execution and validation.

Successful benchmark datasets are verified and then removed to avoid unnecessary disk accumulation.

Failed outputs are preserved for inspection.

The benchmark report is retained at:

```text
benchmarks/results/trades-merge/benchmark.json
```

## Resource Management

MarketForge uses resource-aware partition scheduling.

| Resource                  | Implementation              |
| ------------------------- | --------------------------- |
| Worker concurrency        | Configured and enforced     |
| Estimated memory budget   | Reservation-based admission |
| Maximum open files        | Configured budget           |
| Actual process RAM limit  | Not strictly enforced       |
| Transactional publication | Enabled                     |
| Output validation         | Enabled                     |

The benchmark uses a configured memory budget of 60 GiB.

Resource estimates influence partition admission, but the configured budget is not a hard operating-system memory limit.

### Independent Memory Observation

A separate four-worker production merge of the same 50,106,717 trade events reported:

| Metric                  | Result       |
| ----------------------- | ------------ |
| Workers                 | 4            |
| Partitions              | 16           |
| Execution time          | 5.911 s      |
| Throughput              | 8.48M/s      |
| Output size             | 270.21 MiB   |
| Peak resident memory    | ~890 MiB     |
| Average CPU utilization | 238%         |
| Execution status        | PASS         |

These measurements come from an independent `/usr/bin/time -v` execution.

Peak memory and CPU utilization were not recorded for every configuration in the ten-session benchmark, so worker-dependent memory scaling cannot yet be evaluated.

## Conclusion

MarketForge achieved **10.26 million merged trade events per second** using 16 workers on an AMD Ryzen 9 9950X.

The benchmark merged **50,106,717 real trade events from 21 canonical datasets across five cryptocurrency exchanges**.

All ten benchmark executions completed successfully, with identical event counts and validated transactional commits.

Compared with four workers, the 16-worker configuration delivered:

- **1.19× higher throughput**
- **Approximately 15.7% lower execution time**
- **10.26 million merged events per second**

For the tested workload, **16 workers provided the best measured performance** among the evaluated configurations.

Performance declined slightly at 24 and 32 workers, indicating diminishing returns from additional concurrency under the current partitioning strategy.

The relatively modest parallel speedup suggests that further optimization should focus on identifying bottlenecks in Parquet I/O, partition overhead, memory bandwidth, and validation rather than simply increasing worker counts.

These benchmarks measure end-to-end production merging of already normalized canonical trade datasets, including deterministic ordering, unified Parquet persistence, physical validation, manifest generation, and transactional commits.

They are not directly comparable to raw trade-processing throughput because the two pipelines perform different operations.