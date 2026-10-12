Performance benchmarks of MarketForge's Rust engine for historical tick-level order-book depth merging across multiple cryptocurrency exchanges and instruments.

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
| Timestamp regression     | Drop                       |
| Memory budget            | 60 GiB                     |
| Maximum open files       | 1,024                      |
| Transactional commits    | Enabled                    |

Memory and file budgets use resource-aware scheduling rather than strict operating-system limits.

The benchmark measures the complete production depth merge operation, including:

- Canonical dataset discovery and metadata resolution.
- Depth source inventory construction.
- Reconstruction boundary reconciliation.
- Reconstruction checkpoint resolution.
- Timestamp-range partition planning.
- Parallel chronological depth event merging.
- Redundant initialization handling.
- Timestamp regression filtering.
- Deterministic event ordering.
- Parquet writing.
- Physical output validation.
- Manifest generation.
- Transactional dataset publication.

Unlike depth processing, merging operates on already reconstructed canonical depth datasets.

Archive decompression, raw-data parsing, order-book reconstruction, snapshot differencing, and normalization are excluded.

## Depth Dataset

| Metric                   | Value                       |
| ------------------------ | --------------------------- |
| Exchanges                | Bitget, Bybit, Gate.io, OKX  |
| Canonical input datasets | 15                          |
| Available canonical rows | 815,146,563                 |
| Merged depth events      | 405,581,426                 |
| Benchmark interval       | September 1–4, 2026 (UTC)   |
| Benchmark sessions       | 10                          |
| Failed executions        | 0                           |
| Merge mode               | Depth-only                  |
| Manifest version         | 1                           |

The benchmark uses real historical L2 order-book datasets produced by MarketForge's depth-processing engine.

Inputs include spot, perpetual linear, perpetual inverse, and dated futures instruments.

Supported canonical sources originate from:

- Bybit snapshot and incremental depth updates.
- Gate.io hourly snapshots and incremental updates.
- OKX periodic snapshots and incremental updates.
- Bitget standalone 500-level order-book snapshots.

MarketForge processes these exchange-specific formats into a common canonical depth representation before merging.

### Dataset Coverage

The 15 input datasets contain **815,146,563 canonical depth-level rows**.

The benchmark produces **405,581,426 merged depth timeline events** within the selected timestamp interval.

Canonical depth-level rows and logical depth events represent different quantities.

A single depth event may reference multiple canonical level changes, while some events may represent initialization checkpoints or book-state transitions.

Consequently, the difference between canonical row counts and merged event counts should not be interpreted as discarded data.

Every benchmark execution uses the same input datasets and timestamp interval.

## Parallel Merge Performance

| Workers | Partitions | Average time | Throughput        | Speedup vs. 4 workers |
| ------- | ---------- | ------------ | ----------------- | --------------------- |
| 4       | 16         | 45.927 s     | 8.83M events/s    | 1.00×                 |
| 8       | 32         | 32.303 s     | 12.56M events/s   | 1.42×                 |
| **16**  | **64**     | **26.453 s** | **15.33M events/s** | **1.74×**           |
| 24      | 96         | 27.621 s     | 14.68M events/s   | 1.66×                 |
| 32      | 128        | 28.817 s     | 14.07M events/s   | 1.59×                 |

Results represent two repetitions per worker configuration.

Throughput is calculated using the total merged event count divided by the total execution time across both repetitions.

The **16-worker configuration achieved the highest measured throughput**, processing approximately 15.33 million depth events per second.

Increasing concurrency from 4 to 8 workers improved throughput by approximately 42%.

Increasing concurrency from 4 to 16 workers improved throughput by approximately 74%.

Increasing concurrency beyond 16 workers reduced throughput.

### Individual Benchmark Runs

| Workers | Repetition 1 | Repetition 2 | Average  |
| ------- | ------------ | ------------ | -------- |
| 4       | 46.238 s     | 45.616 s     | 45.927 s |
| 8       | 32.040 s     | 32.565 s     | 32.303 s |
| 16      | 26.664 s     | 26.242 s     | 26.453 s |
| 24      | 27.610 s     | 27.632 s     | 27.621 s |
| 32      | 28.768 s     | 28.865 s     | 28.817 s |

The benchmark alternates worker execution order between repetitions to reduce systematic cache and execution-order bias.

All ten executions completed successfully.

### Performance Stability

The two repetitions produced relatively consistent execution times.

| Workers | Time difference | Relative difference |
| ------- | --------------- | ------------------- |
| 4       | 0.622 s         | 1.35%               |
| 8       | 0.525 s         | 1.63%               |
| 16      | 0.422 s         | 1.60%               |
| 24      | 0.022 s         | 0.08%               |
| 32      | 0.097 s         | 0.34%               |

Relative differences are calculated against the average execution time for each worker configuration.

The relatively small differences indicate stable performance across the two measured repetitions.

However, two repetitions are insufficient to establish detailed statistical confidence intervals.

## Scaling Efficiency

| Workers | Speedup | Parallel efficiency |
| ------- | ------- | ------------------- |
| 4       | 1.00×   | 100.0%              |
| 8       | 1.42×   | 71.1%               |
| 16      | 1.74×   | 43.4%               |
| 24      | 1.66×   | 27.7%               |
| 32      | 1.59×   | 19.9%               |

Parallel efficiency is measured relative to the four-worker baseline.

The results demonstrate meaningful parallel scaling up to 16 workers, followed by diminishing returns.

Potential limiting factors include:

- Parquet decoding and encoding throughput.
- Memory bandwidth.
- Storage bandwidth.
- Partition initialization overhead.
- Depth event index traversal.
- Reconstruction metadata access.
- Physical output validation.
- Scheduling and synchronization overhead.

The benchmark does not independently isolate these bottlenecks.

### Partition Scaling Considerations

The benchmark uses four partitions per worker.

| Workers | Partitions |
| ------- | ---------- |
| 4       | 16         |
| 8       | 32         |
| 16      | 64         |
| 24      | 96         |
| 32      | 128        |

Increasing worker counts also increases partition granularity.

These measurements therefore represent combined worker-concurrency and partition-granularity scaling rather than isolated worker scaling.

A future fixed-partition benchmark could separate the effects of worker concurrency from partition overhead.

## Merge Validation

All benchmark executions completed successfully.

| Metric                  | Result      |
| ----------------------- | ----------- |
| Canonical input datasets| 15          |
| Benchmark sessions      | 10          |
| Successful executions   | 10          |
| Failed executions       | 0           |
| Events per execution    | 405,581,426 |
| Event count consistency | Verified    |
| Manifest verification   | Passed      |
| Transactional commits   | Successful  |

Every execution produced exactly **405,581,426 merged depth events**, independently of worker count and partition configuration.

This verifies event-count consistency across the tested parallel configurations.

The benchmark validates committed dataset manifests against execution results.

Validation includes:

- Job identity.
- Total event count.
- Parquet file inventory.
- File existence and size.
- Canonical row-count accounting.
- Safe manifest-relative file paths.

The production depth merge executor additionally performs chronological ordering and physical Parquet validation before publication.

### Deterministic Ordering

MarketForge constructs a unified chronological depth timeline using event timestamps and deterministic source tie-breaking.

The merge preserves references to canonical depth events, including:

- Dataset identity.
- Source task identity.
- Event ordinal.
- Canonical row offset.
- Canonical row count.
- Original event timestamp.

The benchmark confirms identical event counts across worker configurations.

It does not independently establish byte-for-byte or row-by-row equivalence between all benchmark executions.

Sequential and parallel equivalence require separate validation.

### Reconstruction Boundaries

Depth merging differs from trade merging because depth events represent stateful order-book updates.

MarketForge reconciles reconstruction boundaries between consecutive source tasks.

When adjacent archives contain equivalent reconstructed book states, redundant initialization checkpoints may be excluded from the logical merged timeline.

Independent initializations and genuine reconstruction resets remain meaningful events.

The merge preserves canonical reconstruction metadata and references without rewriting the original depth datasets.

## Timestamp Regression Handling

During benchmark preparation, MarketForge detected timestamp regressions in one Gate.io depth dataset.

### Source Investigation

| Metric                     | Result                     |
| -------------------------- | -------------------------- |
| Dataset                    | 96                         |
| Exchange                   | Gate.io                    |
| Instrument                 | BTC_USDT linear perpetual  |
| Source tasks               | 72                         |
| Tasks with regressions     | 26                         |
| Detected regressions       | 35                         |
| Maximum timestamp lateness | 100 ms                     |
| Other depth datasets       | No within-task regressions |

The regressions occurred inside canonical depth event indexes rather than exclusively at archive boundaries.

Inspection of consecutive canonical events showed increasing event ordinals, canonical row offsets, and exchange sequence numbers despite a 100 ms backward timestamp movement.

This indicates that the canonical event index preserves source-sequence progression in the inspected cases.

The original raw archive timestamps were not independently verified around the affected events, so the precise origin of the timestamp disorder remains unconfirmed.

### Default Regression Policy

MarketForge uses a **drop policy** for timestamp-regressing depth events during partitioned merging.

When an event timestamp is earlier than the previously accepted timestamp within its partition stream, the event is excluded from the merged timeline.

The original canonical dataset remains unchanged.

The policy:

- Preserves chronological ordering in merged output.
- Does not modify event timestamps.
- Does not rewrite canonical depth rows.
- Excludes regressing event references.
- Counts dropped events.
- Records the count in the merge manifest.

This policy intentionally sacrifices a small number of timeline events to preserve chronological output.

Because canonical reconstruction is unchanged, dropping an event reference does not undo its effect on the reconstructed book.

Consequently, a filtered merged timeline should not automatically be treated as a complete standalone order-book reconstruction history.

### Independent Gate.io Validation

A separate production merge using dataset 96 verified the implementation.

| Metric                        | Result      |
| ----------------------------- | ----------- |
| Input datasets                | 1           |
| Workers                       | 4           |
| Partitions                    | 16          |
| Merged events                 | 303,557,574 |
| Timestamp regressions dropped | 35          |
| Skipped initializations       | 68          |
| Parquet files                 | 16          |
| Output size                   | 792.40 MiB  |
| Execution status              | PASS        |

The **35 dropped events exactly matched the independent timestamp-regression audit**.

This confirmed correct dropped-event accounting for the tested partition configuration.

The full benchmark subsequently completed all ten executions with identical merged event counts.

However, per-execution dropped-regression counters were not included in the supplied benchmark console output. Therefore, identical drop counts across every worker configuration have not been independently established.

## Resource Management

MarketForge uses resource-aware partition scheduling.

| Resource                  | Implementation              |
| ------------------------- | --------------------------- |
| Worker concurrency        | Configured and enforced     |
| Estimated memory budget   | Reservation-based admission |
| Maximum open files        | Configured budget           |
| Actual process RAM limit  | Not strictly enforced       |
| Transactional publication | Enabled                     |
| Physical output validation| Enabled                     |

The benchmark uses a configured memory budget of 60 GiB.

Resource reservations influence partition admission, but the configured budget is not a hard operating-system memory limit.

### Independent Memory Observation

A separate four-worker production depth merge of dataset 96 reported:

| Metric                  | Result      |
| ----------------------- | ----------- |
| Workers                 | 4           |
| Partitions              | 16          |
| Merged events           | 303,557,574 |
| Execution time          | 30.93 s     |
| Throughput              | 9.81M/s     |
| Output size             | 792.40 MiB  |
| Peak resident memory    | ~1.62 GiB   |
| Average CPU utilization | 299%        |
| Execution status        | PASS        |

These measurements come from an independent `/usr/bin/time -v` execution.

Peak memory and CPU utilization were not recorded for every configuration in the ten-session benchmark.

Consequently, worker-dependent memory scaling cannot yet be evaluated.

### Benchmark Report

The complete benchmark results are stored at:

```text
benchmarks/results/depth-merge-v2/benchmark.json
```

Successful benchmark outputs are removed after validation to avoid unnecessary disk accumulation.

Failed outputs are preserved for debugging.

## Conclusion

MarketForge achieved **15.33 million merged depth events per second** using 16 workers on an AMD Ryzen 9 9950X.

The benchmark merged **405,581,426 real depth timeline events from 15 canonical datasets across four cryptocurrency exchanges**.

All ten benchmark executions completed successfully, producing identical event counts and validated transactional commits.

Compared with four workers, the 16-worker configuration delivered:

- **1.74× higher throughput**
- **42.4% lower execution time**
- **15.33 million merged depth events per second**

For the tested workload, **16 workers provided the best measured performance** among the evaluated configurations.

Performance declined at 24 and 32 workers, suggesting that additional concurrency introduces overhead exceeding its benefits under the current partitioning strategy.

The depth merge engine also successfully handled the previously identified Gate.io timestamp regressions using the default drop policy, preserving chronological output without modifying canonical source datasets.

Unlike depth processing, which includes stateful order-book reconstruction, snapshot differencing, sequence validation, and normalization, depth merging operates on already reconstructed canonical event indexes.

These benchmarks measure end-to-end production depth merging, including partition planning, chronological event merging, reconstruction boundary handling, Parquet persistence, physical validation, manifest generation, and transactional commits.

The results demonstrate that MarketForge can merge hundreds of millions of historical depth events at sustained throughput exceeding **15 million events per second** on the tested hardware.