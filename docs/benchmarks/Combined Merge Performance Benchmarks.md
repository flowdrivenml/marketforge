# MarketForge — Combined Merge Performance Benchmarks

Performance benchmarks of MarketForge's Rust engine for unified historical tick-level trade and order-book depth merging across multiple cryptocurrency exchanges and instruments.

## Quick Navigation

- [Hardware](#hardware)
- [Merge Configuration](#merge-configuration)
- [Combined Dataset](#combined-dataset)
- [Parallel Merge Performance](#parallel-merge-performance)
- [Scaling Efficiency](#scaling-efficiency)
- [Merge Validation](#merge-validation)
- [Timestamp Regression Handling](#timestamp-regression-handling)
- [Resource Management](#resource-management)
- [Comparison With Individual Merge Benchmarks](#comparison-with-individual-merge-benchmarks)
- [Conclusion](#conclusion)

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

| Parameter                | Value                        |
| ------------------------ | ---------------------------- |
| Execution                | Unified parallel merging     |
| Merge mode               | Combined trades + depth      |
| Worker configurations    | 4, 8, 16, 24, 32             |
| Repetitions              | 2 per configuration          |
| Partitions per worker    | 4                            |
| Parquet row-group target | 128 MiB                      |
| Parquet file target      | 512 MiB                      |
| Compression              | ZSTD level 3                 |
| Physical validation      | Enabled                      |
| Canonical validation     | Enabled                      |
| Timestamp regression     | Drop                         |
| Memory budget            | 60 GiB                       |
| Maximum open files       | 1,024                        |
| Transactional commits    | Enabled                      |
| Timeline schema          | Unified v2                   |

Memory and file budgets use resource-aware scheduling rather than strict operating-system limits.

The benchmark measures the complete production unified merge operation, including:

- Canonical dataset discovery and metadata resolution.
- Trade and depth source inventory construction.
- Reconstruction boundary reconciliation.
- Timestamp-range partition planning.
- Parallel chronological merging of heterogeneous event streams.
- Deterministic trade and depth event ordering.
- Redundant depth initialization handling.
- Timestamp regression filtering.
- Unified Arrow and Parquet writing.
- Physical output validation.
- Canonical source-reference validation.
- Manifest generation.
- Transactional dataset publication.

Unlike raw processing, merging operates on already normalized canonical Parquet datasets.

Archive decompression, raw-data parsing, trade normalization, order-book reconstruction, and snapshot differencing are excluded.

## Combined Dataset

| Metric                   | Value                       |
| ------------------------ | --------------------------- |
| Exchanges                | Binance, Bitget, Bybit, Gate.io, OKX |
| Trade datasets           | 21                          |
| Depth datasets           | 15                          |
| Total input datasets     | 36                          |
| Available canonical trades | 71,874,226                |
| Available canonical depth rows | 815,146,563            |
| Merged trade events      | 50,106,717                  |
| Merged depth events      | 405,581,426                 |
| Total merged events      | 455,688,143                 |
| Benchmark interval       | September 1–4, 2026 (UTC)   |
| Benchmark sessions       | 10                          |
| Failed executions        | 0                           |
| Manifest version         | 2                           |

The benchmark uses real historical market data from five cryptocurrency exchanges.

Input datasets cover spot, perpetual linear, perpetual inverse, and dated futures instruments.

### Dataset Composition

The 36 canonical input datasets contain:

| Content type | Datasets | Available canonical rows |
| ------------ | -------- | ------------------------ |
| Trades       | 21       | 71,874,226               |
| Depth        | 15       | 815,146,563              |
| **Total**    | **36**   | **887,020,789**          |

Canonical depth rows and logical depth events represent different quantities.

A depth event may reference multiple canonical order-book level changes, while some events represent state transitions without materialized canonical changes.

The benchmark produces the following logical timeline:

| Event type | Events | Share |
| ---------- | ------ | ----- |
| Trades     | 50,106,717 | 11.00% |
| Depth      | 405,581,426 | 89.00% |
| **Total**  | **455,688,143** | **100%** |

The merged timeline is dominated by depth events.

Every worker configuration processes the same canonical datasets and timestamp interval.

### Unified Timeline

MarketForge combines heterogeneous market events into a common chronological representation.

Each merged event retains its original source identity and canonical reference metadata.

The unified timeline supports:

- Cross-exchange chronological analysis.
- Trade and order-book event alignment.
- Market microstructure research.
- Cross-market liquidity analysis.
- Trade-to-depth event relationships.
- Price discovery and lead-lag studies.

The merge engine does not duplicate or renormalize the original canonical datasets.

Instead, it produces a chronological event timeline referencing existing canonical records.

## Parallel Merge Performance

| Workers | Partitions | Average time | Throughput | Speedup vs. 4 workers |
| ------- | ---------- | ------------ | ---------- | --------------------- |
| 4       | 16         | 69.845 s     | 6.52M events/s | 1.00× |
| 8       | 32         | 54.813 s     | 8.31M events/s | 1.27× |
| **16**  | **64**     | **48.394 s** | **9.42M events/s** | **1.44×** |
| 24      | 96         | 49.539 s     | 9.20M events/s | 1.41× |
| 32      | 128        | 50.531 s     | 9.02M events/s | 1.38× |

Results represent two repetitions per worker configuration.

Throughput is calculated using the total merged event count divided by the total execution time across both repetitions.

The **16-worker configuration achieved the highest measured throughput**, processing approximately 9.42 million combined trade and depth events per second.

Increasing concurrency from 4 to 8 workers improved throughput by approximately 27%.

Increasing concurrency from 4 to 16 workers improved throughput by approximately 44%.

Increasing concurrency beyond 16 workers reduced throughput.

### Individual Benchmark Runs

| Workers | Repetition 1 | Repetition 2 | Average |
| ------- | ------------ | ------------ | ------- |
| 4       | 69.756 s     | 69.933 s     | 69.845 s |
| 8       | 54.655 s     | 54.971 s     | 54.813 s |
| 16      | 48.259 s     | 48.529 s     | 48.394 s |
| 24      | 49.400 s     | 49.678 s     | 49.539 s |
| 32      | 50.484 s     | 50.577 s     | 50.531 s |

The benchmark alternates worker execution order between repetitions to reduce systematic cache and execution-order bias.

All ten executions completed successfully.

### Performance Stability

| Workers | Time difference | Relative difference |
| ------- | --------------- | ------------------- |
| 4       | 0.177 s         | 0.25%               |
| 8       | 0.316 s         | 0.58%               |
| 16      | 0.270 s         | 0.56%               |
| 24      | 0.278 s         | 0.56%               |
| 32      | 0.093 s         | 0.18%               |

Relative differences are calculated against the average execution time for each worker configuration.

The small differences indicate consistent execution times across the two repetitions.

However, two repetitions are insufficient to establish detailed statistical confidence intervals.

## Scaling Efficiency

| Workers | Speedup | Parallel efficiency |
| ------- | ------- | ------------------- |
| 4       | 1.00×   | 100.0%              |
| 8       | 1.27×   | 63.7%               |
| 16      | 1.44×   | 36.1%               |
| 24      | 1.41×   | 23.5%               |
| 32      | 1.38×   | 17.3%               |

Parallel efficiency is measured relative to the four-worker baseline.

The results demonstrate meaningful scaling up to 16 workers, followed by diminishing returns.

The combined workload introduces additional complexity compared with homogeneous trade-only or depth-only merging.

Potential limiting factors include:

- Heterogeneous event dispatch.
- Trade and depth source cursor coordination.
- Parquet decoding and encoding throughput.
- Memory bandwidth.
- Storage bandwidth.
- Partition initialization overhead.
- Reconstruction metadata access.
- Canonical reference validation.
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

A future fixed-partition benchmark could separate worker concurrency from partition overhead.

## Merge Validation

All benchmark executions completed successfully.

| Metric                  | Result      |
| ----------------------- | ----------- |
| Trade datasets          | 21          |
| Depth datasets          | 15          |
| Total input datasets    | 36          |
| Benchmark sessions      | 10          |
| Successful executions   | 10          |
| Failed executions       | 0           |
| Events per execution    | 455,688,143 |
| Event count consistency | Verified    |
| Manifest verification   | Passed      |
| Transactional commits   | Successful  |

Every execution produced exactly **455,688,143 merged events**, independently of worker count and partition configuration.

This verifies aggregate event-count consistency across the tested configurations.

### Event Count Reconciliation

The combined benchmark's total event count matches the sum of the individual merge benchmarks:

| Benchmark | Events |
| --------- | ------ |
| Trade-only merge | 50,106,717 |
| Depth-only merge | 405,581,426 |
| **Combined merge** | **455,688,143** |

The equality provides an additional aggregate consistency check.

It does not independently establish that every individual canonical reference is identical across all three benchmark outputs.

### Physical Output Validation

The unified merge executor validates committed output structure and physical Parquet contents.

Validation includes:

- Job identity.
- Dataset and stream identities.
- Event type consistency.
- Total event counts.
- Trade and depth event counts.
- Parquet file inventory.
- File existence and size.
- Partition timestamp boundaries.
- Chronological event ordering.
- Canonical reference metadata.
- Safe manifest-relative file paths.

The unified physical validator accepts depth events that legitimately reference zero canonical rows.

Trade events remain required to reference exactly one canonical row.

This distinction is essential because depth events may represent source updates or reconstruction state transitions without producing materialized order-book level changes.

### Canonical Reference Integrity

MarketForge validates references against the original canonical source inventories.

The validator checks:

- Dataset and source task identities.
- Trade/depth event-kind consistency.
- Canonical row bounds.
- Event ordinal progression.
- Canonical row offset progression.
- Overlapping canonical row references.

Dropped or intentionally skipped depth events may create gaps in the retained source-reference sequence.

Such gaps do not inherently indicate invalid canonical data.

The original canonical datasets remain unchanged.

### Deterministic Ordering

MarketForge orders unified events using:

```text
event_timestamp_ns
    → stream_rank
    → task_id
    → event_ordinal
```

This provides deterministic tie-breaking when events from different exchanges or instruments share identical timestamps.

The benchmark confirms identical aggregate event counts across worker configurations.

It does not independently establish byte-for-byte or row-by-row output equivalence between all benchmark executions.

Sequential and parallel equivalence are covered by separate integration tests.

## Timestamp Regression Handling

The combined merge uses the same partitioned depth stream implementation as the depth-only merge.

MarketForge applies the default **drop policy** when a depth event timestamp regresses relative to the previously accepted event within its source partition stream.

The policy:

- Preserves chronological ordering.
- Excludes regressing depth events.
- Does not modify timestamps.
- Does not rewrite canonical datasets.
- Preserves original source references for retained events.
- Maintains source reconstruction metadata.

### Gate.io Regression Investigation

During depth benchmark preparation, MarketForge identified timestamp disorder in one Gate.io dataset.

| Metric                     | Result                    |
| -------------------------- | ------------------------- |
| Dataset                    | 96                        |
| Exchange                   | Gate.io                   |
| Instrument                 | BTC_USDT linear perpetual |
| Source tasks               | 72                        |
| Affected tasks             | 26                        |
| Detected regressions       | 35                        |
| Maximum timestamp lateness | 100 ms                    |
| Other depth datasets       | No within-task regressions |

The regressions occurred within individual canonical depth event indexes.

A separate production depth merge verified that exactly 35 events were dropped and recorded in the depth-only merge manifest.

The combined benchmark successfully processed the same canonical depth dataset without encountering chronological validation failures.

However, dropped-event accounting has not yet been propagated into the unified v2 manifest, so the combined benchmark's per-execution dropped-event counts were not independently reported.

### Reconstruction Semantics

Dropping a depth event reference does not undo its effect on the already reconstructed canonical order book.

Consequently, a filtered unified timeline should not automatically be treated as a complete standalone reconstruction history.

Consumers requiring exact order-book replay must respect canonical reconstruction metadata, source sequence semantics, and any intentionally omitted timeline events.

## Resource Management

MarketForge uses resource-aware partition scheduling.

| Resource                   | Implementation              |
| -------------------------- | --------------------------- |
| Worker concurrency         | Configured and enforced     |
| Estimated memory budget    | Reservation-based admission |
| Maximum open files         | Configured budget           |
| Actual process RAM limit   | Not strictly enforced       |
| Transactional publication  | Enabled                     |
| Physical output validation | Enabled                     |
| Canonical validation       | Enabled                     |

The benchmark uses a configured memory budget of 60 GiB.

Resource reservations influence partition admission, but the configured budget is not a hard operating-system memory limit.

Peak resident memory and CPU utilization were not recorded for every worker configuration in the supplied benchmark results.

Consequently, worker-dependent memory scaling cannot yet be evaluated.

### Benchmark Report

The complete benchmark results are stored at:

```text
benchmarks/results/combined-merge-v2/benchmark.json
```

Successful benchmark outputs are removed after validation to avoid unnecessary disk accumulation.

Failed outputs are preserved for debugging.

## Comparison With Individual Merge Benchmarks

MarketForge completed three independent merge benchmark workloads using the same hardware and worker configurations.

### Peak Performance

| Benchmark | Datasets | Merged events | Best workers | Peak throughput |
| --------- | -------- | ------------- | ------------ | --------------- |
| Trade-only | 21 | 50,106,717 | 16 | 10.26M/s |
| Depth-only | 15 | 405,581,426 | 16 | 15.33M/s |
| **Combined** | **36** | **455,688,143** | **16** | **9.42M/s** |

All three workloads achieved their highest measured throughput using 16 workers.

### Worker Scaling Comparison

| Workers | Trade merge | Depth merge | Combined merge |
| ------- | ----------- | ----------- | -------------- |
| 4       | 8.65M/s     | 8.83M/s     | 6.52M/s        |
| 8       | 10.01M/s    | 12.56M/s    | 8.31M/s        |
| 16      | 10.26M/s    | 15.33M/s    | 9.42M/s        |
| 24      | 9.78M/s     | 14.68M/s    | 9.20M/s        |
| 32      | 9.23M/s     | 14.07M/s    | 9.02M/s        |

Throughput values represent logical merged events per second.

The combined workload achieves lower event throughput than either homogeneous workload.

This is consistent with the additional work involved in merging heterogeneous event types and validating their canonical references, although the benchmark does not isolate the precise causes.

### Scaling Comparison

| Benchmark | 4-worker throughput | 16-worker throughput | Speedup |
| --------- | ------------------- | -------------------- | ------- |
| Trade-only | 8.65M/s | 10.26M/s | 1.19× |
| Depth-only | 8.83M/s | 15.33M/s | 1.74× |
| Combined | 6.52M/s | 9.42M/s | 1.44× |

Depth-only merging demonstrated the strongest scaling relative to four workers.

Trade-only merging showed the smallest relative improvement, suggesting that additional concurrency provides limited benefits for its comparatively lightweight workload.

Combined merging achieved intermediate scaling, with throughput increasing by approximately 44% between four and sixteen workers.

These differences should not be interpreted as isolated measurements of event-type processing costs because the workloads differ in size, source characteristics, and validation requirements.

## Conclusion

MarketForge achieved **9.42 million combined trade and depth events per second** using 16 workers on an AMD Ryzen 9 9950X.

The benchmark merged **455,688,143 real market events from 36 canonical datasets across five cryptocurrency exchanges**.

All ten benchmark executions completed successfully, producing identical aggregate event counts and validated transactional commits.

Compared with four workers, the 16-worker configuration delivered:

- **1.44× higher throughput**
- **30.7% lower execution time**
- **9.42 million merged events per second**

For the tested combined workload, **16 workers provided the best measured performance** among the evaluated configurations.

Performance declined slightly at 24 and 32 workers, indicating diminishing returns from additional concurrency under the current partitioning strategy.

Across all three completed merge benchmarks, the 16-worker configuration consistently achieved the highest throughput:

- Trade-only: **10.26M events/s**
- Depth-only: **15.33M events/s**
- Combined: **9.42M events/s**
