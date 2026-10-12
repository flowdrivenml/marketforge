
**Status:** Deferred until later development.

### Benchmark Implementation

Planned file:

`benchmarks/merge_benchmark.py`

A Python benchmark orchestrator will execute the production Rust merge engine and collect performance metrics.

### Workloads

| Mode       | Datasets | Available canonical rows |
| ---------- | -------: | -----------------------: |
| Trade-only |       21 |               71,874,226 |
| Depth-only |       15 |              815,146,563 |
| Combined   |       36 |              887,020,789 |

Exclude test dataset `900001` to avoid potentially overlapping source data.

Canonical depth rows are not equivalent to merged timeline events.

### Configuration

| Parameter | Value |
|---|---|
| Workers | 4, 8, 16, 24, 32 |
| Repetitions | 2 |
| Partitions per worker | 4 |
| Parquet row-group target | 128 MiB |
| Parquet file target | 512 MiB |
| Compression | ZSTD level 3 |
| Memory budget | 60 GiB |
| Maximum open files | 1,024 |
| Physical validation | Enabled |
| Canonical reference validation | Enabled |
| Transactional commits | Enabled |

### Implementation Requirements

The orchestrator should:

- Execute the production `marketforge-merge` release binary.
- Generate isolated merge jobs with unique output directories.
- Preserve identical input datasets and timestamp ranges across worker configurations.
- Measure wall-clock execution time, throughput, peak RSS, and output size.
- Record trade events, depth events, partitions, and Parquet files.
- Verify manifest integrity and successful transactional publication.
- Export benchmark results to CSV and JSON.
- Calculate speedup and parallel efficiency relative to four workers.
- Support a single-run preflight before executing the complete benchmark matrix.
- Support fixed-partition benchmarks to distinguish worker scaling from partitioning overhead.

### Benchmark Execution

Three workloads × five worker configurations × two repetitions:

**30 total benchmark executions.**

The existing depth-only CLI uses manifest v1, while trade-only and combined merging use the unified v2 implementation. Results must distinguish these execution paths.

### Reporting

Produce a performance report consistent with the existing trade and depth processing benchmarks, including hardware specifications, workload characteristics, scheduler scaling, resource consumption, and conclusions.

**Implementation and execution are postponed.**