## Concept

Use **Optuna**, a Python hyperparameter optimization framework, to automatically discover optimal MarketForge processing configurations for different hardware and workloads.

Instead of manually benchmarking worker counts, memory budgets, and Parquet settings, Optuna would execute controlled benchmarks and identify configurations that maximize processing efficiency.

The optimizer would leverage MarketForge's existing Rust benchmark infrastructure.

## Optimization Parameters

| Parameter | Description |
| --------- | ----------- |
| Worker count | Number of concurrent processing tasks |
| Parquet row-group size | Target size of Parquet row groups |
| Parquet file size | Target size of output files |
| Memory allocation | Resource reservation and admission settings |
| Task scheduling | Scheduling strategy and workload distribution |
| Compression settings | Parquet compression algorithm and level, if configurable |

Optimization should consider three workload categories:

- **Trades:** Historical tick-level trade processing.
- **Depth:** Order-book reconstruction and normalization.
- **Combined:** Concurrent trade and depth processing.

Different workloads may require different optimal configurations.

## Optimization Objectives

The primary objective is maximizing end-to-end processing throughput.

Secondary objectives include minimizing:

- Peak memory consumption.
- Temporary disk usage.
- Processing latency.
- CPU and I/O contention.

Optuna could support multi-objective optimization to identify Pareto-optimal configurations balancing throughput and resource consumption.

For example:

```text
Optuna
   │
   ▼
Generate processing configuration
   │
   ▼
Execute MarketForge benchmark
   │
   ▼
Collect performance metrics
   │
   ▼
Evaluate objective functions
   │
   ▼
Select next configuration
   │
   ▼
Optimal configurations
```

## Implementation

Extend the existing Python orchestration layer with an optional optimization workflow.

Potential CLI:

```bash
marketforge optimize --dataset trades
marketforge optimize --dataset depth
marketforge optimize --dataset all
```

The optimizer would:

1. Generate candidate configurations using Optuna.
2. Execute the existing Rust benchmark binary.
3. Collect throughput, execution time, and resource metrics.
4. Evaluate configurations across repeated runs.
5. Save the best-performing configurations for future processing jobs.

Results could be stored in:

```text
benchmarks/
├── results/
└── optimization/
    ├── trades/
    ├── depth/
    └── combined/
```

Optimization studies should preserve hardware specifications, dataset definitions, software versions, and benchmark settings to ensure reproducibility.

## Considerations

- Use representative workloads rather than optimizing exclusively for one exchange or archive format.
- Run repeated measurements to reduce benchmark noise.
- Include warm-up runs when appropriate.
- Monitor actual memory consumption rather than relying exclusively on estimated reservations.
- Reject configurations exceeding resource constraints.
- Avoid excessive optimization trials that provide diminishing returns.
- Reuse existing benchmark configurations as initial Optuna trials.
- Validate the selected configuration on workloads excluded from optimization to reduce overfitting.

## Expected Benefits

Automated configuration optimization would make MarketForge adaptable to different hardware environments without requiring users to manually tune processing parameters.

For example, the optimal configuration on a 16-core Ryzen workstation may differ substantially from a server with 64 cores, slower storage, or limited RAM.

**Priority:** Low — future optimization.

**Dependencies:** Stable benchmark infrastructure, configurable processing parameters, and reliable resource measurements.

**Status:** Idea; not implemented.