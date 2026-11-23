# Performance

Measure one workload and one processing boundary at a time. Separate correctness, compatibility, wall time, and resource consumption in the results. Start from the application's effective runtime settings and actual input inventory.

## Inspect before benchmarking

Use normalization's runtime diagnostics and processing counters to identify phases, tasks, resource admission and integrity observations. Use dataset metadata for rows, row groups and physical bytes. Source compressed bytes, decoded rows, canonical rows and logical events measure different work. A preview limit constrains the returned rows, while Parquet page and batch reads can decode more. [Runtime policy](../reference/runtime.md) · [Python dataset API](../reference/python-datasets.md)

When comparing an apparent regression, establish whether the difference came from download, source decoding, processing, publish validation or registry work. A timer spanning the entire Forge command includes costs outside the Engine. A profile of one process can miss worker admission, storage or other processes. Keep the measured window explicit.

## Use the normalization benchmark harness

The repository contains `marketforge-normalize-benchmark`. Its CLI accepts an explicit prepared batch, configuration root, worker counts, scheduler, repetitions, warmups, and retention policy. Build the benchmark target in release mode. The example below plans the maintained workload. It requires the referenced inputs, installed profiles and the historical runtime configuration. Planning does not run the trials.

```bash
cargo build --locked --manifest-path engine/Cargo.toml \
  --release --features historical --bin marketforge-normalize-benchmark
engine/target/release/marketforge-normalize-benchmark \
  --input engine/tests/historical/fixtures/normalize.json --project-root . --dry-run
```

A real run requires an explicitly supplied workload and authorization under the repository's benchmark guidance. Use an isolated output under `data/benchmarks/historical/`. Preserve the plan, report and retained diagnostics. The target directory in the command is Cargo's default; use the actual binary location if you override it.

For already collected runs, use the harness's `summarize --run PATH` and `compare --baseline PATH --candidate PATH` subcommands. Review the reported comparability before interpreting ratios. The harness records trial status and available process metrics; inspect missing metrics, failed trials and warmups before aggregating. [Benchmark argument model](https://github.com/flowdrivenml/marketforge/blob/main/engine/src/historical/normalize/benchmark/model.rs) · [Benchmark runner](https://github.com/flowdrivenml/marketforge/blob/main/engine/src/historical/normalize/benchmark/runner.rs)

## Record enough context to reproduce a comparison

| Record | Why it matters |
| --- | --- |
| Revision, build profile, features and executable checksum | Debug/release or feature differences can dominate one code change |
| Input fixture and fingerprints, content type, bytes, tasks and logical events | Workload changes can change both cost and correctness |
| Effective workers, memory, scratch, file/row-group targets and scheduler | Requested settings can be constrained by actual admission and runtime configuration |
| Machine, storage, OS, cache conditions and competing load | CPU, physical IO and page cache affect repeated runs |
| Time window, repetition order, warmups and failures | A single fastest run, censored timeout or mixed set of phases can mislead |
| Correctness check and expected output | A faster execution can result from omitted work or changed semantics |

Separate measured RSS from Engine-accounted memory. Treat missing process metrics as unknown. Compare event or row counts and independently understand any intentional output difference before accepting a throughput claim. The [historical verifier](https://github.com/flowdrivenml/marketforge/blob/main/engine/scripts/README.md) includes synthetic and dry-run benchmark contract tests. Those tests establish harness behavior. Record the input, executable revision, and resource settings alongside any measured result.
