# Bounded Rust test execution

Run commands from `engine/`. Compile separately, keeping Cargo's JSON artifact output:

```sh
mkdir -p target/refactor-validation
cargo test --locked --offline --features parquet --no-run --message-format=json > target/refactor-validation/parquet-artifacts.jsonl 2> target/refactor-validation/parquet-build.log
python3 scripts/bounded_tests.py target/refactor-validation/parquet-artifacts.jsonl target/refactor-validation/parquet-discovery
python3 scripts/bounded_tests.py target/refactor-validation/parquet-artifacts.jsonl target/refactor-validation/parquet-run-1 --run --targets 'parquet_(decimal|depth_batch|depth_events|depth_segments|depth_writer|trade_batch|writer)'
```

Use a fresh output directory for each execution. `--targets` and `--cases` are full-match regular expressions; use `--cases 'namespace::exact_test_name'` for one case. The default discovery command only lists tests. Review a target's fixture and output requirements before selecting it: some existing Historical tests generate persistent datasets outside temporary directories and must remain excluded from this workflow.

Each selected libtest case gets its own sequential process and 30-second monotonic deadline, including setup. On expiry the runner terminates its process group, waits a 0.2-second grace period, then kills surviving owned processes. Cargo compilation/linking is outside that deadline. The runner preserves the package working directory, inherited environment, Cargo manifest variables, compiler-artifact library directories, and Rust sysroot library path on Linux.

Results contain full identities, ignore status, raw exit codes, elapsed times, and individual logs. Exactly one passed case is required for `PASS`; zero-test and ignored-only invocations cannot pass. Existing ignored tests and network suites are excluded; benchmark execution is excluded. `should_panic` retains libtest behavior. Doctests and non-libtest/custom harnesses need separate supervision and are not run here.

Execution exits nonzero if any discovered case is failed, timed out, blocked, or unexecuted. A selective run therefore normally exits 1 even when every selected case passed. Inspect `results.json`, `results.jsonl`, and `discovery.json`; a non-success result is not suppressed. Empty artifact discovery also returns an explicit incomplete result. Missing fixtures and sandbox denials should be reviewed with the raw failure preserved, rather than inferred from arbitrary assertions.

See [the shared refactor report](../../docs/architecture/SHARED_REFACTOR_REPORT.md) for the exact migration commands, exclusions, identity maps, and recorded results.

The [focused follow-up report](../../docs/architecture/SHARED_REFACTOR_FOLLOWUP_REPORT.md) records the subsequent storage-directory removal and validation under `target/refactor-followup-validation/`.
