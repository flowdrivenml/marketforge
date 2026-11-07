# Task 03 — Shared Normalization Test Infrastructure

Status: completed. Commit: `f25a907`.

Implementation: `engine/tests/historical/support/{fixtures,runtime,assertions}.rs` loads the sole maintained batch through production parsing, selects structured exact identities/feeds/filenames with explicit expected cardinality, serializes isolated subsets, and resolves through the production loader. No test-specific normalization rules or policies. Conservative in-memory resource defaults and explicit overrides preserve general.toml. Every dataset/staging/scratch path is redirected centrally.

Persistent runs use exclusive directory creation with nanosecond timestamp, PID and atomic sequence plus collision retry. Existing ancestor symlinks and escaping paths are rejected. No cleanup Drop. Atomic test.json replacement records timestamps, status, errors, original sources, selected tasks, effective runtime, resolved profile/catalog provenance, job/dataset paths and results. Assertions check identities, inventory, actual Parquet row counts/file sizes, provenance snapshots and depth auxiliary artifacts.

Files: shared support modules and main.rs tests; Cargo real-test feature and target; narrow .gitignore exceptions; normalization-tests.md; progress and Task 02 hash update.

Verification: 8 tests passed with historical-real-tests (6 source-only harness tests and 2 explicitly selected bounded real representatives); historical cargo check passed; changed-file rustfmt passed. Whole-project formatting continues to fail on unrelated pre-existing live files. Tests verify missing/ambiguous selection, missing-source diagnostics, retained failed metadata, concurrent unique directories, multi-job redirection and runtime overrides.

Real cases: Bybit spot BTCUSDT trade source `BTCUSDT_2026-10-27.csv.gz`, header plus first two records: 2 persisted trades, including independently inspected price 80143.4/base quantity 0.00016. Bybit spot depth source `2026-10-27_BTCUSDT_ob200.data.zip`, initial snapshot plus next event: 420 canonical levels, including independently inspected bid 80143.3/base quantity 0.744607. Sampling uses production source reading and new complete wrappers; compressed bytes are never truncated. Repeated runs retain distinct datasets, manifests, processing-job snapshots, depth events/segments/boundary and metadata under data/tests/historical/normalize. Source files and authoritative fixture were not modified.

Limitations: broad old test setup still has private-job and automatic-cleanup dependencies (~90 references found), to migrate under Task 06. No full corpus, month-long processing or benchmarks. No new production behavior. Future Forge changes remain those documented in Task 02.
