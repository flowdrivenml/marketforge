# Normalization performance diagnostics and implementation report

The implementation reduces repeated spool work and adds timer-driven diagnostics to job execution, session/CLI execution, the multi-archive fixture test, and benchmark output verification. No completed real-archive baseline or speedup was measured during implementation. No resource or integrity limits were raised.

## Runtime controls

`data/.config/historical/general.toml` remains authoritative. Existing schema-version-1 files remain valid. The new execution-only fields never enter normalization job JSON:

```toml
schema_version = 1
merge_fan_in = 8                # 2..64; requested upper bound
scratch_format = "json"        # json | typed_trades
scratch_codec = "zlib"         # zlib | uncompressed

[diagnostics]
mode = "disabled"              # disabled | basic | detailed
interval_ms = 1000              # 100..60000
```

Keep these top-level fields above TOML table headers. Existing resource and integrity sections remain unchanged. Typed trade payloads and uncompressed scratch are opt-in; they have correctness coverage but no completed representative performance comparison. Family blocks use at most `min(reconciliation_bytes / 16, 256 KiB)` across all symbols, plus one bounded serialization buffer and fixed metadata accounted against the same parent quota. Oversized individual records bypass block accumulation within the enforced frame limit.

Effective fan-in is reduced for file admission and for the memory remaining after output/codec headroom. Run generation observes the maximum generic decoded-record reservation and uses that conservative bound for merge cursors. Two-way merging remains the fallback. Resource estimates reserve cursor, output and source/Parquet handle headroom together. Parallel sorting borrows at most one spare session compute permit, coordinated with `resources.workers`; sources and stateful depth replay remain sequential within each stream.

## Retained artifacts and interpretation

Enabled diagnostics create a unique `performance-*` directory beside a job's staging path. The fixture test instead owns this directory directly under its isolated test run. The directory contains:

- `effective-config.json`: resolved runtime settings.
- `live.jsonl`: versioned, coalesced snapshots during execution, including timer heartbeats when no records arrive.
- `performance.json`: the final snapshot, marked committed or incomplete/failed.

The stream records workload job fingerprints and source identifiers, software version/build metadata, task counters and last-progress timestamps, active job/stream/task phases, phase start timestamps, completed inclusive phase durations, process CPU and logical/physical I/O, process peak RSS, scheduler queue occupancy, admission wait wall time, and reconciliation memory/scratch/handle and work counters. Detailed mode additionally probes current Linux RSS once per snapshot; unsupported measurements stay null. Basic mode avoids that probe. Snapshots and progress counters are batched; no per-trade process probes, directory scans, output flushes, or event queues are introduced.

Source normalization completion, reconciliation completion, staged output and dataset commit have separate labels. Source counters do not represent committed output. Spool frame counters include intermediate sort traffic; they are not source record counts. `uncompressed_bytes_*` count frame payloads; `codec_input_bytes` includes framing and stream markers. Disk-write counters measure calls and bytes accepted by file I/O, not physical device writes. Process I/O counters are separate and may include unrelated concurrent activity in that process.

Memory quota usage is accounted retained allocation, not measured RSS. Scratch peaks are workspace peaks, not sums of the shared scratch pool's peak. Phase durations are inclusive wall times and can overlap. Do not sum them to manufacture CPU percentages. The fixture records engine wall time and output-verification wall time separately. Benchmark verification runs in its own diagnostic interval; existing benchmark execution/process measurements remain separate.

Workers update shared snapshots and send only a nonblocking, one-slot wake notification. A stalled consumer coalesces transitions; phase totals retain completed phases. Diagnostic I/O stays on the reporting thread. Optional diagnostic-output errors issue bounded warnings and disable affected output; they do not change integrity decisions or replace mandatory processing/failure reports. Ordinary return failures and unwinding mark diagnostics incomplete. SIGKILL, abort, and other uncatchable terminations can leave only a running live stream. No library signal handlers are installed.

Successful datasets also retain `processing-runtime.json` and its checksum as runtime provenance, separate from the job definition. Live diagnostic files are outside canonical data and scratch cleanup.

## Implemented work reductions

| Area | Change and deterministic evidence |
| --- | --- |
| JSON spool serialization | Two serializer traversals become one; a counting serializer test asserts one invocation. The reusable buffer reserves before growth and retains an independent capacity lease. |
| Codec calls | Serializer fragments no longer enter the codec individually. A frame uses a combined metadata header and its existing payload bytes. |
| Reader allocation | Readers reuse an independently charged payload buffer while decoded record leases remain owned by returned records. |
| Scratch locking | RAII reservations include in-flight bytes, release the mutex during file I/O, and release unused capacity on short writes, errors and unwinding. |
| Family routing | Selection is prepared once per group; admitted symbols use binary lookup; immutable filtered-observation scope is cached. Shared blocks batch interleaved symbols before opening a compressor. |
| Family deterministic test | 1,000 alternating rows in two symbols use two data members and two seal members, rather than one data member per row. Five cycling symbols in 100 rows use five data compressors, not 100. |
| Metrics | Existing last-window entries update without allocating identical scope/path keys; regressions and older windows retain the existing checked fallback. Overflow updates remain atomic. |
| Source attribution | Publication looks up a precomputed source index instead of scanning executions per trade. |
| External merges | Both logical sorts use deterministic heap-based k-way merging with bounded, disk-backed inventories. Odd runs rename without any scratch writes. |
| Final chronological merge | When the last run group fits, checked cursors feed Parquet directly, avoiding a complete final-spool write and reread. Late errors prevent commit. |
| Typed trade scratch | Explicit version/type tags, little-endian widths, bounded lengths, decimal mantissa/scale/sign, options and enum tags. The complete-field test asserts a smaller payload than JSON for its synthetic trade. |
| Compute | Large run sorting can split canonical references across parent and one spare worker, then combine the sorted halves into one run. Quota leases and scratch handles stay on the parent. |

Source precedence, native identity domains, physical source ordinals, repeated IDs in the preferred source, chronological validation, suppression accounting and source attribution remain covered by the reference oracle and independent expected-result tests. Ambiguous identities still fail. Depth events remain complete, including snapshots, zero-change events, sequences, recovery segments and fingerprints. Canonical schemas and transactional publication are unchanged.

The typed implementation currently covers trades. Complete depth events and dynamic family-routing values deliberately retain bounded JSON frames; this is a remaining format optimization, not a generic binary-Serde assumption. No new dependencies, archive-date identity heuristics or independent thread pools were added.

## Validation and commands

Focused synthetic normalization tests (103), runtime configuration tests (9), and tiny synthetic CLI contract tests (9, including diagnostics with clean JSON stdout) passed. The release integration target was compiled with `historical-real-tests`, without executing it. Existing unrelated compiler warnings remain. The implementation report in the delivery message records the final test counts.

From the repository root, compile without running archives:

```sh
cargo test --release --manifest-path engine/Cargo.toml --features historical-real-tests --test historical_normalize --no-run
```

Run exactly the fixture-driven multi-archive workload, with diagnostics and no TUI:

```sh
MARKETFORGE_TEST_DIAGNOSTICS=detailed cargo test --release --manifest-path engine/Cargo.toml --features historical-real-tests --test historical_normalize process_executor::processes_multiple_trade_tasks_into_staging -- --exact --nocapture
```

This environment variable is an explicit diagnostic-mode override in this test only. Resources still come from `general.toml`, with isolated scratch/output locations. The fixture supplies archive selection, priorities and task IDs. A native-identity rejection remains a failed run, with retained diagnostics; it is not a performance baseline.

Find reports without deleting or modifying retained artifacts:

```sh
rg --files --hidden --no-ignore data/tests/historical/normalize/execution/real_multi_archive_trades -g performance.json -g live.jsonl -g effective-config.json
jq '{status, resources, process_wide, phase_completed_inclusive_ms}' /absolute/path/from/above/performance.json
tail -f /absolute/path/from/above/live.jsonl
```

Run bounded synthetic verification:

```sh
cargo test --manifest-path engine/Cargo.toml --features historical --lib historical::normalize
cargo test --manifest-path engine/Cargo.toml --features historical --lib shared::historical::config
```

For before/after comparisons, use the same fixture and source fingerprints, release build profile, effective resource settings, diagnostic mode/interval, verification mode and declared cache policy. Keep each isolated run and record its build revision plus local patch state. Declare warm OS cache unless an independently controlled cold-cache setup is available; do not stop processes or clear user caches. Compare only successful, fully verified three-archive runs. Compare engine wall time separately from verification time and inspect merge passes, serialization calls, codec initializations, scratch traffic and accounted memory before interpreting throughput. No full workload or benchmark sweep was automatically launched here.

Remaining performance questions require that controlled run: real source normalization versus reconciliation cost, typed/compressed versus typed/raw scratch, useful fan-in on the actual record distribution, and the benefit of spare-worker sorting relative to serialization/compression. Parallel source decoding, parallel compression and a typed depth format have not been enabled speculatively.

## Files changed for this task

The repository already contained substantial uncommitted normalization changes; the following identifies task edits, rather than attributing the entire working-tree diff to this work:

- `engine/src/shared/historical/config.rs` and `config/tests.rs`: execution-only tuning, defaults, validation and serialization tests.
- `data/.config/historical/general.toml`: documented controls; existing resource and integrity values/comments retained. This local data configuration is ignored by Git in this checkout.
- `engine/src/historical/normalize/progress/{mod.rs,reporter.rs,diagnostics.rs}`: live coalesced phases/counters, optional reporting thread and retained artifacts.
- `engine/src/historical/normalize/{executor.rs,session.rs,scheduler.rs}`: production wiring, admission waits/queue occupancy, finalization phases and runtime provenance.
- `engine/src/historical/normalize/reconcile/{spool.rs,external.rs,typed.rs}` and `reconcile.rs`: reusable buffers, scratch reservations, family blocks, k-way/final streaming merge, typed trade payloads and quota accessors.
- `engine/src/historical/normalize/{family.rs,reconciled.rs,streams.rs,streams_depth.rs}`: prepared routing, progress propagation, admission estimates, direct publication and bounded source lookup.
- `engine/src/historical/normalize/metrics/windows.rs`: existing-window allocation fast path.
- `engine/src/historical/normalize/resources/{mod.rs,compute.rs,safety_tests.rs}`: shared compute admission and updated file-reservation test.
- `engine/src/historical/normalize/benchmark/runner.rs`: verification phase diagnostics.
- `engine/tests/historical/normalize/{equivalence.rs,execution/process_executor.rs,cli_contract.rs}`: new runtime defaults and explicit test diagnostics with separate engine/verification timing. The tiny CLI fixture has explicit smaller safety bounds matching its synthetic data and existing 1 GiB budget; the real fixture-driven test retains production resource settings.
- This document: usage, retained-report semantics, structural evidence and comparison protocol.
