# Normalization memory and TUI stabilization

Implemented both workstreams without resetting the working tree, changing the runtime budgets, changing the prepared batch, or running the full production batch. The independent Bitget identity rejection remains enforced. Production-scale RSS and throughput improvements have **not** been measured.

## Starting state and ownership note

Read `.agents/AGENTS.md`, the current changes, `bounded-retained-state-implementation.md`, the admission diagnostics and the retained production log `/tmp/marketforge-normalize-222174-1794774350336911865.log`. The prior report correctly described a partial implementation:

- Depth boundary validation already used a bounded streaming reader, including byte/string/book limits and malformed/truncated envelope rejection. Boundary books were no longer reserved for every completed source. This implementation was preserved.
- Aggregation already prevalidated arithmetic without cloning its full retained scope maps. Borrowed manifests and buffered report serialization were present. These improvements were preserved.
- Detailed `TaskMetrics.scoped_integrity` maps, aggregate maps and family-routing maps still remained resident. Admission still multiplied worst-case detailed observations by six for every task. There was no completed-observation spill lifecycle.
- The Ratatui application is `engine/src/tui/processing.rs`; state, view and tests are in `engine/src/tui/processing/`. Terminal ownership is in `shared/terminal/guard.rs`; the normalization CLI integration is in `src/bin/marketforge-normalize.rs`. The loop already polled/redrew every 100 ms and bounded its legacy event drain.

Ownership decision, made before implementation: input integrity observations become immutable after normalization and family attribution. Output counters, timestamps, suppression decisions, source attribution and task outcomes remain mutable through reconciliation and writer finalization. Persist the former and keep the latter compact. EOF is not publication or task finalization.

## Implemented ownership and storage

`metrics/retained.rs` adds immutable framed observation storage behind `ScopedIntegrityMetrics`. It uses the existing reconciliation spool codec, framing, checksums, footer validation, shared scratch pool and bounded handle machinery. It writes buffered task-level files containing observation frames, not one file per observation. The shared disk reservation precedes writes. A sealed spool transfers to a thread-safe retained owner together with its disk reservation; it survives destruction of its worker-local `Rc` workspace. The last consumer removes it and releases disk capacity. References are keyed by **job and task**, including when task IDs repeat across jobs.

During a long reconciled stream, each source spills before the next source is retained. Family routing spills each completed physical routing group; consuming a routing summary releases its obsolete spill reference. Thus neither scheduler result collections nor a second routing statistics inventory retain equivalent detailed maps in RAM. Per-task bounded diagnostic samples and compact source/output summaries remain resident intentionally.

Reconciliation still amends written, suppressed and unpublished rows, timestamps and outcomes after spilling. Only observation maps are immutable; recording new observations after sealing is rejected. Single-source trade/depth stream paths spill before returning their worker results as well. Both session execution and direct job execution use the production admission model. Direct jobs transfer retained-state and finalization reservations into `ExecutionContext`, keeping them alive through failure serialization; its report data drops before its reservation guards.

Job aggregation retains compact summaries and shared detail references. It does not reconstruct all source maps. Integrity evaluation visits stored file/hour/day observations, retains compact per-stream statistics, and evaluates the **whole job** with the original category ordering, denominators, minimum samples, thresholds, unattributed rules, violation totals and bounded diagnostic sampling. Combining routing and normalization observations materializes at most one task's observations, within its active allowance. Aggregate updates still prevalidate arithmetic before committing. Immediate worker policy checks also prevalidate the merge but copy only their required global statistics, rather than cloning detailed task maps.

Serialization preserves the existing authoritative JSON fields and arrays. Aggregate observation order follows scope/task ordering independently of worker completion; task reports retain prepared-task ordering through the scheduler. Array serializers decode one framed observation at a time and validate the footer and retained frame count. Manifests and failure reports continue using buffered streaming writers. No report-wide JSON `String`, byte vector or `Value` was introduced. Family selection and manifest checks consume the compact task fields they actually need.

## Admission and pressure handling

Queued tasks reserve metadata, bounded diagnostic samples, compact results and reference inventories, rather than maximum detailed observation maps. Active admission additionally covers routing/normalization map overlap and atomic per-task combination, plus a report codec/frame workspace of at least 8 MiB, sized from validated scope identity lengths so the spill codec does not introduce a smaller fixed string limit. Finalization uses its separately admitted workspace after scheduled processing has joined; their lifetimes are exclusive, so the existing maximum is valid. Worker concurrency continues to be constrained by the existing global resource scheduler and worker ceiling.

A spill error leaves authoritative observations resident and preserves the processing error. A bounded failure allowance covers one such source per active worker plus its aggregate copy during failure reporting. A cancellation gate prevents new units from starting normalization after a report persistence failure; already admitted workers can drain without acquiring additional report memory. Scratch/codec failures return immediately rather than waiting while holding resources required to drain. Report storage uses the same effective scratch capacity as reconciliation; file headroom covers its bounded extra handle. No independent full-size scratch allowance or alternate temporary root was added.

Combined processing/report persistence errors preserve their primary typed error and stream context. If writing `failure.json` also fails, the definitive result reports both errors and leaves `failure_report_path` unset. Already sealed observation evidence remains on disk with job/task/path information in diagnostics. A small, coalesced pool owner per scratch root keeps those bytes charged for the rest of the process; this does not retain a queue of detailed reports in memory. A partial JSON file is not claimed to be a complete report.

The nonexecuting example `normalize-admission` loads the authoritative batch and runtime configuration using production resolvers and calls production estimation. It stats source paths but does not execute archive payloads. Saved calculations:

- `normalization-memory-and-tui-admission-before.json`
- `normalization-memory-and-tui-admission-after.json`

Both resolve 107 trade tasks and 257 depth tasks dynamically. Exact reservations, in bytes:

| Component | Before | After |
|---|---:|---:|
| Prepared/compact metadata inventory | 13,571,445 | 13,571,445 |
| Session-wide maximum detailed report copies | 74,423,050,224 | 0 |
| Compact reports, diagnostics and reference inventory | included above | 10,057,508 |
| Bounded failed-spill retention and aggregation allowance | included above | 1,199,570,944 |
| All-source boundary allowance | 0 | 0 |
| Largest active unit, including active report workspaces after refactor | 7,270,368,288 | 7,413,761,152 |
| Finalization workspace, exclusive with active processing | 2,281,201,792 | 2,289,590,400 |
| **Retained state + max(active unit, finalization)** | **81,706,989,957** | **8,636,961,049** |
| Configured memory ceiling | 68,719,476,736 | 68,719,476,736 |

The baseline reproduces the reported rejection exactly at its named execution unit: 74,436,621,669 retained bytes + 3,808,524,120 active-unit bytes = **78,245,145,789**. The larger before total in the table reflects the largest unit across the entire batch, rather than the first rejecting unit. The corresponding unit after implementation is 3,965,024,160 bytes; with retained state its calculated requirement is 5,188,224,057 bytes. This is broader retained/session state plus an active unit, not that OKX stream's measured working set.

The post-change batch admission fits the unchanged 64-GiB ceiling. The 8,636,961,049-byte figure admits retained state and any one execution unit; it is not the maximum simultaneous reservation with several workers. The scheduler may reserve more as units run concurrently, within the unchanged global ceiling. Scratch remains 137,438,953,472 bytes and the file limit remains 256; the largest planned per-unit file reservation is 14, with existing session headroom. Actual scratch capacity and processing constraints can still reject execution. Metadata and compact indexes remain proportional to task/source count. Existing dependency/transient safety allowances are estimates, not proofs of allocator RSS.

The diagnostic's live reservation, peak-accounted usage and measured RSS fields are intentionally null: this helper calculates admission; it does not measure execution. Quota/lifetime regressions exercise live accounting separately. The serialization regression asserts an incremental allocator-tracked workspace peak below 4 MiB. Its oracle and compact inventory are constructed before tracking; this is not a whole-process memory or production throughput measurement.

## TUI findings and corrections

The production log shows reconciliation and replay continuing after source collection. It does not prove that the renderer stopped refreshing during the user's observation. Demonstrated presentation defects were:

1. `WorkerProgress::maybe_update` checked its timer only after another 8,192 input records. That could hide changes to other counters, and could never describe post-input work by itself.
2. Replay and sorting emitted phase/resource telemetry, but the main view did not display those phases or consume their stage counters. Family sources also remained associated with a routing key while concrete output telemetry used a different key. The phase was available only in selected-source details; selection could remain on an already settled unit while another unit worked.
3. Reconciled streams did not emit task-completion events after writer finalization. Their dashboard rows could remain running. Stream-level rejection had no dedicated lifecycle event/reason, and definitive job failure was delayed until final job finalization.
4. Final output summaries printed planned destinations regardless of whether a manifest was published.

The counter sampler now accepts coalesced publication requests from snapshot consumers at their existing timer cadence. Its per-record path checks an atomic flag and does not read the clock or require an input-record delta. Explicit phase instrumentation remains independent of input counters. Routing, source observation persistence, resource admission/waiting, sorting/run generation/merging, trade replay, depth-message replay, Parquet finalization, integrity evaluation, manifest validation and publication expose stages. Available run/pass/replay counts have explicit units; depth source messages are distinct from canonical price-level rows. Unknown totals remain indeterminate.

The main view now follows unfinished execution units independently of stable table selection and shows the phase and monotonic phase duration. Family sources rebind to their concrete output unit before sorting/replay, so concrete-stream telemetry is not hidden behind the parent routing key. Details show the selected unit's stage count and last meaningful activity age, or explicitly state that progress information is unavailable. Existing Unix timestamps remain available for machine diagnostics; elapsed time, phase time, rates and meaningful-update ages use `Instant`.

The TUI constructs a coalesced lifecycle reporter: at most one pending event per task/job/stream identity, with counters in atomic snapshots and phases/stage activity in bounded keyed state. It drains at most 256 events per frame, including when the worker finishes, and polls input/redraws independently. It does not perform normalization, report I/O, directory scans or worker joins on the rendering thread. Shared locks do not span report I/O. A 100,000-event repeated-identity burst does not create a 100,000-event backlog or replace terminal outcomes with delayed starts.

Terminal task counters are emitted after stream output finalization, including late reconciliation and family-attribution changes, even on failure. Canonical sink/spool counters are preserved; a separate staged-output snapshot is populated by Parquet submissions. Normalized canonical rows, staged rows and definitive committed rows have separate display values, so a rejected spool is not labeled as Parquet output. Dedicated stream failures settle affected running sources without inventing failed-input counts. Preflight rejection publishes its job reason promptly. At termination, authoritative job results supersede provisional presentation state. Stopped, not-run and skipped states are distinct. Failed results do not advertise planned paths as published datasets; only definitive successful manifest paths are presented as published. Existing terminal restoration, diagnostic logging and machine-readable CLI output are preserved.

These corrections address demonstrated stale telemetry and lifecycle paths. They do not establish the exact terminal/OS cause of the historical manual observation. A full manual batch remains the appropriate production validation.

## Verification

Final verification records are summarized below; no full production batch or benchmark was launched.

- Production build: `cargo build --release --manifest-path engine/Cargo.toml --bin marketforge-normalize --features "historical,monitor-tui"` — passed.
- `cargo check --manifest-path engine/Cargo.toml --bins --features "historical,monitor-tui"` — passed, including affected normalization, merge and conversion targets.
- `cargo test --release --manifest-path engine/Cargo.toml --features "historical,monitor-tui,normalization-test-hooks" --test normalization_dashboard --test normalization_retained_memory` — 17 passed in normalization_dashboard plus one explicitly ignored PTY harness, and 18 passed in normalization_retained_memory (35 targeted passes total).
- Library tests with `historical,monitor-tui`: metrics 27 passed; resources 13 passed; progress 3 passed; reconciliation spool 10 passed; reconciliation policy 10 passed; family routing 10 passed; failure persistence 1 passed; immediate trade integrity regressions 5 passed. These cover exact thresholds, overflow atomicity, finite capacity, owned reservations, timer/coalescing behavior, disk/handle pressure, codec framing and malformed/truncated spool rejection.
- Required historical integration command: `cargo test --release --manifest-path engine/Cargo.toml --no-default-features --features "historical,historical-real-tests" --test historical_normalize -- --test-threads=2` — 192 passed, one existing ignored test for an OKX inverse-futures source absent from the authoritative batch. The repeat also passed 192 tests with the same one ignored test (290.77 seconds). Final direct-call guard/telemetry refinements were additionally checked by the targeted production-path regressions and builds.
- PTY driver: `python3 .agents/reports/normalization-tui-pty.py` — passed against the actual terminal loop and production execution path, gated during replay. It verifies help input, elapsed redraw with no new records, resize and exact terminal-attribute restoration, plus alternate-screen exit. The explicitly ignored PTY harness is executed separately by this driver, not counted as an ordinary headless pass. The driver interprets differential terminal cursor updates; a raw complete-timestamp search was insufficient during its initial development.
- Regular release Clippy for the normalization binary and targeted tests — completed with 98 existing library warnings. Strict `-D warnings` does not pass repository-wide; the new type-complexity warning was corrected. No unrelated lint repairs were undertaken.
- `cargo fmt --manifest-path engine/Cargo.toml --all -- --check` — blocked by the pre-existing missing configured `engine/src/bin/marketforge-tui.rs`. Explicit `rustfmt --edition 2024 --check` for the Rust files changed here passed. `git diff --check` also encounters pre-existing trailing whitespace in `engine/Cargo.toml`.

Behavioral regressions include 48 sources in one stream with spilled observations and late output amendments; exact whole-job policy equivalence and bounded serialization; job/task ownership isolation; truncated retained-frame rejection and large scope identity compatibility; a two-worker scratch-quota failure that cancels queued normalization and preserves metrics without deadlock; a real overlapping-source rejection with zero failed-input events and a complete spilled failure report; failure-report I/O injection preserving existing framed evidence and the original error; a controlled production replay barrier with responsive rendering/navigation; and byte-identical canonical Parquet output and definitive results between monitored and non-TUI synthetic execution. The PTY pass retained terminal output at `data/tests/historical/normalize/stabilization/pty-1794781712487920007/terminal.log`. Persistent execution artifacts remain under `data/tests/historical/normalize/stabilization/`.

`normalization-test-hooks` is an opt-in test feature containing a phase barrier callback. It is absent from the required production build. Test callbacks execute after state locks are released. It does not add production delays or an environment-controlled sleep to the executable.

## I/O and remaining practical limits

Reports now add task-level scratch files, compression/framing writes and bounded sequential reads. Policy and JSON field-order passes may scan the same task file more than once; each pass holds one frame and a bounded codec workspace rather than a full report. There is a temporary directory/open/close cost per task-level read pass. Routing observations can briefly coexist on disk with their combined normalized report until their old reference is consumed. This is additional I/O, not a claimed throughput improvement.

Scratch exhaustion, malformed data, configured metric/depth limits, genuine active workspace limits and integrity failures remain explicit errors. The Bitget overlap policy is unchanged: no identity contract was added, no overlap comparison changed, no boundary data dropped and no safeguard disabled. The full batch may still fail for that independent reason. Existing temporary-storage cleanup errors are logged. Sealed evidence deliberately survives a failure to write failure.json, and is kept charged for the remaining process lifetime. Scratch accounting is process-owned, not an operating-system-wide filesystem quota across separate invocations.

Verification logs are retained alongside this report with the `normalization-stabilization-` prefix.

No completion commit was created: the checkout contains extensive pre-existing changes, including shared files modified by this task; no unrelated changes were staged or reset.

## Manual production validation

From the repository root:

```bash
cargo build \
  --release \
  --manifest-path engine/Cargo.toml \
  --bin marketforge-normalize \
  --features "historical,monitor-tui"

./engine/target/release/marketforge-normalize \
  --project-root . \
  --config data/.config/historical/general.toml \
  --batch engine/tests/historical/fixtures/normalize.json \
  --output-root "data/tests/historical/normalize/full-run/$(date +%Y%m%d-%H%M%S)" \
  --tui
```

Admission-only reproduction, without archive execution:

```bash
cargo run --release --manifest-path engine/Cargo.toml \
  --example normalize-admission --features historical
```
