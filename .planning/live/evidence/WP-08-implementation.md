# WP-08 — Foundations and native observability

Status: ReadyForReview (not Accepted). Date: 2026-10-21.
Initial primitive checkpoint: `18e5f0e`; completion checkpoint is the commit
carrying this report. Upstream localized C01/C06/C12 gates remain visible.

## Requirement ledger

| Requirement | Implementation / witness |
|---|---|
| Monotonic versus wall time | Production SystemMonotonicClock/SystemWallClock; explicit ManualClock; wall jumps cannot expire deadlines |
| Bounded cancellation | Fallible pre-scheduling waiter reservation, configurable cap (4096 default), checked IDs, wake outside locks, drop unregister; router admission propagates capacity |
| Deadline scheduling | One native deadline worker; bounded timer slots retained until future drop, monotonic expiry, explicit unfinished shutdown; deterministic adapter shares expiry core |
| Task/deadline/resource bounds | BoundedTaskSpawner wraps frozen task ports; permits survive dropped running handles, failed spawn releases; atomic count/byte budget and close without revocation |
| Scoped metrics | NativeMetrics implements ScopedMetricsPort and ObservationSinkPort; closed vocabulary covers all frozen owners; finite scopes/series including retained obsolete handles; explicit retire, generation/reset fencing |
| Metric semantics | Checked monotonic counters, typed gauges, 65 fixed histogram buckets with exact extrema and bucket-upper quantile estimates, sticky overflow, reset-aware monotonic rate estimation |
| Configuration / disabled timing | MetricsSettings controls enabled and detailed/sampled lazy measurement; disabled closures do not run; health and diagnostics remain active; explicit reset before measurement resumption |
| Snapshot/aggregation | Monotonic receipt freshness, missing/disabled/stale/overflow values; duplicate owner aggregation rejected; RuntimeInstance counters survive owner-generation changes |
| Health | Required capability policy, optional failure isolation, missing/stale success Unknown, confirmed failures retained; configured pressure hysteresis/stabilization cannot clear integrity without authoritative recovery |
| Local diagnostics | Typed redacted records fingerprint IDs, omit free text/native payloads/endpoints; bounded local history; create-new byte-capped JSONL writer, restrictive file mode, sync and uncertain-write fencing |
| Diagnostic lifecycle | Bounded nonblocking logging-worker queue; explicit queued versus written distinction; deadline shutdown/join report and rejected-record accounting |
| Deterministic harness / composition | Real admission, clock, cancellation, accounting and native observations composed; actual InstrumentedOwner constructor; reusable bounded fault script and REST adapter use frozen ports; existing filesystem storage/replay fault witnesses preserved |
| Test safety | Existing unsafe RawWaker ownership replaced with safe Arc<Wake>, preserving assertions |

## Verification

Latest foundation checkpoint full run: `./scripts/verify-live.sh full`, exit 0;
123 library tests, 366 integration passed / 2 opt-in tests ignored, 1 doctest.
All five gates pass. Receipts:
`engine/target/live-verification/20261021T031217Z-2/`.
Earlier full/quick runs are retained under the same raw-log parent. Intermediate
SHA-256 formatting and exhaustive timer-error conversion compile failures were
repaired and subsequently verified; no assertions were weakened.

`./scripts/verify-live.sh tui`: exit 0, compile check (not behavioral tests).
Receipt `20261021T030647Z-2/live-tui.log`.
`cargo check --locked --offline --no-default-features --features live,historical`
(engine/): exit 0.
`./scripts/verify-live.sh historical`: exit 1; actual library result 151 passed /
1 known unchanged failure `historical::merge::resources::tests::rejects_empty_streams`.
Receipt `20261021T030818Z-2/historical-library.log`. Historical code unchanged.

Release overhead command (engine/): `cargo test --locked --offline --release
--no-default-features --features live --test live_suite
native_measurement_overhead_baseline -- --ignored --nocapture`, exit 0, 1 passed.
System monotonic clock, 100000 operations: counters 2555699 ns; detailed timing
sampled 1/10 763300 ns; disabled 22031 ns. 1000 two-series snapshots: 68162 ns.
These are reproducible primitive baselines on this machine, not service latency
or production throughput certification. No timing threshold is used as a test.

## Limits and integration obligations

No concrete async network/SQL executor was selected; BoundedTaskSpawner is an
admission wrapper over the frozen executor port. C12 remains open before real
network/SQL adapters. Native timer and diagnostic workers are bounded owned
foundation workers, not a second market-data runtime. Timed-out shutdown reports
unfinished ownership; it does not claim an OS-blocked write was cancelled.

Metrics replacements need transition headroom while old handles are retained;
capacity failures are explicit. Histogram quantiles are approximations. Local
logging is operational history, not a canonical durability journal. Health only
consumes authoritative input; it cannot establish market-data validity. Each
subsequent production owner must bind these handles through its constructor and
provide its domain observations in its own package. No exporter/server/TUI owner
is claimed implemented here. Existing WP-10 behavior is preserved except for
fallible timer/cancellation admission integration.

Changed: foundation clock/cancellation/deadline/resources/shutdown/diagnostics;
metrics registry/histogram/aggregation/health; observation admission outcomes;
router wait-capacity conversion; deterministic foundation/support tests.
No Python, historical execution, dependency, lockfile, ignored profile, or
production database changes. Next authorized package: WP-09.
