# WP-08 foundations and native observability

Status: Partial / InProgress. Date: 2026-10-21. Not Accepted.

## Implemented and exercised

- Separate SystemWallClock and SystemMonotonicClock; explicit ManualClock adapter
  independently controls wall time and monotonic advancement. Signed wall values
  and checked timestamp overflow are retained.
- Mandatory atomic count/byte resource admission with move-owned RAII permits,
  checked arithmetic, explicit capacity failure and close-without-revocation.
  Concurrent reservations cannot exceed capacity.
- Executor-independent BoundedTaskSpawner uses the frozen task interfaces.
  Capacity survives dropped handles while futures run, and is released on
  rejection. Counts include retained completed handles. This does not select or
  implement the unresolved concrete executor.
- NativeMetrics and ScopedMetricsPort implementation: finite prebound catalog,
  scoped owner generation/reset identity, replacement fencing, retained obsolete
  handle accounting, monotonic freshness, explicit missing/disabled/overflow,
  lazy disabled path, nonblocking observation admission and dropped count.
  Counter overflow is sticky until explicit reset; decreasing counters rejected.
- Fixed 65-bucket histogram, checked count/sum, exact extrema and bounded bucket
  upper quantile estimates. No unbounded sample reservoir.
- Bounded counter aggregation rejects duplicate owner contributions, preserves
  missing/stale semantics and reports arithmetic overflow.
- Capability health evaluates only supplied authoritative observations, ignores
  unrelated optional failures for the consumer total, marks expired success
  Unknown and retains unresolved confirmed failures despite expiry.
- Bounded redacted diagnostic history and local create-new JSONL writer with
  restrictive file mode, byte cap, no overwrite, sync and uncertain-write fencing.
  Typed persisted records omit free text/payload/endpoints and fingerprint IDs.
  Blocking file writes are explicitly for an owned diagnostic worker only.

## Verification

`./scripts/verify-live.sh quick` passes at successive increments: 99, 100, 106,
112 and 113 library tests; each successful invocation exits 0. One intermediate
quick run failed compiling SHA-256 hex formatting; fixed for the current sha2
API, then full verification passed. No dependency/lockfile changes.

Latest `./scripts/verify-live.sh full`, exit 0, all five gates pass:
113 library, 363 integration passed / 1 opt-in PostgreSQL test ignored by default,
1 doctest. Raw logs: `engine/target/live-verification/20261021T025854Z-2/`.
The isolated PostgreSQL test was executed separately and passed (WP-06 report).

## Remaining requirements / truthful limits

WP-08 is not complete: concrete bounded deadline scheduler and joined shutdown,
reusable transport/storage fault harness and composition runner, native sink
integration, rate/reset policy, diagnostic worker lifecycle, broader health
policy/stabilization consumers, and reproducible overhead baseline remain.
Cancellation implementation is preserved; its waiter bounds need reconciliation.
Scaffold modules are not claimed implemented by adjacent working modules.
C12 concrete runtime/TLS/SQL adapter choice remains an open prerequisite for
real task/network/storage adapters, not permission to invent a replacement stack.
WP-05 C01 metadata/identity integration and C06 signed timestamp mapping remain
localized unresolved requirements. No downstream package is certified complete.

Changed modules: foundation clock/resources/shutdown/diagnostics and exports;
metrics registry/histogram/aggregation/health and exports. Existing routing,
historical execution, Python, database schemas and ignored profiles unchanged.
Checkpoint: commit carrying this report (see Git history). Next authorized work:
continue WP-08 independently executable contracts, then WP-09 through WP-29.
