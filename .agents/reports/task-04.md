# Task 04 — Integrity Observation Accounting

Status: completed. Commit: `89f0518`.

## Confirmed findings and changes

Trade parsing successes previously disappeared when later normalization failed; depth success paths recorded no integrity observations. Error timestamps were discarded. Corrected these through small completed-check traces in shared processors and task-local worker recording. No per-success diagnostic allocation, synchronization or alternate evaluator. Record failures preserve applicable earlier successes; the failing category is counted once in its validation unit.

Eligibility is documented in shared/validation/observations.rs: physical parsing per CSV row/JSONL message/XLSX row; applicable validation per source unit, with CSV scalar-row and assembled-event units distinct; quantity transformations per attempted trade/message/snapshot batch; synchronization per incremental event reaching initialization validation; continuity only for initialized sequenced updates. Timestamp comparisons are deferred to Task 05. Current level validation belongs to InvalidRecord; there is no independent InvalidBook check and no artificial successes are added. No-change messages still count applicable checks; 500 snapshot levels produce one parse observation.

XLSX decode exposes a callback for each actual row parse attempt before sorting, preserving partial observations and known timestamps even on a later cell failure. JSONL/CSV/XLSX worker paths collect actual completed stages. Rejected trade timestamps use the production parser and record errors remain observable; malformed trade timestamps now enter InvalidRecord rather than an untyped InvalidCanonical fatal path.

## Attribution / compatibility

Valid timestamps survive later transformation/validation failures. Grouped snapshot failures use their validated group timestamp. Unattributed observations/violations remain in global/file totals and additive defaulted scoped-metrics `unattributed` fields. Window policies fail explicitly with `UnattributedWindow` when relevant observations lack event time, including missing denominators; count/global-rate behavior is unchanged. Epoch-based UTC windows and negative timestamps are preserved. Existing immediate Fail and Degrade semantics remain.

Confirmed prior-task regression fixed: Gate.io incremental feeds must retain initialization snapshot rules, not filter them out. The synthetic CSV test independently verifies two set rows followed by a make update. This preserves required reconstruction initialization.

Files: shared validation traces; trade/depth processor stage hooks; small observed variants of existing snapshot/update/relative helpers; XLSX row callback; normalize workers; scoped metrics/policy; batch feed resolution; support-registered integrity regression tests; runtime docs and progress.

## Verification

14 focused synthetic integrity tests passed: 999 successes + 1 malformed row (eligible=1000, rate=.001), 1000 clean records, minimum-sample boundaries, partial transformation failure with event time, unattributed invalid timestamps, multi-window boundaries, aggregation order, immediate/degraded/disabled policies, missing initialization, unsequenced feeds, CSV snapshot units, no-change updates, 500-level snapshot and XLSX row accounting. All 195 library tests passed. Eight existing relative-depth, two scalar-depth and six XLSX-decoder tests passed. Shared harness/representative rerun, historical check, Clippy and formatting results are recorded in progress after final verification. No benchmarks or full archives executed.

An extra legacy depth_processor target failed all four tests because its private data/.jobs/process/bybit-spot-spot-BTCUSDT-l2-20260901-20260904-d105.json fixture is absent. It never reached processor code; migration remains Task 06. Whole-project fmt retains existing unrelated live formatting failures.

Limitations / next task: fatal JSONL I/O classification and timestamp ordering remain Task 05; no claim of newly enforced ordering. Full real-data/family coverage remains unavailable as documented in Task 02. Canonical schemas, reconstruction rules, writers and production configuration values are unchanged. Future Forge: accept additive metrics field/new integrity threshold diagnostics; identity producer requirements remain Task 02.
