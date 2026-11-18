# Historical normalization refactor progress

Branch: `codex-refactor`. No pushes or performance benchmarks authorized.
Pre-existing user edit: `.agents/tasks/Task 12.md` (preserved, excluded from commits).
Instruction-only initial commit: `5e43705`.
Baseline: historical cargo check passed; whole-repository fmt check fails in unrelated live code. Initial configuration tests could not compile because normalize.toml was missing.

| Task | Description | Status | Summary / verification | Commit | Blockers |
|---|---|---|---|---|---|
| 01 | Shared Historical Runtime Configuration | completed | Shared configuration, validation, paths and existing resource accounting; compatibility re-exports. 7 configuration tests pass; historical and no-default-features checks pass; Clippy passes with existing warnings; changed-file rustfmt passes. | `3c65b3d` | Existing catalog/CLI tests require removed raw_schemas tree (Task 02); legacy integration fixtures remain for Tasks 03/06. |
| 02 | Production V3 Batch Loading and Unified Profiles | completed | Stable persisted batch instrument authority; unified profiles; strict production batch loader and family expansion. | `7cc9b7d` | Real OKX family metadata unavailable; explicitly diagnosed, independent concrete coverage continues. |
| 03 | Shared Normalization Test Infrastructure | completed | Production subset selection, conservative overrides, persistent isolated runs and metadata; two bounded real representatives passed. | `f25a907` | Legacy suite migration remains Task 06. |
| 04 | Integrity Observation Accounting | completed | Stage traces, partial success, depth source-event units, trustworthy error timestamps, explicit unattributed/window-policy handling. | `89f0518` | Legacy private-job depth_processor tests await Task 06 migration. |
| 05 | Source-Error Handling and Timestamp Ordering | completed | Typed fatal source errors, compression completion, concrete-stream task-local timestamps and policy-safe depth behavior. | `45ff971` | Cross-archive ordering remains later tasks; unsupported stateful sorting rejected. |
| 06 | Exchange-Profile Test Migration | completed | 150 source-only +34 support +8 shared tests passed; bounded Binance oracle passed; full185 inventory compiled/listed. | `53ecffc` | September real oracles lack prepared interval; family metadata unresolved. |
| 07 | CLI Results, Manifests, and Forge Integration | completed | Batch CLI, versioned responses/nonzero failures, report paths and provenance/manifest validation;158 normalization +34 support +198 library +1 failure unit passed. | `f7cc14e` | No new blocker; prior real-data coverage limitations remain. |
| 08 | Benchmark Infrastructure | completed | Explicit batch planning, isolated runner, metadata/measurements/verification/retention;15 mock/dry-run tests,201 lib and158 normalization tests passed. | `2de662b` | Real performance execution prohibited; macOS runtime unverified. |
| 09 | Benchmark Statistics and Comparisons | completed | Persisted statistics/scaling/comparability and JSON/Markdown CLI;36 synthetic infrastructure tests,203 lib and158 normalization tests passed. | `02a39f3` | No real performance observations; source evidence caveats explicit. |
| 10 | Stream-Oriented Trade Output | completed | Stream-owned writers, declared/observed ordering, manifest3 source spans, span-aware consumers and focused tests. See task-10 report and Git history. | `292b3f7` | Conservative exact-profile grouping; no generalized overlap reconciliation. |
| 11 | Stream-Oriented Depth Output | completed | Shared writer; independent reconstruction; boundary2 source event/row ranges; global segments; source-scoped readers; persisted replay/failure tests. | `ba51a16` | Real family authority/large-workload/macOS coverage remain unverified. |
| 12 | Final Integration Verification | completed | Integrated semantics, provenance/consumer/failure corrections;204 lib+1 CLI+158 normalization+54 support+36 benchmark infrastructure tests;211 shared+192 materialization;3 real prefixes. | task-12 report creation commit | Full workload: missing authoritative family metadata; whole-project fmt baseline; macOS/performance unverified. |

Task 01: general.toml unchanged, schema 1 and all values preserved. Merge consumes shared budget directly; conversion retains its existing operation-specific MergeResources model (different units and fields). Scheduling, estimators, policy evaluation and storage algorithms unchanged. Configuration tests use a deterministic fixture. Broad config test selection: 27 passed, 7 failed due to pre-existing raw_schemas references; CLI configuration test likewise blocked by that removed tree. No archives opened.

## Task 02 blocker (2026-11-06)

The production contract deliberately omits `instrument_id` from every task (`forge/historical/normalization/README.md`, "Forge task representation"; `mapping.py` only retains IDs in report provenance). Rust `WorkTask` and canonical `EventEnvelope` require that ID; canonical validation rejects IDs <= 0. The existing `InstrumentCatalog` maps identity to base/quote currencies only: it provides neither authoritative numeric IDs nor family-member tick sizes/contract specifications. No historical loader consumes such a mapping. Family tasks legitimately contain null symbol/instrument metadata.

Completing executable batch resolution would require a defined authoritative mapping input, a producer contract change, or a canonical identity migration. Task 02 prohibits Python changes, canonical schema changes, fabricated metadata, and speculative replacements for missing infrastructure. Reading report provenance as a newly required sidecar would also invent an undocumented loading dependency. No ID hashing, placeholder IDs, PostgreSQL access, or arbitrary representative instruments were introduced.

Stopped before incomplete Task 02 implementation. Tasks 03–12 remain pending because the required production batch/profile foundation is incomplete; their acceptance cannot be verified independently in the mandated sequential execution. No successful-task commit was created for Task 02. This progress update remains uncommitted so it does not imply Task 02 completion.

Resume prerequisite: specify an authoritative offline mapping from full instrument identity to canonical ID and, for family archives, concrete instrument specifications; alternatively explicitly authorize a revised identity contract and scope. Existing source archives/datasets and benchmark history were untouched. No real-data workloads or performance benchmarks were executed.

Updated authorization resolves the earlier architectural blocker: schema-3 now carries a persisted offline instrument authority. Original blocker above is historical context. Correctness limitations for real family metadata remain explicit rather than blocking independent work.

Task 02 verification: 28 config tests, 1 CLI test, 14 depth-transform tests and 6 XLSX decoder tests passed; historical check and Clippy passed (warnings). Changed-file formatting passed; repository fmt still fails on unrelated live files. Legacy depth_spec selected test fails on absent private job files and remains assigned to Task 06.

Task 03: 8 harness tests passed (6 source-only, 2 bounded real prefixes); check passed; changed-file fmt passed. Generated artifacts retained and ignored. Broad legacy migration deferred to Task 06.

Task 04 final verification: 22 support tests passed (14 accounting + 6 harness + 2 bounded real representatives); all 195 library tests passed; 16 shared depth/XLSX tests passed. Historical check and Clippy passed with warnings; changed-file rustfmt passed, whole-project fmt has prior unrelated live failures. Four legacy depth_processor tests fail before processing on absent private job config, deferred to Task 06.

Task 05: 12 focused source/ordering integration tests passed; full support selection 34 passed including bounded representatives; 198 library tests passed including 3 JSONL classification/progress tests. Historical check/Clippy and changed-file formatting passed; repository fmt still has unrelated baseline failures. No performance workloads.

## Final integration audit (Task 12)

Tasks10–12 completed sequentially on the existing branch; Task10 `292b3f7`, Task11 `ba51a16`, Task12 is the creation commit of its report. Full-source readiness remains conditional: full-workload dry-run rejects Task88 without authoritative family members/specifications. All33 supported profiles have passing synthetic cases; three bounded current archive prefixes pass. No complete corpus, actual performance benchmark, warmup, scaling run or profiling session executed. Outputs retained under data/tests/historical/normalize. No Python integration started. Final reports distinguish supported verified paths, known limitations and unverified real-data coverage.

## Normalization retained-state follow-up (partial)

Implemented bounded V2 source-boundary consumption, phase-owned boundary admission, atomic scope aggregation without whole-map clones, and borrowed manifest reporting. Completed-report spilling and bounded end-to-end report aggregation remain outstanding; the full prepared batch still exceeds 64 GiB. Implementation and admission evidence: `.agents/reports/bounded-retained-state-implementation.md`.

## Source-derived merge recipes

Implemented and verified registry-backed merge recipe creation and preparation from a managed normalization source. The testing registry yielded an actionable blocked full `engine_test` report and a Rust-validated single-stream trade job. No workload ran. See `.agents/reports/merge-from-normalize.md`.
