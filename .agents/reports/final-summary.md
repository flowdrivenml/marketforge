# Historical normalization refactor — final summary

All twelve numbered implementation/verification tasks are complete within the supported scope. This is **conditional readiness**, not verification of the complete maintained historical workload. Full-workload planning is blocked by missing authoritative family-member metadata. No unsupported source combination is reported as normalized successfully.

| Task | Implemented and verified area | Local commit |
|---|---|---|
|01|Shared historical runtime configuration, path/resource validation|`3c65b3d`|
|02|Prepared batch3, offline instrument authority, self-contained profiles/fingerprints, family expansion|`7cc9b7d`|
|03|Production-loader test harness, isolated persistent artifacts, bounded representatives|`f25a907`|
|04|Eligible integrity observations, violation accounting and timestamp attribution|`89f0518`|
|05|Fatal/recoverable source errors and task-local timestamp/sequence ordering|`45ff971`|
|06|33 independently asserted synthetic profile cases and retained archive oracle migration|`53ecffc`|
|07|Batch/job CLI, response1, nonzero failures, manifests/provenance and commit contracts|`f7cc14e`|
|08|Explicit benchmark planning, isolated runner, measurements/retention and correctness verification|`2de662b`|
|09|Persisted statistical summaries, scaling/comparison checks, JSON/Markdown reports|`02a39f3`|
|10|Stream-owned trade writer, source ordering/spans, resource lifetime and consumers|`292b3f7`|
|11|Stream-owned depth, independent reconstruction, source ranges/boundaries and replay readers|`ba51a16`|
|12|Integrated verification and targeted policy/resource/provenance/checkpoint/reader fixes|This report's creation commit; subject `task-12: verify historical normalization integration`|

Task12's exact commit is available with `git log --diff-filter=A --format=%H -- .agents/reports/final-summary.md`. Initial instruction commit: `5e43705`; baseline checkpoint: `e6647d1`. Work remained on `codex-refactor`; no push, reset, amend, squash or branch change. Each numbered task has one local commit. Pre-existing user edits to normalize.json, the Task12 specification, Task09 report and Task09 progress hash remain outside these final commits. Necessary pre-existing source_time_bounds loader/initializer work was included in Task10, with disclosure.

## Final architecture

Prepared schema3 batches resolve through one production loader against a persisted offline instrument namespace/catalog and self-contained TOML profiles. Effective general.toml settings flow through validation, source planning, the global resource budget, workers, Parquet writers and integrity evaluation. Concrete compatible streams within each job are execution units; independent units run concurrently and sources within each unit run sequentially. Reservations cover writer lifetime and the maximum source estimate plus bookkeeping. No full-stream buffering or speculative decode-ahead exists.

Trade and depth writers survive source EOF and rotate parts according to supported targets. Every depth source constructs independent processor/book/sequence state, preserving snapshots, replacement/recovery segments, source-local ordinals and zero-change events. Physical adjacency does not establish continuity. Finalization checks persisted rows, source spans, depth event coverage/initializations/boundaries, metrics and policy results, writes the manifest, then commits with same-filesystem rename. Failures retain diagnostics and cannot commit success. Existing destinations are refused.

## Interfaces and persisted contracts

- Batch3 now includes offline namespace/instrument authority and optional inclusive source_time_bounds with positive partition ordering. Multiple consolidated sources need trustworthy periods/partitions, checked against accepted timestamps. Overlaps requiring reconciliation fail.
- Canonical trade/depth columns and exact decimal semantics remain unchanged. Existing canonical row counters retain their meanings.
- Manifest3 adds safe stream directories and half-open per-task canonical row spans. Readers retain manifest2 task-layout support. Files are counted once, irrespective of how many sources contribute.
- Depth boundary2 stores every source's full boundary1 evidence plus task identity, canonical row ranges and event-index ranges; continuity is explicitly independent_sources. Legacy boundary1 remains readable.
- Event-index schema remains unchanged: event_ordinal stays source-local; physical event-index row position is the zero-based global output ordinal, including zero-change events. Canonical row offsets are stream-global. Segment1 offsets refer to the owning writer's canonical row space; initialization/recovery/source boundaries remain explicit.
- Task reports add optional accepted_depth_events, distinct from canonical levels. Earlier reports deserialize with unknown/null accepted-event count.
- processing-job.json records exact effective interpretation, source/member/profile fingerprints and identity provenance; its exact bytes are checksum-validated by merge/conversion readers.
- CLI response1 and failure1 remain independent of batch/manifest versions. Forge must inspect exit status and each job status; only committed manifests are successful registration inputs.
- Consumer interfaces include source-scoped depth cursors, canonical physical slices, and CanonicalReferenceResolver::register_source_task. Legacy complete-file registration remains strict.

## Verification evidence

Final Linux runs:204 library tests,1 normalization CLI unit,158 source-only normalization tests,54 support/accounting/ordering/stream tests,36 synthetic benchmark infrastructure/analysis tests,211 tests across26 shared book/depth/Parquet/merge/conversion targets, and192 synthetic materialization tests passed. Additional focused runs are subsets, not extra unique coverage. New tests register once. Persistent direct-run metadata records success/failure without cleanup.

All33 supported profiles have passing synthetic cases with independent prices/quantities/sides/timestamps or book-state expectations. Stream tests verify consolidation, rotation, exact spans, source IDs, repeated ordinals, zero changes, recovery, intermediate persisted replay, independent initialization, ties/partitions, worker counts, resource failure isolation, metadata/path rejection, legacy readers and physical source-slice decoding. Existing source corruption, sequence, integrity-denominator/window, CLI subprocess and transaction tests pass. Production Degrade behavior remains distinct from cross-source append validation.

Three narrowly selected real-prefix cases passed on the final architecture: Bybit spot trades (2 rows), Bybit spot depth (420 canonical levels), Binance linear perpetual first trade (1 row). Sources were read only; complete small sampled containers and outputs remain under data/tests/historical/normalize. These checks do not prove whole-archive or exchange-wide semantics.

Historical and no-default-features checks, historical+monitor-tui binary checks, merge/convert test-target compilation and Clippy lib/binaries passed. Existing Clippy warnings remain. Changed-file rustfmt and diff checks pass. Whole-repository fmt still fails on pre-existing unrelated live formatting; no broad formatting cleanup was performed.

## Known limitations and unverified coverage

No known unresolved correctness failure remains in the exercised supported paths. Important limits remain:

- Full maintained-workload nonexecuting dry-run rejects Task88, BTC-USD family, for missing authoritative concrete scope.members/specifications. Six OKX profile classifications have unresolved family scopes. Referenced archive paths currently exist, but existence is not source verification. Do not allocate invented identities/specifications.
- Retained September archive oracles lack their prepared interval in the October fixture. Other real profiles, feeds, multipart/monthly combinations and full archives remain unverified. Complete corpus/monthly normalization was not executed.
- Stateful timestamp-sortable feeds without supported sorting paths are rejected. General overlap reconciliation, deduplication and cross-archive book continuation are unsupported.
- Exact-profile compatibility is conservative; semantic profile revisions can yield separate output groups. Family routing may reread a family archive per concrete member.
- Unified merge still requires one logical instrument stream per input dataset; multi-instrument normalization itself is supported, but a mixed-instrument dataset is explicitly rejected by that consumer. Conversion still needs its instrument/currency catalog and reachable canonical provenance paths.
- Degraded within-source timestamp regressions remain in source order. Such output is not guaranteed globally monotonic and chronological readers may reject it.
- Memory budgets govern admission estimates, not exact RSS or OS-level allocation. Event/segment buffering is bounded; excessive segment metadata fails explicitly. Large snapshot/workload resource behavior and macOS runtime remain unverified. Rename provides the existing atomic publication contract, not added crash durability.

## Benchmark status

Tasks08/09 implement the runner and comparison/reporting infrastructure. Synthetic/mock planning, metadata, statistics, semantic verification and comparison tests pass. Full-workload planning reports the real family prerequisite; selected synthetic dry-runs do not normalize sources. Correctness-only verifier tests compare semantic evidence across physical layouts with explicitly unmeasured envelopes.

**Actual performance benchmarks were not executed.** No real warmups, worker-scaling runs, profiling, I/O stress tests or measured performance improvements are claimed. Reduced synthetic file counts verify storage structure only. Real throughput/CPU/RSS/I/O/scaling and macOS collectors require separately authorized experiments.

## Python Forge handoff

Python integration was not started. Remaining work: persist/reuse the offline instrument namespace/positive IDs; emit complete family members and exact specifications; produce trustworthy source bounds/partitions and source-feed options; emit prepared batch3; use general.toml; consume response1 with exact integers and process/per-job status; register only committed manifests; understand manifest3 spans and depth boundary2/local-global event mappings; retain resolved-job checksums, profiles, task/source relationships and failure diagnostics; implement transactional/idempotent PostgreSQL registry handling in Python. Broader independent real-data correctness coverage should precede claiming complete-workload readiness.

Detailed implementation evidence: task-01.md through task-12.md. Output contract: docs/historical/normalize-stream-output.md. Final commands/profile inventory/readiness: docs/historical/normalize-integration-verification.md.
