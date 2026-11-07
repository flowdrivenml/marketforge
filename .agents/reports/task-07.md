# Task 07 — CLI results, manifests and Forge boundary

Status: completed. Commit: recorded in the next progress update.

Implementation: --batch directly consumes the existing schema-3 production parser/resolver. --job and --jobs-dir remain, with exactly-one-mode Clap validation and the shared executor/session paths. All modes apply root-relative paths/runtime and optional collision-protected output-root redirection. Batch run_id is preserved; legacy modes return null. No source path rewriting. Directory order is deterministic. Explicit --tui remains interactive stdout rendering; normal machine mode emits one JSON document with stderr diagnostics.

Response schema1/kind normalization_result owns explicit public job DTOs independent of batch3/manifest2/failure1. Fields: schema_version,kind,run_id,status,jobs,summary,error. Each job preserves job_id,dataset_id,status,manifest_path,failure_report_path,events_written,files_written,start_timestamp_ns,end_timestamp_ns,failure. Summary totals distinguish complete/failed/not_reported. Top-level error has code/message; per-job error codes retain existing serialization. Complete requires every requested job committed. Approved degraded integrity remains complete. Failed jobs, missing definitive results, initialization/commit failures exit nonzero after emitting available structured results. Argument parse errors remain nonzero stderr diagnostics. Exact u64 identifiers are never converted through floats.

Decisions: existing source/stream relationships are already reconstructable from manifest source_tasks, per-task metrics and processing-job source/stream/rank/instrument/profile provenance. No redundant manifest registry added. A minimal session result callback preserves earlier definitive outcomes if later operations error, without changing scheduling. Failure persistence errors now retain the definitive failed job and append the persistence diagnostic; report paths are only populated for actual regular files. Partial counters remain in failure reports, not claimed as committed output.

Manifest version2 unchanged. Existing footer-derived rows/statistics and filesystem sizes retained. Discovery now rejects symlink/nonregular artifacts before traversal. Every depth task requires all three auxiliary artifacts, and segment total_levels must equal its canonical rows. Staging/collision/same-filesystem rename behavior remains. No canonical schema, compression, writer targets or rotation redesign. Trade file target observes writer bytes after flush; depth target accumulates Arrow batch memory, while row-group triggers estimate buffered records. These are targets, not hard persisted-size limits; actual manifest sizes remain authoritative.

Provenance: processing-job.json and exact-byte checksum retained, effective runtime/policy retained. Shared feed selection now serves both prepared tasks and resolved snapshot verification. All33 profile execution cases additionally reload committed provenance and compare task configuration exactly, including Gate snapshot/incremental selection. This fixes an otherwise nonreloadable Gate feed snapshot without introducing a competing authority.

Modified files: normalize CLI; normalize response/result/executor/session; config batch/catalog/job feed resolver; manifest builder/discovery; CLI contract tests and profile provenance assertions; docs/historical/normalization-cli.md; progress and prior report hash.

Verification: 8 actual subprocess/serialization/artifact CLI tests passed; full historical_normalize158 passed; support34 passed including bounded Bybit representatives; library198 passed plus newly added unavailable-failure-report unit1 passed; binary CLI configuration test1 passed. Historical CLI compiled with monitor-tui; historical check and Clippy lib/bin passed with baseline warnings. Changed-file rustfmt/git diff check passed. Full-repository fmt still fails in unrelated live baseline files. Initial test-helper TOML serialization and original-vs-redirected output path errors were corrected; final focused checks pass. No unresolved Task07 test failures. Real full corpus, benchmarks and scaling experiments not executed.

Tests cover: mutually exclusive inputs, unsupported schema, invalid runtime, absolute/relative paths, exact IDs above2^53 and u64::MAX serialization, one clean JSON document, legacy modes and output-root, mixed independent jobs, failure report paths/metadata, successful depth aux, manifest/checksum/counters, collision protection, symlink rejection, unreported summary counters, and unavailable report persistence. All artifact-producing tests retain isolated outputs under data/tests/historical/normalize.

Future Forge: supply revised batch instrument authority (Task02), parse response envelope rather than old bare object/array, inspect exit status plus per-job status, register only committed manifests, consume optional failure_report_path, retain failure diagnostics and use exact integer parsing. No Python orchestration or PostgreSQL changes made.

Limitations: interactive TUI output is intentionally not a machine-readable stream; disable --tui for JSON consumers. No claim of stronger crash durability than existing atomic rename. September real oracle/family metadata limitations remain from Task06. Benchmark tooling and stream-oriented storage belong to subsequent tasks.

Illustrative response shapes (paths/counts illustrative):

```json
{"schema_version":1,"kind":"normalization_result","run_id":"run","status":"complete","jobs":[{"job_id":1,"dataset_id":2,"status":"complete","manifest_path":"/assigned/2/manifest.json","failure_report_path":null,"events_written":1,"files_written":1,"start_timestamp_ns":1250000000,"end_timestamp_ns":1250000000,"failure":null}],"summary":{"jobs_total":1,"jobs_complete":1,"jobs_failed":0,"jobs_not_reported":0},"error":null}
```

```json
{"schema_version":1,"kind":"normalization_result","run_id":null,"status":"failed","jobs":[],"summary":{"jobs_total":0,"jobs_complete":0,"jobs_failed":0,"jobs_not_reported":0},"error":{"code":"execution_error","message":"invalid input"}}
```

```json
{"schema_version":1,"kind":"normalization_result","run_id":"run","status":"failed","jobs":[{"job_id":1,"dataset_id":2,"status":"complete","manifest_path":"/assigned/2/manifest.json","failure_report_path":null,"events_written":1,"files_written":1,"start_timestamp_ns":1250000000,"end_timestamp_ns":1250000000,"failure":null},{"job_id":3,"dataset_id":4,"status":"failed","manifest_path":null,"failure_report_path":"/assigned/staging/4/failure.json","events_written":0,"files_written":0,"start_timestamp_ns":null,"end_timestamp_ns":null,"failure":{"code":"missing_source","message":"source unavailable"}}],"summary":{"jobs_total":2,"jobs_complete":1,"jobs_failed":1,"jobs_not_reported":0},"error":null}
```
