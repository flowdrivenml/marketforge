# Task 05 — Source Errors and Timestamp Ordering

Status: completed. Commit hash: task-05 Git history / next progress update.

Confirmed findings: JSONL reader errors were wrapped in serde errors and entered depth record-recovery loops. CSV depth likewise treated I/O as malformed rows; trade I/O was fatal but untyped. Timestamp extrema were recorded without ordering comparisons. Source sequence checks already existed and remain unchanged.

Implemented typed JsonlError::Source/Record with complete byte-line boundaries, EOF distinction, preserved blank-line behavior and recoverable invalid UTF-8 records. Underlying I/O aborts immediately, never via a policy threshold. CSV I/O retains SourceRead causes/path; source preflight preserves SourceOpen. Normalization readers validate compression completion; GZIP handles concatenated members and TAR/GZIP validates the outer trailer after selected-member processing. The existing prefix reader remains explicitly available for bounded inspection. Damaged XLSX is fatal with archive context. Output errors remain terminal.

Timestamp checks run per concrete source task/member, after identity resolution. Equal/increasing timestamps pass; eligible comparisons exclude the first accepted event. Watermarks advance only after sink acceptance. Degrade retains regressed events in source order with diagnostics; Fail rejects and aborts, invalidating depth synchronization and boundaries. XLSX uses its existing stable sort; other TimestampSortable inputs with no supported ordering path are rejected instead of silently promising sorted output. Stateful depth deltas are never timestamp-sorted. Cross-archive chronology is explicitly not advertised yet; no persistent shared writer state was introduced.

Existing failure codes are reused: unavailable inputs/members -> MissingSource, read/archive corruption -> CorruptSource. Resolved source/task provenance and failure metrics remain preserved. No canonical schema or runtime value changes. Public source API changes: JsonlError distinguishes typed failures; next_record exposes bytes; open_gzip returns MultiGzDecoder. Read-based callers retain their interface.

Files: shared source decoder/readers; normalize workers and new ordering helper; preflight resource inspection error; failure persistence classification; focused source_ordering tests and updated integrity expectation for newly applicable comparison; runtime docs/progress.

Verification: 12 focused integration tests passed, including increasing/equal/regressed trades, fail/degrade depth sequence preservation, malformed-line invalidation/recovery, independent family-stream watermarks, rejected unsupported sorting, task-boundary scope, truncated CSV/JSONL gzip, ZIP CRC, TAR trailer, missing members, damaged XLSX, no committed failed datasets and independent multi-job session failure. Full support target with bounded real representatives: 34 passed. All 198 library tests passed, including deterministic persistent-reader failure call count (one failed underlying read), malformed-line progress and invalid UTF-8 recovery. Historical check and Clippy completed with warnings; changed-file formatting passed. Whole-project fmt retains unrelated baseline live formatting errors. No benchmarks or full archive normalization.

Limitations: only task/member-local ordering is established; cross-archive consolidation remains Tasks 10–11. Legacy private-job regression targets await Task 06 migration. Real OKX family metadata remains unavailable. Future Forge: consume existing typed source result codes and additive integrity diagnostics; offline identity producer changes remain Task 02. No Python orchestration work performed.

Task commit: `45ff971`.
