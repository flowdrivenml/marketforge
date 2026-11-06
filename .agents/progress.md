# Historical normalization refactor progress

Branch: `codex-refactor`. No pushes or performance benchmarks authorized.
Pre-existing user edit: `.agents/tasks/Task 12.md` (preserved, excluded from commits).
Instruction-only initial commit: `5e43705`.
Baseline: historical cargo check passed; whole-repository fmt check fails in unrelated live code. Initial configuration tests could not compile because normalize.toml was missing.

| Task | Description | Status | Summary / verification | Commit | Blockers |
|---|---|---|---|---|---|
| 01 | Shared Historical Runtime Configuration | completed | Shared configuration, validation, paths and existing resource accounting; compatibility re-exports. 7 configuration tests pass; historical and no-default-features checks pass; Clippy passes with existing warnings; changed-file rustfmt passes. | pending | Existing catalog/CLI tests require removed raw_schemas tree (Task 02); legacy integration fixtures remain for Tasks 03/06. |
| 02 | Production V3 Batch Loading and Unified Profiles | pending | | | |
| 03 | Shared Normalization Test Infrastructure | pending | | | |
| 04 | Integrity Observation Accounting | pending | | | |
| 05 | Source-Error Handling and Timestamp Ordering | pending | | | |
| 06 | Exchange-Profile Test Migration | pending | | | |
| 07 | CLI Results, Manifests, and Forge Integration | pending | | | |
| 08 | Benchmark Infrastructure | pending | | | |
| 09 | Benchmark Statistics and Comparisons | pending | | | |
| 10 | Stream-Oriented Trade Output | pending | | | |
| 11 | Stream-Oriented Depth Output | pending | | | |
| 12 | Final Integration Verification | pending | | | |

Task 01: general.toml unchanged, schema 1 and all values preserved. Merge consumes shared budget directly; conversion retains its existing operation-specific MergeResources model (different units and fields). Scheduling, estimators, policy evaluation and storage algorithms unchanged. Configuration tests use a deterministic fixture. Broad config test selection: 27 passed, 7 failed due to pre-existing raw_schemas references; CLI configuration test likewise blocked by that removed tree. No archives opened.
