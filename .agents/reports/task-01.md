# Task 01 — Shared Historical Runtime Configuration

Status: completed. Commit: `3c65b3d`.

Shared historical configuration, strict TOML loading, validation, path resolution and reusable resource accounting now belong to `engine/src/shared/historical/`. Normalization/job compatibility re-exports retain public interfaces. Existing merge consumers use the shared budget; conversion retains distinct resource units and semantics. CLI defaults to general.toml. No production configuration values, schema version, processing algorithms or policy evaluation changed.

Files: shared/historical modules; normalization config/budget compatibility modules; job configuration exports; merge resource imports; normalization CLI; affected configuration tests and runtime docs.

Verification: 7 focused configuration tests passed; historical and no-default-features cargo checks passed; changed-file rustfmt passed. Clippy completed with 95 warnings. Broad configuration selection: 27 passed, 7 failed from pre-existing missing raw_schemas references. CLI configuration test likewise failed on that missing tree. Whole-project formatting failed in unrelated live files. CLI --help passed. No real-data normalization or benchmarks executed.

Limitations: legacy profile/test fixture dependencies remain for later tasks. Future Forge changes: use general.toml; batch producer integration will follow the finalized Task 02 contract.
