# Task 02 — Production V3 Batch Loading and Unified Profiles

Status: completed implementation; real OKX family metadata coverage remains unavailable. Commit: recorded in the next progress update / Git history (`task-02`).

## Decision and former blocker

The old producer deliberately omitted canonical instrument IDs. Latest authorization resolves that contract conflict: schema-3 now carries `instrument_namespace` and a persisted `instruments` authority containing full classified identity, positive ID, assets and exact specifications. No canonical schema change, unstable hashing or PostgreSQL access. Fixture IDs were allocated once from sorted concrete identities; additions append IDs and never renumber/reuse existing mappings. Cross-batch producers must retain one namespace authority. Task-level IDs and specifications are checked against it. An implicit run-local allocator and mandatory undocumented report sidecar were rejected because they would not establish stable identities.

Family scopes can explicitly bind multiple concrete catalog instruments and archive members. Expansion preserves original task/stream/rank in resolved provenance and assigns checked internal task IDs and deterministic expanded ranks. Trade filtering rejects symbols absent from the supplied family authority; depth uses explicit member selection and strict record identity. The real fixture lacks trustworthy family specifications: these tasks fail actionably rather than borrowing metadata. Synthetic family bindings verify independent IDs/specifications. This is unavailable real metadata, not an architectural execution blocker.

## Implementation / contracts

One production batch reader validates schema/status, metadata, nested fields, jobs, IDs, paths, task/stream consistency, source feeds and authority conflicts without opening archives. One resolver compiles tasks with shared runtime settings. Public loader supports future CLI/test/benchmark consumers. Existing single-job compatibility remains separate.

Only self-contained schema-2 profiles are discovered. Filenames now equal profile IDs; no external raw-schema definitions are required. Source fields compile through existing trade/depth spec compilers. Fingerprints include effective source/rules and are path/comment independent. Unsupported profiles remain inventory-only. XLSX profiles are constrained to actual fixed decoder behavior. Gate.io feed variants resolve format/rules explicitly.

## Source evidence and corrections

Read only representative headers/initial records, never full workloads. Gate.io futures snapshot gzip files in the maintained fixture begin with JSON objects containing `update` seconds and `asks/bids` object arrays `{p,s}`. Existing CSV incremental interpretation was wrong for snapshot feeds. Added explicit JSONL snapshot feed definitions and object-level field mappings; an independent synthetic worker test verifies 1.25 seconds -> 1,250,000,000 ns and 2 contracts * 0.0001 BTC -> 0.0002 BTC. Incremental CSV support remains.

Bybit linear futures gzip CSV header matches linear perpetual fields; initial ZIP depth events match the supported JSONL snapshot/delta model. Added properly identified future profiles. Other inspected headers matched current profiles. No source was modified.

## Files

`historical/config/{batch,catalog,job}` and tests; self-contained profile filenames and two Bybit future profiles; Gate.io snapshot variants; depth object-level extraction/spec; trade family identity enforcement; maintained normalize.json authority; runtime documentation; updated AGENTS and Task 01 retrospective report.

## Verification

28 focused config tests passed, including the final symlink regression. CLI unit test passed. 14 depth-transform and 6 XLSX-decoder tests passed. Historical cargo check passed; Clippy completed with warnings. Changed Rust files formatted. Whole-project fmt retains unrelated live formatting failures. A separately selected legacy depth_spec test failed on missing private job configuration; migrate in Task 06. No full archive execution or benchmarks.

## Limitations / future Forge

Real OKX family specifications/members are unavailable and not invented; these selections remain unverified. Forge must persist namespace/catalog mappings, emit concrete IDs or exact matching identities, and supply complete per-family bindings/specifications and depth archive members. Old Forge batches lacking the authority are rejected. No broad Python changes made. CLI batch/result integration is Task 07; persistent dataset test infrastructure is Task 03.
