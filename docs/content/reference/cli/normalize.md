# Normalization CLI

## Normalization

```bash
mf engine build normalize
mf normalize run engine_test --testing
mf normalize run engine_test --testing --tui
mf normalize run engine_test --testing --tui --allow-partial
```

| Invocation | Result |
| --- | --- |
| `mf normalize new NAME [--from-acquisition SOURCE]` | Create an exclusive managed recipe from a template or saved acquisition recipe, open it in the editor and validate its structure. |
| `mf normalize validate RECIPE` | Parse and structurally validate TOML. No processing or registry selection. |
| `mf normalize prepare RECIPE --testing [--dry-run] [--from-acquisition SOURCE]` | Resolve inputs and report the plan. Normally writes and validates the prepared batch. A dry run skips batch/report publication. Supplying `--from-acquisition` first creates and edits the named recipe even with `--dry-run`. |
| `mf normalize execute RECIPE --testing [--dry-run] [--tui] [--allow-partial]` | Prepare then execute. Dry run stops after planning and does not launch the workload. |
| `mf normalize run REFERENCE --testing [--tui] [--allow-partial]` | Prepare and execute a recipe, or submit a prepared JSON file. |
| `mf normalize register OUTPUT --testing` | Verify and register a committed publication root, manifest, individual primary file, or execution-intent receipt without rerunning. |

Managed recipes live in `data/.config/historical/normalization/`. Existing legacy requests under `historical/normalization_jobs/` remain readable. Use a path with a directory component for an explicit file. For `run`, Forge distinguishes JSON file content from TOML; a colliding local file and managed name is ambiguous. Normalization `new` and `prepare --from-acquisition` open an editor even with global JSON, which controls the final result. See [normalization configuration and runtime](../../configuration/normalization.md).

A run by name prepares once and submits the saved request. An explicit TOML path also selects a recipe. To inspect just the preparation or submit a previously prepared request, use:

```bash
mf normalize prepare engine_test --testing
mf normalize run data/runs/PREPARE_ID/normalize-batch.json --testing
```

Preparation reads the verified testing registry and validates the batch with Rust. It uses persisted ranking positions when available and falls back to deterministic stream identity order for unranked streams. Coverage gaps and missing prerequisite metadata block execution. The run freezes the submitted JSON, executable, runtime and profiles. A request alongside a preparation report must match the recorded fingerprints. An independently authored batch goes through the same production validation and managed raw-input checks.

The default output is `data/datasets/normalized/<engine-dataset-id>` beneath the configured data root. Existing committed paths remain at their registered locations. Rust publishes from staging to a fresh destination. The structured engine response is recorded in both terminal modes. Global `--json` is incompatible with `--tui`.

Forge checks the submitted job, committed manifest, runtime and job provenance checksums, inventory, observed bounds and raw-input lineage. Verified primary files share one publication record. Their proven `historical.dataset_raw_inputs` contributions and publication-level selection evidence are registered together in the verified testing PostgreSQL database. The report maps each primary file to a database ID and identifies the shared namespace and engine publication ID. File-level sizes, rows and observed bounds are available in [dataset inspection](../../workflows/datasets.md). Clean and degraded integrity remain visible separately from the run outcome.

Strict mode returns nonzero if any submitted job fails. When ordinary job failures coexist with independently verified output, `--allow-partial` registers only the verified set, returns zero after registration succeeds, and reports `partial` with omissions. Integrity failures, interruptions, zero verified outputs and registration failures return nonzero in both modes. The flag does not change any Rust processing or integrity policy.

Inspect `data/runs/NORMALIZE_EXECUTION_ID/execution-result.json` for execution, verification and registration outcomes. After an outage, strict-mode failure or database error, use registration-only recovery:

```bash
mf normalize register data/runs/NORMALIZE_EXECUTION_ID/execution-intent.json --testing
mf normalize register /absolute/committed-dataset/manifest.json --testing
```

The intent recovers verifiable committed outputs from the frozen submitted destinations. To recover a specific output, pass its primary file path. Pass the directory or manifest to register all primary members. Registration reuses the database ID for identical evidence and fails on conflicting identity, content or lineage.
