# Forge engine and normalization

Both `mf` and `marketforge` use the same parser. Global `--json` precedes the command. Interactive `--tui` and global `--json` are incompatible. Errors go to stderr and an incomplete managed workflow returns a nonzero exit code.

## Managed engine

```bash
mf engine build
mf engine build normalize --dry-run
mf engine build normalize --offline --jobs 4
mf engine status
```

The explicit default set contains only `normalize`: package `marketforge-engine`, Cargo binary `marketforge-normalize`, release profile, no default features, approved features `historical,monitor-tui`. Build prerequisites are native Linux/macOS, Cargo and a compatible Rust edition-2024 toolchain. PostgreSQL, credentials and market files are unnecessary. Linux builds were exercised; macOS support is unverified.

Build uses `--locked`; ordinary builds may fetch lockfile dependencies, while `--offline` prohibits fetching. It respects Cargo artifact paths, including custom target directories. A configured foreign target is rejected. Jobs controls compiler concurrency. No Rust installation, dependency update, optimization override or workload submission occurs.

The executable is an independent copy at repository-root `.bin/marketforge-normalize`, independent of the data root. Forge checks its non-processing `--help` before atomic replacement. Existing launchers are preserved. A file lock prevents competing Forge installers. `.bin/manifest.json` records toolchain, features, host, smoke result, timestamps, size, SHA-256 and revision/dirty indication. The latter is not an exact modified-source snapshot.

Status is read-only and distinguishes missing installation, missing/malformed metadata, non-executable files, unsafe symlinks, verified checksums and mismatches. It never executes existing bytes. Executable and manifest replacements are separate; interruption can leave a detectable mismatch. An explicit build repairs it. Normalization never builds automatically, modifies shell PATH or installs system-wide.

Maintainers approve future targets by extending the typed `TARGETS` catalogue in `forge/historical/engine/managed.py`, specifying package, binary, installed filename, features, default-feature policy, enabled/default selection, non-processing smoke arguments and the expected output marker. Reuse the same build/install service. Build the merge target with `mf engine build merge`.

## Profiles and registration

```bash
mf acquisition profiles list --type trades --exchange bitget
mf acquisition profiles inspect bitget.perpetual.linear.depth
mf acquisition profiles validate bitget.perpetual.linear.depth
mf acquisition profiles validate --file /absolute/profile-id.toml
mf acquisition profiles add bitget.perpetual.linear.depth --from /absolute/bitget.perpetual.linear.depth.toml --testing
mf acquisition register new example --from docs/content/examples/catalogue/registration.toml
mf acquisition register edit example
mf acquisition register inspect example
mf acquisition register validate example --testing
mf acquisition register plan example --testing
mf acquisition register run example --testing
```

`add` is canonical; `register` is an alias for profile publication. Profile IDs have exactly four components: `exchange.instrument_type.market_category.trades|depth`. The filename must be exactly `<id>.toml`. The production Rust catalogue is `data/.config/historical/profiles/{trades,depth}`; schema-2 self-contained profiles compile through the installed normalizer.

Users author all fields. Forge does not generate profiles or infer units, side semantics, sequence domains, resets or snapshot/deletion rules. Discovery/inspection require no database and report TOML/namespace inspection, not engine acceptance. Validation uses production semantic compilation and a staged candidate catalogue. Unsupported inventory-only definitions cannot be installed for processing. Acceptance does not prove arbitrary source files conform to the definition. Different interpretations under the same ID conflict; equivalent compiled interpretations can reuse an installed definition without replacing its bytes. Gate.io feed variants remain selected by the supported explicit source feed; arbitrary competing variants under one ID are unsupported.

Register the [catalogue prerequisites](../../workflows/catalogue/index.md) before validating or running an archive recipe. Registration names resolve under `data/.config/historical/registration/`; explicit paths are accepted. `new --from` copies a recipe without overwriting an existing draft, or creates an incomplete declaration for a directory/file. Source roots relative to the repository resolve independently of the invocation directory. File selectors are relative to that source root; enumeration is deterministic and bounded. A registration freezes selected files; files arriving afterward require a new registration. Duplicate physical inputs fail.

See the [complete synthetic workflow](../../workflows/acquisition/forge.md) for examples and limitations.

## Normalization

```bash
mf engine build normalize
mf normalize run engine_test --testing
mf normalize run engine_test --testing --tui
mf normalize run engine_test --testing --tui --allow-partial
```

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

The intent recovers verifiable committed outputs from the frozen submitted destinations. To recover a specific output, pass its committed directory or manifest. Registration reuses the database ID for identical evidence and fails on conflicting identity, content or lineage.

## Merge

See [Merge registered normalized datasets](../../workflows/merge.md) for installation, recipe syntax, compatibility restrictions and registration recovery.
