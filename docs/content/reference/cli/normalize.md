# Normalization commands

Start with [Normalize historical data](../../workflows/normalize.md). Use this reference for command effects and options. See [recipe fields](../../configuration/normalization.md) and the [prepared batch contract](../prepared-normalization.md) for input formats.

<a id="important-normalization-options"></a>
<a id="-testing"></a>

## Synopsis and arguments

Commands below follow `mf normalize`. `NAME` is a managed recipe name with an optional `.toml` suffix. Where a TOML path is accepted, use an absolute or directory-qualified path such as `./recipe.toml`.

| Command | Arguments and options | PostgreSQL | Main effect |
| --- | --- | --- | --- |
| `new` | `NAME [--from-acquisition SOURCE]` | No | Create a draft or copy, open the editor, and validate the saved TOML. |
| `edit` | `NAME` | No | Open the saved recipe and validate after editing. |
| `list` | None | No | Read the local recipe inventory and structural status. |
| `show` | `NAME` | No | Show the parsed configuration and declared inputs. |
| `validate` | `NAME_OR_TOML` | No | Check TOML and recipe constraints. |
| `delete` | `NAME [--apply [--force]]` | No | Preview or unlink a managed recipe. |
| `prepare` | `NAME_OR_TOML [--dry-run] [--from-acquisition SOURCE]` | Yes | Resolve registered inputs. A persisted plan writes artifacts and validates executable plans with Rust. |
| `execute` | `NAME_OR_TOML [--dry-run] [--tui] [--allow-partial]` | Yes | Prepare, execute, verify, and register. Dry run stops at planning. |
| `run` | `REFERENCE [--tui] [--allow-partial]` | Yes | Execute a recipe or submit prepared batch JSON. |
| `register` | `REFERENCE` | Yes | Verify committed output and register its primary files. |
| `config` | `show\|get\|schema\|validate\|set\|unset\|edit` | No | Inspect, validate, or change the Engine runtime. |
| `profiles` | Subcommand and arguments | Varies | Inspect, author, compile, or install normalization profiles. |

For registry-backed commands, choose `--testing` or `--production` (the default). Keep the same registry for selection, execution, registration, and inspection. Recipe and processing commands use local files and registered evidence with no upstream archive or market API calls. Profile operations have their own requirements.

<a id="understanding-recipe-names-and-paths"></a>
<a id="why-use-a-path-containing-a-directory"></a>
<a id="what-about-older-recipes"></a>

## Recipe management

<a id="-from-acquisition-source"></a>

### `mf normalize new`

Create and open a new managed recipe. The command refuses to overwrite an existing recipe. `--from-acquisition SOURCE` copies a managed acquisition recipe's TOML into the normalization library. This is an independent copy. Post-edit validation checks only the saved structure. If validation fails, fix the saved draft with `edit`.

```bash
mf normalize new my_dataset
```

<a id="mf-normalize-validate"></a>

### List, show, edit, and validate

```bash
mf normalize list | cpout
mf normalize show my_dataset | cpout
mf normalize edit my_dataset
mf normalize validate my_dataset | cpout
```

`list` reports invalid entries alongside valid recipes. `show` displays declared source requests or explicit inputs. `edit` uses the configured editor and rechecks the file when the editor exits successfully. `validate` accepts a managed name or explicit TOML path and checks local fields and constraints. Registered file availability, identity resolution, and Rust batch validation belong to preparation.

### Delete a recipe

```bash
mf normalize delete my_dataset | cpout
```

Preview inspects the managed file and checks live references from source-derived merge and conversion recipes. It lists recipes it could not inspect. Detected references block deletion by default. To remove the TOML after reviewing the preview, use `mf normalize delete my_dataset --apply`. To deliberately break the listed references, use `--apply --force`. The command accepts only a regular, single-link managed recipe file and rechecks its identity before unlinking. Raw archives, normalized and merged datasets, registered publications, historical runs, reports, and installed profiles are outside the deletion scope.

## Engine runtime configuration

`mf normalize config` manages `data/.config/engine/normalize.toml`. Recipes select data and profiles specify how to decode it. The runtime controls Engine behavior.

```bash
mf normalize config show --section resources | cpout
mf normalize config schema --field resources.workers | cpout
mf normalize config set resources.workers 12 | cpout
```

`set` and `unset` preview by default and require `--apply` to save. `edit` validates a draft before asking for confirmation. See [Normalization runtime configuration](../../configuration/normalization-runtime.md) for field semantics, defaults, and integrity acknowledgement.

## Preparation

<a id="-dry-run"></a>

### `mf normalize prepare`

```bash
mf normalize prepare my_dataset --testing --dry-run | cpout
mf normalize prepare my_dataset --testing | cpout
```

A dry run resolves registry and local-file evidence and previews job, task and stream counts. It can use the Engine to check an external source's profile fingerprint. It skips Rust batch validation and returns prospective batch and report locations. Persisted preparation writes the batch and report and, for a `prepared` plan, runs the production Rust batch validator. A saved `partial` plan is diagnostic and cannot be submitted. See [preparation results](../prepared-normalization.md#preparation-status).

`prepare NAME --from-acquisition SOURCE` first creates a managed copy and opens the editor, then prepares the saved recipe. Even with `--dry-run`, this shortcut still creates and edits that recipe. Use a new managed name for this mode.

## Execution

<a id="use-the-terminal-processing-interface"></a>
<a id="run-with-the-tui"></a>
<a id="what-does-the-tui-show"></a>
<a id="why-does-progress-sometimes-appear-to-pause"></a>
<a id="can-i-use-tui-and-json-together"></a>

<a id="-tui"></a>

### `mf normalize execute`

```bash
mf normalize execute my_dataset --testing
```

Accepts a managed recipe name or explicit TOML path. It performs persisted preparation and starts execution only after a fully prepared result. Use `--dry-run` for planning only. Use `--tui` on a terminal to show Engine progress. TUI requires interactive stdin, stdout and stderr and is incompatible with global `--json`.

<a id="ambiguous-references"></a>
<a id="how-does-run-distinguish-json-from-toml"></a>

### `mf normalize run`

`run` can execute a recipe like `execute`, or submit a prepared JSON batch without repeating recipe selection. Submitted JSON still goes through Rust batch validation, raw-input checks, execution, verification and registration. Use a directory-qualified path to resolve an ambiguity between a local file and a managed recipe name. Existing explicit files are identified as JSON from their opening content, rather than by filename suffix alone. See [manual batch submission](../prepared-normalization.md#manual-batch-submission) for a complete example.

<a id="inspect-the-normalization-result"></a>
<a id="what-should-you-look-for"></a>
<a id="save-the-execution-result"></a>
<a id="inspect-registered-datasets"></a>
<a id="show-one-registered-file"></a>
<a id="inspect-the-schema"></a>
<a id="preview-records"></a>
<a id="verify-a-registered-file"></a>
<a id="inspect-source-lineage"></a>
<a id="understand-processing-and-integrity-results"></a>
<a id="execution-status"></a>
<a id="verification-status"></a>
<a id="registration-status"></a>
<a id="integrity-status"></a>
<a id="why-are-these-statuses-separate"></a>
<a id="what-happens-when-some-jobs-fail"></a>
<a id="strict-mode"></a>
<a id="allow-partial-completion"></a>
<a id="what-does-allow-partial-actually-change"></a>
<a id="what-does-it-not-allow"></a>
<a id="when-should-you-use-it"></a>
<a id="find-execution-reports"></a>
<a id="where-are-reports-stored"></a>
<a id="what-does-the-execution-report-contain"></a>
<a id="why-keep-execution-reports"></a>
<a id="should-i-inspect-the-report-before-retrying"></a>

<a id="-allow-partial"></a>

### Execution status and partial results

Engine completion, output commit, verification, and registry registration are separate stages. Use `normalization`, `verification`, `registration`, and per-job outcomes in the retained execution result. `status = "complete"` means the managed execution verified and registered the eligible outputs for the completed batch. Manifest integrity describes processing quality, including degraded evidence, and is separate from successful registration or historical coverage.

For `execute` and `run`, the default withholds automatic registration when any job fails, even if other jobs committed valid outputs. `--allow-partial` permits automatic registration of successful jobs only when the Engine returns definitive outcomes for the whole batch, at least one output is verified, and registration succeeds. A verification failure still blocks registration. The resulting overall status is `partial`. This flag does not permit partial preparation or incomplete staging output.

The result points to the retained execution report, Engine outcome and execution intent. Use the [run history](runs.md) for the logical run ID, [publication evidence](../publication-normalized.md) for integrity and lineage, and [dataset inspection](../../workflows/datasets.md) for registered file IDs.

<a id="recover-a-completed-publication-without-rerunning"></a>
<a id="why-is-registration-only-recovery-useful"></a>
<a id="recover-using-an-execution-intent"></a>
<a id="recover-using-a-publication-manifest"></a>
<a id="register-a-specific-primary-file"></a>
<a id="register-an-entire-publication"></a>
<a id="does-recovery-rerun-engine"></a>
<a id="what-if-the-publication-is-already-registered"></a>
<a id="what-if-the-output-is-incomplete"></a>

## Registration and recovery

### `mf normalize register`

Use the original registry. Registration verifies existing committed output and writes registration receipts. It uses the Engine's output validator and does not run normalization jobs.

| Reference | Result |
| --- | --- |
| Retained `execution-intent.json` | Recheck the submitted batch fingerprint and registry identity, verify eligible committed job outputs against the retained evidence, and register them. Keep the sibling `submitted-batch.json` and execution evidence. |
| Publication directory or its `manifest.json` | Verify the committed publication and register its eligible primary files. |
| Canonical primary Parquet path | Resolve and verify its owning publication, then register the selected primary file. |

```bash
mf normalize register PATH_TO_EXECUTION_INTENT --testing | cpout
```

Replace the path with the retained `execution-intent.json` shown by the result. An explicit registration retry can register a verified subset of committed jobs. Inspect which outputs survived before recovery. When no usable publication was committed, fix the execution problem and prepare a fresh attempt. Registration recovery can reuse existing matching registrations. See [publication recovery](../publication-normalized.md#publication-recovery-and-registration).

## Profile commands

A normalization profile describes source interpretation. A normalization recipe selects registered inputs. Use the [profile authoring guide](../../workflows/catalogue/profiles.md) and [profile reference](../profiles.md) for authoring, validation, and installation. Run `mf normalize profiles --help` for the full subcommand inventory.

```bash
mf normalize profiles list | cpout
mf normalize profiles schema --type trades | cpout
mf normalize profiles guide trades | cpout
```

<a id="when-should-i-use-json"></a>

<a id="global-json"></a>

## JSON and exit status

Place global `--json` before `normalize`. It emits the command's structured result on stdout where supported. `new` still opens the editor. `edit` rejects JSON mode, as does `--tui`. See [shared CLI behavior](index.md) for global options.

| Code | Meaning |
| --- | --- |
| `0` | Successful local action, preview, preparation, complete execution, registered partial execution, or successful registration. A deletion preview can return zero while reporting a live reference. |
| `1` | Reported operation failure, blocked or partial preparation, interrupted execution returned as a report, unsuccessful registration, or deletion blocked during apply. |
| `2` | Usage, configuration, setup, or preflight error, including rejected Rust batch validation raised as a command error. |
| `130` | A keyboard interruption handled by the top-level CLI. |

<a id="common-questions"></a>
<a id="common-normalization-problems"></a>
<a id="the-normalization-recipe-is-invalid"></a>
<a id="preparation-finds-no-matching-archives"></a>
<a id="preparation-reports-missing-historical-specifications"></a>
<a id="the-required-profile-is-missing"></a>
<a id="the-rust-normalizer-is-missing"></a>
<a id="preparation-reports-an-unsupported-batch"></a>
<a id="the-submitted-batch-differs-from-preparation"></a>
<a id="normalization-reports-invalid-records"></a>
<a id="trade-quantities-appear-incorrect"></a>
<a id="trade-sides-are-reversed"></a>
<a id="timestamps-are-incorrect"></a>
<a id="depth-reconstruction-fails"></a>
<a id="the-processing-job-runs-out-of-resources"></a>
<a id="engine-writes-files-but-the-run-fails"></a>
<a id="postgresql-registration-fails-after-engine-completes"></a>
<a id="a-partial-run-contains-fewer-datasets-than-expected"></a>
<a id="dataset-inspection-shows-unexpected-row-counts"></a>
<a id="the-output-interval-differs-from-the-requested-interval"></a>

## Troubleshooting

| Symptom | First check |
| --- | --- |
| Missing inputs or a gap | Compare recipe scope, archive dates and statuses in [managed storage](../../workflows/acquisition/managing-acquisitions.md). |
| Missing specification or incompatible profile | Read the archive's preparation diagnostics and check [specification applicability](../../workflows/catalogue/specifications.md) and [installed profiles](../profiles.md). |
| Invalid or changed batch | Review the Rust diagnostics and reprepare against the intended registry and runtime. |
| Failed Engine job or unexpected output | Compare the retained execution report and [publication integrity evidence](../publication-normalized.md#integrity-evaluation). For Engine setup and resource configuration, see [runtime settings](../runtime.md). |
| Verified output with pending registration | Recover from the retained intent against the original registry. |
