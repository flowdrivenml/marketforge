# Merge commands

Recipe listings default to ascending name and accept `--order desc`. See [collection sorting](sorting.md).

Use the [merge guide](../../workflows/merge.md) for a complete recipe-first workflow. See the [recipe fields](../configuration/merge.md), [runtime settings](../configuration/merge-runtime.md), and [prepared job contract](../data-formats/prepared-merge.md) for their respective schemas.

<a id="command-reference"></a><a id="recipe-names-and-paths"></a>
<a id="which-commands-change-state"></a><a id="why-does-new-require-a-managed-name"></a>

## Synopsis and arguments

Commands in the tables follow `mf merge`. `NAME` identifies a managed recipe, optionally with a `.toml` suffix. `RECIPE` accepts that name or a directory-qualified or absolute TOML path. Explicit paths resolve from the command's working directory.

| Command | Syntax after `mf merge` | Reads and effects |
| --- | --- | --- |
| [new](#mf-merge-new) | `new NAME [--from-normalize SOURCE] [--testing or --production]` | Creates a managed TOML. Source-derived creation also discovers registry candidates. |
| [edit](#recipe-management) | `edit RECIPE` | Opens a draft in the editor and replaces the file after local validation and a conflict check. |
| [list](#recipe-management) | `list` | Reads the managed library and reports local parse and structure status. |
| [show](#recipe-management) | `show RECIPE` | Reads a recipe and explains configured selection, ordering and precedence. |
| [validate](#mf-merge-validate) | `validate RECIPE` | Checks local structure and, for derived recipes, the local source reference. |
| [delete](#delete-a-recipe) | `delete NAME [--apply [--force]]` | Previews or removes one managed recipe after checking live references. |
| [prepare](#mf-merge-prepare) | `prepare RECIPE [--dry-run] [--testing or --production]` | Resolves registry and local evidence. Persisted preparation writes job and report evidence and attempts Rust validation. |
| [run](#mf-merge-run) | `run REFERENCE [--tui] [--testing or --production]` | Runs a recipe or submits a prepared job. Verifies output and registers eligible primary files. |
| [register](#mf-merge-register) | `register REFERENCE [--testing or --production]` | Verifies existing committed output and registers or reuses eligible primary-file records. |
| [config](#engine-runtime-configuration) | `config SUBCOMMAND …` | Inspects or changes the base Engine runtime. |

Production is the default for registry-backed actions. Use `--testing` for the testing registry. Prepare, run and register need PostgreSQL and local input or output evidence. Plain recipe creation and local recipe operations need no database. The runtime commands require a managed Engine and use local files.

## Identifiers

| Identifier | Meaning and where to find it | Where to use it |
| --- | --- | --- |
| Forge run ID | Logical orchestration run from the result or run history | `mf runs show RUN_ID` and `mf runs report RUN_ID` |
| Registry publication ID | Registry record for one publication, which can contain multiple files and streams. Find it in publication results or the dataset publication view | `selection.publication_ids` for normalized inputs and `mf datasets show --publication-id PUBLICATION_ID` |
| Registered primary-file ID | Registry record for one physical Parquet primary. Find it in file inventories or registration results | `dataset_ids` for explicit normalized inputs and positional `mf datasets show FILE_ID` or `mf datasets verify FILE_ID` |
| Engine publication ID | Engine-side publication identity from output evidence, exposed as `engine_dataset_id` | Engine manifest and provenance bindings |
| Engine job ID | Submitted job's `job_id` from the prepared job or execution report | Engine diagnostics and retained job evidence |

Run and dataset inspection require the registry environment associated with the identity. The execution intent path is a separate registration-recovery reference.

## Recipe management

### `mf merge new`

```bash
mf merge new first_merge --from-normalize my_dataset --testing
```

Creates a new managed recipe exclusively. Without `--from-normalize` it creates a template with illustrative file IDs. With the option, it accepts a managed normalization recipe name or an explicit TOML path, copies the interval, saves the source reference, and reports discovery from the selected registry. Discovery blockers can appear in a successfully created draft. A human interactive invocation opens an editor. Global JSON and noninteractive invocations create the file without opening an editor. Creation precedes editing, so an editor or validation failure can leave the original draft. The discovery report comes from before the editor. Preview preparation again after editing.

### Edit, list, and show

```bash
mf merge edit first_merge
mf merge list | cpout
mf merge show first_merge | cpout
```

`edit` opens a temporary draft using the configured editor or its Neovim fallback. It checks recipe structure and applicable source reference before replacing the original, and refuses a changed original. An invalid draft leaves the original intact and its path is reported for correction. Editing requires a terminal and rejects global JSON.

`list` keeps malformed recipes visible with an invalid status. `show` explains declared selection and ordering without resolving registry candidates or validating the referenced normalization file. Use `--json` before `merge` for the structured inventory or recipe fields.

### `mf merge validate`

```bash
mf merge validate first_merge | cpout
```

Checks both recipe shapes and loads the referenced normalization recipe for the derived form. Use preparation to verify eligible files, matching provenance, ranks and Engine compatibility.

### Delete a recipe

```bash
mf merge delete first_merge | cpout
```

Preview accepts managed names only and can inspect a malformed target TOML because the target is treated as a file. Forge checks managed conversion recipes for live references and lists recipes it could not inspect. Detected live references block apply. After reviewing the preview, use `mf merge delete first_merge --apply` to remove the recipe. `--apply --force` allows breaking detected references and still enforces managed scope and file safety. Apply checks the file's identity before unlinking and rejects unsafe entries or a changed target during the operation. The preview and a later apply each inspect the current file.

Deletion affects only the managed recipe. Datasets, run evidence, runtime configuration and registry records are outside its scope. Explicit publication deletion is a separate [Datasets operation](datasets.md).

<a id="can-i-merge-only-one-depth-parquet-part"></a><a id="depth-layout-compatibility"></a>
<a id="does-merging-repair-missing-order-book-updates"></a><a id="preparation-failure"></a>
<a id="what-does-merge-preparation-verify"></a><a id="what-is-segmented-depth"></a>
<a id="what-preparation-does-not-prove"></a><a id="why-does-a-depth-publication-have-auxiliary-files"></a>
<a id="why-does-a-snapshot-matter"></a>

## Preparation

### `mf merge prepare`

```bash
mf merge prepare first_merge --testing --dry-run | cpout
mf merge prepare first_merge --testing | cpout
```

Preparation resolves source selection, registered file and publication identities, checksums and stream ownership. It derives ranks and ordering and checks observed bounds. Persisted preparation writes evidence and asks the managed Rust Engine to validate the job. See [preparation evidence](../data-formats/prepared-merge.md#preparation-evidence).

| Status | Meaning |
| --- | --- |
| `planned` | Dry-run selection passed Forge's checks. Paths are prospective. Rust job validation did not run. |
| `generated` | Forge saved a job, but successful Rust validation is unavailable. Read `validation_error`. Standalone `prepare` can return zero for this status. |
| `prepared` | Rust validated the generated job and compatibility is `validated`. This is the usable preparation result. |
| `blocked` | Selection or provenance could not be resolved. Persisted derived preparation can retain a blocker report. Persisted direct selection errors may instead return a command error. |
| `complete` with `disposition = "reused"` | Persisted preparation found a verified completed merge. Use its existing output identities and path. |

Dry run reads registry and local evidence, including input files. It skips the job, report, and execution registry writes. Selection and provenance checks are distinct from validating the job with Rust.

<a id="execution-failure"></a><a id="find-merge-execution-reports"></a>
<a id="what-about-prepared-json"></a><a id="what-does-the-report-contain"></a>
<a id="what-happens-when-a-merge-fails"></a><a id="why-inspect-the-report-before-retrying"></a>

## Execution

### `mf merge run`

```bash
mf merge run first_merge --testing
```

A recipe reference runs the full prepare, execute, verify, and register sequence. A prepared job JSON reference, such as `mf merge run ./PATH_TO_MERGE_JOB.json --testing`, submits that job and checks it again. Use the exact reported path. `run` distinguishes a JSON object from a recipe by file content. If a bare local file and a managed recipe both match, qualify the intended path. Use the original registry and keep a prepared job with its report and retained runtime. Standalone JSON without retained registry identity is accepted only in testing, subject to job and registry checks.

```bash
mf merge run first_merge --testing --tui
```

`--tui` opens the Rust progress monitor on interactive stdin, stdout and stderr. It is incompatible with global JSON. The dashboard shows input verification, timeline construction, output writing, and physical verification as separate phases. It distinguishes active partitions from the worker ceiling. Keep the terminal attached through shutdown when cancelling. Forge prints the final managed result after the monitor closes. Engine commit is followed by Forge verification and registration. In ordinary runs Forge captures Engine stdout and stderr in the execution directory where available and prints the final Forge result on stdout. The result points to the retained intent, submission, Engine outcome and Forge execution report. For standalone Forge execution, look under the configured run root in `merge-execute-…/execution-result.json`. Engine diagnostics can include `engine-stderr.log` or `engine-tui.log`. An interruption can leave only partial evidence or a committed output. Check the output and retained state before retrying.

## Completed-work reuse

The merge work index keys by registry identity, executable SHA-256, the ordered job and verified input evidence, and effective runtime settings. It excludes the generated job ID and destination directory. A changed selected file, manifest, rank, ordering, interval, effective runtime value or binary fingerprint can produce a different key. A recipe name alone has no bearing on the key.

Dry-run `reuse_plan.status` can report `would_execute`, `would_reuse`, `needs_registration_recovery`, `in_progress` or `blocked`. This is a lookup snapshot. A new preparation can return an existing verified result instead of creating a new prepared job. Execution rechecks indexed output against the production verification and registered-inventory contract. Successful reuse has `status = "complete"`, `disposition = "reused"` and `engine_execution_skipped = true`. It returns the actual existing path and registered identities. An active claim can block a competing execution. A committed indexed attempt with a recoverable intent can enter registration recovery before the claim is acquired.

| Receipt state | What to inspect |
| --- | --- |
| `execution = "complete"` | Engine outcome and commit evidence. Read verification and registration separately. |
| `verification = "verified"` | Manifest, checksums, physical parts and input lineage passed Forge's verification. |
| `registration = "registered"` | Registered output identities and registration receipt. |
| `verification = "failed"` or registration failure | The run can retain committed output. Read the specific verification or registration error before retrying. |
| `status = "failed"` or `"interrupted"` | Engine outcome, diagnostics and intent where retained. Confirm whether a commit exists. |

<a id="does-recovery-rerun-the-merger"></a><a id="does-verification-prove-historical-completeness"></a>
<a id="does-verification-rerun-normalization"></a><a id="how-forge-verifies-a-merge"></a>
<a id="publications-and-inspection"></a><a id="recover-a-completed-merge-without-rerunning"></a>
<a id="recover-from-a-manifest"></a><a id="recover-from-a-publication-directory"></a>
<a id="recover-from-an-execution-intent"></a><a id="recover-one-primary-file"></a>
<a id="registration-failure"></a><a id="what-does-recovery-verify"></a>
<a id="what-does-verification-establish"></a><a id="what-if-the-merge-never-completed-publication"></a>
<a id="what-if-the-publication-is-already-registered"></a><a id="why-is-registration-only-recovery-useful"></a>
<a id="why-is-verification-necessary"></a>

## Registration and recovery

### `mf merge register`

```bash
mf merge register PATH_TO_EXECUTION_INTENT --testing | cpout
```

Replace the operand with the retained `execution-intent.json`. Use the original environment and registry. Intent recovery checks registry identity, the submitted job fingerprint, input lineage and the expected committed output. A committed publication directory or its `manifest.json` selects all eligible primary files, and a canonical primary Parquet path selects that file. A Forge run ID and staging directory are not registration references.

Forge uses the managed Engine to validate existing output, checks the physical provenance, and writes registration receipts. It can reuse existing matching registry records. Registration does not rerun the merger. If no verified committed output exists, correct the execution failure and prepare a new attempt. See [publication identities](../data-formats/publication-merge.md#identities-and-counts).

## Engine runtime configuration

Every leaf operates on the base `data/.config/engine/merge.toml` and takes no database option. Sections are `resources`, `parquet` and `partitioning`. See the [runtime reference](../configuration/merge-runtime.md) for types, defaults, validation, precedence and safe editing.

| Command after `mf merge config` | Purpose | Example after `mf merge config` |
| --- | --- | --- |
| `show [--all or --section SECTION]` | Read the overview, all fields, or a table section. | `show --section resources` |
| `get FIELD` | Read one dotted field with its configured and effective values. | `get resources.workers` |
| `schema [--field FIELD]` | Inspect installed Rust schema metadata. | `schema --field resources.memory_limit_mb` |
| `validate` | Validate the whole base file with Rust. | `validate` |
| `set FIELD VALUE [--apply] [--acknowledge-risk]` | Preview or apply one typed change. | `set resources.memory_limit_mb 4GiB` |
| `unset FIELD [--apply] [--acknowledge-risk]` | Preview or remove an optional leaf. | `unset resources.max_open_files` |
| `edit` | Open a private draft, validate, review, and confirm before saving. | `edit` |

`schema` can still describe the installed schema when the base document fails validation and reports that failure separately. `show` and `get` require successful base validation. JSON inspection returns fields and validation state. JSON previews and applies return structured changes and status.

Config write commands preview by default and require `--apply` to save. The shared parser accepts `--acknowledge-risk` only with `--apply`. No current merge runtime field is classified as integrity-critical. Config errors return 1 and interruption can return 130. Interactive editing rejects JSON.

<a id="json-output"></a><a id="save-a-merge-result"></a>
<a id="why-use-json"></a>

## JSON output and exit status

Place global `--json` before `merge`. Stdout contains the structured result, while command errors can be emitted to stderr. Execution receipts use schema version 1 and keep numeric IDs, counters and nanosecond timestamps exact as JSON integers. The prepared-job wire contract separately encodes the pinned input Engine publication ID as decimal text. Persisted operation output may also carry the logical run identity.

```bash
mf --json merge run first_merge --testing > merge-result.json
```

Check `status`, `execution`, `verification`, and `registration`. Registered primary-file IDs are the values of `catalogue_dataset_ids`, keyed by output path. Use them with [dataset inspection](../../workflows/inspection/datasets.md). See [run inspection](runs.md) for the logical Forge run record.

| Exit | Meaning |
| --- | --- |
| `0` | Successful local action, preview, accepted preparation result, completed registered merge, or successful registration. Read preparation status even on zero. Deletion preview can report blockers on zero. |
| `1` | Blocked preparation, failed operation, registration failure, unvalidated generated result on recipe run, an execution receipt with status `interrupted`, or deletion blocked during apply. |
| `2` | Usage, configuration, or preflight error reported by the top-level CLI. |
| `130` | Keyboard interruption caught by the top-level CLI. A managed interruption returned as a merge receipt uses exit 1. |
