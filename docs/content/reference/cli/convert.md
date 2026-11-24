# Conversion commands

Manage conversion recipes and submit quote-conversion jobs for verified merge publications. Run `mf convert --help` or `mf convert config --help` for the command groups. Each group requires a subcommand. [Workflow and prerequisites](../../workflows/conversion.md) · [Shared CLI behavior](index.md)


## Inspect and maintain recipes

Commands in this section use the local managed library and accept no environment flag. See [collection sorting](sorting.md) for sort behavior. `REFERENCE` can be a managed name or explicit TOML path. `NAME` for deletion is a managed name with an optional `.toml` suffix.

| Syntax after `mf convert` | Effects and output |
| --- | --- |
| `list [--sort name] [--order asc\|desc]` | Reads managed recipes in name order (ascending by default). Shows the binding state and records inspection diagnostics for invalid entries. Global `--json` returns a versioned `recipes` array. |
| `show REFERENCE` | Reads and presents source binding, interval, selectors, policy, resources and reference selection. JSON returns the recipe inspection document. A malformed recipe carries diagnostics and exits 1. A changed upstream binding is displayed as stale and fails local validation. |
| `edit REFERENCE` | Opens a private editor draft and checks it before replacement. Requires a terminal and rejects global `--json`. An invalid draft leaves the original in place. The resulting recipe inspection is shown on return. |
| `delete NAME [--apply [--force]]` | Previews by default. Apply removes the managed recipe after checking live references. Force requires apply and permits breaking the reported references. Human output lists affected references or unreadable recipes, and JSON returns the versioned deletion result. A blocked apply exits 1. Historical data and retained evidence are outside the operation. |

```bash
mf convert list --order desc
mf --json convert show getting_started_usdt
mf convert edit getting_started_usdt
mf convert delete getting_started_usdt
```

## Synopsis and arguments

### `mf convert new`

```text
mf convert new NAME (--from-merge SOURCE | --from-normalize SOURCE |
                     --from-acquisition SOURCE | --from-publication MERGED_PUBLICATION_ID)
                    --target-quote CURRENCY [--testing | --production]
```

| Argument or option | Meaning |
| --- | --- |
| `NAME` | Name of the new managed conversion recipe. The destination must be unused. |
| `--from-merge SOURCE` | Existing merge recipe name or TOML path. Numeric names still select recipes. |
| `--from-normalize SOURCE` | Existing normalization recipe name or TOML path. |
| `--from-acquisition SOURCE` | Existing acquisition recipe name or TOML path. |
| `--from-publication MERGED_PUBLICATION_ID` | Registry publication ID of one eligible committed merge in the selected environment. Get it from the [publication view](datasets.md). |
| `--target-quote CURRENCY` | Required uppercase ASCII alphanumeric currency code, such as `USDT`. |
| `--testing`, `--production` | Mutually exclusive. Defaults to production. Publication lookup uses this registry. |

Supply exactly one source option. The first two commands below are alternatives for the same destination name. Replace `MERGED_PUBLICATION_ID` with the registry publication ID shown by `mf datasets list --testing --view publications --operation merge`. Engine IDs, primary-file IDs, job or run IDs, recipe names, and paths identify other artifacts.

```bash
mf convert new getting_started_usdt --from-merge getting_started --target-quote USDT --testing
mf convert new getting_started_usdt --from-normalize getting_started --target-quote USDT --testing
mf convert new merged_usdt --from-publication MERGED_PUBLICATION_ID --target-quote USDT --testing
```

Recipe-based creation fingerprints the source TOML locally. Publication-based creation reads the selected registry and checks the merge’s committed metadata before writing the recipe. Missing, incompatible, superseded, retired, or invalidated publications stop creation. To choose a replacement, supply its ID explicitly. Physical verification and reference resolution happen during preparation. Creation leaves processing and execution readiness for later.

### `mf convert validate`

```text
mf convert validate [-h] reference
```

| Argument or option | Parser contract |
| --- | --- |
| `reference` | Required. |

### `mf convert prepare`

```text
mf convert prepare [-h] [--testing | --production] [--dry-run] reference
```

| Argument or option | Parser contract |
| --- | --- |
| `reference` | Required. |
| `--testing`, `--production` | Mutually exclusive. Defaults to production. |
| `--dry-run` | Default `false`. |

### `mf convert run`

```text
mf convert run [-h] [--testing | --production] [--tui] reference
```

| Argument or option | Parser contract |
| --- | --- |
| `reference` | Required. |
| `--testing`, `--production` | Mutually exclusive. Defaults to production. |
| `--tui` | Default `false`. |


## Command inputs and effects

| Action | Input and effects | Current JSON result |
| --- | --- | --- |
| `new` | A new managed name, exactly one upstream selector and an explicit target currency. Binds an upstream recipe fingerprint or an exact registered merge and writes a new recipe. Interactive human mode opens the editor | `status: "template_created"`, `path`, and `source` with the retained declaration. Direct selection also reports binding, execution scope, reference review and execution readiness |
| `validate` | Managed recipe name, TOML path, individual job JSON, or retained Forge collection. A recipe check validates its structure and local constraints, plus the source fingerprint for recipe-based bindings. A publication declaration is checked locally; current registry eligibility is checked during preparation. Forge validates a standalone job or checks collection pins and validates its individual jobs with the managed converter | `status: "valid"`, `path`, `validation`, and `recipe` for TOML. JSON job checks return `validation_layers` and `validated_job_count` |
| `prepare` | A recipe and the selected guarded registry. Resolves complete registered sources and references, then generates job documents and validates them with the installed converter if available. Dry run performs selection and computes job specifications without validating newly planned jobs or writing preparation artifacts. It can revalidate an earlier job when checking reuse | `prepare-report.json` shape described below |
| `run` | Managed recipe, TOML path, retained collection or retained individual job path. A recipe is prepared first. A retained submission reuses its pinned inputs. This action can publish converted data. Forge indexes verified converted outputs without catalogue registration | Run status and execution results with artifact and publication evidence |

`--tui` is an execution option and is mutually exclusive with global `--json`. An action report with `status` of `blocked`, `failed`, or `interrupted` returns exit 1. A retained `generated` preparation with unvalidated compatibility returns exit 1. A successful dry run can return exit 0 without Rust job validation. Parse and handled configuration errors use the [shared CLI rules](index.md).

## Preparation result

Read `status` and `compatibility` together. `blocked` identifies failed selection, evidence or metadata and lists `blockers`. `generated` identifies a successfully constructed job specification. The CLI uses `status: "generated"` even when the production parser passes. In that case, `compatibility: "validated"` records an installed binary that validated every saved job with no remaining compatibility blockers. Otherwise `compatibility: "not_validated"` retains the exact reasons in `compatibility_blockers`.

A persisted preparation can also return `status: "complete"`, `disposition: "reused"` and `execution: "not_started"` when all jobs match verified completed work.

The report identifies requested and effective publication scope, the upstream recipe fingerprint or exact publication binding, source and external reference publications, resolved streams and effective settings. `job_path` points to the Forge collection for generated submissions or is null on dry run. `report_path` identifies the saved report when written. In a persisted report `jobs[]` contains `path` and `sha256`, an integer `job_id`, destination `output`, selected `references`, source registry publication ID and string Engine publication ID, and `engine_validation` of `not_run`, `passed`, or `failed`. Successful validation can include `validation_output`. The dry-run job records `intended_job_path` in place of the saved path and digest. `artifacts` records file pins when persisted.

## Runtime configuration commands

Commands below operate on the local [base runtime](../configuration/conversion-runtime.md) with the managed converter. They accept no registry flag. Fields use dotted paths, and size fields also accept exact binary sizes such as `1GiB`. Global `--json` returns a versioned configuration report with action, status, path, field or change details as applicable and diagnostics on errors. The editor requires a terminal and rejects JSON mode.

| Syntax after `mf convert config` | Effect and human result |
| --- | --- |
| `show [--all \| --section SECTION]` | Read the base file and display configured and effective settings. Choose one expanded view, or omit both for the summary. |
| `get FIELD` | Read one supported setting with its configured value, effective value and default. |
| `schema [--field FIELD]` | Ask the installed binary for the current schema and show available fields or the requested field. |
| `validate` | Run Rust validation of the complete base runtime and report success or diagnostics. |
| `set FIELD VALUE [--apply] [--acknowledge-risk]` | Parse a typed value and validate the candidate document. Preview by default. Apply atomically after checking for concurrent changes. |
| `unset FIELD [--apply] [--acknowledge-risk]` | Preview or apply removing an optional field so its loader default is used. Required and immutable fields are rejected. |
| `edit` | Edit a private copy, validate it and review the diff. Type `apply` at the terminal to save an ordinary change. |

`--acknowledge-risk` requires `--apply` and is reserved for integrity-critical changes. The converter schema currently contains ordinary and resource-sensitive fields. Missing runtime files, incompatible binaries, unknown paths, invalid values and conflicts produce diagnostics. Rebuild an incompatible managed binary with `mf engine build convert`.

```bash
mf convert config show --section parquet
mf convert config get resources.max_open_files
mf convert config schema --field parquet.compression_level
mf convert config validate
mf convert config set resources.memory_limit_mb 1GiB
mf convert config unset parquet.compression_level
mf convert config edit
```

## Execution and inspection

For a noninteractive run, omitting `--tui` is the default. Interactive monitoring requires an attached terminal and a managed converter with structured result handoff. The human report identifies job outcomes, available publication evidence and the indexed output reference. JSON includes per-job results and `not_attempted_job_ids`. See [execution and reuse](../../concepts/conversion.md#execution-and-reuse) for disposition and recovery.

```bash
mf convert validate getting_started_usdt
mf --json convert prepare getting_started_usdt --testing --dry-run
mf convert run PATH_TO_CONVERT_JSON --testing
```

[Conversion workflow](../../workflows/conversion.md) · [Recipe fields](../configuration/conversion.md) · [Prepared job](../data-formats/prepared-conversion.md) · [Converted formats](../data-formats/publication-converted.md)
