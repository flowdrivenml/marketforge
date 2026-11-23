# Conversion commands

Generate and submit quote-conversion jobs for verified merge publications. [Workflow and prerequisites](../../workflows/conversion.md) · [Shared CLI behavior](index.md)


## Recipe and runtime tools

| Command | Effect |
| --- | --- |
| `mf convert list` | Inspect the managed conversion recipe library locally. |
| `mf convert show RECIPE` | Inspect the declared policy and current upstream binding. |
| `mf convert edit RECIPE` | Validate and save an editor draft. Requires a terminal. |
| `mf convert delete NAME [--apply [--force]]` | Preview or remove one managed recipe. Its publications and retained artifacts remain available. |
| `mf convert config show [--all \| --section SECTION]` | Inspect base runtime settings. |
| `mf convert config get FIELD` | Inspect one configured and effective field. |
| `mf convert config schema [--field FIELD]` | Read the managed Rust converter's schema. |
| `mf convert config validate` | Validate the complete base runtime through Rust. |
| `mf convert config set FIELD VALUE [--apply]` | Preview or atomically apply a typed change. |
| `mf convert config unset FIELD [--apply]` | Preview or restore an optional field's default. |
| `mf convert config edit` | Validate a private draft and confirm before saving. Requires a terminal. |

The [runtime reference](../runtime.md#merge-and-conversion-runtime) describes the base file and its relationship to prepared jobs. Run `mf engine build convert` if the installed converter lacks runtime schema inspection or managed TUI support.

Execution reports verified publications and their reusable output references. Use `mf datasets show --output-ref REF --testing` for a testing result. Converted outputs use the durable output index. Catalogue registration is unavailable.

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
| `validate` | Managed recipe name, TOML path, or individual job JSON. A recipe check validates its structure and local constraints, plus the source fingerprint for recipe-based bindings. A publication declaration is checked locally; current registry eligibility is checked during preparation. A job check runs the managed converter’s `--validate`; install it first. The Forge collection is not a standalone Rust job | `status: "valid"`, `path`, `validation`, and `recipe` for TOML |
| `prepare` | A recipe and the selected guarded registry. Resolves complete registered sources and references, then generates job documents and validates them with the installed converter if available. Dry run performs selection and computes job specifications without Rust job validation or writing preparation artifacts | `prepare-report.json` shape described below |
| `run` | Managed recipe, TOML path, retained collection or retained individual job path. A recipe is prepared first. A retained submission reuses its pinned inputs. This action can publish converted data. Forge indexes verified converted outputs without catalogue registration | Run status and execution results with artifact and publication evidence |

`--tui` is an execution option and is mutually exclusive with global `--json`. An action report with `status` of `blocked`, `failed`, or `interrupted` returns exit 1. A retained `generated` preparation with unvalidated compatibility returns exit 1. A successful dry run can return exit 0 without Rust job validation. Parse and handled configuration errors use the [shared CLI rules](index.md).

## Preparation result

Read `status` and `compatibility` together. `blocked` identifies failed selection, evidence or metadata and lists `blockers`. `generated` identifies a successfully constructed job specification. The CLI uses `status: "generated"` even when the production parser passes. In that case, `compatibility: "validated"` records an installed binary that validated every saved job with no remaining compatibility blockers. Otherwise `compatibility: "not_validated"` retains the exact reasons in `compatibility_blockers`.

The report identifies requested and effective publication scope, the upstream recipe fingerprint or exact publication binding, source and external reference publications, resolved streams and effective settings. `job_path` points to the Forge collection or is null on dry run. `report_path` identifies the saved report when written. In a persisted report `jobs[]` contains `path` and `sha256`, an integer `job_id`, destination `output`, selected `references`, source registry publication ID and string Engine publication ID, and `engine_validation` of `not_run`, `passed`, or `failed`. Successful validation can include `validation_output`. The dry-run job records `intended_job_path` in place of the saved path and digest. `artifacts` records file pins when persisted.

## Compatibility and publication

Current standalone conversion consumes chronological and causal physical merge publications under its validation rules. Matching depth membership does not prevent a configured trade stream from being a reference. Trade-only execution uses internal references. Depth-only requires an external trade-only merge. Combined can bind selected internal trades and supported external references. The policy can consider multiple currency paths within the configured hop limit. It still needs eligible prior observations at event execution.

Instrument metadata is embedded in the prepared job. Forge checks the workspace preparation catalogue against verified historical evidence if the file exists. The Engine does not read a fixed instrument catalogue to run a standalone job. Atomic conversion publication currently requires Linux. Committed converted files can be inspected through Arrow. Conversion registration is unavailable.

```bash
mf convert validate NAME
mf --json convert prepare NAME --testing --dry-run
mf convert run PATH_TO_CONVERT_JSON --testing
```

See [conversion recipes](../../configuration/conversion.md), [prepared Engine job](../prepared-conversion.md), [Forge collection and run evidence](../prepared-forge.md), [conversion workflow](../../workflows/conversion.md), and [converted publication format](../publication-converted.md).
