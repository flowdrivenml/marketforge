# Pipeline commands

Use `mf pipeline list --sort name --order desc` to browse managed recipes by name in reverse order. See [collection sorting](sorting.md).

Apply one historical recipe through acquisition, normalization, merge, and optional conversion. Plan and validate before starting. Use the [pipeline workflow](../../workflows/pipeline.md) for acquisition and reconciliation prerequisites and the [recipe contract](../configuration/pipeline.md) for accepted settings.

## Synopsis and arguments

### `mf pipeline validate`

```text
mf pipeline validate [-h] NAME
```

| Argument or option | Parser contract |
| --- | --- |
| `NAME` | Required. |

### `mf pipeline plan`

```text
mf pipeline plan [-h] [--testing | --production] NAME
```

| Argument or option | Parser contract |
| --- | --- |
| `NAME` | Required. |
| `--testing`, `--production` | Mutually exclusive. Defaults to production. |

### `mf pipeline run`

```text
mf pipeline run [-h] [--testing | --production] [--tui]
                [--stage-terminal {auto,window,inline}]
                [--stop-after {normalize.prepare}]
                                NAME
```

| Argument or option | Parser contract |
| --- | --- |
| `NAME` | Required. |
| `--testing`, `--production` | Mutually exclusive. Defaults to production. |
| `--tui` | Show the parent pipeline dashboard. Default off in inline mode. |
| `--stage-terminal` | `auto` (default), `window`, or `inline`. See modes below. |
| `--stop-after` | Optional. Accepted `normalize.prepare`. |

### `mf pipeline resume`

```text
mf pipeline resume [-h] [--testing | --production] [--tui]
                   [--stage-terminal {auto,window,inline}] RUN_ID
```

| Argument or option | Parser contract |
| --- | --- |
| `RUN_ID` | Required. |
| `--testing`, `--production` | Mutually exclusive. Defaults to production. |
| `--tui` | Show the parent pipeline dashboard. Default off in inline mode. |
| `--stage-terminal` | `auto` (default), `window`, or `inline`. See modes below. |

### `mf pipeline status`

```text
mf pipeline status [-h] RUN_ID
```

| Argument or option | Parser contract |
| --- | --- |
| `RUN_ID` | Required. |

## Stage terminal modes

| Mode | Behavior |
| --- | --- |
| `auto` | Use Linux Ghostty when the parent is interactive and the desktop is reachable. A startup failure reports the reason and allows inline fallback before execution is authorized. JSON and noninteractive calls use inline mode. |
| `window` | Require supported Linux Ghostty. A startup failure returns an error. Explicit window mode can be paired with global `--json` to keep parent stdout machine-readable. |
| `inline` | Keep execution in the parent terminal. Applies to headless use and macOS. |

The option applies to `run` and `resume`. It affects presentation and process supervision. The resolved processing request and work-equivalence key are independent of the terminal. The parent dashboard and any managed Engine monitor use separate terminals in window mode.

## Results and effects

- `validate` resolves the managed recipe and returns `schema_version: 1`, `kind: "historical_pipeline_validation"`, `state: "valid"`, the source path and normalized recipe.
- `plan` reads registered evidence and reports deferred work. It starts no provider requests and writes no processing artifacts or registry rows. JSON contains `schema_version: 1`, `kind: "historical_pipeline_plan"`, `state`, `source`, `recipe`, `environment`, `inventory`, `stages` and `blockers`.
- `run` creates a retained run, can download data, executes processing children, and records verified outputs. Normalization and Merge register publications. Conversion indexes verified output. `--stop-after normalize.prepare` retains a validated batch before normalization execution. `resume` verifies the original run context and pins before moving to the next eligible step.
- `status` reads the retained report and returns zero when it can read it, even for an interrupted or failed run. Inspect the report's state. `run` and `resume` return 1 for `failed`, 2 for `blocked`, 130 for `interrupted`, and 0 for successful or deliberately stopped runs.
- `--tui` and global `--json` are mutually exclusive. The dashboard needs interactive input and stderr. Redirected terminal output uses a plain summary. Global JSON returns the report object.

For the persisted run, stage, phase and receipt relationships, see [Forge preparation and run evidence](../data-formats/prepared-forge.md).

## Recipe inventory and creation

| Command | Arguments and options | Effect and result |
| --- | --- | --- |
| `mf pipeline list [--sort name] [--order asc\|desc]` | No positional arguments. Name sort and ascending order are the defaults | Reads managed recipes locally. Human output shows stage count, request count, interval, conversion target and structural validity. JSON preserves `schema_version` and the ordered `recipes` name list. An empty library returns zero |
| `mf pipeline new NAME` | Required managed name, with optional `.toml` suffix | Creates a starter recipe and refuses an existing destination. JSON returns `status: "draft_created"` and `path` |

Use a name made of letters, digits, underscores or hyphens, starting with a
letter or digit. Recipe commands accept a managed basename with or without
`.toml`. Directory paths are rejected. `status` and `resume` take exact
retained run IDs. Database flags belong to `plan`, `run` and `resume`.
Human `status` reads the retained report and uses its recorded environment
and registry UUID for an optional index lookup. JSON status returns the
retained report without that extra lookup.

```bash
mf pipeline new example
mf pipeline list --order desc
mf pipeline validate example.toml
mf pipeline plan example --testing
mf pipeline run example --testing --stage-terminal inline
mf pipeline status RUN_ID
mf pipeline resume RUN_ID --testing --stage-terminal inline
```

Local validation success returns zero and confirms the declared structure,
including enabled stages and conversion options. Invalid TOML, unknown recipes,
unreadable reports, and unsupported arguments return 2. A blocked plan also
returns 2 and identifies missing runtime or registry evidence in its summary.
The runtime checks are reported separately from deferred preparation.
Machine plan output adds `runtime_availability` to the existing fields.

A completed run needs no further stage work on resume. When the summary
reports an incomplete run-history index, use exact metadata reconciliation.

```bash
mf runs reconcile --run-id RUN_ID --testing
mf runs reconcile --run-id RUN_ID --testing --apply
```

The selector reviews the latest indexed attempt of that run in the selected
workspace and registry. Apply revalidates retained identity and report
evidence. Successful processing and run-history finalization are separate
results. A run-history warning preserves the processing exit status.
