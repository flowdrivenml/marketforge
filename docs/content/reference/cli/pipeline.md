# Pipeline commands

Apply one historical recipe through acquisition, normalization and merge. Plan and validate before starting. Use the [pipeline workflow](../../workflows/pipeline.md) for acquisition and reconciliation prerequisites and the [recipe contract](../../configuration/pipeline.md) for accepted settings.

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
                                [--stop-after {normalize.prepare}]
                                NAME
```

| Argument or option | Parser contract |
| --- | --- |
| `NAME` | Required. |
| `--testing`, `--production` | Mutually exclusive. Defaults to production. |
| `--tui` | Default `false`. |
| `--stop-after` | Optional. Accepted `normalize.prepare`. |

### `mf pipeline resume`

```text
mf pipeline resume [-h] [--testing | --production] [--tui] RUN_ID
```

| Argument or option | Parser contract |
| --- | --- |
| `RUN_ID` | Required. |
| `--testing`, `--production` | Mutually exclusive. Defaults to production. |
| `--tui` | Default `false`. |

### `mf pipeline status`

```text
mf pipeline status [-h] RUN_ID
```

| Argument or option | Parser contract |
| --- | --- |
| `RUN_ID` | Required. |

## Results and effects

- `validate` resolves the managed recipe and returns `schema_version: 1`, `kind: "historical_pipeline_validation"`, `state: "valid"`, the source path and normalized recipe.
- `plan` reads registered evidence and reports deferred work. It starts no provider requests and writes no processing artifacts or registry rows. JSON contains `schema_version: 1`, `kind: "historical_pipeline_plan"`, `state`, `source`, `recipe`, `environment`, `inventory`, `stages` and `blockers`.
- `run` creates a retained run, can download data, executes processing children, and registers verified outputs. `--stop-after normalize.prepare` retains a validated batch before normalization execution. `resume` verifies the original run context and pins before moving to the next eligible step.
- `status` reads the retained report and returns zero when it can read it, even for an interrupted or failed run. Inspect the report's state. `run` and `resume` return 1 for `failed`, 2 for `blocked`, 130 for `interrupted`, and 0 for successful or deliberately stopped runs.
- `--tui` and global `--json` are mutually exclusive. The dashboard needs interactive input and stderr. Redirected terminal output uses a plain summary. Global JSON returns the report object.

For the persisted run, stage, phase and receipt relationships, see [Forge preparation and run evidence](../prepared-forge.md).
