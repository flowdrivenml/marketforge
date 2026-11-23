# Application settings

Forge reads an explicit global `--config PATH` relative to the caller's directory, or the workspace's default `data/.config/forge/marketforge.toml`. If the default is absent, Forge uses built-in settings. A legacy default without its migrated counterpart produces a migration diagnostic. An explicit missing path is an error. Select the workspace with `--project-root`, then `MARKETFORGE_PROJECT_ROOT`, then discovery from the current directory. A workspace has `forge/pyproject.toml` and `engine/`. Storage path values are resolved from the workspace. The recipe libraries continue to live under its configuration directory.

## Fields and defaults

All fields below are optional. Unknown keys and unintended type coercions are rejected.

| Path | Type and constraint | Default and meaning |
| --- | --- | --- |
| `version` | Integer, exactly 1 | `1`, application schema |
| `storage.data_root` | Nonblank path string | `"data"`, root for managed raw bytes, publications and runs |
| `presentation.mode` | `progress` or `silent` | `progress`, terminal progress preference |
| `presentation.color` | `auto`, `always`, or `never` | `auto`, color policy |
| `presentation.editor` | Nonblank string | Omitted, editor resolution uses the environment or available fallback |
| `http.requests_per_second` | Finite positive number | `2.0` requests per second |
| `http.max_concurrent` | Integer 1–64 | `4` concurrent requests |
| `http.timeout_seconds` | Finite positive number in seconds | `30.0` |
| `http.max_attempts` | Integer 1–10 | `3` attempts |

## Inspect effective values

The terminal view shows each effective setting and its source. JSON retains the existing settings fields and adds `setting_sources`, keyed by dotted field name. A null source means a built-in default.

```bash
mf config show
mf --json config show
```

Provider-specific rate limits can further constrain HTTP work. Database DSNs use [separate credential resolution](../getting-started/installation.md#postgresql-credentials). Engine resources use [historical runtime settings](../reference/runtime.md). [Settings model](https://github.com/flowdrivenml/marketforge/blob/main/forge/shared/config/models.py) · [Path resolver](https://github.com/flowdrivenml/marketforge/blob/main/forge/shared/paths.py)
