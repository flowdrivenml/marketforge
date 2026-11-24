# Application settings

Forge reads an explicit global `--config PATH` relative to the caller's directory, or the workspace's default `data/.config/forge/marketforge.toml`. If the default is absent, Forge uses built-in settings. A legacy default without its migrated counterpart produces a migration diagnostic. An explicit missing path is an error. Select the workspace with `--project-root`, then `MARKETFORGE_PROJECT_ROOT`, then discovery from the current directory. A workspace has `forge/pyproject.toml` and `engine/`. Storage path values are resolved from the workspace. The recipe libraries continue to live under its configuration directory.

## Fields and defaults

All fields below are optional. Unknown keys and unintended type coercions are rejected.

| Path | Type and constraint | Default and meaning |
| --- | --- | --- |
| <a id="setting-version"></a>`version` | Integer, exactly 1 | `1`, application schema |
| <a id="setting-storage.data_root"></a>`storage.data_root` | Nonblank path string | `"data"`, root for managed raw bytes, publications and runs |
| <a id="setting-presentation.mode"></a>`presentation.mode` | `progress` or `silent` | `progress`, terminal progress preference |
| <a id="setting-presentation.color"></a>`presentation.color` | `auto`, `always`, or `never` | `auto`, color policy |
| <a id="setting-presentation.editor"></a>`presentation.editor` | Nonblank string | Omitted, editor resolution uses the environment or available fallback |
| <a id="setting-http.requests_per_second"></a>`http.requests_per_second` | Finite positive number | `2.0` requests per second |
| <a id="setting-http.max_concurrent"></a>`http.max_concurrent` | Integer 1–64 | `4` concurrent requests |
| <a id="setting-http.timeout_seconds"></a>`http.timeout_seconds` | Finite positive number in seconds | `30.0` |
| <a id="setting-http.max_attempts"></a>`http.max_attempts` | Integer 1–10 | `3` attempts |

## Edit from the TUI

Open **Configs → General** to edit and review the selected application file. The schema version is read-only. See the [Configuration Explorer](explorer.md) for validation, field help, and safe application.

## Inspect effective values

The terminal view shows each effective setting and its source. JSON retains the existing settings fields and adds `setting_sources`, keyed by dotted field name. A null source means a built-in default.

```bash
mf config show
mf --json config show
```

Provider-specific rate limits can further constrain HTTP work. Database DSNs use [separate credential resolution](../../getting-started/installation.md#postgresql-credentials). Engine resources use [historical runtime settings](../runtime.md). [Settings model](https://github.com/flowdrivenml/marketforge/blob/main/forge/shared/config/models.py) · [Path resolver](https://github.com/flowdrivenml/marketforge/blob/main/forge/shared/paths.py)
