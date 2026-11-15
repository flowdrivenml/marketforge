# Configuration

TOML is central to the user experience: application settings establish the workspace and services, while named recipes describe selections or operations. Generated processing artifacts are a separate contract and may be JSON.

| Category | Role |
| --- | --- |
| Application settings | Storage, presentation, and service settings; the default path is `data/.config/marketforge.toml`. |
| Acquisition and import recipes | Describe input scope, source options, or external inputs. |
| Normalization recipes | Describe what Forge should prepare, separately from Engine resource settings. |
| Engine runtime settings | Historical runtime configuration defaults to `data/.config/historical/general.toml`. |
| Format definitions and profiles | Describe supported physical layouts and normalization semantics. |
| Live configuration | Configures live operation; a repository example exists at `engine/examples/live/general.toml`. |

Detailed user contracts are now available for [acquisition and external imports](acquisition.md) and [markets/ranking policies](markets.md), including complete TOML examples, defaults, and constraints.

> **Documentation draft.** Other configuration categories remain introductions. Existing examples are not interchangeable contracts.

The CLI accepts `--project-root` and `--config` for workspace and application configuration selection. Avoid publishing local connection values. See [Reference](../reference/index.md) for the small command sample and future contract coverage.
