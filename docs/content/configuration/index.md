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

Use [effective application settings](../reference/cli/maintenance.md#application-configuration) to verify workspace and invocation precedence. See the [reference index](../reference/index.md#file-and-interface-contracts) for each recipe and generated contract.
