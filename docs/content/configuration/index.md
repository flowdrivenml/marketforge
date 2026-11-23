# Configuration and recipes

| Editable input | Reference |
| --- | --- |
| Workspace, storage, terminal and HTTP settings | [Application](application.md) |
| Download and external file import | [Acquisition and import](acquisition.md) |
| Source identity, management and applicability | [Managed registration](../reference/registration.md) · [Catalogue declarations](catalogue.md) |
| Interpretation | [Format profiles](../reference/profiles.md) and [normalization runtime](normalization-runtime.md) and [merge runtime](merge-runtime.md), and [other Engine runtimes](../reference/runtime.md) |
| Operation selection | [Normalization](normalization.md) · [Merge](merge.md) · [Conversion](conversion.md) · [Pipeline](pipeline.md) |
| Ranking and inferred historical evidence | [Ranking policies and historical metadata](markets.md) |

TOML recipes express intent. The generated [prepared contracts](../reference/prepared.md) carry resolved evidence and execution destinations. Keep their version numbers and identifiers distinct.

## Workspace layout

```text
data/.config/
├── forge/
│   └── marketforge.toml
├── engine/
│   ├── normalize.toml
│   ├── merge.toml
│   ├── convert.toml
│   ├── pipeline.toml
│   └── normalize/profiles/
│       ├── trades/
│       └── depth/
├── workflows/data_engineering/
│   ├── pipeline/
│   ├── acquisition/
│   ├── normalization/
│   ├── merge/
│   ├── conversion/
│   └── import/
│       └── registration/
├── markets/rankings/
└── live/
    ├── general.toml
    ├── exchanges/
    └── formats/
```

Forge settings select the workspace storage, HTTP and presentation behavior.
Engine TOMLs control runtime resources and processing policies. Workflow
recipes select the operation inputs and outputs. Profiles define how source
records are interpreted. Ranking policies define eligibility and comparison.
Live has a separate configuration root and loader.

| Configuration | Discovery and loader | Validator and consumer |
| --- | --- | --- |
| Forge | `build_context`, `load_settings` | Strict `AppSettings` for CLI and TUI |
| Normalize | `ProjectPaths.runtime_config`, Rust `load_processing_config` | `validate_processing_config`, shared with Engine execution |
| Merge | `ProjectPaths.runtime_config`, Rust `load_merge_runtime_settings` | Shared Rust resource, partition and Parquet checks for Forge and the merge binary |
| Convert | `ProjectPaths.runtime_config`, Rust `load_conversion_runtime_settings` | Rust validates runtime inspection. Forge reuses the conversion job types to validate and clamp recipe requests, and the converter validates the prepared job. |
| Pipeline runtime | Forge `compose_pipeline` | Stage table validation followed by the normalize and merge Rust validators |
| Profiles | Forge `discover` and Rust `FormatCatalog::load` | Forge checks names and namespaces. Rust compiles schema 2 and fingerprints the interpretation. |
| Workflow recipes | Managed libraries and explicit paths where accepted | Forge recipe models and reference resolvers. Preparation checks registry, input and Engine prerequisites. |
| Rankings | `resolve_config_file`, `load_policy` | `RankingPolicy` and ranking eligibility checks for published snapshots |
| Live | `GeneralConfig::load`, `ExchangeConfigStore::load_dir`, `FormatConfigStore::load_dir` | Live general, exchange and format validators |

The workspace root anchors all managed configuration paths, even when
`storage.data_root` points elsewhere. Named lookups use the relevant library. Acquisition map, plan, and download use a saved name, with an optional `.toml` suffix. Normalize, merge, and convert accept explicit recipe paths where the operation synopsis indicates; pipeline recipe commands use a saved name. Global `--config` selects Forge settings and follows the caller’s working directory for a relative path.
Active lookups diagnose a legacy source without its migrated counterpart.
Retained paths in historical evidence keep their original names and checksums.

## Validation stages

TOML parsing, schema validation, reference checks, preparation and execution
succeed independently. For example, a merge recipe can point to a valid
normalization recipe while the expected publications are still absent. An
import validator also checks its local file selection. A registration validator
uses the selected registry to check its declared bindings.

```bash
mf acquisition validate bybit-btc-20261027
mf normalize validate docs/content/examples/bybit/acquisition.toml
mf merge validate MERGE_RECIPE
mf convert validate CONVERSION_RECIPE
mf acquisition import validate NAME
mf pipeline validate bybit_spot
mf normalize profiles validate bybit.spot.spot.trades
mf markets rank validate turnover
```

Acquisition and registration offer editor commands. Normalize `new` opens an
editor after creating the file. Merge offers [`mf merge edit`](../reference/cli/merge.md#recipe-management) and opens the editor on interactive creation. For pipeline and conversion recipes, use `nvim` on the managed file after creation. Creating a named recipe
uses an exclusive write. Edits apply to subsequent preparation. A prepared
run retains its submitted request, evidence and settings.


Conversion additionally checks an optional installed instrument catalogue at
`data/catalog/instruments.json` against historical instrument evidence. The
generated catalogue and job bindings are execution artifacts. The installed
Engine manifest lives at `.bin/manifest.json` and verifies the selected binaries.
These files have separate contracts from the editable TOMLs.

The Forge TUI System actions **Config show** and **Engine runtime** use the
same reports as the CLI. Recipe actions use the same managed libraries and
validators. Existing editor actions hand off the selected source file.
