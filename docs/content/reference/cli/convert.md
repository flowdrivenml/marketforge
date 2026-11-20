# Historical conversion CLI

Forge prepares quote-currency conversion jobs from registered historical merge publications. The workflow uses the guarded testing registry.

| Command | Behavior |
| --- | --- |
| `mf convert new NAME --from-merge SOURCE --target-quote USDT` | Create an editable recipe with the upstream TOML path and SHA-256. The file must be new. |
| `mf convert new NAME --from-normalize SOURCE --target-quote USDT` | Derive the initial interval from a modern normalization recipe. |
| `mf convert new NAME --from-acquisition SOURCE --target-quote USDT` | Derive the initial interval from an acquisition recipe. |
| `mf convert validate NAME_OR_PATH` | Check the recipe, local constraints, and upstream fingerprint. |
| `mf convert prepare NAME_OR_PATH --testing [--dry-run]` | Verify lineage and committed inputs, resolve references, and generate the job collection and report. Dry-run performs the same checks without writing artifacts or mutating the database. |
| `mf convert run NAME_OR_JSON --testing [--tui]` | Prepare a recipe or read a retained collection or job. Submit only compatible per-job documents, with revalidated inputs and a pinned executable. |

Use exactly one `--from-*` flag with `new`. Each accepts a managed name or explicit TOML path. Creation and structural validation are local. Preparation and execution use `--testing`. Use global `--json` before `convert` for machine-readable responses. TUI requires an interactive terminal and a converter built with `monitor-tui`. It is mutually exclusive with global `--json`.

`mf engine build convert` explicitly builds and installs the managed converter with the historical and monitor features. Its smoke check uses `--help`. The default build selection is unchanged.

Preparation returns `blocked` for invalid selection, provenance, or metadata. It returns `generated` when it can write the contract-correct job and reports compatibility separately. The current converter offers no production validation-only command, so this version of Forge cannot mark its jobs `prepared` with validated compatibility. `run` exits nonzero if launch is blocked, the engine fails, or the committed output cannot be verified. Forge retains execution intent and result evidence for runs it attempts. Conversion registration is unavailable in this phase.

See the [conversion workflow](../../workflows/conversion.md) for selection, references, and the walkthrough.
