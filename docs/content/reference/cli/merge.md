# Merge CLI

## Merge

| Invocation | Result |
| --- | --- |
| `mf merge new NAME` | Write an explicit-ID starter TOML exclusively in `data/.config/historical/merge/`. No editor, database or engine required. Optional `--testing` has no effect here. |
| `mf merge new NAME --from-normalize SOURCE --testing` | Write a source-derived starter and report candidate or selected registered files using metadata and retained provenance. No processing. |
| `mf merge validate RECIPE` | Parse TOML and check explicit file IDs and nanosecond bounds or the source-derived interval and filters without executing. |
| `mf merge prepare RECIPE --testing` | Resolve complete normalized file IDs, verify publications and write a validated frozen job and preparation report. Requires managed engines and PostgreSQL. |
| `mf merge run REFERENCE --testing [--tui]` | Prepare and execute a recipe or execute a frozen file-selected JSON job. Registers verified primary outputs. |
| `mf merge register OUTPUT --testing` | Retry registration of a publication root, manifest, primary file or execution-intent receipt. |

Managed names or explicit TOML paths are accepted for recipe actions other than `new`. Prepared JSON must contain file entries with `stream_id`, `stream_rank`, and `content_type`. The files must match active registered inputs and the pinned publications. Use `mf merge prepare RECIPE --testing` to regenerate older submissions. Source-derived preparation returns exit 1 and a durable report when provenance, availability or Rust compatibility blocks the selection. A run can return failed or interrupted receipts. Registration failures return nonzero. Global JSON returns the structured action result and cannot be combined with TUI.

See [Merge registered normalized files](../../workflows/merge.md) for the TOML example, runtime settings, layout constraints and recovery. Selected files must be in one instrument namespace and each selection must overlap the requested half-open interval. Prepared reports pin publication fingerprints and selected paths. Prepared recipes resolve `timeline_ordering` to causal for segmented depth and chronological for other selections unless the recipe sets it explicitly. The canonical mixed fixture declares causal ordering. Run it through the managed binary and PostgreSQL registration with the TUI attached to the terminal.

```bash
mf merge run engine/tests/historical/fixtures/merge.json --testing --tui
```
