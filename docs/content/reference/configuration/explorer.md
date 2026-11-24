# Configuration Explorer

Open **Configs** in the Forge TUI. Each tab opens its workspace configuration directly.

| Tab | Configuration | Reference |
| --- | --- | --- |
| General | Selected Forge application file. Default `data/.config/forge/marketforge.toml` | [Application settings](application.md) |
| Normalize | `data/.config/engine/normalize.toml` | [Normalization runtime](normalization-runtime.md) |
| Merge | `data/.config/engine/merge.toml` | [Merge runtime](merge-runtime.md) |
| Convert | `data/.config/engine/convert.toml` | [Conversion runtime](conversion-runtime.md) |
| Pipeline | `data/.config/engine/pipeline.toml` | [Pipeline overrides and precedence](pipeline-runtime.md) |

General exposes all nine Forge settings, including the shared storage root, HTTP limits, presentation options, and the read-only schema version. With an explicit `--config`, General edits that file. When the default file is missing, Forge starts with built-in values and creates the file on the first successful apply.

Each tab retains its search, expanded groups, selected field, and pending draft. Click an inline section heading or select it and press **Enter** to collapse or expand it. Search matches paths, group names, and schema descriptions, including fields in collapsed sections. Clear the search to return to the saved groups. **More → Show basic fields** filters the list while advanced mode exposes the entire schema.

The table shows configured, effective, and default values separately. Yellow rows and a star mark pending changes, and the tab label counts changed settings. Cyan identifies groups and inherited values. Muted rows indicate read-only or inactive settings. Green **VALID** confirms successful validation, and red **INVALID** includes the diagnostic. A valid draft can still be marked **MODIFIED** until applied.

**Ctrl+E** switches the registry environment after confirmation. Use the command palette (**Ctrl+P**) for **Edit Forge application settings**, **Inspect registry settings**, and Engine or database operations. Runtime editing is separate from recipe editing.

- **Edit field** changes the draft using the field’s type. Resource inputs accept binary units while retaining exact integer values. Enum values come from the managed schema.
- **Use default** writes the displayed default when a value exists. **Unset / Inherit** removes an optional value or pipeline override. **Reset pending field** restores the originally inspected value in the draft.
- **Review & apply** validates the full candidate and shows the configuration diff and changes to effective values. The risk confirmation expressly acknowledges integrity-critical changes. There is one review and atomic replacement per file.
- **Edit TOML** opens the current draft in the configured editor. The same validation and review apply when it returns.
- **Reload file** creates a new draft from disk after confirmation if there are unsaved changes. An external edit invalidates the reviewed snapshot and leaves the pending draft available for inspection.
- **Pinned run** inspects fingerprinted runtime TOML and recorded effective values for a retained pipeline run in the selected environment. The inspector uses retained evidence and never saves changes.
- **More → Validate all and show coverage** checks the application settings and four runtime files, including pipeline composition, while keeping the selected tab. Missing or incompatible managed binaries are reported with their build command. Validation here concerns configuration.

Select a field and press **F1** or choose **Help** to see its purpose, constraints, value sources, and risk. Field help has **Full documentation** and **Copy field path** actions. **Documentation** on the toolbar opens the active tab’s reference. **More → Copy documentation link** works without a graphical browser. Leaving Configs with pending drafts prompts for confirmation. For local documentation development, set `MARKETFORGE_DOCS_URL` to the HTTP base address printed by `zensical serve`.

[Application](application.md) · [Normalization](normalization-runtime.md) · [Merge](merge-runtime.md) · [Conversion](conversion-runtime.md) · [Pipeline Runtime](pipeline-runtime.md)
