# Interactive Forge workspace

```text
mf tui --testing
```

The Textual workspace starts in the selected Forge project. It requires interactive stdin, stdout and stderr, and rejects global `--json`. Production is the default. Use `--production` to select it explicitly or `--testing` to select testing. Operations launched inside the UI use the selected verified registry and can write the same managed data and registry state as their CLI counterparts. See [Forge workspace](../../workflows/forge-tui.md) for screens and interaction. [CLI index](index.md)
