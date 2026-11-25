# Releases and changelog

[v0.1.0 — Historical Data Infrastructure](https://github.com/flowdrivenml/marketforge/releases/tag/v0.1.0), published November 25, 2026, is the first official source release. It covers historical acquisition, external file registration, normalization, merge, and optional quote conversion. Forge runs the workflow from a recipe, registers and inspects outputs in the CLI and TUI, and exposes Parquet data to Python and Jupyter.

The [standard installer](getting-started/installation.md) supports Ubuntu, Debian, and supported macOS releases on x86-64 and ARM64. The conversion publication step currently requires Linux. Historical coverage varies by provider, instrument, feed, and date.

## Release history

- [Canonical changelog](https://github.com/flowdrivenml/marketforge/blob/main/CHANGELOG.md) records changes and limitations for each version.
- [GitHub Releases](https://github.com/flowdrivenml/marketforge/releases) provides the official notes, source archives, and release tags.
- [Development roadmap](https://github.com/flowdrivenml/marketforge/blob/main/ROADMAP.md) tracks live infrastructure, historical-to-live bootstrap, and planned research features.

Maintainers can follow the [release procedure](development/releases.md).
