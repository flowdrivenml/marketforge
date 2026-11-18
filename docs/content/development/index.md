# Development & collaboration

Contributions are welcome in engineering, integrations, testing, documentation, and research. Begin with a focused change, inspect the local implementation, and preserve unrelated work and runtime data.

## Contributor setup

Forge packaging and development dependency declarations live in `forge/pyproject.toml`. Engine targets and feature combinations live in `engine/Cargo.toml`. The [installation draft](../getting-started/installation.md) describes package locations; it is not a verified contributor bootstrap.

Documentation uses a separate environment. From the repository root:

```bash
python3 -m venv docs/.venv
docs/.venv/bin/python -m pip install -r docs/requirements.txt
(cd docs && .venv/bin/zensical serve)
(cd docs && .venv/bin/zensical build --strict)
```

## Architecture, testing, and extensions

Read the [architecture introduction](architecture.md) and [extension outline](extending.md). Select focused tests for the component being changed; some integration tests require databases, network access, or retained market-data fixtures. A documentation build does not test processing correctness. Never substitute a missing fixture with a claimed pass.

> **Documentation draft.** A verified contributor environment, test-selection guide, and complete contribution policy remain to be written. Existing Engine and module-local engineering documents remain in place for later curation.

## Build and preview the documentation

```bash
uvx zensical build -f docs/zensical.toml
uvx zensical serve -f docs/zensical.toml --open
```
