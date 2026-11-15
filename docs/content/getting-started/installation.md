# Installation

> **Documentation draft.** Package locations and command targets were inspected locally. Application installation and Rust compilation were not run for this assignment.

Forge requires Python 3.12+ and uses Hatchling packaging at `forge/pyproject.toml`. Engine uses Rust edition 2024 with explicit Cargo features at `engine/Cargo.toml`. Linux and macOS are the project targets; platform installation recipes still need verification.

From the repository root, in your chosen application Python environment:

```bash
python3 -m pip install -e ./forge
cargo build --manifest-path engine/Cargo.toml --features historical --bin marketforge-normalize
```

The editable Python package exposes `marketforge` and `mf`. With Cargo's default target directory, the normalization executable is `engine/target/debug/marketforge-normalize`. Other Rust executables require their own feature/target selection; this is not an all-components installation.

Registry-backed operations require PostgreSQL, configured connections, and appropriate schema initialization. Workspace resolution expects `forge/pyproject.toml` and `engine/`; run from the checkout or use the CLI's `--project-root` option. Do not copy private connection values into documentation.

Next: [explore the command surface](quickstart.md). Database initialization and a safe sample dataset setup remain documentation work.


uv add --project forge --group lab ipykernel
uv sync --project forge --group lab
uv run --project forge --group lab \
    python -m ipykernel install \
    --user \
    --name marketforge \
    --display-name "MarketForge (Python)"
