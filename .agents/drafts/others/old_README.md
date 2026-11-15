<div align="center">
  <img src="docs/content/assets/branding/logo.png" alt="MarketForge — From data to financial engineering" width="95%">

  <p>A developing platform for data infrastructure and financial engineering.</p>

  <a href="forge/pyproject.toml"><img src="https://img.shields.io/badge/Python-3.12%2B-7aa2f7" alt="Python 3.12+"></a>
  <a href="engine/Cargo.toml"><img src="https://img.shields.io/badge/Rust-2024-9ece6a" alt="Rust edition 2024"></a>

  <p><a href="#overview">Overview</a> · <a href="#marketforge-in-action">In Action</a> · <a href="#installation">Getting Started</a> · <a href="#roadmap">Roadmap</a> · <a href="#contributing--collaboration">Contributing</a></p>
</div>

## Overview

MarketForge brings data acquisition, preparation, and processing into a shared foundation for quantitative research and financial engineering. Its primary interface is CLI commands, TOML configuration, and terminal tools.

The local implementation includes source discovery and acquisition, external-file import tooling, and Rust processing for historical trades and order books. Live ingestion and terminal management also have implementations; their end-to-end readiness was not assessed for this documentation draft.

- **Acquire and bring data:** use supported sources or explicitly configured imports. External formats require compatible profiles and verified semantics.
- **Prepare and process:** Forge handles planning and registry concerns; Engine handles data-intensive normalization, merging, and conversion.
- **Build on shared foundations:** historical and live processing use shared data and processing components. Broader research and algorithmic workflows are the direction of development, not a claim of a complete trading platform.

## MarketForge in Action

> **Demonstration pending verification.** This space is reserved for one real terminal capture showing a small configured input becoming an inspected dataset, with its provenance and processing result. No verified end-to-end capture is included yet.

## Installation

> **Documentation draft.** These commands follow local packaging; application installation and compilation were not tested for this assignment.

Use Python 3.12+ for Forge and a Rust toolchain supporting edition 2024 for Engine. Linux and macOS are the project targets. From the repository root, using a separate application environment of your choice:

```bash
python3 -m pip install -e ./forge
cargo build --manifest-path engine/Cargo.toml --features historical --bin marketforge-normalize
```

The Python package exposes `marketforge` and `mf`. Cargo places the binary under `engine/target/debug/` with the default target directory. Registry-backed operations additionally need PostgreSQL and workspace configuration; installing the package does not initialize those services. See [installation notes](docs/content/getting-started/installation.md).

## Quick Start

After installing Forge, explore the current command surface without starting a data job:

```bash
marketforge --help
marketforge acquisition --help
marketforge normalize --help
```

These commands are supported by the local parser; execution was not tested here. The minimum data workflow is to configure a source or import, establish its storage and catalogue prerequisites, acquire or register inputs, and prepare processing inputs.

> **Runnable walkthrough pending verification.** Forge's `normalize prepare` creates preparation artifacts and does not execute Engine. Compatibility between the current Forge output and Rust input contract needs verification before publishing a combined command sequence. See [the first-workflow draft](docs/content/getting-started/quickstart.md).

## Architecture

**Engine (`engine/`, Rust)** implements data-intensive historical and live processing, including normalization, persistence, and shared market-data components.

**Forge (`forge/`, Python)** provides the user-facing CLI, acquisition/import planning, orchestration, storage management, and PostgreSQL registry integration. It prepares inputs for processing; the existence of both components does not establish a complete integrated workflow.

Read the [architecture introduction](docs/content/development/architecture.md) for boundaries and current documentation gaps.

## Roadmap

| Area | Current evidence and direction |
| --- | --- |
| Acquisition and external data | CLI and services implemented; source coverage and end-to-end import readiness not assessed here. |
| Historical processing | Rust normalization, merge, and conversion implementations present; Forge-to-Engine handoff in development and needs contract verification. |
| Live operation | Connectivity, processing, streaming, and terminal-management code present; operational readiness not assessed here. |
| Research and financial engineering | Broader workflows planned; lab and feature package presence alone does not establish usability. |
| Algorithmic workflows | Planned direction on shared historical/live foundations; no trading capability promised. |
| Documentation | Initial local site established; verified installation, demonstrations, and contract reference remain to be written. |

## Documentation

Start at the [documentation overview](docs/content/index.md), then [Getting Started](docs/content/getting-started/index.md), [Workflows](docs/content/workflows/index.md), [Configuration](docs/content/configuration/index.md), or [Reference](docs/content/reference/index.md).

The [local documentation guide](docs/README.md) explains preview and strict builds. Existing [historical engineering records](docs/historical/) and [Engine documentation](engine/docs/) remain available separately; their age and scope vary. There is no published documentation URL configured in this scaffold.

## Contributing & Collaboration

Contributions are welcome in engineering, source integrations, testing, documentation, and research. Start with the [development entry point](docs/content/development/index.md) and [extension guide outline](docs/content/development/extending.md). Keep changes focused and distinguish verified behavior from proposed capabilities.

## License

Licensing needs clarification: `forge/pyproject.toml` declares MIT, but no repository license file was found during this assignment. A project-wide license link is pending maintainer confirmation.
