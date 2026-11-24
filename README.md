<div align="center">

<img src="docs/content/assets/branding/logo.png" alt="MarketForge — From Data to Financial Engineering" width="95%">

<br>

**High-performance data infrastructure for quantitative research and financial engineering.**

*Spend less time engineering data. More time engineering strategies.*

<br>

[![Python](https://img.shields.io/badge/Python-3.12%2B-7aa2f7?logo=python&logoColor=white)](forge/pyproject.toml)
[![Rust](https://img.shields.io/badge/Rust-2024-7aa2f7?logo=rust&logoColor=white)](engine/Cargo.toml)
![Linux](https://img.shields.io/badge/Linux-9ece6a?logo=linux&logoColor=white)
![macOS](https://img.shields.io/badge/macOS-9ece6a?logo=apple&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-7aa2f7?logo=postgresql&logoColor=white)
![Prometheus](https://img.shields.io/badge/Prometheus-e0af68?logo=prometheus&logoColor=white)
![Grafana](https://img.shields.io/badge/Grafana-e0af68?logo=grafana&logoColor=white)

<br>

**[Documentation](docs/content/index.md)** · **[Installation](docs/content/getting-started/installation.md)** · **[Quick Start](docs/content/getting-started/quickstart.md)** · **[Research & Analysis](docs/content/research/index.md)** · **[Roadmap](ROADMAP.md)** · **Discord (Coming Soon)**

## Install MarketForge

On Ubuntu, Debian, or macOS:

```bash
git clone https://github.com/flowdrivenml/marketforge.git
cd marketforge
./install.sh
```

Use `./install.sh --dev` for a contributor environment. The installer sets up
Neovim, Forge, the managed Engine binaries, and the selected PostgreSQL registry.
See [installation](docs/content/getting-started/installation.md) for existing
database connections, manual setup, and options.


</div>

---

> [!IMPORTANT]
> **MarketForge is under active development.** Historical data acquisition is the primary ready-to-use workflow. Processing and live infrastructure have existing implementations, but their end-to-end integration is ongoing. Advanced research and algorithmic capabilities are planned. Follow the [Roadmap](ROADMAP.md) for development progress.

## Overview

**MarketForge** is an open-source, configuration-driven platform designed to remove the repetitive data-engineering work behind quantitative research and algorithmic trading.

Before a strategy can be tested, researchers often spend countless hours finding historical data, handling inconsistent formats, synchronizing records, and building custom pipelines. High-quality financial datasets can also be expensive, despite substantial amounts of useful market data being freely available.

**MarketForge aims to make that infrastructure reusable, accessible, and automated—so the focus stays on research, not data plumbing.**

Built with **Rust and Python**, the platform combines high-performance processing with CLI commands and TOML configuration. It is designed for a **single computer**, from a laptop to a powerful workstation, without requiring cloud services or distributed infrastructure.

## From Raw Data to Research-Ready Datasets

### Acquire Market Data

**Five exchanges. 10,000+ instruments. One CLI.**

MarketForge automates the discovery and downloading of freely available historical trade ticks and order-book data across five cryptocurrency exchanges.

Define what you need and let MarketForge locate and acquire the available archives—without manually searching providers, tracking URLs, or maintaining separate downloaders.

*Historical coverage varies by source, instrument, data type, and period. Catalog coverage does not guarantee complete data availability.*

### Bring Your Own Data

Research shouldn't be restricted to built-in providers.

MarketForge is developing configuration-driven imports and registration, allowing compatible external datasets to become part of the same processing environment. For supported formats, users define how their data should be interpreted through configuration rather than writing new integration code.

### Normalize, Merge, and Convert

Financial data arrives in different formats, timestamp conventions, and instrument representations.

MarketForge's Rust Engine provides components for normalization, merging, and conversion, with configurable interpretation and processing rules. Their integration into a complete CLI-driven workflow is ongoing.

**The objective: turn heterogeneous inputs into consistent, reusable datasets without rebuilding the pipeline for every experiment.**

---

## Research Beyond a Single Market

**What if the information driving BTC isn't contained in BTC trades on a single exchange?**

Perhaps another venue leads price discovery. Perhaps perpetual futures influence spot markets. Perhaps liquidity changes or activity in related instruments reveal information that conventional price analysis misses.

Meaningful quantitative research often requires combining **multiple exchanges, instruments, market structures, data types, and time horizons**.

MarketForge is being built to streamline that preparation, making it easier to investigate cross-market relationships, lead-lag effects, liquidity dynamics, and market microstructure.

### From Price Charts to Market Microstructure

Understanding financial markets requires more than drawing trendlines or calculating indicators.

Trade ticks and order-book events provide a foundation for studying actual market activity: aggressive execution, resting liquidity, order cancellations, absorption, imbalances, and volume distribution.

MarketForge's planned financial engineering capabilities will extend this foundation into:

- **Order flow and liquidity analysis** — Trading pressure, depth, imbalances, and execution dynamics.
- **Volume-based market structure** — Price regions supported by observed volume and liquidity.
- **Cross-market analysis** — Relationships between instruments, exchanges, spot, and derivatives.
- **Financial feature engineering** — Event-driven sampling, information-based bars, and quantitative features.
- **Research and modeling** — Optimization, machine learning, evaluation, and backtesting.

These analyses support hypothesis testing; observed patterns do not automatically establish participant intent or predictive relationships.

**The goal is not to tell researchers what the market is doing, but to give them the tools and data to investigate it.**

These advanced research capabilities remain part of the development roadmap.

---

## From Research to Live Systems

Research is only one part of developing an algorithmic system. Operating it on live data introduces additional challenges: continuous connectivity, synchronization, recovery, state management, and monitoring.

MarketForge is developing a live processing infrastructure intended to share data representations and processing components with historical research.

The longer-term goal is to let algorithms **bootstrap from historical datasets, transition into live streams, and reuse compatible research computations** without rebuilding the entire data infrastructure.

The Rust Engine already contains substantial live streaming, integrity, and recovery components. Complete research-to-live integration and production readiness remain under development.

MarketForge provides infrastructure for user-defined algorithms—not ready-made trading strategies or guaranteed trading performance.

---

## Architecture & Extensibility

**Rust processes the data. Python orchestrates the workflows.**

MarketForge follows a modular architecture designed to accommodate new data sources, processing methods, and research requirements.

### Engine — Rust

High-performance historical and live processing, including normalization, merging, conversion, integrity validation, streaming, and recovery.

### Forge — Python

The user-facing application layer responsible for CLI commands, TOML configuration, data acquisition, orchestration, storage management, and PostgreSQL registry integration.

### Extending MarketForge

Supported operations can be customized through configuration and source profiles, often without changing application code.

More substantial extensions—such as adding new live providers, data types, or processing operations—require code changes. The architecture separates these responsibilities, with developer documentation describing extension points and implementation guidance as it matures.

Explore the [Architecture](docs/content/development/architecture.md) and [Extension Guide](docs/content/development/extending.md).

---

## Development & Community

MarketForge is an evolving open-source project. Current development focuses on integrating its existing infrastructure, expanding research capabilities, and connecting historical workflows with live systems.

The [Development Roadmap](ROADMAP.md) tracks implementation progress, current priorities, and planned functionality.

Feedback, technical discussions, feature proposals, and contributions are welcome through [GitHub Issues](https://github.com/flowdrivenml/marketforge/issues).

A dedicated Discord community is planned.

---

## License

MarketForge is licensed under the [Apache License, Version 2.0](LICENSE).

See [NOTICE](NOTICE) for original authorship and attribution.