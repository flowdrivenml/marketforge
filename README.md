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

**[Documentation](https://flowdrivenml.github.io/marketforge/)** · **[Quick Start](https://flowdrivenml.github.io/marketforge/getting-started/quickstart/)** · **[Roadmap](ROADMAP.md)**

</div>

MarketForge prepares market data for quantitative research on a single machine. It automates the work of finding archives, reconciling provider formats, and publishing datasets you can inspect and reuse.

![Forge interactive workspace showing market coverage, storage, and recent runs](docs/content/assets/images/tui.png)

## Current capabilities

- Discover and acquire historical trade ticks and L2 order-book data from Binance, Bybit, OKX, Bitget, and Gate.io. Coverage varies by market, feed, and date.
- Run a single historical recipe through the CLI or interactive Forge workspace. Forge orchestrates acquisition, normalization, merge, and optional quote conversion, with verified outputs and retained execution evidence.
- Browse registered datasets, inspect their provenance, and query Parquet with the Python API, Arrow, or Polars.

Historical pipeline orchestration is operational. MarketForge is under active development, with current work focused on live streaming and historical-to-live bootstrap. Spectra financial feature engineering is planned. [Development roadmap](ROADMAP.md)

## License

[Apache License, Version 2.0](LICENSE).
