# Quick Start

## 1. Install MarketForge

```bash
git clone https://github.com/flowdrivenml/marketforge.git
cd marketforge
./install.sh
```

Continue after installation and market initialization succeed. See the [installation guide](installation.md) for supported platforms and setup.

## 2. Run your first pipeline

```bash
mf pipeline run bybit_quickstart
```

The included recipe acquires October 27–29, 2026 UTC trades and L2 order-book depth for the Bybit BTCUSDT linear perpetual, normalizes the records, and publishes a merged dataset. Runtime and storage depend on archive sizes, network availability, and local hardware. [Download the recipe](../examples/bybit/pipeline.toml) or see [pipeline troubleshooting](../workflows/pipeline.md#troubleshooting).

## 3. Explore your data

```bash
mf tui
```

Open **Data → Merged** and choose the publication identified in the run summary. See the [interactive workspace guide](../workflows/forge-tui.md) or continue with [Python](../research/getting-started.md) and [Jupyter notebooks](../research/notebooks.md).
