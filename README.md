# MarketForge

> **🚧 Active development**
>
> MarketForge is currently under development. The acquisition layer is usable, while normalization, processing, merging, and live-data components are still being built.

**Historical market microstructure data without manually hunting through exchange archives.**

MarketForge is a Python CLI for discovering and downloading free historical market data from multiple cryptocurrency exchanges.

Select an exchange, instrument, dataset, and date range; MarketForge handles availability discovery, planning, and downloads.

## Quick Navigation

- [Supported Exchanges](#supported-exchanges)
- [Features](#features)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Typical Workflow](#typical-workflow)
- [Development Status](#development-status)

## Supported Exchanges

Current acquisition support targets:

```text
Bybit
Binance
OKX
Bitget
Gate.io
```

Depending on exchange availability, MarketForge can acquire:

```text
tick trades
L2 / order-book data
spot
perpetuals
futures
options where publicly available
```

Only publicly available/free historical data is targeted.

## Features

- multi-exchange historical-data acquisition
- instrument discovery
- historical availability discovery
- acquisition planning before downloading
- automatic archive downloading
- immutable raw-data storage
- Spot, Perpetual, Futures, and supported Options markets
- tick trades and L2 depth where available
- no paid data providers required
- CLI-first workflow

## Installation

The project is currently intended to be installed from source:

```bash
git clone https://github.com/flowdrivenml/MarketForge.git
cd MarketForge

python -m pip install -e .
```

Check the CLI:

```bash
marketforge --help
```

Available command groups include:

```text
database
plan
download
instruments
availability
metadata
```

## Quick Start

### Discover instruments

Use the instrument command to discover instruments available from an exchange:

```bash
marketforge instruments --help
```

### Check historical availability

Before downloading large datasets:

```bash
marketforge availability --help
```

This determines which historical files or periods are actually available from the selected exchange.

### Plan a download

```bash
marketforge plan --help
```

Planning resolves the requested instruments, dates, datasets, and available remote files before acquisition begins.

### Download

```bash
marketforge download --help
```

MarketForge downloads the planned exchange archives into structured immutable raw storage.

## Typical Workflow

```text
Discover instruments
        ↓
Check availability
        ↓
Create acquisition plan
        ↓
Download
        ↓
Raw historical archives
```

In practice:

```bash
marketforge instruments --help
marketforge availability --help
marketforge plan --help
marketforge download --help
```

Exchange-specific options and supported datasets are documented directly by each command:

```bash
marketforge <command> --help
```

## Development Status

The current focus is expanding MarketForge from acquisition into a complete market-microstructure data engine:

```text
Exchange Archives
      ↓
Acquire
      ↓
Normalize
      ↓
Validate
      ↓
Trades + L2
      ↓
Merge / Synchronize
      ↓
Partitioned Parquet
```

Planned processing capabilities include:

```text
canonical Trade / L2 schemas
order-book reconstruction
data-integrity validation
chronological trades + depth datasets
cross-exchange synchronization
high-performance Rust processing
Parquet output
live market-data ingestion
```

These components are still under active development and should not yet be considered stable.

## Disclaimer

MarketForge is an independent open-source project and is not affiliated with or endorsed by Bybit, Binance, OKX, Bitget, Gate.io, or other supported exchanges.