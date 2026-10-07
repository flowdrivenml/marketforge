
```
marketforge database init
marketforge metadata sync
marketforge instruments refresh
```

## Offline Processing and Market Discovery CLI

Implement the offline-processing and market-discovery CLI around six commands:

```text
marketforge instruments
marketforge archives
marketforge process
marketforge datasets
marketforge merge
marketforge config
```

The CLI is the user-facing control plane. Users should not normally need to edit job JSON, configuration files, or PostgreSQL directly.

Persistent processing configuration is stored in PostgreSQL under `config.*` and validated by Python/Pydantic before being saved or used.

Current market metrics are stored separately as factual instrument metadata under `catalog.instrument_metrics`.

---

## `marketforge instruments`

Inspect available instruments and current market activity across supported exchanges.

MarketForge periodically retrieves public 24-hour market statistics from:

```text
Bybit
Binance
OKX
Bitget
Gate.io
```

The exchange-specific responses are normalized into `catalog.instrument_metrics`.

Metrics may include:

```text
last price

24h high
24h low
24h price change

24h native volume
24h base volume
24h quote volume
24h contract volume

normalized 24h turnover
turnover denomination

open interest
open-interest value
funding rate

measurement timestamp
```

Not every exchange provides every metric. Missing metrics remain unavailable rather than being artificially estimated unless a documented normalization rule exists.

### Market Discovery

The command should make it easy to identify active markets before downloading large historical datasets.

```bash
marketforge instruments
```

Example:

```text
EXCHANGE  SYMBOL          TYPE        MARKET   TURNOVER     VOLUME      OI
okx       BTC-USDT-SWAP   perpetual   linear     5.72B      ...         ...
bybit     BTCUSDT          perpetual   linear     4.27B      ...         ...
binance   BTCUSDT          perpetual   linear     3.91B      ...         ...
```

Support existing instrument filters plus market-metric filtering and sorting:

```bash
marketforge instruments \
  --exchange bybit \
  --market linear
```

```bash
marketforge instruments \
  --market linear \
  --sort turnover
```

```bash
marketforge instruments \
  --exchange okx \
  --sort open-interest
```

```bash
marketforge instruments \
  --symbol BTC \
  --sort turnover
```

A compact default view should show the most useful discovery fields without dumping every available metric.

A detailed view may expose additional statistics:

```bash
marketforge instruments \
  --symbol BTC \
  --details
```

### Turnover Normalization

Exchange APIs represent volume and turnover differently.

MarketForge normalizes these differences before storing the canonical turnover metric.

Current rules:

```text
BYBIT

spot
    → turnover24h

linear
    → turnover24h

inverse
    → volume24h


BINANCE

spot
    → quoteVolume

linear
    → quoteVolume

inverse
    → volume × contract_value


OKX

spot
    → volCcy24h

linear
    → vol24h × contract_value × last

inverse
    → vol24h × contract_value


BITGET

spot
    → turnover24h

linear
    → turnover24h

inverse
    → turnover24h


GATE.IO

spot
    → quote_volume

linear
    → volume_24h_quote

inverse
    → volume_24h_quote
```

Contract economics such as:

```text
contract_value
contract_value_asset
quantity_type
```

are resolved from `catalog.instrument_specs` where required.

For contract markets, normalized turnover represents an approximate common USD-equivalent economic notional.

For spot markets, turnover remains denominated in the quote asset.

### Turnover Ranking

MarketForge does **not** persist rank as fundamental data.

It persists the market measurement:

```text
instrument
    ↓
turnover_24h
```

Rank is derived when required:

```text
instrument metrics
    ↓
ORDER BY turnover_24h DESC
    ↓
rank
```

This allows the same metrics to support:

```text
market discovery
instrument selection
CLI sorting
merge ordering
future market analysis
```

without maintaining separate rank state.

---

## `marketforge archives`

Inspect locally downloaded raw archives using MarketForge's deterministic raw storage hierarchy:

```text
data/raw/
└── {exchange}/
    └── {instrument_type}/
        └── {market_category}/
            └── {data_type}/
                └── {symbol}/
                    └── {archive}
```

Provide a compact inventory:

```text
EXCHANGE  TYPE        MARKET   DATA   SYMBOL    FILES   RANGE
bybit     spot        spot     trade  BTCUSDT       7   2026-09-01 → 2026-09-07
bybit     perpetual   linear   l2     BTCUSDT       7   2026-09-01 → 2026-09-07
```

Support filtering by exchange, instrument/market type, dataset, symbol, and date range.

An optional detailed view can display individual physical files.

Example:

```bash
marketforge archives \
  --exchange bybit \
  --symbol BTCUSDT
```

---

## `marketforge process`

Select one or more raw archive groups and construct a `ProcessJob`.

```text
CLI selection
    ↓
raw archive inventory
    +
PostgreSQL catalog
    ├── raw_formats
    ├── normalization_rules
    ├── instruments
    └── instrument_specs
    +
processing profile
    ↓
Python planning
    ↓
ProcessJob
    ↓
marketforge-process
    ↓
N canonical datasets
```

Example:

```bash
marketforge process \
  --exchange bybit \
  --market spot \
  --symbol BTCUSDT \
  --dataset trades \
  --start 2026-09-01 \
  --end 2026-09-07
```

A processing profile may be selected explicitly:

```bash
marketforge process \
  --exchange bybit \
  --market spot \
  --symbol BTCUSDT \
  --dataset trades \
  --start 2026-09-01 \
  --end 2026-09-07 \
  --profile workstation
```

The profile resolves persistent settings such as:

```text
workers
memory budget
scratch path
scratch budget
Parquet row-group target
Parquet file target
integrity profile
```

Multiple independent inputs may be processed concurrently.

Processing does **not** implicitly merge their outputs.

```text
N raw WorkTasks
    ↓
parallel processing
    ↓
N canonical datasets
```

---

## `marketforge datasets`

Inspect canonical datasets already produced by MarketForge.

```bash
marketforge datasets
```

Compact output may look like:

```text
ID    EXCHANGE   TYPE        MARKET   SYMBOL     DATA       RANGE                  STATUS
101   bybit      spot        spot     BTCUSDT    trade      2026-09-01 → 09-07     complete
102   bybit      perpetual   linear   BTCUSDT    l2         2026-09-01 → 09-07     complete
103   binance    perpetual   linear   ETHUSDT    trade      2026-09-01 → 09-07     complete
```

Support filters such as:

```bash
marketforge datasets --exchange bybit
```

```bash
marketforge datasets --status complete
```

```bash
marketforge datasets --symbol BTCUSDT
```

Dataset IDs are subsequently used by `marketforge merge`.

---

## `marketforge merge`

Select two or more completed canonical datasets and construct a `MergeJob`.

```bash
marketforge merge 101 102 103
```

Internally:

```text
selected canonical datasets
    ↓
Python resolves dataset metadata
    ↓
validate merge compatibility
    ↓
resolve current instrument metrics
    ↓
rank selected streams by turnover
    ↓
assign deterministic stream_rank
    ↓
resolve processing profile
    ↓
MergeJob
    ↓
marketforge-process
    ↓
ONE canonical merged dataset
```

A profile may be selected explicitly:

```bash
marketforge merge 101 102 103 \
  --profile workstation
```

Optionally support time-range clipping:

```bash
marketforge merge 101 102 103 \
  --start 2026-09-03 \
  --end 2026-09-05
```

There are no separate trade, depth, combined, same-exchange, or cross-exchange merge modes.

MarketForge simply merges whatever compatible canonical datasets were selected.

### Merge Compatibility

Compatibility is validated before Rust starts:

```text
SPOT + SPOT
    → require common quote denominator

CONTRACT + CONTRACT
    → allowed

SPOT + CONTRACT
    → rejected

OPTIONS
    → postponed
```

Spot markets require a common quote denominator because their prices and turnover must remain economically comparable.

Contract datasets may contain different:

```text
underlyings
exchanges
linear contracts
inverse contracts
perpetuals
futures
```

Options processing and merging are postponed.

### Merge Stream Ranking

When multiple streams contain events with exactly the same canonical timestamp, MarketForge requires a deterministic tie-break.

Selected streams are ranked using current normalized 24-hour turnover:

```text
selected streams
    ↓
catalog.instrument_metrics
    ↓
turnover_24h DESC
    ↓
stream_rank 0..N
    ↓
MergeJob
```

Higher-turnover streams receive higher ordering priority.

The resulting rank is written into the immutable `MergeJob`.

`stream_rank` is therefore **job-specific** and is not persisted as fundamental instrument metadata.

The ordering policy is:

```text
event_timestamp_ns
        ↓
stream_rank
        ↓
preserved source-event order
```

The turnover-based rank is a deterministic tie-break heuristic. It does not claim that an event from the higher-ranked stream physically occurred before another event when their source timestamps are identical.

For extremely ordering-sensitive long-term microstructure research, users may require more sophisticated historical venue-ranking or synchronization methodology.

For MarketForge's primary use case of recent data for trading-system development, current 24-hour market activity provides a practical deterministic ordering heuristic.

---

## `marketforge config`

Manage persistent MarketForge execution configuration without manually editing configuration files or PostgreSQL.

Configuration changes follow:

```text
CLI
    ↓
Pydantic validation
    ↓
configuration service
    ↓
PostgreSQL config.*
```

Invalid configuration must be rejected before it is persisted.

Market activity and instrument rankings are **not configuration** and therefore do not belong under `marketforge config`.

They are exposed through:

```text
marketforge instruments
```

and stored as factual observations in:

```text
catalog.instrument_metrics
```

### List Processing Profiles

```bash
marketforge config profiles
```

Example:

```text
ID   NAME             WORKERS   MEMORY   SCRATCH   INTEGRITY
1    default                4      4G       20G    standard
2    laptop                 4      8G       50G    standard
3    workstation           16     64G      500G    standard
4    low-memory             2      4G      100G    strict
```

### Show a Processing Profile

```bash
marketforge config profile workstation
```

Example:

```text
Profile: workstation

workers                         16
memory_budget                   64G
scratch_path                    data/.work
scratch_budget                  500G
parquet_row_group_target        128M
parquet_file_target             512M
integrity_profile               standard
```

### Create a Processing Profile

```bash
marketforge config profile create workstation \
  --workers 16 \
  --memory 64G \
  --scratch-path data/.work \
  --scratch 500G \
  --row-group 128M \
  --file-target 512M \
  --integrity standard
```

Python converts human-readable sizes into concrete byte values, validates the resulting profile with Pydantic, and only then persists it.

```text
CLI values
    ↓
64G / 500G / 128M / ...
    ↓
parse units
    ↓
Pydantic ProcessingProfile
    ↓
validate
    ↓
config.processing
```

### Modify a Processing Profile

```bash
marketforge config profile update workstation \
  --workers 24 \
  --memory 96G
```

Only explicitly supplied values are changed.

The complete resulting profile must pass validation before the database update is committed.

### Delete a Processing Profile

```bash
marketforge config profile delete workstation
```

Deletion should require the profile to exist and produce a clear error for an unknown profile.

---

## Configuration Profiles

`config.processing` contains multiple reusable profiles rather than one global configuration.

Profiles can represent different machines or objectives:

```text
default
laptop
workstation
low-memory
high-throughput
```

The database stores concrete validated values.

The immutable `ProcessJob` or `MergeJob` records the exact resolved values used for a particular execution.

```text
persistent profile
        ↓
Python resolves profile
        ↓
immutable job configuration
        ↓
Rust
```

Changing a profile later does not change the configuration recorded in an already-created job.

---

## Intended Workflow

MarketForge's complete discovery-to-processing workflow becomes:

```text
marketforge instruments
        ↓
inspect available markets
and current activity
        ↓
choose useful instruments
        ↓
marketforge download
        ↓
marketforge archives
        ↓
marketforge process
        ↓
marketforge datasets
        ↓
marketforge merge
```

Persistent execution configuration is managed separately:

```text
marketforge config
        ↓
processing profiles
        ↓
process / merge jobs
```

The responsibilities are:

```text
instruments
    → what markets exist?
    → which markets are active?
    → what are their current metrics?

archives
    → what raw data exists locally?

process
    → convert raw data into canonical datasets

datasets
    → what canonical datasets exist?

merge
    → combine selected compatible canonical datasets

config
    → how should processing execute?
```

Users interact with these commands while Python handles:

```text
market discovery
catalog resolution
metric normalization
configuration validation
input selection
merge compatibility
planning
job construction
Rust invocation
result handling
```

Rust receives only fully resolved, immutable execution jobs.