## Two Layers of Discovery

MarketForge separates **instrument discovery** from **historical-data discovery**.

These answer different questions:

```text
Layer 1 — Instrument Discovery

exchange APIs
    ↓
marketforge instruments
    ↓
Which instruments and markets exist?
```

```text
Layer 2 — Historical Data Discovery

exchange historical archives / APIs
    ↓
marketforge plan
    ↓
Which historical files actually exist
for the requested date range?
```

An instrument appearing in:

```bash
marketforge instruments
```

does **not** guarantee that historical data exists for that instrument or for the requested period.

An exchange may currently expose an instrument such as:

```text
BTCUSDT
perpetual
linear
```

while its historical data may:

- begin after the requested start date;
- end before the requested end date;
- contain gaps inside the requested interval;
- provide trades but not order-book data;
- use different archive granularities;
- not expose downloadable historical data at all.

The normal discovery workflow is therefore:

```text
marketforge instruments
        ↓
choose exchange + instrument
        ↓
marketforge plan
        ↓
discover historical archives
        ↓
inspect coverage and gaps
        ↓
marketforge download
```

### Instrument Discovery

Use:

```bash
marketforge instruments
```

to explore instruments known to MarketForge and currently exposed by exchange APIs.

For example:

```bash
marketforge instruments \
    --symbol BTC \
    --market linear \
    --sort turnover
```

Instrument discovery can expose information such as:

```text
exchange
symbol
instrument type
market category
current price
24h turnover
open interest
```

It answers:

> **What instruments can I potentially request?**

It does not answer:

> **Does the historical dataset I need actually exist?**

### Historical Data Discovery

After selecting an instrument, use:

```bash
marketforge plan
```

with the exact dataset and date range required.

For example:

```bash
marketforge plan \
    --exchange bybit \
    --type perpetual \
    --category linear \
    --symbol BTCUSDT \
    --data trade_ticks \
    --start 2026-09-01 \
    --end 2026-09-07
```

Planning queries the exchange's historical-data source and determines which physical archives actually exist for the requested interval.

It can therefore expose incomplete historical coverage before anything is downloaded.

For example:

```text
Requested

2026-09-01 ───────────────────── 2026-09-07

Historical discovery

2026-09-01    available
2026-09-02    available
2026-09-03    missing
2026-09-04    available
2026-09-05    available
2026-09-06    missing

        ↓

Acquisition plan

available archives
+
historical gaps
```

This distinction is important:

```text
marketforge instruments
    → discover markets

marketforge plan
    → discover historical data
      for a specific request

marketforge download
    → download the available files
      identified by the plan
```

Therefore the recommended acquisition workflow is:

```text
instruments
    ↓
plan
    ↓
download
    ↓
archives
```

After acquisition, the resulting raw archives can continue through the processing pipeline:

```text
instruments
    ↓
plan
    ↓
download
    ↓
archives
    ↓
process
    ↓
datasets
    ↓
merge
```

MarketForge separates historical-data acquisition into two explicit steps:

```text
plan
  ↓
inspect exactly what MarketForge intends to acquire
  ↓
download
  ↓
store immutable source archives under data/raw/
```

`plan` performs discovery only.

`download` performs the same planning step and then downloads or reuses the required raw archives.

## Quick Navigation

- [Basic Workflow](#basic-workflow)
- [Date Range Semantics](#date-range-semantics)
- [Planning Downloads](#planning-downloads)
- [Downloading Data](#downloading-data)
- [Required Market Arguments](#required-market-arguments)
- [Trade and Order-Book Data](#trade-and-order-book-data)
- [Spot Markets](#spot-markets)
- [Linear Perpetuals](#linear-perpetuals)
- [Inverse Perpetuals](#inverse-perpetuals)
- [Dated Futures](#dated-futures)
- [OKX Instrument Families](#okx-instrument-families)
- [Inspecting Downloaded Archives](#inspecting-downloaded-archives)
- [Existing Archives](#existing-archives)
- [Examples](#examples)

## Basic Workflow

Before downloading data, inspect the acquisition plan:

```bash
marketforge plan \
    --exchange bybit \
    --type perpetual \
    --category linear \
    --symbol BTCUSDT \
    --data trade_ticks \
    --start 2026-09-01 \
    --end 2026-09-04
```

If the plan is correct, run the same request with `download`:

```bash
marketforge download \
    --exchange bybit \
    --type perpetual \
    --category linear \
    --symbol BTCUSDT \
    --data trade_ticks \
    --start 2026-09-01 \
    --end 2026-09-04
```

The conceptual workflow is:

```text
marketforge plan
        ↓
resolve exchange source
        ↓
validate market/data capability
        ↓
discover remote archives
        ↓
resolve local destination paths
        ↓
show acquisition plan


marketforge download
        ↓
perform the same planning
        ↓
reuse existing archives
        +
download missing archives
        ↓
data/raw/
```

Use `plan` when you want to inspect what will happen without downloading anything.

Use `download` when you are ready to acquire the files.

## Date Range Semantics

MarketForge uses half-open date ranges:

```text
[start, end)
```

The start date is included.

The end date is excluded.

For example:

```bash
--start 2026-09-01 \
--end 2026-09-04
```

means:

```text
2026-09-01
2026-09-02
2026-09-03
```

It does **not** conceptually request September 4.

This convention avoids ambiguity when adjacent ranges are combined:

```text
[2026-09-01, 2026-09-04)
[2026-09-04, 2026-09-07)
```

The two intervals join without overlap.

> Exchange archive boundaries may differ internally. MarketForge's user-facing request model remains `[start, end)`.

## Planning Downloads

General form:

```bash
marketforge plan \
    --exchange EXCHANGE \
    --type INSTRUMENT_TYPE \
    --category MARKET_CATEGORY \
    --symbol SYMBOL \
    --data DATA_TYPE \
    --start YYYY-MM-DD \
    --end YYYY-MM-DD
```

Example:

```bash
marketforge plan \
    --exchange bybit \
    --type perpetual \
    --category linear \
    --symbol BTCUSDT \
    --data order_book_l2 \
    --start 2026-09-01 \
    --end 2026-09-04
```

Planning performs remote discovery but does not download archive contents.

It is useful for checking:

```text
exchange
instrument
market type
dataset
requested interval
remote files
local destinations
existing files
files still requiring download
```

before committing to an acquisition.

## Downloading Data

`download` accepts the same acquisition request:

```bash
marketforge download \
    --exchange bybit \
    --type perpetual \
    --category linear \
    --symbol BTCUSDT \
    --data order_book_l2 \
    --start 2026-09-01 \
    --end 2026-09-04
```

Downloaded archives are stored in the normal MarketForge raw-data hierarchy:

```text
data/
└── raw/
    └── {exchange}/
        └── {instrument_type}/
            └── {market_category}/
                └── {data_type}/
                    └── {symbol}/
                        └── source archives
```

For example:

```text
data/raw/bybit/perpetual/linear/trade_ticks/BTCUSDT/
```

and:

```text
data/raw/bybit/perpetual/linear/order_book_l2/BTCUSDT/
```

Raw archives are preserved as downloaded.

Processing into canonical Parquet datasets is a separate stage.

## Required Market Arguments

### `--exchange`

Select the exchange:

```text
bybit
binance
okx
bitget
gateio
```

Example:

```bash
--exchange bybit
```

### `--type`

Select the instrument type:

```text
spot
perpetual
future
option
```

For the current non-option workflow:

```text
spot
perpetual
future
```

Examples:

```bash
--type spot
```

```bash
--type perpetual
```

```bash
--type future
```

### `--category`

Select the market category:

```text
spot
linear
inverse
option
```

Common combinations are:

```text
TYPE         CATEGORY

spot         spot
perpetual    linear
perpetual    inverse
future       linear
future       inverse
```

Do not treat `type` and `category` as interchangeable.

For example:

```text
perpetual + linear
```

describes a perpetual linear derivative, while:

```text
future + linear
```

describes a dated linear future.

### `--symbol`

Use the exchange-native symbol stored in MarketForge metadata.

Examples:

```text
Bybit
BTCUSDT

Binance
BTCUSD_PERP

OKX
BTC-USDT-SWAP

Gate.io
BTC_USDT
```

Do not assume symbol syntax is identical across exchanges.

Use:

```bash
marketforge metadata list
```

or:

```bash
marketforge instruments
```

to discover valid instruments.

### `--data`

Select the raw dataset:

```text
trade_ticks
order_book_l2
```

Trades:

```bash
--data trade_ticks
```

Order-book data:

```bash
--data order_book_l2
```

Support depends on the exchange and market.

For example, Binance currently provides historical trade archives within MarketForge's supported acquisition scope, but historical L2 is not supported.

## Trade and Order-Book Data

Trades and order books are separate raw datasets.

To obtain both for the same market, issue two downloads.

Trades:

```bash
marketforge download \
    --exchange bybit \
    --type perpetual \
    --category linear \
    --symbol BTCUSDT \
    --data trade_ticks \
    --start 2026-09-01 \
    --end 2026-09-04
```

L2:

```bash
marketforge download \
    --exchange bybit \
    --type perpetual \
    --category linear \
    --symbol BTCUSDT \
    --data order_book_l2 \
    --start 2026-09-01 \
    --end 2026-09-04
```

They are stored separately:

```text
data/raw/bybit/perpetual/linear/
├── trade_ticks/
│   └── BTCUSDT/
└── order_book_l2/
    └── BTCUSDT/
```

Later processing can normalize these into canonical:

```text
trade
l2
```

datasets and merge them where appropriate.

## Spot Markets

Spot uses:

```text
--type spot
--category spot
```

Example:

```bash
marketforge plan \
    --exchange bybit \
    --type spot \
    --category spot \
    --symbol BTCUSDT \
    --data trade_ticks \
    --start 2026-09-01 \
    --end 2026-09-04
```

Download:

```bash
marketforge download \
    --exchange bybit \
    --type spot \
    --category spot \
    --symbol BTCUSDT \
    --data trade_ticks \
    --start 2026-09-01 \
    --end 2026-09-04
```

Non-USDT quote pairs work the same way.

For example:

```bash
marketforge download \
    --exchange bybit \
    --type spot \
    --category spot \
    --symbol ETHUSDC \
    --data trade_ticks \
    --start 2026-09-01 \
    --end 2026-09-04
```

## Linear Perpetuals

Use:

```text
--type perpetual
--category linear
```

Bybit example:

```bash
marketforge download \
    --exchange bybit \
    --type perpetual \
    --category linear \
    --symbol BTCUSDT \
    --data trade_ticks \
    --start 2026-09-01 \
    --end 2026-09-04
```

Bitget:

```bash
marketforge download \
    --exchange bitget \
    --type perpetual \
    --category linear \
    --symbol BTCUSDT \
    --data order_book_l2 \
    --start 2026-09-01 \
    --end 2026-09-04
```

Gate.io:

```bash
marketforge download \
    --exchange gateio \
    --type perpetual \
    --category linear \
    --symbol BTC_USDT \
    --data trade_ticks \
    --start 2026-09-01 \
    --end 2026-09-04
```

Exchange-native symbols differ even when the economic market is equivalent.

## Inverse Perpetuals

Use:

```text
--type perpetual
--category inverse
```

Binance example:

```bash
marketforge download \
    --exchange binance \
    --type perpetual \
    --category inverse \
    --symbol BTCUSD_PERP \
    --data trade_ticks \
    --start 2026-09-01 \
    --end 2026-09-04
```

MarketForge metadata for contract markets may include:

```text
quantity_type
contract_value
contract_value_asset
settlement_asset
```

For example, an inverse contract may be represented conceptually as:

```text
quantity_type        = contracts
contract_value       = 100
contract_value_asset = USD
```

These values become important during normalization.

Inspect them with:

```bash
marketforge metadata list \
    --base BTC \
    --type perpetual \
    --category inverse
```

## Dated Futures

Use:

```text
--type future
```

with either:

```text
--category linear
```

or:

```text
--category inverse
```

Linear Binance example:

```bash
marketforge download \
    --exchange binance \
    --type future \
    --category linear \
    --symbol BTCUSDT_261225 \
    --data trade_ticks \
    --start 2026-09-01 \
    --end 2026-09-04
```

Inverse Binance example:

```bash
marketforge download \
    --exchange binance \
    --type future \
    --category inverse \
    --symbol BTCUSD_261225 \
    --data trade_ticks \
    --start 2026-09-01 \
    --end 2026-09-04
```

Discover currently known dated futures before choosing a symbol:

```bash
marketforge metadata list \
    --exchange binance \
    --type future \
    --category linear \
    --base BTC
```

or:

```bash
marketforge metadata list \
    --exchange binance \
    --type future \
    --category inverse \
    --base BTC
```

This is preferable to hardcoding a dated contract indefinitely because futures expire.

## OKX Instrument Families

OKX non-spot historical discovery requires the instrument family.

For example:

```text
instrument:
BTC-USDT-SWAP

family:
BTC-USDT
```

Therefore an OKX linear perpetual request is:

```bash
marketforge download \
    --exchange okx \
    --type perpetual \
    --category linear \
    --symbol BTC-USDT-SWAP \
    --family BTC-USDT \
    --data trade_ticks \
    --start 2026-09-01 \
    --end 2026-09-04
```

For inverse BTC derivatives:

```text
family = BTC-USD
```

Example:

```bash
marketforge download \
    --exchange okx \
    --type perpetual \
    --category inverse \
    --symbol BTC-USD-SWAP \
    --family BTC-USD \
    --data order_book_l2 \
    --start 2026-09-01 \
    --end 2026-09-04
```

Dated futures also require the appropriate family:

```bash
marketforge download \
    --exchange okx \
    --type future \
    --category inverse \
    --symbol BTC-USD-261225 \
    --family BTC-USD \
    --data trade_ticks \
    --start 2026-09-01 \
    --end 2026-09-04
```

Spot requests do not require this non-spot family argument.

## Inspecting Downloaded Archives

After downloading, inspect the local raw inventory:

```bash
marketforge archives
```

Filter by exchange:

```bash
marketforge archives \
    --exchange bybit
```

Filter down to one market:

```bash
marketforge archives \
    --exchange bybit \
    --type perpetual \
    --category linear \
    --data trade_ticks \
    --symbol BTCUSDT
```

Filter by date:

```bash
marketforge archives \
    --exchange bybit \
    --type perpetual \
    --category linear \
    --data trade_ticks \
    --symbol BTCUSDT \
    --start 2026-09-01 \
    --end 2026-09-04
```

The archive inventory reflects physical source files.

Different exchanges use different archive granularities.

For example, an exchange may expose:

```text
daily archives
hourly archives
monthly archives
multiple archive parts per day
```

Therefore three requested days do **not** necessarily mean three physical files.

Examples from supported source structures include:

```text
Bybit
daily files

Bitget
multiple trade archive parts per day

Gate.io
hourly L2 files
monthly trade archives

OKX
daily archives
```

MarketForge preserves these source-level differences in `data/raw/`.

## Existing Archives

`marketforge download` is safe to rerun against the same request.

The acquisition plan determines which expected archives already exist locally and which still require downloading.

Conceptually:

```text
requested remote files
        ↓
compare with local destinations
        ↓
┌───────────────────────┬───────────────────────┐
│ already present       │ missing               │
│ reuse                 │ download              │
└───────────────────────┴───────────────────────┘
```

This makes commands such as:

```bash
marketforge download \
    --exchange bybit \
    --type perpetual \
    --category linear \
    --symbol BTCUSDT \
    --data trade_ticks \
    --start 2026-09-01 \
    --end 2026-09-04
```

safe to rerun when part or all of the requested data has already been acquired.

Use `plan` first when you want to inspect the resulting acquisition decision.

## Examples

### Plan three days of Bybit trades

```bash
marketforge plan \
    --exchange bybit \
    --type perpetual \
    --category linear \
    --symbol BTCUSDT \
    --data trade_ticks \
    --start 2026-09-01 \
    --end 2026-09-04
```

### Download the same data

```bash
marketforge download \
    --exchange bybit \
    --type perpetual \
    --category linear \
    --symbol BTCUSDT \
    --data trade_ticks \
    --start 2026-09-01 \
    --end 2026-09-04
```

### Download Bybit L2

```bash
marketforge download \
    --exchange bybit \
    --type perpetual \
    --category linear \
    --symbol BTCUSDT \
    --data order_book_l2 \
    --start 2026-09-01 \
    --end 2026-09-04
```

### Download Binance inverse trades

```bash
marketforge download \
    --exchange binance \
    --type perpetual \
    --category inverse \
    --symbol BTCUSD_PERP \
    --data trade_ticks \
    --start 2026-09-01 \
    --end 2026-09-04
```

### Download OKX trades

```bash
marketforge download \
    --exchange okx \
    --type perpetual \
    --category linear \
    --symbol BTC-USDT-SWAP \
    --family BTC-USDT \
    --data trade_ticks \
    --start 2026-09-01 \
    --end 2026-09-04
```

### Download OKX L2

```bash
marketforge download \
    --exchange okx \
    --type perpetual \
    --category linear \
    --symbol BTC-USDT-SWAP \
    --family BTC-USDT \
    --data order_book_l2 \
    --start 2026-09-01 \
    --end 2026-09-04
```

### Inspect the resulting raw archives

```bash
marketforge archives \
    --exchange okx \
    --type perpetual \
    --category linear \
    --symbol BTC-USDT-SWAP
```

The acquisition layer ends with immutable raw archives under:

```text
data/raw/
```

Those archives become the inputs to:

```text
marketforge process
```

which resolves catalog metadata and processing configuration, constructs an immutable processing job, and passes the raw source data into the canonical processing pipeline.