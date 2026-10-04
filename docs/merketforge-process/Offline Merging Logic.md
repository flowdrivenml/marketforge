MarketForge can produce canonical historical datasets at several levels:

```text
Trades
Depth
Combined Trades + Depth
Cross-dataset synchronized streams
```

The most important output for downstream feature generation is the **combined chronological event stream**, where trades and depth events are interleaved in exact event-time order.

Economic normalization is separate. Any canonical events can share a timeline, while prices from different instruments require explicit conversion or transformation before they can be compared economically.

## Quick Navigation

- [Canonical Dataset Outputs](#canonical-dataset-outputs)
- [Combined Trades and Depth](#combined-trades-and-depth)
- [Chronological Ordering](#chronological-ordering)
- [Cross-Dataset Merge](#cross-dataset-merge)
- [Common Numeraire](#common-numeraire)
- [Conversion Paths](#conversion-paths)
- [Reference Markets](#reference-markets)
- [Temporal Alignment](#temporal-alignment)
- [Disconnected Instruments](#disconnected-instruments)
- [Depth Merging](#depth-merging)
- [Relative Depth](#relative-depth)
- [Merge Operations](#merge-operations)
- [Options and Merging](#options-and-merging)
- [Core Rules](#core-rules)

## Canonical Dataset Outputs

`marketforge-process` supports three primary canonical content modes:

```text
trades
depth
combined
```

### Trades

Contains only canonical executions:

```text
Trade
Trade
Trade
...
```

### Depth

Contains the events required to represent and reconstruct the order book:

```text
L2Snapshot
L2Update
L2Update
L2Update
...
```

### Combined

Contains trades and depth in one chronological event stream:

```text
L2Snapshot
L2Update
Trade
L2Update
Trade
L2Update
...
```

`combined` is a first-class output rather than something downstream consumers must construct themselves.

## Combined Trades and Depth

For one instrument, the combined stream is:

\[
E_{\text{combined}}
=
\operatorname{sort}
\left(
E_{\text{trades}}
\cup
E_{\text{depth}}
\right)
\]

For example:

```text
09:00:00.000100  L2Snapshot
09:00:00.000130  L2Update
09:00:00.000170  Trade
09:00:00.000190  L2Update
09:00:00.000240  Trade
09:00:00.000270  L2Update
```

This allows a downstream consumer to process:

```text
event
    ↓
update current market state
    ↓
generate features
    ↓
next event
```

The current book at every trade can therefore be reconstructed from the same ordered stream.

Combined datasets retain the canonical event type and all source identity:

```text
event_type
exchange
instrument_id
event_timestamp_ns
```

Trade-specific and L2-specific fields remain available according to the event type.

## Chronological Ordering

All merged output must be deterministic.

The primary ordering key is:

```text
event_timestamp_ns
```

Events with identical timestamps require deterministic tie-breaking using available source information such as:

```text
native sequence
source ordering
event type
stable original order
```

The exact tie-breaking policy must be defined once and applied consistently.

A combined dataset must never depend on nondeterministic worker completion order.

## Cross-Dataset Merge

Any canonical datasets can be merged chronologically because every event retains its source identity.

For \(n\) input datasets:

\[
E_{\text{merged}}
=
\operatorname{sort}
\left(
E_1 \cup E_2 \cup \dots \cup E_n
\right)
\]

For example:

```text
09:00:00.000100  Binance  BTCUSDT       Trade
09:00:00.000130  Bybit    BTCUSDT       L2Update
09:00:00.000170  OKX      BTC-USDT-SWAP Trade
09:00:00.000190  Binance  BTCUSDT       L2Update
```

This operation is lossless.

It does **not** imply that the instruments are economically interchangeable.

A cross-exchange combined dataset may therefore contain:

```text
Trade
L2Snapshot
L2Update
```

from many instruments and exchanges while preserving each event's original identity.

A downstream consumer may maintain independent books:

```text
Book[Binance, BTCUSDT]
Book[Bybit, BTCUSDT]
Book[OKX, BTC-USDT-SWAP]
...
```

while consuming one globally ordered event stream.

## Common Numeraire

Chronological merging requires no price conversion.

Economic comparison does.

Prices quoted in different assets can be converted into a common numeraire such as:

```text
USDT
USD
USDC
BTC
```

If an instrument is already quoted in the target numeraire:

\[
P^{*}_{BTC/USDT}
=
P_{BTC/USDT}
\]

No conversion is required.

If another instrument shares an asset with the reference instrument:

```text
ETHBTC
BTCUSDT
```

then:

\[
P_{ETH/USDT}
=
P_{ETH/BTC}
\times
P_{BTC/USDT}
\]

Example:

```text
ETHBTC  = 0.035
BTCUSDT = 100,000
```

therefore:

\[
P_{ETH/USDT}
=
0.035
\times
100{,}000
=
3{,}500
\]

No external `ETHUSDT` dataset is required.

## Conversion Paths

The general conversion rule is:

\[
P_{A/C}
=
P_{A/B}
\times
P_{B/C}
\]

Longer paths follow the same rule:

\[
P_{A/D}
=
P_{A/B}
\times
P_{B/C}
\times
P_{C/D}
\]

For example:

```text
SOLETH
ETHBTC
BTCUSDT
```

gives:

\[
P_{SOL/USDT}
=
P_{SOL/ETH}
\times
P_{ETH/BTC}
\times
P_{BTC/USDT}
\]

Inverse traversal uses the reciprocal:

\[
P_{B/A}
=
\frac{1}{P_{A/B}}
\]

Available instruments therefore form a conversion network:

```text
SOL ─── ETH ─── BTC ─── USDT
               │
              EOS
```

For:

```text
ETHEOS
EOSBTC
BTCUSDT
```

MarketForge can derive:

\[
P_{ETH/USDT}
=
P_{ETH/EOS}
\times
P_{EOS/BTC}
\times
P_{BTC/USDT}
\]

Shorter conversion paths are preferable because every additional market introduces another price source and another temporal-alignment dependency.

## Reference Markets

When multiple conversion routes exist, MarketForge should prefer a high-quality reference market.

Initial selection can prioritize:

```text
highest volume
shortest conversion path
```

Later selection may incorporate:

```text
spread
depth
data quality
uptime
price-discovery score
```

For example, if `BTCUSDT` is the preferred BTC reference:

```text
BTCUSDT
    → direct

ETHBTC
    → ETHBTC × BTCUSDT

SOLBTC
    → SOLBTC × BTCUSDT
```

A direct high-liquidity route should normally be preferred over an unnecessarily long synthetic route.

Multiple valid paths may still be useful analytically.

For example:

\[
P_{ETH/USDT}^{direct}
\]

can be compared against:

\[
P_{ETH/USDT}^{synthetic}
=
P_{ETH/BTC}
\times
P_{BTC/USDT}
\]

The difference measures cross-market divergence.

## Temporal Alignment

Conversion prices must correspond to approximately the same market time.

For an `ETHBTC` event at time \(t\):

\[
P_{ETH/USDT}(t)
=
P_{ETH/BTC}(t)
\times
P_{BTC/USDT}(t)
\]

Exact event timestamps will usually differ.

Offline processing can use an as-of relationship:

```text
ETHBTC event @ T
        +
latest valid BTCUSDT reference <= T
        ↓
synthetic ETHUSDT price @ T
```

Reference freshness must be preserved or measurable.

Long conversion paths accumulate more timing uncertainty:

```text
SOLETH @ T₁
ETHBTC @ T₂
BTCUSDT @ T₃
```

where:

\[
T_1 \approx T_2 \approx T_3
\]

but they are not necessarily identical.

This is another reason to prefer short, liquid conversion paths.

## Disconnected Instruments

Some datasets may have no conversion path to the selected numeraire.

For example:

```text
BTC ↔ USDT
ETH ↔ BTC
SOL ↔ ETH

XYZ ↔ ABC
```

`XYZABC` is disconnected from the USDT component.

MarketForge must not fabricate a conversion.

```text
no conversion path
    ↓
cannot participate in numeraire-normalized comparison
```

The canonical dataset remains valid and can still participate in ordinary chronological merging.

## Depth Merging

Depth requires a distinction between **event merging** and **liquidity aggregation**.

Depth events from any instruments can be chronologically merged:

\[
E_{\text{depth}}
=
\operatorname{sort}
\left(
E_{\text{depth},1}
\cup
E_{\text{depth},2}
\cup
\dots
\right)
\]

Each instrument retains its own independent book.

However, raw price levels from economically different books cannot simply be combined.

For example:

```text
BTCUSDT
mid = 100,000

ETHUSDT
mid = 3,500
```

Combining:

```text
100,001 → BTC liquidity
3,501   → ETH liquidity
```

into one absolute-price book has no useful economic meaning.

Even related BTC instruments may differ:

```text
BTC spot
BTC perpetual
BTC dated future
```

because basis and term structure are real market information.

Canonical merging therefore preserves independent books.

Liquidity aggregation requires an explicit transformation.

## Relative Depth

Book shape can optionally be compared by transforming levels into distance from a reference price.

A useful coordinate is basis points:

\[
d_{\text{bps}}
=
10{,}000
\left(
\frac{P_{\text{level}}}
     {P_{\text{reference}}}
-
1
\right)
\]

For example:

```text
-10 bps
 -5 bps
 -1 bp
  0
 +1 bp
 +5 bps
+10 bps
```

Two books can then be compared at equivalent relative distances:

```text
              Bybit      Binance
-10 bps       8 BTC       6 BTC
 -5 bps       4 BTC       5 BTC
 -1 bp        2 BTC       3 BTC
```

Aggregated relative liquidity becomes:

```text
-10 bps → 14 BTC
 -5 bps →  9 BTC
 -1 bp  →  5 BTC
```

Possible references include:

```text
local instrument midpoint
common underlying reference
index price
explicit reference instrument
```

Using the local midpoint compares **book shape**.

Using a common underlying reference preserves relationships such as:

```text
spot/perpetual basis
futures premium
cross-venue price divergence
```

Relative depth is derived data.

Native canonical prices are never overwritten.

## Merge Operations

MarketForge distinguishes several operations.

### Combine

Combine trades and depth belonging to the same dataset scope:

\[
E_{\text{combined}}
=
\operatorname{sort}
\left(
E_{\text{trades}}
\cup
E_{\text{depth}}
\right)
\]

Typical output:

```text
L2Snapshot
L2Update
Trade
L2Update
Trade
...
```

This is the primary representation used for downstream event-driven feature generation.

### Merge

Losslessly combine multiple canonical event streams:

\[
E_{\text{merged}}
=
\operatorname{sort}
\left(
\bigcup_i E_i
\right)
\]

No economic transformation is required.

### Convert

Express native prices in a common numeraire:

\[
P_{A/C}
=
P_{A/B}
\times
P_{B/C}
\]

Conversion may use one or more bridge instruments.

### Transform

Map prices into another coordinate system:

\[
d_{\text{bps}}
=
10{,}000
\left(
\frac{P}
     {P_{\text{ref}}}
-
1
\right)
\]

### Aggregate

Combine economically comparable transformed liquidity:

```text
converted / transformed books
        ↓
comparable price coordinates
        ↓
aggregate liquidity
        ↓
synthetic depth representation
```

These operations remain separate:

```text
Canonical Data
      ↓
Combine / Merge
      ↓
optional Convert
      ↓
optional Transform
      ↓
optional Aggregate
```

## Options and Merging

Options are normalized and preserved by MarketForge but are excluded from economic price/depth merging.

Option prices depend on additional dimensions:

```text
underlying price
strike
expiry
option type
implied volatility
```

For the initial processing engine:

```text
Options
    ✓ parse
    ✓ normalize
    ✓ validate
    ✓ write canonical Parquet
    ✓ preserve IV / mark / index metadata when available
    ✓ chronological event merging when useful

    ✗ common-price depth aggregation
    ✗ IV transformation
    ✗ volatility-surface construction
    ✗ strike/expiry normalization
```

`marketforge-process` focuses economic merging and price conversion on:

```text
Spot
Perpetuals
Futures
```

Advanced option transformations are outside the current processing scope.

## Core Rules

MarketForge offline merging follows these rules:

- `trades`, `depth`, and `combined` are first-class dataset outputs
- combined datasets interleave trades and L2 events chronologically
- combined output is directly replayable for event-driven feature generation
- any canonical datasets may be chronologically merged
- chronological merging never requires price conversion
- every merged event retains exchange and instrument identity
- different instruments retain independent reconstructed books
- deterministic tie-breaking is required for equal timestamps
- economic comparison requires compatible units or an explicit transformation
- native prices are never overwritten
- common-numeraire prices are derived values
- conversion uses temporally aligned reference prices
- direct and short high-liquidity conversion paths are preferred
- inverse conversion paths use reciprocal prices
- disconnected instruments are never assigned fabricated conversions
- raw depth from economically different instruments is never blindly aggregated
- relative-depth aggregation requires an explicit reference-price transformation
- options are excluded from economic price/depth merging
- transformation provenance must remain traceable to canonical inputs