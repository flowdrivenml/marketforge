## Objective

Convert market data from different instruments into a common denomination while preserving original canonical datasets.

For example:

```text
ETHBTC + BTCUSDT → ETHUSDT_converted
ETHSOL + SOLBTC + BTCUSDT → ETHUSDT_converted
```

## Conversion Graph

MarketForge builds a graph of available trading pairs using the instrument catalog.

Each instrument provides two possible conversion directions:

- Base → Quote: multiply by price.
- Quote → Base: divide by price.

The engine automatically discovers conversion paths between currencies.

Path selection considers:

- Number of conversion hops.
- Market liquidity and reliability.
- Reference data availability.
- Exchange and instrument compatibility.

The source instrument is excluded from its own reference path by default.

## Conversion Model

Use **Model A — Event-Level Conversion**.

Only source instrument events trigger converted output. Changes in reference instruments do not generate additional events.

For each source event, use the latest valid reference midpoint at or before its timestamp.

For a multi-hop conversion:

\[
R(t) = \prod_{i=1}^{n} R_i(t)
\]

Where each \(R_i\) is the appropriate direct or reciprocal reference midpoint.

Converted values:

\[
P_{\text{converted}} = P_{\text{source}} \times R(t)
\]

\[
Q_{\text{quote,converted}} = Q_{\text{base}} \times P_{\text{converted}}
\]

Base quantities remain unchanged.

The result is an event-level valuation, **not a continuously reconstructable synthetic order book**.

## Timestamp Alignment

Use backward as-of alignment to avoid future information.

For every reference instrument:

\[
\Delta t_i = t_{\text{source}} - t_{\text{reference},i}
\]

Preserve:

- Reference timestamps.
- Reference midpoint values.
- Individual reference ages.
- Maximum reference age across the conversion path.

Reject or flag conversions with missing, invalid, or excessively stale reference prices.

## Performance

Resolve conversion paths once before processing.

Maintain the latest valid midpoint for each reference instrument in memory.

```text
Instrument Catalog
        ↓
Currency Graph
        ↓
Conversion Path
        ↓
Chronological Streaming Merge
        ↓
Cached Reference Midprices
        ↓
Event-Level Conversion
        ↓
Derived Parquet Dataset
```

Avoid repeated graph searches and per-level reference lookups.

## Output

Create a separate derived dataset:

```text
ETHBTC              → Original canonical dataset
BTCUSDT             → Original canonical dataset
ETHUSDT_converted   → Derived dataset
```

Preserve conversion metadata:

```text
source_instrument_id
conversion_path_id
reference_timestamps
reference_ages
max_reference_age_ns
conversion_method
```

Use Decimal256 for exact numerical representation.

## Design Decisions

- Native canonical datasets remain immutable.
- Conversion is optional and performed only when requested.
- Support direct, reciprocal, and multi-hop conversions.
- Use backward as-of midpoint alignment.
- Convert only source events (Model A).
- Use one fixed primary conversion path per job.
- Allow explicitly configured fallback paths.
- Preserve conversion provenance and timestamp differences.
- Never silently use stale or invalid reference data.
- Store results in separate derived Parquet datasets.

**Stage 2 converts independently processed instruments into a common denomination using graph-based reference pricing, without modifying their original market data.**