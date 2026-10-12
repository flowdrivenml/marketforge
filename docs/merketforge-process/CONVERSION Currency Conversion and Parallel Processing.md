## Overview

MarketForge will support time-aware currency conversion across instruments with different base and quote currencies.

Examples:

- ETHBTC → ETHUSDT
- BTCUSDC → BTCUSDT
- ETHBTC → ETHUSDC
- Multi-hop conversion across several intermediate currencies.

Conversion will be implemented as a **separate processing stage after chronological merging**.

The merging engine will preserve original canonical data without modifying prices or quantities.

The conversion engine will produce derived datasets containing converted prices, conversion paths, source references, and quality metadata.

### Core Decisions

| Property | Decision |
|---|---|
| Implementation | MarketForge Rust engine |
| Execution | Parallel, partition-based |
| Input | Merged chronological timeline |
| Output | Separate derived Parquet dataset |
| Original prices | Never modified |
| Conversion graph | Directed, with inverse edges |
| Multiple paths | Supported |
| Default maximum hops | 3 |
| Timestamp policy | Deterministic timeline ordering |
| Reference staleness | Configurable |
| Checkpoint generation | Automatic |
| Initial state resolution | Indexed backward lookup |
| Checkpoint caching | Supported later |
| Conversion provenance | Required |

## Architecture

Currency conversion will remain independent of chronological merging.

```text
Canonical Trades + Depth
          |
          v
   Chronological Merge
          |
          v
     Unified Timeline
          |
          v
    Conversion Planner
          |
          v
   Dependency Resolution
          |
          v
   Partition Checkpoints
          |
    +-----+-----+
    |     |     |
    v     v     v
 Worker Worker Worker
    |     |     |
    +-----+-----+
          |
          v
    Converted Parquet
```

This separation allows multiple conversion strategies to operate on the same merged dataset without repeating the merge.

## Conversion Graph

Trading pairs form a directed currency graph.

Each pair supports two conversion directions.

For ETHBTC:

- ETH → BTC: multiply by ETHBTC.
- BTC → ETH: divide by ETHBTC.

### Example

Available instruments:

```text
BTCUSDT
ETHBTC
ETHUSDC
USDCUSDT
BTCUSDC
```

Possible ETH → USDT conversion paths:

```text
ETH → BTC → USDT

ETH → USDC → USDT

ETH → BTC → USDC → USDT
```

Each path produces an independently calculated price.

### Maximum Hops

`max_hops` limits the number of conversion edges in a path.

| Value | Meaning |
|---|---|
| 1 | Direct conversion |
| 2 | One intermediate currency |
| 3 | Two intermediate currencies |
| 4 | Three intermediate currencies |

Default:

```json
"max_hops": 3
```

Repeated currencies within a conversion path are prohibited to prevent cycles.

The maximum number of paths is separate from the maximum number of hops.

## Conversion Configuration

Currency conversion will use a separate job configuration.

Proposed configuration:

```json
{
  "job_id": 201,
  "operation": "convert",
  "input_merge": 200,

  "conversion": {
    "target_quote": "USDT",
    "price_source": "last_trade",
    "path_policy": "all",
    "max_hops": 3,
    "max_staleness_ns": 5000000000,
    "same_timestamp_policy": "timeline_order"
  },

  "resources": {
    "workers": 16,
    "memory_limit_mb": 65536,
    "max_open_files": 1024
  },

  "partitioning": {
    "partitions_per_worker": 4
  },

  "output": {
    "dataset_path": "data/converted/201"
  }
}
```

This configuration is a design proposal and has not yet been implemented.

The user specifies conversion requirements, not checkpoint locations.

MarketForge automatically determines the required reference instruments and initial states.

## Timestamp Ordering

MarketForge will use **Policy B — Deterministic Timeline Ordering**.

The ordering key is:

\[
K = (timestamp,\ stream\_rank,\ task\_rank,\ event\_ordinal)
\]

Reference observations become available according to their position in this ordering.

### Example

```text
Timestamp       Rank    Event
------------------------------------
12:00:00.000    0       BTCUSDT trade
12:00:00.000    1       ETHBTC trade
12:00:00.000    2       ETHUSDC trade
```

Processing order:

1. Update BTCUSDT reference state.
2. Convert ETHBTC using the updated state.
3. Process ETHUSDC using currently available reference states.

This ordering is deterministic but does not necessarily represent the true execution order of simultaneous exchange events.

The conversion policy must remain consistent across sequential and parallel execution.

## Parallel Conversion and Checkpoints

Currency conversion is stateful but does not require sequential processing of the entire dataset.

Each partition can execute independently once its initial reference state has been established.

### Indexed Backward Lookup

For a partition beginning at timestamp \(t_0\), MarketForge searches backward through indexed Parquet data to locate the latest valid reference observations.

```text
Partition Start
      |
      v
Search Current File
      |
      v
Search Previous Files
      |
      v
Resolve Reference State
      |
      v
Initialize Worker
      |
      v
Process Partition
```

Backward lookup should use Parquet row-group statistics and page indexes where available.

If an observation is unavailable, the corresponding reference state remains unresolved.

### Partition Checkpoints

A checkpoint contains the latest known state of all required reference instruments.

Example:

```json
{
  "timestamp_ns": 1788224400000000000,

  "reference_prices": {
    "BTCUSDT": {
      "price": "65000.25",
      "observation_timestamp_ns": 1788224399998000000
    },
    "ETHBTC": {
      "price": "0.04001",
      "observation_timestamp_ns": 1788224399999000000
    }
  }
}
```

Production checkpoints must additionally preserve source event identities and sufficient ordering metadata.

A single checkpoint can support multiple conversion paths.

Separate checkpoints per conversion path are unnecessary when those paths share the same underlying reference state.

### Checkpoint Boundary Semantics

For a partition beginning at \(t_0\):

- Initialize using observations strictly preceding the partition's first event in the deterministic timeline.
- Process all events assigned to the partition in timeline order.
- Never apply a boundary event twice.
- Never expose observations that occur later in the deterministic ordering.

Sequential and parallel conversion must produce identical results.

## Reference Price Staleness

`max_staleness_ns` defines the maximum permitted age of a reference price.

The value is expressed in nanoseconds.

Example:

```json
"max_staleness_ns": 5000000000
```

This corresponds to **5 seconds**.

### Example

Suppose the latest BTCUSDT observation occurred at:

```text
12:00:00.000
```

An ETHBTC trade occurs at:

```text
12:00:02.000
```

Reference age:

\[
2\text{ seconds}
\]

Conversion is allowed.

If the ETHBTC trade occurs at:

```text
12:00:07.000
```

Reference age:

\[
7\text{ seconds}
\]

The reference exceeds the configured maximum age.

The conversion is marked as stale rather than producing an unreliable value.

### Common Values

| Nanoseconds | Duration |
|---|---|
| 1,000,000 | 1 millisecond |
| 100,000,000 | 100 milliseconds |
| 1,000,000,000 | 1 second |
| 5,000,000,000 | 5 seconds |
| 30,000,000,000 | 30 seconds |
| 60,000,000,000 | 1 minute |

### Multi-Hop Staleness

Every reference observation in a conversion path must satisfy the staleness requirement independently.

For:

```text
ETH → BTC → USDC → USDT
```

All three conversion edges must have valid reference prices.

If one reference is stale, the entire path becomes unavailable.

Other valid paths remain usable.

### Important Distinction

Staleness measures observation age, not necessarily price accuracy.

An illiquid market may remain stable without trades for several minutes, while a highly volatile market can change substantially within milliseconds.

Therefore:

- Staleness must be configurable.
- No universal threshold should be enforced.
- Actual reference ages should be preserved.
- Different price sources may eventually use different thresholds.

## Conversion Path Selection

MarketForge will support multiple conversion paths.

Example:

```text
ETH → BTC → USDT
ETH → USDC → USDT
```

Possible outputs:

```text
Path 1: 2600.00 USDT
Path 2: 2601.25 USDT
```

The difference may reflect market inefficiencies, liquidity differences, stale prices, or exchange-specific behavior.

### Path Policies

Planned policies:

| Policy | Behavior |
|---|---|
| `all` | Preserve all valid paths |
| `shortest` | Prefer paths with fewer hops |
| `freshest` | Prefer paths using newer observations |
| `best_executable` | Prefer the best executable conversion after costs |

The `best_executable` policy requires bid/ask and liquidity-aware pricing and will be implemented separately.

For research, `all` is the preferred initial policy.

## Edge Cases

### Missing Reference

A required reference instrument has no available observation.

Result:

```text
missing_reference
```

### Stale Reference

A required observation exceeds the configured age threshold.

Result:

```text
stale_reference
```

### Invalid Price

A conversion price is zero, negative, non-finite, or otherwise invalid.

Result:

```text
invalid_price
```

### No Conversion Path

The currency graph contains no valid route to the target quote currency.

Result:

```text
no_conversion_path
```

### Multiple Valid Paths

Preserve each path independently when `path_policy = "all"`.

Do not silently average prices.

### Stablecoin Conversion

USDC, USDT, USD, and other quote currencies must remain distinct.

A 1:1 stablecoin conversion must never be assumed unless explicitly configured.

### Cross-Exchange References

Conversion paths may reference instruments from different exchanges.

Source exchange identity and observation timestamps must remain available.

### Missing Historical State

If no valid reference exists before a partition begins, the worker must not invent an initial price.

It should leave the state unresolved until a valid observation becomes available.

### Checkpoint Invalidation

Cached checkpoints depend on:

- Source dataset versions.
- Reference instruments.
- Timeline ordering.
- Conversion price-source policy.
- Relevant state construction rules.

Changes to these dependencies must invalidate affected checkpoints.

## Implementation Plan

### Phase 1 — Reference Resolution

Implement indexed backward lookup for the latest valid reference observation before an arbitrary timeline position.

### Phase 2 — Conversion Graph

Implement currency graph construction, inverse edges, path discovery, maximum-hop constraints, and cycle prevention.

### Phase 3 — Partition State

Construct initial reference states for independently executable partitions.

### Phase 4 — Parallel Conversion

Reuse MarketForge's existing resource-aware Rust scheduler and partition execution infrastructure.

### Phase 5 — Conversion Persistence

Write converted results to separate Parquet datasets.

Preserve:

- Original event references.
- Converted prices.
- Target quote currency.
- Conversion path.
- Reference observation identities.
- Reference timestamps and ages.
- Conversion status.
- Conversion configuration.

### Phase 6 — Checkpoint Caching

Persist reusable conversion checkpoints and validate their dependencies before reuse.

### Phase 7 — Liquidity-Aware Conversion

Extend conversion beyond last-trade prices to:

- Best bid/ask.
- Midprice.
- Spread-aware conversion.
- Liquidity-constrained executable prices.
- Synthetic order-book conversion.

## Development Priority

Currency conversion will be implemented **after completing trade-only and combined trade/depth merging**.

The immediate MarketForge development priorities remain:

1. Unified timeline persistence.
2. Parallel trade-only merging.
3. Parallel combined merging.
4. Manifest and event-coverage validation.
5. Merge benchmarks.
6. Currency conversion engine.

The existing merger must preserve stable stream identities, canonical event references, and deterministic ordering so the conversion engine can be implemented without redesigning historical merging.