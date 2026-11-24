# Pipeline recipes

Save the version-1 TOML under `data/.config/workflows/data_engineering/pipeline/`. The name resolves in that library. The recipe compiles inline acquisition requests into acquisition and normalization inputs. Merge receives the registered file IDs from normalization. Stage resource overrides live in the separate [pipeline runtime](../runtime.md#pipeline-runtime). [Pipeline guide](../../workflows/pipeline.md) · [CLI](../cli/pipeline.md)

The recipe derives source raw, file dataset, and merge publication IDs at runtime. An optional external conversion reference uses a registry publication ID. Forge binds the acquired revisions and normalized primary-file registrations to the stage outputs. See [Forge evidence](../data-formats/prepared-forge.md) for artifacts used by resume. [Pipeline model](https://github.com/flowdrivenml/marketforge/blob/main/forge/historical/pipelines/config.py)

## Minimal recipe

This recipe selects one Bybit trade archive scope. Save it in the pipeline
library and run `mf pipeline validate NAME` before inspecting the plan. The
interval selects archives, and local readiness is checked separately.

```toml
version = 1

[interval]
start = 2026-10-27T00:00:00Z
end = 2026-10-28T00:00:00Z

[[requests]]
exchange = "bybit"
instrument_type = "perpetual"
market_category = "linear"
data_type = "trades"
scope = { kind = "instrument", identifier = "BTCUSDT" }
```

Validation checks the recipe and request structure. Planning also checks the
selected database and effective runtime. Run with current managed engines and
the appropriate catalogue, specification evidence, and profiles.

## Optional conversion

Add the table below to the minimal BTCUSDT recipe for an identity valuation.
Omitting `[convert]` leaves the pipeline at Merge.

```toml
[convert]
target_quote = "USDT"
max_staleness_ns = 60000000000
```

The table accepts the [conversion policy fields](conversion.md#common-fields)
directly, including `price_source`, `max_hops`, `path_policy`, and
`instrument_kinds`. It also accepts `reference_stream_ids`,
`reference_publication_id`, and a nested `[convert.resources]` table with the
same types and defaults as a Conversion recipe. Forge binds the exact registered
merge from this pipeline and applies whole-publication scope.

The optional stage uses the frozen base [Conversion runtime](conversion-runtime.md).
The separate pipeline runtime overrides apply to Normalization and Merge.

## Complete field reference

Tables for Normalize and Merge configure mandatory stages. Omit the
Conversion table for three-stage processing. TOML keys are strict.

| Field | Type | Required | Default | Meaning |
| --- | --- | --- | --- | --- |
| `version` | Integer, exactly `1` | Yes | — | Recipe schema |
| `interval.start` | Timezone-aware TOML datetime | Yes | — | Inclusive beginning |
| `interval.end` | Timezone-aware TOML datetime | Yes | — | Exclusive end, later than start |
| `requests` | Nonempty array of unique request tables | Yes | — | Requests must pass both the acquisition adapter and normalization recipe contracts |
| `requests[].exchange` | Lowercase exchange code | Yes | — | Configured historical acquisition adapter |
| `requests[].instrument_type` | `spot`, `perpetual`, `future`, `option` | Yes | — | Instrument type supported by the adapter |
| `requests[].market_category` | `spot`, `linear`, `inverse`, `option` | Yes | — | Market category supported by the adapter |
| `requests[].data_type` | `trades` or `order_book_l2` | Yes | — | Requested event feed |
| `requests[].scope.kind` | `instrument`, `base_coin`, `family` | Yes | — | Native scope supported for the selected source |
| `requests[].scope.identifier` | Nonblank string | Yes | — | Source-native scope identifier |
| `requests[].source_options.feed` | `incremental` or `snapshot` | No | `incremental` | Gate.io depth only |
| `normalize.ranking_policy` | Token made of letters, digits, underscores or hyphens | No | `turnover` | Persisted ranking used in normalization preparation |
| `merge.timeline_ordering` | `causal` or `chronological` | No | Resolved from inputs | Engine timeline policy |
| `convert.target_quote` | Uppercase ASCII letters or digits | With Conversion | — | Target quote currency |
| `convert.price_source` | `last_trade` | No | `last_trade` | Eligible reference price |
| `convert.max_staleness_ns` | Positive unsigned 64-bit integer | With Conversion | — | Maximum reference observation age in nanoseconds |
| `convert.max_hops` | Integer from 1 through 4 | No | `3` | Maximum path length |
| `convert.path_policy` | `all` or `primary` | No | `all` | Conversion path selection policy |
| `convert.instrument_kinds` | Nonempty unique list of `spot`, `perpetual`, `future`, `option`, `equity` | No | All applicable kinds | Limits which instruments receive valuations |
| `convert.reference_stream_ids` | List of unique nonblank strings | With external publication | Empty | Exact observed trade-stream identities |
| `convert.reference_publication_id` | Positive signed 64-bit registry publication ID | No | — | External merged reference publication in the same registry |
| `convert.resources.workers` | Positive unsigned 64-bit integer | No | `1` | Worker request |
| `convert.resources.memory_limit_mb` | Positive unsigned 64-bit integer | No | `1024` | Memory request in MiB |
| `convert.resources.max_open_files` | Positive unsigned 64-bit integer | No | `64` | Open-file request |

Both interval bounds must fit signed 64-bit Unix nanoseconds. Resource
defaults are explicit job requests. The converter runtime separately supplies
admission limits and execution settings. When `convert.resources` is omitted,
Forge submits the defaults in the table and validates them against the retained
runtime. [Runtime composition](../runtime.md#pipeline-runtime)

## Four-stage Bybit spot recipe

This example uses the canonical stream identities for the two Bybit spot
trade requests. A stage report or `mf datasets streams --publication-id PUB_ID
--testing` lets you check the exact IDs after registration. Structural
validation checks the declarations. Preparation checks whether the selected
streams are present and eligible in the committed merge.

```toml
version = 1

[interval]
start = 2026-10-27T00:00:00Z
end = 2026-10-28T00:00:00Z

[normalize]
ranking_policy = "turnover"

[merge]
timeline_ordering = "causal"

[convert]
target_quote = "USDT"
price_source = "last_trade"
max_staleness_ns = 60000000000
max_hops = 3
path_policy = "all"
reference_stream_ids = [
  "bybit.spot.spot.BTCUSDT.trades",
  "bybit.spot.spot.BTCUSDC.trades",
]

[[requests]]
exchange = "bybit"
instrument_type = "spot"
market_category = "spot"
data_type = "trades"
scope = { kind = "instrument", identifier = "BTCUSDT" }

[[requests]]
exchange = "bybit"
instrument_type = "spot"
market_category = "spot"
data_type = "order_book_l2"
scope = { kind = "instrument", identifier = "BTCUSDT" }

[[requests]]
exchange = "bybit"
instrument_type = "spot"
market_category = "spot"
data_type = "trades"
scope = { kind = "instrument", identifier = "BTCUSDC" }
```

BTCUSDT already has the requested quote, so its applicable valuations use
the identity route. An eligible BTCUSDC trade can supply the inverse
USDC → BTC edge. An eligible BTCUSDT trade can supply the direct BTC → USDT
edge. The Engine checks causal ordering, price validity, reference freshness,
and path length at each event. Some valuations can be unavailable even when
both streams are present. See [conversion concepts](../../concepts/conversion.md).
