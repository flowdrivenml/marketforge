# Pipeline recipes

Save the version-1 TOML under `data/.config/workflows/data_engineering/pipeline/`. The name resolves in that library. The recipe compiles inline acquisition requests into acquisition and normalization inputs. Merge receives the registered file IDs from normalization. Stage resource overrides live in the separate [pipeline runtime](../reference/runtime.md#pipeline-runtime). [Pipeline guide](../workflows/pipeline.md) · [CLI](../reference/cli/pipeline.md)

| Path | Type and constraints | Default and meaning |
| --- | --- | --- |
| `version` | Required integer 1 | Pipeline recipe schema |
| `interval.start`, `interval.end` | Required timezone-aware TOML datetime values, ordered and representable as signed 64-bit Unix nanoseconds | One half-open UTC interval shared across stages |
| `requests` | Required nonempty list of unique [acquisition requests](acquisition.md). Exchange adapter validates the scope. Requests must also form valid normalization requests | Source scope and feed selection |
| `normalize.ranking_policy` | Nonempty token matching `^[a-zA-Z0-9_-]+$` | `turnover` |
| `merge.timeline_ordering` | Optional `causal` or `chronological` | Omission lets Forge select based on inputs |

The recipe contains no raw, file dataset, or publication IDs. Forge binds the acquired revisions and normalized primary-file registrations to the stage outputs. See [Forge evidence](../reference/prepared-forge.md) for artifacts used by resume. [Pipeline model](https://github.com/flowdrivenml/marketforge/blob/main/forge/historical/pipelines/config.py)

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
