# Conversion recipes

The editable Forge recipe selects whole committed merge publications and historical reference trades. Use `mf convert new` to generate a source binding. [Workflow](../../workflows/conversion.md) · [Standalone Engine job](../data-formats/prepared-conversion.md)

A managed recipe takes its name from its filename. The name starts with an ASCII letter or digit and can also contain hyphens and underscores. The CLI accepts an optional `.toml` suffix. The recipe file has no separate name field. Omit optional fields to use the defaults below.

## Common fields

| Field | Type | Required | Default | Meaning and constraints |
| --- | --- | --- | --- | --- |
| `version` | Integer | Yes | — | Recipe schema, 1 or 2. New recipes use 2. |
| `source` | Table | Yes | — | Choose one of the two bindings below. |
| `interval.start_ns`, `interval.end_ns` | Signed 64-bit integers | Yes | — | Half-open Unix nanosecond bounds, with start before end. |
| `selection.whole_publication` | Boolean | For direct binding, must be true | `false` | Approves the complete intersecting publication, including rows outside the request. Disjoint intervals are rejected. |
| `reference_stream_ids` | Array of strings | No | Empty | Manual selection. Values must be unique and nonblank. An explicit empty list selects no references. |
| `reference_publication_id` | Positive signed 64-bit integer | No | Omitted | Registry publication ID for a distinct trade-only merge. In manual mode, requires an explicit, nonempty stream selection. |
| `conversion.target_quote` | String | Yes | — | Target currency using uppercase ASCII letters and digits. |
| `conversion.price_source` | Enum | No | `last_trade` | The only accepted price source is `last_trade`. |
| `conversion.max_staleness_ns` | Positive uint64 | Yes | — | Maximum causal reference age in nanoseconds. The CLI template supplies 60 seconds. |
| `conversion.max_hops` | Integer | No | 3 | Maximum route length, 1–4 legs. |
| `conversion.path_policy` | Enum | No | `all` | `primary` tries the first planned route. `all` tries planned routes until one succeeds. |
| `conversion.instrument_kinds` | Array of enums | No | All kinds | A nonempty, unique subset of `spot`, `perpetual`, `future`, `option`, `equity`. |
| `resources.workers` | Positive uint64 | No | 1 | Requested worker count. |
| `resources.memory_limit_mb` | Positive uint64 | No | 1024 | Requested memory in binary MiB. |
| `resources.max_open_files` | Positive uint64 | No | 64 | Requested file-handle budget. |

The [base runtime](conversion-runtime.md) caps each resource request. The [conversion concepts](../../concepts/conversion.md) describe route and layout constraints.

## Reference selection

Version 2 selects automatically when `reference_stream_ids` and
`reference_selection` are omitted. In either version, a supplied
`reference_stream_ids` array selects manual mode, including an empty array.
Version 1 recipes that omit both fields keep their original empty manual
selection. Set an explicit mode to change a version 1 recipe without migrating
the rest of the document.

In a standalone recipe, put `reference_stream_ids` and
`reference_publication_id` before the first TOML table. Use
`[reference_selection]` for explicit settings. Pipeline uses the same fields
inside `[convert]` and `[convert.reference_selection]`.

| Field | Values | Default |
| --- | --- | --- |
| `reference_selection.mode` | `automatic`, `manual` | `automatic` when the table is present |
| `reference_selection.ranking_policy` | Published turnover policy name | `turnover` |
| `reference_selection.ranking_snapshot` | Exact published ranking UUID in the selected registry | Latest eligible snapshot by publication time at the decision cutoff |
| `reference_selection.temporal_policy` | `historical`, `ex_post` | `historical` |

Ranking settings apply in automatic mode. Remove the manual stream array when
setting `mode = "automatic"`. An analytical override requires
`temporal_policy = "ex_post"` and an exact `ranking_snapshot`. Its ranking
observations may postdate the historical period, and the preparation report
records that choice. See [historical ranking applicability](../../workflows/markets/rankings.md#historical-conversion).

## Upstream recipe binding

| Field | Type | Required | Default | Meaning and constraints |
| --- | --- | --- | --- | --- |
| `source.kind` | Enum | Yes | — | `merge`, `normalize`, or `acquisition`. |
| `source.reference` | String | Yes | — | Nonblank managed upstream name or explicit TOML path. |
| `source.path` | String | Yes | — | Nonblank resolved upstream path. Verified against the current resolution on load. |
| `source.sha256` | String | Yes | — | Exact upstream TOML SHA-256, 64 lowercase hex digits. |
| `selection.file_dataset_ids` | Array of positive signed 64-bit integers | No | Omitted | Unique, nonempty primary-file IDs. Selects complete owning publications. |
| `selection.publication_ids` | Array of positive signed 64-bit integers | No | Omitted | Unique, nonempty registry publication IDs. Mutually exclusive with file IDs. |
| `selection.expected_engine_publication_ids` | Array of strings | No | Omitted | The exact resolved set. Values are nonempty, unique canonical positive uint64 decimal strings. |

Explicit selectors narrow the candidate set while preserving lineage requirements. Each eligible noncompeting merge becomes one job. Overlapping candidates for the same stream scope require revision selection. A normalization binding uses retained request-based submissions. Legacy archive-ID normalization recipes cannot establish conversion lineage. Acquisition binding verifies registered raw contributions and their scope, interval and digest before following merge ancestry.

## Direct publication binding

Use `mf convert new NAME --from-publication MERGED_PUBLICATION_ID --target-quote USDT --testing` to generate this declaration. The selected merge is available independently of the editable TOML that produced it. Each field below is required and has no user-facing default.

| Field | Type | Meaning and constraints |
| --- | --- | --- |
| `source.kind` | Enum | Explicit discriminator `publication`. |
| `source.registry_publication_id` | Positive signed 64-bit integer | Exact registered merge. |
| `source.registry_id` | String | Lowercase hyphenated registry UUID. |
| `source.environment` | Enum | `testing` or `production`. |
| `source.engine_publication_id` | String | Canonical positive uint64 decimal ID. |
| `source.manifest_sha256` | String | Committed merge manifest fingerprint, 64 lowercase hex digits. |
| `source.processing_job_sha256` | String | Committed merge job fingerprint, 64 lowercase hex digits. |
| `source.publication_checksums_sha256` | String | Committed checksum index fingerprint, 64 lowercase hex digits. |
| `source.interval.start_ns`, `source.interval.end_ns` | Signed 64-bit integers | Committed half-open Unix nanosecond bounds. |

A direct recipe requires `selection.whole_publication = true` and its top-level `interval` must equal the committed range. Omit `selection.file_dataset_ids`, `selection.publication_ids`, and `selection.expected_engine_publication_ids` for this source. Reference selection remains independent. Automatic mode discovers reference trades during preparation. Manual mode uses the exact configured list. A target quote equal to the historical quote can use the existing identity route.

List, show, and local recipe validation can inspect this declaration offline. Prepare in the bound environment to recheck eligibility and metadata fingerprints, then verify the full publication, normalized parents, namespace, stream membership, and historical currencies. Prepared submissions retain the publication declaration alongside the existing job and input evidence. Preparation keeps the whole publication, including any state reconstruction; the interval does not crop rows.

Absence of an optional field means omission in TOML. The Engine gets the resolved bindings, resources and writer configuration in a generated job. The Forge collection and execution intent are [separate artifacts](../data-formats/prepared-forge.md). [Recipe model](https://github.com/flowdrivenml/marketforge/blob/main/forge/historical/conversion/recipes.py)


## Complete examples

These declarations use synthetic pins to illustrate valid structures. Generate the source binding with `mf convert new` in your workspace. It writes the real source paths, fingerprints and IDs. Local validation of a direct declaration checks its structure. Registry-backed preparation must resolve the actual publication, streams and committed evidence.

### Minimal recipe from a merge recipe

This example pairs with the following upstream TOML fixture named `upstream.toml`. Its fingerprint assumes the exact bytes shown, including a final newline.

```toml
dataset_ids = [1]
start_ns = 1727683200000000000
end_ns = 1727769600000000000
```

The absolute paths show where to substitute your generated binding. Without explicit selection, preparation looks for an eligible committed merge related to the upstream recipe.

```toml
version = 2

[source]
kind = "merge"
reference = "/absolute/path/to/upstream.toml"
path = "/absolute/path/to/upstream.toml"
sha256 = "cab83c25e7e1f19e6d7852c93175111a3cfe8d429d17562298a9aa0548bad762"

[interval]
start_ns = 1727683200000000000
end_ns = 1727769600000000000

[conversion]
target_quote = "USDT"
max_staleness_ns = 60000000000
```

### Direct publication and identity

With a combined BTCUSDT merge, this USDT target uses the identity rate and needs no reference trades.

```toml
version = 2

[source]
kind = "publication"
registry_publication_id = 1
registry_id = "11111111-1111-4111-8111-111111111111"
environment = "testing"
engine_publication_id = "100"
manifest_sha256 = "1111111111111111111111111111111111111111111111111111111111111111"
processing_job_sha256 = "2222222222222222222222222222222222222222222222222222222222222222"
publication_checksums_sha256 = "3333333333333333333333333333333333333333333333333333333333333333"

[source.interval]
start_ns = 1727683200000000000
end_ns = 1727769600000000000

[interval]
start_ns = 1727683200000000000
end_ns = 1727769600000000000

[selection]
whole_publication = true

[conversion]
target_quote = "USDT"
max_staleness_ns = 60000000000
```

### Direct currency conversion

For BTC-quoted source data in a combined merge that also includes BTCUSDT trades, the selected trade supplies the BTC → USDT edge. Stream IDs must match the actual merge inventory.

```toml
version = 1
reference_stream_ids = ["bybit.spot.spot.BTCUSDT.trades"]

[source]
kind = "publication"
registry_publication_id = 1
registry_id = "11111111-1111-4111-8111-111111111111"
environment = "testing"
engine_publication_id = "100"
manifest_sha256 = "1111111111111111111111111111111111111111111111111111111111111111"
processing_job_sha256 = "2222222222222222222222222222222222222222222222222222222222222222"
publication_checksums_sha256 = "3333333333333333333333333333333333333333333333333333333333333333"

[source.interval]
start_ns = 1727683200000000000
end_ns = 1727769600000000000

[interval]
start_ns = 1727683200000000000
end_ns = 1727769600000000000

[selection]
whole_publication = true

[conversion]
target_quote = "USDT"
max_staleness_ns = 60000000000
```

### Multi-hop currency conversion

With USDC-quoted source data and both references in the combined merge, the planner can use USDC → BTC → USDT. For an external trade-only merge, set the top-level `reference_publication_id` and use the exact stream IDs available to the job. See the [layout and eligibility rules](../../concepts/conversion.md#how-references-become-eligible).

```toml
version = 1
reference_stream_ids = ["bybit.spot.spot.BTCUSDC.trades", "bybit.spot.spot.BTCUSDT.trades"]

[source]
kind = "publication"
registry_publication_id = 1
registry_id = "11111111-1111-4111-8111-111111111111"
environment = "testing"
engine_publication_id = "100"
manifest_sha256 = "1111111111111111111111111111111111111111111111111111111111111111"
processing_job_sha256 = "2222222222222222222222222222222222222222222222222222222222222222"
publication_checksums_sha256 = "3333333333333333333333333333333333333333333333333333333333333333"

[source.interval]
start_ns = 1727683200000000000
end_ns = 1727769600000000000

[interval]
start_ns = 1727683200000000000
end_ns = 1727769600000000000

[selection]
whole_publication = true

[conversion]
target_quote = "USDT"
max_staleness_ns = 60000000000
max_hops = 2
path_policy = "all"
```
