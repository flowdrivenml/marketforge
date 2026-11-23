# Merge recipes

| Artifact | Purpose |
| --- | --- |
| Merge recipe TOML | Select normalized inputs, an interval, and timeline choices. |
| [Merge runtime TOML](merge-runtime.md) | Control Engine resources, partition planning and Parquet output. |
| [Prepared merge JSON](../reference/prepared-merge.md) | Exact Engine submission, with file paths, checksums and ranks. |
| Execution intent and report | Retain the submission, outcomes and evidence for [registration recovery](../reference/cli/merge.md#registration-and-recovery). |

The managed recipe library is `data/.config/workflows/data_engineering/merge/`. Use a managed name with optional `.toml` suffix, or a directory-qualified or absolute path where the [CLI](../reference/cli/merge.md) accepts it. Relative explicit paths resolve from the command's working directory.

## Explicit registered file IDs

Use this form when you know the normalized primary files, including outputs from a legacy archive-ID normalization request. Get real file IDs from normalization results or `mf datasets list --operation normalize --testing | cpout`. Replace the illustrative `1` and `2` with IDs from your selected registry, in precedence order.

```toml
dataset_ids = [1, 2]
start_ns = 1793059200000000000
end_ns = 1793145600000000000
```

| Field | Type and requirement | Meaning |
| --- | --- | --- |
| `dataset_ids` | Required nonempty array of unique positive signed 64-bit integers | Registry primary-file IDs. Each must identify a complete eligible normalized Parquet file. |
| `start_ns`, `end_ns` | Required signed 64-bit integers with `start_ns < end_ns` | Half-open UTC interval in Unix nanoseconds. Use these fields for sub-microsecond endpoints. |

An explicit-file recipe has no `version` field. Use the shared ordering fields below with either form. A top-level `version`, `interval`, or `selection` selects the derived schema. Unknown keys are rejected in each form.

## Selection derived from normalization

Use this form to discover registered normalized files from a saved request-based normalization recipe. This example refers to the Bybit trades recipe from the [normalization guide](../workflows/normalize.md).

```toml
version = 1

[interval]
start = 2026-10-27T00:00:00Z
end = 2026-10-28T00:00:00Z

[selection]
source = "my_dataset"
```

| Field | Type and requirement | Meaning |
| --- | --- | --- |
| `version` | Required integer `1` | Derived recipe version. |
| `interval.start`, `interval.end` | Required timezone-aware datetimes, ordered and representable as signed 64-bit Unix ns | End is exclusive. Datetime resolution is microseconds. |
| `selection.source` | Required nonblank string | Managed normalization recipe name or explicit TOML path. |
| `selection.operation`, `selection.mode` | Strings, default and only values `normalize`, `all` | Consider files matching the normalization requests and filters. |
| `selection.data_types` | Optional nonempty string array of `trades`, `order_book_l2` | Select content requested by the source. Engine jobs call the latter `depth`. |
| `selection.exchanges` | Optional nonempty string array matching `^[a-z][a-z0-9_]*$` | Restrict execution venues, for example `["bybit"]`. |
| `selection.instruments` | Optional nonempty string array | Restrict to canonical `exchange.instrument_type.market_category.symbol` identities, for example `["bybit.spot.spot.BTCUSDT"]`. |
| `selection.instrument_types` | Optional nonempty string array of `spot`, `perpetual`, `future`, `option`, `equity` | Restrict instrument types, for example `["spot"]`. |
| `selection.market_categories` | Optional nonempty string array of `spot`, `linear`, `inverse`, `option`, `equity` | Restrict market categories, for example `["linear"]`. |
| `selection.publication_ids` | Optional nonempty array of unique positive signed 64-bit integers | Approve matching registry **publication** IDs. Read IDs from discovery or `mf --json datasets show FILE_ID --testing` under `publication.publication_id`. |

Omitted filters impose no additional restriction. Filters intersect on matching stream membership. Forge uses resolved source tasks for family and base-coin membership, including registered source keys and matching Gate.io L2 feed. A file with an overlapping unapproved stream can block the selection.

A bare source name resolves in the normalization recipe library. Use `./filename.toml`, a path with a directory, or an absolute path for explicit references. Relative paths resolve from the command's working directory, even when the merge TOML lives elsewhere. `new --from-normalize` saves an absolute path for an explicit source.

## Selection and ambiguity

Forge reads the current source recipe at discovery and records its SHA-256. The recipe keeps a live reference and its own independent interval. Automatic selection uses retained normalization evidence such as the source path, request definitions, interval, ranking policy and submitted-batch fingerprint. Forge can follow retained reuse cohorts and collapse verified canonical aliases for automatic selection. It requires one maximal matching cohort after that resolution. Multiple verified submissions or a candidate without a verified ownership link may require explicit publication approval. Approval still requires matching files and membership, and missing requests can block preparation.

Matched file counts describe candidate files. Selected file counts describe actual input files approved after the checks. One registry publication can contribute several files and logical streams. Eligible candidates require complete normalized file records and compatible physical and provenance evidence. Superseded files are excluded from discovery. A candidate without a complete registry state cannot become a usable input. Missing files, unverifiable pins, missing requested selections and incompatible membership can block preparation.

Here is a complete publication-pinned recipe. Replace the illustrative ID with a compatible selection from your registry. A recipe requesting both trades and depth may need separate publication IDs for each stream.

```toml
version = 1

[interval]
start = 2026-10-27T00:00:00Z
end = 2026-10-28T00:00:00Z

[selection]
source = "my_dataset"
publication_ids = [101]
data_types = ["trades"]
```

Explicit approval narrows discovery to those registry publications. It still checks content filters, interval and stream membership, file pins, and whether the set covers the requested selections. Equal file hashes across builds are evidence about bytes. They do not establish that the two submissions have interchangeable processing or integrity contracts. See [how to inspect and pin a generation](../workflows/merge.md#when-multiple-verified-submissions-match).

## Input precedence and ranks

| Field | Type and default | Meaning |
| --- | --- | --- |
| `stream_ranks` | Table mapping exact stream IDs to unsigned 32-bit integers. Defaults to empty. | Override selected stream ranks. Quote dotted IDs. Every override must refer to a selected stream, and distinct streams need distinct ranks. |
| `timeline_ordering` | Optional string, `chronological` or `causal` | If omitted, Forge chooses causal when a selected depth publication requires it and chronological otherwise. |

Explicit selection orders streams by the first selected file for each stream. Derived selection uses the first matching normalization request, then stream ID. Unspecified ranks use the lowest unused values in that order, starting at zero. Parts of the same stream share one rank. Use exact IDs from the preparation report. A lower rank wins cross-stream ties at equal timestamps.

A complete explicit-input example with a deliberate ordering override follows. The illustrative file IDs and stream keys must match the intended selected registry files before preparation.

```toml
dataset_ids = [1, 2]
start_ns = 1793059200000000000
end_ns = 1793145600000000000
timeline_ordering = "causal"

[stream_ranks]
"bybit.spot.spot.BTCUSDT.trades" = 8
"bybit.spot.spot.ETHUSDT.trades" = 2
```

This reverses the rank precedence of the two example streams at equal timestamps. Unused rank values are allowed.

## Timeline ordering

**Chronological** writes nondecreasing event timestamps, with stream rank for ties. **Causal** chooses the available stream head by timestamp and rank, then advances only that stream. Native dependencies can cause timestamp regressions in the output. Keep physical timeline sequence for replay.

Trades, depth, and combined jobs use compatible registered normalized inputs from one instrument namespace. Timestamp-ordered trade streams need supported contiguous source spans. A segmented depth stream requires all its primary parts and causal ordering. A depth stream spanning multiple publications requires a continuity contract the current merger does not support. See [prepared-job compatibility](../reference/prepared-merge.md#merge-compatibility).

## Interval, reconstruction, and coverage

`[start, end)` selects market observations. For causal depth replay the Engine can emit the required committed segment prefix before the start, marking those events `reconstruction_only`. Those rows are replay dependencies and count separately from market observations. Missing parts or invalid initialization can block the merge.

Preparation reports observed timestamp envelopes, gaps between the envelopes within the request, and files extending beyond it. A file's observed maximum is inclusive. These bounds do not establish event-level continuity. See [merged publication counts](../reference/publication-merge.md#identities-and-counts).
