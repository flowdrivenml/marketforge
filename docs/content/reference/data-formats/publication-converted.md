# Converted publications

Conversion writes staging beside the destination, uses `execution.json` for counts and `conversion-runtime.json` for effective settings and job evidence, and publishes a validated manifest and primary Parquet inventory. The Engine supports atomic no-replace publication on Linux. Staged publish requires the matching job and worker override. Forge verifies output and currently has no conversion registry registration step. [Conversion guide](../../workflows/conversion.md) · [Prepared job](prepared-conversion.md)

## Manifest

| Field | Type and meaning |
| --- | --- |
| `protocol_version`, `conversion_schema_version` | uint32, both 1 |
| `job_id` | uint64 conversion job identity |
| `input` | `dataset_path`, uint64 `merge_job_id`, uint64 `events` |
| `conversion` | Effective quote-conversion policy from the job |
| `statistics` | uint64 `events_written`, `identity`, `converted`, `unavailable`, `reference_updates`, and `skipped` (omitted for zero). Optional `depth` statistics |
| `output` | `content_type`, `files_written`, `row_groups`, nullable int64 `first_timestamp_ns` and `last_timestamp_ns`, and `files`. Conditional `timeline_ordering` and nonzero `reconstruction_events` |
| `output.files[]` | Relative `path`, uint64 `size_bytes`, `rows`, `row_groups` |
| `integrity` | `status`, booleans `conversion_validation` and `source_preservation`, uint64 `references_checked` and `source_events_compared` |

The four event status counts sum to `events_written`. The published integrity status is `pass`. Depth statistics count `levels_read`, `levels_converted`, `levels_unavailable`, and optional `levels_skipped` (omitted at zero). The final three counts sum to levels read. Numerical checks and source-preservation checks make different assertions. A successful publication can contain unavailable conversions because reference observations were missing or unsuitable. [Manifest and accounting](https://github.com/flowdrivenml/marketforge/blob/main/engine/src/historical/convert/output/publication/manifest.rs)

## Appended Arrow fields

All original [physical merge fields](publication-merge.md) retain their types and order. Target values and rates use decimal256(76,28).

| Source layout | First appended fields |
| --- | --- |
| Trades | Nullable decimals `price_target`, `quantity_quote_target` |
| Depth | Required list `changes_target` of non-null structs containing the two nullable target decimals |
| Combined | Nullable `trade_target` struct and nullable `depth_target` list with the same two nullable decimals per trade or level |

The final fields, in order, are required UTF-8 `target_quote`, required uint8 `conversion_status`, nullable decimal `conversion_rate`, nullable list of non-null UTF-8 strings `conversion_path`, required `conversion_references` list of non-null structs, and nullable uint8 `conversion_failure`. Each reference struct has required fields `dataset_id` uint64, `stream_id` UTF-8, `event_timestamp_ns` int64, `stream_rank` uint32, `task_id` and `event_ordinal` uint64, `direction` uint8, `reference_price` Decimal256(76,28).

| Code field | Values |
| --- | --- |
| Status | 0 identity, 1 converted, 2 unavailable, 3 skipped |
| Failure | 1 missing, 2 stale, 3 future, 4 identity mismatch, 5 arithmetic, 6 no path |
| Reference direction | 0 direct, 1 inverse |

Python discovery recognizes converted manifests. Use the registry-backed Python opener to resolve a durable output reference or open the publication directory directly. [Converted schema](https://github.com/flowdrivenml/marketforge/blob/main/engine/src/historical/convert/output/schema/schema.rs) · [Python dataset API](../../research/python-api.md)

## Execution receipt

`execution.json` records version 1, uint64 `job_id`, and uint64 counters `events_read`, `events_written`, `identity_events`, `converted_events`, `unavailable_events`, `reference_updates`, `files_written`, and `row_groups_written`. `skipped_events` defaults to zero and is omitted at zero. Optional `depth` uses the same level accounting as the manifest. The combined fixture writes primary files under `partition-NNNNNN/part-NNNNNN.parquet`. The manifest inventory determines the complete set. [Receipt definition](https://github.com/flowdrivenml/marketforge/blob/main/engine/src/historical/convert/output/publication/receipt.rs)

## Read the counts

`events_read` and manifest `input.events` count source observations from the merged input. `events_written` counts output observations, including retained reconstruction context. A combined event is a trade or a depth observation. A depth observation can carry zero or more level changes. Count those with the separate depth level statistics. Combined and depth level counts describe elements of the change lists, including changes on retained reconstruction rows where present, while physical row counts come from `output.files[].rows`. The physical event count includes the manifest’s `reconstruction_events` subset. Subtract that subset when counting market observations alone. Trade and depth event counts can be derived from the output `event_kind` field (0 for trades, 1 for depth) in a combined publication. The conversion receipt reports the total event population.

Identity and converted count successful quote valuations. Unavailable counts events that were kept with an unsuccessful valuation. Skipped counts retained combined events excluded by the instrument-kind policy. `reference_updates` counts price-state updates. An external reference observation can update this state without producing another market event. An internal reference trade that belongs to the source appears once as a source event.

The converted output reference is the durable work key used by `mf datasets show --output-ref OUTPUT_REF --testing`. The manifest `job_id` identifies the physical conversion. `input.merge_job_id` identifies the upstream Engine merge publication. See [inspection identifiers](../../concepts/identity.md#inspection-identifiers).
