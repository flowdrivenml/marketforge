# Merged publications

The physical merger commits `manifest.json`, `merge-job.json`, `merge-input-checksums.json`, `merge-partitions.json`, `merge-runtime.json`, `publication-checksums.json` and primary parts under `timeline/partition-*/part-*.parquet`. The manifest inventory determines membership. Retain referenced parts and sidecars together. Verify committed evidence before registering. [Merge workflow](../../workflows/merge.md) · [Merge job](prepared-merge.md)

## Identities and counts

The Engine publication ID is derived from the merge job ID. Forge assigns a separate registry publication ID and a registry file ID per registered primary. Use file IDs with [dataset verification](../../workflows/inspection/datasets.md#verifying-datasets), the logical Forge run ID with [run inspection](../cli/runs.md), and the retained intent or publication path for [registration recovery](../cli/merge.md#registration-and-recovery).

Two physical quantities are distinct. A partition is an output range, and it can contain multiple Parquet files. A logical stream is membership carried by rows and can span partitions. The manifest's `events_written` equals the sum of primary physical rows and includes reconstruction-only events. Subtract `reconstruction_events` to obtain the count of in-range market observations. The CLI can currently label the gross count **Market events**. `canonical_rows` counts the represented normalized payload rows, which can differ from the physical timeline row count. Reconstruction-only events are a subset of depth events and remain necessary for causal replay.

`merge-runtime.json` records the effective [merge runtime](../configuration/merge-runtime.md). Forge checks the output, file inventories and lineage before registering the eligible primaries.

## Manifest and inventory

| Field | Representation and meaning |
| --- | --- |
| `protocol_version` | uint32 1 for the physical manifest |
| `physical_schema_version`, `timeline_ordering` | 1 and chronological, or 2 and causal. Missing ordering defaults to chronological |
| `job_id`, `mode` | uint64 Engine job identity and `trades`, `depth`, or `combined` |
| `input_datasets` | Entries with uint64 `dataset_id`, `dataset_path`, and `manifest_version` |
| `time_range` | Signed Unix ns `start_ns`, `end_ns`, half-open |
| `streams` | `stream_rank` uint32, normalized `dataset_id` uint64, `kind` (`trade` or `depth`), `instrument_id` int64, UTF-8 `exchange`, `symbol`, `stream_id`, optional `base_currency`, `quote_currency` |
| `events_written`, `trade_events`, `depth_events`, `canonical_rows`, `reconstruction_events` | uint64. Physical event count is trade plus depth. Source canonical rows can differ from physical events. Reconstruction events default to zero when absent |
| `files_written`, `row_groups` | uint64 physical inventory totals |
| `start_timestamp_ns`, `end_timestamp_ns` | Nullable int64 observed extrema |
| `files` | Primary entries contain relative `path`, uint64 `size_bytes`, nullable uint32 `partition_id`, nullable uint64 `rows`, nullable int64 `start_timestamp_ns`, `end_timestamp_ns`. The physical writer supplies partition and row counts |
| `boundary_links`, `skipped_initializations`, `checkpoints`, `initial_reconstruction` | Depth reconstruction evidence. Read in conjunction with physical ordering, file inventory, and source provenance |

## Verification and handoff

The manifest describes the committed layout. `merge-job.json` preserves the submitted job, `merge-input-checksums.json` pins source evidence, `merge-partitions.json` records partition evidence and `publication-checksums.json` binds the published inventory. Forge verifies committed files and their registered input lineage before registration. Keep the entire directory for verification or downstream processing. The Forge execution and registration reports separately record outcomes and registry IDs. Verification checks the publication contract and recorded provenance. Observed extrema and row inventories cannot establish continuous exchange coverage.

Chronological consumers use ordered partition ranges. For causal output, replay by `timeline_sequence`, including reconstruction-only rows needed to establish book state. Physical file rotation can occur inside a partition. A new file alone does not signal a fresh book or stream.

For [conversion](../../workflows/conversion.md), pass a supported committed chronological or causal merge publication with its complete inventory. A primary-file selection can identify an owning publication, which conversion then considers at publication scope.

## Ordered Arrow fields

The first six [canonical identity fields](publication-normalized.md) are `event_timestamp_ns`, nullable `system_timestamp_ns`, `exchange`, `instrument_id`, `symbol`, and `stream_id`. All other fields below are required unless marked nullable.

| Layout | Ordered groups |
| --- | --- |
| Trades | Full canonical trade row, then provenance fields |
| Depth | Six identity fields, provenance fields, then depth metadata and changes |
| Combined | Six identity fields, `event_kind` (uint8), provenance fields, then nullable `trade` and `depth` structs. Exactly one is populated. Code 0 selects trades and 1 selects depth. The trade struct contains canonical trade fields after the identity prefix. The depth struct contains depth metadata and changes |

The provenance fields are `stream_rank` uint32, then `dataset_id`, `task_id`, `event_ordinal`, `canonical_row_offset`, `canonical_row_count` as uint64 in that order. They connect a physical event to normalized source history. The last two fields identify a half-open canonical row span.

Depth metadata is `event_boundary` uint8, `source_operation` uint8, nullable uint64 `sequence_start` and `sequence_end`, and `changes` as a non-null list of non-null structs. Each element uses the canonical depth fields after the identity prefix. Boundary codes are 0 changes and 1 initialization. Source-operation codes are 0 snapshot, 1 absolute update, 2 relative update, and 3 bounded observation.

Physical schema 2 appends `timeline_sequence` uint64 and `reconstruction_only` boolean. Read causal rows by physical timeline sequence. Event timestamp can regress or extend beyond the selected market interval. Timestamp and rank define chronological layout ordering. Support for each layout depends on the reader. [Schema builder](https://github.com/flowdrivenml/marketforge/blob/main/engine/src/historical/merge/materialize/builders/schema.rs) · [Manifest validator](https://github.com/flowdrivenml/marketforge/blob/main/engine/src/historical/merge/materialize/publication/manifest.rs)
