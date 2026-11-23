# Merge runtime configuration

`data/.config/engine/merge.toml` controls resource admission, partition planning, and Parquet output. The [merge recipe](merge.md) selects normalized inputs, interval and timeline ordering. Forge resolves the runtime separately and retains it with the [prepared job](../reference/prepared-merge.md).

## Quick start

```bash
mf merge config show | cpout
mf merge config show --section resources | cpout
mf merge config get resources.workers | cpout
mf merge config schema --field resources.memory_limit_mb | cpout
mf merge config validate | cpout
```

Use `show --all` for every field and `schema` for the whole installed Rust schema. Detailed views distinguish configured values, loader defaults and effective values for the **base runtime**. These commands use local files and the managed Engine. See the [CLI reference](../reference/cli/merge.md#engine-runtime-configuration) for all flags and global JSON.

The Engine validates syntax, the runtime version, known fields, types, ranges and cross-field constraints. This check does not open merge inputs or admit a workload. A missing or incompatible Engine is a failed check. See [managed Engine troubleshooting](#managed-engine-troubleshooting).

## File and resolution

Forge resolves the base file from the workspace root, independent of the data root. The direct Rust merger accepts `--config PATH` with relative paths resolved from its `--project-root`. For read-only validation of a candidate from the workspace, the direct Engine accepts exactly one path:

```bash
.bin/marketforge-merge --runtime-settings ./candidate.toml
```

A base file is required. Current schema-1 files use the fields below. The loader rejects unknown fields. Retained legacy `general.toml` has a distinct compatibility path and is not the current authoring format.

| Stage | Runtime used |
| --- | --- |
| New recipe preparation | Forge copies and fingerprints the base file and validates the job against that copy. |
| New pipeline | The stage inherits the base, then receives recursive `[merge]` overrides from `pipeline.toml`. Forge validates the base and composed stage. Resume uses pinned stage files. |
| Submitted job | Execution uses the preparation copy when the report pins a runtime. It checks retained fingerprints. A standalone job without that evidence resolves the applicable runtime at launch and must satisfy testing and registry restrictions. The public job has no runtime fields or job-specific runtime overrides. |
| Running and committed | The process loads the selected config. The execution intent records the source path, copy path, hash and effective values. The publication retains checksummed `merge-runtime.json`. |

Edits affect later base resolutions. Prepare a new job to apply changed settings to a pinned preparation. See [pipeline runtime](../reference/runtime.md#pipeline-runtime) and [pipeline recipes](pipeline.md).

## Schema and resources

| Field | Type and omission behavior | Effect and bounds |
| --- | --- | --- |
| <a id="schema-version"></a>`schema_version` | Required `u32` | Exactly `1`. Config CLI treats it as immutable. |
| <a id="resources-workers"></a>`resources.workers` | Required `usize` | Positive requested worker ceiling. The product with effective partitions per worker must fit `usize`. Resource admission can reduce active concurrency. |
| <a id="resources-memory-limit-mb"></a>`resources.memory_limit_mb` | Required `u64`, binary MiB | Positive and must fit `u64` bytes after multiplication by 1,048,576. Budget for admitted work and estimates, including readers, buffers and writers. This is not an OS RSS limit. |
| <a id="resources-max-open-files"></a>`resources.max_open_files` | Optional `usize`, default `64` | Positive participating file-handle budget. Physical materialization reserves five handles in its per-partition estimate before admitting forward cursors. Work can fail if those estimates do not fit. The OS descriptor limit is separate. |

A valid budget can still be too small for a particular partition. Physical materialization estimates writer memory at twice the row-group target and reserves memory for source and forward cursors, decoding, and the working window. Multiple admitted workers share the operation budgets. The merge schema has no configurable scratch budget or path, merge fan-in, diagnostics cadence, or normalization integrity policy.

## Parquet

All byte-size TOML fields below use binary MiB. Encoded sizes depend on rows, compression, and metadata.

| Field | Type and omission behavior | Effect and bounds |
| --- | --- | --- |
| <a id="parquet-row-group-target-mb"></a>`parquet.row_group_target_mb` | Required `u64`, MiB | Positive, at most the file target. Guides buffering and row-group flushes and affects writer admission estimates. |
| <a id="parquet-file-target-mb"></a>`parquet.file_target_mb` | Required `u64`, MiB | At least the row-group target. Must fit `u64` bytes. Guides physical part rotation at writer boundaries. It is a target, not a strict file-size cap. |
| <a id="parquet-compression-level"></a>`parquet.compression_level` | Optional `i32`, default `3` | Inclusive range `1–22`. Zstandard compression level for Parquet output. Runtime validation does not predict compression ratio or throughput. |

## Partitioning

<a id="partitioning-partitions-per-worker"></a>

`partitioning.partitions_per_worker` is a positive `usize`. Omit the entire table for a default of **4**. A present table requires this field. For chronological timestamp partitioning, the Engine requests workers × partitions per worker, capped by the interval's duration in nanoseconds. Empty work need not produce a primary file. Actual admitted work is limited by available memory and handles. A partition can rotate into multiple physical files.

The causal path builds one forward logical timeline and uses contiguous sequence ranges for physical materialization. The chronological partition multiplier does not control those causal ranges. See [publication layout](../reference/publication-merge.md).

## Preview, apply, and edit

```bash
mf merge config set resources.workers 2 | cpout
mf merge config set resources.workers 2 --apply | cpout
mf merge config set resources.memory_limit_mb 4GiB | cpout
mf merge config unset resources.max_open_files | cpout
mf merge config unset resources.max_open_files --apply | cpout
mf merge config edit
mf merge config validate | cpout
```

`set` and `unset` preview by default. Bare size values mean whole MiB for merge size fields. Case-sensitive `KiB`, `MiB`, `GiB`, and `TiB` are accepted when they convert exactly to whole MiB, including exact fractions such as `1.5GiB`. The TOML stores integers. Unset only optional leaves. Unsetting the partition multiplier removes its now-empty table.

Each invocation reads and validates its own candidate. Apply revalidates with the managed Engine and atomically replaces the file after checking the original entry and contents had not changed during this invocation. A previous preview is advisory and does not pin a later apply command. Structured edits preserve unrelated TOML and comments. Current merge fields are ordinary or resource-sensitive, so they require no integrity-risk acknowledgement.

Interactive `edit` uses a private draft and the configured editor, with Neovim as fallback. After validation, review the diff summary and type `apply` to save. A declined or invalid edit retains the draft path. Read the current file and reconcile any conflicting changes before applying again.

An example of candidate rejection is a file target below the row-group target. Rust reports `parquet.row_group_target_mb must be positive and no greater than file_target_mb`.

## Managed Engine troubleshooting

```bash
mf engine status | cpout
mf engine build merge | cpout
mf merge config schema | cpout
mf merge config validate | cpout
```

Use the [managed build](../reference/cli/engine.md) from the Forge workspace. It installs the executable and updates the managed binary inventory under `.bin/`. A Cargo build in another output directory leaves that inventory unchanged.

- **Schema inspection fails.** Check which executable Forge resolves and whether the managed inventory and capabilities match the checkout. `mf engine runtime merge` may still inspect effective settings on a binary that supports runtime inspection but predates schema export.
- **The Engine rejects the whole `[parquet]` table.** The current dedicated schema requires it. Check for an older generic loader and rebuild the managed binary before changing the TOML.
- **The installed schema is current and validation names a field or relationship.** Correct the type, range, or cross-field relationship in the base runtime. For example, the file target must be at least the row-group target.
- **Only a retained job fails.** Inspect its preparation report, runtime snapshot and executable checksum. Base validation checks the current base TOML. Prepare a new job to use the current binary and runtime together when the old pins cannot be satisfied.

## Complete runtime example

This document provides the required values and inherits the three loader defaults. The resource values are illustrative, and workload admission remains separate.

```toml
schema_version = 1

[resources]
workers = 2
memory_limit_mb = 4096

[parquet]
row_group_target_mb = 32
file_target_mb = 256
```

For example, preview setting `parquet.file_target_mb` to `16MiB` against this configuration to see the cross-field error. Validate a candidate with the direct loader before applying edits to a workspace.
