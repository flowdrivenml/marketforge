# Conversion runtime

Forge loads the base runtime from `data/.config/engine/convert.toml`. The managed Rust converter defines schema version 1 and validates the complete document. See [runtime commands](../cli/convert.md#runtime-configuration-commands) for inspection and updates.

| TOML path | Type and units | Required / default | Validation and behavior |
| --- | --- | --- | --- |
| `schema_version` | uint32 | Required | Exactly 1. Read-only through the runtime editor commands. |
| `resources.workers` | Positive platform-sized integer, workers | Required | Caps requested concurrency. Engine admission can reduce admitted parallelism. |
| `resources.memory_limit_mb` | Positive uint64, MiB | Required | Must fit uint64 in bytes. Caps the recipe memory budget used in admission checks. |
| `resources.max_open_files` | Positive platform-sized integer, file handles | Optional, 64 | Caps the recipe handle budget used in admission checks. |
| `parquet.row_group_target_mb` | Positive uint64, MiB | Required | Target writer row group size, at most the file target. Participates in memory admission. |
| `parquet.file_target_mb` | Positive uint64, MiB | Required | Part rotation target, at least the row group target and convertible to uint64 bytes. |
| `parquet.compression_level` | Integer, Zstandard level | Optional, 3 | 1–22 inclusive. Sets the Parquet writer compression level. |

Unknown fields and conversion-specific use of a `partitioning` table fail validation. Required values must be present in the base file.

```toml
schema_version = 1

[resources]
workers = 4
memory_limit_mb = 1024

[parquet]
row_group_target_mb = 16
file_target_mb = 128
```

## Effective settings and snapshots

Forge preparation reads and validates a snapshot with matching Python job field constraints. It takes the lower of each recipe resource request and the corresponding base limit. Parquet values come from the base runtime. The prepared job embeds these settings, and the Engine performs its own admission checks for row-group memory, workers and open files. The standalone Engine accepts a lower worker count via `--workers`; managed `mf convert run` uses the prepared settings.

Editing the base configuration affects future preparation. A retained job supplies its own runtime values and pins the executable used to validate it. Re-prepare when you want new base settings or after changing the pinned managed binary. Committed output records the submitted job and effective settings in `conversion-runtime.json`. See the [prepared job contract](../data-formats/prepared-conversion.md).

## Settings Explorer field targets

These entries use the runtime schema’s wording and constraints. The sections above explain how settings interact in a processing run.

<a id="setting-schema_version"></a>

**`schema_version`**

Versions the base runtime document.

Type `integer` · required in the base runtime file · read-only schema identity. Must match the Engine runtime schema version. Risk `ordinary`.

<a id="setting-resources.workers"></a>

**`resources.workers`**

Requested worker concurrency. Admission can reduce effective parallelism.

Type `integer` · required in the base runtime file. Positive. Risk `resource-sensitive`.

<a id="setting-resources.memory_limit_mb"></a>

**`resources.memory_limit_mb`**

Memory budget in binary MiB for admitted work.

Type `mib` · units MiB · required in the base runtime file. Positive and must fit u64 when converted to bytes. Risk `resource-sensitive`.

<a id="setting-resources.max_open_files"></a>

**`resources.max_open_files`**

Budget for participating file handles.

Type `integer` · default `64`. Positive. Risk `resource-sensitive`.

<a id="setting-parquet.row_group_target_mb"></a>

**`parquet.row_group_target_mb`**

Target row group size in binary MiB.

Type `mib` · units MiB · required in the base runtime file. Positive and no greater than parquet.file_target_mb. Risk `resource-sensitive`.

<a id="setting-parquet.file_target_mb"></a>

**`parquet.file_target_mb`**

Part rotation target in binary MiB.

Type `mib` · units MiB · required in the base runtime file. At least the row group target and must fit u64 when converted to bytes. Risk `resource-sensitive`.

<a id="setting-parquet.compression_level"></a>

**`parquet.compression_level`**

Parquet Zstandard compression level.

Type `integer` · default `3`. 1 to 22 inclusive. Risk `resource-sensitive`.
