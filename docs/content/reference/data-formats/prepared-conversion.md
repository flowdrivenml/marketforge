# Prepared conversion job

A version 1 prepared job describes one execution against complete physical merge
publications. Forge uses read-only discovery and verified lineage to produce this
job. The individual Rust job has a separate schema from the Forge collection.

| Job field | Meaning |
| --- | --- |
| `schema_version`, `operation`, `job_id` | Schema version 1, operation `convert`, execution identity |
| `input_merge`, `source` | Absolute path and prepared binding for the source publication |
| `conversion.reference_merge`, `external_reference` | Optional absolute path and prepared binding for an external trade-only publication |
| `catalog_sha256` | Fingerprint of the preparation catalog or the historical catalog snapshot derived from committed authority |
| `conversion` | Target quote, price source, staleness, hop limit, path policy, instrument-kind filter, and explicit reference identities |
| `resources`, `parquet` | Effective resource caps and Parquet row group size, file size, and compression |
| `output.dataset_path` | Absolute destination. Its sibling with extension `.staging` is used for materialization |

Each binding records the engine merge job ID, SHA-256 pins for `manifest.json`
and `publication-checksums.json`, the originating normalized dataset IDs and
evidence hashes, and a record for each stream. Each stream identifies its
normalized dataset, stream ID, rank, kind, instrument ID and namespace,
exchange, symbol, instrument and contract kinds, and base and quote currencies.
Merged and normalized IDs have different roles. The JSON stores IDs as integers.

Forge checks the instrument metadata against committed normalized authority and
checks an installed catalog against that authority when present. It retains
catalog and normalization evidence fingerprints. The Engine compares the
prepared bindings against the physical stream inventory, including currencies
where the physical manifest carries them. Where the manifest omits currencies,
the embedded assignments are statements of verified preparation evidence.
A publication hash establishes source identity and integrity. It cannot
independently establish omitted currency values.

Forge caps recipe resources using historical runtime settings during preparation
and records the effective Parquet writer sizes in MiB and Zstandard level. The [conversion runtime](../runtime.md#merge-and-conversion-runtime) supplies these values. The Engine checks worker
limits and admission estimates for writer row groups and open files.
`--workers` can choose a lower worker count. Publish staged output using the
same job and worker override. The committed `conversion-runtime.json` records
the full job and effective settings.

Execution requires the job, declared physical publications and their commit
metadata, and the output location. The old `--catalog`, `--normalization-root`,
`--config`, and `--project-root` flags report migration errors. Re-prepare
incomplete jobs with Forge. The Engine needs neither database access nor a
runtime configuration file.

See the [conversion workflow](../../workflows/conversion.md) for commands, reference
selection, and bounded verification.

## Structured bindings and limits

All listed job-level fields are required except that `external_reference` is nullable and can be omitted by the Rust decoder. Positive IDs and counters use unsigned 64-bit JSON integers, stream ranks use uint32, and instrument IDs use positive signed 64-bit integers. SHA-256 fields use 64 lowercase hexadecimal characters. Source and output paths are absolute. The external path and binding must be supplied together. Forge emits the optional values explicitly where its serializer uses nulls.

| Object | Serialized fields |
| --- | --- |
| `source`, `external_reference` | `merge_job_id`, `manifest_sha256`, `publication_checksums_sha256`, nonempty `normalized_inputs`, nonempty `streams` |
| `normalized_inputs[]` | `dataset_id`, nullable `manifest_sha256`, `processing_job_sha256` |
| `streams[]` | `dataset_id`, `stream_id`, `stream_rank`, `kind` (`trade` or `depth`), `instrument_id`, `instrument_namespace`, `identity`, `base_currency`, `quote_currency` |
| `identity`, `conversion.references[]` | Nonblank `exchange`, `symbol`, `instrument_kind` (`spot`, `perpetual`, `future`, `option`, `equity`), nullable `contract_kind` (`linear` or `inverse`) |
| `conversion` | Required `target_quote`, `price_source` (`last_trade`), `max_staleness_ns` (positive uint64). Optional `instrument_kinds` and absolute `reference_merge` default to null. `references` defaults to an empty array, `path_policy` to `all`, `max_hops` to 3. Forge resolves and emits explicit policy fields and references |
| `resources` | Positive `workers`, `memory_limit_mb` in MiB and `max_open_files` |
| `parquet` | Positive `row_group_target_mb` and `file_target_mb` in MiB, with row group target no larger than file target. `compression_level` is 1–22 |
| `output` | Absolute `dataset_path` |

Bindings reconcile the source manifest's dataset, stream identity, rank and kind with each entry. Inspect `--validate` diagnostics for inconsistent, duplicate, missing or unsupported evidence. Currency codes use uppercase ASCII letters or digits. Unknown wire fields are rejected. [Engine model](https://github.com/flowdrivenml/marketforge/blob/main/engine/src/historical/job/convert.rs)

## Forge preparation and retained evidence

Read the CLI report’s `status` and `compatibility` together. A dry run computes specifications with prospective paths and creates no saved jobs. A generated submission retains `convert.json`, individual `job-*.json` files, a preparation report, the base `convert.toml` snapshot and applicable catalog and recipe snapshots. The collection pins the selected registry, source evidence, job files and managed executable. Rust validates individual jobs and can emit a route plan. `generated` with `validated` records successful validation of saved jobs. A blocker can prevent generation or keep generated work at `not_validated`. When every job can be reused, preparation can return a retained reuse result instead.

`convert.json` is a Forge collection. Run one retained individual job via its collection context, or use the standalone Engine with the individual JSON. The Engine does not open the Forge collection as a job. Execution evidence records the submitted job and conversion output separately. See [prepare result fields](../cli/convert.md#preparation-result) and [Forge orchestration evidence](prepared-forge.md).

```bash
marketforge-convert --job /absolute/path/to/job-123.json --validate
marketforge-convert --job /absolute/path/to/job-123.json --run
```

With the standalone binary on PATH, validation checks the prepared schema, physical merge checksums, stream inventory and route plan without materializing output. Standalone execution commits the physical files. Forge performs the durable indexing for managed submissions. For a self-contained physical regression fixture, see [Conversion verification](../../development/testing.md#conversion-verification).
