# Prepared historical conversion configuration

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
and records Parquet writer sizes in MiB and Zstd level 3. The Engine checks worker
limits and admission estimates for writer row groups and open files.
`--workers` can choose a lower worker count. Publish staged output using the
same job and worker override. The committed `conversion-runtime.json` records
the full job and effective settings.

Execution requires the job, declared physical publications and their commit
metadata, and the output location. The old `--catalog`, `--normalization-root`,
`--config`, and `--project-root` flags report migration errors. Re-prepare
incomplete jobs with Forge. The Engine needs neither database access nor a
runtime configuration file.

See the [conversion workflow](../workflows/conversion.md) for commands, reference
selection, and bounded verification.
