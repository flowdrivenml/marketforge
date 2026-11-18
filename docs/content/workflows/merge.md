# Merge registered normalized files

Install the managed merge binary and create a recipe.

```bash
mf engine build merge
mf engine status
mf merge new session
```

Edit `data/.config/historical/merge/session.toml`. Replace the illustrative IDs with file dataset IDs returned by normalization registration or `mf datasets list --testing` and set the requested half-open interval in integer nanoseconds.

```toml
dataset_ids = [1, 2]
start_ns = 1727683200000000000
end_ns = 1727769600000000000
```

```bash
mf merge validate session
mf merge prepare session --testing
mf merge run session --testing
mf merge run session --testing --tui
```

Choose one run command. Both modes prepare one `MergeJob`, submit it, verify the committed physical timeline and register the output in PostgreSQL. TUI mode requires an interactive terminal. For machine output, use `mf --json merge run session --testing`.

## Selection and runtime

Files from each publication must appear consecutively in the recipe. Forge verifies each owning publication and records the ordered manifest-relative files, their checksums and the shared manifest checksum in `primary_files`. Rust verifies membership and consumes events from the selected files. Publication precedence follows the recipe. Within a publication, canonical source order resolves ties across selected parts. Forge reports gaps in the observed extents of selected streams.

Both operations read `data/.config/historical/general.toml`. Managed merge preparation uses the shared Rust runtime loader for worker count, memory budget, open-file limit and Parquet sizes. The byte budgets sent to the current merge engine must be whole MiB. Optional merge settings have these defaults.

```toml
[merge]
partitions_per_worker = 4
compression_level = 3
```

These settings are resolved into the current Rust JSON contract as `resources`, `partitioning` and `parquet`. A managed prepared job retains the runtime, executable, request and input fingerprints. To replay exactly that job, use `mf merge run data/runs/PREPARE_ID/merge-job.json --testing`. A standalone legacy `MergeJob` JSON provides its own embedded execution settings and must supply absolute paths for registered inputs. The Rust parser validates that contract.

## Supported layouts

Managed merge accepts v3 single-stream inputs and can select one concrete trade stream from a timestamp-first v4/v5 publication with contiguous source ownership and no repeated source spans. Supported trade selections may contain one part, several parts of the same stream, or parts from different publications. The current engine accepts one selected stream per publication. Native-order or interleaved trade layouts and already-combined inputs are unsupported.

Depth currently requires a compatible single-stream v3 publication and explicit selection of all its primary files for reconstruction. Rust reads the shared auxiliary state from the publication. Requests missing required primary parts are rejected before processing.

## Publication and recovery

A recipe uses a fresh output under `data/datasets/merged/<engine-job-id>` relative to the configured data root. Rust publishes the validated physical timeline, manifest, resolved job, input manifest checksums and physical validation evidence atomically. Forge revalidates the committed physical Parquet and byte fingerprints and registers each primary output in `historical.datasets` under one shared publication. Selected input file IDs are retained as publication-scope lineage.

The report under `data/runs/merge-execute-*/execution-result.json` distinguishes execution, verification and database registration. A complete report includes each file dataset ID and its path. Failed or interrupted runs return nonzero. One merge job publishes one output.

After a power loss or database error, retry registration by passing the execution intent, committed directory or its `manifest.json`.

```bash
mf merge register data/runs/MERGE_EXECUTION_ID/execution-intent.json --testing
mf merge register data/datasets/merged/ENGINE_JOB_ID --testing
```

Registration-only recovery checks the output and required input lineage again. Identical retries reuse the database ID.
