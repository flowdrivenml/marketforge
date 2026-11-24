# Debugging historical workflows

Start with the operation's exit status and retained structured output. Identify the latest verified stage, then compare the pinned inputs at that stage with what the next component consumed. Use the same environment, project root and recipe or submission path as the original invocation.

| Symptom | Evidence and next action |
| --- | --- |
| A recipe selected the wrong inputs or none | Inspect the preparation report's requests, missing evidence, candidate IDs and selection bounds. Compare file dataset IDs with publication IDs and check scope, stream kind, namespace and interval. Use explicit selectors where the recipe supports them. |
| Preparation fails with missing historical metadata | Compare the recorded venue, source scope, instrument and effective specification to the [catalogue and registration workflow](orchestration.md#change-catalogue-or-registry-behavior). Check whether the source has a concrete instrument or family assertion and which normalization tasks it contributes to. |
| Valid TOML produces an Engine rejection | Inspect the prepared or submitted JSON, and read the actual Engine validation result. Check supported schema or mode, paths, hashes and runtime limits. Merge validates via `--validate-job`; current conversion uses `--validate`. Do not pass the Forge conversion collection to the Engine. |
| An expected option or schema is missing | Run `mf engine status` in the Forge environment. Compare the checksum and recorded revision with the installed binary and edited checkout. Use `mf engine build normalize`, `merge`, or `convert` as appropriate, then generate a new preparation if the old one pinned a different executable. |
| Normalized values or counts differ | Open the exact source member and resolved task. Check profile fingerprint, timestamp/quantity units and instrument specification. Compare task counters and canonical fields with independently interpreted source records. Row counts and event counts can differ for depth. |
| Integrity evaluation degrades or fails | Read the recorded metrics and integrity evaluation in the committed manifest, or `failure.json` where emitted. Compare the category and count or rate to the retained runtime policy. A few displayed diagnostic samples do not bound the total count. |
| A book looks incorrect after a boundary | Inspect source observations, segment/index evidence and initialization state. Locate the first wrong event, operation or sequence relation. Compare the prior book and subsequent changes at that point. For a causal merge, inspect sequence and reconstruction markers alongside event timestamps. |
| Files exist but catalogue inspection cannot find them | Check staging versus the final manifest path. Compare the Engine outcome, verification and registration states in the run report. When normalization or merge has committed and the retained receipt is intact, use the operation's documented registration-only command. Conversion has no Forge registration path in this checkout. |
| Python discovery or row inspection fails | Inspect `MarketData.discover().diagnostics`, the publication manifest and primary file metadata. Verify per-file registry evidence and schema support. For Decimal256 or nested values, use Arrow batches for exact values and check the Python API's projection limits. |
| Resource use is unexpectedly high | Compare the effective runtime settings, phase diagnostics, bytes, rows and task counts with the expected workload. Look for archive expansion, reconciliation scratch, open-file pressure, or concurrency admission before changing limits. See [Performance](performance.md). |

## Find the relevant artifact

| Stage | Location or evidence |
| --- | --- |
| Prepare | Use the command's reported path. Normalization retains a batch and report, merge a `merge-job.json` and report, and conversion a `convert.json` collection, per-job JSON and `prepare-report.json`. Dry runs can report proposed artifacts without creating them. |
| Execute | Look under the reported operation run in `data/runs/` or the configured data root. Inspect `execution-intent.json`, `execution-result.json` and pinned submissions. Non-TUI subprocess capture can retain `engine-stdout.log` and `engine-stderr.log`. A TUI owns its terminal output. |
| Validate or diagnose | Read the structured Engine result wherever the operation uses one. Inspect normalization failure metadata, processing counters and relevant integrity diagnostics. Use `manifest.json`, checksums and the declared auxiliary inventory to establish what was committed. |
| Inspect catalogue | Use `mf datasets show ID --testing` or `mf datasets lineage ID --testing` for a registered file ID. Use the filesystem-first Python API if you have a final path. |

For read-only CLI inspection of an existing registered file, substitute the actual file ID. The JSON output is suitable for checking IDs and statuses without relying on terminal formatting.

```bash
uv run --project forge --locked mf --json datasets lineage "$FILE_DATASET_ID" --testing
uv run --project forge --locked mf --json engine status
```

Keep exact filenames and submission hashes when reporting an issue. Use only the recovery commands the affected operation documents. A retry of execution and registration-only recovery affect different state. Never patch a committed manifest or overwrite registered files to pass a check. [Historical lifecycles](operations.md) · [Data and lineage inspection](../reference/cli/datasets.md) · [Python API](../research/python-api.md)
