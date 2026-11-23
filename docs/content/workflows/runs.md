# Inspect execution history

Use Runs to answer what MarketForge attempted, where it stopped, and which output it recorded. Acquisition, normalization, merge, conversion, and pipeline can all appear in run history. A successful acquisition can provide raw archives for normalization. A prepare action can retain a job for later execution. Only processing with the relevant publication and registration or indexing evidence gives you an output to inspect in [Datasets](datasets.md).

## Find the run and read its lifecycle

```bash
mf runs list --testing --limit 20
mf runs list --testing --operation normalize --limit 10
mf runs show RUN_ID --testing
```

Copy the exact run ID from the result. Use the registry that performed the work. The timestamp column records when the run started. Duration is available when both timestamps are recorded. An indexed running record has unknown process liveness.

Look at the overall status and the separate execution, verification, and registration outcomes. An Engine can finish and verify a publication while registry registration fails. A prepared run can finish its action before any Engine execution. [Lifecycle reference](../reference/cli/runs.md#run-lifecycle)

Attempts belong to the same logical run. A [pipeline resume](pipeline.md) can add an attempt under the original run ID. Child runs have their own run IDs and can be opened independently with `runs show`. The attempt UUID identifies one attempt in that history. Use the parent run ID with the Runs commands.

## Read logs and reports

```bash
mf runs logs RUN_ID --testing --limit 25
mf runs report RUN_ID --testing
mf runs report RUN_ID --testing --section diagnostics
```

Logs display recorded events from the start of an event file or bounded tails of named Engine and validation streams. Available timestamps, levels, and stage information depend on what the producer recorded. A report explains the operation itself, such as selected inputs, blockers, committed output, verification and registration. Either source may be absent for historical or interrupted runs.

### Investigate a blocked merge

```bash
mf runs list --testing --operation merge --status blocked
mf runs report RUN_ID --testing --section diagnostics
mf runs report RUN_ID --testing --section inputs
mf datasets show --publication-id PUBLICATION_ID --testing
```

When a preparation report says “Multiple verified submissions match,” review the candidate registry publication IDs and the selected count. Open candidate publications to compare scope and evidence. Follow the [merge selection and pinning walkthrough](merge.md#when-multiple-verified-submissions-match) before editing the recipe. When the report records an Engine failure, inspect Engine diagnostics and the execution artifacts instead. For registration failure after verified output, follow the operation’s registration recovery procedure. [Normalization recovery](../reference/cli/normalize.md#registration-and-recovery) · [Merge recovery](../reference/cli/merge.md#registration-and-recovery)

## From a normalization run to its datasets

```bash
mf runs report RUN_ID --testing --section outputs
mf datasets show --publication-id PUBLICATION_ID --testing
mf datasets schema FILE_ID --testing
```

Use a registry publication ID from the job results or indexed run resources. Use a primary-file ID from the report or publication inspection for schema. If the report records reuse, the requesting run can point to an earlier canonical output; its run ID still identifies the new request. A retained report can also refer to a publication retired later. Check its current catalogue state or use the applicable verification command before relying on its files.

## From a merged publication to its source

```bash
mf datasets list --testing --view publications --operation merge
mf datasets list --testing --view files --publication-id PUBLICATION_ID
mf datasets lineage FILE_ID --testing --publication
mf runs list --testing --operation merge --limit 20
mf runs report RUN_ID --testing --section outputs
```

Lineage gives normalized parent file IDs and publication-level selections. Open a parent file to identify its normalization publication. To identify the producing merge run, compare the exact output IDs in its retained report with the catalogue. Use retained execution paths or known recipe and timing to narrow the run list. The CLI has no direct publication-to-run lookup, and old evidence may be incomplete.

For a retained attempt that reached an outcome before indexing finished, consult [run recovery and reconciliation](../reference/history-maintenance.md#recover-run-index-metadata).

## Remove old runs from history

Copy the exact Forge run ID from `mf runs list`. Preview an unused failed run in the testing registry before removing it.

```bash
mf runs delete RUN_ID --testing
```

Review the affected attempts, removable files, retained paths and blockers. If discovery, registration recovery, pipeline resume or completed-work reuse still needs the evidence, use `--history-only` to hide the entry and keep that evidence. This frees no storage. Published datasets, raw archives, recipes and runtime settings remain available in either mode.

```bash
mf runs delete RUN_ID --testing --history-only
mf runs delete --testing --apply --plan "PLAN_PATH"
```

Replace `PLAN_PATH` with the path from the preview you want to apply. A preview leaves history and target files unchanged. For descendant selection, blockers and interrupted cleanup, see the [run deletion reference](../reference/cli/runs.md#delete-runs).
