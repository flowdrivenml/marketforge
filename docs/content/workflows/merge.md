<a id="merge-registered-normalized-files"></a>

# Merge normalized data

Merge builds one ordered timeline from registered normalized trade and order-book files. Forge resolves the selection and prepares the job. The Rust Engine writes a committed Parquet publication, then Forge verifies it and registers its primary files. One publication can contain trades and L2 depth from several exchanges and instruments in the same instrument namespace. Rows retain their stream and normalized event provenance. Merge combines compatible inputs in timeline order. It rejects unsupported overlaps and does not provide general event deduplication.

## 1. Find normalized inputs

```bash
mf datasets list --operation normalize --status complete --testing | cpout
```

Start with the Bybit trades normalization recipe named `my_dataset` from the [normalization guide](normalization/index.md). Complete that workflow and use the same testing registry here. Production is the default for registry-backed commands. This example explicitly uses testing throughout. Install the [managed merge Engine](../reference/cli/engine.md) if needed, then validate its base runtime.

```bash
mf merge config validate | cpout
```

This checks the runtime TOML against Rust. Workload admission and input compatibility are checked separately.

Look for registered primary-file IDs in the dataset list or normalization result. A Parquet file on disk needs a complete normalized registry record, a committed manifest, and matching provenance to be selected. Use the registry file ID with `datasets show` to inspect its publication and stream membership. Registry publication IDs group files. They serve a different purpose from the file IDs used for inspection.

<a id="start-from-a-normalization-recipe"></a><a id="select-file-dataset-ids-explicitly"></a>

## 2. Create and edit the recipe

In **Workflows → Merge**, choose registered normalized publications, a
normalization recipe, or physical files. The publication selector shows
registry and engine IDs, generation, integrity and observed bounds. Review
the verified membership and adjust stream order before saving. **Prepare**
remains a separate step that resolves the Engine input.

```bash
mf merge new first_merge --from-normalize my_dataset --testing
```

This copies the source recipe's interval into a new managed merge recipe and saves a live reference to `my_dataset`. It searches the selected registry and reports candidate files and blockers. In a human terminal it also opens an editor. To revise the saved selection later, use:

```bash
mf merge edit first_merge
```

Later source edits can change which inputs match, while the merge interval remains as saved. The creation report reflects discovery before the editor opened. Run the preparation preview again after saving to review the current selection.

## 3. Validate the recipe

```bash
mf merge validate first_merge | cpout
```

Validation checks TOML, recipe constraints, and the local source reference. Registry eligibility and physical compatibility are checked during preparation.

<a id="selection-and-runtime"></a>

## 4. Preview preparation

```bash
mf merge prepare first_merge --testing --dry-run | cpout
```

Check the selected files, their publications and stream ranks, the requested interval, ordering, and any missing requests or blockers. If the same normalization recipe has been run more than once, compare the candidate publications against the run you intended to merge. For segmented depth, make sure the selection includes every primary part needed to reconstruct the chosen stream. A `planned` result resolves the Forge selection and provenance. Paths shown in the plan are prospective. Rust job validation happens during persisted preparation. Fix selection blockers before running.

<a id="reading-the-merge-dashboard"></a>

### When multiple verified submissions match

The preview can report matched files while selecting none. Inspect each candidate **registry publication ID**, then compare stream membership, source submission, integrity and interval with the normalization run you intend to use.

```bash
mf datasets show --publication-id PUBLICATION_ID --testing | cpout
mf runs report NORMALIZATION_RUN_ID --testing | cpout
```

In `mf merge edit first_merge`, add or replace `publication_ids = [101]` **inside the existing** `[selection]` table. Replace the illustrative ID with the intended trades publication. For multiple requested streams, list all publications needed from the intended generation. Keep only one `[selection]` table. Preview preparation again and check both the selected publication IDs and the selected file count. See [selection rules](../reference/configuration/merge.md#selection-and-ambiguity).

## 5. Run the merge

```bash
mf merge run first_merge --testing
```

Run by recipe name to prepare, validate, execute, verify, and register in one command. For the same workflow with the Rust progress monitor, use an interactive terminal:

```bash
mf merge run first_merge --testing --tui
```

### Submit a prepared job separately

Use `mf merge prepare first_merge --testing` when you need to inspect the exact job before execution. Check that the result is `prepared` with compatibility `validated`. Pass the reported JSON path to `mf merge run ./PATH_TO_MERGE_JOB.json --testing`. Keep the job, preparation report, and pinned runtime together. See [prepared merge jobs](../reference/data-formats/prepared-merge.md).

For scripting, use global JSON and check the exit status together with the execution, verification and registration fields.

```bash
mf --json merge run first_merge --testing > merge-result.json
```

When Forge can verify an equivalent completed merge, it returns the existing publication and skips processing. See [completed-work reuse](../reference/cli/merge.md#completed-work-reuse).

## 6. Read the result and inspect the output

Check **Execution**, **Verification**, and **Registration** separately. A committed Engine publication can precede a registration failure. Use a registered output **file ID** from the result with `datasets show` or `datasets verify` in the same registry. The Engine job and publication identities are useful for retained evidence.

```bash
set -o pipefail
mf runs show RUN_ID --testing | cpout
mf runs report RUN_ID --testing | cpout
mf datasets show --publication-id PUBLICATION_ID --testing | cpout
mf datasets show FILE_ID --testing | cpout
mf datasets verify FILE_ID --testing | cpout
```

Replace the uppercase placeholders with identities from your result or run history. The publication view shows its manifest and primary-file inventory. Each partition can contain several physical files. See the [identity table](../reference/cli/merge.md#identifiers). For causal output, read in timeline order and keep reconstruction context when replaying book state. See [merged publications](../reference/data-formats/publication-merge.md).

<a id="supported-layouts"></a>
<a id="merge-tests"></a><a id="optional-recording-compatibility"></a>
<a id="timeline-ordering"></a>

## Supported inputs and ordering

Trades and depth can share one merge when their layouts and instrument namespace are compatible. Segmented depth requires causal ordering and all primary parts of each selected depth stream. See [merge Engine tests](../development/merge.md#testing-merge-changes) for fixture coverage. See [layout compatibility](../reference/data-formats/prepared-merge.md#merge-compatibility) and [recipe ordering](../reference/configuration/merge.md#timeline-ordering).

<a id="publication-and-recovery"></a>

## Troubleshooting

| Situation | First check |
| --- | --- |
| No eligible inputs | Confirm selection, interval, normalized input stage, and testing registry. |
| Input or provenance check fails | Read the specific preparation blocker and inspect the selected file or publication. |
| Rust validation unavailable | Check [managed Engine compatibility](../reference/configuration/merge-runtime.md#managed-engine-troubleshooting) and the retained runtime. |
| Engine fails | Inspect the retained execution result and Engine diagnostics. |
| Output committed but registration failed | Run `mf merge register PATH_TO_EXECUTION_INTENT --testing` with the retained `execution-intent.json`. |
| Interrupted | Inspect retained evidence for committed output before deciding between registration recovery and a new run. |

Continue with [dataset inspection](inspection/datasets.md), or use [conversion](conversion.md) for supported chronological or causal publications. See the [CLI reference](../reference/cli/merge.md) and [merge runtime settings](../reference/configuration/merge-runtime.md).
