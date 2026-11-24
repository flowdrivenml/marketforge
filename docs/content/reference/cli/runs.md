# Runs CLI reference

Runs covers logical Forge operations, attempts, child runs, and retained evidence. Start with [inspect execution history](../../workflows/inspection/runs.md) for practical examples. Use [registry identity](../../concepts/identity.md#registry-identity) to choose the environment. Production is the default, and `--testing` and `--production` are mutually exclusive.

## Browse the selected registry

| Command | Arguments and options | Effect |
| --- | --- | --- |
| `mf runs list` | Filters below, plus `--limit N`, `--offset N` | Read indexed run metadata |
| `mf runs show RUN_ID` | Exact run reference, accepts `--limit` | Read lifecycle, timing, attempts, child runs and resources |
| `mf runs logs RUN_ID` | Exact run reference, optional `--limit N` | Read retained event prefix or fixed Engine streams |
| `mf runs report RUN_ID` | Exact reference, optional `--section`, accepts `--limit` | Read the retained operation report or attempt outcome |
| `mf runs index-legacy` | `--limit N`, `--offset N`, `--dry-run` or `--apply` | Preview legacy indexing or write verified index entries |
| `mf runs reconcile` | `--limit N`, `--offset N`, `--dry-run` or `--apply` | Preview or apply index finalization from retained evidence |
| `mf runs delete` | Exact indexed run IDs, optional `--with-children` or `--history-only`, or `--apply --plan PATH` | Preview or apply a reviewed run deletion |

For `list`, filters are exact and run before paging. `--operation` accepts `acquisition`, `normalize`, `merge`, `convert`, and `pipeline`. `--action`, `--status`, and `--recipe` match the recorded fields. `--start` and `--end` accept timezone-qualified ISO 8601 timestamps and filter run start in `[start, end)`. Results default to descending run start, then ascending run ID. `--sort time` aliases `start`, and `--order asc` alone lists oldest starts first. [Sorting fields and defaults](sorting.md). The default limit is 100, range 1–1,000. Offset defaults to zero, range 0–100,000. The count in terminal output describes the page.

```bash
mf runs list --testing --operation normalize --action run --status completed --limit 10
mf runs show RUN_ID --testing
```

Use the full `operation-UUID` from the index. Supported retained references include bare legacy UUIDs and recognized managed execution directories for normalization, merge and conversion. Historical inspection can fall back to local evidence if there is no indexed legacy record. A canonical ID absent from the selected registry returns an error. Managed references must match a supported directory format. Arbitrary filesystem paths are outside this interface.

Indexed `show` reports up to 1,000 attempts, 4,096 resources, and 1,000 child runs. It flags truncated attempt and resource lists. Terminal tables show smaller samples and report when they limit a returned section. The `--limit` accepted by `show` and `report` does not change these bounds or report size limits. There is no attempt selector. Use the attempt number and UUID in `show` and the retained outcome or report pointer for that attempt.

## Run lifecycle

| Object | Meaning |
| --- | --- |
| Run | Logical operation with an action, recipe when available, registry identity, timing and recorded status |
| Attempt | Numbered execution attempt with its own UUID, timing, outcome and report pointers |
| Child run | Separately indexed run linked to its parent in the registry |
| Resource | Stage- and attempt-associated reference for an input, output or artifact |
| Report | Retained operation evidence, which may live in a separate Engine preparation or execution directory |

Resource kinds include discovery scans, remote and local archives, prepared batches and jobs, Engine execution references, source and output Engine publications, registry publications, primary-file IDs, converted paths, reports, and child runs. Read their labels before using an ID. A registry publication reference can describe a source or an output. [Identifier domains](../../concepts/identity.md#inspection-identifiers)

| Recorded status | Interpretation |
| --- | --- |
| `running` | Work began or the index still awaits finalization. Liveness is unknown. |
| `prepared` | Preparation reached its recorded ready state |
| `completed` | Recorded action succeeded under its own contract |
| `partial` | Some requested work succeeded, or a later step such as registration remains incomplete |
| `blocked` | A prerequisite, selection or compatibility check prevented progression |
| `failed` | The action recorded failure |
| `interrupted`, `cancelled` | The retained evidence records interruption or cancellation |
| `stopped` | The operation recorded a stop, for example at a pipeline checkpoint |
| `unknown` | The indexed status could not be established from the domain result |

Engine execution, physical verification, and registration are separate outcomes. A reused result can point to an earlier verified output. A report describes its retained evidence; current physical availability requires a suitable dataset check. A retry of a logical run can produce a new attempt, while lower-level retries or Engine jobs need not create one. See [pipeline resume](pipeline.md) and [prepared jobs and evidence](../data-formats/prepared-forge.md).

## Logs and reports

```bash
mf runs logs RUN_ID --testing --limit 50
mf runs report RUN_ID --testing --section outputs
```

Events use a prefix of the retained JSONL in file order, bounded to 1 MiB and up to `--limit` events, default 100, range 1–1,000. Invalid lines are marked in the result. Fixed Engine and validation stdout and stderr streams use independent 12 KiB tails. The terminal can also show a bounded Engine TUI diagnostic stream when retained. Engine tails do not use the event count limit. Some event producers omit time, level, or stage. There is no follow mode or log filtering by level or stage.

Report sections accept `all` (default), `summary`, `inputs`, `outputs`, `diagnostics`, and `artifacts`. Details vary by operation and phase.

| Operation | Evidence you may find |
| --- | --- |
| Acquisition | Request outcomes and discoveries, then completed downloads, reuse and failures where applicable |
| Normalization | Preparation and Rust compatibility, job results, verification and integrity, registry publications and primary files |
| Merge | Selection and candidate IDs, input streams, ordering, preparation blockers, Engine and registration outcomes |
| Conversion | Source merge publication, target quote, reference selection and route policy, counters when retained or available from matching pinned manifests, indexed output evidence |
| Pipeline | Stage states, retained phase, child identifiers and reports where recorded |

Report reads are bounded to 4 MiB for report documents and 512 KiB for the indexed attempt outcome fallback. Unknown schemas can expose recognized scalars and field names, with original evidence available through JSON. A missing or invalid report describes evidence availability. It does not change the indexed run state.

## Attempts and recovery

Forge retains an attempt outcome before finalizing the registry. If finalization was interrupted, reconciliation can apply the verified metadata. Processing-specific recovery handles committed outputs whose registration is incomplete. [Run and dataset maintenance](../../maintenance/publications.md#recover-run-index-metadata)

## Historical evidence and index maintenance

```bash
mf runs index-legacy --testing --limit 50
mf runs reconcile --testing --limit 50
```

Both commands default to preview. The shared paging range matches `list`. Use the [maintenance reference](../../maintenance/publications.md) for eligibility, decision codes and explicit apply syntax. A preview with blocked or unresolved candidates can still return successfully. Inspect the candidate decisions separately from the process exit code.

## Delete runs

Use the public Forge ID from the selected registry. Delete needs an exact indexed run ID. Engine execution IDs, publication IDs, attempt IDs and local artifact paths have separate purposes. Production is the default. Use the same environment for preview and apply. Existing registries need the run-deletion migration. Check the selected database with `mf database status --testing` or the corresponding production selector.

```bash
mf runs delete FAILED_RUN_ID --testing
mf runs delete RUN_ID_1 RUN_ID_2 --testing
mf runs delete PIPELINE_RUN_ID --testing --with-children
mf runs delete RUN_ID --testing --history-only
mf runs delete --testing --apply --plan "PLAN_PATH"
```

| Mode | Effect |
| --- | --- |
| Full deletion (default) | Remove the indexed run payload, attempts and resource associations, plus the reviewed, exclusively owned disposable files and empty directories. A marker preserves the public run identity and deletion state. Detached paths with unproven ownership are retained. An unrecognized file inside an owned tree blocks full deletion. |
| `--history-only` | Hide the run from ordinary history while preserving its recorded status, attempts, resources, relationships, and files. The result reports zero files removed and zero bytes reclaimed. |

Resource associations are pointers. Removing one leaves its referenced archive or publication intact. Both modes preserve raw archives, managed acquisition records, processed publications and their files, catalogue metadata, recipes, and runtime configuration. Execution intents, prepared work, selection reports, pipeline checkpoints and reusable results can still have consumers after an action finishes. Review the exact blocking path and identity in the preview. For example, use the operation's registration recovery procedure when a verified publication still needs its execution intent. Use history-only removal when you want to hide the run while preserving required evidence.

`--with-children` expands descendants through indexed relationships and deduplicates overlapping selections. Review every expanded ID. A full deletion with surviving descendants is blocked. A surviving parent's recorded references to the child remain historical evidence. Active owned work blocks removal. For a nonterminal record, preview `mf runs reconcile --testing` and assess the retained outcome. Age alone cannot establish that work has ended.

### Apply and recovery

Preview writes a plan under the workspace data root in `.jobs/run-cleanup/` and reports its location. Apply uses that saved plan alone. It accepts no replacement run IDs or mode flags and requires no additional confirmation. The plan pins the environment, registry identity, selections, descendants, attempts, dependencies, and reviewed file inventory. A changed run, new child, new claim, changed dependency, or changed inventory can block apply. Review a new plan if a fresh apply is blocked. A blocked batch leaves targets unchanged.

Full deletion records a receipt and marks history as retiring before file removal. It records confirmed unlinks as they happen and removes the indexed payload after cleanup finishes. If cleanup is interrupted, run `mf runs show RUN_ID --testing` to find the marker and receipt, then retry **the original** `--apply --plan` command. A file already absent during retry is tracked separately from confirmed removals. If the retry is blocked, inspect the receipt and the reported path or dependency before taking further action. Removed file bytes measure logical file sizes. Actual filesystem reclamation is unmeasured for full deletion.

Explicit `show` explains the removal. After history-only removal, `logs` and `report` can still read retained evidence. Full deletion reports evidence unavailable through the removed run ID. Legacy indexing skips removal markers. Reconciliation leaves full-deletion markers alone and keeps history-only entries hidden if their metadata is finalized.

## JSON results

Place [global `--json`](index.md#global-options) before `runs`.

```bash
mf --json runs list --testing --limit 10
mf --json runs show RUN_ID --testing
mf --json runs report RUN_ID --testing
```

List returns schema version 2 with `runs`, `offset`, and `more`. Indexed show returns a schema version 2 record with attempts, resources, children, and truncation flags. Historical show can return a retained report instead. Events return `run_id`, `events`, and a truncation flag or missing status. Engine logs return named streams or a status. Report returns the underlying operation document or an availability record regardless of `--section`. Maintenance returns `dry_run`, `offset`, and per-item `outcomes`. Terminal-only supplementary log streams and projected report summaries do not change those contracts.

Deletion uses its own structured result. Preview includes the plan location, reviewed targets, estimates and blockers. Apply includes the receipt location, state and confirmed effects. A blocked or partial deletion exits nonzero. An unknown ID fails. A completed plan can be reapplied to inspect its recorded outcome.

```bash
mf --json runs delete RUN_ID --testing
```

### Exact reconciliation

Use `mf runs reconcile --run-id RUN_ID --testing` to preview the latest
indexed attempt of one run in the current workspace and registry. Add
`--apply` after review to finalize metadata from the retained outcome.
A repeated, consistent apply reports **Already reconciled**. Conflicts,
missing evidence, and a fully deleted run prevent repair. The selector
does not accept a pagination offset. [Pipeline recovery](../../workflows/pipeline.md#processing-and-execution-history)
