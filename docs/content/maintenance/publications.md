# Run and dataset maintenance

Use this reference after [inspecting the run](../workflows/inspection/runs.md) or [identifying the dataset](../workflows/inspection/datasets.md). Registry repair, physical verification and file removal have different effects. Production is the database default for applicable commands. Explicit deletion, force deduplication and conversion deletion-history lookup require testing. Keep the exact [registry and identifier domains](../concepts/identity.md) with every reviewed plan.

| Action | Default effect |
| --- | --- |
| Runs list, show, logs, report | Read retained history and evidence |
| Datasets browsing, timeline detail, streams, lineage, schema, head, supersession | Read recorded metadata or bounded selected data |
| Datasets verify | Read physical files and check fingerprints |
| Runs index-legacy, reconcile | Preview only. `--apply` writes registry metadata. |
| Datasets deduplicate, delete | Save a review plan. `--apply` can retire outputs and remove files. |
| Datasets migrate-registration | Write eligible file registrations unless `--dry-run` is supplied |
| Datasets refresh-summary | Rewrite the recorded summary for one file |

## Recover run index metadata

Legacy indexing anchors older report directories to the selected registry. It checks the supported identity and operation, matching registry UUID and environment, and valid retained start and finish timing when available. Canonical Forge run directories are outside legacy indexing. A matching existing entry is reported as indexed. A managed retained running intent without conclusive liveness evidence is left for review.

Reconciliation inspects eligible attempts, their exact run and attempt IDs, artifact roots and outcome files. It checks report containment and verifies a retained report fingerprint when supplied. A successful apply finalizes registry metadata from that outcome. It leaves Engine execution, dataset files and publication registration to the corresponding operation. Unindexed local evidence requires review.

```bash
mf runs index-legacy --testing --dry-run --limit 25
mf runs reconcile --testing --dry-run --limit 25
```

Both commands preview without registry writes by default. Use the same `--limit`, `--offset` and environment when reviewing subsequent pages. Re-run the reviewed scope with `--apply` in place of `--dry-run` to authorize metadata writes. The scope is evaluated again at apply time. Reconciliation pages indexed attempts and local candidates independently, so a combined result can exceed the requested limit. Runs maintenance has no saved-plan argument.

```bash
mf runs index-legacy --testing --apply --limit 25
mf runs reconcile --testing --apply --limit 25
```

| Decision | What it means |
| --- | --- |
| `eligible`, `indexed` | An index candidate passed review, or a matching entry exists after the action |
| `unrecognized`, `canonical_run` | The directory name is unsupported, or it denotes a canonical run outside legacy indexing |
| `invalid_evidence`, `ambiguous_operation_or_identity`, `conflict` | Evidence is invalid, insufficient to identify the operation, or disagrees with the index |
| `unknown_environment`, `different_or_unverified_registry`, `missing_or_invalid_timing` | Required environment, registry or recorded timing evidence is unavailable or incompatible |
| `retained_outcome`, `reconciled` | A retained outcome is eligible for metadata finalization, or finalization was applied |
| `running_liveness_unknown`, `missing_outcome` | An outcome could not establish the next index state |
| `conflicting_artifact_root`, `missing_or_unsafe_report`, `invalid_or_conflicting_outcome` | One or more retained paths, fingerprints or identities failed review |
| `unindexed_local_evidence_requires_review` | Local evidence exists without its required index entry |

For a verified committed dataset with incomplete registration, use the original registry and the operation’s registration procedure. [Normalization recovery](../reference/cli/normalize.md#registration-and-recovery) · [Merge recovery](../reference/cli/merge.md#registration-and-recovery)

## Remove retained runs

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

## Deduplicate equivalent publications

```bash
mf datasets deduplicate --testing --operation merge
```

Ordinary deduplication reviews registered normalized and merged candidates against retained work, committed files and physical evidence. It distinguishes proven work equivalence from physical similarity with insufficient semantic evidence. A verified canonical survivor can remain selectable while an equivalent redundant publication is recorded as retained or removed, depending on the dependency and ownership checks. Ordinary apply can also backfill supported reuse evidence. The `convert` operation scope audits indexed outputs and unresolved historical conversion directories. Use explicit deletion to remove an eligible indexed conversion by exact work reference.

The preview writes a review report. Prefer applying that saved plan with the same registry and operation. Ordinary apply also accepts `--apply` without `--plan`; in that form it builds its candidate inventory at apply time. Apply rechecks candidate and canonical evidence and can resume an ordinary retirement already in progress.

```bash
mf datasets deduplicate --testing --operation merge --apply --plan PLAN
```

A protected redundant output can remain physically present while being omitted from default selection. Review decisions and blockers before interpreting a registry retirement as freed storage. Keep the reports for later audit.

### Force deduplication

Force is an explicit **testing-only** cascade review of proven equivalent candidates. It preserves the verified canonical output, evaluates downstream dependencies and can invalidate dependent testing publications when the approved target is removed. Dependent files remain unless a separate operation explicitly removes them. Unindexed or prepared references, active runs, protected survivors and ownership conflicts can block the action.

```bash
mf datasets deduplicate --testing --operation merge --force
mf datasets deduplicate --testing --operation merge --force --apply --plan PLAN
```

Use the exact saved force preview. Apply revalidates the reviewed targets, inventory and dependency graph. A changed registry, plan, or reviewed artifact can block the operation. Production force cleanup is prohibited.

## Delete explicit testing outputs

This operation intentionally discards selected testing outputs without making an equivalence claim. A preview takes one or more exact registry publication IDs or indexed conversion work keys. Repeat each selector to add targets. Repeat `--retain-publication-id` to require named, distinct registry survivors to verify. `--cascade` previews invalidation of dependent testing publications while their files remain. Existing dependents can block a preview without that policy.

```bash
mf datasets delete --testing --publication-id PUBLICATION_ID --retain-publication-id SURVIVOR_ID
mf datasets delete --testing --output-ref OUTPUT_REF
mf datasets delete --testing --publication-id PUBLICATION_ID --cascade
mf datasets delete --testing --apply --plan PLAN
```

Review the saved preview’s targets, survivors, dependencies, blockers and storage ownership. Apply requires explicit `--testing` and the exact reviewed plan. Target selectors, survivor selectors and `--cascade` belong to the preview. Apply takes these from the saved plan and rechecks registry and physical evidence. A partial or interrupted cleanup can leave a journaled retirement for review. Use the original plan when the retained journal requires it.

### Historical storage ownership

Testing and production can reference shared physical roots, including legacy locations. A testing record alone establishes no exclusive filesystem ownership. Cleanup checks the selected registry, foreign references, managed roots and physical inventory. Guards protect against unsafe filesystem layouts, changed files and conflicting dependencies. Known foreign references and unresolved checks outside the narrowly reviewed legacy-risk case remain blockers.

An exact reviewed force or deletion plan can include a target eligible only with legacy-risk acknowledgement. This occurs when legacy production references cannot be anchored sufficiently to establish path ownership under the fallback evidence. Adding the acknowledgement can break unresolved historical production references. It does not override a known foreign reference, registry identity requirements, managed-root checks, dependency checks, or other ownership and physical safety blockers. Use it only after reviewing that reported risk.

```bash
mf datasets deduplicate --testing --operation merge --force --apply --plan PLAN --accept-unverified-legacy-references
mf datasets delete --testing --apply --plan PLAN --accept-unverified-legacy-references
```

### Interpret the cleanup result

| Report item | Unit and meaning |
| --- | --- |
| Candidate and proven equivalent groups | Review group counts. Only proven equivalence supports ordinary deduplication. |
| Canonical publication | Verified preferred survivor, when one exists |
| Retained or retired publication | Logical state or selection change. Physical removal is reported separately. |
| Invalidated downstream publications | Dependent registrations affected by a cascade. Their files can remain. |
| Primary files, physical files, directories removed | Separate physical counts. Full trees can include manifests and auxiliary files. |
| Removed file allocation | Recorded allocation associated with the reviewed files. Shared storage and filesystem accounting can leave reclaimed bytes unknown. |

## Inspect supersession and deletion history

```bash
mf datasets supersession PUBLICATION_ID --testing
mf datasets supersession --output-ref OUTPUT_REF --testing
```

Registered history can record `retained` for a redundant publication still kept, `retiring` for an unfinished retirement, `removed` for a journaled physical removal, or `invalidated` for an affected downstream registration. A supersession can name a canonical replacement. Explicit deletion can leave a tombstone without one. An invalidation identifies the source retired by cleanup. Deleted indexed conversions have their own deletion history, keyed by work reference. No matching history produces an error.

Historical IDs, run reports and lineage can outlive the files. Inspect the history before interpreting references in an older report. Current physical verification can fail for a recorded removed output.

## Migrate or refresh registrations

```bash
mf datasets migrate-registration --testing --all --dry-run
mf datasets refresh-summary FILE_ID --testing
```

Migration inspects eligible legacy aggregate registrations and retained evidence for the selected registry. It supports verified normalization aggregates. Legacy merge aggregates are reported as conflicts when their historical input evidence cannot be resolved by this migration. `--all` is required and there is no single-ID selector. Omitting `--dry-run` writes eligible primary-file registrations. The migration retains the association to the old aggregate ID. Conflicts require review and are reported separately. Inspect the legacy ID with `show` for replacement file IDs after migration. Summary refresh rewrites a selected file’s recorded summary from existing evidence. It cannot repair a damaged file. For explicit recovery of lost file registrations, use the [normalization](../reference/cli/normalize.md#registration-and-recovery) or [merge](../reference/cli/merge.md#registration-and-recovery) registration command.
