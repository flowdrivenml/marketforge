# Extend orchestration and persistence

Use this guide for Forge changes that cross CLI, database, or publication boundaries. Start by identifying the operation owner and the persisted evidence its downstream consumers need.

## Change Forge orchestration

A conversion or merge input selector is a representative change. Keep CLI parsing and rendering in the operation's command module. Resolve the workspace and configuration using `ApplicationContext`, `ProjectPaths`, and the existing managed configuration library. Send both JSON and terminal requests through the same service so they make the same selection and return consistent exit statuses.

1. State whether the selector names an instrument, file dataset, registry publication, or Engine publication. Follow the current merge or conversion discovery service for lineage verification and deterministic handling of competing outputs.
2. Validate editable TOML separately from database-backed preparation. Prepared contracts pin inputs and carry operation-specific fields. Preserve Engine IDs exactly through JSON and registry text representations. Keep dry-run behavior consistent with the operation's current contract. For conversion it writes no artifacts and invokes no Engine.
3. Reuse the managed binary resolver and supported production validation interface. A built binary and an installed binary can differ. Retain the submitted bytes and ensure the operation verifies the pinned job before launching. Test paths with spaces, explicit paths vs managed names, ambiguity, missing inputs, stale pins, and nonzero exits.
4. Separate execution, physical validation, and registration in the result. Review the operation's recovery interfaces before adding a suggested recovery command. Update the [CLI reference](../reference/index.md) and the relevant workflow and [prepared contract](../reference/data-formats/prepared.md).

For a good nearby example, trace merge `prepare_job` from file selection to `--validate-job`. Follow `test_validation_rejection_keeps_generated_job_and_blocks_processing` to see why valid JSON must not become a claim that the Engine can run it. [Merge service](https://github.com/flowdrivenml/marketforge/blob/main/forge/historical/merge/service.py) · [Merge CLI integration tests](https://github.com/flowdrivenml/marketforge/blob/main/forge/tests/integration/cli/test_merge.py)

## Change catalogue or registry behavior

Keep provider identity, execution venue and historical specification authority separate. For example, registering an external perpetual recording needs the observed source provenance and a resolved instrument and effective contract specification. A symbol match against the latest instrument catalogue alone cannot prove how to interpret a historical contract quantity. Inspect registration `preflight` and the normalization preparation path to see where missing or ambiguous evidence stops a request.

Use the existing migration sequence for a schema change. Applied migration scripts are identified and checksum-checked. Add a new migration for a change rather than editing a script already applied. Run it against a clean testing registry and the relevant previous schema in an isolated fixture. Check preserved IDs, rollback on failure and idempotent registration where those guarantees apply. Avoid broad cleanup of shared testing data.

When testing lineage, assert both publication-level dependencies and any exact file-level contribution that the evidence supports. `register_files` derives records from verified publication evidence and distinguishes file membership scope. Test a stale fingerprint or inconsistent path to establish the rejection boundary. Update the [registration reference](../reference/registration.md) and [dataset CLI](../reference/cli/datasets.md) if the visible semantics change. [Registration preflight](https://github.com/flowdrivenml/marketforge/blob/main/forge/historical/registration/service.py) · [Migration runner](https://github.com/flowdrivenml/marketforge/blob/main/forge/shared/database/migrations.py) · [File registration](https://github.com/flowdrivenml/marketforge/blob/main/forge/datasets/registration.py)

## Change publications or dataset readers

For a new column, manifest field or auxiliary index, trace the producer, content and checksum validation, registration summary, and every reader. For example, causal merge preserves sequence and reconstruction in physical schema 2. Python discovery must verify exact per-file rank membership rather than assuming every rank between the Parquet statistics endpoints is present. Registry summaries can enrich that conclusion only when they agree with physical evidence and their declared scope.

| Change | Verify |
| --- | --- |
| Physical column or type | Arrow schema, nullability, decimal scale, signed integer nanoseconds, masked or nested values, and mixed-schema behavior |
| Manifest inventory or ownership range | Commit-time checks, registered primary paths, checksum coverage, referenced auxiliary content, and rejection of conflicting metadata |
| Dataset preview or scan | Field projection, materialization behavior, timestamp filtering, bounded previews and unsupported projection diagnostics |
| Stream membership | Multi-stream primary files, evidence for exact ranks, and behavior with missing or inconsistent registry information |

Use `test_causal_merge.py` for small manifest-backed Parquet examples. Use full publication validation when the change affects conclusions drawn from committed output, and test legacy versions still accepted by the consumer. Update the [publication formats](../reference/data-formats/publications.md) and [Python dataset API](../research/python-api.md) at the boundary changed. [Discovery](https://github.com/flowdrivenml/marketforge/blob/main/forge/datasets/discovery.py) · [Dataset and stream handles](https://github.com/flowdrivenml/marketforge/blob/main/forge/datasets/dataset.py) · [Causal discovery tests](https://github.com/flowdrivenml/marketforge/blob/main/forge/tests/unit/datasets/test_causal_merge.py)

Read the component chapters for implementation details. [Forge](forge.md) · [Normalization Engine](normalize.md) · [Merge Engine](merge.md) · [Conversion Engine](convert.md)

## Verify a persisted-field change end to end

Use the published depth observation row span as a worked boundary. A fixture with a retained zero-row observation followed by one changed level establishes two different expectations. The first observation remains addressable at its offset with count zero. The next observation has the same starting offset and count one. Encode these expectations from the input when extending the schema.

1. Change the operation’s writer and its manifest or schema declaration together. Follow [normalization output](normalize.md) and use the [normalized publication reference](../reference/data-formats/publication-normalized.md) to identify affected fields.
2. Update structural and row-span validators before adding a consumer. In merge, inspect replay of a selected part and the mapping from canonical rows to observation spans. In Forge, check how inventory, registry summaries and Stream readers encounter legacy files.
3. Run the focused historical normalization and merge suites and the Forge dataset reader tests. Check schema nullability, field units, the zero-row event and preservation of physical ordering. Use [test selection](testing.md) for commands and prerequisites.
4. State whether existing artifacts remain readable, then update the publication reference and any affected [inspection procedure](../workflows/inspection/datasets.md).
