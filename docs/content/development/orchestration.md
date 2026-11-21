# Extend orchestration and persistence

Use this guide for Forge changes that cross CLI, database, or publication boundaries. Start by identifying the operation owner and the persisted evidence its downstream consumers need.

## Change Forge orchestration

A conversion or merge input selector is a representative change. Keep CLI parsing and rendering in the operation's command module. Resolve the workspace and configuration using `ApplicationContext`, `ProjectPaths`, and the existing managed configuration library. Send both JSON and terminal requests through the same service so they make the same selection and return consistent exit statuses.

1. State whether the selector names an instrument, file dataset, registry publication, or Engine publication. Follow the current merge or conversion discovery service for lineage verification and deterministic handling of competing outputs.
2. Validate editable TOML separately from database-backed preparation. Prepared contracts pin inputs and carry operation-specific fields. Preserve Engine IDs exactly through JSON and registry text representations. Keep dry-run behavior consistent with the operation's current contract. For conversion it writes no artifacts and invokes no Engine.
3. Reuse the managed binary resolver and supported production validation interface. A built binary and an installed binary can differ. Retain the submitted bytes and ensure the operation verifies the pinned job before launching. Test paths with spaces, explicit paths vs managed names, ambiguity, missing inputs, stale pins, and nonzero exits.
4. Separate execution, physical validation, and registration in the result. Review the operation's recovery interfaces before adding a suggested recovery command. Update the [CLI reference](../reference/index.md) and the relevant workflow and [prepared contract](../reference/prepared.md).

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

Use `test_causal_merge.py` for small manifest-backed Parquet examples. Use full publication validation when the change affects conclusions drawn from committed output, and test legacy versions still accepted by the consumer. Update the [publication formats](../reference/publications.md) and [Python dataset API](../reference/python-datasets.md) at the boundary changed. [Discovery](https://github.com/flowdrivenml/marketforge/blob/main/forge/datasets/discovery.py) · [Dataset and stream handles](https://github.com/flowdrivenml/marketforge/blob/main/forge/datasets/dataset.py) · [Causal discovery tests](https://github.com/flowdrivenml/marketforge/blob/main/forge/tests/unit/datasets/test_causal_merge.py)
