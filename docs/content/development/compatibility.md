# Compatibility and contributions

Review a change by following the contract from its producer to each consumer. Keep editable configuration, submitted jobs, committed publications and registry records distinct. An older installed executable and a newer Forge checkout can encounter a version mismatch even when both components build successfully.

| Contract | Producer, validators and consumers | Review |
| --- | --- | --- |
| Application and operation TOML | CLI configuration library and Pydantic models feed the Forge services | Accepted fields and types, path and name resolution, defaults and explicit legacy rejection |
| Format profiles | Authors and Forge profile lifecycle feed the Rust catalogue compiler and task resolver | Version, physical fields, source mapping and interpretation, installed provenance, exact outputs for sample records |
| Prepared inputs and process results | Forge serializers and Engine loaders, followed by Forge execution and result readers | Normalization batch v4, merge's pinned single job, conversion's standalone job v1 and separate Forge collection. Check actual command capability and reject unsupported fields or formats |
| Catalogue and migrations | Registration and catalogue services, database constraints and current migration runner | Identity and historical authority, applied checksums, upgrading an existing registry, rollback and idempotence |
| Canonical columns and auxiliary data | Engine writers and publication validators feed merge, conversion and dataset readers | Physical types, exact values, indices, ownership and boundary semantics. Keep auxiliary evidence covered by the inventory |
| Publication manifests and registry summaries | Engine commit, Forge verification, publication/file registration and downstream consumption | Supported versions, checksums and bounds, path safety, exact stream and file membership where available |
| Structured CLI and Python dataset API | Operation reporting and filesystem/registry readers feed scripts and notebooks | Fields, status and exit codes, old manifests, scan/projection constraints and conflict diagnostics |

Use the [prepared contracts](../reference/prepared.md), [profile reference](../reference/profiles.md), [publication formats](../reference/publications.md) and [Python API](../reference/python-datasets.md) for exact schemas and supported versions. The Python discovery gate accepts normalized protocol versions 3, 4 and 5, and separate merge contracts including protocol 1 with chronological physical schema 1 or causal physical schema 2. The discovery gate also has a protocol-2 merge branch for timeline schema 1. That branch establishes the reader's version check, with further inventory and identity checks following it. Conversion job acceptance and Python publication discovery are separate boundaries. Current Python discovery recognizes normalization and merge manifests, and converted output is verified by conversion orchestration.

## Follow the change across versions

1. Identify the existing data or document that must still work. Keep a representative prior-format fixture and one for the new format.
2. Trace the serializer, parser, operation validator, producer and actual consumer. For a version change, exercise rejection of unsupported versions as well as acceptance of supported ones. A loader accepting JSON alone cannot prove a useful publication.
3. Review persisted datasets and applied migrations as immutable evidence. Use the existing workflow to regenerate a job or produce a new publication when the change requires it. Keep migrations additive to the applied sequence and test the prior-to-current path.
4. Check a small positive case and a failure case where missing or changed provenance would otherwise be accepted. Use an isolated output destination. Record any remaining limitation such as a platform boundary, a skipped recorded fixture, or a difference between an installed and source-tree executable.

For example, adding a causal marker to a timeline affects the physical schema discriminator, sequence validation, merge writer, conversion reader and Python discovery. Compare the old and new schema branches and retain coverage for timestamp regression with correct physical order. Changing a label in terminal presentation has a smaller scope unless it changes a machine-readable field or identifier.

## Prepare a reviewable contribution

- Describe the user-visible problem, the responsible boundary and the observable result. Include a small reproduction or an exact fixture.
- Keep changes within the affected operation or shared contract. Include matching documentation updates, and link to the authoritative contract rather than duplicating it.
- Run the applicable Ruff, Pyright, Rust formatting/checking and focused tests from [Testing](testing.md). Include verified outcomes, skips and prerequisites. For diagrams, inspect the rendered site on a wide and narrow viewport in addition to building it.
- Review the diff for unrelated changes, generated data and secrets. Include before/after behavior for an interface change and the supported rerun or migration path if applicable.
- State what the verification establishes. For example, “portable conversion validates a prepared fixture without writing output” is a structural and planning check. It leaves publication and database behavior to separate checks.

The repository has engineering notes and dated investigations under `docs/historical/`. Where they describe an older contract or workload, use the current implementation and reference for behavior. [Repository engineering instructions](https://github.com/flowdrivenml/marketforge/blob/main/.agents/AGENTS.md)
