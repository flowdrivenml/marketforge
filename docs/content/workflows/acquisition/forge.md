# Register and normalize manually supplied data

Raw registration copies and verifies bytes; it does not normalize them. Profiles describe interpretation, registration recipes declare source/provenance and reference existing catalogue metadata, normalization recipes select registered inputs, and prepared JSON is the exact Rust workload contract. Normalization runtime policy lives in `data/.config/engine/normalize.toml`.

## Synthetic shared-instrument example

Use the [synthetic trade and depth bundle](../../examples/catalogue/index.md) for complete source files, catalogue declarations and executable commands. Its fictional venue has an independent profile namespace.

## Ownership, history and recovery

Provider/recorder and product/feed are distinct from trading venue. Registration never substitutes `external` for a venue, fabricates a URL/discovery membership, refreshes current market metadata or supplies missing historical specifications. Coverage declarations are separate from observed timestamp bounds and continuity. An interval crossing a specification change cannot silently select one version. Unsupported classes/venues/layouts remain blockers.

Migration `0013_unified_forge.sql` evolves the existing registry without moving raw bytes. `raw_objects` is the source-object authority; remote IDs retain their values. Legacy local IDs map explicitly through `legacy_raw_identity_map`. `raw_sources`, immutable `raw_interpretations`, `local_archives` and append-only `raw_bindings` retain provenance, physical revisions and interpretation revisions. `remote_archives` and `external_archives` are compatibility views; acquisition and registration writes target the shared tables. Existing storage browsing, verification, cleanup and database backups retain local-copy identities and paths. Legacy import commands use the same copy/source-object services, but old format drafts remain unapproved and must be replaced with manually authored schema-2 profiles and full registration declarations for normalization.

Do not migrate a shared testing database underneath another process. Use the existing migration workflow during a coordinated maintenance window. Integration tests instead apply the full chain in operation-owned disposable schemas on the positively verified testing database. Production is not migrated by these commands.

Planning freezes submitted declarations and bounded inventory in an operation-owned directory, with no copies/profile installation/registration rows. Run independently freezes and revalidates its own inventory; it does not silently apply a previously edited plan artifact. Copying uses bounded buffers and before/after source stability evidence. Registration never adopts originals in place or moves/deletes them. Recoverable copy reservations retain intended destinations/checksums before publication. Retries reuse unchanged verified bytes; changed bytes produce revisions. New bindings can reuse the same physical copy, retaining old downstream interpretation evidence.

The register workflow checks catalogue identity, historical applicability, and the installed profile before publishing any managed copies. Use the legacy raw-only import when interpretation is incomplete. A run retains submitted recipe, intent, copy receipts and outcome under `data/runs/register-*`; retry the same recipe to recover. Environment provenance remains `testing`, with no production fallback.

For all command syntax, execution receipts, automatic dataset registration, Degraded integrity, partial sessions and registration-only recovery, see the [Forge command reference](../../reference/cli/forge.md).
