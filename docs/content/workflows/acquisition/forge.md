# Register and normalize manually supplied data

Raw registration copies and verifies bytes; it does not normalize them. Profiles describe interpretation, registration recipes declare source/provenance and reference existing catalogue metadata, normalization recipes select registered inputs, and prepared JSON is the exact Rust workload contract. Runtime policy remains in `general.toml`.

## Synthetic shared-instrument example

These are synthetic assertions, not real historical Binance specifications. Use isolated testing storage and an isolated profile catalogue: the example depth definition conflicts if a different definition is already installed under its ID. Never overwrite an existing profile to run an example.

The complete documents are [registration.toml](../../examples/forge/registration.toml), [normalization.toml](../../examples/forge/normalization.toml), [trade profile](../../examples/forge/binance.spot.spot.trades.toml) and [depth profile](../../examples/forge/binance.spot.spot.depth.toml). Both groups reference a registered instrument and an applicable immutable specification. Register the catalogue prerequisites first using the [manual catalogue workflow](../catalogue/index.md). Optional specification fields include exact decimal `qty_step`, `min_qty`, `max_qty`, `min_notional` and `max_notional` with their denominations, contract value/asset, retrieval `as_of`, effective `valid_from`/`valid_to`, and provenance. Missing limits stay unknown; order-submission limits do not filter historical records.

Create only the small synthetic source archives below; originals remain read-only during registration:

```python
from pathlib import Path
from zipfile import ZipFile
root = Path("data/tests/historical/forge-example/source")
root.mkdir(parents=True, exist_ok=True)
with ZipFile(root / "trades.zip", "x") as out:
    out.writestr("trades.csv", "1,100,1,100,1767225600000000,false,true\n2,101,1,101,1767225601000000,true,true\n")
with ZipFile(root / "depth.zip", "x") as out:
    out.writestr("depth.jsonl", '{"timestamp":1767225600,"asks":[[101,1]],"bids":[[99,1]]}\n{"timestamp":1767225601,"asks":[[102,1]],"bids":[[100,1]]}\n')
```

```bash
mf engine build normalize
mf acquisition profiles add binance.spot.spot.trades --from docs/content/examples/forge/binance.spot.spot.trades.toml --testing
mf acquisition profiles add binance.spot.spot.depth --from docs/content/examples/forge/binance.spot.spot.depth.toml --testing
mf acquisition register new synthetic --from docs/content/examples/forge/registration.toml
mf acquisition register plan synthetic --testing
mf acquisition register run synthetic --testing
mf normalize run docs/content/examples/forge/normalization.toml --testing
```

The unmodified production runtime budgets can be too large for a small development machine: tests use an isolated runtime configuration, never edit production policy as an orchestration workaround.

## Ownership, history and recovery

Provider/recorder and product/feed are distinct from trading venue. Registration never substitutes `external` for a venue, fabricates a URL/discovery membership, refreshes current market metadata or supplies missing historical specifications. Coverage declarations are separate from observed timestamp bounds and continuity. An interval crossing a specification change cannot silently select one version. Unsupported classes/venues/layouts remain blockers.

Migration `0013_unified_forge.sql` evolves the existing registry without moving raw bytes. `raw_objects` is the source-object authority; remote IDs retain their values. Legacy local IDs map explicitly through `legacy_raw_identity_map`. `raw_sources`, immutable `raw_interpretations`, `local_archives` and append-only `raw_bindings` retain provenance, physical revisions and interpretation revisions. `remote_archives` and `external_archives` are compatibility views; acquisition and registration writes target the shared tables. Existing storage browsing, verification, cleanup and database backups retain local-copy identities and paths. Legacy import commands use the same copy/source-object services, but old format drafts remain unapproved and must be replaced with manually authored schema-2 profiles and full registration declarations for normalization.

Do not migrate a shared testing database underneath another process. Use the existing migration workflow during a coordinated maintenance window. Integration tests instead apply the full chain in operation-owned disposable schemas on the positively verified testing database. Production is not migrated by these commands.

Planning freezes submitted declarations and bounded inventory in an operation-owned directory, with no copies/profile installation/registration rows. Run independently freezes and revalidates its own inventory; it does not silently apply a previously edited plan artifact. Copying uses bounded buffers and before/after source stability evidence. Registration never adopts originals in place or moves/deletes them. Recoverable copy reservations retain intended destinations/checksums before publication. Retries reuse unchanged verified bytes; changed bytes produce revisions. New bindings can reuse the same physical copy, retaining old downstream interpretation evidence.

The register workflow checks catalogue identity, historical applicability, and the installed profile before publishing any managed copies. Use the legacy raw-only import when interpretation is incomplete. A run retains submitted recipe, intent, copy receipts and outcome under `data/runs/register-*`; retry the same recipe to recover. Environment provenance remains `testing`, with no production fallback.

For all command syntax, execution receipts, automatic dataset registration, Degraded integrity, partial sessions and registration-only recovery, see the [Forge command reference](../../reference/cli/forge.md).
