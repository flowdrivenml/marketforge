# Historical normalization

Forge prepares production schema-v4 JSON from completed managed raw copies,
executes submitted JSON through the managed Rust binary, and registers verified
committed output in the existing PostgreSQL dataset catalogue. Preparation,
execution, publication and database registration have separate outcomes.

```sh
mf engine build normalize
mf normalize prepare NAME --testing
mf normalize run data/runs/PREPARE_ID/normalize-batch.json --testing
mf normalize execute NAME --testing
mf normalize register /absolute/committed/dataset --testing
```

Preparation is read-only against the registry and persisted rankings. It does not
refresh metadata/statistics, generate profiles or run a workload. Output is
`data/runs/<prepare-id>/{normalize-batch.json,prepare-report.json}`. Instrument IDs
come from persisted `historical.instrument_authority` mappings, separate from
catalogue row IDs. Local historical specifications and profile fingerprints come
from immutable registration bindings. Conflicting/missing metadata blocks handoff.
Initialization enriches exchange downloads with versioned inferred specifications
and persisted catalogue/archive family membership. Preparation consumes these
immutable records, preserving exact concrete selections and explicit family
populations. Inference is operational policy, not verified historical observation.
Rust-supported family references in submitted JSON remain unchanged.

`execution.py` retains exact submitted bytes and snapshots effective configuration,
profiles and executable. It never reconstructs recipes or rewrites user edits.
The Rust compiler performs nonexecuting validation; Rust owns normalization,
reconciliation, resource accounting, the optional terminal UI and publication.
Configuration remains `data/.config/historical/general.toml`.

`datasets.py` verifies committed destinations, checksums, manifest semantics and
real Parquet inventory before short, idempotent catalogue transactions. Clean and
Degraded remain distinct. Independent successful jobs register after partially
failed sessions. A database outage leaves publication committed and registration
pending; `normalize register` retries registration alone. Direct Rust output uses
the same service without inventing an original submission checksum.

See the [command reference](../../../docs/content/reference/cli/forge.md) and
[synthetic workflow](../../../docs/content/workflows/acquisition/forge.md), including
current limitations and migration requirements.

Preparation orders sources by their registered UTC coverage start and exclusive
end within each stream. Sources with equal coverage use registered provider and
object identity, including the numeric part in Bitget trade object keys. Rust
derives source priority from the resulting job position. Missing, reversed, or
internally conflicting coverage for a registered source blocks preparation.
Declared overlaps alone can be legitimate, and Rust applies its reconciliation
policy to observed event boundaries and native depth sequences.

Family selections enumerate each archive's observed symbols. The preparation
report records the registry population, observed and selected populations, and
any unsupported identities. An observed symbol with missing applicable
specifications blocks the archive. A mixed member can bind multiple concrete
instruments by sharing its member name across those instruments.

Native Bitget snapshot workbooks are classified in the registry and decoded by
the snapshot-only Bitget depth profiles. Rust's schema-v4 feed override selects
Gate.io depth variants. Bybit and OKX mixed depth archives are decoded by their
profiles' native record types and may lack a separate acquisition feed label.
