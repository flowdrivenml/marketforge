# Historical normalization contracts

Runtime settings come from `data/.config/historical/general.toml` through `shared::historical`. Relative runtime and prepared-batch paths resolve against an explicit project root without changing process CWD.

The production loader is `historical::config::{read_prepared_batch, load_normalization_batch}`. A schema-3 `normalization_batch` must have status `prepared`, run/ranking/interval metadata, independent jobs, an `instrument_namespace`, and an `instruments` catalog. Tasks contain source paths, full instrument classifications, native symbols/specifications or explicit family scope, stream IDs/ranks, and optional Gate.io feed selection. IDs remain integer values throughout. Structural loading does not open archives or create outputs.

## Offline identity authority

`instruments` persists positive signed-64-bit canonical IDs keyed by `(exchange, instrument_type, market_category, symbol)`. Every entry includes base/quote assets and tick size; contract encodings require positive contract value and asset. Duplicate identities/IDs and task metadata conflicts fail. A namespace must use one durable allocation authority across batches. Explicit external IDs may be copied; otherwise allocate the next unused positive ID and persist it before execution. Never renumber/reuse IDs, allocate on each run, or hash identities. The maintained fixture uses `marketforge-historical-fixture-v1`; its initial concrete identities were sorted lexicographically and assigned 1..N once, and future additions append IDs. These IDs are local to that namespace, not database registry IDs.

Family tasks retain null concrete fields and declare `scope = {kind:"family", identifier:..., members:[{instrument_id:..., archive_member:...}]}`. Each member resolves its own catalog metadata. Depth members require explicit archive member names; existing source selection and strict instrument validation are reused. Trade family expansion uses the existing concrete-instrument matcher and rejects source symbols absent from the family authority. Expanded internal task IDs retain the first original ID and allocate additional checked IDs above the source-task maximum; source task/stream/rank and catalog identity/fingerprint remain in resolved provenance. Family stream ranks are dense in `(requested rank, concrete symbol, stream ID)` order. Concrete-only batches preserve supplied ranks.

The maintained real-data fixture currently has no authoritative metadata for its OKX family members. Those selections fail explicitly until members/specifications are supplied; concrete streams remain independently resolvable. Synthetic tests establish supported family expansion without presenting invented specifications as real metadata.

## Self-contained profiles

Only `profiles/{trades,depth}/**/*.toml` are discovered, deterministically. Schema-2 profile filenames equal `exchange.instrument_type.market_category.data_type`. Profiles contain source, fields, and normalization mappings. No external physical-schema file is required. Existing trade/depth compilers validate generated specs; fingerprints include effective definitions and source interpretation and ignore locations/comments. Unsupported profiles remain inventory-only.

`feeds.snapshot` can provide explicit source, fields and normalization for a Gate.io snapshot representation. Current futures snapshots are gzip JSONL with object levels `{p,s}`; incremental Gate.io rows remain CSV. XLSX support is constrained to the implemented one-sheet, header, timestamp/asks/bids layout with seconds and ascending stable timestamp sorting. Source cadence, observed precision and book depth/frequency facts are descriptive, not truncation controls.

Legacy inline/profile job loading remains a separate compatibility boundary. The CLI still accepts its existing job/directory flags until Task 07 wires prepared batches into the external execution protocol. Canonical schemas, manifests and result JSON are unchanged by Task 02. The old Forge producer must emit the offline authority and family bindings; Rust never queries PostgreSQL.
