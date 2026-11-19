# Architecture

Forge owns CLI parsing, provider I/O, managed raw copies, recipe resolution and registry transactions. Engine owns decoding, reconciliation, validation and publication. PostgreSQL stores identities, specification evidence, nominal raw inventory, rankings and file lineage. The filesystem holds the bytes, run receipts and committed publication artifacts.

| Contract | Producer and consumer |
| --- | --- |
| Source scope and registration | Forge adapters in `forge/historical/archives/sources/` resolve archive identities. Acquisition and storage services persist remote and local evidence used by normalization. |
| Profiles | Forge stages and installs TOML through `forge/historical/profiles/service.py`. Rust compiles it in `engine/src/historical/config/catalog.rs` and shared formats perform decoding. |
| Normalization handoff | Forge emits schema 4 in `forge/historical/normalization/batch.py`. Rust validates in `engine/src/historical/config/batch.rs`. Forge checks capabilities and verifies the structured result in `forge/historical/normalization/execution.py`. |
| Merge handoff | Forge prepares the job in `forge/historical/merge/service.py`. Rust loads and resolves checksum-pinned inputs in `engine/src/historical/job/merge_load.rs` and the historical merge resolver. |
| Publications | Rust commits files and manifests. Forge verifies provenance, inventory and bounds before registering files in PostgreSQL. A failed database transaction leaves filesystem evidence available for registration-only recovery. |

Engine processing does not require PostgreSQL credentials. Read [prepared contracts](../reference/prepared.md), [publication evidence](../reference/publications.md) and the [development verification commands](index.md).
