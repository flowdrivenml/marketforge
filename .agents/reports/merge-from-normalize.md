# Source-derived merge recipes

Forge now accepts `mf merge new NAME --from-normalize SOURCE --testing`. The command creates an exclusive version-1 TOML with the source interval and selection. Validation runs without a registry connection. Preparation resolves normalized primary files against the testing PostgreSQL registry and retains a validated job or a blocked report.

Discovery uses file memberships and contributing resolved normalization tasks for native scope, historical family, feed and registered source evidence. Retained normalization intent and submitted-batch fingerprints identify matching publications. Unlinked or repeated submissions require explicit PostgreSQL publication approval. The selected files remain grouped by publication in deterministic order. Legacy explicit recipes retain their supplied order. Prepared reports retain the source fingerprint, file and publication identities, checksums, streams, paths and interval. The existing Rust validator and execution checks remain authoritative. The database schema and published datasets were untouched.

The live testing registry resolved 34 requests to 93 overlapping files in two publications from one verified submission. Full preparation reported multiple selected streams per publication and unsupported v5 depth, with no job published. Narrowing to `bybit.spot.spot.BTCUSDT` trades selected file dataset 115 and passed Rust job validation. No normalization or merge workload was executed.

The focused Forge test selection passed 111 tests. Merge modules, CLI and new tests passed Pyright. Ruff and the Zensical documentation build passed. Strict analysis of shared dataset registration still reports existing PyArrow typing gaps.
