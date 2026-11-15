# Architecture

MarketForge separates user-facing orchestration from data-intensive processing while sharing data concepts across historical and live work.

| Component | Responsibilities visible in the local tree |
| --- | --- |
| Engine — Rust, `engine/` | Historical normalization, merge and conversion; live connectivity and processing; shared market-data, decoding and persistence components. |
| Forge — Python, `forge/` | CLI, source acquisition and import planning, storage management, normalization preparation, and PostgreSQL registry integration. |

Forge's current normalization commands prepare artifacts without executing Engine. There is a concrete producer/consumer mismatch: Forge emits schema-3 batches, while Rust's prepared-batch loader requires schema 4. Forge preparation also records older handoff limitations; those diagnostics are not a current Rust contract reference. Neither a module nor an old integration report proves current interoperability.

Historical and live components share lower-level capabilities. Broader research and financial engineering workflows are the intended platform direction; this assignment did not establish their operational readiness.

> **Documentation draft.** Detailed boundaries, lifecycle, failure behavior, and contract diagrams will be added after targeted verification. Existing `engine/docs/`, `docs/historical/`, and module-local documentation remain unmigrated.
